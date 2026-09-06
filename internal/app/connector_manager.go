package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"crypto-screener/internal/domain"
)

// connectorEntry инкапсулирует весь жизненный цикл одного коннектора.
// Неизменяема после создания — все поля устанавливаются в конструкторе.
type connectorEntry struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped chan struct{} // закрывается когда все горутины завершены
}

// newConnectorEntry создаёт запись и сразу запускает supervisor-горутины.
func newConnectorEntry(
	ctx context.Context,
	name string,
	conn domain.ExchangeConnector,
	tickChan chan<- domain.MarketTick,
	fundingSink domain.FundingSink,
) *connectorEntry {
	entryCtx, cancel := context.WithCancel(ctx)

	e := &connectorEntry{
		cancel:  cancel,
		stopped: make(chan struct{}),
	}

	streams := []struct {
		suffix    string
		connectFn func(context.Context) error
	}{
		{
			suffix: "Spot",
			connectFn: func(c context.Context) error {
				return conn.ConnectSpot(c, tickChan)
			},
		},
		{
			suffix: "Futures",
			connectFn: func(c context.Context) error {
				return conn.ConnectFutures(c, tickChan)
			},
		},
		{
			suffix: "Funding",
			connectFn: func(c context.Context) error {
				return conn.ConnectFunding(c, fundingSink)
			},
		},
	}

	e.wg.Add(len(streams))

	for _, s := range streams {
		s := s // захват переменной цикла
		streamName := fmt.Sprintf("%s/%s", name, s.suffix)
		go runWithReconnect(entryCtx, &e.wg, streamName, s.connectFn)
	}

	// Отдельная горутина сигнализирует о полной остановке.
	// Это позволяет использовать как wg.Wait() так и select/chan.
	go func() {
		e.wg.Wait()
		close(e.stopped)
	}()

	return e
}

// stop отменяет контекст и ожидает завершения всех горутин
// с таймаутом для защиты от зависания.
func (e *connectorEntry) stop(timeout time.Duration) error {
	e.cancel()

	select {
	case <-e.stopped:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("connector stop timed out after %v", timeout)
	}
}

// ConnectorManager управляет пулом биржевых коннекторов.
// Потокобезопасен. Поддерживает Hot-Swap и Graceful Shutdown.
type ConnectorManager struct {
	// mu защищает только connectors и stopped.
	// Долгие операции (stop, wg.Wait) выполняются БЕЗ удержания мьютекса.
	mu      sync.Mutex
	entries map[string]*connectorEntry

	// stopped — флаг завершения работы менеджера.
	// После StopAll() добавление новых коннекторов запрещено.
	stopped bool

	// stopTimeout — максимальное время ожидания остановки одного коннектора.
	stopTimeout time.Duration

	tickChan    chan<- domain.MarketTick
	fundingSink domain.FundingSink
}

// ConnectorManagerOption — функциональная опция для конфигурации.
type ConnectorManagerOption func(*ConnectorManager)

func WithStopTimeout(d time.Duration) ConnectorManagerOption {
	return func(cm *ConnectorManager) {
		cm.stopTimeout = d
	}
}

func NewConnectorManager(
	tickChan chan<- domain.MarketTick,
	fundingSink domain.FundingSink,
	opts ...ConnectorManagerOption,
) *ConnectorManager {
	cm := &ConnectorManager{
		entries:     make(map[string]*connectorEntry),
		stopTimeout: 15 * time.Second, // разумный дефолт
		tickChan:    tickChan,
		fundingSink: fundingSink,
	}
	for _, opt := range opts {
		opt(cm)
	}
	return cm
}

// ErrManagerStopped возвращается при попытке добавить коннектор
// после вызова StopAll().
var ErrManagerStopped = errors.New("connector manager is stopped")

// AddConnector добавляет или заменяет коннектор с именем name.
// Если коннектор уже существует — корректно останавливает старый (Hot-Swap).
// Потокобезопасен. Не удерживает мьютекс во время ожидания остановки.
func (cm *ConnectorManager) AddConnector(
	parentCtx context.Context,
	name string,
	conn domain.ExchangeConnector,
) error {
	// Фаза 1: извлекаем старый entry под мьютексом, не блокируя надолго.
	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return ErrManagerStopped
	}
	old := cm.entries[name]
	// Удаляем из map сразу — новые вызовы не увидят старый entry.
	delete(cm.entries, name)
	cm.mu.Unlock()

	// Фаза 2: останавливаем старый entry БЕЗ мьютекса.
	// Это предотвращает deadlock и не блокирует другие операции с map.
	if old != nil {
		if err := old.stop(cm.stopTimeout); err != nil {
			// Логируем, но продолжаем — старые горутины завершатся сами
			// по контексту, мы не хотим блокировать Hot-Swap.
			log.Printf("⚠️  [%s] old connector stop warning: %v", name, err)
		}
	}

	// Фаза 3: создаём новый entry БЕЗ мьютекса (дорогая операция).
	newEntry := newConnectorEntry(parentCtx, name, conn, cm.tickChan, cm.fundingSink)

	// Фаза 4: атомарно регистрируем новый entry.
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Проверяем повторно — пока мы останавливали старый,
	// мог прийти StopAll() или другой AddConnector для того же имени.
	if cm.stopped {
		// Менеджер уже остановлен — немедленно останавливаем только что
		// созданный entry. Без ожидания, т.к. StopAll уже завершился.
		go func() {
			if err := newEntry.stop(cm.stopTimeout); err != nil {
				log.Printf("⚠️  [%s] late stop warning: %v", name, err)
			}
		}()
		return ErrManagerStopped
	}

	if existing, conflict := cm.entries[name]; conflict {
		// Параллельный AddConnector успел записать новый entry.
		// Останавливаем наш (проигравший гонку) entry.
		go func() {
			if err := newEntry.stop(cm.stopTimeout); err != nil {
				log.Printf("⚠️  [%s] conflict stop warning: %v", name, err)
			}
		}()
		// Возвращаем ошибку — caller должен решить, что делать.
		_ = existing
		return fmt.Errorf("connector %q was concurrently replaced, retry if needed", name)
	}

	cm.entries[name] = newEntry
	log.Printf("✅ [%s] connector started (Spot/Futures/Funding)", name)
	return nil
}

// RemoveConnector останавливает и удаляет коннектор.
// Блокируется до полной остановки (с таймаутом).
func (cm *ConnectorManager) RemoveConnector(name string) error {
	cm.mu.Lock()
	entry, exists := cm.entries[name]
	if !exists {
		cm.mu.Unlock()
		return fmt.Errorf("connector %q not found", name)
	}
	delete(cm.entries, name)
	cm.mu.Unlock()

	// Ожидание завершения БЕЗ мьютекса.
	if err := entry.stop(cm.stopTimeout); err != nil {
		log.Printf("⚠️  [%s] stop warning: %v", name, err)
		return err
	}

	log.Printf("🛑 [%s] connector stopped cleanly", name)
	return nil
}

// StopAll корректно останавливает все коннекторы параллельно.
// После вызова AddConnector вернёт ErrManagerStopped.
// Идемпотентен.
func (cm *ConnectorManager) StopAll() {
	// Атомарно помечаем как остановленный и забираем все entries.
	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return
	}
	cm.stopped = true
	entries := cm.entries
	cm.entries = make(map[string]*connectorEntry) // новая пустая map
	cm.mu.Unlock()

	// Останавливаем все коннекторы параллельно — без мьютекса.
	var wg sync.WaitGroup
	for name, entry := range entries {
		name, entry := name, entry
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := entry.stop(cm.stopTimeout); err != nil {
				log.Printf("⚠️  [%s] StopAll warning: %v", name, err)
			} else {
				log.Printf("🛑 [%s] stopped", name)
			}
		}()
	}

	wg.Wait()
	log.Println("✅ ConnectorManager: all connectors stopped")
}

// runWithReconnect — чистая функция-supervisor без состояния менеджера.
// Запускается как горутина. Завершается когда ctx отменён.
// Экспоненциальный backoff сбрасывается при успешном соединении
// продолжительностью более resetThreshold.
func runWithReconnect(
	ctx context.Context,
	wg *sync.WaitGroup,
	streamName string,
	connectFn func(context.Context) error,
) {
	defer wg.Done()

	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
		// Если соединение продержалось дольше порога — считаем его
		// "успешным" и сбрасываем backoff. Это предотвращает ситуацию
		// когда нестабильное соединение накапливает максимальный backoff.
		resetThreshold = 10 * time.Second
	)

	backoff := initialBackoff

	for {
		// Проверяем отмену ДО попытки подключения.
		if ctx.Err() != nil {
			return
		}

		start := time.Now()
		err := connectFn(ctx)
		duration := time.Since(start)

		// Проверяем причину завершения connectFn.
		if ctx.Err() != nil {
			// Штатное завершение по отмене контекста.
			// err может быть ненулевым (context.Canceled) — это нормально.
			return
		}

		// connectFn завершилась по иной причине (сетевой сбой, протокольная ошибка).
		// Сбрасываем backoff если соединение было стабильным.
		if duration >= resetThreshold {
			backoff = initialBackoff
		}

		log.Printf("⚠️  Stream [%s] disconnected after %v: %v. Retry in %v...",
			streamName, duration.Round(time.Millisecond), err, backoff)

		// Ожидаем с возможностью прерывания.
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Экспоненциальный рост с ограничением.
		backoff = min(backoff*2, maxBackoff)
	}
}

// min возвращает меньшее из двух Duration.
// Начиная с Go 1.21 можно использовать встроенный min().
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
