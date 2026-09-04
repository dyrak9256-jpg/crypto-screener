package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"crypto-screener/internal/domain"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

const (
	// Публичный WebSocket поток всех Best Bid/Ask тикеров Binance Spot
	binanceSpotWS = "wss://stream.binance.com:9443/ws/!bookTicker"
	// Публичный WebSocket поток всех Best Bid/Ask тикеров Binance Futures
	binanceFuturesWS = "wss://fstream.binance.com/ws/!bookTicker"

	// Таймауты и задержки
	dialTimeout    = 10 * time.Second
	reconnectDelay = 3 * time.Second
)

// binanceBookTickerWS — структура JSON ответа от Binance !bookTicker
type binanceBookTickerWS struct {
	Symbol  string `json:"s"`
	BestBid string `json:"b"`
	BestAsk string `json:"a"`
}

// Adapter — структура адаптера биржи Binance
type Adapter struct{}

// NewAdapter создает новый экземпляр адаптера Binance
func NewAdapter() *Adapter {
	return &Adapter{}
}

// ConnectSpot подключается к WebSocket спотового рынка Binance и транслирует тикеры в outChan
func (a *Adapter) ConnectSpot(ctx context.Context, outChan chan<- domain.MarketTick) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	go a.listenToStream(ctx, binanceSpotWS, domain.MarketTypeSpot, outChan)
	return nil
}

// ConnectFutures подключается к WebSocket фьючерсного рынка Binance и транслирует тикеры в outChan
func (a *Adapter) ConnectFutures(ctx context.Context, outChan chan<- domain.MarketTick) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	go a.listenToStream(ctx, binanceFuturesWS, domain.MarketTypeFutures, outChan)
	return nil
}

// listenToStream отвечает за жизненный цикл соединения: подключение, чтение и авто-переподключение
func (a *Adapter) listenToStream(ctx context.Context, wsURL string, marketType domain.MarketType, outChan chan<- domain.MarketTick) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 Остановка потока %s по контексту", marketType)
			return
		default:
		}

		err := a.connectAndRead(ctx, wsURL, marketType, outChan)

		// Если ошибка возникла из-за отмены контекста, просто выходим
		if ctx.Err() != nil {
			return
		}

		if err != nil {
			log.Printf("⚠️ Ошибка в потоке %s: %v. Переподключение через %v...", marketType, err, reconnectDelay)
			// Ждем перед переподключением, но прерываем ожидание, если контекст отменен
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
		}
	}
}

// connectAndRead устанавливает соединение и читает сообщения до момента ошибки или отмены контекста
func (a *Adapter) connectAndRead(ctx context.Context, wsURL string, marketType domain.MarketType, outChan chan<- domain.MarketTick) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: dialTimeout,
	}

	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("ошибка подключения к %s WS: %w", marketType, err)
	}
	defer conn.Close()

	log.Printf("✅ Успешное подключение к WebSocket Binance %s", marketType)

	// Горутина для принудительного закрытия соединения при отмене контекста.
	// Это необходимо, чтобы разблокировать зависший conn.ReadMessage()
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil // Чистый выход по контексту
			}
			return fmt.Errorf("ошибка чтения WS %s: %w", marketType, err)
		}

		// Парсим JSON байты в структуру
		var wsTick binanceBookTickerWS
		if err := json.Unmarshal(message, &wsTick); err != nil {
			continue // Игнорируем битые/нестандартные сообщения
		}

		// Парсим строковые цены в точные Decimal
		bid, errBid := decimal.NewFromString(wsTick.BestBid)
		ask, errAsk := decimal.NewFromString(wsTick.BestAsk)
		if errBid != nil || errAsk != nil {
			continue
		}

		// Преобразуем в единую доменную модель
		tick := domain.MarketTick{
			Exchange:   "BINANCE",
			Symbol:     wsTick.Symbol,
			MarketType: marketType,
			BestBid:    bid,
			BestAsk:    ask,
			Timestamp:  time.Now(),
		}

		// Неблокирующая отправка в канал.
		// Если канал переполнен, мы пропускаем тик, чтобы не блокировать чтение из WebSocket.
		select {
		case outChan <- tick:
		case <-ctx.Done():
			return nil
		default:
			// Опционально: можно добавить лог, если канал часто переполняется
			// log.Printf("⚠️ Канал %s переполнен, пропуск тика для %s", marketType, wsTick.Symbol)
		}
	}
}
