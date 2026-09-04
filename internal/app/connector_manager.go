package app

import (
	"context"
	"crypto-screener/internal/domain"
	"log"
	"sync"
)

type ConnectorManager struct {
	mu          sync.Mutex
	connectors  map[string]context.CancelFunc
	tickChan    chan<- domain.MarketTick
	fundingSink domain.FundingSink
}

func NewConnectorManager(tickChan chan<- domain.MarketTick, fundingSink domain.FundingSink) *ConnectorManager {
	return &ConnectorManager{connectors: make(map[string]context.CancelFunc), tickChan: tickChan, fundingSink: fundingSink}
}

func (cm *ConnectorManager) AddConnector(name string, conn domain.ExchangeConnector, parentCtx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, exists := cm.connectors[name]; exists {
		return nil
	}

	ctx, cancel := context.WithCancel(parentCtx)
	cm.connectors[name] = cancel

	go func() {
		if err := conn.ConnectSpot(ctx, cm.tickChan); err != nil {
			log.Printf("❌ %s Spot failed: %v", name, err)
		}
	}()
	go func() {
		if err := conn.ConnectFutures(ctx, cm.tickChan); err != nil {
			log.Printf("❌ %s Futures failed: %v", name, err)
		}
	}()
	go func() {
		if err := conn.ConnectFunding(ctx, cm.fundingSink); err != nil {
			log.Printf("❌ %s Funding failed: %v", name, err)
		}
	}()

	log.Printf("✅ Hot-swapped IN: %s", name)
	return nil
}

func (cm *ConnectorManager) RemoveConnector(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cancel, exists := cm.connectors[name]; exists {
		cancel()
		delete(cm.connectors, name)
		log.Printf("🛑 Hot-swapped OUT: %s", name)
	}
}
