This document provides a complete map of the codebase. Use this to understand where every component lives, its exact responsibility, and how they interact.

## 📂 Directory Tree
crypto-screener/
├── cmd/screener/main.go
├── internal/
│   ├── adapters/
│   │   ├── binance/binance.go
│   │   ├── postgres/repo.go
│   │   └── telegram/bot.go
│   ├── app/
│   │   ├── app.go
│   │   ├── connector_manager.go
│   │   ├── funding_manager.go
│   │   ├── notification_router.go
│   │   ├── persistence_worker.go
│   │   ├── sharded_aggregator.go
│   │   └── tracker.go
│   ├── config/config.go
│   └── domain/
│       ├── config.go
│       ├── market.go
│       ├── ports.go
│       ├── signal.go
│       └── user.go
├── docker-compose.yml
├── schema.sql
├── go.mod
└── .env (gitignored)