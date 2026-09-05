# --- Этап 1: Сборка бинарного файла ---
FROM golang:1.22-alpine AS builder

# Установка git и ca-certificates (необходимы для скачивания модулей и работы TLS)
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Сначала копируем файлы зависимостей для эффективного кэширования слоев
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Копируем остальной исходный код
COPY . .

# Собираем приложение с оптимизациями:
# CGO_ENABLED=0 для получения статически скомпонованного бинарного файла
# -ldflags="-s -w" для удаления отладочной информации и уменьшения размера
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /screener cmd/screener/main.go

# --- Этап 2: Создание минимального образа для запуска ---
FROM scratch

# Импортируем CA-сертификаты для HTTPS/WebSocket соединений
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Копируем бинарный файл из сборщика (builder)
COPY --from=builder /screener /screener

# Открываем порт метрик на случай добавления Prometheus в будущем (опционально)
# EXPOSE 8080

# Запуск бинарного файла
ENTRYPOINT ["/screener"]