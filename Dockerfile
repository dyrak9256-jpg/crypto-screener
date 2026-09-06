# Этап 1: Сборка приложения
FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

ENV GOTOOLCHAIN=auto

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o screener ./cmd/screener

# Этап 2: Финальный минимальный образ
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/screener /screener

ENTRYPOINT ["/screener"]