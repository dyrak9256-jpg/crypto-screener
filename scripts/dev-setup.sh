#!/usr/bin/env bash
# ============================================================================
# scripts/dev-setup.sh — быстрое разворачивание dev-окружения crypto-screener
#
# Идемпотентен: повторный запуск ничего не ломает, установленное пропускается.
#
# Что делает:
#   1. Go 1.26.4 — скачивает и ставит в /usr/local/go, если версия ниже нужной
#   2. Зависимости проекта — go mod download
#   3. PostgreSQL — установка (apt), запуск кластера, создание роли/базы
#      screener_db и применение schema.sql (если ещё не создано)
#   4. .env — из .env.example (если ещё нет; не забудь TELEGRAM_TOKEN!)
#   5. Проверочный гейт — go build / go vet / go test + сборка ./screener
#
# Использование:
#   ./scripts/dev-setup.sh            # всё
#   ./scripts/dev-setup.sh --race     # тесты с детектором гонок (-race)
#   ./scripts/dev-setup.sh --no-db    # пропустить PostgreSQL
#   ./scripts/dev-setup.sh --no-go    # не трогать установку Go
#
# Поддержка ОС: Debian/Ubuntu (apt) — полностью; на других дистрибутивах
# ставится Go и зависимости, а для PostgreSQL выводится инструкция.
#
# Крединалы БД можно переопределить через переменные окружения:
#   DB_USER / DB_NAME (по умолчанию screener_user/screener_db).
# Пароль БД, если не задан явно, генерируется случайно и вписывается в .env
# (никаких change_me по умолчанию).
# ============================================================================
set -euo pipefail

GO_VER="1.26.4"          # версия из go.mod
GO_VERSION_MIN="1.26"    # ниже этой версии — переустанавливаем

cd "$(cd "$(dirname "$0")/.." && pwd)"

RACE=0
NO_DB=0; NO_GO=0
for arg in "$@"; do
  case "$arg" in
    --race)  RACE=1 ;;
    --no-db) NO_DB=1 ;;
    --no-go) NO_GO=1 ;;
    *) echo "Неизвестный флаг: $arg (доступны --race, --no-db, --no-go)"; exit 1 ;;
  esac
done

DB_USER="${DB_USER:-screener_user}"
GENERATED_DB_PASSWORD=0
if [ -z "${DB_PASSWORD:-}" ]; then
  # Случайный пароль: base64 без спецсимволов, безопасен для sed/psql/URL.
  DB_PASSWORD="$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)"
  GENERATED_DB_PASSWORD=1
fi
DB_NAME="${DB_NAME:-screener_db}"

# ---------- утилиты ----------
log()  { printf '\n==> %s\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  if have sudo; then SUDO="sudo"; else
    echo "❌ Нужны права root (запусти от root или установи sudo)"; exit 1
  fi
fi

# ---------- 1. Go ----------
if [ "$NO_GO" -eq 0 ]; then
  need_go=1
  if have go; then
    installed="$(go version 2>/dev/null | awk '{print $2}' | sed 's/^go//')"
    if [ "$(printf '%s\n' "$GO_VERSION_MIN" "$installed" | sort -V | head -1)" = "$GO_VERSION_MIN" ]; then
      need_go=0
      log "Go $installed уже установлен — пропускаю"
    fi
  fi
  if [ "$need_go" -eq 1 ]; then
    case "$(uname -m)" in
      x86_64)            GOARCH=amd64 ;;
      aarch64|arm64)     GOARCH=arm64 ;;
      *) echo "❌ Неподдерживаемая архитектура: $(uname -m)"; exit 1 ;;
    esac
    log "Установка Go $GO_VER (linux/$GOARCH)"
    curl -fsSL -o /tmp/go.tgz "https://go.dev/dl/go${GO_VER}.linux-${GOARCH}.tar.gz"
    $SUDO rm -rf /usr/local/go
    $SUDO tar -C /usr/local -xzf /tmp/go.tgz
    $SUDO ln -sf /usr/local/go/bin/go /usr/local/bin/go
    $SUDO ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt
  fi
fi
export PATH="/usr/local/go/bin:$PATH"
have go || { echo "❌ Go не найден после установки"; exit 1; }
log "Использую $(go version)"

# Временные файлы сборки — на диске (не в маленьком /tmp на некоторых хостах)
export GOTMPDIR="${GOTMPDIR:-$HOME/.cache/gotmp}"
mkdir -p "$GOTMPDIR"

# ---------- 2. Зависимости ----------
log "go mod download"
go mod download

# ---------- 3. PostgreSQL ----------
if [ "$NO_DB" -eq 0 ]; then
  if ! have psql; then
    if have apt-get; then
      log "Установка PostgreSQL (apt)"
      $SUDO apt-get update -qq
      $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql
    else
      echo "⚠️  PostgreSQL не установлен, а менеджер пакетов не apt."
      echo "    Установи его вручную и перезапусти скрипт, либо запусти с --no-db."
      exit 1
    fi
  fi
  if ! pg_isready -q 2>/dev/null; then
    log "Запуск кластера PostgreSQL"
    if have pg_ctlcluster; then
      PGVER="$(ls /etc/postgresql 2>/dev/null | sort -V | tail -1)"
      $SUDO pg_ctlcluster "$PGVER" main start
    elif have systemctl; then
      $SUDO systemctl start postgresql
    else
      echo "⚠️  Не удалось определить способ запуска PostgreSQL — запусти его вручную."
      exit 1
    fi
  fi
  sleep 1
  pg_isready -q || { echo "❌ PostgreSQL не отвечает"; exit 1; }

  if ! $SUDO -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1; then
    log "Создание роли ${DB_USER}"
    $SUDO -u postgres psql -c "CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}';"
  fi
  if ! $SUDO -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1; then
    log "Создание базы ${DB_NAME} (владелец ${DB_USER}) + схема"
    $SUDO -u postgres createdb -O "$DB_USER" "$DB_NAME"
    PGPASSWORD="$DB_PASSWORD" psql -h 127.0.0.1 -q -U "$DB_USER" -d "$DB_NAME" -f schema.sql
  fi
fi

# ---------- 4. .env ----------
if [ ! -f .env ]; then
  log "Создание .env из .env.example (заполни TELEGRAM_TOKEN и ADMIN_CHAT_IDS!)"
  cp .env.example .env
  if [ "$GENERATED_DB_PASSWORD" -eq 1 ]; then
    sed -i "s|^DB_PASSWORD=.*|DB_PASSWORD=${DB_PASSWORD}|" .env
    sed -i "s|^DATABASE_URL=.*|DATABASE_URL=postgres://${DB_USER}:${DB_PASSWORD}@localhost:5432/${DB_NAME}?sslmode=disable|" .env
    log "Сгенерирован случайный пароль БД и вписан в .env"
  fi
fi

# ---------- 5. Проверочный гейт ----------
log "go build / go vet"
CGO_ENABLED=0 go build ./...
go vet ./...

if [ "$RACE" -eq 1 ]; then
  log "go test -race"
  go test -race -timeout 600s ./...
else
  log "go test"
  go test -timeout 300s ./...
fi

log "Сборка бинарника ./screener"
CGO_ENABLED=0 go build -trimpath -ldflags="-w -s" -o screener ./cmd/screener

cat << 'EOF'

✅ Окружение готово.

Запуск приложения:
  1) заполни TELEGRAM_TOKEN (у @BotFather) и ADMIN_CHAT_IDS (твой Telegram ID) в .env
  2) ./screener

Либо продакшн-путь через Docker:
  docker compose up --build
EOF
