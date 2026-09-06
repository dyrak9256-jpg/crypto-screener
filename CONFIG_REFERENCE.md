# Settings reference

## Global administrator settings

Source of truth at startup:

- `internal/config/config.go` — environment parsing and validation.
- `.env` / `.env.example` — actual deployment values.
- `HARD_MIN_SPREAD=0.01` means 1%.

The hard spread floor can be changed at runtime by an administrator with `/sethardspread <percent>`. The runtime value is not persisted to PostgreSQL yet, so after restart the environment value is loaded again.

## User settings

Runtime model: `internal/domain/user.go`.

Persisted table: PostgreSQL `users`.

Commands:

- `/setcross <percent>` — personal minimum spread; never below global hard floor.
- `/setvol <USDT>` — personal minimum quote turnover.
- `/settimeframe <1m|5m|15m|30m|1h|4h|24h>` — personal volume window.
- `/setfundingtime <minutes>` — personal minimum time to the next funding payment.

Interval volume is computed from 1-minute candle quote turnover, not from 24h ticker volume.
