# V5 Verification

## Completed in the refactor environment

- `gofmt` over all Go sources.
- `go/parser` with `parser.AllErrors` over all Go files: 46 files parsed successfully.
- No `panic(` in project Go source.
- No remaining raw `return err` / `return nil, err` in internal production Go source.
- No `http.DefaultClient` in exchange adapters.
- No V4 `userCrossSpread` field reference.
- No `RequireFunding` fail-open configuration branch.
- Only one canonical `CandleConnector` / `CandleSink` definition remains in `internal/domain`.

## Not runnable here

The project declares Go 1.26.4. The sandbox has an older Go toolchain and network access to the Go proxy is unavailable, so dependency/toolchain resolution cannot complete.

These commands remain release-gate requirements:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./...
```
