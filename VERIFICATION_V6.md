# V6 Verification

## Static checks completed in the build sandbox

- `gofmt`: clean for all Go files.
- Go parser with `parser.AllErrors`: 50/50 files parsed successfully.
- Production `panic(` search: zero.
- Production `http.DefaultClient` search: zero in adapters.
- Production ignored `UpdateFunding` / `SetReadDeadline` / WebSocket write errors: zero.
- Direct unwrapped `return err`, `return readErr`, `return parseErr`: zero.

## Tests included

19 test files are present, including newly added tests for:

- MEXC protobuf decoding;
- WebSocket heartbeat behavior;
- retry backoff;
- notification target calculation without Telegram.

## Environment limitation

The sandbox has Go 1.23.2 while `go.mod` requires Go 1.26.4. Network access for downloading the Go 1.26.4 toolchain and project dependencies is unavailable.

Therefore these commands were not falsely reported as executed:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

They remain the user's final execution gate on the development machine.
