# Verification

Formatting and Go syntax parsing were completed for every `.go` file.

Full dependency-backed compilation could not be executed in the build sandbox because:
- local Go is 1.23.2;
- the project requires Go 1.26.4;
- automatic Go 1.26.4 toolchain download is blocked by the sandbox network;
- project dependencies are not present in the local module cache.

Required CI gate on a machine with Go 1.26.4 and network/module cache:

```text
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

The V4 archive should not be called release-ready until those four commands are green.
