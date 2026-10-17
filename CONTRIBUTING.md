# Contributing

Issues and pull requests are welcome.

```sh
go build ./...
go test ./...
BRIAN_TEST_DSN=postgres://user:pass@localhost:5432/isolated_test_db go test -race ./...
```

- `cmd/router`, `cmd/plugin-server`, `cmd/room-admin` are the binaries; packages live in `internal/`.
- Plugin domain transitions are in `internal/plugin`; add each new plugin to `internal/catalog/plugins.go`.
- Schemas in `internal/catalog/assets` are copies of the [BRIAN](https://github.com/thiagoribeiro/brian) schemas. Change them in BRIAN first and run its `check-contracts.py --check-consumers`.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/).
- By contributing you agree that your contributions are licensed under the Apache License 2.0.

Security issues: see [SECURITY.md](SECURITY.md).
