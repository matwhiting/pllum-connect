# Pllum Connect

Pllum Connect is Pllum's network helper. Pllum, a macOS inbox, starts it as a child process and talks to it in JSON lines over its standard input and output. The helper keeps an encrypted message store on the Mac and connects to the messaging networks Pllum reads and sends through. It writes nothing anywhere else: no logs, and nothing on standard error.

The protocol is in [PROTOCOL.md](PROTOCOL.md).

## Build

Needs Go 1.27.

```sh
CGO_ENABLED=0 go build ./cmd/pllum-connect
```

## Run

```sh
pllum-connect --data <folder>
```

The data folder is made with mode 0700. Only one copy runs per folder: a second exits with code 3. The helper exits 0 at the end of its input.

## Test

```sh
go test ./...
```

## Licence

GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE).
