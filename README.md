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

`--gate` adds one hidden method, `gateCounts` (counts only), for Messenger's gate: the one sitting in which a Debug Pllum checks whether its encrypted chats can be read before Messenger is built out. With it, every Messenger sign-in lists every folder afresh (with no 365-day reach, so the counts include the oldest threads) and fetches no history. Pllum's own copy never passes it.

## Test

```sh
go test ./...
```

## Licence

GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE).

The helper includes code from [mautrix-meta](https://github.com/mautrix/meta) (AGPL-3.0), used as a library: the packages `pkg/instameow`, `pkg/instameow/slidetypes` and `pkg/messagix` and its subpackages, for Instagram and Messenger. Those packages pull in parts of the mautrix bridge framework at build time, but the helper never uses them. It never imports `pkg/connector` or `pkg/igconnector`.
