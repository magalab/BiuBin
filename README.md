# biubin

`biubin` is a local-first protocol test service. The project is intentionally delivered as one binary; protocol adapters are compiled into the same process.

## Quick start

```sh
cargo run --bin biubin -p biubin
```

Then open <http://127.0.0.1:8080/> or run:

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/http/anything/example
```

The default Compose file starts one `biubin` service container:

```sh
docker compose up --build
```

The image uses a frontend build stage, but the runtime container starts only
`/usr/local/bin/biubin`; Rust and frontend dependencies are resolved from the
committed lockfiles. A local `cargo run` without Node tooling serves the small
embedded fallback page.

## Configuration

Configuration is provided through `BIUBIN_*` environment variables. The
repository includes [`.env.example`](.env.example) as a reference; the binary
does not load `.env` automatically, so export the variables in your shell
before starting it:

```sh
cp .env.example .env
set -a
. ./.env
set +a
cargo run --bin biubin -p biubin
```

Setting a listener port to `0` asks the operating system for a free port;
`info` and the startup log report the selected port rather than the configured
`0`.

The current binary includes HTTP, gRPC (h2c and optional TLS/mTLS), WebSocket,
SSE, TCP/UDP echo, GraphQL, Thrift, and an optional embedded MQTT broker. MQTT
TLS is terminated by an in-process proxy and forwarded to an internal broker
listener; no second runtime service is required.

Useful endpoints and listeners:

- `GET /api/v1/info` and `GET /api/v1/capabilities` describe the running service.
- HTTP, WebSocket, SSE and GraphQL HTTP are on `8080`; GraphQL subscriptions use `ws://127.0.0.1:8080/graphql/ws`.
- gRPC h2c is on `9000`; optional gRPC TLS/mTLS is on `9001`; TCP/UDP echo is on `7000`/`7001`; Thrift binary is on `9090`.
- Socket fault behavior is deterministic and configuration-driven: `BIUBIN_TCP_DELAY_MS`, `BIUBIN_TCP_CLOSE_AFTER` (0 means keep open), `BIUBIN_TCP_READ_LIMIT`, `BIUBIN_UDP_DELAY_MS`, and `BIUBIN_UDP_DROP_PERCENT` (periodic 100-packet schedule).
- `BIUBIN_MAX_CONNECTIONS` bounds shared in-flight HTTP/gRPC work and active TCP/WS/SSE/Thrift connections (default `1000`); rejected work is reported as bounded-resource events. MQTT applies the same numeric limit inside its embedded broker runtime.
- GraphQL introspection is enabled by default and can be disabled with `BIUBIN_GRAPHQL_INTROSPECTION_ENABLED=false`.
- `/sse/events` treats a missing, invalid, or negative `Last-Event-ID` as `0`, replays the fixed sequence `1..5`, and returns an empty stream when the cursor is at least `5`. `/api/v1/events/stream` replays retained events after the numeric cursor, then follows live events; a lagging subscriber receives a `lagged` notice and the retained ring is replayed.
- Set `BIUBIN_MQTT_ENABLED=true` to enable MQTT TCP, authenticated TCP, MQTT 5, and WebSocket listeners. Set `BIUBIN_MQTT_TLS_ENABLED=true` to enable TLS on `8883`; the TLS backend uses the same `BIUBIN_MQTT_USERNAME`/`BIUBIN_MQTT_PASSWORD` credentials. Use `BIUBIN_MQTT_TLS_MODE=mtls` for client certificate verification.
- The embedded MQTT fixture accepts valid topics without an ACL; `/api/v1/capabilities` reports this explicitly. Sessions and offline queues are in-memory for the current process and are not persisted across restart.
- Generate development certificates with `scripts/generate-dev-certs.sh certs/dev`. They are ignored by Git and are for local testing only.

Default listener map:

| Port | Listener |
|---:|---|
| 8080 | HTTP, WS, SSE, GraphQL and control plane |
| 9000 / 9001 | gRPC h2c / TLS or mTLS |
| 7000 / 7001 | TCP / UDP echo |
| 1883 / 1884 / 1885 | MQTT anonymous / authenticated / MQTT 5 |
| 8883 | MQTT TLS or mTLS |
| 8083 | MQTT over WebSocket |
| 9090 | Thrift framed binary |
| 1886 | internal loopback MQTT TLS backend; never expose or map this port |

Protocol smoke clients are kept beside the binary. Start `biubin` first, then
run these in another shell:

```sh
cargo run -p biubin --example graphql_smoke
cargo run -p biubin --example grpc_smoke -- 127.0.0.1 9000 h2c
cargo run -p biubin --example thrift_smoke
cargo run -p biubin --example mqtt_smoke -- 127.0.0.1 1884 biubin biubin-dev tcp
cargo run -p biubin --example mqtt_v5_smoke -- 127.0.0.1 1885 biubin biubin-dev
```

GraphQL HTTP example:

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  --data '{"query":"{ serverInfo { name version } echo(input: { message: \"hello\" }) { message } }"}'
```

Thrift clients should use [`thrift/biubin.thrift`](thrift/biubin.thrift) with
the framed binary protocol. The Rust smoke client is the executable reference
for the wire contract.

The gRPC smoke client accepts `h2c`, `tls`, or `mtls` as its third argument;
TLS modes additionally take the CA path, and mTLS takes the client certificate
and key paths. For example:

```sh
cargo run -p biubin --example grpc_smoke -- 127.0.0.1 9001 mtls \
  certs/dev/ca.pem certs/dev/client.pem certs/dev/client-key.pem
```

`/http/bytes/{n}` returns byte `index % 251` for each zero-based byte index;
`/http/stream-bytes/{n}` uses the same sequence in fixed 16 KiB chunks. Both
endpoints reject values above `BIUBIN_MAX_BYTES_RESPONSE`.

The development HTTP control plane is same-origin only: biubin does not add
open CORS headers, does not validate WebSocket `Origin`, and does not trust
`X-Forwarded-For`; deployments behind a proxy must enforce those policies at
the proxy. Binding `0.0.0.0` exposes a development fixture, default MQTT
credentials, and any enabled control endpoints to the network.

The service is intentionally a development/test fixture. Do not expose the default credentials or generated certificates to an untrusted network.

The local no-public-network black-box check is:

```sh
bash scripts/blackbox.sh
```

## CI and releases

GitHub Actions runs the frontend, Rust, and black-box checks for pushes and
pull requests. A push to `main` or a version tag also builds and publishes a
multi-architecture image to `ghcr.io/magalab/biubin`.

Pushing a version tag such as `v0.1.0` creates a GitHub release containing
Linux `amd64` and `arm64` binary archives with SHA-256 checksums.

## Source layout

- `crates/core/src/` contains shared configuration, bounded event storage, and readiness state.
- `crates/app/src/lifecycle.rs` owns process startup, listener binding, task lifecycle, and shutdown.
- `crates/app/src/http/` contains the router, middleware, control-plane endpoints, HTTP fixtures, WebSocket, and SSE handlers.
- `crates/app/src/{grpc,tcp,udp,mqtt,thrift}.rs` contains the protocol runtimes.
- `crates/app/examples/` contains protocol smoke clients; `crates/app/tests/` contains cross-module Rust integration tests.
- `proto/` and `thrift/` are the wire-contract sources; `web/` is the Svelte control plane embedded into the binary at build time.
