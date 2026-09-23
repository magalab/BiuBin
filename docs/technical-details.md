# Technical details

This document contains the implementation and verification details that are
intentionally kept out of the short [usage guide](../README.md).

Commands are written for the repository root. The source toolchain is pinned in
[`rust-toolchain.toml`](../rust-toolchain.toml); CI and the Docker build use
Node 22 for the frontend. Docker Compose and the external-client checks are
optional development tools.

## Architecture

`biubin` is one Rust process with protocol adapters compiled into the same
binary. The Svelte control plane and Scalar OpenAPI page are built separately,
then their output is embedded by `crates/app/build.rs`. If `web/dist` is not
present, the binary serves the small `web/fallback.html` page instead.

The runtime has three broad layers:

1. shared configuration, readiness, bounded event retention, and resource
   limits in `crates/core/`;
2. protocol listeners and lifecycle supervision in `crates/app/src/`;
3. HTTP fixtures, the control plane, and embedded web assets.

The process is deliberately stateless. Events, MQTT sessions, and offline
queues live in memory and are lost on restart.

## Default listeners

| Port | Listener | Notes |
|---:|---|---|
| 8080 | HTTP, WebSocket, SSE, GraphQL, control plane | Same-origin development UI |
| 9000 | gRPC h2c | Reflection enabled |
| 9001 | gRPC TLS/mTLS | Disabled unless configured |
| 7000 | TCP echo | Line and length-prefixed modes |
| 7001 | UDP echo | Deterministic delay/drop controls |
| 1883 | MQTT anonymous TCP | MQTT is disabled by default |
| 1884 | MQTT authenticated TCP | Uses configured credentials |
| 1885 | MQTT 5 | Uses configured credentials |
| 8883 | MQTT TLS/mTLS | Public TLS proxy; requires MQTT and TLS configuration |
| 8083 | MQTT over WebSocket | Anonymous WebSocket transport; requires MQTT configuration |
| 9090 | Thrift framed binary | Contract in `thrift/biubin.thrift` |
| 1886 | Internal MQTT TLS backend | Loopback-only; never expose or map it |

All MQTT listeners are disabled by default. Set `BIUBIN_MQTT_ENABLED=true` to
enable the TCP, authenticated TCP, MQTT 5, and WebSocket listeners; TLS on
`8883` additionally requires `BIUBIN_MQTT_TLS_ENABLED=true`.

Setting a listener port to `0` asks the operating system for an ephemeral port.
The selected port is reported by `/api/v1/info` and startup logs.

## HTTP fixtures

The root namespace follows the HTTPBin-style shape used by client test suites:

- request echo: `/get`, `/post`, `/put`, `/patch`, `/delete`, and
  `/anything[/...]`;
- status, headers, client address, user agent, delay, redirects, compression,
  authentication, cookies, caching, ETag, and response-header fixtures;
- deterministic JSON, HTML, XML, UTF-8, streaming, byte-count, and failure
  fixtures;
- image, audio, and video fixtures with `HEAD`, `Accept-Ranges`, and single
  byte-range support;
- `/openapi` and `/openapi.json` for interactive and machine-readable API docs.

The less obvious fixture behavior is intentional and deterministic:

- method-specific fixtures accept the documented method plus `HEAD` where
  applicable; unsupported methods return `405`. `/status/{code}` accepts any
  method so callers can test status handling independently;
- `/redirect-to` accepts relative targets by default. Absolute targets require
  `BIUBIN_HTTP_ALLOW_EXTERNAL_REDIRECTS=true` and an exact hostname in
  `BIUBIN_HTTP_EXTERNAL_REDIRECT_HOSTS`; local literal addresses remain
  rejected;
- media, `/bytes/{count}`, and `/range/{count}` advertise byte ranges and
  support a single `Range` request. Media is embedded from
  `crates/app/assets`; regenerate deterministic files with
  `scripts/generate-media-fixtures.sh` when `ffmpeg` and `cwebp` are available;
- `/bytes/{count}` and `/stream-bytes/{count}` use the deterministic byte value
  `index % 251` and are bounded by `BIUBIN_MAX_BYTES_RESPONSE`. `/drip` emits
  immediately for a zero duration and otherwise follows its requested chunk
  size subject to service limits.

The control plane exposes:

- `/healthz` for liveness;
- `/readyz` for listener readiness;
- `/api/v1/info` for runtime identity and selected ports;
- `/api/v1/capabilities` for the endpoint/protocol capability document;
- `/api/v1/events` and `/api/v1/events/stream` for retained and live events.

The capabilities document is schema version `1`. Endpoint entries expose a
`methods` array; `*` denotes any method or protocol operation. HTTP fixture
metadata is shared with `/openapi.json`, which keeps the two descriptions in
sync.

The protocol-oriented HTTP upgrade and streaming paths are:

| Purpose | Paths |
|---|---|
| WebSocket | `/ws/echo`, `/ws/room/{name}`, `/ws/ticker`, `/ws/close` |
| SSE | `/sse/events`, `/sse/ticker` |
| GraphQL | `/graphql`, `/graphql/ws` |

## Streaming and protocol behavior

### WebSocket and SSE

`/ws/echo` is a simple frame echo endpoint. `/ws/room/{name}` provides a named
room, `/ws/ticker` emits a finite sequence, and `/ws/close` exercises a server
initiated close. `/sse/events` replays the fixed event sequence `1..5` using
`Last-Event-ID`; `/sse/ticker` emits a finite configurable sequence. The
control-plane event stream replays retained events after a numeric cursor and
reports a `lagged` notice when a subscriber falls behind the bounded ring.

### GraphQL

GraphQL HTTP is served at `/graphql`; subscriptions use `/graphql/ws`.
Introspection is enabled by default and can be disabled with
`BIUBIN_GRAPHQL_INTROSPECTION_ENABLED=false`. The fixture exposes stable error
categories for syntax, validation, resolver, and authentication examples.

### gRPC

The protobuf source is `proto/biubin.proto`. The smoke client covers unary,
server-streaming, client-streaming, bidirectional streaming, metadata, status,
trailers, reflection, h2c, TLS, and mTLS.

### TCP and UDP

TCP supports line-oriented and four-byte big-endian length-prefixed echo
traffic. UDP supports deterministic delay and periodic packet dropping through
`BIUBIN_UDP_DELAY_MS` and `BIUBIN_UDP_DROP_PERCENT`.

### MQTT

MQTT can expose anonymous TCP, authenticated TCP, MQTT 5, TLS/mTLS, and
WebSocket listeners. The in-process broker accepts valid topics without a
per-packet ACL. QoS, retain, will, authentication, TLS modes, browser
WebSocket traffic, and in-process sessions are smoke-tested.

The public TLS listener on `8883` is an in-process proxy to the loopback-only
`BIUBIN_MQTT_TLS_BACKEND_PORT` listener (default `1886`). Sessions and offline
queues are in memory and are not restored after restart.

### Thrift

The service uses the framed binary protocol and the IDL in
`thrift/biubin.thrift`. The Rust smoke client and the generated Node.js client
cover `echo`, `sum`, and the error contract.

## Smoke clients

Start `biubin` first, then run the relevant example from another shell:

```sh
cargo run -p biubin --example graphql_smoke
cargo run -p biubin --example grpc_smoke -- 127.0.0.1 9000 h2c
cargo run -p biubin --example thrift_smoke
cargo run -p biubin --example mqtt_smoke -- 127.0.0.1 1884 biubin biubin-dev tcp
cargo run -p biubin --example mqtt_v5_smoke -- 127.0.0.1 1885 biubin biubin-dev
```

Generate local certificates before TLS or mTLS checks:

```sh
bash scripts/generate-dev-certs.sh certs/dev
cargo run -p biubin --example grpc_smoke -- 127.0.0.1 9001 mtls \
  certs/dev/ca.pem certs/dev/client.pem certs/dev/client-key.pem
cargo run -p biubin --example mqtt_smoke -- 127.0.0.1 8883 \
  biubin biubin-dev tls certs/dev/ca.pem
```

The GraphQL example defaults to `ws://127.0.0.1:8080/graphql/ws`; the MQTT
example accepts `tcp`, `tls`, or `ws` as its transport argument. These clients
are executable wire-contract references, not general-purpose protocol tools.

## Configuration

All configuration is supplied through environment variables. The defaults are
defined in `crates/core/src/config.rs`; [`.env.example`](../.env.example) is a
copyable reference. The binary does not load a `.env` file automatically.

Important groups include:

| Group | Variables |
|---|---|
| Binding | `BIUBIN_BIND_HOST`, `BIUBIN_ADVERTISE_HOST` |
| Ports | `BIUBIN_HTTP_PORT`, `BIUBIN_GRPC_H2C_PORT`, `BIUBIN_GRPC_TLS_PORT`, `BIUBIN_TCP_PORT`, `BIUBIN_UDP_PORT`, `BIUBIN_MQTT_*_PORT`, `BIUBIN_THRIFT_PORT` |
| TLS | `BIUBIN_GRPC_TLS_ENABLED`, `BIUBIN_GRPC_TLS_MODE`, `BIUBIN_TLS_CERT_PATH`, `BIUBIN_TLS_KEY_PATH`, `BIUBIN_TLS_CLIENT_CA_PATH` |
| MQTT | `BIUBIN_MQTT_ENABLED`, `BIUBIN_MQTT_TLS_ENABLED`, `BIUBIN_MQTT_TLS_MODE`, `BIUBIN_MQTT_USERNAME`, `BIUBIN_MQTT_PASSWORD`, `BIUBIN_MQTT_TLS_BACKEND_PORT` |
| HTTP safety | `BIUBIN_HTTP_BODY_LIMIT`, `BIUBIN_MAX_BYTES_RESPONSE`, `BIUBIN_HTTP_ALLOW_EXTERNAL_REDIRECTS`, `BIUBIN_HTTP_EXTERNAL_REDIRECT_HOSTS` |
| Resource behavior | `BIUBIN_MAX_CONNECTIONS`, `BIUBIN_EVENT_CAPACITY`, `BIUBIN_TCP_READ_LIMIT` |
| Fault simulation | `BIUBIN_TCP_DELAY_MS`, `BIUBIN_TCP_CLOSE_AFTER`, `BIUBIN_UDP_DELAY_MS`, `BIUBIN_UDP_DROP_PERCENT` |
| GraphQL | `BIUBIN_GRAPHQL_INTROSPECTION_ENABLED` |

The service defaults to loopback binding. Binding `0.0.0.0` exposes a
development fixture, default MQTT credentials, and any enabled control
endpoints to the network. The service does not add open CORS headers, validate
WebSocket `Origin`, or trust `X-Forwarded-For`; a reverse proxy must enforce
those policies when needed.

## Container image

The Docker build has a Node frontend stage, a Rust builder stage, and a
minimal `gcr.io/distroless/cc-debian12:nonroot` runtime stage. The runtime
binary is built for glibc, so distroless Debian is the low-risk small image
choice for the current toolchain. Alpine would require a separate musl target
build and should not be used by merely swapping the final base image.

The container runs as the non-root `nonroot` user and uses the binary's
`healthcheck` subcommand for Docker health checks. Runtime dependencies are not
installed in the final image.

### Alpine trade-offs

Alpine is not a drop-in replacement for the current runtime image. The current
builder produces a glibc-linked binary; replacing the final stage with Alpine
would leave the ELF loader and glibc runtime unavailable, so the container
would fail before `biubin` starts.

An Alpine variant would need a separate musl build, normally including a musl
Rust target and native build tools for dependencies such as `ring`. Both
`linux/amd64` and `linux/arm64` builds would need independent validation. The
variant should also explicitly provide CA certificates and, if needed by test
scenarios, timezone data. musl's DNS and libc behavior can differ from glibc,
which is relevant when fixtures test name resolution or timeout behavior.

The trade-off is a smaller base image and a more involved build/compatibility
matrix. The distroless Debian runtime is therefore the default: it is already
small, runs as non-root, has no shell or package manager, and reuses the
existing glibc build without changing protocol behavior. An Alpine image is a
reasonable optional target only after a separate musl build and the full
protocol smoke suite pass on both supported architectures.

## Verification

Local checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
bash scripts/blackbox.sh
node scripts/check-http-route-contract.mjs

cd web
npm ci
npm run check
npm run build
npx playwright install chromium
npm run test:e2e
```

Delivery checks:

```sh
bash scripts/compose-smoke.sh
BIUBIN_EXTERNAL_SMOKE=1 bash scripts/blackbox.sh
```

The Playwright command starts a local fixture automatically through
`web/playwright.config.ts`, after `web/dist` has been built. The external smoke
requires `grpcurl`, Mosquitto clients, and the Apache Thrift compiler; CI
installs these tools for that job.

The CI workflow runs the frontend, Rust, black-box, Compose, browser, and
external-client jobs. It does not claim that the fixture is a full
implementation of every supported protocol.

## Compatibility boundaries

The tested client/tool scope is recorded in the
[protocol compatibility matrix](compatibility-matrix.md). It is not a claim of
full protocol conformance.

Explicit non-goals include performance numbers, maximum sustainable throughput,
long-running stability under load, MQTT persistence across restart, MQTT
per-packet ACLs, and strict broker drain/join semantics.
