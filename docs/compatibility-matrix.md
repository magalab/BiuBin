# Protocol compatibility matrix

This matrix records the clients and paths used by the repository's automated
smoke tests. A `Smoke-tested` entry means that the listed client can complete
the covered calls against the current `biubin` wire contract; it is not a
claim of exhaustive protocol conformance.

| Surface | Client or tool | Status | Automated path | Covered scope |
|---|---|---|---|---|
| HTTP fixtures and control plane | `curl` | Smoke-tested | `scripts/blackbox.sh` | status, echo, media, Range, cache, SSE, GraphQL HTTP, OpenAPI JSON |
| HTTP / WebSocket / SSE / OpenAPI UI | Chromium via Playwright | Smoke-tested | `web/tests/browser-smoke.spec.ts` | same-origin page, HTTP call, WS echo, SSE ticker, Scalar grouping |
| gRPC h2c, TLS and mTLS | Rust `tonic` smoke client | Smoke-tested | `crates/app/examples/grpc_smoke.rs` | unary, server/client/bidi streaming, metadata, status/trailer and TLS modes |
| gRPC reflection | `grpcurl` 1.9.3 | Smoke-tested in CI | `BIUBIN_EXTERNAL_SMOKE=1 bash scripts/blackbox.sh` | service discovery over plaintext gRPC |
| WebSocket | Rust `tokio-tungstenite` client | Smoke-tested | `crates/app/src/http` tests and browser smoke | echo and protocol upgrade behavior |
| SSE | `curl` and browser `EventSource` | Smoke-tested | `scripts/blackbox.sh` and browser smoke | fixed events, ticker and cursor replay |
| GraphQL HTTP / subscription | `curl` and Rust WebSocket client | Smoke-tested | `scripts/blackbox.sh` and `crates/app/examples/graphql_smoke.rs` | query, mutation, subscription and stable errors |
| TCP / UDP | Rust integration client | Smoke-tested | `crates/app/tests/blackbox.rs` | line and length-prefixed TCP echo, UDP echo, ephemeral ports |
| Thrift framed binary | Rust reference client | Smoke-tested | `crates/app/examples/thrift_smoke.rs` | `echo`, `sum`, and error contract |
| Thrift framed binary | Apache Thrift-generated Node.js client | Smoke-tested in CI | `scripts/thrift-cross-language-smoke.sh` | generated `echo` and `sum` client interoperability |
| MQTT TCP | Rust `rumqttc` client | Smoke-tested | `scripts/blackbox.sh` and MQTT examples | anonymous/authenticated TCP, QoS, retain, will and in-process sessions |
| MQTT TCP | Mosquitto CLI | Smoke-tested in CI | `BIUBIN_EXTERNAL_SMOKE=1 bash scripts/blackbox.sh` | standard publish/subscribe interoperability |
| MQTT TLS / mTLS / WebSocket | Rust clients and browser MQTT client | Smoke-tested | `scripts/blackbox.sh` and browser smoke | TLS modes, WebSocket transport and browser publish/subscribe |

## Explicitly not covered by this matrix

- MQTT sessions and offline queues across a `biubin` process restart.
- MQTT per-packet topic ACLs or strict broker drain/join semantics.
- Performance numbers, maximum sustainable throughput, and long-running
  stability under a prescribed load profile.
- Full conformance testing for every HTTP, gRPC, MQTT, Thrift, GraphQL, or
  WebSocket client implementation.

The CI compatibility job installs external tools only for testing. The runtime
image still contains one `biubin` binary and does not start a broker, Thrift,
or protocol-client sidecar.
