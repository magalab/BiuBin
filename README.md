# biubin

[中文](README.zh.md)

`biubin` is a local-first, deterministic protocol fixture for testing clients,
SDKs, gateways, and proxies. It runs HTTP, gRPC, WebSocket, SSE, TCP/UDP,
GraphQL, Thrift, and optional MQTT adapters in one binary.

Run the commands below from the repository root. Source builds use the Rust
toolchain pinned in [`rust-toolchain.toml`](rust-toolchain.toml); Docker is
optional unless you use the image or Compose workflow.

## Quick start

### Run from source

```sh
cargo run --bin biubin -p biubin
```

Without a prior frontend build, the binary serves a small fallback page. For
the full Svelte control plane, run `cd web && npm ci && npm run build` before
starting the binary.

Open <http://127.0.0.1:8080/> or try:

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/anything/example
```

### Run the image

The image is published for `linux/amd64` and `linux/arm64`:

```sh
docker run --rm --name biubin \
  -p 8080:8080 \
  -e BIUBIN_BIND_HOST=0.0.0.0 \
  ghcr.io/magalab/biubin:0.2.1
```

The binary binds to loopback by default. The explicit bind setting is required
when a container port is published to the host.

For a local build using the repository's Compose configuration:

```sh
docker compose up --build
```

The default Compose configuration publishes the protocol ports but leaves
gRPC TLS and MQTT disabled; use the all-listeners command below or
`bash scripts/compose-smoke.sh` when those adapters are needed.

The image defaults to the HTTP control plane. Enable MQTT explicitly when it is
needed:

```sh
docker run --rm --name biubin \
  -p 8080:8080 -p 8083:8083 \
  -e BIUBIN_BIND_HOST=0.0.0.0 \
  -e BIUBIN_MQTT_ENABLED=true \
  ghcr.io/magalab/biubin:0.2.1
```

To start every public listener in one local test container, generate throwaway
development certificates first, then run:

```sh
bash scripts/generate-dev-certs.sh certs/dev

docker run --rm --name biubin-all \
  --user "$(id -u):$(id -g)" \
  -p 8080:8080 -p 9000:9000 -p 9001:9001 \
  -p 7000:7000 -p 7001:7001/udp -p 9090:9090 \
  -p 1883:1883 -p 1884:1884 -p 1885:1885 \
  -p 8883:8883 -p 8083:8083 \
  -v "$PWD/certs/dev:/certs/dev:ro" \
  -e BIUBIN_BIND_HOST=0.0.0.0 \
  -e BIUBIN_ADVERTISE_HOST=127.0.0.1 \
  -e BIUBIN_MQTT_ENABLED=true \
  -e BIUBIN_GRPC_TLS_ENABLED=true \
  -e BIUBIN_MQTT_TLS_ENABLED=true \
  -e BIUBIN_TLS_CERT_PATH=/certs/dev/server.pem \
  -e BIUBIN_TLS_KEY_PATH=/certs/dev/server-key.pem \
  -e BIUBIN_TLS_CLIENT_CA_PATH=/certs/dev/ca.pem \
  ghcr.io/magalab/biubin:0.2.1
```

This local-only mode enables the public listeners below:

- HTTP / WebSocket / SSE / GraphQL / control plane: `8080`
- gRPC h2c and TLS: `9000` / `9001`
- TCP and UDP echo: `7000` / `7001`
- Thrift framed binary: `9090`
- MQTT anonymous / authenticated / v5: `1883` / `1884` / `1885`
- MQTT TLS and WebSocket: `8883` / `8083`

Operational notes:

- `1886` is an internal MQTT TLS backend and is intentionally not published.
- `--user "$(id -u):$(id -g)"` lets the non-root process read the mounted
  development certificates, which intentionally remain mode `600`.
- The generated certificates and default MQTT credentials are for local testing;
  do not expose this command directly to an untrusted network.

## Common usage

- Open `/` for the interactive control plane.
- Open `/openapi` for the bundled HTTP API explorer.
- Use `/api/v1/info` and `/api/v1/capabilities` for machine-readable discovery.
- Use `/anything/<path>` for request echoing and `/status/<code>` for status
  testing.
- Use `/graphql` and `/graphql/ws` for GraphQL HTTP and subscriptions.
- Use `/sse/events`, `/sse/ticker`, `/api/v1/events`, and
  `/api/v1/events/stream` for SSE and event-stream behavior.
- Use `/ws/echo`, `/ws/room/<name>`, `/ws/ticker`, and `/ws/close` for
  WebSocket upgrade scenarios.
- Use the protocol smoke clients under `crates/app/examples/` for gRPC,
  GraphQL, MQTT, and Thrift examples.

## Configuration

Configuration is supplied through `BIUBIN_*` environment variables. Copy
[`.env.example`](.env.example) as a starting point, then export the values
before launching the service. The binary does not load `.env` automatically.

The service is a development and integration-test fixture. Do not expose the
default MQTT credentials, generated certificates, or an unrestricted
`0.0.0.0` deployment to an untrusted network.

## Documentation

- [Technical details](docs/technical-details.md): architecture, listeners,
  endpoint catalog, configuration, testing, and security notes.
- [Protocol compatibility matrix](docs/compatibility-matrix.md): tested client
  paths and explicit non-goals.
- [`proto/biubin.proto`](proto/biubin.proto): gRPC contract.
- [`thrift/biubin.thrift`](thrift/biubin.thrift): Thrift contract.

## Development checks

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
bash scripts/blackbox.sh
node scripts/check-http-route-contract.mjs
cd web && npm ci && npm run check && npm run build
```

The CI workflow additionally runs the browser, Docker Compose, and external
cross-language compatibility smokes.
