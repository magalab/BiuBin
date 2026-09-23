# biubin

[English](README.md)

`biubin` 是一个本地优先、确定性的协议测试 fixture，用于测试客户端、SDK、
网关和代理。HTTP、gRPC、WebSocket、SSE、TCP/UDP、GraphQL、Thrift 以及可选
的 MQTT 都运行在同一个二进制进程中。

以下命令均从仓库根目录执行。源码构建使用
[`rust-toolchain.toml`](rust-toolchain.toml) 中固定的 Rust toolchain；只有使用
镜像或 Compose 工作流时才需要 Docker。

## 快速开始

### 从源码运行

```sh
cargo run --bin biubin -p biubin
```

如果没有提前构建前端，二进制会提供一个小型 fallback 页面；需要完整的 Svelte
控制面板时，请先执行 `cd web && npm ci && npm run build`，再启动二进制。

打开 <http://127.0.0.1:8080/>，或执行：

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/anything/example
```

### 运行镜像

镜像支持 `linux/amd64` 和 `linux/arm64`：

```sh
docker run --rm --name biubin \
  -p 8080:8080 \
  -e BIUBIN_BIND_HOST=0.0.0.0 \
  ghcr.io/magalab/biubin:0.2.1
```

二进制默认只绑定回环地址；发布容器端口时必须显式设置该变量，宿主机才能访问。

本地构建镜像并使用仓库中的 Compose 配置启动：

```sh
docker compose up --build
```

默认 Compose 配置会发布协议端口，但 gRPC TLS 和 MQTT 仍保持关闭；需要这些适配器
时，请使用下面的全监听器命令，或执行 `bash scripts/compose-smoke.sh`。

需要 MQTT 时显式开启：

```sh
docker run --rm --name biubin \
  -p 8080:8080 -p 8083:8083 \
  -e BIUBIN_BIND_HOST=0.0.0.0 \
  -e BIUBIN_MQTT_ENABLED=true \
  ghcr.io/magalab/biubin:0.2.1
```

如果需要在一个本地测试容器中开启所有对外特性，先生成一次临时开发证书，
再执行：

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

该本地测试模式会开启以下对外监听器：

- HTTP / WebSocket / SSE / GraphQL / 控制面板：`8080`
- gRPC h2c / TLS：`9000` / `9001`
- TCP / UDP echo：`7000` / `7001`
- Thrift framed binary：`9090`
- MQTT 匿名 / 认证 / v5：`1883` / `1884` / `1885`
- MQTT TLS / WebSocket：`8883` / `8083`

运行注意事项：

- `1886` 是内部 MQTT TLS 后端，刻意不映射到宿主机。
- `--user "$(id -u):$(id -g)"` 让非 root 进程可以读取挂载的开发证书；这些
  证书刻意保持为 `600`。
- 生成的证书和默认 MQTT 凭据只适合本地测试，不要直接暴露到不受信任的网络。

## 常用用法

- 打开 `/` 使用交互式控制面板。
- 打开 `/openapi` 使用内置 HTTP API 调试器。
- 使用 `/api/v1/info` 和 `/api/v1/capabilities` 做机器可读的服务发现。
- 使用 `/anything/<path>` 回显请求，使用 `/status/<code>` 测试状态码。
- 使用 `/graphql` 和 `/graphql/ws` 测试 GraphQL HTTP 与订阅。
- 使用 `/sse/events`、`/sse/ticker`、`/api/v1/events` 和
  `/api/v1/events/stream` 测试 SSE 与事件流。
- 使用 `/ws/echo`、`/ws/room/<name>`、`/ws/ticker` 和 `/ws/close` 测试
  WebSocket 升级场景。
- gRPC、GraphQL、MQTT、Thrift 示例位于 `crates/app/examples/`。

## 配置

配置通过 `BIUBIN_*` 环境变量提供。可复制 [`.env.example`](.env.example)
作为起点，然后在启动前导出变量；二进制不会自动加载 `.env`。

该服务定位为开发和集成测试 fixture。不要将默认 MQTT 凭据、生成的证书或未
加限制的 `0.0.0.0` 部署暴露到不受信任的网络。

## 文档

- [技术细节](docs/technical-details.zh.md)：架构、监听器、端点、配置、测试和安全说明。
- [协议兼容性矩阵](docs/compatibility-matrix.md)：已验证的客户端路径及明确的非目标。
- [`proto/biubin.proto`](proto/biubin.proto)：gRPC 契约。
- [`thrift/biubin.thrift`](thrift/biubin.thrift)：Thrift 契约。

## 开发检查

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
bash scripts/blackbox.sh
node scripts/check-http-route-contract.mjs
cd web && npm ci && npm run check && npm run build
```

CI 还会执行浏览器、Docker Compose 和跨语言协议兼容性冒烟测试。
