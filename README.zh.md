# biubin

[English](README.md)

`biubin` 是一个本地优先的协议测试服务。项目有意以单个二进制文件交付；各协议适配器都会编译到同一个进程中。

## 快速开始

```sh
cargo run --bin biubin -p biubin
```

然后打开 <http://127.0.0.1:8080/>，或运行：

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/anything/example
```

默认的 Compose 文件会启动一个 `biubin` 服务容器：

```sh
docker compose up --build
```

镜像包含前端构建阶段，但运行时只启动
`/usr/local/bin/biubin`；Rust 和前端依赖均从已提交的锁文件解析。在没有
Node 工具链的情况下执行本地 `cargo run`，服务会使用内置的小型 fallback 页面。

## 配置

配置通过 `BIUBIN_*` 环境变量提供。仓库中的 [`.env.example`](.env.example)
可作为参考；二进制不会自动加载 `.env`，因此启动前需要在 shell 中导出变量：

```sh
cp .env.example .env
set -a
. ./.env
set +a
cargo run --bin biubin -p biubin
```

将监听端口设置为 `0` 会请求操作系统分配空闲端口；`info` 接口和启动日志会
报告实际选中的端口，而不是配置中的 `0`。

当前二进制包含 HTTP、gRPC（h2c 以及可选的 TLS/mTLS）、WebSocket、SSE、
TCP/UDP echo、GraphQL、Thrift，以及一个可选的内置 MQTT Broker。MQTT TLS
由进程内代理终止，再转发到内部 Broker listener；不需要第二个运行时服务。

常用端点和监听器：

- `GET /api/v1/info` 和 `GET /api/v1/capabilities` 用于描述正在运行的服务。
- `/api/v1/capabilities` 返回描述服务的 JSON 文档。端点条目通常使用单个
  `methods` 数组；单方法端点包含一个值，`*` 表示任意方法或协议操作。
  当前仍为预发布阶段，capabilities schema 版本为 `1`。HTTP fixture
  条目和 `/openapi.json` 使用同一份 endpoint metadata。
- 根路径提供请求回显端点：`GET/HEAD /get`、`POST /post`、`PUT /put`、
  `PATCH /patch`、`DELETE /delete`，以及支持任意方法的 `/anything`、
  `/anything/` 和任意 `/anything/...` 子路径。这些端点返回 BiuBin 自有的
  请求回显 JSON；方法专用端点使用其他方法时返回 `405`。
- 除了为测试状态映射而故意支持任意方法的 `/status/{code}` 外，其他附加的
  读取型 fixture 均只注册 `GET/HEAD`，使用其他方法时返回 `405`。包括
  `/headers`、`/ip`、`/user-agent`、延迟、重定向、压缩、认证、媒体、缓存、
  内容表示、流式和失败模拟 fixture。
- HTTP fixture 统一使用根路径，包括 `/status/{code}`、`/headers`、
  `/ip`、`/user-agent`、`/delay/{seconds}`、`/redirect/{count}`、
  `/redirect-to`、`/bytes/{n}`、`/stream-bytes/{n}`、`/range/{n}`、
  `/gzip`、`/deflate`、`/basic-auth/{user}/{password}`、
  `/response-headers`、`/cookies`、`/cache`、`/cache/{seconds}`、
  `/etag/{value}`、`/json`、`/html`、`/xml`、`/encoding/utf8`、
  `/drip`、`/unstable` 和 `/bearer`。
- `/redirect-to` 默认只允许相对 URL。设置
  `BIUBIN_HTTP_ALLOW_EXTERNAL_REDIRECTS=true` 后才允许绝对 URL；绝对 URL
  仅支持 HTTP(S)，长度不超过 2048 字节，且会拒绝本地字面量地址；hostname
  还必须精确列在 `BIUBIN_HTTP_EXTERNAL_REDIRECT_HOSTS` 中。
- 媒体 fixture 位于 `/image`、`/image/png`、`/image/jpeg`、
  `/image/svg`、`/image/webp`、`/video`、`/video/mp4` 和
  `/video/webm`。媒体 fixture、`/bytes/{n}` 和 `/range/{n}` 支持
  `Accept-Ranges: bytes` 及单段 byte range 请求；图片、视频和音频 fixture
  统一支持 Range/HEAD 行为。
- 音频 fixture 位于 `/audio`、`/audio/wav` 和 `/audio/mp3`；默认 WAV/MP3
  是确定性的 3 秒测试音调。
- 媒体文件位于 `crates/app/assets`，编译时通过 `include_bytes!` 嵌入服务。
  如需重新生成确定性 fixture，可运行
  `scripts/generate-media-fixtures.sh`（需要 `ffmpeg` 和 `cwebp`）。
- 交互式 HTTP 文档位于 `/openapi`，原始 OpenAPI 3.0 文档位于
  `/openapi.json`。文档 UI 使用随服务打包的 Scalar API Reference，
  无公网访问也可使用。
- HTTP、WebSocket、SSE 和 GraphQL HTTP 使用 `8080`；GraphQL subscription 使用
  `ws://127.0.0.1:8080/graphql/ws`。
- gRPC h2c 使用 `9000`；可选的 gRPC TLS/mTLS 使用 `9001`；TCP/UDP echo
  使用 `7000`/`7001`；Thrift binary 使用 `9090`。
- Socket 故障行为是确定性的，并由配置控制：`BIUBIN_TCP_DELAY_MS`、
  `BIUBIN_TCP_CLOSE_AFTER`（`0` 表示保持连接）、`BIUBIN_TCP_READ_LIMIT`、
  `BIUBIN_UDP_DELAY_MS` 和 `BIUBIN_UDP_DROP_PERCENT`（按每 100 个数据包的
  固定周期执行）。
- `BIUBIN_MAX_CONNECTIONS` 限制 HTTP/gRPC 的共享并发工作数，以及 TCP/WS/SSE/
  Thrift 活跃连接数（默认 `1000`）；被拒绝的工作会记录为有界资源事件。
  MQTT 在其内置 Broker 运行时中使用同一个数值限制。
- GraphQL introspection 默认启用，可通过
  `BIUBIN_GRAPHQL_INTROSPECTION_ENABLED=false` 禁用。
- `/sse/events` 会将缺失、无效或负数的 `Last-Event-ID` 视为 `0`，重放固定
  序列 `1..5`；当游标至少为 `5` 时返回空流。`/api/v1/events/stream` 会先
  重放指定数字游标之后保留的事件，然后跟随实时事件；落后的订阅者会收到
  `lagged` 通知，并重新重放保留环中的事件。
- 设置 `BIUBIN_MQTT_ENABLED=true` 可启用 MQTT TCP、认证 TCP、MQTT 5 和
  WebSocket listener。设置 `BIUBIN_MQTT_TLS_ENABLED=true` 可启用 `8883`
  上的 TLS；TLS backend 使用相同的 `BIUBIN_MQTT_USERNAME`/
  `BIUBIN_MQTT_PASSWORD` 凭据。使用 `BIUBIN_MQTT_TLS_MODE=mtls` 可启用客户端
  证书校验。
- 内置 MQTT fixture 接受合法 topic，不启用 ACL；`/api/v1/capabilities` 会
  明确报告这一点。Session 和离线队列只保存在当前进程内，重启后不会持久化。
- 使用 `scripts/generate-dev-certs.sh certs/dev` 生成开发证书。证书会被 Git
  忽略，仅用于本地测试。

默认监听器映射：

| 端口 | Listener |
|---:|---|
| 8080 | HTTP、WS、SSE、GraphQL 和控制面 |
| 9000 / 9001 | gRPC h2c / TLS 或 mTLS |
| 7000 / 7001 | TCP / UDP echo |
| 1883 / 1884 / 1885 | MQTT 匿名 / 已认证 / MQTT 5 |
| 8883 | MQTT TLS 或 mTLS |
| 8083 | MQTT over WebSocket |
| 9090 | Thrift framed binary |
| 1886 | 内部 loopback（仅监听回环地址）MQTT TLS backend；禁止暴露或映射此端口 |

协议 smoke client 位于二进制旁边。先启动 `biubin`，再在另一个 shell 中运行：

```sh
cargo run -p biubin --example graphql_smoke
cargo run -p biubin --example grpc_smoke -- 127.0.0.1 9000 h2c
cargo run -p biubin --example thrift_smoke
cargo run -p biubin --example mqtt_smoke -- 127.0.0.1 1884 biubin biubin-dev tcp
cargo run -p biubin --example mqtt_v5_smoke -- 127.0.0.1 1885 biubin biubin-dev
```

CI 还会使用 `grpcurl`、`mosquitto_pub/sub` 和 Apache Thrift 生成的 Node.js
客户端执行跨语言兼容性检查。这些仅是测试依赖，不会进入运行时镜像；本地
安装对应工具及 Thrift compiler 后，可运行
`BIUBIN_EXTERNAL_SMOKE=1 bash scripts/blackbox.sh` 执行同一组检查。

当前已验证的客户端/工具范围，以及明确未承诺的能力，记录在
[协议兼容性矩阵](docs/compatibility-matrix.md)中。

GraphQL HTTP 示例：

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  --data '{"query":"{ serverInfo { name version } echo(input: { message: \"hello\" }) { message } }"}'
```

Thrift 客户端应使用带 framed binary protocol 的
[`thrift/biubin.thrift`](thrift/biubin.thrift)。Rust smoke client 是该 wire
contract 的可执行参考实现。

gRPC smoke client 的第三个参数可以是 `h2c`、`tls` 或 `mtls`；TLS 模式还需要
提供 CA 路径，mTLS 还需要客户端证书和密钥路径。例如：

```sh
cargo run -p biubin --example grpc_smoke -- 127.0.0.1 9001 mtls \
  certs/dev/ca.pem certs/dev/client.pem certs/dev/client-key.pem
```

`/bytes/{n}` 为每个从零开始的字节索引返回 `index % 251`；
`/stream-bytes/{n}` 使用相同序列，并以固定的 16 KiB chunk 返回。两个
端点都会拒绝超过 `BIUBIN_MAX_BYTES_RESPONSE` 的值。

`/drip` 按请求的 duration 输出数据。duration 为 0 表示立即输出，此时即使
`chunk_size=1` 也会合并为一个 chunk；正数 duration 才按请求的 chunk size
输出（并受服务上限限制）。

开发用 HTTP 控制面仅允许同源访问：biubin 不添加开放 CORS header、不校验
WebSocket `Origin`，也不信任 `X-Forwarded-For`；部署在代理后面时必须由代理
执行这些策略。绑定 `0.0.0.0` 会将开发 fixture、默认 MQTT 凭据以及所有启用的
控制端点暴露到网络中。

该服务明确定位为开发/测试 fixture。不要将默认凭据或生成的证书暴露给不受信任
的网络。

本地、无公网访问的 black-box 检查命令：

```sh
bash scripts/blackbox.sh
```

Docker 交付 smoke 会构建镜像、启动唯一的 Compose 服务，并检查健康状态、
静态资源、媒体 Range、gRPC TLS 和 MQTT listener：

```sh
bash scripts/compose-smoke.sh
```

真实浏览器 smoke 需要先生成 `web/dist` 并安装 Playwright 浏览器：

```sh
cd web
npm ci
npm run build
npx playwright install chromium
npm run test:e2e
```

## CI 和发布

GitHub Actions 会在 push 和 pull request 时运行前端、路由契约、Rust、
Docker Compose、真实浏览器、black-box 以及外部协议兼容性检查。推送到
`main` 或推送版本 tag 时，还会将多架构镜像发布到
`ghcr.io/magalab/biubin`。

推送类似 `v0.2.0` 的版本 tag 会创建 GitHub release，其中包含 Linux `amd64`
和 `arm64` 二进制归档及 SHA-256 校验和。

## 源码结构

- `crates/core/src/` 包含共享配置、有界事件存储和 readiness 状态。
- `crates/app/src/lifecycle.rs` 负责进程启动、listener 绑定、任务生命周期和
  关闭流程。
- `crates/app/src/http/` 包含路由、中间件、控制面端点、HTTP fixture、WebSocket
  和 SSE handler。
- `crates/app/src/{grpc,tcp,udp,mqtt,thrift}.rs` 包含各协议运行时。
- `crates/app/examples/` 包含协议 smoke client；`crates/app/tests/` 包含跨模块
  Rust 集成测试。
- `proto/` 和 `thrift/` 是 wire contract 的源文件；`web/` 是嵌入二进制并由其
  托管的 Svelte 控制面。
