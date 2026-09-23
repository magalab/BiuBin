# 技术细节

这里记录实现、监听器、协议行为和验证方式；快速使用请先看
[README.zh.md](../README.zh.md)。完整英文版本见
[technical-details.md](technical-details.md)。

以下命令均从仓库根目录执行。源码使用
[`rust-toolchain.toml`](../rust-toolchain.toml) 中固定的 Rust toolchain；CI 和
Docker 构建使用 Node 22 构建前端，Docker Compose 与跨语言客户端检查是可选工具。

## 架构

`biubin` 是一个 Rust 单进程服务，所有协议适配器都会编译进同一个二进制。
Svelte 控制面板和 Scalar OpenAPI 页面先单独构建，再由
`crates/app/build.rs` 嵌入二进制；没有 `web/dist` 时会使用
`web/fallback.html`。

服务不持久化状态：事件、MQTT 会话和离线队列都只保存在当前进程中，重启
后会丢失。

## 默认监听器

| 端口 | 协议监听 | 说明 |
|---:|---|---|
| 8080 | HTTP、WebSocket、SSE、GraphQL、控制面 | 开发控制面板 |
| 9000 | gRPC h2c | 开启 reflection |
| 9001 | gRPC TLS/mTLS | 默认关闭 |
| 7000 | TCP echo | 行模式和 4-byte length-prefix |
| 7001 | UDP echo | 支持确定性的延迟/丢包 |
| 1883 | MQTT anonymous TCP | MQTT 默认关闭 |
| 1884 | MQTT authenticated TCP | 使用配置的凭据 |
| 1885 | MQTT 5 | 使用配置的凭据 |
| 8883 | MQTT TLS/mTLS | 对外 TLS 代理，需要 MQTT 和 TLS 配置 |
| 8083 | MQTT over WebSocket | 匿名 WebSocket 传输，需要 MQTT 配置 |
| 9090 | Thrift framed binary | IDL 位于 `thrift/biubin.thrift` |
| 1886 | 内部 MQTT TLS 后端 | 仅回环访问，不得暴露 |

所有 MQTT 监听默认关闭。设置 `BIUBIN_MQTT_ENABLED=true` 后才会开启 TCP、认证
TCP、MQTT 5 和 WebSocket；`8883` 还需要额外设置
`BIUBIN_MQTT_TLS_ENABLED=true`。

将端口设置为 `0` 会请求操作系统分配临时端口，实际端口可从
`/api/v1/info` 和启动日志中获取。

## HTTP 和协议行为

- 请求回显：`/get`、`/post`、`/put`、`/patch`、`/delete`、`/anything[/...]`。
- 状态、headers、延迟、重定向、压缩、认证、cookies、缓存、ETag、流式、失败和
  确定性媒体 fixture。
- `/openapi` 和 `/openapi.json` 提供交互式及机器可读的 API 文档。
- `/healthz`、`/readyz`、`/api/v1/info`、`/api/v1/capabilities` 和
  `/api/v1/events` 提供健康、发现和事件能力。
- WebSocket 使用 `/ws/echo`、`/ws/room/{name}`、`/ws/ticker` 和 `/ws/close`；
  SSE 使用 `/sse/events` 和 `/sse/ticker`；控制面事件流使用
  `/api/v1/events/stream`。
- GraphQL 使用 `/graphql` 和 `/graphql/ws`。
- gRPC 契约在 `proto/biubin.proto`，覆盖 unary、streaming、metadata、
  status、TLS/mTLS 和 reflection。
- TCP/UDP、Thrift 和 MQTT 的客户端/跨语言验证范围见
  [协议兼容性矩阵](compatibility-matrix.md)。

## 配置

配置全部通过 `BIUBIN_*` 环境变量提供，默认值位于
`crates/core/src/config.rs`，[`.env.example`](../.env.example) 是可复制的参考；
二进制不会自动加载 `.env` 文件。

常用配置分组：

- 监听：`BIUBIN_BIND_HOST`、`BIUBIN_ADVERTISE_HOST`、各协议的 `*_PORT`；
- TLS：`BIUBIN_GRPC_TLS_ENABLED`、`BIUBIN_GRPC_TLS_MODE`、证书路径变量；
- MQTT：`BIUBIN_MQTT_ENABLED`、`BIUBIN_MQTT_TLS_ENABLED`、用户名、密码和
  `BIUBIN_MQTT_TLS_BACKEND_PORT`；
- HTTP 限制：body/bytes 上限和外部重定向 allowlist；
- 故障模拟：TCP 延迟/关闭/读取限制、UDP 延迟/丢包比例；
- 资源：`BIUBIN_MAX_CONNECTIONS`、`BIUBIN_EVENT_CAPACITY`。

服务默认只绑定回环地址。绑定 `0.0.0.0` 会暴露开发 fixture、默认 MQTT 凭据及
已启用的控制端点；部署到代理后还需要由代理处理 CORS、WebSocket Origin 和
真实客户端地址策略。

## 容器镜像

镜像使用 Node 前端、Rust 构建器和
`gcr.io/distroless/cc-debian12:nonroot` 运行时三阶段构建。当前二进制使用
glibc，因此 distroless Debian 可以直接复用现有构建链；Alpine 需要另行构建
musl target，不能只替换最终基础镜像。

最终容器以 `nonroot` 用户运行，不在运行时镜像中安装 Rust、Node 或 shell，
Docker healthcheck 直接调用二进制的 `healthcheck` 子命令。

### 如果改用 Alpine

Alpine 不是当前运行时镜像的直接替换。现有构建器产出的二进制链接
的是 glibc；只把最终基础镜像换成 Alpine 会缺少 ELF loader 和 glibc runtime，
容器会在 `biubin` 启动前直接失败。

要提供 Alpine 变体，需要单独构建 musl target，通常还要在 builder 中安装 musl
Rust target，以及 `ring` 等依赖所需的本地编译工具。`linux/amd64` 和
`linux/arm64` 需要分别验证；运行时还应明确安装 CA 证书，并按测试场景决定
是否需要 timezone 数据。musl 与 glibc 在 DNS 和 libc 行为上存在差异，可能影响
测试名称解析和超时行为。

Alpine 的收益是基础镜像更小，但代价是额外的构建链和兼容性矩阵。因此当前默认
使用 distroless Debian：它已经足够小、以非 root 运行、没有 shell 和包管理器，
并且可以直接复用现有 glibc 构建，不改变协议行为。只有在独立的 musl 构建完成，
并且两个支持架构都通过完整协议冒烟测试后，才建议把 Alpine 作为可选变体。

## 验证

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
bash scripts/blackbox.sh
node scripts/check-http-route-contract.mjs
bash scripts/compose-smoke.sh
```

前端检查使用 `npm ci`、`npm run check`、`npm run build` 和 Playwright；Playwright
会通过 `web/playwright.config.ts` 自动启动本地 fixture。CI 还会安装
`grpcurl`、Mosquitto client 及 Apache Thrift compiler，执行跨语言冒烟测试。

明确不覆盖性能指标、最大吞吐、长时间稳定性、MQTT 重启后持久化会话、逐包
ACL 和完整协议一致性，详见[兼容性矩阵](compatibility-matrix.md)。
