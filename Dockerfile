FROM node:22-bookworm-slim AS web-builder
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM rust:1.97-bookworm AS builder
WORKDIR /src
COPY . .
COPY --from=web-builder /src/web/dist ./web/dist
# The 1.97-bookworm image ships 1.97.1 for the native target. Selecting it
# explicitly avoids rustup fetching the workspace's 1.97.0 toolchain in an
# otherwise offline image build.
RUN cargo +1.97.1 build --release --locked --bin biubin

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/biubin /usr/local/bin/biubin
EXPOSE 8080 9000 9001 7000 7001/udp 1883 1884 1885 8883 8083 9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["/usr/local/bin/biubin", "healthcheck"]
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/biubin"]
