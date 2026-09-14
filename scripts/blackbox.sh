#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${BIUBIN_BIN:-$repo_dir/target/debug/biubin}"
http_port="${BIUBIN_BLACKBOX_HTTP_PORT:-58081}"
grpc_port="${BIUBIN_BLACKBOX_GRPC_PORT:-59001}"
grpc_tls_port="${BIUBIN_BLACKBOX_GRPC_TLS_PORT:-59002}"
tcp_port="${BIUBIN_BLACKBOX_TCP_PORT:-57002}"
udp_port="${BIUBIN_BLACKBOX_UDP_PORT:-57003}"
thrift_port="${BIUBIN_BLACKBOX_THRIFT_PORT:-59091}"
mqtt_port="${BIUBIN_BLACKBOX_MQTT_PORT:-51883}"
mqtt_auth_port="${BIUBIN_BLACKBOX_MQTT_AUTH_PORT:-51884}"
mqtt_v5_port="${BIUBIN_BLACKBOX_MQTT_V5_PORT:-51885}"
mqtt_ws_port="${BIUBIN_BLACKBOX_MQTT_WS_PORT:-51886}"
mqtt_tls_port="${BIUBIN_BLACKBOX_MQTT_TLS_PORT:-51887}"
mqtt_tls_backend_port="${BIUBIN_BLACKBOX_MQTT_TLS_BACKEND_PORT:-51889}"
tls_smoke_http_port="${BIUBIN_BLACKBOX_TLS_SMOKE_HTTP_PORT:-58082}"
tls_smoke_grpc_port="${BIUBIN_BLACKBOX_TLS_SMOKE_GRPC_PORT:-59003}"
tls_smoke_mqtt_port="${BIUBIN_BLACKBOX_TLS_SMOKE_MQTT_PORT:-51897}"
tmp_dir="$(mktemp -d)"
pid=""
tls_pid=""
external_mqtt_pid=""
require_external_smoke="${BIUBIN_EXTERNAL_SMOKE:-0}"

cleanup() {
    if [[ -n "$tls_pid" ]] && kill -0 "$tls_pid" 2>/dev/null; then
        kill "$tls_pid" 2>/dev/null || true
        wait "$tls_pid" 2>/dev/null || true
    fi
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    if [[ -n "$external_mqtt_pid" ]] && kill -0 "$external_mqtt_pid" 2>/dev/null; then
        kill "$external_mqtt_pid" 2>/dev/null || true
        wait "$external_mqtt_pid" 2>/dev/null || true
    fi
    rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

if [[ ! -x "$binary" ]]; then
    cargo +1.97.0 build --manifest-path "$repo_dir/Cargo.toml" --locked --bin biubin
fi

bash "$repo_dir/scripts/generate-dev-certs.sh" "$tmp_dir/certs" >/dev/null 2>&1

(
    cd "$repo_dir"
    BIUBIN_BIND_HOST=127.0.0.1 \
    BIUBIN_ADVERTISE_HOST=127.0.0.1 \
    BIUBIN_HTTP_PORT="$tls_smoke_http_port" \
    BIUBIN_GRPC_H2C_PORT=0 \
    BIUBIN_GRPC_TLS_PORT="$tls_smoke_grpc_port" \
    BIUBIN_GRPC_TLS_ENABLED=true \
    BIUBIN_GRPC_TLS_MODE=tls \
    BIUBIN_TLS_CERT_PATH="$tmp_dir/certs/server.pem" \
    BIUBIN_TLS_KEY_PATH="$tmp_dir/certs/server-key.pem" \
    BIUBIN_TLS_CLIENT_CA_PATH="$tmp_dir/certs/ca.pem" \
    BIUBIN_TCP_PORT=0 \
    BIUBIN_UDP_PORT=0 \
    BIUBIN_THRIFT_PORT=0 \
    BIUBIN_MQTT_ENABLED=true \
    BIUBIN_MQTT_TCP_PORT=0 \
    BIUBIN_MQTT_AUTH_TCP_PORT=0 \
    BIUBIN_MQTT_V5_PORT=0 \
    BIUBIN_MQTT_WS_PORT=0 \
    BIUBIN_MQTT_TLS_ENABLED=true \
    BIUBIN_MQTT_TLS_MODE=tls \
    BIUBIN_MQTT_TLS_PORT="$tls_smoke_mqtt_port" \
    BIUBIN_MQTT_TLS_BACKEND_PORT=0 \
    BIUBIN_MQTT_USERNAME=biubin \
    BIUBIN_MQTT_PASSWORD=biubin-dev \
    "$binary" >"$tmp_dir/biubin-tls.log" 2>&1
) &
tls_pid=$!

tls_ready=0
for _ in $(seq 1 100); do
    if curl --fail --silent "http://127.0.0.1:$tls_smoke_http_port/readyz" >/dev/null 2>&1; then
        tls_ready=1
        break
    fi
    sleep 0.05
done
if [[ "$tls_ready" != 1 ]]; then
    sed -n '1,160p' "$tmp_dir/biubin-tls.log"
    exit 1
fi
cargo +1.97.0 run --quiet -p biubin --example grpc_smoke -- 127.0.0.1 "$tls_smoke_grpc_port" tls \
    "$tmp_dir/certs/ca.pem"
cargo +1.97.0 run --quiet -p biubin --example mqtt_smoke -- 127.0.0.1 "$tls_smoke_mqtt_port" \
    biubin biubin-dev tls "$tmp_dir/certs/ca.pem"

kill "$tls_pid" 2>/dev/null || true
wait "$tls_pid" 2>/dev/null || true
tls_pid=""

(
    cd "$repo_dir"
    BIUBIN_BIND_HOST=127.0.0.1 \
    BIUBIN_ADVERTISE_HOST=127.0.0.1 \
    BIUBIN_HTTP_PORT="$http_port" \
    BIUBIN_GRPC_H2C_PORT="$grpc_port" \
    BIUBIN_GRPC_TLS_PORT="$grpc_tls_port" \
    BIUBIN_GRPC_TLS_ENABLED=true \
    BIUBIN_GRPC_TLS_MODE=mtls \
    BIUBIN_TLS_CERT_PATH="$tmp_dir/certs/server.pem" \
    BIUBIN_TLS_KEY_PATH="$tmp_dir/certs/server-key.pem" \
    BIUBIN_TLS_CLIENT_CA_PATH="$tmp_dir/certs/ca.pem" \
    BIUBIN_TCP_PORT="$tcp_port" \
    BIUBIN_UDP_PORT="$udp_port" \
    BIUBIN_THRIFT_PORT="$thrift_port" \
    BIUBIN_MQTT_ENABLED=true \
    BIUBIN_MQTT_TCP_PORT="$mqtt_port" \
    BIUBIN_MQTT_AUTH_TCP_PORT="$mqtt_auth_port" \
    BIUBIN_MQTT_V5_PORT="$mqtt_v5_port" \
    BIUBIN_MQTT_WS_PORT="$mqtt_ws_port" \
    BIUBIN_MQTT_TLS_ENABLED=true \
    BIUBIN_MQTT_TLS_MODE=mtls \
    BIUBIN_MQTT_TLS_PORT="$mqtt_tls_port" \
    BIUBIN_MQTT_TLS_BACKEND_PORT="$mqtt_tls_backend_port" \
    BIUBIN_TLS_CERT_PATH="$tmp_dir/certs/server.pem" \
    BIUBIN_TLS_KEY_PATH="$tmp_dir/certs/server-key.pem" \
    BIUBIN_TLS_CLIENT_CA_PATH="$tmp_dir/certs/ca.pem" \
    "$binary" >"$tmp_dir/biubin.log" 2>&1
) &
pid=$!

ready=0
for _ in $(seq 1 100); do
    if curl --fail --silent "http://127.0.0.1:$http_port/readyz" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.05
done
if [[ "$ready" != 1 ]]; then
    sed -n '1,160p' "$tmp_dir/biubin.log"
    exit 1
fi
BIUBIN_HTTP_PORT="$http_port" "$binary" healthcheck

wait_for_port() {
    local port="$1"
    for _ in $(seq 1 100); do
        if (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    sed -n '1,160p' "$tmp_dir/biubin.log"
    return 1
}

wait_for_port "$grpc_port"
wait_for_port "$grpc_tls_port"
wait_for_port "$mqtt_port"
wait_for_port "$mqtt_auth_port"
wait_for_port "$mqtt_v5_port"
wait_for_port "$mqtt_ws_port"
wait_for_port "$mqtt_tls_port"

curl --fail --silent --show-error "http://127.0.0.1:$http_port/healthz" >/dev/null
curl --fail --silent --show-error "http://127.0.0.1:$http_port/" | grep -q "biubin"
curl --fail --silent --show-error "http://127.0.0.1:$http_port/bytes/64" -o "$tmp_dir/bytes"
test "$(wc -c <"$tmp_dir/bytes" | tr -d ' ')" = 64
status_code="$(curl --silent --show-error -o /dev/null -w '%{http_code}' "http://127.0.0.1:$http_port/status/418")"
test "$status_code" = 418
curl --fail --silent --show-error "http://127.0.0.1:$http_port/image/png" -o "$tmp_dir/image.png"
head -c 4 "$tmp_dir/image.png" | cmp -s - <(printf '\x89PNG')
image_range_status="$(curl --silent --show-error -o "$tmp_dir/image-range" -w '%{http_code}' \
    -H 'Range: bytes=0-9' "http://127.0.0.1:$http_port/image/png")"
test "$image_range_status" = 206
test "$(wc -c <"$tmp_dir/image-range" | tr -d ' ')" = 10
curl --fail --silent --show-error "http://127.0.0.1:$http_port/video/mp4" -o "$tmp_dir/video.mp4"
head -c 8 "$tmp_dir/video.mp4" | grep -q "ftyp"
curl --fail --silent --show-error "http://127.0.0.1:$http_port/audio/wav" -o "$tmp_dir/audio.wav"
head -c 4 "$tmp_dir/audio.wav" | grep -q "RIFF"
curl --fail --silent --show-error "http://127.0.0.1:$http_port/openapi.json" | grep -q '"openapi":"3.0.3"'
curl --fail --silent --show-error "http://127.0.0.1:$http_port/openapi" -o "$tmp_dir/openapi.html"
grep -Eq '/assets/|/openapi.json' "$tmp_dir/openapi.html"
curl --fail --silent --show-error "http://127.0.0.1:$http_port/response-headers?X-Test=blackbox" -D "$tmp_dir/response-headers" -o /dev/null
grep -qi '^x-test: blackbox' "$tmp_dir/response-headers"
forbidden_header_status="$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:$http_port/response-headers?Content-Type=text%2Fplain")"
test "$forbidden_header_status" = 400
redirect_status="$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:$http_port/redirect-to?url=https%3A%2F%2Fexample.com%2Ftarget")"
test "$redirect_status" = 400
curl --fail --silent --show-error "http://127.0.0.1:$http_port/cache" -D "$tmp_dir/cache-headers" -o /dev/null
cache_etag="$(awk 'BEGIN{IGNORECASE=1} /^etag:/ {sub(/^[^:]*:[[:space:]]*/, ""); print; exit}' "$tmp_dir/cache-headers" | tr -d '\r')"
cache_status="$(curl --silent --show-error -o /dev/null -w '%{http_code}' -H "If-None-Match: $cache_etag" "http://127.0.0.1:$http_port/cache")"
test "$cache_status" = 304
range_status="$(curl --silent --show-error -o "$tmp_dir/range" -w '%{http_code}' -H 'Range: bytes=2-5' "http://127.0.0.1:$http_port/range/32")"
test "$range_status" = 206
test "$(wc -c <"$tmp_dir/range" | tr -d ' ')" = 4
curl --fail --silent --show-error -N --max-time 5 \
    "http://127.0.0.1:$http_port/sse/ticker?interval_ms=10&count=2" \
    | grep -q "id: 2"
curl --fail --silent --show-error -N --max-time 5 \
    -H 'Last-Event-ID: 3' \
    "http://127.0.0.1:$http_port/sse/events" \
    | grep -q "id: 5"

event_stream_file="$tmp_dir/events-stream"
set +e
curl --fail --silent --show-error -N --max-time 2 \
    -H 'Last-Event-ID: 0' \
    "http://127.0.0.1:$http_port/api/v1/events/stream" >"$event_stream_file"
event_stream_status=$?
set -e
test "$event_stream_status" = 28
grep -q "event: event" "$event_stream_file"

graphql_result="$(curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data '{"query":"{ serverInfo { name version } }"}' \
    "http://127.0.0.1:$http_port/graphql")"
grep -q '"name":"biubin"' <<<"$graphql_result"
graphql_error="$(curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data '{"query":"{ echo(input: { message: \"__resolver_error\" }) { message } }"}' \
    "http://127.0.0.1:$http_port/graphql")"
grep -q '"code":"RESOLVER_ERROR"' <<<"$graphql_error"
graphql_syntax_error="$(curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data '{"query":"{ echo("}' \
    "http://127.0.0.1:$http_port/graphql")"
grep -q '"code":"SYNTAX_ERROR"' <<<"$graphql_syntax_error"
graphql_validation_error="$(curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data '{"query":"{ missingField }"}' \
    "http://127.0.0.1:$http_port/graphql")"
grep -q '"code":"VALIDATION_ERROR"' <<<"$graphql_validation_error"
graphql_auth_error="$(curl --fail --silent --show-error \
    -H 'content-type: application/json' \
    --data '{"query":"mutation { publishTestEvent(input: { protocol: \"http\", summary: \"smoke\", requireAuth: true, token: \"bad\" }) { id } }"}' \
    "http://127.0.0.1:$http_port/graphql")"
grep -q '"code":"UNAUTHENTICATED"' <<<"$graphql_auth_error"

if command -v grpcurl >/dev/null 2>&1; then
    grpcurl -plaintext "127.0.0.1:$grpc_port" list | grep -q 'biubin.v1.Biubin'
elif [[ "$require_external_smoke" == 1 ]]; then
    echo "BIUBIN_EXTERNAL_SMOKE=1 requires grpcurl" >&2
    exit 1
fi

if [[ "$require_external_smoke" == 1 ]]; then
    for command in mosquitto_pub mosquitto_sub thrift node; do
        if ! command -v "$command" >/dev/null 2>&1; then
            echo "BIUBIN_EXTERNAL_SMOKE=1 requires $command" >&2
            exit 1
        fi
    done

    mqtt_external_topic="biubin/external/smoke"
    mqtt_external_output="$tmp_dir/mosquitto-sub"
    mosquitto_sub \
        -h 127.0.0.1 \
        -p "$mqtt_port" \
        -t "$mqtt_external_topic" \
        -C 1 \
        -W 10 \
        >"$mqtt_external_output" 2>"$tmp_dir/mosquitto-sub.log" &
    external_mqtt_pid=$!
    sleep 0.5
    mosquitto_pub \
        -h 127.0.0.1 \
        -p "$mqtt_port" \
        -t "$mqtt_external_topic" \
        -m "hello from mosquitto"
    wait "$external_mqtt_pid"
    external_mqtt_pid=""
    grep -Fxq "hello from mosquitto" "$mqtt_external_output"

    bash "$repo_dir/scripts/thrift-cross-language-smoke.sh" "127.0.0.1:$thrift_port"
fi

cargo +1.97.0 run --quiet -p biubin --example grpc_smoke -- 127.0.0.1 "$grpc_port" h2c
cargo +1.97.0 run --quiet -p biubin --example grpc_smoke -- 127.0.0.1 "$grpc_tls_port" mtls \
    "$tmp_dir/certs/ca.pem" "$tmp_dir/certs/client.pem" "$tmp_dir/certs/client-key.pem"
cargo +1.97.0 run --quiet -p biubin --example graphql_smoke -- "ws://127.0.0.1:$http_port/graphql/ws"
cargo +1.97.0 run --quiet -p biubin --example thrift_smoke -- "127.0.0.1:$thrift_port"
cargo +1.97.0 run --quiet -p biubin --example mqtt_advanced_smoke -- 127.0.0.1 "$mqtt_port"
cargo +1.97.0 run --quiet -p biubin --example mqtt_smoke -- 127.0.0.1 "$mqtt_auth_port" \
    biubin biubin-dev tcp
cargo +1.97.0 run --quiet -p biubin --example mqtt_smoke -- 127.0.0.1 "$mqtt_ws_port" ws
cargo +1.97.0 run --quiet -p biubin --example mqtt_v5_smoke -- 127.0.0.1 "$mqtt_v5_port" biubin biubin-dev
cargo +1.97.0 run --quiet -p biubin --example mqtt_smoke -- 127.0.0.1 "$mqtt_tls_port" \
    biubin biubin-dev tls "$tmp_dir/certs/ca.pem" "$tmp_dir/certs/client.pem" "$tmp_dir/certs/client-key.pem"

echo "biubin single-binary blackbox passed"
