#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the Compose smoke test" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required for the Compose smoke test" >&2
  exit 1
fi

bash scripts/generate-dev-certs.sh certs/dev

project="biubin-compose-smoke-${RANDOM}"
compose=(docker compose -f docker-compose.yml -f docker-compose.smoke.yml -p "$project")

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

"${compose[@]}" up --build --detach

ready=0
for _ in {1..60}; do
  if curl --fail --silent --show-error --max-time 3 \
    http://127.0.0.1:8080/readyz >/dev/null; then
    ready=1
    break
  fi
  sleep 2
done

if [[ "$ready" -ne 1 ]]; then
  echo "Compose service did not become ready" >&2
  "${compose[@]}" logs --no-color biubin >&2 || true
  exit 1
fi

container_id="$("${compose[@]}" ps -q biubin)"
healthy=0
health_status="starting"
for _ in {1..30}; do
  health_status="$(docker inspect --format '{{.State.Health.Status}}' "$container_id")"
  if [[ "$health_status" == "healthy" ]]; then
    healthy=1
    break
  fi
  if [[ "$health_status" == "unhealthy" ]]; then
    break
  fi
  sleep 1
done

if [[ "$healthy" -ne 1 ]]; then
  echo "Compose container health status is $health_status" >&2
  "${compose[@]}" logs --no-color biubin >&2 || true
  exit 1
fi

curl --fail --silent --show-error http://127.0.0.1:8080/healthz >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/ >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/openapi >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/openapi.json >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/image/png >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/video/mp4 >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/audio/wav >/dev/null

range_headers="$(curl --silent --show-error --max-time 5 \
  -H 'Range: bytes=0-31' -D - -o /dev/null \
  http://127.0.0.1:8080/image/png)"
if ! printf '%s\n' "$range_headers" | rg -q '^HTTP/[^ ]+ 206 '; then
  echo "image range request did not return 206" >&2
  exit 1
fi

tls_output="$(openssl s_client -connect 127.0.0.1:9001 \
  -CAfile certs/dev/ca.pem -verify_return_error -brief </dev/null 2>&1 || true)"
if ! printf '%s\n' "$tls_output" | rg -q 'Verification: OK'; then
  echo "gRPC TLS listener did not complete certificate verification" >&2
  printf '%s\n' "$tls_output" >&2
  exit 1
fi

if ! bash -c ': > /dev/tcp/127.0.0.1/1883'; then
  echo "MQTT TCP listener is not accepting connections" >&2
  exit 1
fi

if ! bash -c ': > /dev/tcp/127.0.0.1/8083'; then
  echo "MQTT WebSocket listener is not accepting TCP connections" >&2
  exit 1
fi

echo "Compose smoke passed"
