#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
address="${1:-127.0.0.1:9090}"
generated_dir="$(mktemp -d)"

cleanup() {
    rm -rf "$generated_dir"
}
trap cleanup EXIT INT TERM

thrift --gen js:node -out "$generated_dir" "$repo_dir/thrift/biubin.thrift"
NODE_PATH="$repo_dir/web/node_modules" \
    node "$repo_dir/scripts/thrift_cross_language_smoke.cjs" "$address" "$generated_dir"
