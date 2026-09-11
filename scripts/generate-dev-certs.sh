#!/usr/bin/env bash
set -euo pipefail

cert_dir="${1:-certs/dev}"
mkdir -p "$cert_dir"
umask 077

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$cert_dir/ca-key.pem" \
  -out "$cert_dir/ca.pem" \
  -days 3650 \
  -subj "/CN=biubin development CA" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:1" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -newkey rsa:2048 -nodes \
  -keyout "$cert_dir/server-key.pem" \
  -out "$tmp_dir/server.csr" \
  -subj "/CN=localhost"
cat > "$tmp_dir/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req \
  -in "$tmp_dir/server.csr" \
  -CA "$cert_dir/ca.pem" \
  -CAkey "$cert_dir/ca-key.pem" \
  -CAcreateserial \
  -out "$cert_dir/server.pem" \
  -days 825 \
  -extfile "$tmp_dir/server.ext"

openssl req -newkey rsa:2048 -nodes \
  -keyout "$cert_dir/client-key.pem" \
  -out "$tmp_dir/client.csr" \
  -subj "/CN=biubin test client"
cat > "$tmp_dir/client.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl x509 -req \
  -in "$tmp_dir/client.csr" \
  -CA "$cert_dir/ca.pem" \
  -CAkey "$cert_dir/ca-key.pem" \
  -CAcreateserial \
  -out "$cert_dir/client.pem" \
  -days 825 \
  -extfile "$tmp_dir/client.ext"

chmod 600 "$cert_dir"/*-key.pem
echo "Generated development certificates in $cert_dir"

