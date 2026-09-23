# Repository Guidelines

## Project Structure & Module Organization

- `crates/core/` contains shared configuration, readiness, limits, and event storage.
- `crates/app/` contains the executable, protocol adapters, HTTP fixtures, integration tests, and smoke clients.
- `web/` is the Svelte/Vite control plane; browser tests live in `web/tests/` and static assets in `web/public/`.
- `proto/` and `thrift/` are the gRPC and Thrift contracts; `scripts/` contains black-box, Compose, certificate, fixture, and compatibility checks.
- `docs/` holds technical details and the compatibility matrix. Keep `README.md` focused on usage and quick starts.

## Build, Test, and Development Commands

Run commands from the repository root. The Rust toolchain is pinned in `rust-toolchain.toml`.

```sh
cargo run --bin biubin -p biubin                 # run locally
cargo fmt --all -- --check                       # formatting
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked                  # Rust tests
bash scripts/blackbox.sh                         # protocol smoke suite
node scripts/check-http-route-contract.mjs       # route/OpenAPI/Vite consistency
(cd web && npm ci && npm run check && npm run build)
bash scripts/compose-smoke.sh                    # Docker delivery smoke
```

For browser coverage, run `cd web && npx playwright install chromium && npm run test:e2e`; Playwright starts the fixture automatically. External-client checks require the tools described in `docs/technical-details.md`.

## Coding Style & Naming Conventions

Use `rustfmt` and pass Clippy with warnings denied. Rust modules, functions, and variables use `snake_case`; types and Svelte components use `PascalCase`; constants use `SCREAMING_SNAKE_CASE`. Match the existing two-space TypeScript/Svelte style. Keep protocol behavior deterministic and update OpenAPI/capability metadata and the Vite proxy contract when adding HTTP routes.

## Testing Guidelines

Put Rust unit tests beside implementation code, cross-module tests in `crates/app/tests/`, and browser scenarios in `web/tests/` with behavior-focused names. No coverage threshold is configured; protocol changes should add or update a focused smoke test. Run the smallest relevant check locally, then the full checks before submission.

## Commit & Pull Request Guidelines

Use concise Conventional Commit subjects seen in history, such as `feat:`, `fix:`, `test:`, `docs:`, `ci:`, or `refactor:`; keep them imperative and scoped to one change. Pull requests should explain behavior, list validation commands, call out port/configuration changes, and include screenshots for frontend changes. Update docs when public behavior changes.

## Security & Configuration Tips

The fixture binds to loopback by default. Treat `0.0.0.0`, default MQTT credentials, and generated development certificates as local-test settings only. Keep certificate files mode `600`, do not expose the internal MQTT TLS backend port `1886`, and avoid committing generated files under `certs/dev/`.
