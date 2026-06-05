# Repository Guidelines

## Project Structure & Module Organization

This repository contains a Rust backend and a Vue 3 frontend. Backend code lives in `backend/src`, with modules such as `server`, `netdisk`, `downloader`, `uploader`, `transfer`, `autobackup`, and `share_sync`. Backend unit tests are colocated with modules. Frontend code lives in `frontend/src`; use `views` for pages, `components` for reusable UI, `api` for HTTP clients, `stores` for Pinia state, and `utils` or `composables` for shared helpers. Config lives under `config`, scripts under `scripts`, and docs under `docs`.

## Build, Test, and Development Commands

Run backend commands from `backend`:

```bash
cargo run          # start the API server
cargo build        # compile the backend
cargo test         # run Rust tests
cargo clippy       # lint Rust code
cargo fmt          # format Rust code
```

Run frontend commands from `frontend`:

```bash
npm install        # install dependencies
npm run dev        # start Vite dev server
npm run build      # type-check and build production assets
npm run lint       # run ESLint with autofix
npm test           # run Vitest once
```

For Docker development, use `docker-compose.dev.yml` or scripts in `scripts`.

## Coding Style & Naming Conventions

Rust uses edition 2021 and root `rustfmt.toml`: 4-space indentation, 100-column width, grouped imports, and normalized comments. Use `snake_case` for functions, modules, and fields; `PascalCase` for types. Keep async backend code on existing Axum/Tokio patterns and route handlers under `backend/src/server/handlers`.

Frontend code uses Vue SFCs, TypeScript, Element Plus, Pinia, and ESLint. Name components `PascalCase.vue`, API modules by feature such as `shareSync.ts`, and page logic in `views`.

## Testing Guidelines

Add Rust tests near covered code with `#[cfg(test)]` modules and descriptive names such as `diff_detects_added_files`. Run `cargo test` before backend changes and `cargo clippy` for shared or async logic. Frontend tests use Vitest; place tests near relevant components or utilities and run `npm test`. For UI changes, also run `npm run build`.

## Commit & Pull Request Guidelines

Recent history uses short conventional prefixes such as `feat:` and `fix:`, sometimes with Chinese descriptions. Prefer focused commits like `fix: verify share password before preview tree`. PRs should describe the behavior change, list tests run, link issues, and include screenshots or recordings for visible frontend changes.

## Security & Configuration Tips

Do not commit cookies, tokens, Baidu account data, local databases, logs, or downloads. Keep machine-specific values in local config files under `config` and verify ports match `config.app.toml` when changing frontend proxy settings.
