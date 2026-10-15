# Working on Tabular with an AI coding agent

This file is read by Claude Code, Codex, Cursor and similar tools. Keep it short and factual.

## What this is

Tabular is a native desktop SQL/NoSQL client in Rust (`eframe`/`egui`), single crate, about
115k lines. Drivers: PostgreSQL, MySQL, SQLite, SQL Server, Redis, MongoDB. It also ships an
MCP server for agents (`tabular mcp`, see `docs/MCP.md`).

## Commands

```bash
cargo check                                     # fast compile check
cargo clippy --all-targets -- -D warnings       # CI gate, must be clean
cargo clippy --all-targets --features collab -- -D warnings
cargo test                                      # unit + integration tests
cargo test --lib agent::                        # only the agent / MCP layer
cargo run -- mcp --print-config                 # MCP config snippet
```

CI runs on ubuntu, macos and windows (`.github/workflows/rust.yml`). `cargo fmt` is not yet
enforced on the whole tree; format only the files you touch.

## Layout

| Area | Path | Notes |
|---|---|---|
| Entrypoint | `src/lib.rs` (`run()`), `src/main.rs` | CLI dispatch happens before eframe starts |
| App state / UI | `src/window_egui/` (`Tabular` struct in `app_impl.rs`) | Most GUI functions take `&mut Tabular` |
| Editor | `src/editor*.rs`, `src/query_tools/` | Statement parser, formatter, lints |
| Connections & execution | `src/connection/` | `pool.rs` builds pools (SSH/TLS), `execute.rs` runs `QueryJob`s |
| Drivers | `src/driver_*.rs` | Per-database metadata fetching |
| Local cache | `connections.db` (SQLite) via `src/sidebar_database.rs`, `src/cache_data.rs` | Tables: `connections`, `table_cache`, `column_cache`, `foreign_key_cache`, `query_history` |
| Secrets | `src/secrets.rs` | OS keychain, encrypted-file fallback |
| AI assistant (in-app) | `src/ai_assistant.rs`, `src/vector_index.rs` | Local feature-hash embeddings via sqlite-vec |
| Agent / MCP layer | `src/agent/` | Headless; must never depend on `window_egui` |
| Sync / collab | `src/sync/` | E2E encrypted vault; `collab` feature is optional |
| Plugins | `src/plugin_runtime/` | Wasm plugins via `wasmi` |
| Import / export / transfer | `src/data_transfer/` (headless), `src/window_egui/transfer_*.rs` (UI) | File formats, encrypted exports, Data Files workspace (SQLite, `data_files.sqlite`), table transfer, data compare; see `docs/DATA_TRANSFER.md` |
| Multi-source comparison | `src/data_transfer/multi_compare*.rs`, `mongo_source.rs` (headless), `src/window_egui/multi_compare_{ui,jobs}.rs` | Sidebar DBs > Comp; N queries on different connections (incl. MongoDB JSON queries), `${var}` substitution, only differing keys shown; runs as a background job (`TaskOwner::Compare`); stored in `multi_compare_saved`; see `docs/DATA_TRANSFER.md`, ADR 0005 |
| Repository setup | `src/repo_setup/` (headless discovery), `src/window_egui/repo_setup_jobs.rs` (apply + background watcher) | Git folder → project, connections from `.env`/compose/framework config, API folder per service, diagram group per service; personal store `repo_setups.json`; see `docs/REPO_SETUP.md`, ADR 0004 |
| Integration testing | `src/http_tests.rs` (model, runner, cleanup, lint), `src/http_test_report.rs` (ISTQB/ISO 29119 stats, HTML/MD/JUnit/JSON), `src/http_test_cli.rs` (`tabular test`), `src/window_egui/integration_test_*.rs` (tab + jobs) | Project-scoped suites of ordered HTTP steps with phases (setup/test/cleanup) and groups; cleanup always runs; see `docs/HTTP_API_REPOSITORY.md`, ADR 0006 |
| File Viewer | `src/file_viewer/` (headless: type detection, language map, image/SVG/PDF decode), `src/syntax_code.rs`, `src/window_egui/file_viewer_{ui,jobs}.rs` | Opens images, SVG, PDF (`pdf` feature, hayro), Markdown, CSV/XLSX (result grid), source code and config in a tab; Cmd+S saves keeping CRLF/BOM; entry points: File > Open File…, drag and drop, Git sidebar > Files; see `docs/FILE_VIEWER.md`, ADR 0007 |
| Git client | `src/git/` (headless), `src/window_egui/git_*.rs` (UI) | Runs the `git` CLI; Merge Review calls GitHub/GitLab REST; repo list merges diagram groups, HTTP API folders, projects and `git_repos.json` by normalized git URL. Sidebar state is per repository (`RepoUi`, keyed by repo key); Git Graph lane layout is pure in `git/graph.rs`, its tab data lives in `GitUiState::graphs` |

## Conventions

- Code comments and doc comments are written in **Bahasa Indonesia**; UI strings and
  user-facing docs are in **English**.
- Errors: `thiserror` enums per domain (`QueryExecutionError`, `AgentError`). Do not
  `unwrap()` on I/O or network paths.
- Logging: `log::{debug,info,warn,error}!` with a bracket tag, e.g. `log::warn!("[AGENT] ...")`.
- Headless code (anything an agent or a test calls) takes plain data (`&ConnectionConfig`,
  `&SqlitePool`), never `&mut Tabular`. See `src/export_import_all.rs::export_all_data_payload`
  and `src/agent/core.rs` for the pattern.
- Tests: unit tests inline under `#[cfg(test)]`; integration tests in `tests/`. SQLite
  in-memory pools are the preferred fixture (see `src/connection/execute.rs` tests).
- Lints allowed globally: `collapsible_if`, `too_many_arguments`, `type_complexity`. Everything
  else must pass `clippy -D warnings`.
- Never serialize `ConnectionConfig` towards an agent or a network peer; it carries secrets.

## Things that bite

- `libsqlite3-sys` is pinned to 0.37 with `bundled`; `sqlite-vec` links against it. Do not
  add a second SQLite.
- `mssql-client` is pre-1.0 and pinned to 0.20.x.
- iOS builds the crate as a `staticlib`; gate anything that needs stdio, processes or
  file dialogs with `#[cfg(not(target_os = "ios"))]`.
- The GUI and `tabular mcp` may run concurrently against the same `connections.db` (WAL).
