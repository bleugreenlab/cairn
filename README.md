# Cairn

Agent orchestration engine in Rust.

## Crates

**cairn-core** — Orchestration engine. Recipe-based DAG execution, agent backend process management, Turso-backed database operations, configuration resolution, memory capture, and all business logic.

**cairn-common** — Shared types. `cairn://` URI parser and serializer, authentication, and the callback protocol types that connect cairn-cmd to cairn-core.

**cairn-cmd** — MCP server binary. Provides the tool interface agents use during execution: file I/O, shell commands, sub-agent spawning, memory management, and execution history navigation. Stateless — all operations are forwarded to cairn-core via HTTP callbacks.

## Building

```bash
cargo build
```

## Documentation

Engineering documentation for the system lives in the repository's top-level
`docs/` directory, which is the current source of truth. The most relevant here:

- [State machines](../../docs/state-machines.md) — run, turn, job, execution, and issue status
- [Execution lifecycle map](../../docs/execution-lifecycle-map.md) — the full control-flow map
- [Recipe execution](../../docs/recipe-execution.md) — recipes, node types, and DAG advancement
- [Sessions](../../docs/sessions.md) — session lifecycle and warm-process retention
- [Backends](../../docs/backends.md) — Claude, Codex, and OpenRouter process management
- [Memories](../../docs/memories.md) — the memory capture and triage ledger

This crate keeps one document of its own:

- [URI System](docs/uri-system.md) — `cairn://` resource addressing and parser internals

[`docs/archive/`](docs/archive) holds superseded design documents. They are kept
for history, are labeled as retired, and are not current guidance.

## License

Business Source License 1.1, converting to Apache 2.0 on 2030-03-14. See [LICENSE](LICENSE).

[cairn.computer](https://cairn.computer)
