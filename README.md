# Cairn

Agent orchestration engine in Rust.

## Crates

**cairn-core** — Orchestration engine. Recipe-based DAG execution, Claude process management, Turso-backed database operations, configuration resolution, memory system, and all business logic.

**cairn-common** — Shared types. `cairn://` URI parser and serializer, authentication, and the callback protocol types that connect cairn-cli to cairn-core.

**cairn-cli** — MCP server binary. Provides the tool interface agents use during execution: file I/O, shell commands, sub-agent spawning, memory management, and execution history navigation. Stateless — all operations are forwarded to cairn-core via HTTP callbacks.

## Building

```bash
cargo build
```

## Documentation

- [URI System](docs/uri-system.md) — `cairn://` resource addressing
- [Execution Engine](docs/dag-engine.md) — recipes, DAG advancement, executor expansion
- [Interaction Model](docs/interaction-model.md) — session lifecycle, callback architecture, process management
- [Memory System](docs/memory-system.md) — trigger-based knowledge surfacing

## License

Business Source License 1.1, converting to Apache 2.0 on 2030-03-14. See [LICENSE](LICENSE).

[cairn.computer](https://cairn.computer)
