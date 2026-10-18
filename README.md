# Cairn

The Rust crates of [Cairn](https://cairn.computer), an agent orchestration
engine, published from Cairn's private source repository.

This repository is a path-preserving subset of that one. It holds every crate
under `src-tauri/os/`, and, at their original paths, exactly the files outside
`os/` those crates compile in: database migrations, output schemas, bundled
recipes and routes, two frontend action manifests, and test fixtures. Because
the paths are the source repository's own, every crate builds here unchanged.
The workspace root at `src-tauri/Cargo.toml` and its `Cargo.lock` are generated
for this repository: every crate under `src-tauri/os/` is a member, pinned to
the dependency versions Cairn ships.

## Crates

**Engine**

| Crate | Purpose |
|---|---|
| `cairn-core` | Orchestration engine: recipe-based DAG execution, agent backend processes, configuration resolution, memory capture, and the business logic, with no Tauri dependency |
| `cairn-workflow` | Durable recipe advancement, effect processing, and workflow persistence |
| `cairn-session` | Agent session runtime and provider process protocols |
| `cairn-attention` | Durable agent attention, wake subscriptions, and delivery policy |
| `cairn-db` | Database records, domain conversions, and Turso + Tantivy storage |
| `cairn-domain` | Domain values, serialization, and pure transformations |
| `cairn-config` | Configuration files, defaults, validation, and preset resolution |
| `cairn-packs` | Pack schemas, ownership, and filesystem installation |
| `cairn-channels` | Channel message contracts and Telegram/Discord transport adapters |
| `cairn-analytics` | Agent-economics SQL aggregates, pricing, and tool extraction |
| `cairn-mcp-server` | The MCP server: the read/write/run tool surface over stdio and streamable HTTP |
| `cairn-cmd` | The `cairn` command-line helper agents and scripts use |
| `cairn-common` | Shared types: the `cairn://` URI parser, the resource contract, authentication, and protocol types |

**The models facet**, hostable outside Cairn

| Crate | Purpose |
|---|---|
| `cairn-models` | HTTP protocols, provider vocabulary, accounts and routing, and usage as data |
| `cairn-llm-wire` | Model wires: HTTP protocol families with generation assembly and replay, and the Claude and Codex CLI streams |
| `cairn-image-wire` | Image wires: OpenRouter's Image API and the OpenAI Images API, with image sniffing |
| `cairn-speech-wire` | Speech wires: ElevenLabs' stream-input WebSocket and OpenAI-compatible `audio/speech` |
| `cairn-models-types` | Facet vocabulary: provider ids, the provider catalog, usage windows and snapshots, pooled capacity |
| `cairn-credentials` | Provider accounts, credential storage, leases, and selection |

Subscription mechanics (CLI sign-in, account-home placement, token refresh) are
not published; `cairn-models` defines the `Subscriptions` seam a host implements.

**Utilities**

| Crate | Purpose |
|---|---|
| `cairn-codec` | Tagged event-payload compression, and the in-process git object, packfile, and diff substrate |
| `cairn-symbols` | Structural code intelligence over ast-grep |
| `ast-grep-outline` | Code outline model for ast-grep (a vendored subset) |
| `cairn-tokenize` | Offline Claude token-count estimator derived from ctok |
| `cairn-vcs` | Repository operations and publication transactions |
| `cairn-cell` | Shared build-cell policy and filesystem primitives |
| `cairn-sandbox` | OS-level filesystem sandbox applied where commands are spawned |
| `cairn-platform` | Filesystem and process support shared by Cairn hosts |
| `cairn-checkenv` | Check execution provenance and source-sensitive implementation identity |

## Using these crates

Depend on a crate by git, pinned to a commit or a release tag:

```toml
[dependencies]
cairn-models = { git = "https://github.com/bleugreenlab/cairn", rev = "<commit>" }
```

Cargo finds the crate by name anywhere in the repository. Copying
`src-tauri/Cargo.lock` into your own workspace pins the versions Cairn builds
against; Cargo prunes the copy to what you use.

To build everything here:

```bash
cargo check --manifest-path src-tauri/Cargo.toml --workspace
```

Every library, binary, and test builds, except `cairn-core`'s tests: its fleet
tests drive Cairn's private executor runtime, and some of its unit tests read
repository configuration that is not published.

## Documentation

- [URI System](src-tauri/os/docs/uri-system.md): `cairn://` resource addressing
  and parser internals.
- [`src-tauri/os/docs/archive/`](src-tauri/os/docs/archive) holds superseded
  design documents, kept for history and labeled as retired.

Cairn's engineering documentation lives in the private repository and is not
published here.

## How this repository is updated

It is mirrored from the private repository's `main` branch on every change to
the published crates or the files they compile in, so it trails `main` by one
sync. Each commit's `Source-Commit:` trailer names the private commit it was
assembled from. Release tags (`v*`) mark the sources of each desktop release.

## License

The crates are licensed under the Business Source License 1.1. The Licensor is
Bleugreen Lab LLC.

The Additional Use Grant lets you use the code for any purpose, including in
production, except to provide an Agent Orchestration Service: a product,
service, or platform that uses the code to offer agent orchestration to third
parties other than your own employees and contractors. On the Change Date,
**2029-03-22**, or on the fourth anniversary of a version's first public
distribution if that comes first, the code becomes available under the Apache
License, Version 2.0.

The legal text is each crate's `LICENSE` file, and [LICENSE](LICENSE) for the
repository as a whole. Two exceptions are MIT-licensed: `ast-grep-outline`, and
the portion of `cairn-tokenize` derived from ctok (`LICENSE-ctok`). `cairn-toks`
links the prebuilt toks library, which is Apache-2.0; its `LICENSE` and `NOTICE`
ship beside the crate as `LICENSE-toks` and `NOTICE-toks`.
