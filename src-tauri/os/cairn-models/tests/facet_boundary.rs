//! The public cairn-models facet reaches neither the runtime nor the private crate.
#[path = "support/facet_graph.rs"]
mod facet_graph;

#[test]
fn the_public_models_facet_keeps_its_boundary() {
    facet_graph::assert_public_facet(env!("CARGO_MANIFEST_DIR"));
}

/// Synthetic `cargo metadata` for a workspace: `(name, manifest dir)` members
/// and `(from, to, kind)` edges, where kind is `None` for normal or `"dev"`.
fn metadata(
    workspace_root: &str,
    members: &[(&str, &str)],
    edges: &[(&str, &str, Option<&str>)],
) -> serde_json::Value {
    let id = |name: &str| format!("path+file://{name}");
    serde_json::json!({
        "workspace_root": workspace_root,
        "workspace_members": members.iter().map(|(name, _)| id(name)).collect::<Vec<_>>(),
        "packages": members.iter().map(|(name, dir)| serde_json::json!({
            "id": id(name),
            "name": name,
            "manifest_path": format!("{dir}/Cargo.toml"),
        })).collect::<Vec<_>>(),
        "resolve": { "nodes": members.iter().map(|(name, _)| serde_json::json!({
            "id": id(name),
            "deps": edges.iter().filter(|(from, _, _)| from == name).map(|(_, to, kind)| serde_json::json!({
                "pkg": id(to),
                "dep_kinds": [{ "kind": kind }],
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>() },
    })
}

const PUBLIC_EDGES: &[(&str, &str, Option<&str>)] = &[
    ("cairn-models", "cairn-llm-wire", None),
    ("cairn-models", "cairn-image-wire", None),
    ("cairn-models", "cairn-speech-wire", None),
    ("cairn-models", "cairn-models-types", None),
    ("cairn-models", "cairn-credentials", None),
    ("cairn-credentials", "cairn-models-types", None),
    ("cairn-credentials", "cairn-domain", None),
];

fn members_under(root: &str) -> Vec<(&'static str, String)> {
    [
        "cairn-models",
        "cairn-llm-wire",
        "cairn-image-wire",
        "cairn-speech-wire",
        "cairn-models-types",
        "cairn-credentials",
        "cairn-domain",
        "cairn-core",
    ]
    .into_iter()
    .map(|name| (name, format!("{root}/{name}")))
    .collect()
}

fn violations(
    workspace_root: &str,
    members: &[(&str, String)],
    edges: &[(&str, &str, Option<&str>)],
) -> Vec<String> {
    let members: Vec<(&str, &str)> = members.iter().map(|(n, d)| (*n, d.as_str())).collect();
    facet_graph::Graph::from_metadata(&metadata(workspace_root, &members, edges))
        .public_violations()
}

/// The public mirror preserves the repository's paths: its workspace root is
/// `src-tauri/Cargo.toml` and every crate sits under `src-tauri/os/`, with no
/// runtime crate beside them. Ordinary facet edges must pass there, and the
/// root above `os/` is not mistaken for the public source root: a crate placed
/// beside `os/` is outside it.
#[test]
fn the_mirror_layout_is_public_and_confined_to_os() {
    let mut members = members_under("/mirror/src-tauri/os");
    assert_eq!(
        violations("/mirror/src-tauri", &members, PUBLIC_EDGES),
        Vec::<String>::new()
    );
    members.push(("stray", "/mirror/src-tauri/stray".to_string()));
    let mut edges = PUBLIC_EDGES.to_vec();
    edges.push(("cairn-credentials", "stray", None));
    let found = violations("/mirror/src-tauri", &members, &edges);
    assert!(
        found
            .iter()
            .any(|v| v.contains("stray, which lives outside os/")),
        "{found:?}"
    );
}

/// In the mirror, rule 4 still has members to check: an `os/` crate (here
/// `cairn-core`, by a dev edge) reaching the private crate is refused.
#[test]
fn the_mirror_layout_still_guards_the_private_crate() {
    let mut members = members_under("/mirror/src-tauri/os");
    members.push((
        "cairn-models-subscriptions",
        "/elsewhere/cairn-models-subscriptions".to_string(),
    ));
    let mut edges = PUBLIC_EDGES.to_vec();
    edges.push(("cairn-core", "cairn-models-subscriptions", Some("dev")));
    let found = violations("/mirror/src-tauri", &members, &edges);
    assert!(
        found
            .iter()
            .any(|v| v.starts_with("cairn-core reaches cairn-models-subscriptions")),
        "{found:?}"
    );
}

/// The monorepo layout: public crates under `src-tauri/os/`, a runtime crate
/// beside them in `src-tauri/`. Reaching it is refused as outside the public
/// source root; staying inside passes.
#[test]
fn the_monorepo_layout_confines_the_public_closure_to_os() {
    let mut members = members_under("/repo/src-tauri/os");
    assert_eq!(
        violations("/repo/src-tauri", &members, PUBLIC_EDGES),
        Vec::<String>::new()
    );
    members.push(("cairn-orchish", "/repo/src-tauri/cairn-orchish".to_string()));
    let mut edges = PUBLIC_EDGES.to_vec();
    edges.push(("cairn-credentials", "cairn-orchish", None));
    let found = violations("/repo/src-tauri", &members, &edges);
    assert!(
        found
            .iter()
            .any(|v| v.contains("cairn-orchish, which lives outside os/")),
        "{found:?}"
    );
}

/// The CLI stream wires live in `cairn-llm-wire` only because it stays a leaf:
/// the Claude and Codex readers must never pull in the session runtime that
/// consumes them, nor the database, not even from a test fixture. A root dev
/// edge counts, so a fixture that reaches for `cairn-db` is refused like a
/// normal dependency would be.
#[test]
fn a_wire_leaf_reaching_the_session_runtime_is_refused() {
    let mut members = members_under("/repo/src-tauri/os");
    members.push((
        "cairn-session",
        "/repo/src-tauri/os/cairn-session".to_string(),
    ));
    members.push(("cairn-db", "/repo/src-tauri/os/cairn-db".to_string()));

    let mut edges = PUBLIC_EDGES.to_vec();
    edges.push(("cairn-llm-wire", "cairn-session", None));
    let found = violations("/repo/src-tauri", &members, &edges);
    assert!(
        found
            .iter()
            .any(|v| v.starts_with("cairn-llm-wire reaches cairn-session")),
        "{found:?}"
    );
    assert!(
        found.iter().any(|v| v.starts_with(
            "cairn-llm-wire must reach no workspace crate, but reaches cairn-session"
        )),
        "{found:?}"
    );

    let mut edges = PUBLIC_EDGES.to_vec();
    edges.push(("cairn-llm-wire", "cairn-db", Some("dev")));
    let found = violations("/repo/src-tauri", &members, &edges);
    assert!(
        found
            .iter()
            .any(|v| v.starts_with("cairn-llm-wire reaches cairn-db")),
        "{found:?}"
    );
    assert!(
        found
            .iter()
            .any(|v| v
                .starts_with("cairn-llm-wire must reach no workspace crate, but reaches cairn-db")),
        "{found:?}"
    );
}
