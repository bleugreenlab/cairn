//! Project upstream ctok's v4.7 vocabulary into the reduced form this crate
//! embeds, and check that the embedded copy is still exactly that projection.
//!
//! ```text
//! cargo run --example vocab -- /path/to/ctok            # check
//! cargo run --example vocab -- /path/to/ctok --write    # repin
//! ```
//!
//! The reduction keeps the v4.7 piece strings, the byte-prefix keys and the
//! contraction suffixes, and drops the witness metadata that pins each piece to
//! the probe it was measured with. The check compares the three sets
//! independently, so a difference in one of them cannot hide inside a matching
//! total, and it compares the upstream commit the data claims with the commit
//! the given checkout is on.

use serde_json::{Map, Value};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Command,
};

/// Upstream's own grouping is provenance, not meaning: every group but the byte
/// fallback is a cost-1 piece.
const BYTE_GROUP: &str = "bytes_fallback";
const CONTRACTION_GROUP: &str = "contractions";

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(upstream) = args.next().map(PathBuf::from) else {
        eprintln!("usage: cargo run --example vocab -- <ctok checkout> [--write]");
        std::process::exit(2);
    };
    let write = args.next().as_deref() == Some("--write");

    let (pieces_path, commit) = if upstream.is_dir() {
        (
            upstream.join("ctok/data/pieces_v4_7.json"),
            head_commit(&upstream),
        )
    } else {
        (upstream, None)
    };
    let doc: Value = serde_json::from_str(
        &std::fs::read_to_string(&pieces_path).expect("upstream vocabulary is readable"),
    )
    .expect("upstream vocabulary is JSON");
    let reduced = reduce(&doc, commit.as_deref());

    let target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/pieces_v4_7.json");
    if write {
        std::fs::write(&target, render(&reduced)).expect("reduced vocabulary is writable");
        println!("wrote {}", target.display());
    }
    let embedded: Value = serde_json::from_str(
        &std::fs::read_to_string(&target).expect("embedded vocabulary is readable"),
    )
    .expect("embedded vocabulary is JSON");

    let mut failed = false;
    for field in ["pieces", "bytes", "contractions"] {
        let want = set(&reduced, field);
        let have = set(&embedded, field);
        let missing: Vec<_> = want.difference(&have).collect();
        let extra: Vec<_> = have.difference(&want).collect();
        failed |= !missing.is_empty() || !extra.is_empty();
        println!("{field}: {} upstream, {} embedded", want.len(), have.len());
        for (label, entries) in [("missing", &missing), ("unknown", &extra)] {
            if !entries.is_empty() {
                println!("  {label} ({}): {entries:?}", entries.len());
            }
        }
    }
    for piece in set(&embedded, "pieces") {
        // `parse` decodes the named marker atoms only. A byte-escaped bracket
        // would silently tile as its literal characters.
        if piece.contains("\u{27e8}0x") || piece.contains("\u{27e8}caps\u{27e9}") {
            println!("  unparseable piece: {piece:?}");
            failed = true;
        }
    }
    let claimed = embedded["meta"]["source_commit"].as_str().unwrap_or("");
    match commit {
        Some(commit) if commit != claimed => {
            println!("source_commit: embedded {claimed}, checkout {commit}");
            failed = true;
        }
        _ => println!("source_commit: {claimed}"),
    }
    if failed {
        std::process::exit(1);
    }
    println!("embedded vocabulary matches the upstream projection");
}

fn head_commit(checkout: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(["-C", checkout.to_str()?, "rev-parse", "HEAD"])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn reduce(doc: &Value, commit: Option<&str>) -> Value {
    let groups = doc["tokens"]
        .as_object()
        .expect("upstream tokens is an object");
    let entries = |group: &str| -> Vec<String> {
        let mut out: Vec<String> = groups
            .get(group)
            .and_then(Value::as_object)
            .map(|g| g.keys().cloned().collect())
            .unwrap_or_default();
        out.sort();
        out
    };
    let mut pieces: Vec<String> = groups
        .iter()
        .filter(|(group, _)| group.as_str() != BYTE_GROUP)
        .flat_map(|(_, group)| {
            group
                .as_object()
                .expect("a group is an object")
                .keys()
                .cloned()
        })
        .collect();
    pieces.sort();

    let mut meta = doc["meta"].as_object().cloned().unwrap_or_else(Map::new);
    meta.remove("witness");
    if let Some(commit) = commit {
        meta.insert("source_commit".into(), Value::from(commit));
    }
    serde_json::json!({
        "meta": meta,
        "pieces": pieces,
        "bytes": entries(BYTE_GROUP),
        "contractions": entries(CONTRACTION_GROUP),
    })
}

fn set(doc: &Value, field: &str) -> BTreeSet<String> {
    doc[field]
        .as_array()
        .expect("a vocabulary field is an array")
        .iter()
        .map(|v| v.as_str().expect("an entry is a string").to_string())
        .collect()
}

/// One entry per line, so a repin's diff is the set difference.
fn render(doc: &Value) -> String {
    let mut out = serde_json::to_string_pretty(doc).expect("reduced vocabulary serializes");
    out.push('\n');
    out
}
