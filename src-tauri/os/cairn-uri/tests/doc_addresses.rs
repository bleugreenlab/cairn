//! Every concrete cairn:// address the docs, the skills and the agent tool
//! descriptions teach is one the grammar parses.
//!
//! Agents copy addresses out of these texts verbatim, so an address that names
//! nothing in the grammar becomes an `Invalid resource URI` the moment it is
//! followed. Templates are not addresses and are skipped by the few rules in
//! [`is_template`]; a line that says an address is invalid is a counterexample,
//! not a teaching, and is skipped too. `docs/plans/**` records proposals,
//! legacy shapes included, and is not scanned.

use std::fs;
use std::path::{Path, PathBuf};

use cairn_uri::parse_uri;

/// All-caps segments the docs use as stand-ins for a real value.
const PLACEHOLDER_SEGMENTS: &[&str] = &[
    "P",
    "N",
    "PROJ",
    "PROJECT",
    "NUMBER",
    "ISSUE",
    "EXEC",
    "NODE",
    "TASK",
    "THREAD",
    "NAME",
    "ID",
    "SEQ",
    "PROVIDER",
    "CONVERSATION",
];

/// `cairn://` hosts the desktop app's URL handler reserves for itself
/// (`src-tauri/src/deep_links.rs`): links the OS hands the app, not resources.
const APP_LINK_HOSTS: &[&str] = &["auth-callback"];

/// The same delimiters `scan_uris` stops a prose URI at, plus a table pipe.
fn continues_uri(c: char) -> bool {
    !c.is_whitespace()
        && !matches!(
            c,
            ')' | ']' | '}' | '>' | '"' | '\'' | '`' | '<' | '(' | '|' | '\\'
        )
}

const TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', ':', '!', '?', '*'];

/// Whether `candidate` (followed in the text by `next`) is a template rather
/// than a concrete address.
fn is_template(candidate: &str, next: Option<char>) -> bool {
    if matches!(next, Some('<' | '{'))
        || candidate.contains('{')
        || candidate.contains("...")
        || candidate.contains('…')
    {
        return true;
    }
    if candidate.ends_with('/') {
        return true;
    }
    let path = candidate["cairn://".len()..]
        .split(['?', '#'])
        .next()
        .unwrap_or_default();
    path.split('/')
        .any(|segment| PLACEHOLDER_SEGMENTS.contains(&segment))
}

/// The concrete addresses in `text` that do not parse, as `(line, address)`.
fn unparseable(text: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.to_ascii_lowercase().contains("invalid") {
            continue;
        }
        let mut rest = line;
        while let Some(start) = rest.find("cairn://") {
            let tail = &rest[start..];
            let end = tail
                .char_indices()
                .find(|(_, c)| !continues_uri(*c))
                .map(|(at, _)| at)
                .unwrap_or(tail.len());
            let candidate = tail[..end].trim_end_matches(TRAILING_PUNCTUATION);
            let next = tail[end..].chars().next();
            let host = candidate["cairn://".len()..].split(['/', '?']).next();
            let app_link = host.is_some_and(|host| APP_LINK_HOSTS.contains(&host));
            if !app_link && !is_template(candidate, next) && parse_uri(candidate).is_none() {
                found.push((index + 1, candidate.to_string()));
            }
            rest = &tail[end.max(1)..];
        }
    }
    found
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files_under(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn every_taught_cairn_address_parses() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let docs = root.join("docs");
    let plans = docs.join("plans");
    let skills = root.join(".cairn/skills");
    let mcp_server = root.join("src-tauri/os/cairn-mcp-server/src");

    let mut files = Vec::new();
    let mut doc_files = Vec::new();
    files_under(&docs, &mut doc_files);
    files.extend(
        doc_files
            .into_iter()
            .filter(|path| !path.starts_with(&plans))
            .filter(|path| path.extension().is_some_and(|ext| ext == "md")),
    );
    files_under(&skills, &mut files);
    // The tool descriptions and input-schema docs every agent is handed.
    files.push(mcp_server.join("server.rs"));
    files.push(mcp_server.join("schemas.rs"));
    files.sort();
    assert!(
        files.len() > 100,
        "scanned only {} files; is the repository root {}?",
        files.len(),
        root.display()
    );

    let mut failures = Vec::new();
    for file in &files {
        let Ok(text) = fs::read_to_string(file) else {
            continue;
        };
        let shown = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .display()
            .to_string();
        for (line, address) in unparseable(&text) {
            failures.push(format!("  {shown}:{line}: {address}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} cairn:// address(es) in docs, skills or tool descriptions do not parse. \
         Fix each address, or add the kind through cairn-uri (src-tauri/os/cairn-uri) \
         if the resource is real:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
