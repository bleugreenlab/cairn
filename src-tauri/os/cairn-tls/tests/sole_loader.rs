//! No crate beside this one loads the platform root store, or builds a TLS
//! client that would load it again: every reqwest client and `wss://` dial in
//! `os/` starts from `cairn_tls`. A source scan, because the failure it guards
//! against compiles and passes every functional test; it only shows up as a
//! runtime worker parked in the OS trust store on a loaded host.

use std::path::{Path, PathBuf};

/// What may not appear outside this crate, and what to use instead.
const FORBIDDEN: &[(&str, &str)] = &[
    (
        "rustls_native_certs",
        "the platform root store is loaded by cairn-tls alone",
    ),
    ("Client::new()", "use cairn_tls::client()"),
    (
        "Client::builder()",
        "use cairn_tls::client_builder() or blocking_client_builder()",
    ),
    ("ClientBuilder::new()", "use cairn_tls::client_builder()"),
    ("reqwest::get(", "use cairn_tls::client().get(..)"),
    (
        "reqwest::blocking::get(",
        "use cairn_tls::blocking_client_builder()",
    ),
    (
        "connect_async(",
        "dial with connect_async_tls_with_config and cairn_tls::websocket_config()",
    ),
    (
        "connect_async_with_config(",
        "dial with connect_async_tls_with_config and cairn_tls::websocket_config()",
    ),
];

/// Crates the scan skips, and why.
const EXEMPT: &[(&str, &str)] = &[
    ("cairn-tls", "the one loader"),
    (
        "cairn-cmd",
        "the CLI is its own process and trusts reqwest's compiled-in webpki roots, asking the OS nothing",
    ),
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Whether `needle` occurs in `line` as its own token: the character before it
/// is not part of an identifier, so `MockHttpClient::new()` is not `Client::new()`.
fn contains_token(line: &str, needle: &str) -> bool {
    line.match_indices(needle).any(|(at, _)| {
        line[..at]
            .chars()
            .next_back()
            .is_none_or(|before| !(before.is_alphanumeric() || before == '_'))
    })
}

#[test]
fn only_cairn_tls_loads_the_platform_root_store() {
    let os = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cairn-tls sits under os/");
    let mut crates: Vec<PathBuf> = std::fs::read_dir(os)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.join("Cargo.toml").is_file())
        .collect();
    crates.sort();
    assert!(
        crates.len() > 10,
        "found only {} crates under {}",
        crates.len(),
        os.display()
    );

    let mut findings = Vec::new();
    for krate in &crates {
        let name = krate.file_name().unwrap().to_string_lossy();
        if EXEMPT.iter().any(|(exempt, _)| *exempt == name) {
            continue;
        }
        let mut files = Vec::new();
        rust_files(krate, &mut files);
        files.sort();
        for file in files {
            let text = std::fs::read_to_string(&file).unwrap();
            for (number, line) in text.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                for (needle, instead) in FORBIDDEN {
                    if contains_token(line, needle) {
                        findings.push(format!(
                            "{}:{}: `{needle}`: {instead}",
                            file.strip_prefix(os).unwrap_or(&file).display(),
                            number + 1
                        ));
                    }
                }
            }
        }
    }
    assert!(
        findings.is_empty(),
        "TLS clients built outside cairn-tls:\n  {}",
        findings.join("\n  ")
    );
}
