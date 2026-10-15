//! One launch path per provider: only the cairn-models facet reaches a provider.
//!
//! Every HTTP token request goes through `cairn_models::interface::complete::Complete`,
//! whose one real implementation (`cairn_models::complete::HttpComplete`) is the only
//! code that names the protocol families' POSTs. A second caller anywhere in the
//! runtime would be a launch path the facet cannot see, so this scans the
//! runtime's source for one.
//!
//! Crates outside `os/` (the transport, the app, the private subscriptions crate)
//! are scanned when present and skipped in the public mirror, where they are not.

use std::path::{Path, PathBuf};

fn os_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cairn-core lives under os/")
        .to_path_buf()
}

/// Source trees that must not open a second launch path.
fn scanned_roots() -> Vec<PathBuf> {
    let os = os_root();
    let beside_os = os.parent().map(Path::to_path_buf).unwrap_or_default();
    [
        os.join("cairn-core/src"),
        os.join("cairn-session/src"),
        os.join("cairn-models/src"),
        beside_os.join("cairn-transport/src"),
        beside_os.join("cairn-models-subscriptions/src"),
        beside_os.join("src"),
    ]
    .into_iter()
    .filter(|root| root.is_dir())
    .collect()
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read source dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every `(file, line number, line)` whose code (comments stripped) contains
/// one of `needles`, outside `allowed` files.
fn hits(needles: &[&str], allowed: &[&str]) -> Vec<String> {
    let os = os_root();
    let mut found = Vec::new();
    for root in scanned_roots() {
        let mut files = Vec::new();
        rust_files(&root, &mut files);
        for file in files {
            let relative = file
                .strip_prefix(os.parent().unwrap_or(&os))
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            if allowed.iter().any(|allowed| relative.ends_with(allowed)) {
                continue;
            }
            let text = std::fs::read_to_string(&file).expect("read source");
            for (index, line) in text.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }
                // Comments stripped; `//` inside a string literal (a URL) stays.
                let code = match line.find(" // ") {
                    Some(at) => &line[..at],
                    None => line,
                };
                if needles.iter().any(|needle| contains_word(code, needle)) {
                    found.push(format!("{relative}:{}: {}", index + 1, line.trim()));
                }
            }
        }
    }
    found
}

/// `needle` as a whole identifier: not part of a longer name. A needle that
/// is not itself an identifier (a call or a string literal) matches as text.
fn contains_word(code: &str, needle: &str) -> bool {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    if !needle.chars().all(is_ident) {
        return code.contains(needle);
    }
    code.match_indices(needle).any(|(start, _)| {
        let before = code[..start].chars().next_back();
        let after = code[start + needle.len()..].chars().next();
        !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
    })
}

/// The protocol families' POSTs, streamed and whole.
const WIRE_POSTS: &[&str] = &[
    "post_chat_completion",
    "post_chat_completion_streaming",
    "post_message",
    "post_message_streaming",
    "post_response",
    "post_response_streaming",
];

#[test]
fn only_the_facet_posts_to_a_provider() {
    let found = hits(WIRE_POSTS, &["cairn-models/src/complete.rs"]);
    assert!(
        found.is_empty(),
        "a provider POST outside cairn_models::complete; route it through the \
         orchestrator's `models_runtime().complete` instead:\n  {}",
        found.join("\n  ")
    );
}

/// Every place outside the facet allowed to name `needle`, and why. Anything
/// not listed fails.
fn outside_the_facet(needles: &[&str], allowed: &[(&str, &str)]) -> Vec<String> {
    let files: Vec<&str> = allowed.iter().map(|(file, _)| *file).collect();
    hits(needles, &files)
}

/// Starting `codex app-server`.
const APP_SERVER_SPAWNS: &[&str] = &["AppServerClient::spawn", "AppServerClient::launch"];

#[test]
fn only_the_facet_starts_an_app_server_for_a_session() {
    let found = outside_the_facet(
        APP_SERVER_SPAWNS,
        &[
            ("cairn-models/src/codex/app_server.rs", "the client itself"),
            (
                "cairn-models/src/session/codex/server.rs",
                "CodexAppServer::start_server",
            ),
            (
                "cairn-core/src/backends/codex/mod.rs",
                "a test fixture: a scripted app-server process",
            ),
            (
                "cairn-models-subscriptions/src/usage/codex.rs",
                "subscription usage probe, not a session",
            ),
            (
                "cairn-models-subscriptions/src/signin/codex.rs",
                "the sign-in flow, not a session",
            ),
        ],
    );
    assert!(
        found.is_empty(),
        "a Codex app-server started outside the facet; start it through the \
         orchestrator's `models_runtime().codex.start_server` instead:\n  {}",
        found.join("\n  ")
    );
}

/// Finding a CLI to run. A session's process is spawned only by a provider on
/// its host, so resolving a CLI elsewhere is either a check that it exists or
/// a mechanics flow that is not a session.
const CLI_RESOLUTION: &[&str] = &[
    "find_binary(\"claude\")",
    "find_binary(\"codex\")",
    "get_claude_path",
    "claude_path()",
];

#[test]
fn only_the_session_host_resolves_a_cli_to_launch() {
    let found = outside_the_facet(
        CLI_RESOLUTION,
        &[
            (
                "cairn-models/src/session/local.rs",
                "LocalSessionHost::resolve_cli, the launch seam",
            ),
            (
                "cairn-core/src/orchestrator/models.rs",
                "the LocalSessionHost's Claude resolver, the launch seam",
            ),
            (
                "cairn-core/src/orchestrator/session.rs",
                "get_claude_path itself",
            ),
            (
                "cairn-core/src/backends/claude.rs",
                "availability and catalog checks, not launches",
            ),
            (
                "cairn-core/src/backends/codex/backend_impl.rs",
                "the availability check, not a launch",
            ),
            (
                "cairn-core/src/orchestrator/identity.rs",
                "ModelHost::claude_path and codex_path, for subscription mechanics",
            ),
            (
                "cairn-transport/src/routes/invoke/sync_commands/config_family.rs",
                "an installed-CLI check for Settings",
            ),
            ("src/commands/setup.rs", "an installed-CLI check for setup"),
            (
                "cairn-models-subscriptions/src/usage/codex.rs",
                "subscription usage probe",
            ),
            (
                "cairn-models-subscriptions/src/signin/codex.rs",
                "sign-in flow",
            ),
            (
                "cairn-models-subscriptions/src/signin/claude.rs",
                "sign-in flow",
            ),
            (
                "cairn-models-subscriptions/src/claude_home.rs",
                "managed-home sign-in state",
            ),
            (
                "cairn-models/src/subscription.rs",
                "ModelHost::claude_path itself",
            ),
        ],
    );
    assert!(
        found.is_empty(),
        "a CLI resolved outside the session host; launch through the \
         orchestrator's `models_runtime()` providers instead:\n  {}",
        found.join("\n  ")
    );
}

/// A Codex subscription's credential presentation is subscription mechanics:
/// it is built only in the private crate, behind `Subscriptions`.
#[test]
fn only_subscription_support_presents_a_codex_subscription() {
    let found = hits(
        &["\"chatgptAuthTokens\""],
        &["cairn-models-subscriptions/src/codex_refresh.rs"],
    )
    .into_iter()
    .filter(|hit| !hit.starts_with("cairn-models-subscriptions/"))
    .collect::<Vec<_>>();
    assert!(
        found.is_empty(),
        "Codex subscription login params built outside cairn-models-subscriptions; \
         ask `Subscriptions::codex_session_login` instead:\n  {}",
        found.join("\n  ")
    );
}

/// A managed Claude account's refresh token is spent by Cairn's refresh
/// authority alone; the CLIs it launches never hold one. A second caller of the
/// token endpoint would be a second spender, and single-use refresh tokens make
/// two spenders fork or lose the account.
#[test]
fn only_the_claude_refresh_authority_calls_the_token_endpoint() {
    let found = hits(
        &[
            "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
            "platform.claude.com/v1/oauth",
            "console.anthropic.com/v1/oauth",
        ],
        &["cairn-models-subscriptions/src/claude_refresh.rs"],
    );
    assert!(
        found.is_empty(),
        "the Claude OAuth token endpoint named outside the refresh authority; \
         renew through `Subscriptions::renew_claude_credentials` or \
         `claude_credential_rejected` instead:\n  {}",
        found.join("\n  ")
    );
}

#[test]
fn the_scan_covers_the_runtime() {
    let roots = scanned_roots();
    assert!(roots.iter().any(|root| root.ends_with("cairn-core/src")));
    assert!(roots.iter().any(|root| root.ends_with("cairn-models/src")));
    // Not vacuous: the facet's own caller is what the rule allows.
    assert!(!hits(WIRE_POSTS, &[]).is_empty());
}
