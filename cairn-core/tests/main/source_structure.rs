use std::path::PathBuf;

fn core_source(relative: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(root.join("src").join(relative))
        .unwrap_or_else(|error| panic!("failed to read core source {relative}: {error}"))
}

/// Every source and doc file the promotion ban walks: the whole Rust workspace
/// and the design docs, minus build output and the memory-triage archive (a
/// historical record of what agents once observed, not current canon).
fn banned_symbol_search_roots() -> Vec<PathBuf> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root resolves from the crate manifest");
    vec![repo.join("src-tauri"), repo.join("docs")]
}

fn walk_source_files(root: &std::path::Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if matches!(name.as_ref(), "target" | "node_modules" | "memory-triage") {
                continue;
            }
            walk_source_files(&path, found);
        } else if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "md")
        ) {
            found.push(path);
        }
    }
}

/// Promotion is retired: a `run` item that outlives its bound is killed, and a
/// batch that outlives its grace window suspends and resumes with its finished
/// result. Reclassifying a live child into a terminal is what made publication
/// depend on elapsed time — the same command with the same `commit_msg`
/// committed if it finished fast and was silently voided if it did not. These
/// symbols named that machinery; a reappearance is the defect returning, so the
/// ban is asserted structurally rather than left to review.
#[test]
fn timeout_promotion_never_returns() {
    const BANNED: &[&str] = &[
        "PromotedTerminalProcess",
        "promote_command_process",
        "promote_timeouts",
        "promote_on_timeout",
        "ActivatePromotedTerminal",
        "activate_promoted_executor_terminal",
        "build_promoted_terminal_uri",
        "subscribe_promoted_terminal_exit_wake",
        "command-promoted",
        "promoted_terminal",
    ];
    let this_file = PathBuf::from(file!())
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .expect("this test file has a name");

    let mut files = Vec::new();
    for root in banned_symbol_search_roots() {
        walk_source_files(&root, &mut files);
    }
    assert!(
        files.len() > 100,
        "the promotion ban walked too few files ({}) to be meaningful",
        files.len()
    );

    let mut offenders = Vec::new();
    for file in files {
        if file.file_name().is_some_and(|name| name == &*this_file) {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };
        for symbol in BANNED {
            if source.contains(symbol) {
                offenders.push(format!("{} contains {symbol}", file.display()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "timeout promotion reappeared:\n{}",
        offenders.join("\n")
    );
}

/// `jj::seal` resolves a workspace's branch ownership by reading
/// `.jj/cairn-branch` AT CALL TIME. That is sound for a fixture which provisions a
/// workspace and seals it in the same breath, and unsound for the commit barrier,
/// whose seal runs after a batch that can write that very file: a batch which
/// deleted the marker would get a locally committed, UNPUBLISHED seal reported to
/// it as a successful commit on its branch, and one which planted a marker would
/// opt a checkout Cairn does not own into Cairn publication.
///
/// The VCS backend therefore captures ownership when it is constructed during
/// request preflight and passes it to `jj::seal_paths`. Nothing in the type system
/// separates the two entry points — `jj::seal` is `pub`, and reaching for it here
/// would compile and silently reintroduce the defect — so the production edge is
/// banned structurally rather than left to a doc comment and review.
#[test]
fn the_vcs_backend_never_resolves_seal_ownership_at_call_time() {
    // `jj::seal_paths(` does not contain `jj::seal(`, so the ban names the
    // marker-reading entry point exactly, with no carve-out for the file's own
    // fixtures: they pass their branch explicitly too.
    assert_absent("mcp/vcs.rs", &["jj::seal("]);
}

/// Every Rust file in the workspace, paired with its source.
fn workspace_rust_files() -> Vec<(PathBuf, String)> {
    let mut paths = Vec::new();
    for root in banned_symbol_search_roots() {
        walk_source_files(&root, &mut paths);
    }
    paths
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("rs"))
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path).ok()?;
            Some((path, source))
        })
        .collect()
}

/// Assert a needle appears nowhere in workspace Rust outside an allowlist of
/// files, ignoring test modules.
///
/// This file is always exempt: a ban has to be able to spell what it bans.
fn assert_only_in(needles: &[&str], allowed: &[&str], why: &str) {
    let mut offenders = Vec::new();
    for (path, source) in workspace_rust_files() {
        let display = path.to_string_lossy().into_owned();
        if display.ends_with("tests/main/source_structure.rs")
            || allowed.iter().any(|allow| display.ends_with(allow))
        {
            continue;
        }
        for (line_number, line) in non_test_lines(&source) {
            for needle in needles {
                if line.contains(needle) {
                    offenders.push(format!("{display}:{line_number} ({needle})"));
                }
            }
        }
    }
    assert!(offenders.is_empty(), "{why}: {offenders:?}");
}

/// Credential stores are reachable only through the broker.
///
/// This is the structural half of "no direct store accessor remains on migrated
/// paths". Before the broker, any code that wanted a configured MCP token
/// called the keychain accessor and got a `String` — which is `Debug`,
/// `Serialize`, and unregistered. Two such call sites existed with *different*
/// resolution orders, so what a `${VAR}` meant depended on which one you
/// reached, and only one of them registered anything.
///
/// A single-caller rule is what makes "registered before it is returned"
/// checkable at all: with one producer it is one function's postcondition, and
/// with N producers it is a review convention.
#[test]
fn credential_stores_are_reached_only_through_the_broker() {
    assert_only_in(
        &[
            "secrets::get_secret(",
            "store::get_valid_access_token(",
            "credentials::app_signing_key(",
        ],
        &["security/broker.rs"],
        "credential stores must be read only by the broker, which registers what it resolves",
    );
}

/// The GitHub App's private key is signed with in exactly one place.
///
/// This is the structural half of "provider operations are broker-performed".
/// The key authenticates Cairn *as the application*: one signature mints a
/// token for any repository the app is installed on, it never expires, and
/// rotating it means generating a new key on the app. It used to be passed by
/// reference through the GitHub API client, and model-callable handlers were
/// among the callers.
///
/// A second signing site would be a second place the key becomes a live `&str`,
/// which is precisely the shape that made the old arrangement unsafe — so the
/// ban names the RSA constructor rather than trusting review to notice.
#[test]
fn the_github_app_key_is_signed_with_only_in_the_broker() {
    assert_only_in(
        &["EncodingKey::from_rsa_pem"],
        &["security/broker.rs"],
        "the GitHub App private key must be signed with only inside the broker",
    );
}

/// Credential plaintext is unwrapped only where it is injected.
///
/// `BrokeredSecret::expose` and `Presented::expose` are the two ways brokered
/// plaintext becomes a `&str`. Everything else in the subsystem — the carriers,
/// the leases, the receipts — exists so that this list stays short and so that
/// every entry on it is a *transport boundary*: an HTTP header, a request body,
/// a child process's environment, a file only the operator can read.
///
/// The list is the reveal surface, and it is the checkable form of "model
/// principals have no reveal API": no tool handler that answers a
/// model-originated call appears on it. `mcp/handlers/search_web.rs` and
/// `fetch_web.rs` are reached *from* such a call, but what they expose is the
/// provider key going out on the wire in Cairn's own request — the credential
/// never enters the result the model reads.
#[test]
fn brokered_plaintext_is_unwrapped_only_where_it_is_injected() {
    assert_only_in(
        &[".expose()"],
        &[
            // The broker itself, and the lease it hands out.
            "security/broker.rs",
            "security/lease.rs",
            // Cairn's own outbound requests.
            "cairn-transport/src/mcp_gateway.rs",
            "mcp/handlers/search_web.rs",
            "mcp/handlers/fetch_web.rs",
            "account/team_token_minter.rs",
            // Channel provider clients inject credentials into their SDK transports.
            "channels/mod.rs",
            // The runner's own callback credential, which predates the broker
            // and is minted rather than resolved from a store.
            "mcp/auth.rs",
            "cairn-common/src/auth.rs",
        ],
        "brokered plaintext must be unwrapped only at a transport boundary",
    );
}

/// The shared HTTP transport never follows redirects.
///
/// The other half of binding a credential to a destination. The audience check
/// governs the URL a caller names; a transport that follows redirects then
/// resends that request — headers included — to a URL nothing checked, which
/// makes the check govern only the first hop.
///
/// It is tempting to let the HTTP library handle this, and that is the trap this
/// ban exists for: reqwest strips sensitive headers when a redirect crosses to a
/// different host, but the comparison is host and port, *not* scheme, so a
/// same-host `https` → `http` redirect keeps the `Authorization` header. Restoring
/// a following policy here would reopen that hole silently, since every test
/// would still pass against the mock transport.
#[test]
fn the_shared_http_transport_never_follows_redirects() {
    let transport = core_source("services/http.rs");
    assert!(
        transport.contains("redirect::Policy::none()"),
        "the shared HTTP transport must refuse redirects; the broker decides each hop"
    );
    assert_absent("services/http.rs", &["Policy::limited"]);
}

/// The GitHub client never handles an authenticated header.
///
/// This is what keeps the audience check from decaying into a formality. A
/// `HeaderMap` handed back from the broker is a bearer that has *already*
/// passed its audience check, and it can then be attached to any URL at all —
/// so a `headers()` accessor would let the credential and the destination come
/// apart again, exactly as they were before this work. The authorities expose
/// request methods instead, binding both in one call, and this ban is how that
/// stays true: `github/api.rs` builds URLs and parses responses, and never sees
/// a header.
#[test]
fn the_github_client_never_handles_an_authenticated_header() {
    assert_absent("github/api.rs", &["HeaderMap", "AUTHORIZATION"]);

    // And the audience the broker presents to is parsed from the request's own
    // URL rather than supplied as a constant, which is what makes the check
    // able to fail at all.
    let broker = core_source("security/broker.rs");
    assert!(
        broker.contains("fn authenticated_headers(") && broker.contains("reqwest::Url::parse(url)"),
        "the GitHub broker must derive its audience from the URL it is about to send to"
    );
}

/// A lease is presented only where its audience is a real destination.
///
/// `present` is the audience check, so every call site is a place that had to
/// name where the credential was going. Keeping the list short is what makes
/// that claim reviewable: an audience named far from the send is an audience
/// nobody verified.
#[test]
fn a_lease_is_presented_only_at_a_destination() {
    assert_only_in(
        &[".present(&"],
        &[
            "security/broker.rs",
            "security/lease.rs",
            "account/team_token_minter.rs",
        ],
        "a lease must be presented only where the credential is actually sent",
    );
}

/// An expanded MCP configuration is unwrapped only where it is injected.
///
/// `BrokeredMcpConfig` has no `Debug`, `serde`, or `Clone`, so the carrier
/// cannot be logged or persisted. `resolved_for_connect()` is the one call that
/// gets the plaintext-bearing `McpServerConfig` back out, which makes it the one
/// place that could put it somewhere durable — exactly what a persisted
/// continuation row used to do. It belongs at the transport's connect boundary
/// and nowhere else. The name is deliberately distinctive so this textual ban
/// names one thing and cannot be tripped by an unrelated `resolved()`.
#[test]
fn a_brokered_config_is_unwrapped_only_at_the_connect_boundary() {
    assert_only_in(
        &[".resolved_for_connect()"],
        &["security/broker.rs", "cairn-transport/src/mcp_gateway.rs"],
        "an expanded MCP config must be unwrapped only where it is handed to a transport",
    );
}

/// The persisted MCP continuation row expands its credentials per call.
///
/// The row survives between protocol rounds, so passing `state.config` straight
/// to the gateway is what put resolved credentials in the database in the first
/// place. Every gateway call goes through `state.brokered(...)`, which expands
/// the authored reference for that call and nothing longer. Named structurally
/// because the type system cannot see through the field: `config` is the
/// authored form and is legitimately `Serialize`.
#[test]
fn the_mcp_continuation_row_expands_its_credentials_per_call() {
    assert_absent("mcp/handlers/mcp_continuation.rs", &["&state.config"]);
    let source = core_source("mcp/handlers/mcp_continuation.rs");
    assert!(
        source.contains("fn brokered("),
        "the continuation state must expand its config through the broker"
    );
}

fn assert_absent(relative: &str, forbidden: &[&str]) {
    let source = core_source(relative);
    for needle in forbidden {
        assert!(
            !source.contains(needle),
            "production agent path {relative} must not contain obsolete edge {needle:?}"
        );
    }
}

#[test]
fn agent_job_preparation_has_no_checkout_lifecycle_edges() {
    assert_absent(
        "execution/jobs/lifecycle.rs",
        &[
            "add_workspace",
            "populate_worktree",
            "managed_worktree",
            "worktree_path",
            "owns_ephemeral_worktree",
            "restore_workspace_assignment",
        ],
    );
}

#[test]
fn authenticated_run_identity_never_resolves_from_cwd() {
    assert_absent(
        "mcp/handlers/run_context.rs",
        &[
            "lookup_run_by_cwd",
            "ORDER BY r.created_at DESC",
            "request.cwd",
        ],
    );
}

/// The canon invariant, pinned where it is decided. A search-shaped run item is
/// a read in run's clothing: it must be served *below* head resolution, so it
/// reads the same coordinate a read reads, and *above* both lease acquisition
/// and slot placement, so no served search can admit a cell. Ordering in that
/// function is the entire mechanism, and type checking cannot see it.
#[test]
fn search_interception_sits_below_head_resolution_and_above_admission() {
    let source = core_source("mcp/handlers/run/mod.rs");
    let at = |needle: &str| {
        source
            .find(needle)
            .unwrap_or_else(|| panic!("run dispatch no longer mentions {needle:?}"))
    };
    let head = at("resolve_current_for_read");
    let interception = at("try_run_search_batch");
    let lease = at("acquire_job_residency");
    let placement = at("submit_run_batch");

    assert!(
        head < interception,
        "a served search must read the resolved head coordinate, so interception belongs below head resolution"
    );
    assert!(
        interception < lease && interception < placement,
        "a served search must not be able to take a lease or admit a cell, so interception belongs above both"
    );
}

/// The other half of the same invariant: whatever the seam order says, the
/// serving path itself must have no way to schedule anything.
#[test]
fn served_searches_have_no_scheduling_edges() {
    assert_absent(
        "mcp/handlers/run/search.rs",
        &[
            "acquire_job_residency",
            "submit_run_batch",
            "CellRequest",
            "execution_residency",
            "WorktreeSearchPool",
        ],
    );
}

#[test]
fn logical_reads_cannot_acquire_or_refresh_materializations() {
    for relative in [
        "mcp/handlers/read/file.rs",
        "mcp/handlers/read/object_read.rs",
        "mcp/handlers/read/overlay.rs",
    ] {
        assert_absent(
            relative,
            &[
                "AcquireLease",
                "acquire_lease",
                "RefreshCheckout",
                "refresh_checkout",
                "WorktreeSearchPool",
            ],
        );
    }
}

// ── CAIRN-3822: the typed secret crossings stay closed ───────────────────────────
//
// For dispatch the wrapper IS the guarantee: handlers are reachable only through
// `CheckedInvocation`, and the final-response guard is the return type, so the
// signatures do not admit a bare request or a bare `DispatchOutput`.
//
// For the transcript it is not. `TranscriptEvent` still derives `Serialize`, so
// a bare `serde_json::to_string` of one compiles anywhere; `to_event_json` being
// wrapper-only makes the guarded path convenient, not mandatory. Until that
// becomes a type-level fact (a private `Serialize`, or a serialization newtype),
// the check below over what feeds `EventInsert { data }` is the whole backstop —
// which is why it checks the invariant rather than a set of spellings.

/// Every Rust source file in cairn-core, excluding tests.
/// A stream scrubber that is never flushed truncates its stream.
///
/// This is the failure mode that hides: the scrubber withholds a suffix long
/// enough to catch a credential split across a chunk boundary, so a finalize
/// path that drops it without an end-of-stream flush silently loses the last
/// bytes of the output. That reads as a rendering glitch — a missing final line,
/// a truncated exit message — rather than as a bug, so nobody goes looking for
/// a scrubber.
///
/// Scope is the enclosing block, not the file. A file-wide check passes as soon
/// as *any* reader in it flushes, which is worthless in a file that drains
/// several pipes: deleting one flush leaves the others to satisfy the rule. That
/// version of this test was written first and did not fail when a flush was
/// removed, which is the difference between a test and a decoration.
///
/// A scrubber whose owner deliberately spans functions — handed to a long-lived
/// registry that drains it elsewhere — is built through a constructor reference
/// rather than a call, and is covered by that registry's own tests instead.
#[test]
fn every_stream_scrubber_is_flushed_in_the_block_that_builds_it() {
    let mut offenders = Vec::new();
    for (path, source) in workspace_rust_sources() {
        let lines = non_test_lines(&source);
        for (index, (number, line)) in lines.iter().enumerate() {
            if !line.contains("StreamingScrubber::new()") {
                continue;
            }
            if !block_from(&lines, index).any(|line| line.contains(".flush()")) {
                offenders.push(format!("{}:{number}", path.to_string_lossy()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a StreamingScrubber is built here and never flushed, so this stream is \
         truncated at its tail rather than scrubbed at it: {offenders:?}"
    );
}

/// The rest of the block enclosing `start`, by brace depth.
fn block_from<'a>(
    lines: &'a [(usize, &'a str)],
    start: usize,
) -> impl Iterator<Item = &'a str> + 'a {
    let mut depth: i32 = 0;
    let mut closed = false;
    lines[start..].iter().map_while(move |(_, line)| {
        if closed {
            return None;
        }
        depth += line.matches('{').count() as i32;
        depth -= line.matches('}').count() as i32;
        if depth < 0 {
            closed = true;
        }
        Some(*line)
    })
}

/// Every Rust source file in the workspace, for checks that span crates.
fn workspace_rust_sources() -> Vec<(PathBuf, String)> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root resolves from the crate manifest");
    let mut paths = Vec::new();
    walk_source_files(&repo.join("src-tauri"), &mut paths);
    paths
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("rs"))
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path).ok()?;
            Some((path, source))
        })
        .collect()
}

fn core_source_files() -> Vec<(PathBuf, String)> {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths = Vec::new();
    walk_source_files(&src, &mut paths);
    paths
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("rs"))
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path).ok()?;
            Some((path, source))
        })
        .collect()
}

/// Whether an attribute line gates its item on `test`.
///
/// Covers `#[cfg(test)]` and compound forms like
/// `#[cfg(all(test, feature = "test-utils"))]`, without matching a feature that
/// merely has "test" in its name: the delimiter after `test` distinguishes the
/// cfg predicate from `"latest"` or `"test-utils"`.
fn gates_on_test(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("#[cfg(") && (line.contains("test)") || line.contains("test,"))
}

/// Lines of `source` outside any test-gated module, paired with 1-based line
/// numbers.
///
/// Brace-depth tracking rather than a parser: a test module is closed at the
/// depth it opened. Test modules are excluded because a fixture that serializes
/// a type directly cannot leak anything at runtime.
fn non_test_lines(source: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut depth: i32 = 0;
    let mut test_module_depth: Option<i32> = None;
    let mut pending_cfg_test = false;
    for (index, line) in source.lines().enumerate() {
        let opens = line.matches('{').count() as i32;
        let closes = line.matches('}').count() as i32;
        if test_module_depth.is_none() {
            if pending_cfg_test && line.contains("mod ") && line.contains('{') {
                test_module_depth = Some(depth);
                pending_cfg_test = false;
            } else {
                if gates_on_test(line) {
                    pending_cfg_test = true;
                }
                out.push((index + 1, line));
            }
        }
        depth += opens - closes;
        if let Some(entry_depth) = test_module_depth {
            if depth <= entry_depth {
                test_module_depth = None;
            }
        }
    }
    out
}

/// The detector must actually exclude a test module, or every ban built on it
/// silently passes. An earlier version matched only the literal `#[cfg(test)]`
/// and let a whole `#[cfg(all(test, feature = ...))]` module through.
#[test]
fn the_test_module_detector_excludes_compound_cfg_gates() {
    let source = "fn live() {}\n#[cfg(all(test, feature = \"test-utils\"))]\nmod tests {\n    fn hidden() {}\n}\nfn also_live() {}\n";
    let kept: Vec<&str> = non_test_lines(source)
        .into_iter()
        .map(|(_, line)| line)
        .collect();
    assert!(kept.iter().any(|line| line.contains("fn live")));
    assert!(kept.iter().any(|line| line.contains("fn also_live")));
    assert!(
        !kept.iter().any(|line| line.contains("fn hidden")),
        "a compound test cfg must still exclude the module"
    );
    // A feature merely named for testing is not a test gate.
    assert!(gates_on_test("#[cfg(test)]"));
    assert!(!gates_on_test("#[cfg(feature = \"latest\")]"));
    assert!(!gates_on_test("#[cfg(feature = \"test-utils\")]"));
}

/// Every durable transcript row's `data` comes from the sanitized wrapper.
///
/// This checks the invariant rather than a spelling. An earlier version of this
/// test grepped for three literal serializer spellings, and the Codex backend
/// slipped past it by calling `serde_json::to_string(event)` on a parameter that
/// happened to be named `event` — a whole backend's transcript path unguarded
/// while the test stayed green. `TranscriptEvent` still derives `Serialize`, so
/// the compiler cannot close this on its own yet; until the type-level move
/// lands, the check is over what feeds `EventInsert { data }`.
#[test]
fn every_event_insert_takes_its_data_from_the_sanitized_wrapper() {
    let mut offenders = Vec::new();
    for (path, source) in core_source_files() {
        let display = path.to_string_lossy().into_owned();
        let lines: Vec<(usize, &str)> = non_test_lines(&source);
        for (index, (line_number, line)) in lines.iter().enumerate() {
            // `data: <expr>` inside an EventInsert literal. The value must be a
            // local bound from `to_event_json`, or that call inline.
            let Some(value) = line.trim().strip_prefix("data: ") else {
                continue;
            };
            if !is_inside_event_insert(&lines, index) {
                continue;
            }
            let value = value.trim_end_matches(',').trim();
            let binding = value.trim_end_matches(".clone()");
            let sourced_from_guard = value.contains("to_event_json")
                || lines.iter().any(|(_, candidate)| {
                    let candidate = candidate.trim();
                    (candidate.starts_with(&format!("let {binding} "))
                        || candidate.starts_with(&format!("let {binding} =")))
                        && candidate.contains("to_event_json")
                })
                // A row rebuilt from an already-persisted event carries data
                // that crossed the guard when it was first written.
                || lines.iter().any(|(_, candidate)| {
                    let candidate = candidate.trim();
                    candidate.starts_with(&format!("let {binding} "))
                        && (candidate.contains("event.data") || candidate.contains("row"))
                });
            if !sourced_from_guard {
                offenders.push(format!("{display}:{line_number} -> {value}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "every EventInsert.data must come from ObservedSafe::to_event_json: {offenders:?}"
    );
}

/// Whether the `data:` field at `index` sits inside an `EventInsert` literal,
/// found by scanning back a bounded window for the opening of the literal.
fn is_inside_event_insert(lines: &[(usize, &str)], index: usize) -> bool {
    lines[index.saturating_sub(12)..=index]
        .iter()
        .any(|(_, line)| line.contains("EventInsert {"))
}

/// Only the crossing module and the two modules that own a guarded payload may
/// teach a type how to call itself sanitized.
///
/// A no-op `Sanitize` implementation elsewhere would make `ObservedSafe` accept a
/// type it never actually scrubbed, which is the one way to defeat the wrapper
/// without touching it.
#[test]
fn sanitize_is_implemented_only_where_a_guarded_payload_lives() {
    const ALLOWED: &[&str] = &[
        "security/crossing.rs",
        "dispatch.rs",
        "agent_process/stream.rs",
    ];
    let mut offenders = Vec::new();
    for (path, source) in core_source_files() {
        let display = path.to_string_lossy().into_owned();
        if ALLOWED.iter().any(|allowed| display.ends_with(allowed)) {
            continue;
        }
        for (line_number, line) in non_test_lines(&source) {
            if line.contains("impl") && line.contains("Sanitize for") {
                offenders.push(format!("{display}:{line_number}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "Sanitize implementations must stay with the guarded payload types: {offenders:?}"
    );
}

/// Handlers are reachable only through a checked invocation.
///
/// `dispatch_tool` is the single authenticated entry, and the two functions
/// between it and the handlers take `CheckedInvocation`. Asserting the parameter
/// type keeps a future refactor from widening them back to a raw request — which
/// would compile, and would silently reopen the inbound crossing.
#[test]
fn dispatch_reaches_handlers_only_through_a_checked_invocation() {
    let source = core_source("dispatch.rs");
    for function in ["async fn dispatch_with_dedup(", "async fn execute_tool("] {
        let start = source
            .find(function)
            .unwrap_or_else(|| panic!("dispatch.rs defines {function}"));
        let signature: String = source[start..].chars().take(400).collect();
        assert!(
            signature.contains("CheckedInvocation"),
            "{function} must take a CheckedInvocation, not a raw request"
        );
    }
    assert!(
        source.contains("CheckedInvocation::from_model(request)"),
        "dispatch_tool must construct the inbound guard itself"
    );
    assert!(
        source.contains(") -> ObservedSafe<DispatchOutput> {"),
        "dispatch_tool must return the final-response guard, so no branch can skip it"
    );
}

/// The inbound guard has exactly one constructor and no way to opt out of it.
///
/// A second origin is legitimate once the broker exists, but it must arrive as
/// its own constructor: a boolean parameter would let any caller skip the check
/// by passing `false`, which is the whole failure mode this gate exists to make
/// impossible.
#[test]
fn the_inbound_guard_cannot_be_told_to_skip_its_check() {
    let source = core_source("security/crossing.rs");
    let start = source
        .find("impl<'a, T: ModelInvocation> CheckedInvocation<'a, T> {")
        .expect("crossing.rs has the CheckedInvocation impl");
    let end = start
        + source[start..]
            .find("\n}\n")
            .expect("the CheckedInvocation impl is closed");
    let constructors = source[start..end].matches("pub fn from_").count();
    assert_eq!(
        constructors, 1,
        "CheckedInvocation must expose exactly one constructor"
    );
    for flag in ["bool", "skip", "bypass", "unchecked"] {
        let start = source
            .find("pub fn from_model(")
            .expect("crossing.rs defines from_model");
        let signature: String = source[start..].chars().take(200).collect();
        assert!(
            !signature.contains(flag),
            "from_model must not take a {flag} parameter"
        );
    }
}

/// Every layer of the shared logging subscriber writes through the scrubbing
/// writer.
///
/// The log sink is the one crossing with no type to hold it: a `tracing` layer
/// takes any `MakeWriter`, so a new layer — or a revert of one — opts out of
/// redaction just by naming the raw writer, and nothing fails to compile. The
/// rotated JSONL files and the runner service's `runner.err.log` are both
/// durable, so an unscrubbed layer is a durable plaintext sink.
///
/// Scope is every *layer construction*, not every `with_writer` call. Checking
/// only the `with_writer` lines misses the case that actually needs catching: a
/// `fmt::layer()` that never calls `with_writer` logs to stderr through the
/// default writer, so a check that inspects only `with_writer` lines stays green
/// while the calls already present satisfy any non-vacuity assertion beside it.
#[test]
fn every_log_layer_writes_through_the_scrubbing_writer() {
    let (path, source) = workspace_rust_sources()
        .into_iter()
        .find(|(path, _)| path.ends_with("cairn-common/src/logging.rs"))
        .expect("the shared logging module is part of the workspace");
    let lines = non_test_lines(&source);

    let mut offenders = Vec::new();
    for (index, (number, line)) in lines.iter().enumerate() {
        if !line.contains("fmt::layer()") {
            continue;
        }
        if !statement_from(&lines, index).any(|line| line.contains("ScrubbedWriter::")) {
            offenders.push(format!("{}:{number}", path.to_string_lossy()));
        }
    }
    assert!(
        offenders.is_empty(),
        "a logging layer is built without naming the scrubbing writer, so records reach its sink without passing the redaction seam: {offenders:?}"
    );

    // Entry points that install a subscriber carrying a writer of their own,
    // bypassing the layers checked above entirely.
    for banned in ["tracing_subscriber::fmt()", "fmt::init(", "FmtSubscriber"] {
        assert!(
            !lines.iter().any(|(_, line)| line.contains(banned)),
            "`{banned}` installs a logging sink that never passes the redaction seam"
        );
    }

    assert!(
        lines.iter().any(|(_, line)| line.contains("fmt::layer()")),
        "this check is worthless if the module builds no layers at all"
    );
}

/// The rest of the statement beginning at `start`, by `;` termination. A layer
/// is built as one chained expression, so this is the span in which it must name
/// its writer.
fn statement_from<'a>(
    lines: &'a [(usize, &'a str)],
    start: usize,
) -> impl Iterator<Item = &'a str> + 'a {
    let mut done = false;
    lines[start..].iter().map_while(move |(_, line)| {
        if done {
            return None;
        }
        if line.trim_end().ends_with(';') {
            done = true;
        }
        Some(*line)
    })
}

/// Coverage must not claim more than the crossings deliver.
///
/// `Enforced` is the end of the whole CAIRN-3822 program, after every child
/// issue's acceptance bar. A variant added early would let a surface state an
/// invariant the system does not hold.
#[test]
fn coverage_cannot_claim_enforcement_before_the_program_completes() {
    let source = core_source("security/mod.rs");
    let start = source
        .find("pub enum Coverage")
        .expect("security/mod.rs defines Coverage");
    let body_end = start
        + source[start..]
            .find('}')
            .expect("the Coverage enum is closed");
    let body = &source[start..body_end];
    let variants: Vec<&str> = body
        .lines()
        .filter_map(|line| line.trim().strip_suffix(','))
        .filter(|line| !line.starts_with("///") && !line.starts_with("//"))
        .collect();
    assert_eq!(
        variants,
        vec!["Unenforced", "SinkGuarded"],
        "Coverage must not gain an Enforced variant until CAIRN-3824 through CAIRN-3828 land"
    );
    assert!(source.contains("pub const COVERAGE: Coverage = Coverage::SinkGuarded;"));
}
