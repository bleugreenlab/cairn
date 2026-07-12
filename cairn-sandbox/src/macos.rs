//! macOS sandbox: Seatbelt SBPL profile generation + `sandbox-exec` wrapping.
//!
//! The profile uses last-matching-rule-wins semantics to express:
//!   - allow everything by default (so the toolchain's broad reads/execs work),
//!   - deny all filesystem writes,
//!   - re-allow writes under the worktree, temp dirs, granted crossings, /dev,
//!   - deny reads of the sensitive denylist.

use super::SandboxPolicy;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Generate the SBPL profile string for a policy.
pub fn sbpl_profile(policy: &SandboxPolicy) -> String {
    let mut out = String::new();
    out.push_str("(version 1)\n");
    out.push_str("(allow default)\n");
    // Confine writes: deny everything, then re-allow the writable set.
    out.push_str("(deny file-write* (subpath \"/\"))\n");

    let mut writable: Vec<PathBuf> = policy.writable_paths();
    // /dev is I/O plumbing (tty, null, fd) and must stay writable.
    writable.push(PathBuf::from("/dev"));

    // Combine concrete subpath grants with any regex grants (service sandboxes
    // express cross-worktree scopes like `{worktrees}/**/target/**` as regexes
    // because the matching dirs do not all exist at profile-build time).
    let writable_rules = join_rules(
        &subpath_rules(&writable),
        &regex_rules(&policy.writable_regex),
    );
    if !writable_rules.is_empty() {
        out.push_str(&format!("(allow file-write* {})\n", writable_rules));
    }

    // Hard-deny reads of the sensitive denylist.
    let deny_rules = subpath_rules(&policy.deny_read);
    if !deny_rules.is_empty() {
        out.push_str(&format!("(deny file-read* {})\n", deny_rules));
    }

    // Re-allow reads of the readable set LAST (last-match-wins) so a denylist
    // entry (e.g. a user-configured one covering a prefix of the worktree) can
    // never block the agent from reading its own source. The readable set keeps
    // the worktree even for a read-only-checkout policy, whose writable set drops
    // it — so a non-worktree live checkout stays readable while its writes are
    // denied. Regex write scopes are re-allowed for reads too.
    let mut readable: Vec<PathBuf> = policy.readable_paths();
    readable.push(PathBuf::from("/dev"));
    let read_allow_rules = join_rules(
        &subpath_rules(&readable),
        &regex_rules(&policy.writable_regex),
    );
    if !read_allow_rules.is_empty() {
        out.push_str(&format!("(allow file-read* {})\n", read_allow_rules));
    }

    out
}

/// Join two space-separated SBPL clause runs, dropping empties.
fn join_rules(a: &str, b: &str) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, true) => String::new(),
        (false, true) => a.to_string(),
        (true, false) => b.to_string(),
        (false, false) => format!("{a} {b}"),
    }
}

/// Build a space-joined run of `(regex #"...")` clauses. The patterns are
/// already anchored regex strings (see `super::glob_to_regex`); only the SBPL
/// string delimiter `"` is escaped — backslashes are intentional regex escapes.
fn regex_rules(patterns: &[String]) -> String {
    let mut seen: Vec<String> = Vec::new();
    for p in patterns {
        let escaped = p.replace('"', "\\\"");
        let clause = format!("(regex #\"{}\")", escaped);
        if !seen.contains(&clause) {
            seen.push(clause);
        }
    }
    seen.join(" ")
}

/// Rewrite argv to run under `sandbox-exec` with the generated profile.
pub fn wrap_argv(program: &str, args: &[String], policy: &SandboxPolicy) -> (String, Vec<String>) {
    let profile = sbpl_profile(policy);
    let mut wrapped = vec!["-p".to_string(), profile, program.to_string()];
    wrapped.extend(args.iter().cloned());
    ("/usr/bin/sandbox-exec".to_string(), wrapped)
}

/// Build a space-joined run of `(subpath "...")` clauses for canonicalized,
/// existing paths. Non-existent paths are dropped (subpath of a missing path
/// is inert), and each literal is escaped for SBPL.
fn subpath_rules(paths: &[PathBuf]) -> String {
    let mut seen: Vec<String> = Vec::new();
    for p in paths {
        let canonical = canonicalize_best_effort(p);
        let literal = escape_sbpl(&canonical.to_string_lossy());
        let clause = format!("(subpath \"{}\")", literal);
        if !seen.contains(&clause) {
            seen.push(clause);
        }
    }
    seen.join(" ")
}

/// Canonicalize a path if it exists; otherwise return it unchanged. SBPL
/// `subpath` matches the literal prefix, so a stable absolute path is what
/// matters — canonicalization mainly resolves macOS `/tmp` → `/private/tmp`
/// and `/var` → `/private/var` symlinks so the rule matches the real path the
/// kernel reports.
fn canonicalize_best_effort(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Escape a string for an SBPL double-quoted literal.
fn escape_sbpl(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Recover the denied path for a sandbox violation by `pid` from the unified
/// log. Best-effort: returns `None` if the log query fails or no violation is
/// found (the caller then falls back to a command-scoped denial).
pub fn detect_violation(pid: u32, since: SystemTime) -> Option<PathBuf> {
    // Query a small window starting a couple seconds before the spawn to absorb
    // clock skew. `log show` wants a local "YYYY-MM-DD HH:MM:SS" start string.
    let start = since
        .checked_sub(std::time::Duration::from_secs(2))
        .unwrap_or(since);
    let start_str = format_log_start(start)?;

    // The unified log has ingestion lag, so the violation record is often not
    // queryable immediately after the child exits. Retry a few times within a
    // small budget. This is only reached on an actual denial (signature present
    // + non-zero exit), so the cost is not on the hot path.
    const ATTEMPTS: usize = 3;
    const DELAY: std::time::Duration = std::time::Duration::from_millis(150);
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(DELAY);
        }
        if let Some(path) = query_violation(pid, &start_str) {
            return Some(path);
        }
    }
    None
}

/// One `log show` query + parse for a sandbox-deny path by `pid`.
fn query_violation(pid: u32, start_str: &str) -> Option<PathBuf> {
    let output = std::process::Command::new("/usr/bin/log")
        .args([
            "show",
            "--style",
            "ndjson",
            "--start",
            start_str,
            "--predicate",
            &format!("processID == {pid}"),
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let ndjson = String::from_utf8_lossy(&output.stdout);
    parse_violation_path(&ndjson, pid)
}

/// Format a `SystemTime` as a local `YYYY-MM-DD HH:MM:SS` string for
/// `log show --start`.
fn format_log_start(t: SystemTime) -> Option<String> {
    let secs = t.duration_since(SystemTime::UNIX_EPOCH).ok()?.as_secs();
    let dt = chrono::DateTime::<chrono::Local>::from(
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs),
    );
    Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Parse the first sandbox-deny path for `pid` out of `log show` ndjson output.
///
/// Sandbox violation messages look like:
///   `Sandbox: bash(1234) deny(1) file-write-create /Users/x/escape.txt`
/// We extract the path that follows the `deny(N) <operation>` prefix.
fn parse_violation_path(ndjson: &str, pid: u32) -> Option<PathBuf> {
    let re = regex::Regex::new(r"deny\(\d+\)\s+\S+\s+(/.+)$").ok()?;
    for line in ndjson.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Match the offending pid; the field is numeric in ndjson output.
        if value.get("processID").and_then(|v| v.as_u64()) != Some(pid as u64) {
            continue;
        }
        let msg = match value.get("eventMessage").and_then(|v| v.as_str()) {
            Some(m) => m,
            None => continue,
        };
        if let Some(caps) = re.captures(msg.trim()) {
            if let Some(path) = caps.get(1) {
                return Some(PathBuf::from(path.as_str().trim()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    #[test]
    fn profile_has_expected_structure() {
        let policy = SandboxPolicy {
            worktree: PathBuf::from("/work/wt"),
            writable_extra: vec![],
            deny_read: vec![PathBuf::from("/home/x/.aws")],
            writable_regex: vec![],
            worktree_writable: true,
        };
        let profile = sbpl_profile(&policy);
        assert!(profile.starts_with("(version 1)\n(allow default)\n"));
        assert!(profile.contains("(deny file-write* (subpath \"/\"))"));
        assert!(profile.contains("(allow file-write*"));
        assert!(profile.contains("(subpath \"/dev\")"));
        // deny-read appears after the writable allow (last-match-wins ordering),
        // and the worktree read re-allow appears after deny-read so it wins.
        let write_idx = profile.find("(allow file-write*").unwrap();
        let deny_idx = profile.find("(deny file-read*").unwrap();
        let read_allow_idx = profile.rfind("(allow file-read*").unwrap();
        assert!(deny_idx > write_idx);
        assert!(
            read_allow_idx > deny_idx,
            "worktree read re-allow must come after deny-read"
        );
    }

    #[test]
    fn wrap_argv_prefixes_sandbox_exec() {
        let policy = SandboxPolicy {
            worktree: PathBuf::from("/work/wt"),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
        };
        let (program, args) = wrap_argv("bash", &["-c".into(), "echo hi".into()], &policy);
        assert_eq!(program, "/usr/bin/sandbox-exec");
        assert_eq!(args[0], "-p");
        assert_eq!(args[2], "bash");
        assert_eq!(args[3], "-c");
        assert_eq!(args[4], "echo hi");
    }

    #[test]
    fn parse_violation_path_extracts_denied_path() {
        let ndjson = r#"{"processID":1234,"eventMessage":"Sandbox: bash(1234) deny(1) file-write-create /Users/x/escape.txt"}
{"processID":1234,"eventMessage":"some unrelated log line"}"#;
        let path = super::parse_violation_path(ndjson, 1234);
        assert_eq!(path, Some(PathBuf::from("/Users/x/escape.txt")));
    }

    #[test]
    fn parse_violation_path_ignores_other_pids_and_non_deny() {
        let ndjson = r#"{"processID":9999,"eventMessage":"deny(1) file-read-data /Users/x/.aws/credentials"}
{"processID":1234,"eventMessage":"allow file-read-data /Users/x/ok"}"#;
        assert_eq!(super::parse_violation_path(ndjson, 1234), None);
    }

    #[test]
    fn parse_violation_path_handles_read_denials() {
        let ndjson =
            r#"{"processID":42,"eventMessage":"deny(1) file-read-data /home/u/.ssh/id_rsa"}"#;
        assert_eq!(
            super::parse_violation_path(ndjson, 42),
            Some(PathBuf::from("/home/u/.ssh/id_rsa"))
        );
    }

    #[test]
    fn escape_handles_quotes_and_backslashes() {
        assert_eq!(escape_sbpl("a\"b"), "a\\\"b");
        assert_eq!(escape_sbpl("a\\b"), "a\\\\b");
    }

    // Live end-to-end: spawn real commands under the generated profile via
    // sandbox-exec and assert the kernel enforces the policy. Runs only on
    // macOS, where sandbox-exec is present.
    //
    // These tests spawn a real `sandbox-exec`. When the test process is itself
    // already confined by a Cairn worktree fence (`CAIRN_SANDBOXED=1`, set on
    // every fenced `run`/skill/PTY spawn), the nested `sandbox-exec` cannot
    // perform even the in-bounds writes they assert, so they cannot run
    // meaningfully. Each live test detects that and skips rather than fails;
    // unfenced CI still runs them for real kernel-level coverage. This is what
    // lets `bun run test:rust` go fully green inside an agent fence without a
    // hand-maintained skip-list.
    fn skip_when_fenced(test: &str) -> bool {
        if std::env::var_os("CAIRN_SANDBOXED").is_some() {
            eprintln!(
                "skipping {test}: nested sandbox-exec is unsupported inside a Cairn worktree fence"
            );
            record_fence_skip(test);
            true
        } else {
            false
        }
    }

    // Best-effort: append a self-skipped test name to `$CAIRN_SKIP_LOG` (set by
    // `scripts/test-rust.ts`) so the runner can report how many tests skipped
    // under the fence. libtest swallows the skip message of a passing test, so
    // without this the skip is indistinguishable from a real pass. (#157)
    fn record_fence_skip(test: &str) {
        if let Some(log) = std::env::var_os("CAIRN_SKIP_LOG") {
            use std::io::Write as _;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log)
            {
                // One write_all (vs writeln!'s multiple syscalls) keeps the
                // append atomic when parallel test threads all skip at once.
                let _ = f.write_all(format!("{test}\n").as_bytes());
            }
        }
    }

    #[test]
    fn enforces_write_confinement_and_read_denylist_live() {
        if skip_when_fenced("enforces_write_confinement_and_read_denylist_live") {
            return;
        }
        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let secret = outside.path().join("secret");
        std::fs::create_dir_all(&secret).unwrap();
        std::fs::write(secret.join("token"), "s3cr3t").unwrap();

        // Empty writable_extra so the sibling temp dir is genuinely outside the
        // writable set. (In production temp dirs are writable, but a real
        // escape targets $HOME or a sibling worktree, not the temp tree.)
        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![],
            deny_read: vec![secret.clone()],
            writable_regex: vec![],
            worktree_writable: true,
        };

        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // In-worktree write succeeds.
        let inside_file = wt.path().join("ok.txt");
        let out = run(&format!("echo hi > {}", inside_file.display()));
        assert!(out.status.success(), "in-worktree write should succeed");
        assert!(inside_file.exists());

        // Out-of-worktree write is blocked by the kernel.
        let escape = outside.path().join("escape.txt");
        let out = run(&format!("echo x > {}", escape.display()));
        assert!(
            !out.status.success(),
            "out-of-worktree write must be denied"
        );
        assert!(!escape.exists(), "escape file must not be created");

        // Denylisted read is blocked even though reads are otherwise broad.
        let out = run(&format!("cat {}", secret.join("token").display()));
        assert!(!out.status.success(), "denylisted read must be denied");

        // A non-denylisted read outside the worktree still works (read-broad).
        let public = outside.path().join("public.txt");
        std::fs::write(&public, "hello").unwrap();
        let out = run(&format!("cat {}", public.display()));
        assert!(out.status.success(), "non-denylisted reads stay allowed");
    }

    // Live: a writable_extra carve-out (the mechanism behind keeping
    // ~/.cargo, ~/.npm, etc. writable) actually permits writes there while the
    // surrounding tree stays read-only. This is the regression guard the review
    // flagged as missing for the toolchain-cache fix.
    // Live: even when a (user-configured) denylist covers a prefix of the
    // worktree, the worktree stays readable via the final writable re-allow,
    // while a sensitive sibling under the same denied base is blocked.
    #[test]
    fn worktree_under_denied_prefix_stays_readable_live() {
        if skip_when_fenced("worktree_under_denied_prefix_stays_readable_live") {
            return;
        }
        let base = tempdir().unwrap();
        let wt = base.path().join("worktrees").join("wt");
        std::fs::create_dir_all(wt.join("src")).unwrap();
        std::fs::write(wt.join("src/lib.rs"), "fn main() {}").unwrap();
        std::fs::write(base.path().join("secret"), "s3cr3t").unwrap();

        let policy = SandboxPolicy {
            worktree: wt.clone(),
            writable_extra: vec![],
            deny_read: vec![base.path().to_path_buf()], // a broad denylist entry
            writable_regex: vec![],
            worktree_writable: true,
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // Worktree nested under the denied base is still readable.
        let out = run(&format!("cat {}", wt.join("src/lib.rs").display()));
        assert!(
            out.status.success(),
            "worktree under a denied prefix must stay readable"
        );
        // A sensitive sibling under the denied base is blocked.
        let out = run(&format!("cat {}", base.path().join("secret").display()));
        assert!(
            !out.status.success(),
            "sibling under the denied base must be denied"
        );
    }

    #[test]
    fn writable_extra_carveout_permits_writes_live() {
        if skip_when_fenced("writable_extra_carveout_permits_writes_live") {
            return;
        }
        let wt = tempdir().unwrap();
        let cache_parent = tempdir().unwrap();
        let cache = cache_parent.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();

        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![cache.clone()],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // Write inside the carve-out succeeds.
        let inside = cache.join("artifact");
        let out = run(&format!("echo x > {}", inside.display()));
        assert!(
            out.status.success(),
            "write into writable_extra must succeed"
        );
        assert!(inside.exists());

        // Write to the carve-out's parent (not granted) is still denied.
        let sibling = cache_parent.path().join("escape.txt");
        let out = run(&format!("echo x > {}", sibling.display()));
        assert!(
            !out.status.success(),
            "write outside the carve-out must be denied"
        );
        assert!(!sibling.exists());
    }

    #[test]
    fn readonly_checkout_profile_reads_but_does_not_write_cwd() {
        // A non-worktree live-checkout policy keeps the checkout readable but
        // drops it from the write-allow, so a kernel write into it is denied.
        let checkout = PathBuf::from("/project/live");
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &[], vec![]);
        let profile = sbpl_profile(&policy);

        // The checkout appears in the final read re-allow.
        let read_allow = profile.rfind("(allow file-read*").unwrap();
        assert!(
            profile[read_allow..].contains("/project/live"),
            "read-only checkout must stay readable:\n{profile}"
        );
        // ... but NOT in the write-allow line.
        let write_allow_line = profile
            .lines()
            .find(|l| l.starts_with("(allow file-write*"))
            .unwrap_or("");
        assert!(
            !write_allow_line.contains("/project/live"),
            "read-only checkout must NOT be writable: {write_allow_line}"
        );
    }

    #[test]
    fn for_run_profile_writes_cwd() {
        let wt = PathBuf::from("/work/wt");
        let policy = SandboxPolicy::for_run(&wt, &[], vec![]);
        let profile = sbpl_profile(&policy);
        let write_allow_line = profile
            .lines()
            .find(|l| l.starts_with("(allow file-write*"))
            .unwrap_or("");
        assert!(
            write_allow_line.contains("/work/wt"),
            "for_run must keep cwd writable: {write_allow_line}"
        );
    }

    #[test]
    fn service_profile_contains_regex_write_allow() {
        let policy = SandboxPolicy::for_service(
            Path::new("/home/u/.cairn/sccache"),
            &["/home/u/.cairn/worktrees/**/target/**".to_string()],
            vec![],
        );
        let profile = sbpl_profile(&policy);
        assert!(profile.contains("(allow file-write*"));
        // The worktrees-target glob is emitted as an anchored SBPL regex.
        assert!(
            profile.contains("(regex #\"^/home/u/\\.cairn/worktrees/.*/target/.*\")"),
            "profile missing worktrees-target regex allow:\n{profile}"
        );
    }

    // Live: a service sandbox writes worktree `target/` trees (the cross-worktree
    // grant) and its own state dir, but NOT worktree source or arbitrary paths.
    // This is the regression guard for the Managed Build Services fix.
    #[test]
    fn service_sandbox_permits_worktree_target_writes_only_live() {
        if skip_when_fenced("service_sandbox_permits_worktree_target_writes_only_live") {
            return;
        }
        let base = tempdir().unwrap();
        // Canonicalize so the glob's literal prefix matches the path the kernel
        // reports (macOS /var/folders -> /private/var/folders).
        let root = base.path().canonicalize().unwrap();
        let worktrees = root.join("worktrees");
        let wt = worktrees.join("CAIRN-1").join("src-tauri");
        std::fs::create_dir_all(wt.join("target/release/deps")).unwrap();
        std::fs::create_dir_all(wt.join("src")).unwrap();
        let state = root.join("sccache");
        std::fs::create_dir_all(&state).unwrap();

        let glob = format!("{}/**/target/**", worktrees.display());
        // Build the policy with only the state dir writable (no blanket temp
        // carve-out) so the test isolates the regex grant: the worktree source
        // lives under the temp tree, which `for_service` would otherwise make
        // writable in production. The regex is the mechanism under test.
        let policy = SandboxPolicy {
            worktree: state.clone(),
            writable_extra: vec![state.clone()],
            deny_read: vec![],
            writable_regex: vec![super::super::glob_to_regex(&glob)],
            worktree_writable: true,
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        // Write into a worktree target tree: allowed via the regex grant.
        let target_file = wt.join("target/release/deps/x.d");
        let out = run(&format!("echo x > {}", target_file.display()));
        assert!(
            out.status.success(),
            "service must write worktree target dirs"
        );
        assert!(target_file.exists());

        // Write into worktree SOURCE: denied (it is not a target dir).
        let src_file = wt.join("src/evil.rs");
        let out = run(&format!("echo x > {}", src_file.display()));
        assert!(
            !out.status.success(),
            "service must NOT write worktree source"
        );
        assert!(!src_file.exists());

        // Write into the service's own state dir: allowed.
        let state_file = state.join("cache.bin");
        let out = run(&format!("echo x > {}", state_file.display()));
        assert!(out.status.success(), "service must write its own state dir");

        // Write to an arbitrary sibling outside all grants: denied.
        let escape = root.join("escape.txt");
        let out = run(&format!("echo x > {}", escape.display()));
        assert!(
            !out.status.success(),
            "service must not write arbitrary paths"
        );
        assert!(!escape.exists());
    }
}
