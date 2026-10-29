//! macOS sandbox: Seatbelt SBPL profile generation + `sandbox-exec` wrapping.
//!
//! The profile uses last-matching-rule-wins semantics to express:
//!   - allow everything by default (so the toolchain's broad reads/execs work),
//!   - deny all filesystem writes,
//!   - re-allow writes under the worktree, temp dirs, granted crossings, /dev,
//!   - deny reads of the sensitive denylist,
//!   - and, last, the operator credential boundary, which nothing re-allows.

use super::{
    escape_sbpl, Confinement, NestedSandboxWriteProbe, OperatorCredentialFloor, SandboxPolicy,
};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

pub(crate) fn current_process_is_confined() -> bool {
    process_confinement().unwrap_or(true)
}

/// Whether this process runs under an inherited profile, or `None` when the
/// question could not be asked because `sandbox-exec` did not launch.
///
/// A profile cannot be removed from a running process, so the answer holds for
/// the life of the process and is measured once.
pub(crate) fn process_confinement() -> Option<bool> {
    static CONFINED: OnceLock<Option<bool>> = OnceLock::new();
    *CONFINED.get_or_init(|| {
        // Seatbelt profiles are inherited and cannot be replaced. Applying an
        // allow-default profile in a disposable child therefore succeeds only
        // when the parent is unconfined; a child of a sandboxed process exits
        // because sandbox_apply is not permitted.
        //
        // Do not use the private sandbox_check API here. On current macOS it
        // reports the platform's baseline policy as well as an inherited
        // process profile: even an ordinary shell receives "denied" for a
        // file-create query it can perform successfully.
        Command::new("/usr/bin/sandbox-exec")
            .args(["-p", "(version 1)(allow default)", "/usr/bin/true"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .ok()
            .map(|status| !status.success())
    })
}

/// Spawn a command exactly as a spawn under `outer` is wrapped, and inside it
/// apply a nested profile exactly as a fenced agent command is wrapped, then
/// attempt a write that nested profile permits.
///
/// That is what a sandbox test running under `outer` does, so this measures
/// the workload's own path rather than this process's.
pub(crate) fn probe_nested_sandbox_write_under(outer: &Confinement) -> NestedSandboxWriteProbe {
    let outcome = tempfile::tempdir().map(|dir| {
        let root = dir
            .path()
            .canonicalize()
            .unwrap_or_else(|_| dir.path().into());
        let probe = root.join("probe");
        let inner = Confinement::Fenced(SandboxPolicy::for_run(&root, &[], Vec::new()));
        let (inner_program, inner_args) = inner.wrap_argv(
            "/bin/sh",
            &["-c".to_string(), "echo ok > probe".to_string()],
        );
        let (program, args) = outer.wrap_argv(&inner_program, &inner_args);
        let mut command = Command::new(&program);
        command.args(&args).current_dir(&root).stdin(Stdio::null());
        outer.install_pre_exec(&mut command);
        match command.output() {
            Ok(output) if output.status.success() && probe.exists() => NestedSandboxWriteProbe {
                available: true,
                apply_refused: false,
                detail: "an in-bounds temp write succeeded under a nested Seatbelt profile".into(),
            },
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                NestedSandboxWriteProbe {
                    available: false,
                    apply_refused: output.status.code() == Some(71)
                        && stderr.contains("sandbox_apply"),
                    detail: format!(
                        "nested Seatbelt in-bounds temp write refused (exit {:?}): {}",
                        output.status.code(),
                        stderr.lines().next().unwrap_or("no stderr")
                    ),
                }
            }
            Err(error) => NestedSandboxWriteProbe {
                available: false,
                apply_refused: false,
                detail: format!("could not launch nested Seatbelt probe: {error}"),
            },
        }
    });
    outcome.unwrap_or_else(|error| NestedSandboxWriteProbe {
        available: false,
        apply_refused: false,
        detail: format!("could not create nested Seatbelt probe directory: {error}"),
    })
}

/// Generate the SBPL profile string for a policy, closed by the operator
/// credential boundary `floor` describes.
pub(crate) fn sbpl_profile(policy: &SandboxPolicy, floor: &OperatorCredentialFloor) -> String {
    let mut out = String::new();
    out.push_str("(version 1)\n");
    out.push_str("(allow default)\n");
    // Every fence deny carries the spawn's tag into the violation log, which is
    // how a denial made by any descendant is found again (see `ViolationTag`).
    let report = format!("(with message \"{}\")", policy.violation_tag.as_str());
    // Confine writes: deny everything, then re-allow the writable set.
    out.push_str(&format!("(deny file-write* (subpath \"/\") {report})\n"));

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
        out.push_str(&format!("(deny file-read* {deny_rules} {report})\n"));
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

    // The operator credential boundary goes after every allow above, so no
    // worktree, grant, or read re-allow can reopen it.
    out.push_str(&floor.sbpl_rules());

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
///
/// Also makes sure the violation stream is watching, since it can only report
/// a denial made while it was running (see [`detect_violation`]).
pub(crate) fn wrap_argv(
    program: &str,
    args: &[String],
    policy: &SandboxPolicy,
) -> (String, Vec<String>) {
    let _ = ViolationStream::watching();
    wrap_argv_with_profile(
        program,
        args,
        sbpl_profile(policy, &OperatorCredentialFloor::for_this_machine()),
    )
}

/// Rewrite argv to run under an already-generated SBPL profile.
pub(crate) fn wrap_argv_with_profile(
    program: &str,
    args: &[String],
    profile: String,
) -> (String, Vec<String>) {
    let mut wrapped = vec!["-p".to_string(), profile, program.to_string()];
    wrapped.extend(args.iter().cloned());
    ("/usr/bin/sandbox-exec".to_string(), wrapped)
}

/// Build a space-joined run of `(subpath "...")` clauses for canonicalized
/// paths, each literal escaped for SBPL. A path that does not exist yet still
/// gets a clause: the rule is inert until something appears there, and emitting
/// it is what lets a confined process create a path the policy has granted.
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

/// A sandbox violation recovered from the unified log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxViolation {
    pub(crate) operation: String,
    pub(crate) path: PathBuf,
}

/// Recover the denied path for a violation that carries `tag`, made since
/// `since`. Best-effort: `None` when no violation stream is running or none
/// arrives in time (the caller then falls back to a command-scoped denial).
///
/// The kernel logs a violation as process 0, naming the process that made the
/// denied call only inside the message, so the spawned pid cannot select it;
/// the tag the spawn's profile attached to its deny rules does. `since` keeps
/// apart spawns that share one policy, and so one tag.
pub(crate) fn detect_violation(
    tag: &crate::ViolationTag,
    since: SystemTime,
) -> Option<SandboxViolation> {
    // A stream started now could not have seen the denial, so only one that
    // was already watching is asked.
    let stream = ViolationStream::current()?;
    stream.await_tagged(tag.as_str(), since, VIOLATION_ARRIVAL_WAIT)
}

/// How long a denial waits for its violation to arrive on the stream. The
/// kernel's entry normally lands well inside this; waiting is paid only by a
/// failed command whose output already reads as a permission denial.
const VIOLATION_ARRIVAL_WAIT: Duration = Duration::from_millis(1500);

/// What the kernel's Seatbelt extension is logged as sending from.
const KERNEL_SANDBOX_SENDER: &str = "/Sandbox";

/// A command that runs `argv` and stops it when this process exits.
///
/// macOS has no parent-death signal, so a long-lived child such as `log stream`
/// would outlive this process until its next matching entry. The wrapper ties
/// it to this process's lifetime instead: a background reader blocks on the
/// wrapper's stdin, which the caller must keep open and never write, and kills
/// the command when that pipe closes. The command's own stdout is the
/// wrapper's, and its stderr goes to /dev/null, so a command that exits on its
/// own still closes the caller's end.
fn tied_to_this_process(argv: &[&str]) -> Command {
    const WRAPPER: &str = r#"exec 3<&0
"$@" </dev/null &
l=$!
{ read -r _ <&3; kill "$l"; } >/dev/null 2>&1 &
wait "$l""#;
    let mut command = Command::new("/bin/sh");
    command
        .args(["-c", WRAPPER, "sh"])
        .args(argv)
        .stdin(Stdio::piped());
    command
}

/// One process-wide `log stream` of tagged kernel Seatbelt violations.
///
/// A stream rather than a `log show` query afterwards: the persisted log does
/// not hold these entries. Asked for a spawn's denials moments later, `log
/// show` returns nothing, top-level process or descendant alike, while a live
/// stream receives them as the kernel emits them.
pub(crate) struct ViolationStream {
    seen: Mutex<Seen>,
    arrived: Condvar,
    alive: AtomicBool,
    /// Held, never written: when this process exits the pipe closes, and the
    /// wrapper in [`ViolationStream::start`] stops `log stream` in response.
    _lifeline: Mutex<std::process::ChildStdin>,
}

#[derive(Default)]
struct Seen {
    violations: VecDeque<(SystemTime, String, SandboxViolation)>,
    /// Tagged lines that were not kernel violations, kept for a failing live
    /// test to show.
    #[cfg(test)]
    rejected: VecDeque<String>,
}

/// The running stream, or when it last failed to start, so a process that
/// cannot stream (a sandboxed one: `log` refuses to run there) does not pay a
/// spawn attempt for every fenced command.
static VIOLATION_STREAM: Mutex<Option<Result<Arc<ViolationStream>, Instant>>> = Mutex::new(None);

const STREAM_RETRY_AFTER: Duration = Duration::from_secs(60);
const STREAM_READY_WAIT: Duration = Duration::from_secs(2);
const KEPT_VIOLATIONS: usize = 1024;
const VIOLATION_RETENTION: Duration = Duration::from_secs(600);

impl ViolationStream {
    /// The running stream, starting one when there is none.
    pub(crate) fn watching() -> Option<Arc<Self>> {
        let mut slot = VIOLATION_STREAM.lock().ok()?;
        match slot.as_ref() {
            Some(Ok(stream)) if stream.alive.load(Ordering::Acquire) => {
                return Some(stream.clone())
            }
            Some(Err(failed)) if failed.elapsed() < STREAM_RETRY_AFTER => return None,
            _ => {}
        }
        let started = Self::start();
        *slot = Some(started.clone().ok_or_else(Instant::now));
        started
    }

    fn current() -> Option<Arc<Self>> {
        match VIOLATION_STREAM.lock().ok()?.as_ref()? {
            Ok(stream) => Some(stream.clone()),
            Err(_) => None,
        }
    }

    fn start() -> Option<Arc<Self>> {
        let predicate = format!("eventMessage CONTAINS \"{}\"", crate::ViolationTag::PREFIX);
        let mut child = tied_to_this_process(&[
            "/usr/bin/log",
            "stream",
            "--style",
            "ndjson",
            "--predicate",
            &predicate,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
        let lifeline = child.stdin.take()?;
        let stdout = child.stdout.take()?;
        let stream = Arc::new(Self {
            seen: Mutex::new(Seen::default()),
            arrived: Condvar::new(),
            alive: AtomicBool::new(true),
            _lifeline: Mutex::new(lifeline),
        });
        let (ready, first_line) = std::sync::mpsc::channel();
        let reader = stream.clone();
        std::thread::Builder::new()
            .name("cairn-sandbox-violations".into())
            .spawn(move || {
                let mut ready = Some(ready);
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    // `log stream` announces its filter before any entry.
                    if let Some(ready) = ready.take() {
                        let _ = ready.send(());
                    }
                    reader.record(&line);
                }
                reader.alive.store(false, Ordering::Release);
                reader.arrived.notify_all();
                let _ = child.wait();
            })
            .ok()?;
        match first_line.recv_timeout(STREAM_READY_WAIT) {
            Ok(()) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return None,
        }
        stream.alive.load(Ordering::Acquire).then_some(stream)
    }

    fn record(&self, line: &str) {
        let Ok(mut seen) = self.seen.lock() else {
            return;
        };
        match parse_violation_entry(line) {
            Some((tag, violation)) => {
                let now = SystemTime::now();
                let violations = &mut seen.violations;
                while violations.len() >= KEPT_VIOLATIONS
                    || violations.front().is_some_and(|(at, ..)| {
                        now.duration_since(*at).unwrap_or_default() > VIOLATION_RETENTION
                    })
                {
                    violations.pop_front();
                }
                violations.push_back((now, tag, violation));
                self.arrived.notify_all();
            }
            #[cfg(test)]
            None => {
                if seen.rejected.len() >= 16 {
                    seen.rejected.pop_front();
                }
                seen.rejected.push_back(line.to_string());
            }
            #[cfg(not(test))]
            None => {}
        }
    }

    fn await_tagged(
        &self,
        tag: &str,
        since: SystemTime,
        wait: Duration,
    ) -> Option<SandboxViolation> {
        let deadline = Instant::now() + wait;
        let mut seen = self.seen.lock().ok()?;
        loop {
            if let Some((.., violation)) = seen
                .violations
                .iter()
                .find(|(at, seen_tag, _)| seen_tag == tag && *at >= since)
            {
                return Some(violation.clone());
            }
            let left = deadline.checked_duration_since(Instant::now())?;
            if left.is_zero() || !self.alive.load(Ordering::Acquire) {
                return None;
            }
            seen = self.arrived.wait_timeout(seen, left).ok()?.0;
        }
    }

    #[cfg(test)]
    fn describe(&self, tag: &str) -> String {
        let Ok(seen) = self.seen.lock() else {
            return "violation stream state poisoned".into();
        };
        let ours: Vec<_> = seen
            .violations
            .iter()
            .filter(|(_, t, _)| t == tag)
            .collect();
        format!(
            "stream alive={}, {} violation(s) recorded, ours {ours:?}, rejected tagged lines {:?}",
            self.alive.load(Ordering::Acquire),
            seen.violations.len(),
            seen.rejected
        )
    }
}

/// Parse one `log stream --style ndjson` entry into the spawn tag it carries
/// and the violation it reports, if it is a tagged kernel Seatbelt violation.
///
/// A tagged violation message is the kernel's line with the rule's message
/// after it, on the same line or the next:
///   `Sandbox: touch(1234) deny(1) file-write-create /Users/x/escape.txt`
///   `cairn-fence-…`
/// The path is what follows the `deny(N) <operation>` prefix on the first line.
///
/// Only the kernel's own entries count. Any process in the tree can log text
/// shaped like a violation, tag included, but not as process 0 from the
/// Seatbelt extension, and the path parsed here becomes a crossing's scope.
/// The tag is the message's last one, because the rule's message is appended
/// after the path and a path can itself be named to look like a tag.
fn parse_violation_entry(line: &str) -> Option<(String, SandboxViolation)> {
    static DENY: OnceLock<regex::Regex> = OnceLock::new();
    static TAG: OnceLock<regex::Regex> = OnceLock::new();
    let deny = DENY.get_or_init(|| regex::Regex::new(r"deny\(\d+\)\s+(\S+)\s+(/.+)$").unwrap());
    let tag_re = TAG.get_or_init(|| {
        regex::Regex::new(&format!(
            "{}[0-9a-z]+",
            regex::escape(crate::ViolationTag::PREFIX)
        ))
        .unwrap()
    });
    let value: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    // The field is numeric in ndjson output.
    if value.get("processID")?.as_u64()? != 0 {
        return None;
    }
    if !value
        .get("senderImagePath")?
        .as_str()?
        .contains(KERNEL_SANDBOX_SENDER)
    {
        return None;
    }
    let msg = value.get("eventMessage")?.as_str()?;
    let tag = tag_re.find_iter(msg).last()?;
    let first = msg.lines().next()?;
    let violation = if tag.end() <= first.len() {
        &first[..tag.start()]
    } else {
        first
    };
    let caps = deny.captures(violation.trim())?;
    Some((
        tag.as_str().to_string(),
        SandboxViolation {
            operation: caps.get(1)?.as_str().to_string(),
            path: PathBuf::from(caps.get(2)?.as_str().trim()),
        },
    ))
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
            violation_tag: crate::ViolationTag::fresh(),
        };
        let profile = sbpl_profile(&policy, &test_floor());
        assert!(profile.starts_with("(version 1)\n(allow default)\n"));
        let report = format!("(with message \"{}\")", policy.violation_tag.as_str());
        assert!(profile.contains(&format!("(deny file-write* (subpath \"/\") {report})")));
        assert!(
            profile.contains(&format!("(subpath \"/home/x/.aws\") {report})")),
            "the read denylist reports the spawn's tag too:\n{profile}"
        );
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

    /// A floor pinned to a fixed home, so a profile's text does not depend on
    /// the machine running the test.
    fn test_floor() -> OperatorCredentialFloor {
        OperatorCredentialFloor::new(Some(PathBuf::from("/Users/tester")), [])
    }

    #[test]
    fn the_operator_credential_boundary_closes_every_profile() {
        // A worktree inside a Cairn home and a session grant of the home itself
        // are both re-allowed for reads; the boundary must still come after
        // them, because Seatbelt applies the last matching rule.
        let policy = SandboxPolicy::for_run(
            Path::new("/Users/tester/.cairn/scratch/job"),
            &["/Users/tester/.cairn-dev-x".to_string()],
            vec![],
        );
        let profile = sbpl_profile(&policy, &test_floor());
        let boundary = profile
            .find("(deny file-read-data file-write* (regex")
            .expect("the boundary is in every fenced profile");
        let last_allow = profile.rfind("(allow ").unwrap();
        assert!(
            boundary > last_allow,
            "the boundary must follow every allow:\n{profile}"
        );
        assert!(profile.contains(r#"^/Users/tester/\.cairn[^/]*/operator_auth_secret$"#));
    }

    #[test]
    fn wrap_argv_prefixes_sandbox_exec() {
        let policy = SandboxPolicy {
            worktree: PathBuf::from("/work/wt"),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            violation_tag: crate::ViolationTag::fresh(),
        };
        let (program, args) = wrap_argv("bash", &["-c".into(), "echo hi".into()], &policy);
        assert_eq!(program, "/usr/bin/sandbox-exec");
        assert_eq!(args[0], "-p");
        assert_eq!(args[2], "bash");
        assert_eq!(args[3], "-c");
        assert_eq!(args[4], "echo hi");
    }

    const TAG: &str = "cairn-fence-0123abcd";
    const KERNEL: &str = r#""processID":0,"senderImagePath":"/System/Library/Extensions/Sandbox.kext/Contents/MacOS/Sandbox""#;

    /// The first violation tagged `tag` among `ndjson`'s stream entries, the
    /// way [`ViolationStream`] selects it.
    fn parse_violation(ndjson: &str, tag: &str) -> Option<SandboxViolation> {
        ndjson
            .lines()
            .filter_map(super::parse_violation_entry)
            .find(|(seen, _)| seen == tag)
            .map(|(_, violation)| violation)
    }

    /// A path named to look like another spawn's tag cannot claim that spawn:
    /// the kernel appends the rule's message after the path, so the last tag
    /// is the one the profile attached.
    #[test]
    fn parse_violation_takes_the_rule_tag_not_a_tag_shaped_path() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: touch(5) deny(1) file-write-create /x/cairn-fence-victim\n{TAG}"}}"#
        );
        assert_eq!(
            super::parse_violation_entry(ndjson.trim()).map(|(tag, v)| (tag, v.path)),
            Some((TAG.to_string(), PathBuf::from("/x/cairn-fence-victim")))
        );
        assert_eq!(parse_violation(&ndjson, "cairn-fence-victim"), None);
    }

    #[test]
    fn parse_violation_path_extracts_denied_path() {
        // The rule's message follows the kernel's line; the process named in
        // the message is a child of the spawn, which no pid filter could find.
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: touch(1235) deny(1) file-write-create /Users/x/escape.txt\n{TAG}"}}
{{{KERNEL},"eventMessage":"some unrelated log line"}}"#
        );
        let violation = parse_violation(&ndjson, TAG);
        assert_eq!(
            violation,
            Some(SandboxViolation {
                operation: "file-write-create".into(),
                path: PathBuf::from("/Users/x/escape.txt"),
            })
        );
    }

    #[test]
    fn parse_violation_path_accepts_the_tag_on_the_violation_line() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: git(77) deny(1) file-write-create /repo/.git/FETCH_HEAD {TAG}"}}"#
        );
        assert_eq!(
            parse_violation(&ndjson, TAG).map(|v| v.path),
            Some(PathBuf::from("/repo/.git/FETCH_HEAD"))
        );
    }

    #[test]
    fn parse_violation_path_ignores_other_spawns_and_non_deny() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: cat(9) deny(1) file-read-data /Users/x/.aws/credentials\ncairn-fence-ffff"}}
{{{KERNEL},"eventMessage":"Sandbox: cat(9) deny(1) file-read-data /Users/x/.aws/config"}}
{{{KERNEL},"eventMessage":"allow file-read-data /Users/x/ok\n{TAG}"}}"#
        );
        assert_eq!(parse_violation(&ndjson, TAG), None);
    }

    /// The parsed path becomes a crossing's scope, and a process in the tree
    /// can log a line shaped like a violation, tag and all.
    #[test]
    fn parse_violation_path_ignores_entries_the_kernel_did_not_write() {
        let ndjson = format!(
            r#"{{"processID":1235,"senderImagePath":"/usr/bin/logger","eventMessage":"Sandbox: touch(1) deny(1) file-write-create /Users/x/.ssh\n{TAG}"}}
{{"processID":0,"senderImagePath":"/usr/lib/libSystem.B.dylib","eventMessage":"Sandbox: touch(1) deny(1) file-write-create /Users/x/.ssh\n{TAG}"}}
{{"processID":1235,"senderImagePath":"/System/Library/Extensions/Sandbox.kext/Contents/MacOS/Sandbox","eventMessage":"Sandbox: touch(1) deny(1) file-write-create /Users/x/.ssh\n{TAG}"}}"#
        );
        assert_eq!(parse_violation(&ndjson, TAG), None);
    }

    #[test]
    fn parse_violation_path_handles_read_denials() {
        let ndjson = format!(
            r#"{{{KERNEL},"eventMessage":"Sandbox: cat(42) deny(1) file-read-data /home/u/.ssh/id_rsa\n{TAG}"}}"#
        );
        assert_eq!(
            parse_violation(&ndjson, TAG),
            Some(SandboxViolation {
                operation: "file-read-data".into(),
                path: PathBuf::from("/home/u/.ssh/id_rsa"),
            })
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
    // macOS sandboxes do not nest: inside an outer sandbox a nested
    // `sandbox-exec` cannot write even where its own profile permits it, so there
    // is no policy left to observe. That refusal IS the behavior here, which makes
    // these the one family of tests a skip legitimately belongs to — but the skip
    // has to state a measured fact rather than a guess about the environment.
    // Reading `CAIRN_SANDBOXED` was such a guess, and it was wrong in both
    // directions: inherited down a spawn chain where no sandbox existed, and
    // absent where one did (CAIRN-3164). So each live test PROBES the capability
    // and records a skip naming it, declared in `src-tauri/skip-manifest.toml`.
    //
    // The probe costs one process spawn and every live test asks, so it resolves
    // once per test binary.
    fn nested_sandbox_exec_available() -> bool {
        static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *AVAILABLE.get_or_init(|| {
            let Ok(dir) = tempdir() else {
                return false;
            };
            // The narrowest form of what every live test below does: a write the
            // profile explicitly permits.
            let policy = SandboxPolicy {
                worktree: dir.path().to_path_buf(),
                writable_extra: vec![],
                deny_read: vec![],
                writable_regex: vec![],
                worktree_writable: true,
                violation_tag: crate::ViolationTag::fresh(),
            };
            let probe = dir.path().join("probe");
            let (program, args) = wrap_argv(
                "/bin/bash",
                &["-c".into(), format!("echo ok > {}", probe.display())],
                &policy,
            );
            Command::new(program)
                .args(args)
                .output()
                .is_ok_and(|out| out.status.success())
                && probe.exists()
        })
    }

    fn skip_without_nested_sandbox_exec(test: &str) -> bool {
        if nested_sandbox_exec_available() {
            return false;
        }
        eprintln!("skipping {test}: a nested sandbox-exec cannot write inside its own profile");
        cairn_common::skip_ledger::record_skip(test, "nested-sandbox-exec");
        true
    }

    #[test]
    fn enforces_write_confinement_and_read_denylist_live() {
        if skip_without_nested_sandbox_exec("enforces_write_confinement_and_read_denylist_live") {
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
            violation_tag: crate::ViolationTag::fresh(),
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
        if skip_without_nested_sandbox_exec("worktree_under_denied_prefix_stays_readable_live") {
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
            violation_tag: crate::ViolationTag::fresh(),
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

    // The interactive-shell grant has two properties worth proving at the
    // kernel rather than in the derivation: a shell can create its own state on
    // a home that has never run it, and nothing else under the same XDG base
    // becomes writable in the process.
    #[test]
    fn interactive_shell_state_grant_creates_shell_paths_but_not_neighbours_live() {
        if skip_without_nested_sandbox_exec(
            "interactive_shell_state_grant_creates_shell_paths_but_not_neighbours_live",
        ) {
            return;
        }
        let wt = tempdir().unwrap();
        let home = tempdir().unwrap();
        let data = home.path().join(".local/share");
        // The host ensures the base exists; the shell's own dir does not yet.
        std::fs::create_dir_all(&data).unwrap();
        let neighbour = data.join("some-other-app");
        std::fs::create_dir_all(&neighbour).unwrap();

        let policy = SandboxPolicy {
            worktree: wt.path().to_path_buf(),
            writable_extra: vec![data.join("fish"), data.join("z")],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            violation_tag: crate::ViolationTag::fresh(),
        };
        let run = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        let fish = data.join("fish");
        let out = run(&format!(
            "mkdir -p {0} && echo hi > {0}/fish_history",
            fish.display()
        ));
        assert!(
            out.status.success(),
            "a first-run shell must be able to create its own state: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(fish.join("fish_history").exists());

        let out = run(&format!("echo x > {}/db", neighbour.display()));
        assert!(
            !out.status.success(),
            "an unrelated application's state under the same base must stay unwritable"
        );

        let out = run(&format!("mkdir {}/newly-invented-app", data.display()));
        assert!(
            !out.status.success(),
            "the XDG base itself must stay unwritable"
        );
    }

    /// What a read-only-checkout profile actually costs, at the kernel. Dropping
    /// the checkout from the writable set also drops every carve-out that
    /// contains it, so the writable set narrows to the explicit grants: the
    /// checkout's own build directory and any Cairn state directory beside it
    /// (a jj store, a sibling cell) become unwritable while both stay readable.
    ///
    /// That is the mechanism behind every specimen denial in CAIRN-3227. Which
    /// spawns receive this profile is the fence dial's decision and lives in
    /// `cairn-core`; what the profile does once applied is this.
    #[test]
    fn readonly_checkout_profile_denies_the_checkout_and_neighbouring_state_live() {
        if skip_without_nested_sandbox_exec(
            "readonly_checkout_profile_denies_the_checkout_and_neighbouring_state_live",
        ) {
            return;
        }
        let base = tempdir().unwrap();
        let checkout = base.path().join("live-checkout");
        let build_dir = checkout.join("target");
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::write(checkout.join("Cargo.toml"), "[package]").unwrap();
        // Stands in for `~/.cairn/jj-stores/<store>`: Cairn's own state, beside
        // the checkout and in nobody's writable set.
        let store = base.path().join("cairn-home/jj-stores/store");
        std::fs::create_dir_all(&store).unwrap();
        // Stands in for the per-job scratch the executor grants explicitly.
        let scratch = base.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();

        let policy = SandboxPolicy::for_readonly_checkout(
            &checkout,
            &[scratch.to_string_lossy().into_owned()],
            vec![],
        );
        let confined = |cmd: &str| -> std::process::Output {
            let (program, args) = wrap_argv("/bin/bash", &["-c".into(), cmd.into()], &policy);
            Command::new(program).args(args).output().unwrap()
        };

        let build_probe = build_dir.join("probe");
        let store_probe = store.join("working_copy.lock");
        let scratch_probe = scratch.join("probe");
        let write = |path: &Path| format!("echo x > {}", path.display());

        assert!(
            !confined(&write(&build_probe)).status.success(),
            "a read-only checkout denies its own build directory"
        );
        assert!(
            !confined(&write(&store_probe)).status.success(),
            "Cairn's own state beside the checkout is denied too"
        );
        assert!(
            confined(&format!("cat {}", checkout.join("Cargo.toml").display()))
                .status
                .success(),
            "the checkout stays readable"
        );
        assert!(
            confined(&write(&scratch_probe)).status.success(),
            "an explicit grant stays writable"
        );

        // The control is the whole point of the dial: with no policy applied —
        // what every spawn surface produces for a `fence: allow` agent — the same
        // three operations are ordinary filesystem writes.
        std::fs::remove_file(&scratch_probe).unwrap();
        for path in [&build_probe, &store_probe, &scratch_probe] {
            let out = Command::new("/bin/bash")
                .args(["-c", &write(path)])
                .output()
                .unwrap();
            assert!(
                out.status.success() && path.exists(),
                "unconfined spawn must perform the specimen write: {}",
                path.display()
            );
        }
    }

    /// Run `script` under `policy` and recover its denial the way a fenced
    /// spawn's caller does, with what the violation stream saw attached for a
    /// failure to show.
    fn denial_under(
        policy: &SandboxPolicy,
        script: String,
    ) -> (Option<crate::SandboxDenial>, String) {
        let since = SystemTime::now();
        let (program, args) = wrap_argv("/bin/sh", &["-c".into(), script], policy);
        let out = Command::new(program).args(args).output().unwrap();
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let denial = crate::detect_denial(
            out.status.code(),
            &output,
            Some(&policy.violation_tag),
            since,
            true,
        );
        let stream = ViolationStream::current().map_or_else(
            || "no violation stream is running (`log stream` did not start)".to_string(),
            |stream| stream.describe(policy.violation_tag.as_str()),
        );
        (
            denial,
            format!("exit {:?}, output {output:?}; {stream}", out.status.code()),
        )
    }

    fn outside_write_policy(worktree: &Path) -> SandboxPolicy {
        // Empty writable_extra so a sibling temp dir is genuinely outside the
        // writable set.
        SandboxPolicy {
            worktree: worktree.to_path_buf(),
            writable_extra: vec![],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            violation_tag: crate::ViolationTag::fresh(),
        }
    }

    /// The kernel logs a violation under the process that made the denied
    /// call. Here that is `touch`, a child of the spawned `sh` (the trailing
    /// `; true` stops sh from exec-ing it in place), so a lookup keyed on the
    /// spawned pid finds nothing and the prompt would blame `sh -c`.
    #[test]
    fn a_denial_made_by_a_child_process_is_recovered_by_path_live() {
        if skip_without_nested_sandbox_exec(
            "a_denial_made_by_a_child_process_is_recovered_by_path_live",
        ) {
            return;
        }
        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let outside = outside.path().canonicalize().unwrap();
        let policy = outside_write_policy(wt.path());
        let target = outside.join("x");
        let (denial, evidence) = denial_under(&policy, format!("touch {}; true", target.display()));
        assert!(!target.exists(), "the write must have been refused");
        assert!(
            matches!(&denial, Some(crate::SandboxDenial::Path { path, .. }) if *path == target),
            "expected a path-scoped denial of {}, got {denial:?}; {evidence}",
            target.display()
        );
    }

    /// Two spawns denied in the same window each recover their own path, never
    /// the other's: the tag, not the time window, is what attributes a denial.
    #[test]
    fn concurrent_spawns_recover_only_their_own_denials_live() {
        if skip_without_nested_sandbox_exec("concurrent_spawns_recover_only_their_own_denials_live")
        {
            return;
        }
        let wt = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let outside = outside.path().canonicalize().unwrap();
        let (first, second) = (outside.join("first"), outside.join("second"));
        let first_policy = outside_write_policy(wt.path());
        let second_policy = outside_write_policy(wt.path());
        let first_script = format!("exec touch {}", first.display());
        let second_script = format!("touch {}; true", second.display());
        let (first_denial, first_evidence) = std::thread::scope(|scope| {
            let first = scope.spawn(|| denial_under(&first_policy, first_script));
            let (second_denial, second_evidence) = denial_under(&second_policy, second_script);
            assert!(
                matches!(&second_denial, Some(crate::SandboxDenial::Path { path, .. }) if *path == second),
                "got {second_denial:?}; {second_evidence}"
            );
            first.join().unwrap()
        });
        assert!(
            matches!(&first_denial, Some(crate::SandboxDenial::Path { path, .. }) if *path == first),
            "got {first_denial:?}; {first_evidence}"
        );
    }

    /// The stream must not outlive the process that started it. Closing the
    /// lifeline is what this process's exit does to it, so a stand-in command
    /// that would otherwise run for ten minutes has to stop within moments, and
    /// the wrapper only exits once its command has.
    #[test]
    fn a_tied_command_stops_when_its_lifeline_closes() {
        let mut child = tied_to_this_process(&["/bin/sleep", "600"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let lifeline = child.stdin.take().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            child.try_wait().unwrap().is_none(),
            "the command must keep running while its lifeline is open"
        );
        drop(lifeline);
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("the tied command outlived its lifeline");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Every fenced spawn asks for the stream, and only the first starts one:
    /// a stream per spawn would cost a `log` process each and would miss
    /// violations made before it was ready.
    #[test]
    fn a_second_spawn_reuses_the_running_violation_stream_live() {
        if skip_without_nested_sandbox_exec(
            "a_second_spawn_reuses_the_running_violation_stream_live",
        ) {
            return;
        }
        let first = ViolationStream::watching().expect("an unconfined process can stream the log");
        let second = ViolationStream::watching().expect("the stream is still running");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn nested_write_probe_is_authoritative_when_allow_default_false_negatives() {
        let allow_default_claims_unconfined = !current_process_is_confined();
        let measured = crate::probe_nested_sandbox_write();
        if allow_default_claims_unconfined && !measured.available {
            assert!(
                measured.detail.contains("refused") || measured.detail.contains("could not"),
                "the measured refusal must retain actionable provenance: {}",
                measured.detail
            );
        }
        // Capability classification is the behavioral result in every state;
        // the allow-default probe is intentionally not consulted.
        assert_eq!(
            measured.available,
            crate::probe_nested_sandbox_write().available
        );
    }

    #[test]
    fn writable_extra_carveout_permits_writes_live() {
        if skip_without_nested_sandbox_exec("writable_extra_carveout_permits_writes_live") {
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
            violation_tag: crate::ViolationTag::fresh(),
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
        let profile = sbpl_profile(&policy, &test_floor());

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
        let profile = sbpl_profile(&policy, &test_floor());
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
        let profile = sbpl_profile(&policy, &test_floor());
        assert!(profile.contains("(allow file-write*"));
        // The worktrees-target glob is emitted as an anchored SBPL regex.
        assert!(
            profile.contains("(regex #\"^/home/u/\\.cairn/worktrees/.*/target/.*\")"),
            "profile missing worktrees-target regex allow:\n{profile}"
        );
    }

    /// A throwaway OS home holding a normal Cairn home and a dev-instance home,
    /// each with both secrets, plus a same-named fixture outside any home.
    struct CredentialFixture {
        _dir: tempfile::TempDir,
        root: PathBuf,
        os_home: PathBuf,
        floor: OperatorCredentialFloor,
    }

    const SECRET_MARKER: &str = "OPERATOR-SECRET-MARKER-32-BYTES!";

    impl CredentialFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            let os_home = root.join("home");
            for home in [".cairn", ".cairn-dev-agent-cairn-1-builder-0"] {
                let home = os_home.join(home);
                std::fs::create_dir_all(&home).unwrap();
                std::fs::write(home.join("operator_auth_secret"), SECRET_MARKER).unwrap();
                std::fs::write(home.join("mcp_auth_secret"), "mcp-secret").unwrap();
            }
            std::fs::create_dir_all(root.join("fixture")).unwrap();
            std::fs::write(root.join("fixture/operator_auth_secret"), "fixture").unwrap();
            let floor = OperatorCredentialFloor::new(Some(os_home.clone()), []);
            Self {
                _dir: dir,
                root,
                os_home,
                floor,
            }
        }

        fn dev_secret(&self) -> PathBuf {
            self.os_home
                .join(".cairn-dev-agent-cairn-1-builder-0/operator_auth_secret")
        }

        fn run(&self, profile: &str, script: &str) -> std::process::Output {
            let (program, args) = wrap_argv_with_profile(
                "/bin/bash",
                &["-c".into(), script.into()],
                profile.to_string(),
            );
            Command::new(program)
                .args(args)
                .current_dir(&self.root)
                .output()
                .unwrap()
        }

        fn floor_only_profile(&self) -> String {
            format!("(version 1)\n(allow default)\n{}", self.floor.sbpl_rules())
        }
    }

    fn leaked(out: &std::process::Output) -> bool {
        String::from_utf8_lossy(&out.stdout).contains(SECRET_MARKER)
    }

    // Live: every way of naming a Cairn home's operator credential that the
    // boundary exists to refuse, under the floor an unfenced agent spawn gets.
    #[test]
    fn operator_credential_boundary_refuses_every_spelling_live() {
        if skip_without_nested_sandbox_exec(
            "operator_credential_boundary_refuses_every_spelling_live",
        ) {
            return;
        }
        use std::os::unix::fs::MetadataExt as _;

        let fixture = CredentialFixture::new();
        let profile = fixture.floor_only_profile();
        let secret = fixture.dev_secret();
        let home = secret.parent().unwrap().to_path_buf();
        let root = &fixture.root;
        let metadata = std::fs::metadata(&secret).unwrap();
        let mut attempts = vec![
            ("direct", format!("cat {}", secret.display())),
            (
                "normal home",
                format!("cat {}/.cairn/operator_auth_secret", fixture.os_home.display()),
            ),
            (
                "relative",
                format!("cd {} && cat operator_auth_secret", home.display()),
            ),
            (
                "dot segments",
                format!(
                    "cd {} && cat ./.cairn/../.cairn-dev-agent-cairn-1-builder-0/./operator_auth_secret",
                    fixture.os_home.display()
                ),
            ),
            (
                "symlinked file",
                format!("ln -s {} {r}/l1 && cat {r}/l1", secret.display(), r = root.display()),
            ),
            (
                "symlinked home",
                format!(
                    "ln -s {} {r}/l2 && cat {r}/l2/operator_auth_secret",
                    home.display(),
                    r = root.display()
                ),
            ),
            (
                "case variant",
                format!("cat {}/OPERATOR_AUTH_SECRET", home.display()),
            ),
            (
                "hard link",
                format!("ln {} {r}/h1 && cat {r}/h1", secret.display(), r = root.display()),
            ),
            (
                "APFS clone",
                format!("cp -c {} {r}/c1 && cat {r}/c1", secret.display(), r = root.display()),
            ),
            (
                "inode path",
                format!("cat /.vol/{}/{}", metadata.dev(), metadata.ino()),
            ),
            (
                "rename the file",
                format!("mv {} {r}/m1 && cat {r}/m1", secret.display(), r = root.display()),
            ),
            (
                "rename the home",
                format!(
                    "mv {} {r}/moved && cat {r}/moved/operator_auth_secret",
                    home.display(),
                    r = root.display()
                ),
            ),
        ];
        let firmlink = Path::new("/System/Volumes/Data");
        if firmlink.is_dir() {
            attempts.push((
                "data-volume firmlink",
                format!("cat {}{}", firmlink.display(), secret.display()),
            ));
        }

        for (spelling, script) in attempts {
            let out = fixture.run(&profile, &script);
            assert!(
                !leaked(&out),
                "{spelling} read the operator credential: {script}"
            );
        }
        assert!(
            secret.exists(),
            "no attempt may move the credential out from under its home"
        );
    }

    // Live: the boundary takes nothing else. The MCP secret beside it, the
    // home's other files, a new dev-instance home, and a same-named fixture in a
    // temp directory all behave exactly as they do with no profile at all.
    #[test]
    fn operator_credential_boundary_leaves_ordinary_work_alone_live() {
        if skip_without_nested_sandbox_exec(
            "operator_credential_boundary_leaves_ordinary_work_alone_live",
        ) {
            return;
        }
        let fixture = CredentialFixture::new();
        let profile = fixture.floor_only_profile();
        let home = fixture.dev_secret().parent().unwrap().to_path_buf();
        for (what, script) in [
            ("read the MCP secret", format!("cat {}/mcp_auth_secret", home.display())),
            ("stat the credential", format!("ls -l {}", fixture.dev_secret().display())),
            (
                "write inside a home",
                format!("echo x > {h}/new && mkdir {h}/sub", h = home.display()),
            ),
            (
                "create a new dev-instance home",
                format!(
                    "mkdir {h}/.cairn-dev-new && echo x > {h}/.cairn-dev-new/f",
                    h = fixture.os_home.display()
                ),
            ),
            (
                "use a same-named fixture outside any home",
                format!(
                    "cat {r}/fixture/operator_auth_secret && echo y > {r}/fixture/operator_auth_secret",
                    r = fixture.root.display()
                ),
            ),
        ] {
            let out = fixture.run(&profile, &script);
            assert!(
                out.status.success(),
                "{what} must still work: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    // Live: a family home that is a symlink to a directory elsewhere is read
    // through its real location, which the family pattern never spells; the
    // discovered floor covers it there. And no agent may plant such a link.
    #[test]
    fn a_relocated_home_and_a_planted_link_are_both_refused_live() {
        if skip_without_nested_sandbox_exec(
            "a_relocated_home_and_a_planted_link_are_both_refused_live",
        ) {
            return;
        }
        let fixture = CredentialFixture::new();
        let peer = fixture.root.join("volumes/peer-home");
        std::fs::create_dir_all(&peer).unwrap();
        std::fs::write(peer.join("operator_auth_secret"), SECRET_MARKER).unwrap();
        std::os::unix::fs::symlink(&peer, fixture.os_home.join(".cairn-dev-peer")).unwrap();
        let floor = OperatorCredentialFloor::discovered(Some(fixture.os_home.clone()), []);
        let profile = format!("(version 1)\n(allow default)\n{}", floor.sbpl_rules());

        for spelling in [
            peer.join("operator_auth_secret"),
            fixture.os_home.join(".cairn-dev-peer/operator_auth_secret"),
        ] {
            let out = fixture.run(&profile, &format!("cat {}", spelling.display()));
            assert!(!leaked(&out), "{} was readable", spelling.display());
        }

        let planted = fixture.os_home.join(".cairn-dev-future");
        let out = fixture.run(
            &profile,
            &format!("ln -s {} {}", fixture.root.display(), planted.display()),
        );
        assert!(
            !out.status.success() && !planted.exists(),
            "a symlink planted at a family name would relocate a future home"
        );
        let out = fixture.run(&profile, &format!("mkdir {}", planted.display()));
        assert!(out.status.success(), "creating a real home stays allowed");
    }

    // Live: a fenced profile whose worktree and grants cover a Cairn home still
    // refuses its credential, because the boundary is the profile's last word.
    #[test]
    fn a_fenced_profile_cannot_reopen_the_operator_credential_live() {
        if skip_without_nested_sandbox_exec(
            "a_fenced_profile_cannot_reopen_the_operator_credential_live",
        ) {
            return;
        }
        let fixture = CredentialFixture::new();
        let home = fixture.dev_secret().parent().unwrap().to_path_buf();
        let policy = SandboxPolicy {
            worktree: home.clone(),
            writable_extra: vec![fixture.os_home.clone()],
            deny_read: vec![],
            writable_regex: vec![],
            worktree_writable: true,
            violation_tag: crate::ViolationTag::fresh(),
        };
        let profile = sbpl_profile(&policy, &fixture.floor);
        let out = fixture.run(&profile, &format!("cat {}", fixture.dev_secret().display()));
        assert!(!leaked(&out), "a fence grant reopened the credential");
        let out = fixture.run(&profile, &format!("cat {}/mcp_auth_secret", home.display()));
        assert!(out.status.success(), "the worktree itself stays readable");
    }

    // Live: a service sandbox writes worktree `target/` trees (the cross-worktree
    // grant) and its own state dir, but NOT worktree source or arbitrary paths.
    // This is the regression guard for the Managed Build Services fix.
    #[test]
    fn service_sandbox_permits_worktree_target_writes_only_live() {
        if skip_without_nested_sandbox_exec(
            "service_sandbox_permits_worktree_target_writes_only_live",
        ) {
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
            violation_tag: crate::ViolationTag::fresh(),
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
