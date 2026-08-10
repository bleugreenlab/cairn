//! OS-level filesystem sandbox applied at the process-spawn seam.
//!
//! The `run` verb's commands execute in a Cairn-owned host process: ordinary
//! workspace commands are spawned by the runner/server, while build-slot batches
//! are spawned by the supervised `cairn-executor`. Neither path inherits the
//! agent CLI's own sandbox. A worktree boundary therefore has to be enforced by
//! wrapping each command at its actual spawn seam. This module builds
//! a per-spawn [`SandboxPolicy`] and applies it to the spawned `Command`:
//!
//! - **macOS**: rewrite argv to `sandbox-exec -p <SBPL profile> <program> …`.
//! - **Linux**: install a `landlock` ruleset in a `pre_exec` hook (child-side,
//!   before `exec`).
//! - **Windows / other**: no kernel primitive — runs unconfined (documented).
//!
//! The policy confines **writes** to `{worktree, tmp, granted paths}`, allows
//! **reads broadly** (the toolchain reads `~/.cargo`, `~/.npm`, `~/.gitconfig`,
//! `/usr/lib`, …), and **hard-denies reads** of a narrow sensitive denylist of
//! external credential stores (see [`default_deny_read`], which deliberately
//! does *not* deny Cairn's own `~/.cairn[-dev]`). See `docs/worktree-fence.md`.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Whether this process is already running under an inherited kernel sandbox.
///
/// A child sandbox can narrow an inherited policy but cannot widen it. Long-lived
/// machine services must therefore refuse to launch from a confined supervisor.
#[cfg(target_os = "macos")]
pub fn current_process_is_confined() -> bool {
    macos::current_process_is_confined()
}

#[cfg(not(target_os = "macos"))]
pub fn current_process_is_confined() -> bool {
    false
}

/// A per-spawn filesystem confinement profile.
///
/// Built fresh for each spawn from the current worktree and the session's
/// granted crossings, so a user-approved crossing path widens the very next
/// spawn's writable set (and drops out of the read denylist).
#[derive(Debug, Clone)]
pub struct SandboxPolicy {
    /// The agent's worktree — writable (and always readable).
    pub worktree: PathBuf,
    /// Additional writable subpaths: temp dirs, toolchain caches, and
    /// session-granted crossings. Also re-allowed for reads.
    pub writable_extra: Vec<PathBuf>,
    /// Subpaths whose reads are denied — the narrow set of obviously-sensitive
    /// secrets (external credential stores). For an `Ask` agent a denied read
    /// still surfaces as an approvable crossing; only `Deny` makes it final.
    pub deny_read: Vec<PathBuf>,
    /// Additional writable scopes expressed as anchored regexes rather than
    /// concrete subpaths. These let a long-lived **service** sandbox (see
    /// [`SandboxPolicy::for_service`]) grant writes across many sibling paths
    /// that do not yet exist — e.g. every worktree's `target/` tree — which a
    /// fixed `subpath` list cannot express. Empty for ordinary `for_run`
    /// policies. Enforced on macOS via SBPL `(regex ...)`; the Linux landlock
    /// path is allowlist-by-concrete-fd and does not yet translate these (the
    /// service sandbox is macOS-first, mirroring the read-denylist gap).
    pub writable_regex: Vec<String>,
    /// Whether the `worktree` root is writable. True for an ordinary worktree run
    /// and for a service. False for a **read-only-checkout** policy (see
    /// [`SandboxPolicy::for_readonly_checkout`]), where the project's live
    /// checkout must stay readable but every write into it is kernel-denied. The
    /// worktree is always in [`readable_paths`](Self::readable_paths) regardless;
    /// this flag only gates the write-allow.
    pub worktree_writable: bool,
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    // Compare canonical forms so macOS /var -> /private/var and /tmp ->
    // /private/tmp aliases cannot smuggle an ancestor write allowance back over
    // a read-only checkout.
    let a = a.canonicalize().unwrap_or_else(|_| a.to_path_buf());
    let b = b.canonicalize().unwrap_or_else(|_| b.to_path_buf());
    a.starts_with(&b) || b.starts_with(&a)
}

impl SandboxPolicy {
    /// Build a policy for a worktree run.
    ///
    /// `granted` is the set of session-allowed crossing descriptors (resolved
    /// paths for path crossings). Any granted path is added to the writable set,
    /// which is re-allowed for reads after the denylist — so an approved crossing
    /// wins over a denylist entry whose subtree contains it.
    pub fn for_run(worktree: &Path, granted: &[String], deny_read: Vec<PathBuf>) -> Self {
        let granted_paths: Vec<PathBuf> = granted
            .iter()
            .filter(|g| g.starts_with('/'))
            .map(PathBuf::from)
            .collect();

        let mut writable_extra = default_writable_extra();
        writable_extra.extend(granted_paths);

        Self {
            worktree: worktree.to_path_buf(),
            writable_extra,
            deny_read,
            writable_regex: Vec::new(),
            worktree_writable: true,
        }
    }

    /// Build a policy for a non-worktree run on the project's **live checkout**.
    ///
    /// Like [`for_run`](Self::for_run) but the checkout itself is **read-only**:
    /// it stays in the readable set (the agent reads the project source) but is
    /// dropped from the writable set, so any write into the live checkout is
    /// kernel-denied. Only temp dirs, toolchain caches, and session grants stay
    /// writable. This makes "a non-worktree agent cannot leave a file behind in
    /// the live checkout" true at the kernel, complementing the read-only
    /// dirt-detection warning. See `docs/worktree-fence.md`.
    pub fn for_readonly_checkout(
        checkout: &Path,
        granted: &[String],
        deny_read: Vec<PathBuf>,
    ) -> Self {
        let granted_paths: Vec<PathBuf> = granted
            .iter()
            .filter(|g| g.starts_with('/'))
            .map(PathBuf::from)
            // A grant within the read-only checkout (or an ancestor whose subpath
            // grant would include it) must NOT re-open the checkout to writes: the
            // read-only guarantee outranks any session grant. Dropped here so the
            // policy is bulletproof regardless of caller.
            .filter(|p| !paths_overlap(p, checkout))
            .collect();

        let mut writable_extra = default_writable_extra();
        // The read-only checkout outranks every writable carve-out, including a
        // temp/toolchain root that happens to contain it. An ancestor allowance
        // would otherwise re-open the checkout on allowlist sandboxes.
        writable_extra.retain(|path| !paths_overlap(path, checkout));
        writable_extra.extend(granted_paths);

        Self {
            worktree: checkout.to_path_buf(),
            writable_extra,
            deny_read,
            writable_regex: Vec::new(),
            worktree_writable: false,
        }
    }

    /// Build a policy for a long-lived **build service** daemon (see
    /// `docs/worktree-fence.md` — Managed Build Services).
    ///
    /// A service is shared across every worktree it serves, so it cannot be
    /// confined to a single worktree like `for_run`. Instead it is allowed to
    /// write only its own `state_dir` (its cache/state home), the standard
    /// temp/toolchain dirs, and the configured `writable_globs` (the explicit
    /// cross-worktree grant, e.g. `{worktrees}/**/target/**`). Reads stay broad
    /// minus `deny_read`, so the daemon still cannot read external secret
    /// stores, and it notably cannot write worktree source, `$HOME`, or secrets.
    ///
    /// `writable_globs` are already template-expanded absolute glob patterns;
    /// each is converted to an anchored regex for the OS layer.
    pub fn for_service(
        state_dir: &Path,
        writable_globs: &[String],
        deny_read: Vec<PathBuf>,
    ) -> Self {
        let mut writable_extra = default_writable_extra();
        writable_extra.push(state_dir.to_path_buf());

        Self {
            // The daemon's state dir is its primary writable+readable root; it
            // stands in for `worktree` (a service has no single worktree).
            worktree: state_dir.to_path_buf(),
            writable_extra,
            deny_read,
            writable_regex: writable_globs.iter().map(|g| glob_to_regex(g)).collect(),
            worktree_writable: true,
        }
    }

    /// Grant the writes an **interactive shell** makes on its own behalf.
    ///
    /// A PTY terminal deliberately boots the user's real shell, so that shell's
    /// startup config runs inside the fence: it rewrites its universal
    /// variables, appends history, and lets plugins update their state under the
    /// XDG data and state homes. None of that is the work a fence exists to
    /// contain — it is the cost of having chosen the user's shell over a bare
    /// one — and denying it makes every terminal open print kernel-denial
    /// stanzas before the prompt appears (`mktemp: … Operation not permitted`,
    /// fish's `Unable to open universal variable file`). Commands typed into the
    /// shell inherit the spawn's confinement, so this widens what they may write
    /// by exactly the named leaves below, and by nothing else.
    ///
    /// Opt-in per spawn, because only the terminal seam boots a shell: a `run`
    /// batch executes through `/bin/sh -c`, which sources no user config and
    /// needs nothing here.
    ///
    /// The grant names individual leaves — the shell's own directory under each
    /// XDG base, and the state directories of plugins that write as a side
    /// effect of the shell running. The bases themselves stay unwritable,
    /// because a write rule is recursive and granting `~/.local/share` would
    /// hand every command typed into the terminal write access to every
    /// application's state beneath it.
    ///
    /// Ensures those bases exist as a side effect: the kernel permits creating a
    /// granted leaf that does not exist yet, but only when its parent already
    /// does (see [`ensure_shell_state_bases`]).
    pub fn grant_interactive_shell_state(&mut self, shell_path: &str) {
        ensure_shell_state_bases();
        self.grant_state_dirs(interactive_shell_state_dirs(shell_path));
    }

    /// Add writable roots, skipping any the read denylist covers. A grant is
    /// re-allowed for reads (see [`Self::readable_paths`]), so admitting a
    /// denied subtree here would silently undo the denial.
    fn grant_state_dirs(&mut self, dirs: Vec<PathBuf>) {
        for dir in dirs {
            if self
                .deny_read
                .iter()
                .any(|denied| paths_overlap(&dir, denied))
            {
                continue;
            }
            if !self.writable_extra.contains(&dir) {
                self.writable_extra.push(dir);
            }
        }
    }

    /// Writable subpaths that drive the OS **write-allow**: the worktree (only
    /// when [`worktree_writable`](Self::worktree_writable)) + extras (temp +
    /// toolchain + grants). A read-only-checkout policy drops the worktree here so
    /// writes into the live checkout are kernel-denied.
    pub fn writable_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        if self.worktree_writable {
            paths.push(self.worktree.clone());
        }
        paths.extend(self.writable_extra.iter().cloned());
        paths
    }

    /// Readable subpaths re-allowed after the read denylist: the worktree
    /// (**always**, even for a read-only checkout) + extras. So a denylist entry
    /// covering a prefix of the checkout can never block the agent from reading
    /// its own source, and a read-only checkout stays fully readable while its
    /// writes are denied.
    #[cfg(any(test, target_os = "macos"))]
    fn readable_paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.worktree.clone()];
        paths.extend(self.writable_extra.iter().cloned());
        paths
    }
}

/// The default read denylist, anchored at the user's home directory.
///
/// Deliberately **narrow** — hard-deny is a last resort for *obviously sensitive*
/// secrets the agent cannot otherwise reach: external credential stores and
/// private keys (`~/.aws`, `~/.config/gcloud`, `~/.netrc`, `~/.ssh`, `~/.gnupg`).
///
/// Cairn's own `~/.cairn[-dev]` is **not** denied: its DB contents are already
/// reachable through `cairn://` resources, the callback secret is injected into
/// every spawn's env regardless, and the workspace packages (`skills/`,
/// `agents/`, `recipes/`, `tools/`, `resources/`) plus the worktree itself live
/// there and must stay readable. The agent reading its own state dir is
/// expected, not a leak. `~/.config/gh` is likewise not denied — the toolchain
/// reads it to push, and it is already an app-scoped token.
///
/// (For an `Ask` agent these denials are not impassable: a denied read surfaces
/// as an approvable crossing and a grant lets it through. Only `Deny` agents
/// hard-fail. `~/.ssh` is conservative — an ssh git remote needs a one-time
/// grant or its removal via settings.)
pub fn default_deny_read() -> Vec<PathBuf> {
    match home_dir() {
        Some(h) => default_deny_read_in(&h),
        None => Vec::new(),
    }
}

fn default_deny_read_in(home: &Path) -> Vec<PathBuf> {
    [".aws", ".config/gcloud", ".netrc", ".ssh", ".gnupg"]
        .iter()
        .map(|rel| home.join(rel))
        .collect()
}

/// Whether OS-level sandboxing is available on this platform/host at runtime.
///
/// macOS: `sandbox-exec` is always present. Linux: requires a landlock-capable
/// kernel (ABI ≥ 1, kernel 5.13+). Other platforms: unavailable.
pub fn is_available() -> bool {
    #[cfg(target_os = "macos")]
    {
        true
    }
    #[cfg(target_os = "linux")]
    {
        linux::is_available()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        false
    }
}

/// Rewrite a spawn's `(program, args)` to apply the sandbox.
///
/// On macOS this returns the `sandbox-exec`-wrapped invocation. On Linux and
/// other platforms the argv is unchanged (Linux confines via `pre_exec`; see
/// [`install_pre_exec`]).
pub fn wrap_argv(program: &str, args: &[String], policy: &SandboxPolicy) -> (String, Vec<String>) {
    #[cfg(target_os = "macos")]
    {
        macos::wrap_argv(program, args, policy)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = policy;
        (program.to_string(), args.to_vec())
    }
}

/// Install the sandbox on a `std::process::Command` for platforms that confine
/// in-process (Linux landlock via `pre_exec`). No-op on macOS (argv-wrapped)
/// and unsupported platforms.
pub fn install_pre_exec(cmd: &mut std::process::Command, policy: &SandboxPolicy) {
    #[cfg(target_os = "linux")]
    {
        linux::install_pre_exec(cmd, policy);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (cmd, policy);
    }
}

/// A detected sandbox denial, used to drive the worktree fence after a command
/// was blocked by the kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxDenial {
    /// A precise denied path was recovered (macOS unified log) — the fence can
    /// raise a path-scoped crossing whose grant generalizes across commands.
    Path {
        path: PathBuf,
        /// Kernel operation reported by the sandbox log, such as
        /// `file-write-create`, when the platform exposes it.
        operation: Option<String>,
    },
    /// A denial occurred but no path was recovered — the fence raises a
    /// command-scoped crossing.
    Command,
}

/// Detect whether a sandboxed command was blocked by the kernel.
///
/// The unified trigger across platforms: command output carrying a
/// permission-denial signature. A clean final shell exit suppresses synthetic
/// command-scoped fallback for Deny agents, but not for Ask agents: shells can
/// mask an earlier sandbox denial with `;`, `||`, traps, or harness cleanup. On
/// macOS the precise denied path is recovered from the unified log (best-effort),
/// upgrading a command-scoped denial to a path-scoped one.
/// `command_scoped_fallback` lets Ask agents raise a recoverable prompt when
/// macOS path recovery misses while Deny agents keep the raw command failure.
/// Only call this when the sandbox was actually applied (worktree agent with
/// `OnEscape` Ask/Deny).
pub fn detect_denial(
    exit_code: Option<i32>,
    combined_output: &str,
    pid: Option<u32>,
    since: SystemTime,
    command_scoped_fallback: bool,
) -> Option<SandboxDenial> {
    // A clean final shell exit can mask an earlier sandbox denial (`cmd; echo
    // $?`, `cmd || true`, traps, harness cleanup). For Ask agents, keep looking
    // for a denial signature so the user has a recovery path. For Deny agents
    // (fallback disabled), preserve the historical raw-output behavior.
    if exit_code == Some(0) && !command_scoped_fallback {
        return None;
    }
    // Gate on a denial signature so ordinary exits (test failures, grep
    // no-match, successful commands) never raise the fence — and so the macOS log
    // query only runs for plausibly-denied commands.
    if !has_denial_signature(combined_output) {
        return None;
    }

    #[cfg(target_os = "macos")]
    {
        // Prefer the authoritative, path-scoped kernel log violation. For Ask
        // agents, fall back to a recoverable command-scoped crossing if the log
        // lookup misses; for Deny agents, preserve the historical fail-fast raw
        // command output rather than synthesizing a denial.
        match pid {
            Some(pid) => match macos::detect_violation(pid, since) {
                Some(violation) => Some(SandboxDenial::Path {
                    path: violation.path,
                    operation: Some(violation.operation),
                }),
                None if command_scoped_fallback => Some(SandboxDenial::Command),
                None => None,
            },
            None if command_scoped_fallback => Some(SandboxDenial::Command),
            None => None,
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        // No out-of-band violation signal; the signature is the only trigger
        // (command-scoped). This can false-positive on a genuine non-sandbox
        // permission error — documented in docs/worktree-fence.md. Keep Linux
        // behavior unchanged for both Ask and Deny agents.
        let _ = (pid, since, command_scoped_fallback);
        Some(SandboxDenial::Command)
    }
}

/// Whether output carries a filesystem permission-denial signature.
fn has_denial_signature(output: &str) -> bool {
    const SIGS: [&str; 4] = [
        "Operation not permitted",
        "Permission denied",
        "EACCES",
        "EPERM",
    ];
    SIGS.iter().any(|s| output.contains(s))
}

/// The static writable carve-outs shared by the OS sandbox **and** the `write`
/// verb's fence: temp dirs + toolchain cache/state dirs (cargo registry +
/// package-cache lock, npm cache, …). `$HOME` is otherwise read-only, so without
/// these a cold-cache or dependency-adding `cargo build`/`npm ci` would be
/// kernel-denied. Excludes session grants (those flow through the fence's grant
/// check) and the worktree. The `write` verb treats a write here as in-sandbox
/// (no prompt), matching a shell write under `run`.
pub fn default_writable_extra() -> Vec<PathBuf> {
    let mut dirs = temp_dirs();
    dirs.extend(toolchain_writable_dirs());
    // Cairn-managed shared uv package cache (`<cairn_home>/uv-cache`), pointed at
    // by `UV_CACHE_DIR` on every agent spawn. Existence-filtered like the
    // toolchain dirs; the host creates it at startup (`env::ensure_uv_cache_dir`),
    // so a fenced agent can populate it without tripping an out-of-worktree
    // write, while uv's default `~/.cache/uv` stays out of the picture.
    if let Some(uv_cache) = std::env::var_os("UV_CACHE_DIR").map(PathBuf::from) {
        if uv_cache.exists() {
            dirs.push(uv_cache);
        }
    }
    // Cairn's own per-job scratch root (`<cairn_home>/scratch`), which every
    // spawn's `TMPDIR`/`TMP`/`TEMP` may name and whose job directory is the
    // residence of a command that has no checkout. It used to live under the
    // platform temp root and be covered by that wholesale grant; naming it here
    // is what keeps a scratch write prompt-free now that the root is readable
    // instead. Granted unconditionally rather than existence-filtered: the very
    // first spawn of a fresh install writes there, and an absent path is inert
    // in the generated rules.
    dirs.push(cairn_common::scratch::scratch_root());
    // Cairn's own log directory is deliberately absent; see
    // `the_cairn_log_dir_is_never_writable`.
    dirs
}

/// Standard temp directories that must stay writable so the toolchain (cargo,
/// rustc, npm, git) can use scratch space.
/// Toolchain cache/state dirs that must stay writable so build tools work with
/// their shared caches in `$HOME`. Confining writes to the worktree alone breaks
/// cargo (`~/.cargo` registry cache + `.package-cache` lock), npm (`~/.npm`),
/// and friends — they write to these on a cold cache or when a dependency is
/// added. Only existing dirs are emitted (a missing one is inert in the rules).
fn toolchain_writable_dirs() -> Vec<PathBuf> {
    match home_dir() {
        Some(h) => toolchain_writable_dirs_in(&h),
        None => Vec::new(),
    }
}

fn toolchain_writable_dirs_in(home: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = [
        ".cargo",
        ".rustup",
        ".npm",
        ".bun",
        ".yarn",
        ".cache",
        ".pnpm-store",
        ".local/share/pnpm",
        ".gradle",
        ".m2",
        ".deno",
        "go",
        // macOS XDG-cache equivalent (go-build, many tools).
        "Library/Caches",
    ]
    .iter()
    .map(|rel| home.join(rel))
    .collect();
    dirs.retain(|p| p.exists());
    dirs
}

/// Directory names of shell plugins that write state as a side effect of the
/// shell simply running: directory trackers that record every `cd`, and history
/// stores. Each is named individually because the XDG bases they sit under also
/// hold unrelated application state, and a write rule is recursive — granting a
/// base would hand every command typed into the terminal write access to all of
/// it. A plugin that is not listed surfaces as an ordinary, visible denial,
/// which is the signal to add it here deliberately rather than to widen the
/// boundary.
const SHELL_PLUGIN_STATE_DIRS: [&str; 5] = ["z", "zoxide", "autojump", "fasd", "atuin"];

/// The paths an interactive shell writes for its own bookkeeping: its own
/// directory beneath each XDG base (fish keeps `fish_variables` under the config
/// home and its history under the data home), the state directories of the
/// plugins in [`SHELL_PLUGIN_STATE_DIRS`], and the history files zsh and bash
/// keep directly in `$HOME`.
///
/// Deliberately **not** filtered to paths that already exist: a grant naming a
/// path that does not exist yet is exactly what lets a first-run shell create
/// it, and the OS rule is inert until something does. The XDG bases are never
/// granted, only entered.
pub fn interactive_shell_state_dirs(shell_path: &str) -> Vec<PathBuf> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    interactive_shell_state_dirs_in(
        &home,
        &xdg_base(&home, "XDG_CONFIG_HOME", ".config"),
        &xdg_base(&home, "XDG_DATA_HOME", ".local/share"),
        &xdg_base(&home, "XDG_STATE_HOME", ".local/state"),
        shell_path,
    )
}

/// Best-effort creation of the XDG base directories a shell keeps its state
/// under.
///
/// The grants name leaves beneath these bases rather than the bases themselves,
/// which is what keeps unrelated application state out of the writable set. The
/// kernel will let a confined shell create a granted leaf that does not exist
/// yet — but only if the parent already does, since `mkdir -p
/// ~/.local/share/fish` on a fresh home must create `~/.local/share` first and
/// that path is not granted. Creating the standard bases before confining the
/// shell is what makes a first-run home behave like an established one. Mirrors
/// `env::ensure_uv_cache_dir`, which exists for the same reason.
fn ensure_shell_state_bases() {
    let Some(home) = home_dir() else {
        return;
    };
    for base in [
        xdg_base(&home, "XDG_CONFIG_HOME", ".config"),
        xdg_base(&home, "XDG_DATA_HOME", ".local/share"),
        xdg_base(&home, "XDG_STATE_HOME", ".local/state"),
    ] {
        let _ = std::fs::create_dir_all(base);
    }
}

/// Resolve an XDG base directory, falling back to its default under `home`. A
/// relative value is ignored, as the XDG spec requires.
fn xdg_base(home: &Path, var: &str, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(|| home.join(fallback))
}

fn interactive_shell_state_dirs_in(
    home: &Path,
    config_home: &Path,
    data_home: &Path,
    state_home: &Path,
    shell_path: &str,
) -> Vec<PathBuf> {
    let shell = shell_basename(shell_path);
    let mut dirs = Vec::new();
    if !shell.is_empty() {
        // nushell is the one common shell whose dir is not its binary name.
        let name = if shell == "nu" { "nushell" } else { &shell };
        dirs.push(config_home.join(name));
        dirs.push(data_home.join(name));
        dirs.push(state_home.join(name));
    }
    for plugin in SHELL_PLUGIN_STATE_DIRS {
        dirs.push(data_home.join(plugin));
        dirs.push(state_home.join(plugin));
    }
    // Shells that keep history in `$HOME` rather than under an XDG base.
    let home_state: &[&str] = match shell.as_str() {
        "zsh" => &[".zsh_history", ".zsh_sessions"],
        "bash" => &[".bash_history"],
        _ => &[],
    };
    dirs.extend(home_state.iter().map(|relative| home.join(relative)));
    dirs
}

/// The shell's lowercased binary name, without directory or `.exe` suffix.
fn shell_basename(shell_path: &str) -> String {
    Path::new(shell_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_lowercase()
        .trim_end_matches(".exe")
        .to_string()
}

fn temp_dirs() -> Vec<PathBuf> {
    let mut dirs = vec![std::env::temp_dir()];
    for p in [
        "/tmp",
        "/private/tmp",
        "/var/folders",
        "/private/var/folders",
    ] {
        let path = PathBuf::from(p);
        if path.exists() && !dirs.contains(&path) {
            dirs.push(path);
        }
    }
    dirs
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
}

/// Convert an absolute writable glob into a start-anchored regex string for the
/// OS sandbox layer (macOS SBPL `(regex ...)`).
///
/// Glob semantics: `**` matches any depth (including `/`), `*` matches within a
/// single path segment. The result is anchored at the start (`^`) but not the
/// end, so it matches the glob's prefix and everything beneath it (subpath
/// semantics). Regex metacharacters in the literal portions are escaped.
pub fn glob_to_regex(glob: &str) -> String {
    let chars: Vec<char> = glob.chars().collect();
    let mut re = String::from("^");
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => {
                if i + 1 < chars.len() && chars[i + 1] == '*' {
                    // `**` — any depth, including path separators.
                    re.push_str(".*");
                    i += 2;
                } else {
                    // `*` — within a single segment.
                    re.push_str("[^/]*");
                    i += 1;
                }
            }
            // Escape regex metacharacters that can appear in literal path parts.
            c @ ('.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '?' | '|' | '\\') => {
                re.push('\\');
                re.push(c);
                i += 1;
            }
            c => {
                re.push(c);
                i += 1;
            }
        }
    }
    re
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_denylist_is_external_secrets_only() {
        // Pure: no global-env mutation, so it can't race other tests.
        let deny = default_deny_read_in(Path::new("/home/tester"));
        // External credential stores + keys are denied (the narrow last resort).
        assert!(deny.contains(&PathBuf::from("/home/tester/.aws")));
        assert!(deny.contains(&PathBuf::from("/home/tester/.ssh")));
        assert!(deny.contains(&PathBuf::from("/home/tester/.gnupg")));
        assert!(deny.contains(&PathBuf::from("/home/tester/.config/gcloud")));
        // Cairn's own state dir is NOT denied: DB contents are URI-reachable,
        // the secret is in env, and skills/agents/recipes + the worktree live
        // there and must stay readable.
        assert!(!deny.contains(&PathBuf::from("/home/tester/.cairn")));
        assert!(!deny.contains(&PathBuf::from("/home/tester/.cairn/cairn.db")));
        assert!(!deny.contains(&PathBuf::from("/home/tester/.cairn/skills")));
    }

    #[test]
    fn interactive_shell_state_dirs_are_scoped_to_the_shell_not_the_xdg_bases() {
        let home = Path::new("/home/tester");
        let (config, data, state) = (
            home.join(".config"),
            home.join(".local/share"),
            home.join(".local/state"),
        );

        let dirs =
            interactive_shell_state_dirs_in(home, &config, &data, &state, "/opt/homebrew/bin/fish");

        // The shell's own directory beneath each base: `fish_variables` under
        // the config home, history under the data home.
        assert!(dirs.contains(&config.join("fish")));
        assert!(dirs.contains(&data.join("fish")));
        assert!(dirs.contains(&state.join("fish")));
        // Plugins that write as a side effect of the shell running.
        assert!(dirs.contains(&data.join("z")));
        // The bases themselves are never granted. A write rule is recursive, so
        // granting one would open every application's state beside the shell's
        // to anything typed into the terminal.
        assert!(!dirs.contains(&config));
        assert!(!dirs.contains(&data));
        assert!(!dirs.contains(&state));
        assert!(!dirs.contains(&home.to_path_buf()));
    }

    #[test]
    fn interactive_shell_state_dirs_are_creation_safe_and_shell_specific() {
        // A home where nothing exists yet: the paths are granted all the same,
        // because naming a path that does not exist is what lets a first-run
        // shell create it.
        let home = Path::new("/home/never-logged-in");
        let (config, data, state) = (
            home.join(".config"),
            home.join(".local/share"),
            home.join(".local/state"),
        );

        let dirs = interactive_shell_state_dirs_in(home, &config, &data, &state, "/bin/zsh");

        assert!(dirs.contains(&data.join("zsh")));
        assert!(dirs.contains(&home.join(".zsh_history")));
        assert!(
            !dirs.contains(&home.join(".bash_history")),
            "another shell's history is not this shell's state"
        );
        assert!(
            !dirs.contains(&config.join("fish")),
            "another shell's config is not this shell's state"
        );
    }

    #[test]
    fn interactive_shell_grant_never_reopens_a_denied_root() {
        let denied = PathBuf::from("/home/x/.local/share");
        let mut policy = SandboxPolicy::for_run(Path::new("/work/wt"), &[], vec![denied.clone()]);

        policy.grant_state_dirs(vec![
            denied.clone(),
            denied.join("z"),
            PathBuf::from("/home/x/.config/fish"),
        ]);

        // A grant is re-allowed for reads, so a denied root — or anything inside
        // one — must never enter the writable set through this door.
        assert!(!policy.writable_extra.contains(&denied));
        assert!(!policy.writable_extra.contains(&denied.join("z")));
        assert!(policy
            .writable_extra
            .contains(&PathBuf::from("/home/x/.config/fish")));

        policy.grant_state_dirs(vec![PathBuf::from("/home/x/.config/fish")]);
        assert_eq!(
            policy
                .writable_extra
                .iter()
                .filter(|path| *path == &PathBuf::from("/home/x/.config/fish"))
                .count(),
            1,
            "repeated grants must not accumulate duplicates"
        );
    }

    #[test]
    fn for_run_grant_is_writable_and_read_reallowed() {
        let wt = PathBuf::from("/work/wt");
        let deny = vec![PathBuf::from("/home/x/.aws"), PathBuf::from("/secret/data")];
        let granted = vec!["/secret/data".to_string()];
        let policy = SandboxPolicy::for_run(&wt, &granted, deny);

        // Granted path is writable (and re-allowed for reads via the writable
        // set), so it wins over the denylist entry without mutating the denylist.
        assert!(policy
            .writable_extra
            .contains(&PathBuf::from("/secret/data")));
        assert!(policy
            .writable_paths()
            .contains(&PathBuf::from("/secret/data")));
        assert!(policy.deny_read.contains(&PathBuf::from("/secret/data")));
        assert!(policy.deny_read.contains(&PathBuf::from("/home/x/.aws")));
        // Worktree is always writable (and thus readable via the re-allow).
        assert!(policy.writable_paths().contains(&wt));
    }

    #[test]
    fn for_readonly_checkout_drops_cwd_from_writable_but_keeps_it_readable() {
        let checkout = PathBuf::from("/project/live");
        let granted = vec!["/scratch/ok".to_string()];
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &granted, vec![]);

        // The live checkout is NOT writable: a write into it is kernel-denied,
        // even though it is the policy's `worktree` root.
        assert!(
            !policy.writable_paths().contains(&checkout),
            "the live checkout must be dropped from the writable set"
        );
        // But it stays readable so the agent can read project source.
        assert!(
            policy.readable_paths().contains(&checkout),
            "the live checkout must stay in the readable set"
        );
        // Session grants and the temp/toolchain carve-outs remain writable.
        assert!(policy
            .writable_paths()
            .contains(&PathBuf::from("/scratch/ok")));
        for p in default_writable_extra() {
            assert!(
                policy.writable_paths().contains(&p),
                "toolchain/temp carve-out must stay writable: {}",
                p.display()
            );
        }
    }

    /// Cairn's scratch root left the platform temp root, which the fence grants
    /// wholesale; without a grant of its own, every ordinary scratch write
    /// (`mktemp`, a compiler's temp file, a helper script) would become an
    /// out-of-worktree crossing.
    #[test]
    fn the_cairn_scratch_root_stays_writable() {
        let scratch = cairn_common::scratch::scratch_root();
        assert!(
            default_writable_extra().contains(&scratch),
            "the scratch root must be writable: {}",
            scratch.display()
        );
        let policy = SandboxPolicy::for_run(Path::new("/project/wt"), &[], vec![]);
        assert!(policy.writable_paths().contains(&scratch));
        assert!(policy.readable_paths().contains(&scratch));
    }

    /// Cairn's own log directory must never join the writable set, however
    /// tempting the nested `cairn` CLI makes it.
    ///
    /// A fenced spawn confines an agent's whole process tree, not the one Cairn
    /// binary inside it, so a grant meant to let the CLI keep its log would hand
    /// arbitrary branch code the ability to truncate, delete, or fabricate the
    /// durable logs of the desktop app, the runner, and every concurrent job.
    /// `default_writable_extra` is also what the `write` verb classifies
    /// absolute targets against, and that check is ancestor-based, so every file
    /// beneath the directory would become writable with no prompt on that
    /// surface too. Logs are the evidence trail bug reports are built from;
    /// shared durable state stays protected even from a well-motivated grant.
    ///
    /// Nothing is lost by denying it. A CLI whose log destination is unwritable
    /// still runs — `cairn_common::logging::init` degrades to a fileless logger
    /// — and the CLI is a thin client that forwards to the app, so the
    /// substantive record of what a call did is written by that unfenced process
    /// regardless.
    #[test]
    fn the_cairn_log_dir_is_never_writable() {
        // Derived from the home rather than through `cairn_log_dir`, whose
        // `CAIRN_LOG_DIR` override could name a temp dir the fence grants for
        // unrelated reasons. This is the canonical host location being pinned.
        let logs = cairn_common::paths::cairn_home().join("logs");
        assert!(
            !default_writable_extra().contains(&logs),
            "the Cairn log directory must not be writable: {}",
            logs.display()
        );
        let policy = SandboxPolicy::for_run(Path::new("/project/wt"), &[], vec![]);
        assert!(
            !policy.writable_paths().contains(&logs),
            "no run policy may carry the log directory into its writable set"
        );
    }

    #[test]
    fn for_readonly_checkout_drops_checkout_covering_grants() {
        // No session grant — not even one for the checkout itself, a path inside
        // it, or an ancestor whose subpath grant would include it — may re-open
        // the read-only checkout to writes.
        let checkout = PathBuf::from("/project/live");
        let granted = vec![
            "/project/live".to_string(),     // the checkout itself
            "/project/live/src".to_string(), // inside the checkout
            "/project".to_string(),          // ancestor (subpath would include it)
            "/scratch/ok".to_string(),       // safely outside
        ];
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &granted, vec![]);
        let writable = policy.writable_paths();
        assert!(!writable.contains(&checkout));
        assert!(!writable.contains(&PathBuf::from("/project/live/src")));
        assert!(!writable.contains(&PathBuf::from("/project")));
        // A safely-outside grant still applies.
        assert!(writable.contains(&PathBuf::from("/scratch/ok")));
        // And the checkout stays readable throughout.
        assert!(policy.readable_paths().contains(&checkout));
    }

    #[test]
    fn for_readonly_checkout_drops_overlapping_default_writable_roots() {
        let temp = tempfile::tempdir().unwrap();
        let checkout = temp.path().join("cairn-readonly-checkout");
        std::fs::create_dir(&checkout).unwrap();
        let policy = SandboxPolicy::for_readonly_checkout(&checkout, &[], vec![]);
        assert!(policy
            .writable_paths()
            .iter()
            .all(|path| !(path.starts_with(&checkout) || checkout.starts_with(path))));
        assert!(policy.readable_paths().contains(&checkout));
    }

    #[test]
    fn for_run_keeps_cwd_writable_and_readable() {
        let wt = PathBuf::from("/work/wt");
        let policy = SandboxPolicy::for_run(&wt, &[], vec![]);
        assert!(policy.writable_paths().contains(&wt));
        assert!(policy.readable_paths().contains(&wt));
    }

    #[test]
    fn no_denial_on_success_or_unrelated_failure_without_signature() {
        let t = SystemTime::now();
        // Clean exit with no signature: never a denial.
        assert_eq!(detect_denial(Some(0), "all good", None, t, false), None);
        assert_eq!(detect_denial(Some(0), "all good", None, t, true), None);
        // Non-zero but no signature: a normal failure, not a fence.
        assert_eq!(
            detect_denial(Some(1), "test failed: 2 errors", None, t, false),
            None
        );
        assert_eq!(
            detect_denial(Some(1), "test failed: 2 errors", None, t, true),
            None
        );
    }

    #[test]
    fn streaming_signature_helper_detects_known_denials() {
        assert!(has_denial_signature(
            "bash: /tmp/x: Operation not permitted"
        ));
        assert!(has_denial_signature("Permission denied"));
        assert!(has_denial_signature("EACCES"));
        assert!(has_denial_signature("EPERM"));
        assert!(!has_denial_signature("ordinary test failure"));
    }

    #[test]
    fn ask_fallback_detects_denial_even_when_shell_exit_is_masked() {
        let t = SystemTime::now();
        let output = "bash: /Users/u/probe: Operation not permitted\nexit=1";
        // Ask fallback still prompts: `cmd; echo exit=$?` can make the shell's
        // final status zero even though the sandbox blocked an earlier write.
        assert_eq!(
            detect_denial(Some(0), output, None, t, true),
            Some(SandboxDenial::Command)
        );
        // Deny/no-fallback preserves raw output for clean final exits.
        assert_eq!(detect_denial(Some(0), output, None, t, false), None);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn denial_signature_without_path_is_command_scoped() {
        let t = SystemTime::now();
        // No out-of-band signal: the signature is the trigger (command-scoped).
        assert_eq!(
            detect_denial(
                Some(1),
                "bash: /etc/x: Operation not permitted",
                None,
                t,
                false,
            ),
            Some(SandboxDenial::Command)
        );
        assert_eq!(
            detect_denial(Some(13), "open: Permission denied", None, t, true),
            Some(SandboxDenial::Command)
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_signature_without_logged_violation_is_ask_fallback_only() {
        let t = SystemTime::now();
        // Ask agents get a recoverable command-scoped crossing when the unified
        // log path lookup misses (represented here by pid=None).
        assert_eq!(
            detect_denial(
                Some(1),
                "bash: /etc/x: Operation not permitted",
                None,
                t,
                true,
            ),
            Some(SandboxDenial::Command)
        );
        // Deny agents preserve the old fail-fast raw-output behavior.
        assert_eq!(
            detect_denial(
                Some(1),
                "bash: /etc/x: Operation not permitted",
                None,
                t,
                false,
            ),
            None
        );
    }

    #[test]
    fn toolchain_caches_are_writable_when_present() {
        // Pure: cargo/npm caches present in a home → they are in the writable set,
        // so cold-cache / dependency-adding builds are not kernel-denied.
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".cargo")).unwrap();
        std::fs::create_dir_all(home.path().join(".npm")).unwrap();

        let dirs = toolchain_writable_dirs_in(home.path());
        assert!(dirs.contains(&home.path().join(".cargo")));
        assert!(dirs.contains(&home.path().join(".npm")));
        // A cache dir that doesn't exist is not emitted (inert in the rules).
        assert!(!dirs.contains(&home.path().join(".gradle")));
    }

    #[test]
    fn glob_to_regex_converts_glob_semantics() {
        // `**` spans path separators; `*` stays within a segment; `.` is escaped.
        assert_eq!(
            glob_to_regex("/home/u/.cairn/worktrees/**/target/**"),
            "^/home/u/\\.cairn/worktrees/.*/target/.*"
        );
        assert_eq!(glob_to_regex("/a/*/b"), "^/a/[^/]*/b");
        // A concrete worktree target path matches the worktrees glob.
        let re =
            regex::Regex::new(&glob_to_regex("/home/u/.cairn/worktrees/**/target/**")).unwrap();
        assert!(re.is_match("/home/u/.cairn/worktrees/CAIRN-1/src-tauri/target/release/deps/x.d"));
        // A worktree source path does NOT match (writes there stay denied).
        assert!(!re.is_match("/home/u/.cairn/worktrees/CAIRN-1/src-tauri/src/lib.rs"));
    }

    #[test]
    fn for_service_grants_state_dir_and_globs_only() {
        let state = PathBuf::from("/home/u/.cairn/sccache");
        let policy = SandboxPolicy::for_service(
            &state,
            &["/home/u/.cairn/worktrees/**/target/**".to_string()],
            vec![PathBuf::from("/home/u/.aws")],
        );
        // State dir is writable+readable; the worktrees glob is a regex grant.
        assert!(policy.writable_paths().contains(&state));
        assert_eq!(
            policy.writable_regex,
            vec!["^/home/u/\\.cairn/worktrees/.*/target/.*".to_string()]
        );
        // Secret store stays denied.
        assert!(policy.deny_read.contains(&PathBuf::from("/home/u/.aws")));
    }

    #[test]
    fn for_run_ignores_non_path_grants() {
        // A normalized-command descriptor (no leading slash) is not a path grant.
        let policy = SandboxPolicy::for_run(
            Path::new("/work/wt"),
            &["sudo rm -rf /".to_string()],
            vec![PathBuf::from("/home/x/.aws")],
        );
        assert!(!policy
            .writable_extra
            .contains(&PathBuf::from("sudo rm -rf /")));
        assert!(policy.deny_read.contains(&PathBuf::from("/home/x/.aws")));
    }
}
