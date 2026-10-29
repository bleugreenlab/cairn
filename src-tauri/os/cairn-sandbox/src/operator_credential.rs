//! The operator credential boundary, enforced at the process seam.
//!
//! A Cairn home holds two shared secrets of the same shape and opposite
//! audiences (see `cairn_common::auth`). `operator_auth_secret` authenticates the
//! person at the desktop answering an authority prompt, so a process that can
//! read it can approve its own escalation. An agent and the desktop run as the
//! same OS user, so file modes cannot tell them apart, and a refusal in the
//! `read` and `write` verb handlers does nothing about a shell that opens the
//! file itself.
//!
//! This module draws the boundary where shells actually run. On macOS, every
//! agent shell Cairn spawns — run batches, terminals, REPLs, workflow runtimes —
//! carries the rules below whatever its fence says: they are appended last to
//! every fenced profile, so no writable or readable grant can reopen them, and a
//! spawn the fence leaves unconfined gets them as its whole profile
//! ([`Confinement::CredentialFloor`]). Branch code Cairn runs as its own
//! verification carries them on any machine that holds a credential
//! ([`Confinement::for_branch_code`]). They are not a containment decision, so
//! nothing grants past them.
//!
//! Not yet covered: a dev-instance runtime, which runs the branch's code as a
//! Cairn install that must create, read, and confine with its own credential.
//! Until it runs under a separate boundary of its own, agent-authored
//! dev-runtime code can read operator credentials on its machine.
//!
//! The rules cover every Cairn home on the machine: each `.cairn*` directory
//! directly in the OS user's home (`~/.cairn`, `~/.cairn-dev`, every
//! `~/.cairn-dev-<key>` dev-instance home, an executor's own home), plus any home
//! the spawning process knows about outside that family, such as its own
//! `CAIRN_HOME` override. Within those homes:
//!
//! - the credential file refuses data reads and every write, which also refuses
//!   hard-linking it, cloning it, and renaming it away;
//! - the home directory itself refuses unlink, so it cannot be renamed out of the
//!   pattern that names it. Creating a new home, and everything else inside an
//!   existing one, stays allowed.
//!
//! Seatbelt evaluates a rule against the resolved real path, so a relative path,
//! `..`, a symlinked file or directory, a case variant, the
//! `/System/Volumes/Data` firmlink, and a `/.vol/<device>/<inode>` path all reach
//! the same rule. A file named `operator_auth_secret` anywhere else is untouched,
//! which is why the rules name homes rather than the file name alone: the tests
//! that exercise the credential store create it in temp directories, and they
//! must keep working inside an agent's own `cargo test`.
//!
//! Linux landlock can only allowlist, so it cannot refuse one file inside a
//! readable tree, and Windows has no sandbox primitive here. Neither platform
//! enforces this boundary. That is reported rather than assumed: the containment
//! edge `OperatorCredentialRead` is measured on every executor by an attempted
//! read through the same wrapping a spawn gets (see
//! [`crate::containment::probe_operator_credential_read`]).

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

use cairn_common::auth::OPERATOR_SECRET_FILE;

use crate::SandboxPolicy;

/// The name every Cairn home directly inside the OS user's home begins with.
const HOME_FAMILY_PREFIX: &str = ".cairn";

/// What an agent is told when the operator credential boundary refuses it.
///
/// Says plainly that no version of this succeeds, so it does not read as a
/// transient failure, a file permission, or a fence crossing worth asking about.
pub const OPERATOR_CREDENTIAL_REFUSAL: &str = concat!(
    "Denied by the operator credential boundary. A Cairn home's `operator_auth_secret` ",
    "(in ~/.cairn, a dev-instance home such as ~/.cairn-dev-<branch>, or any other Cairn home) ",
    "is the credential the person at the desktop uses to answer authority prompts, so a process ",
    "holding it could approve its own escalation. No agent process may read, copy, link, rename, ",
    "or write it, and the kernel refuses the access below the tool layer. This is not a file ",
    "permission or a containment crossing: no fence answer, `allowAll`, or permission response ",
    "changes it. Authority requests are answered by the operator in the desktop app."
);

/// Where operator credentials live on this machine, as the boundary sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorCredentialFloor {
    /// The OS user's home, resolved. Every `.cairn*` directory directly inside
    /// it is a Cairn home.
    os_home: Option<PathBuf>,
    /// Cairn homes outside that family, resolved. Each gets its own rules.
    homes: Vec<PathBuf>,
    /// Every home this process was told about, family member or not, resolved.
    /// Checked directly for a credential, so knowing a home never depends on
    /// being able to list the directory it sits in.
    named: Vec<PathBuf>,
}

impl OperatorCredentialFloor {
    /// Every Cairn home this process can name: the `.cairn*` family under the
    /// OS user's home, the real location of any family member that resolves
    /// elsewhere, and this process's own home wherever an override put it.
    pub fn for_this_machine() -> Self {
        Self::discovered(
            cairn_common::paths::os_home_dir(),
            [cairn_common::paths::cairn_home()],
        )
    }

    /// [`Self::new`], plus the real location of every family member that is a
    /// symlink or otherwise resolves outside the family.
    ///
    /// Seatbelt judges a path by where it resolves, so a home at
    /// `~/.cairn-dev-peer -> /Volumes/data/peer-home` is read through
    /// `/Volumes/data/peer-home/operator_auth_secret`, which the family pattern
    /// does not name. Such a home is covered by its target instead. The rules
    /// also refuse creating a symlink at a family name, so no agent can plant
    /// one for a home that does not exist yet.
    ///
    /// What this cannot see is a peer install whose `CAIRN_HOME` was set by
    /// hand to a directory outside the family and that no `.cairn*` entry
    /// points at. Cairn's own tooling never creates one (`dev:instance` homes
    /// are `~/.cairn-dev-<key>`, executors keep theirs in the family), and an
    /// install covers its own home in every spawn it makes.
    pub fn discovered(os_home: Option<PathBuf>, homes: impl IntoIterator<Item = PathBuf>) -> Self {
        let mut floor = Self::new(os_home, homes);
        for target in floor.relocated_family_homes() {
            floor = floor.with_home(target);
        }
        floor
    }

    /// The family's members, or the error that kept this process from listing
    /// them. An unlistable home is not an empty one.
    fn family_members(&self) -> std::io::Result<Vec<PathBuf>> {
        let Some(os_home) = &self.os_home else {
            return Ok(Vec::new());
        };
        Ok(std::fs::read_dir(os_home)?
            .flatten()
            .filter(|entry| name_has_prefix(&entry.file_name(), HOME_FAMILY_PREFIX))
            .map(|entry| os_home.join(entry.file_name()))
            .collect())
    }

    fn relocated_family_homes(&self) -> Vec<PathBuf> {
        // An unlistable home hides relocated members, but the family pattern
        // still covers every lexical member, so the rules are not weakened.
        self.family_members()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|member| {
                let target = member.canonicalize().ok()?;
                (!same_path(&target, &member) && target.is_dir()).then_some(target)
            })
            .collect()
    }

    /// Whether any covered home holds an operator credential right now, or
    /// this process cannot tell.
    ///
    /// A machine with none has nothing for the boundary to protect, which is
    /// what lets work that must start its own sandbox run without one there
    /// (see [`Confinement::for_branch_code`]). Every home this process was
    /// told about is checked on its own, whether or not its name is in the
    /// family; and a family this process cannot list counts as holding one,
    /// because an unanswered question must not open the boundary.
    /// Whether this process could list the home family, which is what makes
    /// [`Self::holds_a_credential`] and the relocated-home rules an answer
    /// rather than a fail-closed assumption.
    pub(crate) fn family_is_listable(&self) -> bool {
        self.family_members().is_ok()
    }

    pub fn holds_a_credential(&self) -> bool {
        let has_credential = |home: &PathBuf| home.join(OPERATOR_SECRET_FILE).exists();
        if self.named.iter().any(has_credential) {
            return true;
        }
        match self.family_members() {
            Ok(members) => members.iter().chain(self.homes.iter()).any(has_credential),
            Err(_) => true,
        }
    }

    /// The operator credential a denied command was refused, judged from the
    /// paths its output names rather than from any one tool's wording.
    ///
    /// Every line carrying a permission-denial signature is split into the
    /// tokens a tool could have named a path with (`cat: PATH: Operation not
    /// permitted`, Python's `...: 'PATH'`, quoted and backticked forms); each
    /// is resolved against `cwd` and through symlinks, so an innocently named
    /// alias, relative or absolute, is judged by what it reaches. Only a path
    /// that resolves to a covered credential counts: a denied file that merely
    /// shares the name, outside every Cairn home, is an ordinary denial.
    pub fn credential_named_by_denial(&self, output: &str, cwd: Option<&Path>) -> Option<PathBuf> {
        output
            .lines()
            .filter(|line| crate::line_has_denial_signature(line))
            .flat_map(path_tokens)
            .map(|token| match cwd {
                Some(cwd) if Path::new(token).is_relative() => cwd.join(token),
                _ => PathBuf::from(token),
            })
            .find(|path| self.names_credential(path))
    }

    pub fn new(os_home: Option<PathBuf>, homes: impl IntoIterator<Item = PathBuf>) -> Self {
        let floor = Self {
            os_home: os_home.map(|home| resolve(&home)),
            homes: Vec::new(),
            named: Vec::new(),
        };
        homes.into_iter().fold(floor, Self::with_home)
    }

    /// Add a Cairn home. One the `.cairn*` family already covers adds no rule,
    /// but it is still remembered as a home this process knows about.
    pub fn with_home(mut self, home: impl AsRef<Path>) -> Self {
        let home = resolve(home.as_ref());
        if !self.named.iter().any(|known| same_path(known, &home)) {
            self.named.push(home.clone());
        }
        if !self.in_home_family(&home) && !self.homes.iter().any(|known| same_path(known, &home)) {
            self.homes.push(home);
        }
        self
    }

    /// Whether `path` names an operator credential this boundary covers.
    ///
    /// The path is resolved first, best effort, so a symlink, `..`, or `/var`
    /// alias is judged by what it reaches; on a case-insensitive platform the
    /// comparison is too. This is the same set the kernel rules describe, which
    /// is what lets the `read` and `write` verbs refuse exactly what a shell
    /// would be refused.
    pub fn names_credential(&self, path: &Path) -> bool {
        [resolve(path), lexically_normalize(path)]
            .iter()
            .any(|candidate| self.names_resolved_credential(candidate))
    }

    fn names_resolved_credential(&self, path: &Path) -> bool {
        let (Some(name), Some(dir)) = (path.file_name(), path.parent()) else {
            return false;
        };
        same_name(name, OPERATOR_SECRET_FILE)
            && (self.in_home_family(dir) || self.homes.iter().any(|home| same_path(home, dir)))
    }

    fn in_home_family(&self, dir: &Path) -> bool {
        let (Some(os_home), Some(parent), Some(name)) =
            (self.os_home.as_deref(), dir.parent(), dir.file_name())
        else {
            return false;
        };
        same_path(parent, os_home) && name_has_prefix(name, HOME_FAMILY_PREFIX)
    }

    /// The Seatbelt rules that enforce this boundary, or an empty string when
    /// there is no home to protect.
    ///
    /// Emitted after every allow in a profile: Seatbelt applies the last
    /// matching rule, so placing these last is what keeps a worktree, a scratch
    /// grant, or a session-approved crossing from reopening a credential.
    #[cfg(any(test, target_os = "macos"))]
    pub(crate) fn sbpl_rules(&self) -> String {
        let file = regex_literal(OPERATOR_SECRET_FILE);
        let mut credentials = Vec::new();
        let mut home_nodes = Vec::new();
        let mut planted_links = String::new();
        if let Some(os_home) = &self.os_home {
            let base = regex_literal(&os_home.to_string_lossy());
            let family = regex_literal(HOME_FAMILY_PREFIX);
            credentials.push(format!("(regex #\"^{base}/{family}[^/]*/{file}$\")"));
            home_nodes.push(format!("(regex #\"^{base}/{family}[^/]*$\")"));
            // A symlink planted at a family name would relocate a future home
            // outside every rule above before its install ever wrote a credential.
            planted_links = format!(
                "(deny file-write-create (require-all (regex #\"^{base}/{family}[^/]*$\") (vnode-type SYMLINK)))\n"
            );
        }
        for home in &self.homes {
            credentials.push(literal_rule(&home.join(OPERATOR_SECRET_FILE)));
            // An explicit home can sit under directories this process may
            // rename, so the home and each ancestor refuse unlink as well.
            home_nodes.extend(
                home.ancestors()
                    .filter(|dir| dir.parent().is_some())
                    .map(literal_rule),
            );
        }
        if credentials.is_empty() {
            return String::new();
        }
        home_nodes.dedup();
        format!(
            ";; operator credential boundary: not a fence rule, never granted\n(deny file-read-data file-write* {})\n(deny file-write-unlink {})\n{planted_links}",
            credentials.join(" "),
            home_nodes.join(" ")
        )
    }
}

/// A macOS Seatbelt profile a spawn runs inside (see [`Confinement::macos_profile`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacosProfile {
    /// Whether the profile carries the operator credential floor's rules, so
    /// Cairn cannot run this spawn without a profile on this machine.
    pub carries_credential_floor: bool,
}

/// How Cairn wraps a process it spawns.
///
/// Every spawn seam states which of these it is, so the boundary in this module
/// is a decision each seam makes once rather than a property of whichever
/// branch of a policy match it happened to take.
#[derive(Debug, Clone)]
pub enum Confinement {
    /// Cairn's own process: a runner, a dev-instance runtime, a service, or a
    /// declared check that must be able to start its own sandbox. Nothing is
    /// applied. A Cairn install owns an operator credential and confines its
    /// own agents, and macOS cannot apply a sandbox inside another one.
    Unconfined,
    /// Agent work the fence leaves unconfined: the operator credential boundary
    /// alone, whatever the fence dial says.
    CredentialFloor,
    /// Agent work under the fence. The boundary rides inside the profile.
    Fenced(SandboxPolicy),
}

impl Confinement {
    /// The confinement for agent work: the fence's policy when it has one, and
    /// the credential floor when it does not.
    pub fn for_agent(policy: Option<SandboxPolicy>) -> Self {
        policy.map_or(Self::CredentialFloor, Self::Fenced)
    }

    /// The confinement for agent-authored code Cairn runs as its own
    /// verification: the check cadences and a project-declared check.
    ///
    /// The command is the project's, but the tests, build scripts, and package
    /// scripts it runs are the branch's, so on a machine holding an operator
    /// credential they carry the boundary like any agent process. Elsewhere
    /// they run with nothing applied, because a suite that starts its own
    /// `sandbox-exec` cannot run inside a profile and there is no credential
    /// to protect.
    pub fn for_branch_code() -> Self {
        Self::for_branch_code_on(&OperatorCredentialFloor::for_this_machine())
    }

    pub fn for_branch_code_on(floor: &OperatorCredentialFloor) -> Self {
        if floor.holds_a_credential() {
            Self::CredentialFloor
        } else {
            Self::Unconfined
        }
    }

    pub fn policy(&self) -> Option<&SandboxPolicy> {
        match self {
            Self::Fenced(policy) => Some(policy),
            Self::Unconfined | Self::CredentialFloor => None,
        }
    }

    pub fn policy_mut(&mut self) -> Option<&mut SandboxPolicy> {
        match self {
            Self::Fenced(policy) => Some(policy),
            Self::Unconfined | Self::CredentialFloor => None,
        }
    }

    /// Whether the fence confines this spawn, which is what `CAIRN_SANDBOXED`
    /// records for tooling that must not start long-lived daemons inside it.
    pub fn is_fenced(&self) -> bool {
        matches!(self, Self::Fenced(_))
    }

    /// Whether this spawn is agent work, and so carries the credential boundary.
    pub fn carries_credential_floor(&self) -> bool {
        !matches!(self, Self::Unconfined)
    }

    /// The macOS Seatbelt profile [`Self::wrap_argv`] puts this spawn inside,
    /// if it puts it inside one. Any profile is what refuses a spawn every
    /// setuid executable; whether it carries the credential floor is what
    /// decides whether Cairn could ever run the spawn without one.
    ///
    /// Both answers come from this machine's homes, just as the wrap's do: the
    /// floor alone applies a profile only where it has rules, and a fenced
    /// profile carries the floor's rules only where there are any.
    pub fn macos_profile(&self) -> Option<MacosProfile> {
        #[cfg(target_os = "macos")]
        {
            let floor_has_rules = || {
                !OperatorCredentialFloor::for_this_machine()
                    .sbpl_rules()
                    .is_empty()
            };
            match self {
                Self::Unconfined => None,
                Self::CredentialFloor => floor_has_rules().then_some(MacosProfile {
                    carries_credential_floor: true,
                }),
                Self::Fenced(_) => Some(MacosProfile {
                    carries_credential_floor: floor_has_rules(),
                }),
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            None
        }
    }

    /// Rewrite `(program, args)` to apply this confinement.
    /// The Seatbelt profile this confinement wraps a spawn in, or `None` when
    /// it applies none. Read back from [`Self::wrap_argv`], so it is the
    /// profile a spawn actually gets rather than a second derivation of it.
    pub fn seatbelt_profile(&self) -> Option<String> {
        let (program, mut args) = self.wrap_argv("/usr/bin/true", &[]);
        if program != "/usr/bin/sandbox-exec" || args.first().map(String::as_str) != Some("-p") {
            return None;
        }
        (args.len() > 1).then(|| args.swap_remove(1))
    }

    pub fn wrap_argv(&self, program: &str, args: &[String]) -> (String, Vec<String>) {
        match self {
            Self::Unconfined => (program.to_string(), args.to_vec()),
            Self::CredentialFloor => credential_floor_wrap_argv(program, args),
            Self::Fenced(policy) => crate::wrap_argv(program, args, policy),
        }
    }

    /// Install the in-process half of this confinement (Linux landlock).
    pub fn install_pre_exec(&self, command: &mut std::process::Command) {
        if let Self::Fenced(policy) = self {
            crate::install_pre_exec(command, policy);
        }
    }
}

/// Rewrite argv to run under the operator credential boundary alone.
///
/// Unchanged where the platform cannot enforce the boundary, and where there is
/// no home to protect: a profile with nothing in it would only cost the spawn
/// its ability to start a sandbox of its own.
fn credential_floor_wrap_argv(program: &str, args: &[String]) -> (String, Vec<String>) {
    #[cfg(target_os = "macos")]
    {
        let rules = OperatorCredentialFloor::for_this_machine().sbpl_rules();
        if !rules.is_empty() {
            return crate::macos::wrap_argv_with_profile(
                program,
                args,
                format!("(version 1)\n(allow default)\n{rules}"),
            );
        }
    }
    (program.to_string(), args.to_vec())
}

/// The operator credential a kernel denial refused, if it refused one.
///
/// `logged` is whatever the platform recovered (the macOS unified log's path,
/// when it found one); the command's own output is classified as well, so a
/// missed log lookup, a read made by a child process, or a symlinked spelling
/// is still named as this boundary rather than surfacing as a generic
/// permission error or an approvable crossing.
pub fn credential_denial(
    output: &str,
    cwd: Option<&Path>,
    logged: Option<&crate::SandboxDenial>,
) -> Option<PathBuf> {
    credential_denial_on(
        &OperatorCredentialFloor::for_this_machine(),
        output,
        cwd,
        logged,
    )
}

fn credential_denial_on(
    floor: &OperatorCredentialFloor,
    output: &str,
    cwd: Option<&Path>,
    logged: Option<&crate::SandboxDenial>,
) -> Option<PathBuf> {
    if let Some(crate::SandboxDenial::Path { path, .. }) = logged {
        if floor.names_credential(path) {
            return Some(path.clone());
        }
    }
    floor.credential_named_by_denial(output, cwd)
}

/// A denial on a spawn that carries only the boundary, which can only be the
/// boundary refusing it.
///
/// Recognized from the attempted path the output names. The floor-only profile
/// carries no [`crate::ViolationTag`], so the violation log has nothing to
/// select this spawn's denials by.
pub fn detect_credential_floor_denial(output: &str, cwd: Option<&Path>) -> Option<PathBuf> {
    if !crate::has_denial_signature(output) {
        return None;
    }
    credential_denial(output, cwd, None)
}

/// The substrings of one output line a tool could have named a path with.
fn path_tokens(line: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    for quote in ['\'', '"', '`'] {
        let mut parts = line.split(quote);
        parts.next();
        while let (Some(inside), Some(_)) = (parts.next(), parts.next()) {
            tokens.push(inside);
        }
    }
    // `TOOL: PATH: ERROR` and `PATH: ERROR` are the unquoted shapes. The
    // trailing segment is the error's prose and the leading one of three or
    // more is the tool's name, so neither is a path position; a bare word
    // there (`cat`, `Error`) must not be resolved, or an unrelated symlink of
    // that name in the working directory would be mistaken for what the tool
    // was refused. A middle segment without spaces is a path even with no
    // slash (`cat: notes.txt: ...` names a relative alias); inside a segment
    // with spaces (`open PATH`), only a word with a slash is one.
    let segments: Vec<&str> = line.split(": ").collect();
    let Some(last) = segments.len().checked_sub(1) else {
        return tokens;
    };
    for (index, segment) in segments.iter().enumerate() {
        if index == last {
            continue;
        }
        let segment = segment.trim().trim_matches(['\'', '"', '`']);
        let is_tool_label = index == 0 && segments.len() >= 3;
        if !is_tool_label && !segment.is_empty() && !segment.contains(char::is_whitespace) {
            tokens.push(segment);
        }
        tokens.extend(segment.split_whitespace().filter(|word| word.contains('/')));
    }
    tokens
}

#[cfg(any(test, target_os = "macos"))]
fn literal_rule(path: &Path) -> String {
    format!(
        "(literal \"{}\")",
        crate::escape_sbpl(&path.to_string_lossy())
    )
}

/// Escape a string so an SBPL `#"..."` regex matches it literally.
#[cfg(any(test, target_os = "macos"))]
fn regex_literal(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(
            ch,
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\'
        ) {
            out.push('\\');
        }
        if ch == '"' {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn case_insensitive_platform() -> bool {
    cfg!(target_os = "macos") || cfg!(target_os = "windows")
}

fn same_name(name: &OsStr, expected: &str) -> bool {
    match name.to_str() {
        Some(name) if case_insensitive_platform() => name.eq_ignore_ascii_case(expected),
        Some(name) => name == expected,
        None => false,
    }
}

fn name_has_prefix(name: &OsStr, prefix: &str) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(head) = name.get(..prefix.len()) else {
        return false;
    };
    if case_insensitive_platform() {
        head.eq_ignore_ascii_case(prefix)
    } else {
        head == prefix
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (left.to_str(), right.to_str()) {
        (Some(left), Some(right)) if case_insensitive_platform() => {
            left.eq_ignore_ascii_case(right)
        }
        _ => false,
    }
}

/// Resolve `..` and `.` without touching the filesystem.
fn lexically_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Canonicalize as much of `path` as exists and keep the rest verbatim.
///
/// A home that does not exist yet still has to produce the path the kernel will
/// report once it does, which on macOS means resolving `/var` to `/private/var`
/// through whichever ancestor already exists.
fn resolve(path: &Path) -> PathBuf {
    if let Ok(resolved) = path.canonicalize() {
        return resolved;
    }
    let normalized = lexically_normalize(path);
    let mut trailing = Vec::new();
    let mut cursor = normalized.as_path();
    while let (Some(name), Some(parent)) = (cursor.file_name(), cursor.parent()) {
        trailing.push(name.to_os_string());
        if let Ok(mut resolved) = parent.canonicalize() {
            for part in trailing.iter().rev() {
                resolved.push(part);
            }
            return resolved;
        }
        cursor = parent;
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floor_at(os_home: &Path) -> OperatorCredentialFloor {
        OperatorCredentialFloor::new(Some(os_home.to_path_buf()), [])
    }

    #[test]
    fn every_cairn_home_in_the_os_home_is_covered() {
        let os_home = tempfile::tempdir().unwrap();
        let floor = floor_at(os_home.path());
        for home in [
            ".cairn",
            ".cairn-dev",
            ".cairn-dev-agent-cairn-5099-builder-5",
            ".cairn-dev-053c1a4ee6db285846f52613b38ff5e7263f104b",
            ".cairn-exec",
        ] {
            let credential = os_home.path().join(home).join(OPERATOR_SECRET_FILE);
            assert!(
                floor.names_credential(&credential),
                "{home} must be covered"
            );
        }
    }

    #[test]
    fn the_mcp_secret_and_other_files_are_not_the_credential() {
        let os_home = tempfile::tempdir().unwrap();
        let floor = floor_at(os_home.path());
        let home = os_home.path().join(".cairn");
        for name in [
            cairn_common::auth::MCP_SECRET_FILE,
            "settings.yaml",
            "operator_auth_secret.bak",
        ] {
            assert!(!floor.names_credential(&home.join(name)), "{name}");
        }
        // A file of the same name outside any Cairn home is a fixture, not a
        // credential: the credential store's own tests create one in a temp dir.
        for elsewhere in [
            os_home.path().join("project").join(OPERATOR_SECRET_FILE),
            os_home
                .path()
                .join(".cairn/scratch/tmp1")
                .join(OPERATOR_SECRET_FILE),
            os_home.path().join("cairn").join(OPERATOR_SECRET_FILE),
        ] {
            assert!(
                !floor.names_credential(&elsewhere),
                "{}",
                elsewhere.display()
            );
        }
    }

    #[test]
    fn an_explicit_home_outside_the_family_is_covered_once_named() {
        let os_home = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let credential = elsewhere.path().join(OPERATOR_SECRET_FILE);

        assert!(!floor_at(os_home.path()).names_credential(&credential));
        assert!(floor_at(os_home.path())
            .with_home(elsewhere.path())
            .names_credential(&credential));
    }

    #[test]
    fn alternate_spellings_resolve_to_the_credential() {
        let os_home = tempfile::tempdir().unwrap();
        let home = os_home.path().join(".cairn-dev-x");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(home.join(OPERATOR_SECRET_FILE), [0u8; 32]).unwrap();
        let floor = floor_at(os_home.path());

        let dotted = os_home
            .path()
            .join(".cairn/../.cairn-dev-x/./operator_auth_secret");
        assert!(floor.names_credential(&dotted), "`..` and `.` resolve");

        let link = os_home.path().join("elsewhere");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&home, &link).unwrap();
            assert!(
                floor.names_credential(&link.join(OPERATOR_SECRET_FILE)),
                "a symlinked home is judged by what it reaches"
            );
            let file_link = os_home.path().join("innocent");
            std::os::unix::fs::symlink(home.join(OPERATOR_SECRET_FILE), &file_link).unwrap();
            assert!(floor.names_credential(&file_link), "a symlinked file too");
        }
        if case_insensitive_platform() {
            assert!(floor.names_credential(&home.join("OPERATOR_AUTH_SECRET")));
        }
    }

    #[test]
    fn the_rules_name_every_home_and_refuse_renaming_one_away() {
        let floor = OperatorCredentialFloor::new(
            Some(PathBuf::from("/Users/x")),
            [PathBuf::from("/opt/cairn/home")],
        );
        let rules = floor.sbpl_rules();
        assert!(rules.contains(
            r#"(deny file-read-data file-write* (regex #"^/Users/x/\.cairn[^/]*/operator_auth_secret$") (literal "/opt/cairn/home/operator_auth_secret"))"#
        ));
        assert!(rules.contains(r#"(deny file-write-unlink (regex #"^/Users/x/\.cairn[^/]*$")"#));
        for node in ["/opt/cairn/home", "/opt/cairn", "/opt"] {
            assert!(rules.contains(&format!("(literal \"{node}\")")), "{node}");
        }
        assert!(
            !rules.contains("(literal \"/\")"),
            "the root is never named"
        );
        // Metadata stays readable, so `ls -l` of a home does not error.
        assert!(!rules.contains("file-read*"));
    }

    #[test]
    fn a_home_inside_the_family_is_not_repeated_as_a_literal() {
        let floor = OperatorCredentialFloor::new(
            Some(PathBuf::from("/Users/x")),
            [PathBuf::from("/Users/x/.cairn-dev")],
        );
        assert!(floor.homes.is_empty());
        assert!(!floor.sbpl_rules().contains("literal"));
    }

    #[cfg(unix)]
    #[test]
    fn a_family_home_that_lives_elsewhere_is_covered_at_its_real_location() {
        // `~/.cairn-dev-peer -> /Volumes/data/peer-home`: a read of the target
        // never spells the family name, so the target itself must be covered.
        let os_home = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let peer_home = elsewhere.path().join("peer-home");
        std::fs::create_dir_all(&peer_home).unwrap();
        std::os::unix::fs::symlink(&peer_home, os_home.path().join(".cairn-dev-peer")).unwrap();

        let lexical_only = floor_at(os_home.path());
        let discovered = OperatorCredentialFloor::discovered(Some(os_home.path().into()), []);
        let real = peer_home.canonicalize().unwrap().join(OPERATOR_SECRET_FILE);

        assert!(
            !lexical_only.names_resolved_credential(&real),
            "the family pattern alone does not name the real location"
        );
        assert!(discovered.names_credential(&real));
        assert!(discovered.names_credential(&peer_home.join(OPERATOR_SECRET_FILE)));
        assert!(discovered
            .sbpl_rules()
            .contains(&format!("(literal \"{}\")", real.display())));
    }

    #[test]
    fn the_rules_refuse_planting_a_symlinked_home() {
        let rules = floor_at(Path::new("/Users/x")).sbpl_rules();
        assert!(rules.contains(
            r#"(deny file-write-create (require-all (regex #"^/Users/x/\.cairn[^/]*$") (vnode-type SYMLINK)))"#
        ));
    }

    #[test]
    fn branch_code_carries_the_floor_only_where_a_credential_exists() {
        let os_home = tempfile::tempdir().unwrap();
        let home = os_home.path().join(".cairn");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(home.join(cairn_common::auth::MCP_SECRET_FILE), [0u8; 32]).unwrap();
        let floor = floor_at(os_home.path());
        assert!(!floor.holds_a_credential());
        assert!(matches!(
            Confinement::for_branch_code_on(&floor),
            Confinement::Unconfined
        ));

        std::fs::write(home.join(OPERATOR_SECRET_FILE), [0u8; 32]).unwrap();
        assert!(floor.holds_a_credential());
        assert!(matches!(
            Confinement::for_branch_code_on(&floor),
            Confinement::CredentialFloor
        ));
    }

    #[test]
    fn a_denial_is_classified_by_the_path_a_tool_names() {
        let os_home = tempfile::tempdir().unwrap();
        let home = os_home.path().join(".cairn-dev-x");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(home.join(OPERATOR_SECRET_FILE), [0u8; 32]).unwrap();
        let floor = floor_at(os_home.path());
        let secret = home.join(OPERATOR_SECRET_FILE);
        let alias = os_home.path().join("notes.txt");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&secret, &alias).unwrap();

        let named = |output: &str, cwd: Option<&Path>| {
            floor.credential_named_by_denial(output, cwd).is_some()
        };
        // Conventional tool stderr, with no kernel-log evidence at all.
        assert!(named(
            &format!("cat: {}: Operation not permitted", secret.display()),
            None
        ));
        assert!(named(
            &format!(
                "PermissionError: [Errno 1] Operation not permitted: '{}'",
                secret.display()
            ),
            None
        ));
        // A child process's read, buried in a build's output.
        assert!(named(
            &format!(
                "running build script\nError: open {}: operation not permitted\nexit 1",
                secret.display()
            ),
            None
        ));
        // Relative to the working directory.
        assert!(named(
            "cat: operator_auth_secret: Operation not permitted",
            Some(&home)
        ));
        // An innocently named alias is judged by what it reaches.
        #[cfg(unix)]
        assert!(named(
            &format!("cat: {}: Operation not permitted", alias.display()),
            None
        ));

        // Not the boundary: an ordinary denial, or the name with no denial.
        assert!(!named("cat: /etc/master.passwd: Permission denied", None));
        // A denied file that merely shares the name, outside every Cairn
        // home, is an ordinary denial: it stays an approvable crossing.
        let fixture = os_home.path().join("fixture").join(OPERATOR_SECRET_FILE);
        std::fs::create_dir_all(fixture.parent().unwrap()).unwrap();
        std::fs::write(&fixture, b"fixture").unwrap();
        assert!(!named(
            &format!("cat: {}: Permission denied", fixture.display()),
            None
        ));
        assert!(!named(
            "cat: operator_auth_secret: Permission denied",
            Some(fixture.parent().unwrap())
        ));
        assert!(!named(&format!("read {} ok", secret.display()), None));
    }

    /// A relative alias in ordinary `cat` stderr, with no kernel-log path: the
    /// fenced executor path's exact call, given a command-scoped denial, must
    /// still name the boundary rather than hand the runner a crossing.
    #[cfg(unix)]
    #[test]
    fn a_relative_alias_with_no_log_evidence_is_the_boundary() {
        let os_home = tempfile::tempdir().unwrap();
        let home = os_home.path().join(".cairn-dev-x");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(home.join(OPERATOR_SECRET_FILE), [0u8; 32]).unwrap();
        let work = os_home.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        std::os::unix::fs::symlink(home.join(OPERATOR_SECRET_FILE), work.join("notes.txt"))
            .unwrap();
        let floor = floor_at(os_home.path());

        let output = "cat: notes.txt: Operation not permitted";
        assert!(credential_denial_on(
            &floor,
            output,
            Some(&work),
            Some(&crate::SandboxDenial::Command)
        )
        .is_some());
        assert!(
            credential_denial_on(&floor, output, Some(&work), None).is_some(),
            "the floor-only path classifies it the same way"
        );
        assert!(credential_denial_on(
            &floor,
            "cat: other.txt: Operation not permitted",
            Some(&work),
            Some(&crate::SandboxDenial::Command)
        )
        .is_none());

        // The tool's name and the error's prose are not path positions: an
        // unrelated symlink named `cat` (or `Operation`) beside an ordinary
        // denied file must not turn that denial into the boundary's refusal.
        for decoy in ["cat", "Permission", "Error"] {
            std::os::unix::fs::symlink(home.join(OPERATOR_SECRET_FILE), work.join(decoy)).unwrap();
        }
        std::fs::write(work.join(OPERATOR_SECRET_FILE), b"ordinary fixture").unwrap();
        for ordinary in [
            "cat: operator_auth_secret: Permission denied",
            "Error: open operator_auth_secret: permission denied",
        ] {
            assert!(
                credential_denial_on(
                    &floor,
                    ordinary,
                    Some(&work),
                    Some(&crate::SandboxDenial::Command)
                )
                .is_none(),
                "{ordinary}"
            );
        }
        // The alias itself is still found among the decoys.
        assert!(credential_denial_on(&floor, output, Some(&work), None).is_some());
    }

    /// A home this process can search but not list: knowing a home must not
    /// depend on enumerating the directory it sits in, and an unanswered
    /// question must not open the boundary.
    #[cfg(unix)]
    #[test]
    fn a_searchable_but_unlistable_home_still_holds_a_credential() {
        use std::os::unix::fs::PermissionsExt as _;
        let os_home = tempfile::tempdir().unwrap();
        let own = os_home.path().join(".cairn");
        std::fs::create_dir_all(&own).unwrap();
        std::fs::write(own.join(OPERATOR_SECRET_FILE), [0u8; 32]).unwrap();
        std::fs::set_permissions(os_home.path(), std::fs::Permissions::from_mode(0o111)).unwrap();

        let listable = std::fs::read_dir(os_home.path()).is_ok();
        let known = OperatorCredentialFloor::discovered(Some(os_home.path().into()), [own.clone()]);
        let unknown = OperatorCredentialFloor::discovered(Some(os_home.path().into()), []);
        let (known_holds, unknown_holds) =
            (known.holds_a_credential(), unknown.holds_a_credential());
        std::fs::set_permissions(os_home.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(known_holds, "the known own credential exists");
        assert!(matches!(
            Confinement::for_branch_code_on(&known),
            Confinement::CredentialFloor
        ));
        if !listable {
            // Root can list anything; elsewhere the listing failure itself
            // must fail closed even with no home named.
            assert!(unknown_holds, "an unlistable home fails closed");
        }
    }

    #[test]
    fn no_home_means_no_rules() {
        assert_eq!(OperatorCredentialFloor::new(None, []).sbpl_rules(), "");
    }

    #[test]
    fn regex_metacharacters_in_a_home_path_are_literal() {
        assert_eq!(regex_literal("/Users/a.b+(c)"), r"/Users/a\.b\+\(c\)");
    }

    #[test]
    fn the_refusal_names_the_boundary_and_says_it_is_not_approvable() {
        assert!(OPERATOR_CREDENTIAL_REFUSAL.contains("operator credential boundary"));
        assert!(OPERATOR_CREDENTIAL_REFUSAL.contains("not a file permission"));
        assert!(OPERATOR_CREDENTIAL_REFUSAL.contains("dev-instance home"));
    }

    #[test]
    fn seatbelt_profile_is_what_wrap_argv_applies() {
        assert_eq!(Confinement::Unconfined.seatbelt_profile(), None);
        let floor = Confinement::CredentialFloor.seatbelt_profile();
        if cfg!(target_os = "macos") && cairn_common::paths::os_home_dir().is_some() {
            let profile = floor.expect("the credential floor wraps a macOS spawn in a profile");
            assert!(
                profile.contains("operator credential boundary"),
                "{profile}"
            );
        } else {
            assert_eq!(floor, None, "no platform profile is applied off macOS");
        }
    }

    #[test]
    fn agent_confinement_without_a_fence_is_the_floor() {
        assert!(matches!(
            Confinement::for_agent(None),
            Confinement::CredentialFloor
        ));
        assert!(Confinement::CredentialFloor.carries_credential_floor());
        assert!(!Confinement::CredentialFloor.is_fenced());
        assert!(!Confinement::Unconfined.carries_credential_floor());
        assert_eq!(
            Confinement::Unconfined.wrap_argv("/bin/sh", &["-c".into(), "true".into()]),
            (
                "/bin/sh".to_string(),
                vec!["-c".to_string(), "true".to_string()]
            )
        );
    }
}
