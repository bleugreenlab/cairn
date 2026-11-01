//! What the page-call policy admits: the programs a page's code may run, where
//! each resolves, and which directory trees each needs to read.
//!
//! A page's service call may run code ([`crate::SandboxPolicy::for_page_call`]).
//! Its reads are an allowlist, so every program it runs has to be named as a
//! root, and that is the one place the allowlist could grow in a way nobody
//! chose. The rule ([`validate_root`]): a root is the tool's own tree wherever
//! it resolves, even inside a Cairn home (a headless executor's pinned bun lives
//! under `<home>/tools/bun/<version>/`), but never a home itself or anything that
//! contains one, never a home's `bin/` (which holds the `cairn` CLI), `tools/`
//! or `tool-cache/` as a whole, and never a tree holding a credential or a
//! database. A program whose tree breaks the rule is refused, never admitted
//! with a wider allowlist.
//!
//! Python runs as `python3 -c`, the run tool's own fallback, never through
//! `uv`: a page-call batch is offline and has no dependencies to resolve, so
//! `uv` would add a binary, a cache and a managed-python tree to the allowlist
//! and buy nothing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::OperatorCredentialFloor;

/// Every program a page-call batch item may execute, as the run tool names
/// them: `bun` for TypeScript and JavaScript, `python3` for Python, `sh` for a
/// `command`.
pub const PAGE_CALL_PROGRAMS: [&str; 3] = ["bun", "python3", "sh"];

/// File names that mark a directory tree as holding Cairn state an
/// interpreter root must never expose.
const FORBIDDEN_NAMES: [&str; 4] = [
    cairn_common::auth::OPERATOR_SECRET_FILE,
    "mcp_auth_secret",
    "cairn.db",
    "credential",
];

/// Directories directly in a Cairn home that are never a root as a whole.
const FORBIDDEN_HOME_CHILDREN: [&str; 3] = ["bin", "tools", "tool-cache"];

/// How deep a root inside a Cairn home is searched for forbidden files, and
/// how many entries the search may visit before it refuses for want of an
/// answer.
const ROOT_SEARCH_DEPTH: usize = 8;
const ROOT_SEARCH_ENTRIES: usize = 50_000;

/// One program resolved for a page-call batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProgram {
    /// The name a batch item executes.
    pub program: String,
    /// Where it resolves after following every symlink and shim link.
    pub path: PathBuf,
    /// The trees it reads, each checked by [`validate_root`].
    pub roots: Vec<PathBuf>,
}

/// Resolve `program` on this process's `PATH`, which is the `PATH` an
/// executor's batch items resolve on.
pub fn resolve_program(program: &str) -> Result<ResolvedProgram, String> {
    resolve_program_on(
        program,
        std::env::var_os("PATH").as_deref(),
        &OperatorCredentialFloor::for_this_machine(),
    )
}

pub fn resolve_program_on(
    program: &str,
    search_path: Option<&std::ffi::OsStr>,
    floor: &OperatorCredentialFloor,
) -> Result<ResolvedProgram, String> {
    if !PAGE_CALL_PROGRAMS.contains(&program) {
        return Err(format!(
            "a page-call batch runs only {}, not `{program}`",
            PAGE_CALL_PROGRAMS.join(", ")
        ));
    }
    let found = which(program, search_path)
        .ok_or_else(|| format!("`{program}` is not on this executor's PATH"))?;
    let path = found
        .canonicalize()
        .map_err(|error| format!("resolve `{program}` at {}: {error}", found.display()))?;
    let mut roots = vec![path.parent().map(Path::to_path_buf).ok_or_else(|| {
        format!(
            "`{program}` resolved to {}, which has no directory",
            path.display()
        )
    })?];
    if program == "python3" {
        let prefix = python_base_prefix(&path)?;
        if !roots.iter().any(|root| prefix.starts_with(root)) {
            roots.push(prefix);
        }
    }
    let homes = floor.known_homes().map_err(|error| {
        format!("cannot list this machine's Cairn homes to check `{program}`'s tree: {error}")
    })?;
    for root in &roots {
        validate_root(root, &homes)
            .map_err(|why| format!("`{program}` resolves to {}: {why}", path.display()))?;
    }
    Ok(ResolvedProgram {
        program: program.to_string(),
        path,
        roots,
    })
}

fn which(program: &str, search_path: Option<&std::ffi::OsStr>) -> Option<PathBuf> {
    let search_path = search_path?;
    std::env::split_paths(search_path)
        .filter(|dir| dir.is_absolute())
        .map(|dir| dir.join(program))
        .find(|candidate| is_executable(candidate))
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

/// Whether `root` may be read by a page-call batch, given every Cairn home on
/// the machine.
pub fn validate_root(root: &Path, homes: &[PathBuf]) -> Result<(), String> {
    if !root.is_absolute() || root.parent().is_none() {
        return Err(format!("{} cannot be a read root", root.display()));
    }
    for home in homes {
        if home.starts_with(root) {
            return Err(format!(
                "its tree {} contains the Cairn home {}",
                root.display(),
                home.display()
            ));
        }
        let Ok(within) = root.strip_prefix(home) else {
            continue;
        };
        let mut components = within.components();
        if let (Some(first), None) = (components.next(), components.next()) {
            let first = first.as_os_str().to_string_lossy();
            if FORBIDDEN_HOME_CHILDREN.contains(&first.as_ref()) {
                return Err(format!(
                    "its tree {} is the Cairn home {}'s whole `{first}` directory",
                    root.display(),
                    home.display()
                ));
            }
        }
        if let Some(found) = forbidden_file_beneath(root)? {
            return Err(format!(
                "its tree {} inside the Cairn home {} holds {}",
                root.display(),
                home.display(),
                found.display()
            ));
        }
    }
    Ok(())
}

fn forbidden_file_beneath(root: &Path) -> Result<Option<PathBuf>, String> {
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    while let Some((dir, depth)) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|error| format!("cannot search {} for Cairn state: {error}", dir.display()))?;
        for entry in entries.flatten() {
            visited += 1;
            if visited > ROOT_SEARCH_ENTRIES {
                return Err(format!(
                    "{} is too large to show it holds no Cairn state",
                    root.display()
                ));
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if FORBIDDEN_NAMES
                .iter()
                .any(|forbidden| name == *forbidden || name.starts_with(&format!("{forbidden}-")))
            {
                return Ok(Some(entry.path()));
            }
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) && depth < ROOT_SEARCH_DEPTH {
                stack.push((entry.path(), depth + 1));
            }
        }
    }
    Ok(None)
}

/// The prefix a Python interpreter loads its standard library from, asked of
/// the interpreter itself and memoized by the binary's identity.
fn python_base_prefix(python: &Path) -> Result<PathBuf, String> {
    static PREFIXES: Mutex<Option<HashMap<FileIdentity, PathBuf>>> = Mutex::new(None);
    let identity = FileIdentity::of(python)?;
    if let Some(prefix) = PREFIXES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_or_insert_with(HashMap::new)
        .get(&identity)
    {
        return Ok(prefix.clone());
    }
    let output = std::process::Command::new(python)
        .args(["-I", "-c", "import sys; print(sys.base_prefix)"])
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .current_dir("/")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|error| format!("ask {} for its prefix: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} could not report its prefix: {}",
            python.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let prefix = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    let prefix = prefix
        .canonicalize()
        .map_err(|error| format!("resolve python prefix {}: {error}", prefix.display()))?;
    PREFIXES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(identity, prefix.clone());
    Ok(prefix)
}

/// A file as the kernel identifies it: an update in place, a replacement and a
/// relink all change it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileIdentity {
    path: PathBuf,
    len: u64,
    modified_ns: u128,
    inode: u64,
}

impl FileIdentity {
    fn of(path: &Path) -> Result<Self, String> {
        let meta =
            std::fs::metadata(path).map_err(|error| format!("read {}: {error}", path.display()))?;
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(&meta);
        #[cfg(not(unix))]
        let inode = 0;
        Ok(Self {
            path: path.to_path_buf(),
            len: meta.len(),
            modified_ns: meta
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |elapsed| elapsed.as_nanos()),
            inode,
        })
    }
}

/// The sha256 of a file, memoized by its identity, so a fingerprint that is
/// recomputed before every spawn hashes an interpreter only when it changed.
fn file_digest(path: &Path) -> Result<String, String> {
    static DIGESTS: Mutex<Option<HashMap<FileIdentity, String>>> = Mutex::new(None);
    let identity = FileIdentity::of(path)?;
    if let Some(digest) = DIGESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_or_insert_with(HashMap::new)
        .get(&identity)
    {
        return Ok(digest.clone());
    }
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let digest = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(&bytes));
    DIGESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(identity, digest.clone());
    Ok(digest)
}

/// The `PageCallPolicy` invalidation input: everything that could widen what a
/// page-call batch reads, as text that moves whenever any of it does. The
/// system and shell roots, the profile shape, and each program's resolved
/// path, digest and roots (or why it does not resolve). `None` only where no
/// page-call policy exists at all.
pub fn policy_input() -> Option<String> {
    let shape = profile_shape()?;
    let mut out = format!("shape:\n{shape}\n");
    for program in PAGE_CALL_PROGRAMS {
        match resolve_program(program) {
            Ok(resolved) => {
                let digest = file_digest(&resolved.path)
                    .unwrap_or_else(|error| format!("unreadable: {error}"));
                let roots: Vec<String> = resolved
                    .roots
                    .iter()
                    .map(|root| root.to_string_lossy().into_owned())
                    .collect();
                out.push_str(&format!(
                    "{program}={} sha256={digest} roots=[{}]\n",
                    resolved.path.display(),
                    roots.join(",")
                ));
            }
            Err(why) => out.push_str(&format!("{program}=unresolved: {why}\n")),
        }
    }
    Some(out)
}

/// The page-call policy this build installs, for a canonical directory and
/// port, so a change to the generator moves the fingerprint.
fn profile_shape() -> Option<String> {
    let mut policy = crate::SandboxPolicy::for_page_call(
        Path::new("/cairn/containment/page-call"),
        &[PathBuf::from("/cairn/containment/interpreter")],
        1,
    );
    policy.violation_tag = crate::ViolationTag::canonical();
    #[cfg(target_os = "macos")]
    {
        let floor =
            OperatorCredentialFloor::new(Some(PathBuf::from("/cairn/containment/home")), []);
        Some(crate::macos::sbpl_profile(&policy, &floor))
    }
    #[cfg(target_os = "linux")]
    {
        Some(crate::linux::enforcement_description(&policy))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = policy;
        None
    }
}

/// The names a page-call spawn's own directories take beneath its batch
/// directory: its `HOME` and its `TMPDIR`. Short, because a unix socket path
/// beneath `TMPDIR` has little room (cairn/5497).
pub const HOME_DIR: &str = ".h";
pub const TMP_DIR: &str = ".t";

/// The whole environment a page-call spawn starts with, before the Cairn run
/// variables the executor adds: a `PATH` of the programs' own directories and
/// the system binaries, `HOME` and `TMPDIR` inside the batch directory, and a
/// locale. Nothing the executor inherited.
pub fn base_env(dir: &Path, programs: &[ResolvedProgram]) -> Vec<(String, String)> {
    let mut path: Vec<String> = Vec::new();
    for program in programs {
        if let Some(parent) = program.path.parent() {
            let parent = parent.to_string_lossy().into_owned();
            if !path.contains(&parent) {
                path.push(parent);
            }
        }
    }
    for system in ["/usr/bin", "/bin"] {
        if !path.iter().any(|entry| entry == system) {
            path.push(system.to_string());
        }
    }
    let home = dir.join(HOME_DIR).to_string_lossy().into_owned();
    let tmp = dir.join(TMP_DIR).to_string_lossy().into_owned();
    vec![
        ("PATH".into(), path.join(":")),
        ("HOME".into(), home),
        ("TMPDIR".into(), tmp.clone()),
        ("TMP".into(), tmp.clone()),
        ("TEMP".into(), tmp),
        ("LANG".into(), "C.UTF-8".into()),
    ]
}

/// Create a batch directory's own `HOME` and `TMPDIR`.
pub fn prepare_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir.join(HOME_DIR))?;
    std::fs::create_dir_all(dir.join(TMP_DIR))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn homes(root: &Path) -> Vec<PathBuf> {
        vec![root.join(".cairn"), root.join(".cairn-exec")]
    }

    #[test]
    fn a_root_containing_a_home_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        for home in homes(&root) {
            std::fs::create_dir_all(&home).unwrap();
        }
        let refusal = validate_root(&root, &homes(&root)).unwrap_err();
        assert!(refusal.contains("contains the Cairn home"), "{refusal}");
        let refusal = validate_root(&root.join(".cairn"), &homes(&root)).unwrap_err();
        assert!(refusal.contains("contains the Cairn home"), "{refusal}");
    }

    #[test]
    fn a_homes_bin_tools_and_tool_cache_are_never_roots_as_a_whole() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join(".cairn-exec");
        for child in FORBIDDEN_HOME_CHILDREN {
            std::fs::create_dir_all(home.join(child)).unwrap();
            let refusal = validate_root(&home.join(child), &homes(&root)).unwrap_err();
            assert!(refusal.contains(&format!("whole `{child}`")), "{refusal}");
        }
    }

    #[test]
    fn a_pinned_tool_tree_inside_a_home_is_a_root_unless_it_holds_cairn_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join(".cairn-exec");
        let bun = home.join("tools/bun/1.3.2");
        std::fs::create_dir_all(&bun).unwrap();
        std::fs::write(bun.join("bun"), "binary").unwrap();
        std::fs::write(home.join("mcp_auth_secret"), "secret").unwrap();
        std::fs::write(home.join("cairn.db"), "db").unwrap();
        assert_eq!(validate_root(&bun, &homes(&root)), Ok(()));

        std::fs::write(bun.join("cairn.db-wal"), "db").unwrap();
        let refusal = validate_root(&bun, &homes(&root)).unwrap_err();
        assert!(refusal.contains("cairn.db-wal"), "{refusal}");
    }

    #[test]
    fn a_root_outside_every_home_is_admitted_without_a_search() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let framework = root.join("Library/Frameworks/Python.framework/Versions/3.12");
        std::fs::create_dir_all(&framework).unwrap();
        assert_eq!(validate_root(&framework, &homes(&root)), Ok(()));
        assert!(validate_root(Path::new("/"), &homes(&root)).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_program_resolves_through_its_shim_link_to_its_own_tree() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let os_home = root.join("user");
        let shims = os_home.join(".cairn/bin");
        let bundle = root.join("Applications/cairn.app/Contents/MacOS");
        std::fs::create_dir_all(&shims).unwrap();
        std::fs::create_dir_all(&bundle).unwrap();
        let real = bundle.join("bun");
        std::fs::write(&real, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink(&real, shims.join("bun")).unwrap();
        let floor = OperatorCredentialFloor::new(Some(os_home.clone()), []);

        let resolved =
            resolve_program_on("bun", Some(shims.as_os_str()), &floor).expect("bun resolves");
        assert_eq!(resolved.path, real);
        assert_eq!(resolved.roots, vec![bundle.clone()]);

        // A copy in the home's own bin/ would make the whole bin/ a root.
        std::fs::remove_file(shims.join("bun")).unwrap();
        std::fs::copy(&real, shims.join("bun")).unwrap();
        let refusal = resolve_program_on("bun", Some(shims.as_os_str()), &floor).unwrap_err();
        assert!(refusal.contains("whole `bin`"), "{refusal}");

        let refusal = resolve_program_on("node", Some(shims.as_os_str()), &floor).unwrap_err();
        assert!(refusal.contains("not `node`"), "{refusal}");
    }

    #[test]
    fn the_base_env_is_the_programs_own_directories_and_nothing_inherited() {
        let dir = Path::new("/b/pc/1");
        let programs = vec![
            ResolvedProgram {
                program: "bun".into(),
                path: PathBuf::from("/Applications/cairn.app/Contents/MacOS/bun"),
                roots: vec![],
            },
            ResolvedProgram {
                program: "sh".into(),
                path: PathBuf::from("/bin/sh"),
                roots: vec![],
            },
        ];
        let env: HashMap<String, String> = base_env(dir, &programs).into_iter().collect();
        assert_eq!(
            env["PATH"],
            "/Applications/cairn.app/Contents/MacOS:/bin:/usr/bin"
        );
        assert_eq!(env["HOME"], "/b/pc/1/.h");
        assert_eq!(env["TMPDIR"], "/b/pc/1/.t");
        let mut names: Vec<&str> = env.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, ["HOME", "LANG", "PATH", "TEMP", "TMP", "TMPDIR"]);
    }
}
