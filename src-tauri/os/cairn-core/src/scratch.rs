//! Per-job scratch directory: a sanctioned writable temp dir Cairn provisions
//! per job and points the agent's tooling at via `TMPDIR`/`TMP`/`TEMP`.
//!
//! ## Why this exists
//!
//! Agent tooling writes scratch files (build temp, harness logs) to the system
//! temp dir by default. This directory is also the agent CLI process residence:
//! it contains no repository checkout and conveys no project identity. The
//! job-scoped subdir prevents concurrent tools from colliding and gives job
//! teardown one reclaimable scratch tree.
//!
//! ## Where it lives
//!
//! Under [`cairn_common::scratch::scratch_root`] — `<cairn_home>/scratch` — in a
//! directory named for the job's canonical node URI, so a whole path reads as
//! `~/.cairn/scratch/CAIRN.2695.1.builder/terminals/gates.log`. That matters
//! because these paths are advertised to people and to agents (a terminal log
//! line, a chat attachment, the residence a command runs in) and then retyped;
//! the platform temp root this tree used to sit under contributed an opaque
//! per-user hash segment to every one of them and nothing else.
//!
//! The root is outside the platform temp dirs the fence grants wholesale, so
//! `services::sandbox::default_writable_extra` grants it by name — a write here
//! remains in-bounds for both `run` (OS sandbox) and the `write` verb, and takes
//! no prompt.
//!
//! ## Not a security boundary
//!
//! This is deliberately **not** an isolation boundary. The fence allows reads
//! broadly, so scoping *writes* per job does nothing to stop one job from
//! *reading* another's scratch — and co-located jobs are co-trusted anyway
//! (both are agents acting for the same user with broad read reach). The value
//! is collision avoidance and tidy cleanup, plus eliminating the escape-prompt
//! noise a scratch write would otherwise raise if it landed outside any
//! sanctioned dir. We therefore do **not** narrow the platform temp write-allow
//! to gain a false sense of containment.
//!
use cairn_common::scratch::scratch_root;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Compaction cap for a persisted terminal log. When the file grows past this we
/// rewrite it down to its trailing [`TERMINAL_LOG_KEEP_BYTES`] — for a
/// long-running process (e.g. a dev server) the tail is the valuable part.
const TERMINAL_LOG_MAX_BYTES: u64 = 24 * 1024 * 1024;
/// Bytes retained when a terminal log is compacted.
const TERMINAL_LOG_KEEP_BYTES: u64 = 12 * 1024 * 1024;

const SCRATCH_NAME_MAX_BYTES: usize = 220;
/// Hidden sibling of the job directories holding the job-id -> name registry.
/// Hidden so `ls <cairn_home>/scratch` shows nothing but readable job names.
const SCRATCH_MAP_DIR: &str = ".jobs";

/// The directory a job gets before its canonical node URI is known. Job
/// preparation provisions a residence before session startup has resolved the
/// URI, so this is the opening state of an ordinary job rather than a legacy
/// shape; [`ensure_job_scratch_dir`] promotes it in place once the URI arrives.
fn unnamed_job_scratch_dir(job_id: &str) -> PathBuf {
    scratch_root().join(encode_component(job_id))
}

/// Whether a registered name addresses one ordinary directory directly under the
/// scratch root. A traversal (`..`) or a nested path would let a job's teardown
/// reach outside its own tree, and a hidden name would collide with the registry
/// itself — removing [`SCRATCH_MAP_DIR`] would unregister every other job.
fn is_job_dir_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && Path::new(name).file_name().is_some_and(|file| file == name)
}

fn encode_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' => encoded.push(byte as char),
            // Preserve the URI's segment boundaries visibly without creating
            // filesystem subdirectories. Literal dots and underscores are
            // escaped below, so this mapping remains unambiguous.
            b'/' => encoded.push('.'),
            _ => encoded.push_str(&format!("_{byte:02X}")),
        }
    }
    encoded
}

fn scratch_map_path(job_id: &str) -> PathBuf {
    scratch_root()
        .join(SCRATCH_MAP_DIR)
        .join(encode_component(job_id))
}

fn mapped_job_scratch_dir(job_id: &str) -> Option<PathBuf> {
    let name = std::fs::read_to_string(scratch_map_path(job_id)).ok()?;
    let name = name.trim();
    if !is_job_dir_name(name) {
        return None;
    }
    Some(scratch_root().join(name))
}

/// The directory name for a job that knows its canonical node URI: the URI tail
/// with its segment boundaries preserved, so `cairn://p/CAIRN/2695/1/builder`
/// becomes `CAIRN.2695.1.builder`. No prefix — the enclosing directory is
/// already named `scratch` inside the Cairn home, and a `cairn-scratch-` prefix
/// under it would only stutter.
fn node_scratch_name(job_id: &str, home_uri: &str) -> String {
    let uri_tail = home_uri.strip_prefix("cairn://p/").unwrap_or(home_uri);
    let mut name = encode_component(uri_tail);
    if name.len() > SCRATCH_NAME_MAX_BYTES {
        let suffix: String = encode_component(job_id).chars().take(12).collect();
        name.truncate(SCRATCH_NAME_MAX_BYTES - suffix.len() - 2);
        name.push_str("--");
        name.push_str(&suffix);
    }
    // A URI shaped so that its tail encodes to something the registry would
    // later refuse (empty, or leading `.` from a leading separator) must not
    // silently split a job across two directories: fall back to the name every
    // job can always be given.
    if is_job_dir_name(&name) {
        name
    } else {
        encode_component(job_id)
    }
}

/// The scratch directory currently registered for a job. Session startup names
/// it from the canonical node URI; the small registry beside the job directories
/// lets command, terminal, and teardown paths recover that same directory from
/// the internal job id. A job whose URI has not resolved yet uses its
/// job-id-keyed path.
pub fn job_scratch_dir(job_id: &str) -> PathBuf {
    mapped_job_scratch_dir(job_id).unwrap_or_else(|| unnamed_job_scratch_dir(job_id))
}

/// Register and provision a readable scratch directory for a job. Supplying the
/// node's canonical home URI produces `<cairn_home>/scratch/CAIRN.2695.1.builder`;
/// callers without URI context reuse an existing registration or retain the
/// job-id-keyed directory.
///
/// Best-effort: on a create failure the path is still returned (callers export
/// it as `TMPDIR` regardless; a tool then falls back to its own temp handling).
/// Idempotent, so it is safe to call on every spawn and across resumes.
pub fn ensure_job_scratch_dir(job_id: &str, home_uri: Option<&str>) -> PathBuf {
    let dir = if let Some(home_uri) = home_uri {
        let name = node_scratch_name(job_id, home_uri);
        let named = scratch_root().join(&name);
        let map_path = scratch_map_path(job_id);
        let mapping_registered = map_path
            .parent()
            .and_then(|parent| std::fs::create_dir_all(parent).ok().map(|()| parent))
            .and_then(|_| std::fs::write(&map_path, &name).ok())
            .is_some();

        if mapping_registered {
            // Carry across whatever the job already wrote while its URI was
            // still unresolved, so naming the directory never costs a job an
            // attachment or a terminal log it had already produced.
            let unnamed = unnamed_job_scratch_dir(job_id);
            if unnamed != named && unnamed.exists() && !named.exists() {
                if let Err(e) = std::fs::rename(&unnamed, &named) {
                    log::warn!(
                        "Failed to migrate job scratch dir {} to {}: {e}",
                        unnamed.display(),
                        named.display()
                    );
                }
            }
            named
        } else {
            log::warn!(
                "Failed to register readable scratch dir for job {job_id}; using job-id name"
            );
            unnamed_job_scratch_dir(job_id)
        }
    } else {
        job_scratch_dir(job_id)
    };

    if let Err(e) = std::fs::create_dir_all(&dir) {
        log::warn!("Failed to create job scratch dir {}: {e}", dir.display());
    }
    dir
}

/// Remove a job's registered scratch residence and its job-id-keyed directory
/// (idempotent, best-effort). A missing directory is success: the OS may have
/// reaped it, or the job may never have spawned a process.
pub fn remove_job_scratch_dir(job_id: &str) {
    let mapped = mapped_job_scratch_dir(job_id);
    let unnamed = unnamed_job_scratch_dir(job_id);
    let mut dirs = Vec::with_capacity(2);
    if let Some(dir) = mapped {
        dirs.push(dir);
    }
    if !dirs.contains(&unnamed) {
        dirs.push(unnamed);
    }

    for dir in dirs {
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => log::info!("Teardown: removed job scratch dir {}", dir.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => log::warn!(
                "Teardown: failed to remove job scratch dir {}: {e}",
                dir.display()
            ),
        }
    }
    match std::fs::remove_file(scratch_map_path(job_id)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => log::warn!("Failed to remove scratch-dir registration for job {job_id}: {e}"),
    }
}

/// Sanitize a terminal slug into a safe single filename component: keep
/// alphanumerics, `-`, and `_`; map everything else (including any path
/// separator or `..`) to `_`. An empty result becomes `terminal`.
fn sanitize_slug(slug: &str) -> String {
    let sanitized: String = slug
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "terminal".to_string()
    } else {
        sanitized
    }
}

/// Pure path of the persisted output log for a job-scoped agent terminal:
/// `{job_scratch_dir}/terminals/{sanitized_slug}.log`. Keyed by slug (not
/// session id) so a fence respawn and a re-created same-slug terminal share one
/// file. This constructs the path only — [`TerminalLog::open`] creates the
/// `terminals/` subdir when it actually writes, mirroring the
/// [`job_scratch_dir`]/[`ensure_job_scratch_dir`] split so a *read* never
/// provisions directories.
pub(crate) fn terminal_log_path(job_id: &str, slug: &str) -> PathBuf {
    job_scratch_dir(job_id)
        .join("terminals")
        .join(format!("{}.log", sanitize_slug(slug)))
}

/// Append-mode handle to a terminal's persisted output log. The PTY reader
/// thread tees each raw chunk here (see `terminal_host` / the agent terminal
/// spawn), so the full history survives past the 64KB in-memory ring buffer —
/// both live and after exit. File writes are unbuffered (straight to the OS), so
/// history is durable on every end-of-life path, including crashes where the
/// finalizer never runs.
pub struct TerminalLog {
    path: PathBuf,
    file: File,
    /// Tracked byte length, so the cap check is a cheap comparison rather than a
    /// `stat` per chunk.
    len: u64,
}

impl TerminalLog {
    /// Open (append) the log for a job-scoped agent terminal, creating the
    /// `terminals/` subdir. When re-opening a non-empty file (fence respawn or a
    /// re-created same-slug terminal) a session-separator line is written first,
    /// so the two sessions' output stays distinguishable in one file. Returns
    /// `None` on any IO error — teeing is best-effort and never blocks the PTY.
    pub(crate) fn open(job_id: &str, slug: &str) -> Option<Self> {
        let path = terminal_log_path(job_id, slug);
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                log::warn!(
                    "Failed to create terminal log dir {}: {e}",
                    parent.display()
                );
                return None;
            }
        }
        let existing_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => file,
            Err(e) => {
                log::warn!("Failed to open terminal log {}: {e}", path.display());
                return None;
            }
        };
        let mut len = existing_len;
        if existing_len > 0 {
            let marker = format!(
                "\n=== session restarted {} ===\n",
                chrono::Utc::now().to_rfc3339()
            );
            if file.write_all(marker.as_bytes()).is_ok() {
                len += marker.len() as u64;
            }
        }
        Some(Self { path, file, len })
    }

    /// Append a raw PTY chunk, compacting when the file exceeds the cap.
    /// Best-effort: an IO error is dropped (the live buffer and `output_tail`
    /// remain), never surfaced to the reader loop.
    pub(crate) fn append(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        if self.file.write_all(bytes).is_err() {
            return;
        }
        self.len += bytes.len() as u64;
        if self.len > TERMINAL_LOG_MAX_BYTES {
            self.compact();
        }
    }

    /// Flush the handle. File writes are already unbuffered, so this is belt-and-
    /// suspenders on the EOF/error path before the exit callback fires.
    pub fn flush(&mut self) {
        let _ = self.file.flush();
    }

    /// Rewrite the file down to its trailing [`TERMINAL_LOG_KEEP_BYTES`] via a
    /// temp-file swap, then reopen the append handle onto the compacted file.
    /// Runs inline on the reader thread between chunks; blocking IO there is
    /// fine. Any failure leaves the current (oversized) file untouched.
    fn compact(&mut self) {
        let _ = self.file.flush();
        let Ok(mut src) = File::open(&self.path) else {
            return;
        };
        let file_len = src.metadata().map(|m| m.len()).unwrap_or(0);
        let keep = TERMINAL_LOG_KEEP_BYTES.min(file_len);
        let start = file_len - keep;
        if src.seek(SeekFrom::Start(start)).is_err() {
            return;
        }
        let mut tail = Vec::with_capacity(keep as usize);
        if src.read_to_end(&mut tail).is_err() {
            return;
        }
        drop(src);
        let marker = b"=== compacted, older output dropped ===\n";
        let tmp = self.path.with_extension("log.compacting");
        let write_res = (|| -> std::io::Result<()> {
            let mut out = File::create(&tmp)?;
            out.write_all(marker)?;
            out.write_all(&tail)?;
            out.flush()?;
            Ok(())
        })();
        if write_res.is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        if std::fs::rename(&tmp, &self.path).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        if let Ok(file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            self.len = marker.len() as u64 + tail.len() as u64;
            self.file = file;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_dir_is_stable_and_under_the_cairn_scratch_root() {
        let a = job_scratch_dir("job-abc");
        let b = job_scratch_dir("job-abc");
        // Deterministic for a given job id.
        assert_eq!(a, b);
        // Lives under Cairn's own scratch root, which the fence grants by name.
        assert_eq!(a.parent(), Some(scratch_root().as_path()));
        assert!(a.ends_with("job-abc"));
        // Distinct unregistered jobs keep distinct directories.
        assert_ne!(job_scratch_dir("job-abc"), job_scratch_dir("job-xyz"));
    }

    /// The whole point of the move: an agent-visible scratch path carries no
    /// segment a person cannot read back, remember, and retype.
    #[test]
    fn a_scratch_path_carries_no_opaque_segment() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let name = node_scratch_name(&job_id, "cairn://p/cairn/2695/1/builder");
        let path = scratch_root().join(&name);
        assert_eq!(name, "cairn.2695.1.builder");
        // Every segment below the user's home is one Cairn chose and a person
        // can read: no platform temp root, and so no per-user hash.
        let rendered = path.to_string_lossy().into_owned();
        assert!(!rendered.contains("/var/folders/"), "{rendered}");
        assert!(
            rendered.ends_with("/scratch/cairn.2695.1.builder"),
            "{rendered}"
        );
    }

    #[test]
    fn node_uri_produces_a_readable_registered_name() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let dir =
            ensure_job_scratch_dir(&job_id, Some("cairn://p/cairn/2695/1/builder/task/review"));
        assert_eq!(
            dir.file_name().and_then(|name| name.to_str()),
            Some("cairn.2695.1.builder.task.review")
        );
        assert_eq!(job_scratch_dir(&job_id), dir);
        remove_job_scratch_dir(&job_id);
        assert!(!dir.exists());
        assert!(!scratch_map_path(&job_id).exists());
    }

    /// Naming a job's directory must carry its existing contents across, or a
    /// chat attachment uploaded before session startup would be stranded in the
    /// job-id-keyed directory that nothing looks in again.
    #[test]
    fn naming_a_started_job_carries_its_contents_across() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let unnamed = ensure_job_scratch_dir(&job_id, None);
        std::fs::write(unnamed.join("report.pdf"), b"fixture").unwrap();

        let named = ensure_job_scratch_dir(&job_id, Some("cairn://p/cairn/2695/1/builder"));

        assert_ne!(named, unnamed);
        assert!(!unnamed.exists());
        assert_eq!(std::fs::read(named.join("report.pdf")).unwrap(), b"fixture");
        remove_job_scratch_dir(&job_id);
    }

    #[test]
    fn uri_structure_remains_visible_and_stable_without_a_hash() {
        let top_level = "cairn://p/cairn/2695/1/builder-task-review";
        let task = "cairn://p/cairn/2695/1/builder/task/review";

        let top_level_name = node_scratch_name("job-top", top_level);
        let task_name = node_scratch_name("job-task", task);
        assert_eq!(top_level_name, "cairn.2695.1.builder-task-review");
        assert_eq!(task_name, "cairn.2695.1.builder.task.review");
        assert_ne!(top_level_name, task_name);
        assert_eq!(node_scratch_name("another-job", top_level), top_level_name);
        assert_eq!(node_scratch_name("another-job", task), task_name);
    }

    #[test]
    fn unsafe_uri_bytes_are_encoded_into_one_path_component() {
        let name = node_scratch_name("job-abc", "cairn://p/cairn/1/1/a b/%2F._2F");
        assert_eq!(name, "cairn.1.1.a_20b._252F_2E_5F2F");
        assert_eq!(std::path::Path::new(&name).components().count(), 1);
        assert_ne!(encode_component("a/b"), encode_component("a.b"));
        assert_ne!(encode_component("_2F"), encode_component("/"));
    }

    /// A registered name is read back off disk, so it decides which directory a
    /// later teardown removes. Only an ordinary directory directly under the
    /// root may be named.
    #[test]
    fn a_registered_name_can_only_address_one_directory_under_the_root() {
        for rejected in ["", ".", "..", "../elsewhere", "a/b", ".jobs"] {
            assert!(!is_job_dir_name(rejected), "must reject {rejected:?}");
        }
        assert!(is_job_dir_name("cairn.2695.1.builder"));
        // A URI tail that would encode to a rejected name falls back rather than
        // registering a name the lookup would later refuse.
        assert_eq!(node_scratch_name("job-abc", "cairn://p/"), "job-abc");
    }

    #[test]
    fn ensure_creates_and_remove_is_idempotent() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let dir = ensure_job_scratch_dir(&job_id, None);
        assert!(dir.exists(), "ensure should create the dir");
        // A file inside survives until removal.
        std::fs::write(dir.join("scratch.log"), b"x").unwrap();
        remove_job_scratch_dir(&job_id);
        assert!(!dir.exists(), "remove should delete the dir tree");
        // Removing an already-gone dir is a no-op, not an error.
        remove_job_scratch_dir(&job_id);
    }

    #[test]
    fn terminal_log_path_is_under_scratch_and_sanitizes_slug() {
        let path = terminal_log_path("job-abc", "dev");
        assert!(path.starts_with(job_scratch_dir("job-abc")));
        assert!(path.ends_with("terminals/dev.log"));
        // A slug with path separators cannot escape the terminals/ dir.
        let evil = terminal_log_path("job-abc", "../../etc/passwd");
        assert_eq!(
            evil,
            job_scratch_dir("job-abc")
                .join("terminals")
                .join("______etc_passwd.log")
        );
    }

    #[test]
    fn terminal_log_persists_full_history_and_separates_sessions() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        {
            let mut log = TerminalLog::open(&job_id, "dev").unwrap();
            log.append(b"first session output\n");
            log.flush();
        }
        // Re-opening the same slug appends into the one file with a separator.
        {
            let mut log = TerminalLog::open(&job_id, "dev").unwrap();
            log.append(b"second session output\n");
            log.flush();
        }
        let content = std::fs::read_to_string(terminal_log_path(&job_id, "dev")).unwrap();
        assert!(content.contains("first session output"));
        assert!(content.contains("second session output"));
        assert!(content.contains("session restarted"));
        remove_job_scratch_dir(&job_id);
    }

    #[test]
    fn terminal_log_compacts_past_cap_retaining_tail() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let mut log = TerminalLog::open(&job_id, "dev").unwrap();
        // Write well past the 24MB cap in 1MB chunks.
        let chunk = vec![b'x'; 1024 * 1024];
        for _ in 0..30 {
            log.append(&chunk);
        }
        // The most recent output must survive compaction.
        log.append(b"FINAL_TAIL_MARKER\n");
        log.flush();
        let path = terminal_log_path(&job_id, "dev");
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(
            size <= TERMINAL_LOG_MAX_BYTES,
            "compaction should bound size, got {size}"
        );
        let content = std::fs::read(&path).unwrap();
        assert!(
            String::from_utf8_lossy(&content).contains("FINAL_TAIL_MARKER"),
            "tail must be retained after compaction"
        );
        remove_job_scratch_dir(&job_id);
    }
}
