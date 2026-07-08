//! Per-job scratch directory: a sanctioned writable temp dir Cairn provisions
//! per job and points the agent's tooling at via `TMPDIR`/`TMP`/`TEMP`.
//!
//! ## Why this exists
//!
//! Agent tooling writes scratch files (build temp, harness logs) to the system
//! temp dir by default. Those writes are already in-bounds for the worktree
//! fence — the system temp root is in the sandbox writable set
//! (`services::sandbox::default_writable_extra`), so a write there takes no
//! prompt. The job-scoped subdir adds two things on top of that: tools spawned
//! by concurrent jobs no longer collide on shared temp filenames, and the whole
//! dir is removed together at worktree teardown instead of littering temp.
//!
//! ## Not a security boundary
//!
//! This is deliberately **not** an isolation boundary. The fence allows reads
//! broadly, so scoping *writes* per job does nothing to stop one job from
//! *reading* another's scratch — and co-located jobs are co-trusted anyway
//! (both are agents acting for the same user with broad read reach). The value
//! is collision avoidance and tidy cleanup, plus eliminating the escape-prompt
//! noise a scratch write would otherwise raise if it landed outside any
//! sanctioned dir. We therefore do **not** narrow the `/tmp` write-allow to
//! gain a false sense of containment.
//!
//! The dir lives under [`std::env::temp_dir`], which is already in the sandbox
//! writable set, so no fence widening is needed: the scratch path is in-bounds
//! for both `run` (OS sandbox) and the `write` verb by construction.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Compaction cap for a persisted terminal log. When the file grows past this we
/// rewrite it down to its trailing [`TERMINAL_LOG_KEEP_BYTES`] — for a
/// long-running process (e.g. a dev server) the tail is the valuable part.
const TERMINAL_LOG_MAX_BYTES: u64 = 24 * 1024 * 1024;
/// Bytes retained when a terminal log is compacted.
const TERMINAL_LOG_KEEP_BYTES: u64 = 12 * 1024 * 1024;

/// The scratch directory for a job: a stable subdir of the system temp root,
/// keyed on the job id. Pure path construction — see [`ensure_job_scratch_dir`]
/// to create it. Because it sits under `std::env::temp_dir()`, writes here are
/// in the fence's writable set and never prompt.
pub fn job_scratch_dir(job_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!("cairn-scratch-{job_id}"))
}

/// Ensure the job's scratch dir exists, returning its path. Best-effort: on a
/// create failure the path is still returned (callers export it as `TMPDIR`
/// regardless; a tool then falls back to its own default temp handling).
/// Idempotent, so it is safe to call on every spawn and across resumes.
pub fn ensure_job_scratch_dir(job_id: &str) -> PathBuf {
    let dir = job_scratch_dir(job_id);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        log::warn!("Failed to create job scratch dir {}: {e}", dir.display());
    }
    dir
}

/// Remove a job's scratch dir (idempotent, best-effort). Called at worktree
/// teardown for every job that referenced the torn-down worktree. A missing
/// dir is success (the OS may have reaped it, or the job never spawned a
/// command).
pub fn remove_job_scratch_dir(job_id: &str) {
    let dir = job_scratch_dir(job_id);
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => log::info!("Teardown: removed job scratch dir {}", dir.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => log::warn!(
            "Teardown: failed to remove job scratch dir {}: {e}",
            dir.display()
        ),
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
pub fn terminal_log_path(job_id: &str, slug: &str) -> PathBuf {
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
    pub fn open(job_id: &str, slug: &str) -> Option<Self> {
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
    pub fn append(&mut self, bytes: &[u8]) {
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
    fn scratch_dir_is_stable_and_under_temp_root() {
        let a = job_scratch_dir("job-abc");
        let b = job_scratch_dir("job-abc");
        // Deterministic for a given job id.
        assert_eq!(a, b);
        // Lives under the system temp root, so it is already in the sandbox
        // writable set — no fence widening required.
        assert!(a.starts_with(std::env::temp_dir()));
        assert!(a.to_string_lossy().contains("cairn-scratch-job-abc"));
        // Distinct jobs get distinct dirs (collision avoidance across concurrent
        // jobs).
        assert_ne!(job_scratch_dir("job-abc"), job_scratch_dir("job-xyz"));
    }

    #[test]
    fn ensure_creates_and_remove_is_idempotent() {
        let job_id = format!("test-{}", uuid::Uuid::new_v4());
        let dir = ensure_job_scratch_dir(&job_id);
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
