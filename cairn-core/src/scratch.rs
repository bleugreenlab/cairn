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

use std::path::PathBuf;

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
}
