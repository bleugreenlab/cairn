//! An executor installation this machine ran before the transport inversion,
//! retired: its supervisor stopped, its generation proven released, and its home
//! handed to the current executor's storage ladder.
//!
//! Two halves share these types. The executor binary's `service retire-legacy`
//! subcommand finds and stops the legacy supervisor, proves its generation dead,
//! and records each home it retired in [`RETIRED_EXECUTOR_HOMES_FILE`] inside the
//! current executor home. The runner reads the [`LegacyRetirementReport`] that
//! command prints over ssh. The executor's storage sweep reads the record back,
//! re-validates every entry with [`validate_retired_home`], and reclaims it.
//!
//! A retired home is never removed wholesale while it holds runner state: a
//! pre-rename runner kept its database and single-owner lock beside its build
//! slots, and those are the operator's to remove, not the janitor's.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The record of retired homes, inside the current executor home.
pub const RETIRED_EXECUTOR_HOMES_FILE: &str = "retired-executor-homes.json";

/// Where the pre-inversion remote executor kept its home, relative to the
/// user's home directory.
pub const PRE_INVERSION_EXECUTOR_HOME: &str = ".cairn-executor";

/// The default home of a pre-rename runner whose unit names none.
pub const PRE_RENAME_RUNNER_HOME: &str = ".cairn";

/// The pre-inversion ssh-launched executor's owner record, suffixed with the
/// executor id: `nonce pid start`.
pub const PRE_INVERSION_OWNER_FILE_PREFIX: &str = ".cairn-executor-owner";

/// Written into a legacy home once the retirement has validated it and proven
/// its generation released, naming the executor home that retired it. It is
/// the validator's evidence that outlasts the build slots: the storage rung
/// removes it last, so a reclamation interrupted after `build-slots/` is gone
/// still finds the home its own.
pub const RETIRED_HOME_MARKER: &str = ".cairn-retired-executor-home";

/// Mark `home` as retired by `executor_home`.
pub fn write_retired_home_marker(home: &Path, executor_home: &Path) -> Result<(), String> {
    let path = home.join(RETIRED_HOME_MARKER);
    std::fs::write(&path, format!("{}\n", executor_home.display()))
        .map_err(|error| format!("write {}: {error}", path.display()))
}

/// One home the retirement handed to the storage ladder.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetiredExecutorHome {
    pub path: PathBuf,
    pub retired_at_unix_ms: u64,
    /// The legacy supervisor that kept it alive, by the identity it was
    /// registered under, or `None` when the home was found only by the
    /// pre-inversion convention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor: Option<String>,
    /// The home holds a runner's database or single-owner lock, so only its
    /// build slots and managed objects are the ladder's.
    pub runner_state: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reclaimed_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_freed: Option<u64>,
    /// The entry the last bounded reclamation pass stopped in, while the home
    /// is unfinished. Where the next pass resumes is what is still on disk;
    /// this names it for a reader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct RetiredExecutorHomesRecord {
    #[serde(default)]
    homes: Vec<RetiredExecutorHome>,
}

pub fn retired_homes_path(executor_home: &Path) -> PathBuf {
    executor_home.join(RETIRED_EXECUTOR_HOMES_FILE)
}

/// Every home recorded as retired in `executor_home`. No record is none.
pub fn read_retired_homes(executor_home: &Path) -> Result<Vec<RetiredExecutorHome>, String> {
    let path = retired_homes_path(executor_home);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice::<RetiredExecutorHomesRecord>(&bytes)
            .map(|record| record.homes)
            .map_err(|error| format!("parse {}: {error}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(format!("read {}: {error}", path.display())),
    }
}

fn write_retired_homes(
    executor_home: &Path,
    homes: Vec<RetiredExecutorHome>,
) -> Result<(), String> {
    let path = retired_homes_path(executor_home);
    let partial = executor_home.join(format!("{RETIRED_EXECUTOR_HOMES_FILE}.partial"));
    let bytes = serde_json::to_vec_pretty(&RetiredExecutorHomesRecord { homes })
        .map_err(|error| format!("encode retired homes: {error}"))?;
    std::fs::create_dir_all(executor_home)
        .map_err(|error| format!("create {}: {error}", executor_home.display()))?;
    std::fs::write(&partial, bytes)
        .and_then(|()| std::fs::rename(&partial, &path))
        .map_err(|error| {
            let _ = std::fs::remove_file(&partial);
            format!("write {}: {error}", path.display())
        })
}

/// Merge `retired` into the record. A home already listed keeps its entry, so
/// a retirement run twice never duplicates one or forgets what was reclaimed.
pub fn record_retired_homes(
    executor_home: &Path,
    retired: &[RetiredExecutorHome],
) -> Result<(), String> {
    let mut homes = read_retired_homes(executor_home)?;
    let before = homes.len();
    for entry in retired {
        if !homes.iter().any(|known| known.path == entry.path) {
            homes.push(entry.clone());
        }
    }
    if homes.len() == before {
        return Ok(());
    }
    write_retired_homes(executor_home, homes)
}

/// Add what the storage ladder freed from the home at `path`, and with
/// `reclaimed_at_unix_ms` mark it finished. The entry stays, so the executor
/// page can still say what was reclaimed.
pub fn record_retired_home_progress(
    executor_home: &Path,
    path: &Path,
    bytes_freed: u64,
    reclaimed_at_unix_ms: Option<u64>,
    stopped_at: Option<&Path>,
) -> Result<(), String> {
    let mut homes = read_retired_homes(executor_home)?;
    let Some(entry) = homes.iter_mut().find(|entry| entry.path == path) else {
        return Ok(());
    };
    if reclaimed_at_unix_ms.is_some() {
        entry.reclaimed_at_unix_ms = reclaimed_at_unix_ms;
        entry.stopped_at = None;
    } else {
        entry.stopped_at = stopped_at.map(Path::to_path_buf);
    }
    entry.bytes_freed = Some(entry.bytes_freed.unwrap_or(0).saturating_add(bytes_freed));
    write_retired_homes(executor_home, homes)
}

/// Candidate homes a retirement validated and has not yet handed to the
/// ladder, inside the current executor home.
///
/// A legacy supervisor's unit is the only thing that names a home outside the
/// conventions, and the retirement removes that unit before the home is
/// handed over. This record carries the name across a retirement that fails
/// after that point. It authorizes nothing: every later retirement validates
/// the home again and must prove its generation released before reaping or
/// recording anything.
pub const PENDING_LEGACY_HOMES_FILE: &str = "legacy-executor-homes-pending.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingLegacyHome {
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingLegacyHomesRecord {
    #[serde(default)]
    homes: Vec<PendingLegacyHome>,
}

pub fn read_pending_legacy_homes(executor_home: &Path) -> Result<Vec<PendingLegacyHome>, String> {
    let path = executor_home.join(PENDING_LEGACY_HOMES_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice::<PendingLegacyHomesRecord>(&bytes)
            .map(|record| record.homes)
            .map_err(|error| format!("parse {}: {error}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(format!("read {}: {error}", path.display())),
    }
}

/// Replace the pending record; an empty list removes it.
pub fn write_pending_legacy_homes(
    executor_home: &Path,
    homes: &[PendingLegacyHome],
) -> Result<(), String> {
    let path = executor_home.join(PENDING_LEGACY_HOMES_FILE);
    if homes.is_empty() {
        return match std::fs::remove_file(&path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                Err(format!("remove {}: {error}", path.display()))
            }
            _ => Ok(()),
        };
    }
    let partial = executor_home.join(format!("{PENDING_LEGACY_HOMES_FILE}.partial"));
    let bytes = serde_json::to_vec_pretty(&PendingLegacyHomesRecord {
        homes: homes.to_vec(),
    })
    .map_err(|error| format!("encode pending legacy homes: {error}"))?;
    std::fs::create_dir_all(executor_home)
        .map_err(|error| format!("create {}: {error}", executor_home.display()))?;
    std::fs::write(&partial, bytes)
        .and_then(|()| std::fs::rename(&partial, &path))
        .map_err(|error| {
            let _ = std::fs::remove_file(&partial);
            format!("write {}: {error}", path.display())
        })
}

/// Whether `home` holds a runner's own state: its database or a single-owner
/// lock, under the current or pre-rename name.
pub fn home_holds_runner_state(home: &Path) -> bool {
    if home.join("orch.lock").exists() || home.join("runner.lock").exists() {
        return true;
    }
    std::fs::read_dir(home).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with("cairn.db"))
    })
}

/// Whether `candidate` may be treated as a retired executor home, checked
/// against the executor home that is running now and the user's home.
///
/// The retirement and the storage rung both call this, the rung on every
/// sweep, because the record is a file on disk and the deletion it authorizes
/// cannot be taken back. A path is refused unless all of these hold: it is
/// absolute and a real directory, not a symlink; it is owned by this user; it
/// neither contains nor lies inside the current executor home; it is not the
/// user's home or an ancestor of it; it carries an executor's marks, a
/// `build-slots` directory, a pre-inversion owner record, or the retirement's
/// own [`RETIRED_HOME_MARKER`]; and a `build-slots` it has is a real directory,
/// because a link there would carry every deletion and the process scan beyond
/// the home.
pub fn validate_retired_home(
    candidate: &Path,
    current_home: &Path,
    host_home: &Path,
) -> Result<(), String> {
    if !candidate.is_absolute() {
        return Err("the path is not absolute".into());
    }
    let metadata = std::fs::symlink_metadata(candidate)
        .map_err(|error| format!("it cannot be read: {error}"))?;
    if metadata.file_type().is_symlink() {
        return Err("it is a symbolic link".into());
    }
    if !metadata.is_dir() {
        return Err("it is not a directory".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid() is infallible and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        if metadata.uid() != uid {
            return Err(format!(
                "it is owned by uid {}, not this user ({uid})",
                metadata.uid()
            ));
        }
    }
    let canonical =
        |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let candidate = canonical(candidate);
    let current = canonical(current_home);
    let host = canonical(host_home);
    if current.starts_with(&candidate) {
        return Err("it is the current executor home or contains it".into());
    }
    if candidate.starts_with(&current) {
        return Err("it lies inside the current executor home".into());
    }
    if host.starts_with(&candidate) {
        return Err("it is the user's home directory or contains it".into());
    }
    let has_slots = match std::fs::symlink_metadata(candidate.join("build-slots")) {
        Ok(slots) if slots.file_type().is_symlink() => {
            return Err("its build-slots is a symbolic link".into())
        }
        Ok(slots) if !slots.is_dir() => return Err("its build-slots is not a directory".into()),
        Ok(_) => true,
        Err(_) => false,
    };
    let has_marks = std::fs::read_dir(&candidate).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(PRE_INVERSION_OWNER_FILE_PREFIX) || name == RETIRED_HOME_MARKER
        })
    });
    if !has_slots && !has_marks {
        return Err("it has no build-slots directory or executor owner record".into());
    }
    Ok(())
}

/// What `cairn-exec service retire-legacy --json` prints.
///
/// Empty is the answer for a machine with nothing to retire. A home whose
/// generation is [`GenerationRelease::StillHeld`] was not retired: no record
/// was written for it and nothing under it was signalled by the tree scan.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LegacyRetirementReport {
    #[serde(default)]
    pub supervisors: Vec<RetiredSupervisor>,
    #[serde(default)]
    pub homes: Vec<LegacyHomeOutcome>,
    #[serde(default)]
    pub reaped: Vec<ReapedProcess>,
    /// Processes the retirement signalled that were still running at its
    /// deadline. They do not fail it: the storage rung's occupancy gate keeps
    /// their tree, and the executor page names them.
    #[serde(default)]
    pub survivors: Vec<ReapedProcess>,
    /// Candidate homes that failed validation, and why. Nothing in them was
    /// touched.
    #[serde(default)]
    pub refused: Vec<RefusedHome>,
    /// Homes whose generation was released but which could not be handed to
    /// the ladder (the marker could not be written, on a full volume say).
    /// Unlike a refusal this is an operation that failed, so the retirement is
    /// not complete and is asked for again.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<RefusedHome>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetiredSupervisor {
    /// The identity it was registered under: a launchd label, a systemd unit,
    /// or a Run-key value name.
    pub identity: String,
    /// Where its registration lived, when it was a file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub outcome: SupervisorOutcome,
    /// Why its registration could not be removed, for
    /// [`SupervisorOutcome::EvictionFailed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SupervisorOutcome {
    /// It was running, and its process is gone.
    Stopped,
    /// It was registered with no live process; its registration was removed.
    NotRunning,
    /// Its process outlived the deadline.
    Survived,
    /// Its registration could not be removed.
    EvictionFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LegacyHomeOutcome {
    pub path: String,
    pub generation: GenerationRelease,
    pub runner_state: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "state"
)]
pub enum GenerationRelease {
    /// A non-blocking try-lock of the home's generation lock succeeded: no
    /// process holds it. The only evidence accepted as proof.
    Released,
    /// Something still held it at the deadline.
    StillHeld { pid: Option<u32> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReapedProcess {
    pub pid: u32,
    pub start_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    pub source: ReapSource,
}

/// Why a process was signalled.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ReapSource {
    /// It was the legacy supervisor and outlived its eviction.
    Supervisor,
    /// It held a legacy home's generation lock after its supervisor stopped.
    GenerationHolder,
    /// A pre-inversion owner record named it, and its start time matched.
    OwnerRecord,
    /// A legacy cell's persisted state recorded it.
    Recorded,
    /// It was running from, or holding files under, a legacy home's build
    /// slots once that home's generation was proven released.
    TreeScan,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RefusedHome {
    pub path: String,
    pub reason: String,
}

impl LegacyRetirementReport {
    /// Nothing legacy was found at all.
    pub fn is_empty(&self) -> bool {
        self.supervisors.is_empty()
            && self.homes.is_empty()
            && self.reaped.is_empty()
            && self.survivors.is_empty()
            && self.refused.is_empty()
            && self.failures.is_empty()
    }

    /// No legacy supervisor remains, and every legacy generation is proven
    /// released. What lets the runner stop asking.
    pub fn is_complete(&self) -> bool {
        self.supervisors.iter().all(|supervisor| {
            matches!(
                supervisor.outcome,
                SupervisorOutcome::Stopped | SupervisorOutcome::NotRunning
            )
        }) && self
            .homes
            .iter()
            .all(|home| home.generation == GenerationRelease::Released)
            && self.failures.is_empty()
    }

    /// The first home whose generation a live process still holds.
    pub fn still_held(&self) -> Option<(&str, Option<u32>)> {
        self.homes.iter().find_map(|home| match home.generation {
            GenerationRelease::StillHeld { pid } => Some((home.path.as_str(), pid)),
            GenerationRelease::Released => None,
        })
    }

    /// One line per fact, for the runner's account of the machine.
    pub fn summary_lines(&self) -> Vec<String> {
        if self.is_empty() {
            return vec!["nothing found: no pre-inversion supervisor or executor home".into()];
        }
        let mut lines = Vec::new();
        for supervisor in &self.supervisors {
            let outcome = match supervisor.outcome {
                SupervisorOutcome::Stopped => "stopped and removed",
                SupervisorOutcome::NotRunning => "was not running; removed",
                SupervisorOutcome::Survived => "STILL RUNNING after its registration was removed",
                SupervisorOutcome::EvictionFailed => "could not be removed",
            };
            let error = supervisor
                .error
                .as_deref()
                .map(|error| format!(" ({error})"))
                .unwrap_or_default();
            let pid = supervisor
                .pid
                .map(|pid| format!(" (pid {pid})"))
                .unwrap_or_default();
            let program = supervisor
                .program
                .as_deref()
                .map(|program| format!(" running {program}"))
                .unwrap_or_default();
            lines.push(format!(
                "supervisor {}{pid}{program}: {outcome}{error}",
                supervisor.identity
            ));
        }
        for home in &self.homes {
            let generation = match home.generation {
                GenerationRelease::Released => "generation released".to_string(),
                GenerationRelease::StillHeld { pid: Some(pid) } => {
                    format!("generation STILL HELD by pid {pid}")
                }
                GenerationRelease::StillHeld { pid: None } => {
                    "generation STILL HELD (pid unrecorded)".to_string()
                }
            };
            let state = if home.runner_state {
                "; holds runner state, which is kept"
            } else {
                ""
            };
            lines.push(format!("home {}: {generation}{state}", home.path));
        }
        let describe = |process: &ReapedProcess| {
            let source = match process.source {
                ReapSource::Supervisor => "supervisor",
                ReapSource::GenerationHolder => "generation holder",
                ReapSource::OwnerRecord => "owner record",
                ReapSource::Recorded => "recorded workload",
                ReapSource::TreeScan => "tree scan",
            };
            match &process.command {
                Some(command) => format!("pid {} {command} ({source})", process.pid),
                None => format!("pid {} ({source})", process.pid),
            }
        };
        if !self.reaped.is_empty() {
            lines.push(format!(
                "reaped {}: {}",
                self.reaped.len(),
                self.reaped
                    .iter()
                    .map(describe)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !self.survivors.is_empty() {
            lines.push(format!(
                "survived signalling {}: {}",
                self.survivors.len(),
                self.survivors
                    .iter()
                    .map(describe)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        for refused in &self.refused {
            lines.push(format!("left alone {}: {}", refused.path, refused.reason));
        }
        for failure in &self.failures {
            lines.push(format!(
                "could not retire {}: {}; retried at the next runner start",
                failure.path, failure.reason
            ));
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn executor_marks(home: &Path) {
        std::fs::create_dir_all(home.join("build-slots")).unwrap();
    }

    #[test]
    fn a_retirement_run_twice_never_duplicates_or_forgets_a_reclaim() {
        let current = tempfile::tempdir().unwrap();
        let entry = RetiredExecutorHome {
            path: "/home/bleu/.cairn-executor".into(),
            retired_at_unix_ms: 1,
            supervisor: Some("cairn-runner.service".into()),
            runner_state: false,
            reclaimed_at_unix_ms: None,
            bytes_freed: None,
            stopped_at: None,
        };
        record_retired_homes(current.path(), std::slice::from_ref(&entry)).unwrap();
        let stopped = std::path::Path::new("/home/bleu/.cairn-executor/build-slots/cairn");
        record_retired_home_progress(current.path(), &entry.path, 200, None, Some(stopped))
            .unwrap();
        assert_eq!(
            read_retired_homes(current.path()).unwrap()[0]
                .stopped_at
                .as_deref(),
            Some(stopped)
        );
        record_retired_home_progress(current.path(), &entry.path, 24, Some(9), None).unwrap();
        assert_eq!(
            read_retired_homes(current.path()).unwrap()[0].stopped_at,
            None
        );
        record_retired_homes(
            current.path(),
            &[RetiredExecutorHome {
                retired_at_unix_ms: 5,
                ..entry.clone()
            }],
        )
        .unwrap();
        let homes = read_retired_homes(current.path()).unwrap();
        assert_eq!(homes.len(), 1);
        assert_eq!(homes[0].retired_at_unix_ms, 1);
        assert_eq!(homes[0].reclaimed_at_unix_ms, Some(9));
        assert_eq!(homes[0].bytes_freed, Some(224));
    }

    #[test]
    fn no_record_is_no_retired_homes() {
        let current = tempfile::tempdir().unwrap();
        assert!(read_retired_homes(current.path()).unwrap().is_empty());
    }

    #[test]
    fn the_validator_refuses_everything_but_a_separate_executor_home() {
        let host = tempfile::tempdir().unwrap();
        let current = host.path().join(".cairn-exec");
        let legacy = host.path().join(".cairn-executor");
        executor_marks(&current);
        executor_marks(&legacy);

        validate_retired_home(&legacy, &current, host.path()).expect("a separate legacy home");

        let refusal = |path: &Path| validate_retired_home(path, &current, host.path()).unwrap_err();
        assert!(refusal(&current).contains("current executor home"));
        assert!(refusal(host.path()).contains("current executor home"));
        let nested = current.join("old");
        executor_marks(&nested);
        assert!(refusal(&nested).contains("inside the current executor home"));
        assert!(refusal(Path::new("relative/home")).contains("not absolute"));

        let unmarked = host.path().join("photos");
        std::fs::create_dir_all(&unmarked).unwrap();
        assert!(refusal(&unmarked).contains("no build-slots"));

        #[cfg(unix)]
        {
            let link = host.path().join("link");
            std::os::unix::fs::symlink(&legacy, &link).unwrap();
            assert!(refusal(&link).contains("symbolic link"));
        }

        // A build-slots that links elsewhere would carry deletion out of the
        // home.
        #[cfg(unix)]
        {
            let outside = host.path().join("unrelated");
            std::fs::create_dir_all(&outside).unwrap();
            let linked = host.path().join("linked-slots");
            std::fs::create_dir_all(&linked).unwrap();
            std::os::unix::fs::symlink(&outside, linked.join("build-slots")).unwrap();
            assert!(refusal(&linked).contains("build-slots is a symbolic link"));
        }

        // The retirement's own marker outlasts the build slots.
        let marked = host.path().join("marked");
        std::fs::create_dir_all(&marked).unwrap();
        write_retired_home_marker(&marked, &current).unwrap();
        validate_retired_home(&marked, &current, host.path()).expect("the marker marks it");

        // An owner record alone marks a pre-inversion home.
        let owner_only = host.path().join("owner-only");
        std::fs::create_dir_all(&owner_only).unwrap();
        std::fs::write(owner_only.join(".cairn-executor-owner-bbs"), "n 1 2").unwrap();
        validate_retired_home(&owner_only, &current, host.path())
            .expect("an owner record marks it");
    }

    #[test]
    fn the_users_home_is_never_a_retired_home_even_when_the_current_home_is_elsewhere() {
        let host = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        executor_marks(host.path());
        let error = validate_retired_home(host.path(), elsewhere.path(), host.path()).unwrap_err();
        assert!(error.contains("user's home"), "{error}");
    }

    #[test]
    fn runner_state_is_a_database_or_a_single_owner_lock() {
        let home = tempfile::tempdir().unwrap();
        assert!(!home_holds_runner_state(home.path()));
        std::fs::write(home.path().join("cairn.db-wal"), "").unwrap();
        assert!(home_holds_runner_state(home.path()));
        let other = tempfile::tempdir().unwrap();
        std::fs::write(other.path().join("runner.lock"), "").unwrap();
        assert!(home_holds_runner_state(other.path()));
    }

    #[test]
    fn a_report_is_complete_only_when_every_generation_is_released() {
        let mut report = LegacyRetirementReport::default();
        assert!(report.is_empty() && report.is_complete());
        assert_eq!(report.summary_lines().len(), 1);
        report.homes.push(LegacyHomeOutcome {
            path: "/home/bleu/.cairn-executor".into(),
            generation: GenerationRelease::StillHeld { pid: Some(41) },
            runner_state: false,
        });
        assert!(!report.is_complete());
        assert_eq!(
            report.still_held(),
            Some(("/home/bleu/.cairn-executor", Some(41)))
        );
        let failed = LegacyRetirementReport {
            failures: vec![RefusedHome {
                path: "/home/bleu/.cairn-executor".into(),
                reason: "No space left on device".into(),
            }],
            ..LegacyRetirementReport::default()
        };
        assert!(
            !failed.is_complete(),
            "a home that could not be handed over is not retired"
        );
        assert!(!failed.is_empty());
        let json = serde_json::to_string(&report).unwrap();
        assert!(
            json.contains(r#""generation":{"state":"stillHeld","pid":41}"#),
            "{json}"
        );
        assert_eq!(
            serde_json::from_str::<LegacyRetirementReport>(&json).unwrap(),
            report
        );
    }
}
