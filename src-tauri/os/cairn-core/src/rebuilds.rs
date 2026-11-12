//! Credential-free journal ingestion for the detached macOS rebuild consumer.
//!
//! The path names an input, not its authority: reads use a checked directory
//! descriptor and no-follow openat, then check the opened inode. Read-only ledger
//! resources are portable; Linux/Windows do not run this privileged consumer.
use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
#[cfg(target_os = "macos")]
use std::path::PathBuf;

use cairn_db::storage::rebuilds::{self as ledger, IngestionFault};
use serde_json::Value;

use crate::storage::LocalDb;

// Slice one's journals are intentionally readable, but only their owner writes.
// Owner rw[x] is required; group/world may read (and traverse directories),
// never write. This admits slice one's umask-restricted 0755/0644 files without
// weakening integrity. Special mode bits are not part of this format.
const DIRECTORY_MODE: u32 = 0o700;
const JOURNAL_MODE: u32 = 0o600;
const MAX_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LINE_BYTES: usize = 1024 * 1024;
/// The private copy of bun the actor runs from, staged at `<root>/<id>/bun`.
pub(crate) const ACTOR_RUNTIME: &str = "bun";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorState {
    /// launchd reports the label's process running.
    Running,
    /// The label is still loaded, and launchd reports a process that ran and
    /// is gone. A one-shot's definition stays like this until someone boots it
    /// out, so this is death evidence, never a reason to wait.
    Exited,
    /// launchd reports no such label in the domain.
    Absent,
    Unknown,
}

impl ActorState {
    /// launchd positively shows that no actor process is alive.
    pub fn actor_gone(self) -> bool {
        matches!(self, Self::Absent | Self::Exited)
    }
}

/// One `launchctl print` of the actor label: the state, and what the print
/// says about the definition holding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorJob {
    pub state: ActorState,
    pub pid: Option<u32>,
    pub program: Option<String>,
    /// launchd's own record of how the last process ended, verbatim: its exit
    /// code, terminating signal or exit reason lines, joined.
    pub last_exit: Option<String>,
}

impl From<ActorState> for ActorJob {
    fn from(state: ActorState) -> Self {
        Self {
            state,
            pid: None,
            program: None,
            last_exit: None,
        }
    }
}

fn fault(name: &str, detail: impl Into<String>) -> IngestionFault {
    IngestionFault {
        name: name.into(),
        detail: detail.into(),
    }
}

fn check_metadata(
    file: &File,
    uid: u32,
    directory: bool,
    name: &str,
) -> Result<(), IngestionFault> {
    let metadata = file
        .metadata()
        .map_err(|e| fault("metadata_unreadable", format!("{name}: {e}")))?;
    if metadata.uid() != uid {
        return Err(fault(
            "foreign_owner",
            format!("{name}: owned by uid {}, expected {uid}", metadata.uid()),
        ));
    }
    let expected = if directory {
        DIRECTORY_MODE
    } else {
        JOURNAL_MODE
    };
    let mode = metadata.mode() & 0o7777;
    let allowed = if directory { 0o755 } else { 0o644 };
    if mode & 0o700 != expected || mode & !allowed != 0 {
        return Err(fault("unexpected_mode", format!("{name}: mode {mode:04o}, expected owner {expected:04o} with optional read/traverse permissions only")));
    }
    if directory {
        if !metadata.is_dir() {
            return Err(fault("not_directory", name));
        }
    } else {
        if !metadata.is_file() {
            return Err(fault("not_regular_file", name));
        }
        if metadata.nlink() != 1 {
            return Err(fault(
                "hardlink_refused",
                format!("{name}: link count {}", metadata.nlink()),
            ));
        }
    }
    Ok(())
}

fn open_directory(root: &Path, uid: u32) -> Result<Option<File>, IngestionFault> {
    let name = root.display().to_string();
    let path =
        CString::new(root.as_os_str().as_bytes()).map_err(|_| fault("invalid_directory", &name))?;
    // SAFETY: path is NUL-terminated; open returns an owned descriptor.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        let symlink = std::fs::symlink_metadata(root).is_ok_and(|m| m.file_type().is_symlink());
        return Err(fault(
            if symlink {
                "directory_symlink_refused"
            } else {
                "directory_unreadable"
            },
            format!("{name}: {error}"),
        ));
    }
    // SAFETY: successful open transferred this descriptor to us.
    let file = unsafe { File::from_raw_fd(fd) };
    check_metadata(&file, uid, true, &name)?;
    Ok(Some(file))
}

#[cfg(target_os = "macos")]
fn errno_pointer() -> *mut libc::c_int {
    // SAFETY: __error returns the calling thread\'s errno location.
    unsafe { libc::__error() }
}

#[cfg(target_os = "linux")]
fn errno_pointer() -> *mut libc::c_int {
    // SAFETY: __errno_location returns the calling thread\'s errno location.
    unsafe { libc::__errno_location() }
}

/// Enumerate the checked inode, not a second path-based open of its directory.
fn journal_names(directory: &File) -> Result<Vec<String>, IngestionFault> {
    // SAFETY: dup borrows a valid descriptor; fdopendir takes the duplicate.
    let fd = unsafe { libc::dup(directory.as_raw_fd()) };
    if fd < 0 {
        return Err(fault(
            "directory_unreadable",
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: fd is an owned duplicate of an open directory.
    let pointer = unsafe { libc::fdopendir(fd) };
    if pointer.is_null() {
        let error = std::io::Error::last_os_error();
        // SAFETY: fdopendir failed, so ownership remains here.
        unsafe {
            libc::close(fd);
        }
        return Err(fault("directory_unreadable", error.to_string()));
    }
    struct Directory(*mut libc::DIR);
    impl Drop for Directory {
        fn drop(&mut self) {
            // SAFETY: this guard uniquely owns the directory stream.
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let stream = Directory(pointer);
    let mut names = Vec::new();
    loop {
        // Null means either EOF or I/O failure. Clear thread-local errno so
        // failed enumeration is refused rather than mistaken for an empty dir.
        // SAFETY: stream and errno remain live on this thread.
        let entry = unsafe {
            *errno_pointer() = 0;
            libc::readdir(stream.0)
        };
        if entry.is_null() {
            // SAFETY: errno_pointer returns the calling thread\'s errno cell.
            let errno = unsafe { *errno_pointer() };
            if errno != 0 {
                return Err(fault(
                    "directory_unreadable",
                    std::io::Error::from_raw_os_error(errno).to_string(),
                ));
            }
            break;
        }
        // SAFETY: readdir supplies a NUL-terminated d_name until the next call.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        let name = name.to_string_lossy();
        if name.ends_with(".jsonl") {
            names.push(name.into_owned());
        }
    }
    names.sort();
    Ok(names)
}

fn read_journal(directory: &File, name: &str, uid: u32) -> Result<Vec<u8>, IngestionFault> {
    check_metadata(directory, uid, true, "rebuilds directory")?;
    let path = CString::new(name).map_err(|_| fault("invalid_filename", name))?;
    // SAFETY: the directory descriptor is checked; name is one basename, never
    // an actor-supplied path. NONBLOCK ensures a substituted FIFO cannot hang boot.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            path.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        return Err(fault(
            if error.raw_os_error() == Some(libc::ELOOP) {
                "symlink_refused"
            } else {
                "journal_unreadable"
            },
            format!("{name}: {error}"),
        ));
    }
    // SAFETY: successful openat returned an owned descriptor.
    let mut file = unsafe { File::from_raw_fd(fd) };
    check_metadata(&file, uid, false, name)?;
    let length = file
        .metadata()
        .map_err(|e| fault("metadata_unreadable", format!("{name}: {e}")))?
        .len();
    if length > MAX_JOURNAL_BYTES {
        return Err(fault(
            "journal_too_large",
            format!("{name}: {length} bytes exceeds {MAX_JOURNAL_BYTES}"),
        ));
    }
    let mut bytes = Vec::new();
    // An append racing this snapshot belongs to the next pass. Never read an
    // unbounded stream, even when a trusted actor is still writing.
    (&mut file)
        .take(length)
        .read_to_end(&mut bytes)
        .map_err(|e| fault("journal_unreadable", format!("{name}: {e}")))?;
    check_metadata(&file, uid, false, name)?;
    check_metadata(directory, uid, true, "rebuilds directory")?;
    if bytes.len() as u64 != length {
        return Err(fault(
            "journal_short_read",
            format!("{name}: read {} of {length} snapshot bytes", bytes.len()),
        ));
    }
    Ok(bytes)
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn validate_event(value: &Value, rebuild: &str) -> Result<(), &'static str> {
    if !value.is_object()
        || !value["event"].as_str().is_some_and(identifier)
        || value["rebuild"].as_str() != Some(rebuild)
        || !value["at"]
            .as_str()
            .is_some_and(|at| chrono::DateTime::parse_from_rfc3339(at).is_ok())
    {
        return Err("invalid_event_envelope");
    }
    let optional_text = |key: &str| value.get(key).is_none_or(Value::is_string);
    match value["type"].as_str() {
        Some("phase") => {
            if !matches!(
                value["name"].as_str(),
                Some(
                    "free-space"
                        | "fetch"
                        | "preflight"
                        | "dependencies"
                        | "build"
                        | "signature"
                        | "voice-engine"
                        | "executor-coverage"
                        | "backup"
                        | "stop-runner"
                        | "quit-desktop"
                        | "resume-previous"
                        | "install"
                        | "start-runner"
                        | "verify-runner"
                        | "desktop-adoption"
                        | "voice-handoff"
                        | "rollback"
                )
            ) {
                return Err("unexpected_phase");
            }
            if !value["name"].as_str().is_some_and(|s| !s.trim().is_empty())
                || !matches!(value["state"].as_str(), Some("started" | "ended"))
                || !optional_text("detail")
                || value
                    .get("outcome")
                    .is_some_and(|v| !matches!(v.as_str(), Some("ok" | "failed")))
            {
                return Err("malformed_phase");
            }
        }
        Some("fact") if value["facts"].is_object() => {}
        Some("fact") => return Err("malformed_fact"),
        Some("outcome") => {
            if !matches!(
                value["outcome"].as_str(),
                Some("done" | "failed" | "inconclusive" | "rolled_back" | "cancelled")
            ) || !value["summary"].is_string()
            {
                return Err("malformed_outcome");
            }
        }
        _ => return Err("unknown_event_type"),
    }
    Ok(())
}

type Entry = (String, String, Value);

fn parse_journal(bytes: &[u8], id: &str) -> (Vec<Entry>, Vec<IngestionFault>) {
    let mut entries = Vec::new();
    let mut faults = Vec::new();
    if bytes.is_empty() {
        return (entries, faults);
    }
    // Only the delimiter after the final record is omitted; blank records and
    // a torn last record are refused, never silently skipped.
    let records = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    for (index, line) in records.split(|b| *b == b'\n').enumerate() {
        let location = format!("{id}.jsonl line {}", index + 1);
        if line.len() > MAX_LINE_BYTES {
            faults.push(fault("line_too_large", location));
            continue;
        }
        let parsed = std::str::from_utf8(line).ok().and_then(|raw| {
            serde_json::from_str::<Value>(raw)
                .ok()
                .map(|value| (raw, value))
        });
        match parsed {
            Some((raw, value)) => match validate_event(&value, id) {
                Ok(()) => entries.push((
                    value["event"].as_str().expect("validated event").into(),
                    raw.into(),
                    value,
                )),
                Err(name) => faults.push(fault(name, location)),
            },
            None => faults.push(fault("malformed_line", location)),
        }
    }
    (entries, faults)
}

struct JournalRead {
    id: String,
    entries: Vec<Entry>,
    faults: Vec<IngestionFault>,
}

fn scan(root: &Path, uid: u32) -> Result<Vec<JournalRead>, IngestionFault> {
    let Some(directory) = open_directory(root, uid)? else {
        return Ok(vec![]);
    };
    let mut journals = Vec::new();
    for name in journal_names(&directory)? {
        let id = name.strip_suffix(".jsonl").expect("journal suffix");
        if !identifier(id) {
            tracing::warn!(journal = %name, "rebuild journal refused: invalid filename");
            continue;
        }
        let (entries, faults) = match read_journal(&directory, &name, uid) {
            Ok(bytes) => parse_journal(&bytes, id),
            Err(error) => (vec![], vec![error]),
        };
        if entries
            .iter()
            .any(|(_, _, event)| event["type"] == "outcome")
        {
            release_actor_runtime(&directory, id);
        }
        journals.push(JournalRead {
            id: id.into(),
            entries,
            faults,
        });
    }
    Ok(journals)
}

/// Remove a finished rebuild's staged bun copy (~85 MiB). Once its journal
/// holds an outcome the actor needs it no longer; everything else in `<id>/`
/// (spec, script, build and actor logs) is the ledger's evidence and stays.
/// The actor may still be exiting when its outcome lands, which is safe: an
/// unlinked executable stays mapped until its process ends.
///
/// Reached through the checked root descriptor with no-follow opens, like every
/// read here, and only a regular file is unlinked. Best-effort: a failure
/// leaves the copy for the next pass.
fn release_actor_runtime(directory: &File, id: &str) {
    let (Ok(id_name), Ok(runtime)) = (CString::new(id), CString::new(ACTOR_RUNTIME)) else {
        return;
    };
    // SAFETY: the root descriptor is checked and `id` is a validated basename.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            id_name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        // No staged directory: a runner refusal, or a journal written by hand.
        return;
    }
    // SAFETY: successful openat returned an owned descriptor.
    let staged = unsafe { File::from_raw_fd(fd) };
    // SAFETY: an all-zero `stat` is a valid out-parameter for fstatat.
    let mut status: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: both descriptors and the NUL-terminated name are live.
    let found = unsafe {
        libc::fstatat(
            staged.as_raw_fd(),
            runtime.as_ptr(),
            &mut status,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } == 0;
    if !found || status.st_mode & libc::S_IFMT != libc::S_IFREG {
        return;
    }
    // SAFETY: as above; flag 0 unlinks a file, never a directory.
    if unsafe { libc::unlinkat(staged.as_raw_fd(), runtime.as_ptr(), 0) } != 0 {
        let error = std::io::Error::last_os_error();
        tracing::warn!(rebuild = id, %error, "could not remove a finished rebuild's bun copy");
    }
}

/// One boot/live pass. The injected liveness observation is deliberately
/// three-valued: an unreadable launchctl never turns silence into death.
pub async fn ingest_once(db: &LocalDb, root: &Path, actor: ActorState) -> Result<usize, String> {
    ingest_with_observation(db, root, || std::future::ready(actor)).await
}

async fn ingest_with_observation<F, Observation>(
    db: &LocalDb,
    root: &Path,
    observe: F,
) -> Result<usize, String>
where
    F: FnOnce() -> Observation,
    Observation: std::future::Future<Output = ActorState>,
{
    let root = root.to_path_buf();
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let scanned = tokio::task::spawn_blocking(move || scan(&root, uid))
        .await
        .map_err(|e| e.to_string())?;
    let (journals, directory_fault) = match scanned {
        Ok(journals) => (journals, None),
        Err(error) => {
            tracing::warn!(fault = %error.name, detail = %error.detail, "rebuild ingestion refused directory");
            (vec![], Some(error))
        }
    };
    // An absence observed before the scan could refer to the moment just
    // before an operator bootstrapped the actor whose journal we then read.
    let actor = observe().await;
    let mut seen = HashSet::new();
    let mut applied = 0;
    for journal in journals {
        seen.insert(journal.id.clone());
        // Refused/unreadable bytes cannot establish that a terminal record is
        // absent. Keep the fault without inventing an actor-death outcome.
        let can_infer_death = journal.faults.is_empty() && actor.actor_gone();
        let report = ledger::ingest(
            db,
            &journal.id,
            journal.entries,
            journal.faults,
            can_infer_death,
        )
        .await
        .map_err(|e| e.to_string())?;
        applied += report.applied;
        log_faults(&journal.id, &report.new_faults);
    }
    // A deleted/unreadable journal must not silently vanish from the ledger.
    let mut offset = 0;
    loop {
        let rows = ledger::list(db, 256, offset)
            .await
            .map_err(|e| e.to_string())?;
        let count = rows.len();
        for row in rows {
            // A launch intent not yet staged has no journal by design.
            let unstaged = row.events.is_empty()
                && row
                    .launch
                    .as_ref()
                    .is_some_and(|launch| launch.bootstrap_requested_at.is_none());
            if seen.contains(&row.id) || unstaged {
                continue;
            }
            let error = directory_fault
                .clone()
                .unwrap_or_else(|| fault("journal_missing", format!("{}.jsonl is absent", row.id)));
            let report = ledger::ingest(db, &row.id, vec![], vec![error], false)
                .await
                .map_err(|e| e.to_string())?;
            log_faults(&row.id, &report.new_faults);
        }
        if count < 256 {
            break;
        }
        offset += count;
    }
    Ok(applied)
}

fn log_faults(id: &str, faults: &[IngestionFault]) {
    for error in faults {
        tracing::warn!(rebuild = id, fault = %error.name, detail = %error.detail, "rebuild journal ingestion refused");
    }
}

/// Only a successful print or the specific service-not-found diagnostic is
/// evidence. Permission errors, missing domains, failed spawns and timeouts are
/// UNKNOWN, never an absent actor.
fn job_state(code: Option<i32>, stdout: &str, stderr: &str) -> ActorJob {
    if code == Some(0) {
        return parse_print(stdout);
    }
    if code == Some(113)
        && stderr.contains("Could not find service \"computer.cairn.rebuild\"")
        && stderr.contains("in domain for user gui:")
    {
        ActorState::Absent.into()
    } else {
        ActorState::Unknown.into()
    }
}

/// Reads only the service's own top-level keys: nested blocks (coalitions,
/// endpoints) carry their own `state` and `active count`. `Exited` needs the
/// whole picture — not running, nothing active, no pid, and a recorded exit
/// (a code, a terminating signal or an exit reason) of a process that ran; a job never spawned, or any state launchd may add, is
/// unknown.
fn parse_print(stdout: &str) -> ActorJob {
    let mut depth = 0usize;
    let mut fields = std::collections::HashMap::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line == "}" {
            depth = depth.saturating_sub(1);
            continue;
        }
        if depth == 1 {
            if let Some((key, value)) = line.split_once(" = ") {
                fields.entry(key).or_insert(value);
            }
        }
        if line.ends_with('{') {
            depth += 1;
        }
    }
    let number = |key: &str| fields.get(key).and_then(|value| value.parse::<u32>().ok());
    let pid = number("pid");
    // A clean exit prints its code. A killed process prints no code: a signal
    // death prints its terminating signal, a jetsam kill only an exit reason.
    let exits: Vec<String> = [
        "last exit code",
        "last terminating signal",
        "last exit reason",
    ]
    .into_iter()
    .filter_map(|key| fields.get(key).map(|value| format!("{key} = {value}")))
    .collect();
    let exit_recorded = fields.contains_key("last terminating signal")
        || fields.contains_key("last exit reason")
        || fields
            .get("last exit code")
            .is_some_and(|code| *code != "(never exited)");
    let ran_and_exited = number("runs").is_some_and(|runs| runs > 0) && exit_recorded;
    let state = match fields.get("state").copied() {
        Some("running") => ActorState::Running,
        Some("not running")
            if pid.is_none() && number("active count") == Some(0) && ran_and_exited =>
        {
            ActorState::Exited
        }
        _ => ActorState::Unknown,
    };
    ActorJob {
        state,
        pid,
        program: fields.get("program").map(|program| program.to_string()),
        last_exit: (!exits.is_empty()).then(|| exits.join("; ")),
    }
}

#[cfg(target_os = "macos")]
pub(crate) async fn actor_state() -> ActorJob {
    // SAFETY: getuid has no preconditions.
    let target = format!("gui/{}/computer.cairn.rebuild", unsafe { libc::getuid() });
    let mut command = tokio::process::Command::new("/bin/launchctl");
    command.args(["print", &target]).kill_on_drop(true);
    match tokio::time::timeout(std::time::Duration::from_secs(5), command.output()).await {
        Ok(Ok(output)) => job_state(
            output.status.code(),
            &String::from_utf8_lossy(&output.stdout),
            &String::from_utf8_lossy(&output.stderr),
        ),
        _ => ActorState::Unknown.into(),
    }
}

/// Await boot reconciliation before jobs can resume, then consume live appends.
/// Polling avoids watching an unchecked pathname and also detects actor death
/// without any final filesystem event. Every pass reopens and rechecks inodes.
#[cfg(target_os = "macos")]
pub async fn start(db: std::sync::Arc<LocalDb>, root: PathBuf) {
    let observe = || async { actor_state().await.state };
    if let Err(error) = ingest_with_observation(&db, &root, observe).await {
        tracing::warn!(%error, "rebuild boot ingestion failed");
    }
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            if let Err(error) = ingest_with_observation(&db, &root, observe).await {
                tracing::warn!(%error, "rebuild live ingestion failed");
            }
        }
    });
}

// Requests drive only the macOS desktop rebuild; other runners never register it.
#[cfg(target_os = "macos")]
pub(crate) mod consumer;

#[cfg(test)]
mod tests;
