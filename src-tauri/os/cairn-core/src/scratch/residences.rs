//! The runner's scratch root (`<cairn_home>/scratch`), reconciled against who
//! owns each entry in it.
//!
//! The root holds one residence per job (see [`super`]): the agent process's
//! working directory, named for its node. It is not the node's `$TMPDIR`,
//! which is the portable node scratch ([`crate::node_scratch`]), but it is the
//! node's all the same and it ends with the node. The root also holds
//! directories nothing registered: before the fence stopped granting the whole
//! root for writes (it now grants a spawn only its own `$TMPDIR`, see
//! `cairn_sandbox::scratch_refusal`), an agent that wanted a large host-local
//! workspace could make one beside its residence. Those belong to no node, so
//! no node's end reclaims them.
//!
//! Every top-level entry is therefore one of:
//!
//! - **live**: a job registered to it belongs to a node that has not ended, is
//!   held by a live agent process or REPL, or could not be judged;
//! - **ended**: every job registered to it belongs to a resolved issue, a
//!   closed thread, or nothing that still exists;
//! - **unowned**: nothing registered names it.
//!
//! An ended residence goes as soon as nothing holds it: when its owner ends,
//! at runner start, and on every maintenance tick. A live residence is never
//! taken, only reported. An unowned entry goes once nothing has been written
//! into it for seven days, on any pass that measured it, pressure or not.
//! Under disk pressure it goes sooner, after a shorter quiet floor, and among
//! those the largest goes first, so the pass meets its ask by destroying as few
//! entries as it can. Either way an unowned entry goes only if its removal
//! would free at least [`UNOWNED_SIZE_FLOOR`]. An entry under the size floor is
//! never taken: removing it relieves nothing a volume can notice, and a small
//! hand-placed file (a key, a config, a note) is exactly what someone keeps on
//! purpose and cannot regenerate.
//!
//! The root is Cairn's: a large unowned directory in it is reclaimed once it
//! goes quiet, so durable files, credentials especially, belong outside it.
//!
//! Sizes are measured twice: logically (what `du` reports) and as exclusive
//! bytes ([`cairn_common::cow::exclusive_bytes`]), which on a copy-on-write
//! volume is what removal actually frees. A tree full of clones can be hundreds
//! of gigabytes logically and free a few, so the pressure pass credits
//! exclusive bytes, or the free space the volume visibly gained, never logical
//! ones.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};

use cairn_common::executor_protocol::{
    StoragePressureBand, IDLE_RETENTION_PRESSURE_FREE_BYTES, STORAGE_CRITICAL_FREE_BYTES,
};

use super::{
    decode_component, encode_component, is_job_dir_name, RESPONSE_SCRATCH_DIR, SCRATCH_MAP_DIR,
};
use crate::db::DbState;
use crate::node_scratch::owner::{owner_fate, OwnerFate};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// How often the maintenance pass runs. Its first tick is at runner start,
/// which is what catches nodes that ended while the runner was down.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// How long a measurement stands before a tick takes another, when disk
/// pressure does not ask for one sooner. Measuring visits every file.
const CENSUS_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
/// How long nothing may have been written into an unowned entry before disk
/// pressure takes it: long enough that a workspace an agent is still working
/// in is left alone.
const UNOWNED_QUIET_UNDER_PRESSURE: Duration = Duration::from_secs(24 * 60 * 60);
/// The same floor at the critical line, where admission is close.
const UNOWNED_QUIET_WHEN_CRITICAL: Duration = Duration::from_secs(60 * 60);
/// How long nothing may have been written into an unowned entry before a pass
/// takes it with no disk pressure at all. Nothing owns such an entry, so no
/// node's end will ever reclaim it; a week of silence is the evidence that
/// nobody is still working in it.
pub const UNOWNED_QUIET_OUTSIDE_PRESSURE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// The fewest exclusive bytes an unowned entry must hold for pressure to take
/// it, in either band. The pressure line is 64 GiB, so an ask is normally
/// gigabytes, and an entry under this frees a sixty-thousandth of the line at
/// most: what pressure is relieved by (build trees, datasets, clones) sits
/// orders above it, and what a person places by hand sits well below it. A
/// fixed floor, rather than a share of the ask, gives an author one rule to
/// read, where a share would spare a file under one ask and take it under the
/// next.
pub(crate) const UNOWNED_SIZE_FLOOR: u64 = 1024 * 1024;
/// What an entry is renamed to before its removal starts, so nothing resolves
/// its name to a half-deleted tree, and a removal the runner dies in the middle
/// of is finished by the next tick.
const RECLAIMING_PREFIX: &str = ".reclaiming-";
/// Removals the report keeps, newest last.
const REMOVALS_KEPT: usize = 20;

/// Who answers for one top-level entry of the root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Owner {
    /// A job registered to it is live, is held by a live agent process or
    /// REPL, or could not be judged.
    Live { jobs: Vec<String> },
    /// Every job registered to it belongs to a node that has ended.
    Ended {
        jobs: Vec<String>,
        reason: &'static str,
    },
    /// Nothing registered names it.
    Unowned,
}

/// What one entry occupies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Footprint {
    /// File lengths summed: what `du` reports, and what a clone overstates.
    pub logical_bytes: u64,
    /// Each file's bytes no other file shares, summed. Removing the entry frees
    /// at least this much; blocks its own files share with one another count in
    /// neither file, so it can free more.
    pub exclusive_bytes: u64,
    /// The newest modification time of anything in the tree.
    pub newest_write_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub path: PathBuf,
    pub owner: Owner,
    pub footprint: Option<Footprint>,
}

/// One entry the runner removed, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Removal {
    pub name: String,
    pub why: String,
    pub footprint: Footprint,
    pub at_unix_ms: u64,
}

/// The free-space lines a pass judges the volume against: the colocated
/// executor's own, so the runner's root and the executor's trees answer to one
/// verdict about the volume they share.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lines {
    pub pressure_free_bytes: u64,
    pub critical_free_bytes: u64,
}

impl Default for Lines {
    fn default() -> Self {
        Self {
            pressure_free_bytes: IDLE_RETENTION_PRESSURE_FREE_BYTES,
            critical_free_bytes: STORAGE_CRITICAL_FREE_BYTES,
        }
    }
}

impl Lines {
    pub fn band(&self, free_bytes: u64) -> Option<StoragePressureBand> {
        if free_bytes <= self.critical_free_bytes {
            Some(StoragePressureBand::Critical)
        } else if free_bytes <= self.pressure_free_bytes {
            Some(StoragePressureBand::Pressure)
        } else {
            None
        }
    }
}

fn quiet_floor(band: StoragePressureBand) -> Duration {
    match band {
        StoragePressureBand::Pressure => UNOWNED_QUIET_UNDER_PRESSURE,
        StoragePressureBand::Critical => UNOWNED_QUIET_WHEN_CRITICAL,
    }
}

/// A count of entries and what they occupy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub count: usize,
    pub logical_bytes: u64,
    pub exclusive_bytes: u64,
}

impl Tally {
    fn add(&mut self, footprint: &Footprint) {
        self.count += 1;
        self.logical_bytes = self.logical_bytes.saturating_add(footprint.logical_bytes);
        self.exclusive_bytes = self
            .exclusive_bytes
            .saturating_add(footprint.exclusive_bytes);
    }
}

/// What a pass under disk pressure asked for and took.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PressurePass {
    pub at_unix_ms: u64,
    pub band: StoragePressureBand,
    pub free_bytes_before: u64,
    pub free_bytes_after: u64,
    /// The pressure line less free space when the pass started.
    pub asked_bytes: u64,
    /// Per removal, the larger of its exclusive bytes and the free space the
    /// volume gained across it, summed.
    pub credited_bytes: u64,
    pub removed: Vec<Removal>,
    /// Live residences, which pressure never takes.
    pub kept_live: Tally,
    /// Unowned entries written to inside the quiet floor.
    pub kept_quiet: Tally,
    /// Unowned entries past the quiet floor whose exclusive bytes are under
    /// [`UNOWNED_SIZE_FLOOR`], which pressure never takes.
    pub kept_small: Tally,
}

/// Every entry as the last measurement found it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Census {
    pub measured_at_unix_ms: u64,
    pub entries: Vec<Entry>,
}

/// What the executor page renders: the last census, recent removals of ended
/// residences, recent seven-day removals of unowned entries, and the last
/// pressure pass.
#[derive(Clone, Debug, Default)]
pub struct Report {
    pub census: Option<Census>,
    pub removals: VecDeque<Removal>,
    /// Kept apart from `removals`: the ended residences a first pass sweeps
    /// number in the hundreds and would push these out.
    pub quiet_removals: VecDeque<Removal>,
    /// Every seven-day removal since the runner started.
    pub quiet_reclaimed: Tally,
    pub pressure: Option<PressurePass>,
}

/// The orchestrator's handle on the root: one pass at a time, and its report.
#[derive(Default)]
pub struct ResidenceState {
    pass: tokio::sync::Mutex<()>,
    report: Mutex<Report>,
}

impl ResidenceState {
    pub fn report(&self) -> Report {
        self.report.lock().unwrap().clone()
    }
}

/// One top-level entry and the jobs that may answer for it, before any
/// database has been asked.
struct Candidate {
    name: String,
    path: PathBuf,
    registered: Vec<String>,
    /// For an entry nothing registered, the job id its name would be if it is
    /// a residence created before its node's URI resolved.
    unregistered_job: Option<String>,
}

struct Scan {
    candidates: Vec<Candidate>,
    /// Every registry record: (job id, the directory name it registers).
    registrations: Vec<(String, String)>,
    /// Removals an earlier pass staged and did not finish.
    staged: Vec<PathBuf>,
}

fn registrations(root: &Path) -> Vec<(String, String)> {
    let Ok(entries) = std::fs::read_dir(root.join(SCRATCH_MAP_DIR)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let job = decode_component(entry.file_name().to_str()?)?;
            let name = std::fs::read_to_string(entry.path()).ok()?;
            let name = name.trim();
            is_job_dir_name(name).then(|| (job, name.to_owned()))
        })
        .collect()
}

/// Whether two paths name one directory entry, without following a link.
#[cfg(unix)]
fn same_entry(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (
        std::fs::symlink_metadata(left),
        std::fs::symlink_metadata(right),
    ) {
        (Ok(left), Ok(right)) => left.dev() == right.dev() && left.ino() == right.ino(),
        _ => false,
    }
}

/// Whether two paths name one directory entry. Without a file id in std this
/// resolves both; a link entry read as its target errs toward live.
#[cfg(not(unix))]
fn same_entry(left: &Path, right: &Path) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn scan(root: &Path) -> Scan {
    let registrations = registrations(root);
    let mut by_name: HashMap<&str, Vec<String>> = HashMap::new();
    for (job, name) in &registrations {
        by_name.entry(name.as_str()).or_default().push(job.clone());
    }
    // A case-insensitive volume (APFS, NTFS) lists a residence under the
    // spelling it was created with, which a registration rewritten since (a
    // project key lowercased) no longer matches as text. Read as text, a live
    // thread's residence was unowned, and the live-process check never ran.
    let mut by_folded: HashMap<String, Vec<&str>> = HashMap::new();
    for name in by_name.keys() {
        by_folded
            .entry(name.to_lowercase())
            .or_default()
            .push(*name);
    }
    let mut candidates = Vec::new();
    let mut staged = Vec::new();
    for entry in std::fs::read_dir(root).into_iter().flatten().flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(RECLAIMING_PREFIX) {
            staged.push(path);
            continue;
        }
        if name == SCRATCH_MAP_DIR || name == RESPONSE_SCRATCH_DIR {
            continue;
        }
        let registered = match by_name.get(name.as_str()) {
            Some(jobs) => jobs.clone(),
            None => by_folded
                .get(&name.to_lowercase())
                .into_iter()
                .flatten()
                .filter(|spelling| same_entry(&root.join(spelling), &path))
                .flat_map(|spelling| by_name[spelling].iter().cloned())
                .collect(),
        };
        let unregistered_job = registered
            .is_empty()
            .then(|| decode_component(&name))
            .flatten()
            .filter(|job| !job.is_empty() && !job.contains('/'));
        candidates.push(Candidate {
            name,
            path,
            registered,
            unregistered_job,
        });
    }
    candidates.sort_by(|a, b| a.name.cmp(&b.name));
    Scan {
        candidates,
        registrations,
        staged,
    }
}

/// A job's standing, for deciding whose its residence is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum JobFate {
    Owner(OwnerFate),
    /// No job row: the job's issue or project was deleted, or the name was
    /// never a job id at all.
    Missing,
}

async fn job_fate(
    dbs: &DbState,
    job_id: &str,
    owners: &mut HashMap<(Option<String>, Option<String>), OwnerFate>,
) -> JobFate {
    let db = match crate::execution::routing::routing_db_for_id(dbs, job_id).await {
        Ok(db) => db,
        Err(error) => return JobFate::Owner(OwnerFate::Unknown(error.to_string())),
    };
    let row = db
        .query_opt(
            "SELECT issue_id, thread_id FROM jobs WHERE id = ?1",
            (job_id.to_owned(),),
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?)),
        )
        .await;
    match row {
        Err(error) => JobFate::Owner(OwnerFate::Unknown(error.to_string())),
        Ok(None) => JobFate::Missing,
        Ok(Some(key)) => {
            if let Some(fate) = owners.get(&key) {
                return JobFate::Owner(fate.clone());
            }
            let fate = owner_fate(dbs, key.0.as_deref(), key.1.as_deref()).await;
            owners.insert(key, fate.clone());
            JobFate::Owner(fate)
        }
    }
}

/// Jobs whose residence a live process may be using: an agent process the
/// runner holds, or a live REPL.
fn held_jobs(orch: &Orchestrator) -> HashSet<String> {
    let mut held = orch.process_state.live_job_ids();
    held.extend(
        orch.repl_state
            .liveness()
            .into_iter()
            .filter(|(_, (alive, _))| *alive)
            .map(|((job, _), _)| job),
    );
    held
}

fn classify(
    candidate: &Candidate,
    fates: &HashMap<String, JobFate>,
    held: &HashSet<String>,
) -> Owner {
    let jobs = if !candidate.registered.is_empty() {
        candidate.registered.clone()
    } else {
        match &candidate.unregistered_job {
            Some(job) if matches!(fates.get(job), Some(JobFate::Owner(_))) => vec![job.clone()],
            _ => return Owner::Unowned,
        }
    };
    let mut reason = None;
    for job in &jobs {
        if held.contains(job) {
            return Owner::Live { jobs };
        }
        match fates.get(job) {
            Some(JobFate::Missing) => {
                reason.get_or_insert("its job no longer exists");
            }
            Some(JobFate::Owner(OwnerFate::Ended(why))) => {
                reason.get_or_insert(*why);
            }
            _ => return Owner::Live { jobs },
        }
    }
    match reason {
        Some(reason) => Owner::Ended { jobs, reason },
        None => Owner::Live { jobs },
    }
}

fn unix_ms(time: std::time::SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

/// Measure a tree without following a link out of it. Best-effort: an entry
/// that vanishes or cannot be read is skipped.
pub fn measure(path: &Path) -> Footprint {
    let mut footprint = Footprint::default();
    let mut pending = vec![path.to_path_buf()];
    while let Some(current) = pending.pop() {
        let Ok(metadata) = std::fs::symlink_metadata(&current) else {
            continue;
        };
        if let Ok(modified) = metadata.modified() {
            footprint.newest_write_unix_ms = footprint.newest_write_unix_ms.max(unix_ms(modified));
        }
        if metadata.is_dir() {
            if let Ok(children) = std::fs::read_dir(&current) {
                pending.extend(children.flatten().map(|child| child.path()));
            }
        } else if metadata.is_file() {
            footprint.logical_bytes = footprint.logical_bytes.saturating_add(metadata.len());
            footprint.exclusive_bytes = footprint
                .exclusive_bytes
                .saturating_add(cairn_common::cow::exclusive_bytes(&current, &metadata));
        }
    }
    footprint
}

fn remove_staged(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        // A link is removed as the link: nothing it points at is touched.
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Rename an entry out of its name, then delete it.
fn remove(root: &Path, path: &Path) -> io::Result<()> {
    let staged = root.join(format!(
        "{RECLAIMING_PREFIX}{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::rename(path, &staged)?;
    remove_staged(&staged)
}

/// Remove every ended entry, measuring each first so the log and the report
/// say what it held.
fn remove_ended(root: &Path, entries: &mut Vec<Entry>, now_unix_ms: u64) -> Vec<Removal> {
    let mut removals = Vec::new();
    entries.retain(|entry| {
        let Owner::Ended { reason, .. } = &entry.owner else {
            return true;
        };
        let footprint = measure(&entry.path);
        match remove(root, &entry.path) {
            Ok(()) => {
                log::info!(
                    "scratch: removed {} ({} logical, {} exclusive): {reason}",
                    entry.name,
                    footprint.logical_bytes,
                    footprint.exclusive_bytes
                );
                removals.push(Removal {
                    name: entry.name.clone(),
                    why: (*reason).to_owned(),
                    footprint,
                    at_unix_ms: now_unix_ms,
                });
                false
            }
            Err(error) => {
                log::warn!(
                    "scratch: could not remove {} ({reason}); retried next pass: {error}",
                    entry.name
                );
                true
            }
        }
    });
    removals
}

/// Remove every unowned entry nothing has written into for
/// [`UNOWNED_QUIET_OUTSIDE_PRESSURE`], whatever the volume's free space, unless
/// its removal would free less than [`UNOWNED_SIZE_FLOOR`], which no pass takes.
///
/// Acts only on footprints this pass measured, never on a carried-forward
/// reading: the last write that makes an entry due must be current, so the
/// rule is late by at most one census interval and never early.
fn remove_quiet_unowned(root: &Path, entries: &mut Vec<Entry>, now_unix_ms: u64) -> Vec<Removal> {
    let cutoff = now_unix_ms.saturating_sub(UNOWNED_QUIET_OUTSIDE_PRESSURE.as_millis() as u64);
    let mut removals = Vec::new();
    entries.retain(|entry| {
        let (Owner::Unowned, Some(footprint)) = (&entry.owner, entry.footprint) else {
            return true;
        };
        if footprint.newest_write_unix_ms > cutoff
            || footprint.exclusive_bytes < UNOWNED_SIZE_FLOOR
        {
            return true;
        }
        let last_write = rfc3339(footprint.newest_write_unix_ms);
        match remove(root, &entry.path) {
            Ok(()) => {
                log::info!(
                    "scratch: removed unowned {} ({} logical, {} exclusive): nothing written since {last_write}, seven days quiet",
                    entry.name,
                    footprint.logical_bytes,
                    footprint.exclusive_bytes
                );
                removals.push(Removal {
                    name: entry.name.clone(),
                    why: format!("unowned, nothing written since {last_write}, seven days quiet"),
                    footprint,
                    at_unix_ms: now_unix_ms,
                });
                false
            }
            Err(error) => {
                log::warn!(
                    "scratch: could not remove unowned {} (seven days quiet); retried next pass: {error}",
                    entry.name
                );
                true
            }
        }
    });
    removals
}

fn rfc3339(unix_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(unix_ms as i64).map_or_else(
        || format!("{unix_ms} ms"),
        |at| at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )
}

/// Drop the registry record of every ended job whose directory is gone.
fn unregister_ended(root: &Path, registrations: &[(String, String)], ended_jobs: &HashSet<String>) {
    for (job, name) in registrations {
        if !ended_jobs.contains(job) || root.join(name).exists() {
            continue;
        }
        let record = root.join(SCRATCH_MAP_DIR).join(encode_component(job));
        match std::fs::remove_file(&record) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => log::warn!("scratch: could not unregister job {job}: {error}"),
        }
    }
}

/// Take unowned entries past the quiet floor and the size floor, largest first,
/// until the volume is back above the pressure line. `None` when the volume is
/// above it, or cannot be read.
fn pressure_pass(
    root: &Path,
    entries: &mut Vec<Entry>,
    lines: Lines,
    now_unix_ms: u64,
    free: &dyn Fn() -> Option<u64>,
) -> Option<PressurePass> {
    let free_bytes_before = free()?;
    let band = lines.band(free_bytes_before)?;
    let floor = quiet_floor(band);
    let cutoff = now_unix_ms.saturating_sub(floor.as_millis() as u64);
    let asked_bytes = lines
        .pressure_free_bytes
        .saturating_sub(free_bytes_before)
        .max(1);
    let mut kept_live = Tally::default();
    let mut kept_quiet = Tally::default();
    let mut kept_small = Tally::default();
    let mut candidates = Vec::new();
    for (at, entry) in entries.iter().enumerate() {
        let Some(footprint) = entry.footprint else {
            continue;
        };
        match entry.owner {
            Owner::Live { .. } => kept_live.add(&footprint),
            Owner::Unowned if footprint.newest_write_unix_ms > cutoff => kept_quiet.add(&footprint),
            Owner::Unowned if footprint.exclusive_bytes < UNOWNED_SIZE_FLOOR => {
                kept_small.add(&footprint)
            }
            Owner::Unowned => candidates.push((at, footprint)),
            // Removed before any pass; one whose removal failed is retried
            // there, not here.
            Owner::Ended { .. } => {}
        }
    }
    candidates.sort_by(|(a_at, a), (b_at, b)| {
        b.exclusive_bytes
            .cmp(&a.exclusive_bytes)
            .then(a.newest_write_unix_ms.cmp(&b.newest_write_unix_ms))
            .then(entries[*a_at].name.cmp(&entries[*b_at].name))
    });
    let why = format!(
        "unowned, nothing written for {}h, taken under disk pressure ({band:?})",
        floor.as_secs() / 3600
    );
    let mut credited_bytes = 0_u64;
    let mut removed = Vec::new();
    let mut taken = HashSet::new();
    for (at, footprint) in candidates {
        if credited_bytes >= asked_bytes {
            break;
        }
        let entry = &entries[at];
        let ahead = free().unwrap_or(0);
        match remove(root, &entry.path) {
            Ok(()) => {
                let gained = free().unwrap_or(ahead).saturating_sub(ahead);
                credited_bytes =
                    credited_bytes.saturating_add(footprint.exclusive_bytes.max(gained));
                log::info!(
                    "scratch: removed {} under disk pressure ({band:?}): {} logical, {} exclusive, volume gained {gained}; nothing written since {}",
                    entry.name,
                    footprint.logical_bytes,
                    footprint.exclusive_bytes,
                    footprint.newest_write_unix_ms
                );
                removed.push(Removal {
                    name: entry.name.clone(),
                    why: why.clone(),
                    footprint,
                    at_unix_ms: now_unix_ms,
                });
                taken.insert(at);
            }
            Err(error) => log::warn!(
                "scratch: could not remove {} under disk pressure: {error}",
                entry.name
            ),
        }
    }
    let mut at = 0;
    entries.retain(|_| {
        let keep = !taken.contains(&at);
        at += 1;
        keep
    });
    Some(PressurePass {
        at_unix_ms: now_unix_ms,
        band,
        free_bytes_before,
        free_bytes_after: free().unwrap_or(free_bytes_before),
        asked_bytes,
        credited_bytes,
        removed,
        kept_live,
        kept_quiet,
        kept_small,
    })
}

/// One maintenance pass over the runner's own root and volume.
pub async fn maintain(orch: &Orchestrator) {
    let lines = orch.fleet.colocated_storage_lines().map_or_else(
        Lines::default,
        |(pressure_free_bytes, critical_free_bytes)| Lines {
            pressure_free_bytes,
            critical_free_bytes,
        },
    );
    let root = cairn_common::scratch::scratch_root();
    let volume = root.clone();
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    tick(orch, root, lines, now, move || {
        cairn_common::volume::space(&volume)
            .ok()
            .map(|space| space.free_bytes)
    })
    .await;
}

/// The pass itself, over `root` with `free` reading the volume, so a test can
/// stage both. Closes the portable scratch of every ended node, removes every
/// ended residence, and, when the census is stale or the volume is under the
/// pressure line, measures every entry, takes unowned ones seven days quiet,
/// and then takes further unowned ones under pressure.
pub(crate) async fn tick<F>(
    orch: &Orchestrator,
    root: PathBuf,
    lines: Lines,
    now_unix_ms: u64,
    free: F,
) where
    F: Fn() -> Option<u64> + Send + Sync + 'static,
{
    let state = orch.host_scratch.clone();
    let _pass = state.pass.lock().await;
    crate::node_scratch::lifecycle::close_ended(orch).await;

    let scan_root = root.clone();
    let scan = match tokio::task::spawn_blocking(move || scan(&scan_root)).await {
        Ok(scan) => scan,
        Err(error) => {
            log::warn!("scratch: scanning {} failed: {error}", root.display());
            return;
        }
    };
    let jobs: BTreeSet<&String> = scan
        .registrations
        .iter()
        .map(|(job, _)| job)
        .chain(
            scan.candidates
                .iter()
                .filter_map(|candidate| candidate.unregistered_job.as_ref()),
        )
        .collect();
    let mut owners = HashMap::new();
    let mut fates = HashMap::new();
    for job in jobs {
        let fate = job_fate(&orch.db, job, &mut owners).await;
        fates.insert(job.clone(), fate);
    }
    let held = held_jobs(orch);
    let mut entries: Vec<Entry> = scan
        .candidates
        .iter()
        .map(|candidate| Entry {
            name: candidate.name.clone(),
            path: candidate.path.clone(),
            owner: classify(candidate, &fates, &held),
            footprint: None,
        })
        .collect();
    let ended_jobs: HashSet<String> = fates
        .iter()
        .filter(|(job, fate)| {
            !held.contains(*job)
                && matches!(fate, JobFate::Missing | JobFate::Owner(OwnerFate::Ended(_)))
        })
        .map(|(job, _)| job.clone())
        .collect();
    let census_stale = state
        .report
        .lock()
        .unwrap()
        .census
        .as_ref()
        .is_none_or(|census| {
            now_unix_ms.saturating_sub(census.measured_at_unix_ms)
                >= CENSUS_INTERVAL.as_millis() as u64
        });

    let work = tokio::task::spawn_blocking(move || {
        for staged in &scan.staged {
            if let Err(error) = remove_staged(staged) {
                log::warn!(
                    "scratch: could not finish removing {}: {error}",
                    staged.display()
                );
            }
        }
        let removals = remove_ended(&root, &mut entries, now_unix_ms);
        unregister_ended(&root, &scan.registrations, &ended_jobs);
        let pressured = free().is_some_and(|free_bytes| lines.band(free_bytes).is_some());
        let measured = census_stale || pressured;
        let mut pressure = None;
        let mut quiet = Vec::new();
        if measured {
            for entry in &mut entries {
                entry.footprint = Some(measure(&entry.path));
            }
            // Before pressure, so its accounting sees only what is left.
            quiet = remove_quiet_unowned(&root, &mut entries, now_unix_ms);
            if pressured {
                pressure = pressure_pass(&root, &mut entries, lines, now_unix_ms, &free);
            }
        }
        (entries, removals, quiet, measured, pressure)
    })
    .await;
    let (entries, removals, quiet, measured, pressure) = match work {
        Ok(done) => done,
        Err(error) => {
            log::warn!("scratch: maintenance pass failed: {error}");
            return;
        }
    };

    let mut report = state.report.lock().unwrap();
    for removal in removals {
        report.removals.push_back(removal);
    }
    while report.removals.len() > REMOVALS_KEPT {
        report.removals.pop_front();
    }
    for removal in quiet {
        report.quiet_reclaimed.add(&removal.footprint);
        report.quiet_removals.push_back(removal);
    }
    while report.quiet_removals.len() > REMOVALS_KEPT {
        report.quiet_removals.pop_front();
    }
    if measured {
        report.census = Some(Census {
            measured_at_unix_ms: now_unix_ms,
            entries,
        });
    } else if let Some(census) = report.census.as_mut() {
        // Ownership moves between measurements; sizes keep their last reading.
        let footprints: HashMap<String, Option<Footprint>> = census
            .entries
            .drain(..)
            .map(|entry| (entry.name, entry.footprint))
            .collect();
        census.entries = entries
            .into_iter()
            .map(|mut entry| {
                entry.footprint = footprints.get(&entry.name).copied().flatten();
                entry
            })
            .collect();
    }
    if pressure.is_some() {
        report.pressure = pressure;
    }
}

/// Remove the residences of a closed thread's jobs, as its node ends. One a
/// live process still holds is left for the maintenance pass, which removes it
/// once nothing does.
pub async fn end_thread(orch: &Orchestrator, db: &LocalDb, thread_id: &str) {
    let jobs = match db
        .query_all(
            "SELECT id FROM jobs WHERE thread_id = ?1 AND issue_id IS NULL",
            (thread_id.to_owned(),),
            |row| row.text(0),
        )
        .await
    {
        Ok(jobs) => jobs,
        Err(error) => {
            log::warn!("scratch: list the jobs of closed thread {thread_id}: {error}");
            return;
        }
    };
    let held = held_jobs(orch);
    let (held, free): (Vec<String>, Vec<String>) =
        jobs.into_iter().partition(|job| held.contains(job));
    if !held.is_empty() {
        log::info!(
            "scratch: thread {thread_id} closed with {} job(s) still running; their residences go once they stop",
            held.len()
        );
    }
    let _ = tokio::task::spawn_blocking(move || {
        for job in free {
            super::remove_job_scratch_dir(&job);
        }
    })
    .await;
}

#[cfg(test)]
mod tests;
