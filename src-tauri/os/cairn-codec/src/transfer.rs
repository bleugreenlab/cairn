//! Verified Git pack transfer and object-database installation.
//!
//! This module is the gix-free boundary used by the execution fabric. Public
//! values are bytes, paths, counts, and lowercase SHA-1 strings only; Git's
//! internal object and pack types never cross this boundary.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::packfile::{pack_scratch_dir, ExecutionPack};

const PACK_FRAME_MAGIC: &[u8; 8] = b"CAIRNPK1";
const PACK_FRAME_VERSION: u16 = 1;

/// One stable object-store envelope for pack and generated index bytes.
pub fn frame_pack(pack: &[u8], index: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(26 + pack.len() + index.len());
    framed.extend_from_slice(PACK_FRAME_MAGIC);
    framed.extend_from_slice(&PACK_FRAME_VERSION.to_be_bytes());
    framed.extend_from_slice(&(pack.len() as u64).to_be_bytes());
    framed.extend_from_slice(&(index.len() as u64).to_be_bytes());
    framed.extend_from_slice(pack);
    framed.extend_from_slice(index);
    framed
}

/// Decode the canonical frame. The legacy two-length envelope remains readable
/// so existing execution_history.pack_hash pointers can be cataloged lazily.
pub fn unframe_pack(bytes: &[u8]) -> Result<ExecutionPack, String> {
    if bytes.starts_with(PACK_FRAME_MAGIC) {
        if bytes.len() < 26 {
            return Err("truncated pack frame header".into());
        }
        let version = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
        if version != PACK_FRAME_VERSION {
            return Err(format!("unsupported pack frame version {version}"));
        }
        let pack_len = usize::try_from(u64::from_be_bytes(bytes[10..18].try_into().unwrap()))
            .map_err(|_| "pack length exceeds platform size")?;
        let index_len = usize::try_from(u64::from_be_bytes(bytes[18..26].try_into().unwrap()))
            .map_err(|_| "index length exceeds platform size")?;
        let pack_end = 26usize
            .checked_add(pack_len)
            .ok_or("pack frame length overflow")?;
        let end = pack_end
            .checked_add(index_len)
            .ok_or("pack frame length overflow")?;
        if end != bytes.len() {
            return Err("pack frame length mismatch".into());
        }
        return Ok((bytes[26..pack_end].to_vec(), bytes[pack_end..end].to_vec()));
    }

    unframe_legacy_pack(bytes)
}

fn unframe_legacy_pack(bytes: &[u8]) -> Result<ExecutionPack, String> {
    let take = |offset: usize| -> Result<usize, String> {
        let raw: [u8; 8] = bytes
            .get(offset..offset + 8)
            .ok_or("truncated legacy pack frame")?
            .try_into()
            .unwrap();
        usize::try_from(u64::from_le_bytes(raw))
            .map_err(|_| "legacy length exceeds platform size".into())
    };
    let pack_len = take(0)?;
    let pack_end = 8usize
        .checked_add(pack_len)
        .ok_or("legacy pack length overflow")?;
    let index_len = take(pack_end)?;
    let index_start = pack_end + 8;
    let end = index_start
        .checked_add(index_len)
        .ok_or("legacy index length overflow")?;
    if end != bytes.len() {
        return Err("legacy pack frame length mismatch".into());
    }
    Ok((
        bytes[8..pack_end].to_vec(),
        bytes[index_start..end].to_vec(),
    ))
}

const SHA1_HEX_LEN: usize = 40;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackLimits {
    max_pack_bytes: u64,
    max_object_count: u64,
    max_inflated_bytes: u64,
}

impl Default for PackLimits {
    fn default() -> Self {
        Self {
            max_pack_bytes: 512 * 1024 * 1024,
            max_object_count: 1_000_000,
            max_inflated_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectSize {
    pub oid: String,
    kind: String,
    /// Canonical, decompressed and undeltified Git object bytes.
    canonical_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackManifest {
    pub pack_checksum: String,
    pub pack_bytes: u64,
    pub index_bytes: u64,
    pub object_count: u64,
    canonical_bytes: u64,
    pub objects: Vec<ObjectSize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedPack {
    pub pack: Vec<u8>,
    pub index: Vec<u8>,
    pub manifest: PackManifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledPack {
    pub pack_path: PathBuf,
    index_path: PathBuf,
    pub manifest: PackManifest,
    already_present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosureManifest {
    commit: String,
    object_count: u64,
    canonical_bytes: u64,
    pub objects: Vec<ObjectSize>,
}

/// Build a complete, non-thin pack containing objects reachable from `wants`
/// but not reachable from `haves`. Returns `None` when the accepted haves fully
/// cover every want.
pub fn build_reachable_pack(
    repository: &Path,
    wants: &[String],
    haves: &[String],
) -> Result<Option<ExecutionPack>, String> {
    if wants.is_empty() {
        return Err("at least one wanted commit is required".into());
    }
    validate_oids(wants.iter().chain(haves))?;

    let mut args = vec!["rev-list", "--objects"];
    for want in wants {
        args.push(want);
    }
    if !haves.is_empty() {
        args.push("--not");
        for have in haves {
            args.push(have);
        }
    }
    let listed = git(repository, &args, None, &[])?;
    let mut object_ids = String::new();
    for line in listed.lines() {
        if let Some(oid) = line.split_whitespace().next() {
            object_ids.push_str(oid);
            object_ids.push('\n');
        }
    }
    if object_ids.is_empty() {
        return Ok(None);
    }
    build_pack_from_oids(repository, &object_ids).map(Some)
}

/// Build the complete AllowDelta object range `base_commit..delta_commit`.
pub fn build_delta_pack(
    repository: &Path,
    delta_commit: &str,
    base_commit: &str,
) -> Result<Option<ExecutionPack>, String> {
    build_reachable_pack(
        repository,
        &[delta_commit.to_owned()],
        &[base_commit.to_owned()],
    )
}

/// Validate a received non-thin pack, derive its v2 index and canonical object
/// manifest, and enforce transfer limits. Indexing occurs against an empty ODB,
/// so a thin pack with an external delta base is rejected.
pub fn validate_pack(pack: &[u8], limits: PackLimits) -> Result<ValidatedPack, String> {
    if pack.len() as u64 > limits.max_pack_bytes {
        return Err(format!(
            "pack exceeds byte limit: {} > {}",
            pack.len(),
            limits.max_pack_bytes
        ));
    }
    let scratch = tempfile::tempdir().map_err(|e| format!("creating pack quarantine: {e}"))?;
    let git_dir = scratch.path().join("repository.git");
    let init = Command::new("git")
        .args(["init", "--bare", "--quiet"])
        .arg(&git_dir)
        .output()
        .map_err(|e| format!("initializing quarantine repository: {e}"))?;
    if !init.status.success() {
        return Err(format!(
            "initializing quarantine repository: {}",
            String::from_utf8_lossy(&init.stderr).trim()
        ));
    }
    let objects = git_dir.join("objects");
    let pack_path = scratch.path().join("incoming.pack");
    let index_path = scratch.path().join("incoming.idx");
    fs::write(&pack_path, pack).map_err(|e| format!("writing quarantined pack: {e}"))?;

    let input = File::open(&pack_path).map_err(|e| format!("opening quarantined pack: {e}"))?;
    let output = Command::new("git")
        // Indexing against an empty object directory rejects unresolved delta
        // bases. Do not use `--strict`: a legitimate delta-range pack may carry
        // a commit whose parent is intentionally supplied by an alternate ODB.
        .args(["index-pack", "--index-version=2", "-o"])
        .arg(&index_path)
        .arg("--stdin")
        .env("GIT_DIR", &git_dir)
        .env("GIT_OBJECT_DIRECTORY", &objects)
        .stdin(Stdio::from(input))
        .output()
        .map_err(|e| format!("spawning git index-pack: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "invalid or thin pack: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let index = fs::read(&index_path).map_err(|e| format!("reading derived pack index: {e}"))?;
    let objects_manifest = verify_pack_manifest(&index_path)?;
    let object_count = objects_manifest.len() as u64;
    let canonical_bytes = objects_manifest.iter().try_fold(0u64, |sum, object| {
        sum.checked_add(object.canonical_bytes)
            .ok_or_else(|| "canonical object size overflow".to_string())
    })?;
    if object_count > limits.max_object_count {
        return Err(format!(
            "pack exceeds object limit: {object_count} > {}",
            limits.max_object_count
        ));
    }
    if canonical_bytes > limits.max_inflated_bytes {
        return Err(format!(
            "pack exceeds inflated byte limit: {canonical_bytes} > {}",
            limits.max_inflated_bytes
        ));
    }
    // With `--stdin` Git reports `pack\t<checksum>` rather than the bare
    // checksum produced by file-based indexing.
    let checksum_output = String::from_utf8_lossy(&output.stdout);
    let checksum = checksum_output
        .split_whitespace()
        .next_back()
        .ok_or_else(|| "git index-pack did not report a pack checksum".to_string())?
        .to_owned();
    validate_oid(&checksum)?;
    Ok(ValidatedPack {
        pack: pack.to_vec(),
        index,
        manifest: PackManifest {
            pack_checksum: checksum,
            pack_bytes: pack.len() as u64,
            index_bytes: fs::metadata(&index_path).map_err(|e| e.to_string())?.len(),
            object_count,
            canonical_bytes,
            objects: objects_manifest,
        },
    })
}

/// Atomically install an already validated pack into `objects_dir/pack`.
/// Quarantine files are created on the same filesystem, synced, and renamed;
/// reinstallation of the same checksum is idempotent.
pub fn install_pack(
    objects_dir: &Path,
    validated: &ValidatedPack,
) -> Result<InstalledPack, String> {
    let pack_dir = objects_dir.join("pack");
    fs::create_dir_all(&pack_dir).map_err(|e| format!("creating pack directory: {e}"))?;
    let stem = format!("pack-{}", validated.manifest.pack_checksum);
    let pack_path = pack_dir.join(format!("{stem}.pack"));
    let index_path = pack_dir.join(format!("{stem}.idx"));
    if pack_path.is_file() && index_path.is_file() {
        return Ok(InstalledPack {
            pack_path,
            index_path,
            manifest: validated.manifest.clone(),
            already_present: true,
        });
    }
    if pack_path.exists() || index_path.exists() {
        return Err(format!("partial existing pack installation for {stem}"));
    }

    let quarantine = tempfile::Builder::new()
        .prefix(".cairn-pack-")
        .tempdir_in(&pack_dir)
        .map_err(|e| format!("creating same-filesystem pack quarantine: {e}"))?;
    let staged_pack = quarantine.path().join(format!("{stem}.pack"));
    let staged_index = quarantine.path().join(format!("{stem}.idx"));
    write_synced(&staged_pack, &validated.pack)?;
    write_synced(&staged_index, &validated.index)?;
    fs::rename(&staged_pack, &pack_path).map_err(|e| format!("publishing pack: {e}"))?;
    if let Err(error) = fs::rename(&staged_index, &index_path) {
        let _ = fs::remove_file(&pack_path);
        return Err(format!("publishing pack index: {error}"));
    }
    // Windows does not expose directory handles through File::open. The pack
    // and index contents are already durable through write_synced; directory
    // fsync is the additional rename-durability seam on platforms that support it.
    #[cfg(not(windows))]
    File::open(&pack_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| format!("syncing pack directory: {e}"))?;
    Ok(InstalledPack {
        pack_path,
        index_path,
        manifest: validated.manifest.clone(),
        already_present: false,
    })
}

/// Verify and enumerate the complete commit/tree/blob closure through a primary
/// ODB and zero or more alternate ODB directories.
///
/// `shallow` is the history boundary the closure stops at: commits whose
/// parents the objects deliberately do not hold, as a shallow clone's `shallow`
/// file records them. The walk runs in a throwaway repository that cannot see
/// any repository's own `shallow` file, so the boundary must be passed in;
/// without it a shallow source's never-fetched parent reads as a missing
/// object. Anything missing on the near side of the boundary is still refused.
pub fn verify_commit_closure(
    primary_objects_dir: &Path,
    alternate_objects_dirs: &[PathBuf],
    shallow: &[String],
    commit: &str,
) -> Result<ClosureManifest, String> {
    validate_oid(commit)?;
    validate_oids(shallow)?;
    let plumbing_repo = plumbing_repository()?;
    if !shallow.is_empty() {
        write_shallow_list(&plumbing_repo.path().join("shallow"), shallow)?;
    }
    let env = odb_env(
        plumbing_repo.path(),
        primary_objects_dir,
        alternate_objects_dirs,
    );
    let kind = git(Path::new("."), &["cat-file", "-t", commit], None, &env)?;
    if kind.trim() != "commit" {
        return Err(format!("object {commit} is not a commit"));
    }
    let listed = git(
        Path::new("."),
        &["rev-list", "--objects", "--missing=print", commit],
        None,
        &env,
    )?;
    let mut oids = Vec::new();
    for line in listed.lines() {
        let oid = line.split_whitespace().next().unwrap_or_default();
        if let Some(missing) = oid.strip_prefix('?') {
            return Err(format!("commit closure is missing object {missing}"));
        }
        validate_oid(oid)?;
        oids.push(oid.to_owned());
    }
    let objects = canonical_object_sizes(primary_objects_dir, alternate_objects_dirs, &oids)?;
    let canonical_bytes = objects.iter().map(|object| object.canonical_bytes).sum();
    Ok(ClosureManifest {
        commit: commit.to_owned(),
        object_count: objects.len() as u64,
        canonical_bytes,
        objects,
    })
}

/// The most boundary commits one transfer may declare. A `--depth` clone has
/// one per branch tip it was cut at; this bound only refuses a degenerate list
/// before it becomes an oversized header.
pub const MAX_SHALLOW_BOUNDARY_COMMITS: usize = 512;

/// The commits `repository` records in its `shallow` file, sorted. A complete
/// repository has none. Works for bare and non-bare repositories alike.
pub fn shallow_commits(repository: &Path) -> Result<Vec<String>, String> {
    let listed = git(
        repository,
        &["rev-parse", "--git-path", "shallow"],
        None,
        &[],
    )?;
    let path = PathBuf::from(listed.trim());
    let path = if path.is_absolute() {
        path
    } else {
        repository.join(path)
    };
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("reading {}: {error}", path.display())),
    };
    let commits: BTreeSet<String> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    let commits: Vec<String> = commits.into_iter().collect();
    validate_oids(&commits)?;
    Ok(commits)
}

/// The part of `repository`'s shallow boundary that the history of `commit`
/// reaches: what a receiver of `commit`'s closure must also treat as shallow.
/// Empty, without walking anything, for a complete repository.
pub fn shallow_boundary(repository: &Path, commit: &str) -> Result<Vec<String>, String> {
    validate_oid(commit)?;
    let shallow = shallow_commits(repository)?;
    if shallow.is_empty() {
        return Ok(shallow);
    }
    // Run inside the repository, so Git honors the same boundary it reports.
    let history = git(repository, &["rev-list", commit], None, &[])?;
    let reached: BTreeSet<&str> = history.lines().map(str::trim).collect();
    Ok(shallow
        .into_iter()
        .filter(|boundary| reached.contains(boundary.as_str()))
        .collect())
}

/// Add `boundary` to `repository`'s `shallow` file, so every later walk of the
/// repository stops where its objects deliberately stop. Entries already
/// recorded are kept. Returns whether the file changed.
pub fn extend_shallow_boundary(repository: &Path, boundary: &[String]) -> Result<bool, String> {
    validate_oids(boundary)?;
    if boundary.is_empty() {
        return Ok(false);
    }
    let existing = shallow_commits(repository)?;
    let mut merged: BTreeSet<String> = existing.iter().cloned().collect();
    merged.extend(boundary.iter().cloned());
    if merged.len() == existing.len() {
        return Ok(false);
    }
    let listed = git(
        repository,
        &["rev-parse", "--git-path", "shallow"],
        None,
        &[],
    )?;
    let path = PathBuf::from(listed.trim());
    let path = if path.is_absolute() {
        path
    } else {
        repository.join(path)
    };
    let merged: Vec<String> = merged.into_iter().collect();
    write_shallow_list(&path, &merged)?;
    Ok(true)
}

/// Encode a shallow boundary as one header value: space-separated commit IDs.
pub fn encode_shallow_boundary(boundary: &[String]) -> Result<String, String> {
    validate_oids(boundary)?;
    if boundary.len() > MAX_SHALLOW_BOUNDARY_COMMITS {
        return Err(format!(
            "shallow boundary of {} commits exceeds the transfer limit of {MAX_SHALLOW_BOUNDARY_COMMITS}",
            boundary.len()
        ));
    }
    Ok(boundary.join(" "))
}

/// Parse a header value [`encode_shallow_boundary`] produced.
pub fn parse_shallow_boundary(value: &str) -> Result<Vec<String>, String> {
    let boundary: Vec<String> = value.split_whitespace().map(str::to_owned).collect();
    if boundary.len() > MAX_SHALLOW_BOUNDARY_COMMITS {
        return Err(format!(
            "shallow boundary of {} commits exceeds the transfer limit of {MAX_SHALLOW_BOUNDARY_COMMITS}",
            boundary.len()
        ));
    }
    validate_oids(&boundary)?;
    Ok(boundary)
}

/// Atomically replace a `shallow` file with `commits`, one per line.
fn write_shallow_list(path: &Path, commits: &[String]) -> Result<(), String> {
    let directory = path
        .parent()
        .ok_or_else(|| format!("shallow file {} has no parent", path.display()))?;
    let mut staged = tempfile::Builder::new()
        .prefix(".cairn-shallow-")
        .tempfile_in(directory)
        .map_err(|e| format!("staging shallow file: {e}"))?;
    for commit in commits {
        writeln!(staged, "{commit}").map_err(|e| format!("writing shallow file: {e}"))?;
    }
    staged
        .as_file()
        .sync_all()
        .map_err(|e| format!("syncing shallow file: {e}"))?;
    staged
        .persist(path)
        .map_err(|e| format!("publishing shallow file {}: {}", path.display(), e.error))?;
    Ok(())
}

/// Return canonical sizes for object IDs resolved through primary plus alternates.
fn canonical_object_sizes(
    primary_objects_dir: &Path,
    alternate_objects_dirs: &[PathBuf],
    object_ids: &[String],
) -> Result<Vec<ObjectSize>, String> {
    validate_oids(object_ids)?;
    let mut input = object_ids.join("\n");
    if !input.is_empty() {
        input.push('\n');
    }
    let plumbing_repo = plumbing_repository()?;
    let output = git(
        Path::new("."),
        &[
            "cat-file",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ],
        Some(input.as_bytes()),
        &odb_env(
            plumbing_repo.path(),
            primary_objects_dir,
            alternate_objects_dirs,
        ),
    )?;
    let mut result = Vec::with_capacity(object_ids.len());
    for line in output.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 3 || fields[1] == "missing" {
            return Err(format!("unable to resolve canonical object size: {line}"));
        }
        validate_oid(fields[0])?;
        let canonical_bytes = fields[2]
            .parse::<u64>()
            .map_err(|e| format!("invalid canonical object size in {line:?}: {e}"))?;
        result.push(ObjectSize {
            oid: fields[0].to_owned(),
            kind: fields[1].to_owned(),
            canonical_bytes,
        });
    }
    if result.len() != object_ids.len() {
        return Err("git cat-file returned an incomplete size manifest".into());
    }
    Ok(result)
}

fn build_pack_from_oids(repository: &Path, object_ids: &str) -> Result<ExecutionPack, String> {
    let scratch = pack_scratch_dir(repository)?;
    let base = scratch.path().join("range");
    let mut child = Command::new("git")
        .current_dir(repository)
        // Disabling the delta search guarantees every base is carried in the
        // transfer. This is intentionally a non-thin transport pack; archival's
        // historical builder retains its existing compression behavior.
        .args(["pack-objects", "--quiet", "--window=0"])
        .arg(&base)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning git pack-objects: {e}"))?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(object_ids.as_bytes())
        .map_err(|e| e.to_string())?;
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "git pack-objects failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let checksum = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    validate_oid(&checksum)?;
    let pack = fs::read(scratch.path().join(format!("range-{checksum}.pack")))
        .map_err(|e| e.to_string())?;
    let index = fs::read(scratch.path().join(format!("range-{checksum}.idx")))
        .map_err(|e| e.to_string())?;
    Ok((pack, index))
}

fn verify_pack_manifest(index_path: &Path) -> Result<Vec<ObjectSize>, String> {
    let output = Command::new("git")
        .args(["verify-pack", "-v"])
        .arg(index_path)
        .output()
        .map_err(|e| format!("spawning git verify-pack: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "verifying pack: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 5 || validate_oid(fields[0]).is_err() {
            continue;
        }
        let canonical_bytes = fields[2]
            .parse::<u64>()
            .map_err(|e| format!("invalid verify-pack size: {e}"))?;
        if !seen.insert(fields[0].to_owned()) {
            return Err(format!("duplicate object {} in pack manifest", fields[0]));
        }
        objects.push(ObjectSize {
            oid: fields[0].to_owned(),
            kind: fields[1].to_owned(),
            canonical_bytes,
        });
    }
    Ok(objects)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = File::create(path).map_err(|e| format!("creating {}: {e}", path.display()))?;
    file.write_all(bytes)
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
    file.sync_all()
        .map_err(|e| format!("syncing {}: {e}", path.display()))
}

fn plumbing_repository() -> Result<tempfile::TempDir, String> {
    let repo = tempfile::tempdir().map_err(|e| format!("creating plumbing repository: {e}"))?;
    let output = Command::new("git")
        .args(["init", "--bare", "--quiet"])
        .arg(repo.path())
        .output()
        .map_err(|e| format!("initializing plumbing repository: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "initializing plumbing repository: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(repo)
}

fn odb_env(git_dir: &Path, primary: &Path, alternates: &[PathBuf]) -> Vec<(OsString, OsString)> {
    let mut env = vec![
        (OsString::from("GIT_DIR"), git_dir.as_os_str().to_owned()),
        (
            OsString::from("GIT_OBJECT_DIRECTORY"),
            primary.as_os_str().to_owned(),
        ),
    ];
    if !alternates.is_empty() {
        let joined =
            std::env::join_paths(alternates).expect("alternate ODB paths must be joinable");
        env.push((OsString::from("GIT_ALTERNATE_OBJECT_DIRECTORIES"), joined));
    }
    env
}

fn git(
    cwd: &Path,
    args: &[&str],
    stdin: Option<&[u8]>,
    env: &[(OsString, OsString)],
) -> Result<String, String> {
    let mut child = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .envs(env.iter().cloned())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning git {}: {e}", args.join(" ")))?;
    // Batch plumbing commands write one response per input line. Writing all
    // input before draining piped output deadlocks once both OS pipe buffers
    // fill, which is routine for a clone-sized object closure.
    let writer = stdin.map(|input| {
        let input = input.to_vec();
        let mut pipe = child.stdin.take().unwrap();
        std::thread::spawn(move || pipe.write_all(&input))
    });
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    if let Some(writer) = writer {
        writer
            .join()
            .map_err(|_| format!("git {} input writer panicked", args.join(" ")))?
            .map_err(|e| format!("writing git {} input: {e}", args.join(" ")))?;
    }
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn validate_oids<'a>(oids: impl IntoIterator<Item = &'a String>) -> Result<(), String> {
    for oid in oids {
        validate_oid(oid)?;
    }
    Ok(())
}

fn validate_oid(oid: &str) -> Result<(), String> {
    if oid.len() != SHA1_HEX_LEN
        || !oid
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid lowercase SHA-1 object ID {oid:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_all, git, init_repo, write_file};

    fn objects_dir(repo: &Path) -> PathBuf {
        repo.join(".git/objects")
    }

    #[test]
    fn pack_frame_is_deterministic_versioned_and_strict() {
        let first = frame_pack(b"pack", b"index");
        assert_eq!(first, frame_pack(b"pack", b"index"));
        assert_eq!(
            unframe_pack(&first).unwrap(),
            (b"pack".to_vec(), b"index".to_vec())
        );
        let mut wrong_version = first.clone();
        wrong_version[9] = 2;
        assert!(unframe_pack(&wrong_version).is_err());
        assert!(unframe_pack(&first[..first.len() - 1]).is_err());
        let mut trailing = first;
        trailing.push(0);
        assert!(unframe_pack(&trailing).is_err());
    }

    #[test]
    fn legacy_pack_frame_remains_readable_for_catalog_backfill() {
        let mut legacy = (4u64).to_le_bytes().to_vec();
        legacy.extend_from_slice(b"pack");
        legacy.extend_from_slice(&(3u64).to_le_bytes());
        legacy.extend_from_slice(b"idx");
        assert_eq!(
            unframe_pack(&legacy).unwrap(),
            (b"pack".to_vec(), b"idx".to_vec())
        );
    }

    #[test]
    fn canonical_size_exchange_drains_output_while_feeding_clone_sized_input() {
        let source = tempfile::tempdir().unwrap();
        init_repo(source.path());
        write_file(source.path(), "base.txt", b"base");
        let oid = commit_all(source.path(), "base");
        let count = 100_000;
        let object_ids = vec![oid.clone(); count];

        let sizes = canonical_object_sizes(&objects_dir(source.path()), &[], &object_ids).unwrap();

        assert_eq!(sizes.len(), count);
        assert!(sizes.iter().all(|size| size.oid == oid));
    }

    #[test]
    fn cold_closure_pack_validates_and_installs_idempotently() {
        let source = tempfile::tempdir().unwrap();
        init_repo(source.path());
        write_file(source.path(), "base.txt", b"base");
        let base = commit_all(source.path(), "base");
        write_file(source.path(), "nested/new.txt", b"new payload");
        let tip = commit_all(source.path(), "tip");

        let (pack, _) = build_reachable_pack(source.path(), std::slice::from_ref(&tip), &[])
            .unwrap()
            .unwrap();
        let validated = validate_pack(&pack, PackLimits::default()).unwrap();
        assert!(validated.manifest.object_count >= 6);
        assert!(validated.manifest.canonical_bytes > 0);

        let target = tempfile::tempdir().unwrap();
        let target_objects = target.path().join("objects");
        let first = install_pack(&target_objects, &validated).unwrap();
        assert!(!first.already_present);
        let second = install_pack(&target_objects, &validated).unwrap();
        assert!(second.already_present);
        let closure = verify_commit_closure(&target_objects, &[], &[], &tip).unwrap();
        assert_eq!(closure.commit, tip);
        assert!(closure.objects.iter().any(|object| object.kind == "blob"));

        let delta = build_delta_pack(source.path(), &closure.commit, &base)
            .unwrap()
            .unwrap();
        assert!(!delta.0.is_empty());
    }

    #[test]
    fn corrupt_truncated_and_limited_packs_are_rejected_without_installation() {
        let source = tempfile::tempdir().unwrap();
        init_repo(source.path());
        write_file(source.path(), "a", b"content");
        let tip = commit_all(source.path(), "tip");
        let (pack, _) = build_reachable_pack(source.path(), &[tip], &[])
            .unwrap()
            .unwrap();

        let mut corrupt = pack.clone();
        corrupt[20] ^= 0xff;
        assert!(validate_pack(&corrupt, PackLimits::default()).is_err());
        assert!(validate_pack(&pack[..pack.len() - 8], PackLimits::default()).is_err());
        assert!(validate_pack(
            &pack,
            PackLimits {
                max_pack_bytes: 1,
                ..PackLimits::default()
            }
        )
        .is_err());
        assert!(validate_pack(
            &pack,
            PackLimits {
                max_object_count: 1,
                ..PackLimits::default()
            }
        )
        .is_err());
    }

    #[test]
    fn closure_can_resolve_through_primary_and_alternate_storage() {
        let source = tempfile::tempdir().unwrap();
        init_repo(source.path());
        write_file(source.path(), "base", b"base object");
        let base = commit_all(source.path(), "base");
        write_file(source.path(), "tip", b"tip object");
        let tip = commit_all(source.path(), "tip");

        let target = tempfile::tempdir().unwrap();
        git(target.path(), &["init", "-q", "--bare"]);
        let delta = build_delta_pack(source.path(), &tip, &base)
            .unwrap()
            .unwrap();
        let validated = validate_pack(&delta.0, PackLimits::default()).unwrap();
        install_pack(&objects_dir(target.path()), &validated).unwrap();
        let alternate = objects_dir(source.path());
        let closure =
            verify_commit_closure(&objects_dir(target.path()), &[alternate], &[], &tip).unwrap();
        assert!(closure.object_count > validated.manifest.object_count);

        let empty = tempfile::tempdir().unwrap();
        fs::create_dir_all(empty.path().join("objects/pack")).unwrap();
        assert!(verify_commit_closure(&empty.path().join("objects"), &[], &[], &tip).is_err());
    }

    /// A three-commit upstream and a `--depth 1` clone of it: the clone holds
    /// the tip and records it as shallow, and never fetched its parent.
    fn shallow_clone() -> (tempfile::TempDir, PathBuf, String, String) {
        let temp = tempfile::tempdir().unwrap();
        let upstream = temp.path().join("upstream");
        fs::create_dir_all(&upstream).unwrap();
        init_repo(&upstream);
        write_file(&upstream, "one", b"one");
        commit_all(&upstream, "one");
        write_file(&upstream, "two", b"two");
        let parent = commit_all(&upstream, "two");
        write_file(&upstream, "three", b"three");
        let tip = commit_all(&upstream, "three");
        let clone = temp.path().join("clone");
        let url = format!("file://{}", upstream.display());
        git(
            temp.path(),
            &["clone", "-q", "--depth", "1", &url, clone.to_str().unwrap()],
        );
        (temp, clone, tip, parent)
    }

    #[test]
    fn closure_stops_at_a_shallow_source_boundary_and_not_before_it() {
        let (_temp, clone, tip, parent) = shallow_clone();
        let shallow = shallow_commits(&clone).unwrap();
        assert_eq!(shallow, vec![tip.clone()]);

        // Blind to the boundary, the walk crosses it into the parent the clone
        // never fetched: the failure every resident batch of a shallow-cloned
        // project hit.
        let blind = verify_commit_closure(&objects_dir(&clone), &[], &[], &tip).unwrap_err();
        assert_eq!(blind, format!("commit closure is missing object {parent}"));

        let closure = verify_commit_closure(&objects_dir(&clone), &[], &shallow, &tip).unwrap();
        assert!(closure.objects.iter().any(|object| object.oid == tip));
        assert!(closure.objects.iter().all(|object| object.oid != parent));

        // The boundary excuses history, not content: a blob the tip's own tree
        // names is still required.
        let blob = git(&clone, &["rev-parse", "HEAD:three"]).trim().to_owned();
        let packed = tempfile::tempdir().unwrap();
        let (pack, _) = build_reachable_pack(&clone, std::slice::from_ref(&tip), &[])
            .unwrap()
            .unwrap();
        let validated = validate_pack(&pack, PackLimits::default()).unwrap();
        let without_blob = packed.path().join("objects");
        let blob_free: Vec<_> = validated
            .manifest
            .objects
            .iter()
            .filter(|object| object.oid != blob)
            .map(|object| object.oid.clone())
            .collect();
        let (partial, _) =
            build_pack_from_oids(&clone, &format!("{}\n", blob_free.join("\n"))).unwrap();
        install_pack(
            &without_blob,
            &validate_pack(&partial, PackLimits::default()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            verify_commit_closure(&without_blob, &[], &shallow, &tip).unwrap_err(),
            format!("commit closure is missing object {blob}")
        );
    }

    #[test]
    fn a_complete_source_missing_an_object_is_still_refused() {
        let source = tempfile::tempdir().unwrap();
        init_repo(source.path());
        write_file(source.path(), "base", b"base");
        let base = commit_all(source.path(), "base");
        write_file(source.path(), "tip", b"tip");
        let tip = commit_all(source.path(), "tip");
        assert!(shallow_commits(source.path()).unwrap().is_empty());
        assert!(shallow_boundary(source.path(), &tip).unwrap().is_empty());

        // Lose the parent from a repository that never declared a boundary.
        let loose = objects_dir(source.path()).join(&base[..2]).join(&base[2..]);
        fs::remove_file(loose).unwrap();
        let shallow = shallow_commits(source.path()).unwrap();
        assert_eq!(
            verify_commit_closure(&objects_dir(source.path()), &[], &shallow, &tip).unwrap_err(),
            format!("commit closure is missing object {base}")
        );
    }

    #[test]
    fn the_boundary_a_commit_reaches_is_carried_and_recorded_by_a_receiver() {
        let (_temp, clone, tip, _parent) = shallow_clone();
        write_file(&clone, "four", b"four");
        let child = commit_all(&clone, "four");
        assert_eq!(shallow_boundary(&clone, &child).unwrap(), vec![tip.clone()]);

        // A receiver: an empty bare repository that is sent the child's
        // closure, which stops at the clone's boundary.
        let receiver = tempfile::tempdir().unwrap();
        git(receiver.path(), &["init", "-q", "--bare"]);
        let (pack, _) = build_reachable_pack(&clone, std::slice::from_ref(&child), &[])
            .unwrap()
            .unwrap();
        install_pack(
            &receiver.path().join("objects"),
            &validate_pack(&pack, PackLimits::default()).unwrap(),
        )
        .unwrap();
        let boundary = parse_shallow_boundary(
            &encode_shallow_boundary(&shallow_boundary(&clone, &child).unwrap()).unwrap(),
        )
        .unwrap();
        assert!(extend_shallow_boundary(receiver.path(), &boundary).unwrap());
        assert!(!extend_shallow_boundary(receiver.path(), &boundary).unwrap());
        assert_eq!(shallow_commits(receiver.path()).unwrap(), vec![tip]);

        // Git itself now agrees in the receiver: the walk the repository's own
        // tools make reports nothing missing.
        let walked = git(
            receiver.path(),
            &["rev-list", "--objects", "--missing=print", &child],
        );
        assert!(
            !walked.lines().any(|line| line.starts_with('?')),
            "{walked}"
        );
    }

    #[test]
    fn shallow_boundary_encoding_rejects_malformed_and_oversized_lists() {
        assert_eq!(parse_shallow_boundary("").unwrap(), Vec::<String>::new());
        assert!(parse_shallow_boundary("not-an-oid").is_err());
        assert!(parse_shallow_boundary(&"A".repeat(40)).is_err());
        let oversized = vec!["a".repeat(40); MAX_SHALLOW_BOUNDARY_COMMITS + 1];
        assert!(encode_shallow_boundary(&oversized).is_err());
        assert!(parse_shallow_boundary(&oversized.join(" ")).is_err());
    }
}
