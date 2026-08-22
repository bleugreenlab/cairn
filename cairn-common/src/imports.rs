//! Canonical paths and limits for native files staged by the desktop.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use uuid::Uuid;

pub const IMPORTS_ROOT_DIR: &str = "imports";
pub const MAX_IMPORT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// The six-hour transcription ceiling plus two hours of cleanup grace.
pub const IMPORT_TTL: Duration = Duration::from_secs(8 * 60 * 60);
pub const TRANSCRIPTION_LEASE_MAX: Duration = Duration::from_secs(6 * 60 * 60);
pub const FALLBACK_GRACE: Duration = Duration::from_secs(15 * 60);

pub fn imports_root() -> PathBuf {
    crate::paths::cairn_home().join(IMPORTS_ROOT_DIR)
}

pub fn is_reserved_staged_name(name: &str) -> bool {
    matches!(name, "meta.json" | "lease.json")
        || name.starts_with(".lease-tmp-")
        || name.starts_with(".transcription-")
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ImportHandle {
    session: Uuid,
    import: Uuid,
}

impl ImportHandle {
    pub fn new(session: Uuid, import: Uuid) -> Self {
        Self { session, import }
    }

    pub fn session(&self) -> Uuid {
        self.session
    }

    pub fn import(&self) -> Uuid {
        self.import
    }
}

impl FromStr for ImportHandle {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split('/');
        let session = parts.next().ok_or("missing import session")?;
        let import = parts.next().ok_or("missing import id")?;
        if parts.next().is_some() || session.is_empty() || import.is_empty() {
            return Err("import handle must contain exactly two UUID components");
        }
        Ok(Self {
            session: Uuid::parse_str(session).map_err(|_| "invalid import session")?,
            import: Uuid::parse_str(import).map_err(|_| "invalid import id")?,
        })
    }
}

impl fmt::Display for ImportHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.session, self.import)
    }
}

pub fn bind_import_session(session: Uuid) -> io::Result<()> {
    fs::create_dir_all(imports_root())?;
    fs::write(imports_root().join("current-session"), session.to_string())
}

fn current_import_session() -> io::Result<Uuid> {
    let value = fs::read_to_string(imports_root().join("current-session"))?;
    Uuid::parse_str(value.trim())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid current import session"))
}

pub fn staged_dir(handle: &ImportHandle) -> PathBuf {
    imports_root()
        .join(handle.session().to_string())
        .join(handle.import().to_string())
}

pub fn within_size_bound(len: u64) -> bool {
    len <= MAX_IMPORT_BYTES
}

pub fn staged_payload(dir: &Path) -> io::Result<PathBuf> {
    let payloads = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .filter(|entry| !is_reserved_staged_name(&entry.file_name().to_string_lossy()))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    match payloads.as_slice() {
        [payload] => Ok(payload.clone()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged import does not hold exactly one payload",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportLease {
    pub state: LeaseState,
    pub expires_at_ms: u128,
    pub request_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaseState {
    Active,
    Released,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StagedMetadata {
    staged_at_ms: u128,
}

pub enum LeaseStatus {
    Free,
    Held(ImportLease),
    Unreadable,
}

pub fn lease_status(dir: &Path) -> LeaseStatus {
    match fs::read(dir.join("lease.json")) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(LeaseStatus::Held)
            .unwrap_or(LeaseStatus::Unreadable),
        Err(error) if error.kind() == io::ErrorKind::NotFound => LeaseStatus::Free,
        Err(_) => LeaseStatus::Unreadable,
    }
}

fn ensure_current_and_fresh(handle: &ImportHandle, directory: &Path) -> io::Result<()> {
    if handle.session() != current_import_session()? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "import handle belongs to a different desktop session",
        ));
    }
    let metadata: StagedMetadata = serde_json::from_slice(&fs::read(directory.join("meta.json"))?)
        .map_err(io::Error::other)?;
    if now_ms()?.saturating_sub(metadata.staged_at_ms) > IMPORT_TTL.as_millis() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "staged import has expired",
        ));
    }
    Ok(())
}

pub fn validate_attachment_consumption(handle: &ImportHandle) -> io::Result<()> {
    let directory = staged_dir(handle);
    ensure_current_and_fresh(handle, &directory)?;
    match lease_status(&directory) {
        LeaseStatus::Free => Ok(()),
        LeaseStatus::Held(ImportLease {
            state: LeaseState::Released,
            expires_at_ms,
            ..
        }) if now_ms()? <= expires_at_ms => Ok(()),
        LeaseStatus::Held(_) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "staged import lease has expired or is active",
        )),
        LeaseStatus::Unreadable => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged import lease is unreadable",
        )),
    }
}

fn now_ms() -> io::Result<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(io::Error::other)
}

fn renew_active_lease(dir: &Path, request_id: &str) -> io::Result<()> {
    let lease = ImportLease {
        state: LeaseState::Active,
        expires_at_ms: now_ms()? + TRANSCRIPTION_LEASE_MAX.as_millis(),
        request_id: request_id.to_owned(),
    };
    let temp = write_lease_temp(dir, &lease)?;
    match fs::rename(&temp, dir.join("lease.json")) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(temp);
            Err(error)
        }
    }
}

fn write_lease_temp(dir: &Path, lease: &ImportLease) -> io::Result<PathBuf> {
    let temp = dir.join(format!(".lease-tmp-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&serde_json::to_vec(lease).map_err(io::Error::other)?)?;
    file.sync_all()?;
    Ok(temp)
}

pub struct LeaseGuard {
    directory: PathBuf,
    snapshot: PathBuf,
    request_id: String,
    stop: Arc<AtomicBool>,
    renewer: Option<thread::JoinHandle<()>>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(renewer) = self.renewer.take() {
            renewer.thread().unpark();
            let _ = renewer.join();
        }
        let _ = fs::remove_file(&self.snapshot);
        let lease = ImportLease {
            state: LeaseState::Released,
            expires_at_ms: now_ms().unwrap_or_default() + FALLBACK_GRACE.as_millis(),
            request_id: self.request_id.clone(),
        };
        if let Ok(temp) = write_lease_temp(&self.directory, &lease) {
            if let Err(error) = fs::rename(&temp, self.directory.join("lease.json")) {
                let _ = fs::remove_file(&temp);
                eprintln!("failed to release staged import lease: {error}");
            }
        }
    }
}

pub fn acquire_transcription(
    handle: &ImportHandle,
    request_id: &str,
) -> io::Result<(LeaseGuard, PathBuf)> {
    let unresolved = staged_dir(handle);
    ensure_current_and_fresh(handle, &unresolved)?;
    let root = imports_root().canonicalize()?;
    let directory = unresolved.canonicalize()?;
    if !directory.starts_with(&root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "staged import resolves outside the managed root",
        ));
    }
    let payload = staged_payload(&directory)?;
    let link = fs::symlink_metadata(&payload)?;
    if !link.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged payload is not a regular file",
        ));
    }
    let mut source = File::open(&payload)?;
    let metadata = source.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || !within_size_bound(metadata.len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged payload has an invalid size",
        ));
    }
    // Claim before allocating the private snapshot. A losing concurrent or
    // replayed request therefore creates no durable names.
    let lease = ImportLease {
        state: LeaseState::Active,
        expires_at_ms: now_ms()? + TRANSCRIPTION_LEASE_MAX.as_millis(),
        request_id: request_id.to_owned(),
    };
    let temp = write_lease_temp(&directory, &lease)?;
    let published = fs::hard_link(&temp, directory.join("lease.json"));
    let _ = fs::remove_file(&temp);
    published?;

    // The sidecar is a separate process and cannot receive the held descriptor
    // over its JSON protocol. Stream that descriptor into an exclusive private
    // file, then validate the exact read-only object the sidecar will reopen.
    let suffix = payload
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let snapshot = directory.join(if suffix.is_empty() {
        format!(".transcription-{}", Uuid::new_v4())
    } else {
        format!(".transcription-{}.{}", Uuid::new_v4(), suffix)
    });
    let snapshot_result = (|| -> io::Result<()> {
        let mut destination = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&snapshot)?;
        let copied = io::copy(&mut source.take(MAX_IMPORT_BYTES + 1), &mut destination)?;
        if copied != metadata.len() || !within_size_bound(copied) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged payload changed while snapshotting",
            ));
        }
        destination.sync_all()?;
        drop(destination);
        let mut permissions = fs::metadata(&snapshot)?.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&snapshot, permissions)?;

        let mut exact = File::open(&snapshot)?;
        let exact_metadata = exact.metadata()?;
        if !exact_metadata.is_file() || exact_metadata.len() != copied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "transcription snapshot is invalid",
            ));
        }
        let mut prefix = [0u8; 8192];
        let read = exact.read(&mut prefix)?;
        if !looks_like_audio(&prefix[..read]) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged payload is not recognized audio",
            ));
        }
        Ok(())
    })();
    if let Err(error) = snapshot_result {
        let _ = fs::remove_file(&snapshot);
        let _ = fs::remove_file(directory.join("lease.json"));
        return Err(error);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let renew_stop = Arc::clone(&stop);
    let renew_directory = directory.clone();
    let renew_request_id = request_id.to_owned();
    let renewer = thread::spawn(move || {
        while !renew_stop.load(Ordering::Acquire) {
            thread::park_timeout(Duration::from_secs(60));
            if renew_stop.load(Ordering::Acquire) {
                break;
            }
            let _ = renew_active_lease(&renew_directory, &renew_request_id);
        }
    });
    Ok((
        LeaseGuard {
            directory,
            snapshot: snapshot.clone(),
            request_id: request_id.to_owned(),
            stop,
            renewer: Some(renewer),
        },
        snapshot,
    ))
}

pub fn looks_like_audio(bytes: &[u8]) -> bool {
    bytes.starts_with(b"fLaC")
        || bytes.starts_with(b"OggS")
        || bytes.starts_with(b"ID3")
        || bytes.starts_with(b"caff")
        || bytes.starts_with(&[0x1a, 0x45, 0xdf, 0xa3])
        || (bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE")
        || (bytes.len() >= 12
            && &bytes[..4] == b"FORM"
            && matches!(&bytes[8..12], b"AIFF" | b"AIFC"))
        || (bytes.len() >= 8 && &bytes[4..8] == b"ftyp")
        || bytes
            .windows(2)
            .take(4096)
            .any(|pair| pair[0] == 0xff && pair[1] & 0xe0 == 0xe0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_round_trips_and_rejects_noncanonical_shapes() {
        let handle = ImportHandle::new(Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(handle.to_string().parse(), Ok(handle));
        for invalid in ["", "one", "../two", "/one/two", "one/two/three"] {
            assert!(
                invalid.parse::<ImportHandle>().is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn reserved_names_cover_present_and_future_sidecars() {
        assert!(is_reserved_staged_name("meta.json"));
        assert!(is_reserved_staged_name("lease.json"));
        assert!(is_reserved_staged_name(".lease-tmp-deadbeef"));
        assert!(is_reserved_staged_name(".transcription-deadbeef"));
        assert!(!is_reserved_staged_name("recording.m4a"));
    }

    #[test]
    fn size_bound_is_inclusive() {
        assert!(within_size_bound(MAX_IMPORT_BYTES));
        assert!(!within_size_bound(MAX_IMPORT_BYTES + 1));
    }

    #[test]
    fn audio_magic_recognizes_supported_containers_and_rejects_plain_text() {
        assert!(looks_like_audio(b"RIFF\0\0\0\0WAVEfmt "));
        assert!(looks_like_audio(b"fLaC payload"));
        assert!(looks_like_audio(b"\0\0\0\0ftypisom"));
        assert!(!looks_like_audio(b"private key material"));
    }

    #[test]
    fn transcription_lease_is_exclusive_released_and_session_bound() {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::env::var_os("CAIRN_HOME");
        let temp = tempfile::tempdir().unwrap();
        std::env::set_var("CAIRN_HOME", temp.path());

        let session = Uuid::new_v4();
        bind_import_session(session).unwrap();
        let handle = ImportHandle::new(session, Uuid::new_v4());
        let dir = staged_dir(&handle);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("meeting.wav"), b"RIFF\0\0\0\0WAVEfmt ").unwrap();
        fs::write(
            dir.join("meta.json"),
            serde_json::json!({
                "fileName": "meeting.wav", "size": 16, "stagedAtMs": now_ms().unwrap()
            })
            .to_string(),
        )
        .unwrap();

        let (lease, path) = acquire_transcription(&handle, "request-1").unwrap();
        assert!(path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".transcription-"));
        assert_eq!(fs::read(&path).unwrap(), b"RIFF\0\0\0\0WAVEfmt ");
        assert!(fs::metadata(&path).unwrap().permissions().readonly());
        assert!(matches!(
            lease_status(&dir),
            LeaseStatus::Held(ImportLease {
                state: LeaseState::Active,
                ..
            })
        ));
        assert!(acquire_transcription(&handle, "request-2").is_err());
        let snapshots = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".transcription-")
            })
            .count();
        assert_eq!(snapshots, 1, "a losing replay must not leak a snapshot");
        drop(lease);
        assert!(
            !path.exists(),
            "dropping the lease removes its private snapshot"
        );
        assert!(matches!(
            lease_status(&dir),
            LeaseStatus::Held(ImportLease {
                state: LeaseState::Released,
                ..
            })
        ));
        assert!(validate_attachment_consumption(&handle).is_ok());

        fs::write(
            dir.join("lease.json"),
            serde_json::to_vec(&ImportLease {
                state: LeaseState::Released,
                expires_at_ms: now_ms().unwrap().saturating_sub(1),
                request_id: "request-1".into(),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(validate_attachment_consumption(&handle).is_err());

        fs::remove_file(dir.join("lease.json")).unwrap();
        fs::write(
            dir.join("meta.json"),
            serde_json::json!({
                "fileName": "meeting.wav", "size": 16,
                "stagedAtMs": now_ms().unwrap().saturating_sub(IMPORT_TTL.as_millis() + 1)
            })
            .to_string(),
        )
        .unwrap();
        assert!(validate_attachment_consumption(&handle).is_err());
        assert!(acquire_transcription(&handle, "request-expired").is_err());

        let wrong_session = ImportHandle::new(Uuid::new_v4(), handle.import());
        assert!(acquire_transcription(&wrong_session, "request-3").is_err());
        match previous {
            Some(value) => std::env::set_var("CAIRN_HOME", value),
            None => std::env::remove_var("CAIRN_HOME"),
        }
    }
}
