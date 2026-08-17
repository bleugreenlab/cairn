use super::{
    engine_asset_name, ManagedComponent, ModelArtifact, VoiceError, VoiceResult, PROTOCOL_VERSION,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_ENGINE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CHECKSUM_BYTES: u64 = 1024;
/// Shortest gap between two progress events for one download.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

/// One download's observable life. `operation_id` is minted per download and
/// repeated on every event of that download: the UI latches the id it sees start
/// while its own request is in flight, so an id that outlived one download (the
/// component name, say) would let a later background install steer a bar that
/// belongs to something else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallEvent {
    Started {
        operation_id: String,
        component: String,
        total_bytes: Option<u64>,
    },
    Progress {
        operation_id: String,
        component: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    Finished {
        operation_id: String,
        component: String,
    },
}

/// Whether a download that has reached `downloaded` bytes should report again.
///
/// Chunk granularity is a firehose — a 547 MiB model arrives in tens of
/// thousands of chunks, and every event crosses a WebSocket to the webview to
/// move a progress bar a fraction of a pixel. A slow download still reports on
/// the time step so the bar never looks stalled; a fast one reports on whole
/// percent steps of a known total.
fn should_report(
    downloaded: u64,
    last_reported: u64,
    total_bytes: Option<u64>,
    since_last_report: Duration,
) -> bool {
    since_last_report >= PROGRESS_INTERVAL
        || total_bytes.is_some_and(|total| {
            total >= 100 && downloaded.saturating_sub(last_reported) >= total / 100
        })
}

/// Emits one download's throttled event stream to the installer's sink.
struct DownloadReporter<'a> {
    events: &'a EventSink,
    operation_id: String,
    component: String,
    total_bytes: Option<u64>,
    reported_at: Instant,
    reported_bytes: u64,
}

impl<'a> DownloadReporter<'a> {
    fn started(events: &'a EventSink, component: &str, total_bytes: Option<u64>) -> Self {
        let operation_id = Uuid::new_v4().to_string();
        events(InstallEvent::Started {
            operation_id: operation_id.clone(),
            component: component.to_owned(),
            total_bytes,
        });
        Self {
            events,
            operation_id,
            component: component.to_owned(),
            total_bytes,
            reported_at: Instant::now(),
            reported_bytes: 0,
        }
    }

    fn advanced(&mut self, downloaded: u64) {
        if !should_report(
            downloaded,
            self.reported_bytes,
            self.total_bytes,
            self.reported_at.elapsed(),
        ) {
            return;
        }
        self.report(downloaded);
    }

    /// Reports the final byte count before finishing, so a bar that was last
    /// drawn mid-step lands on complete rather than jumping from 99%.
    fn finished(&mut self, downloaded: u64) {
        if self.reported_bytes != downloaded {
            self.report(downloaded);
        }
        (self.events)(InstallEvent::Finished {
            operation_id: self.operation_id.clone(),
            component: self.component.clone(),
        });
    }

    fn report(&mut self, downloaded: u64) {
        (self.events)(InstallEvent::Progress {
            operation_id: self.operation_id.clone(),
            component: self.component.clone(),
            downloaded_bytes: downloaded,
            total_bytes: self.total_bytes,
        });
        self.reported_at = Instant::now();
        self.reported_bytes = downloaded;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedManifest {
    pub components: Vec<ManagedComponent>,
}

type EventSink = Arc<dyn Fn(InstallEvent) + Send + Sync>;

/// Installs only catalogued components beneath a single managed root.
///
/// A per-instance mutex serializes publication so concurrent opt-in requests
/// cannot race the manifest or replace one another's partial files.
pub struct ComponentInstaller {
    root: PathBuf,
    release_base: String,
    app_version: String,
    client: reqwest::Client,
    events: EventSink,
    install_lock: Mutex<()>,
}

impl ComponentInstaller {
    pub fn new(
        root: PathBuf,
        release_base: impl Into<String>,
        app_version: impl Into<String>,
        events: impl Fn(InstallEvent) + Send + Sync + 'static,
    ) -> Self {
        Self {
            root,
            release_base: release_base.into().trim_end_matches('/').to_owned(),
            app_version: app_version.into(),
            client: reqwest::Client::new(),
            events: Arc::new(events),
            install_lock: Mutex::new(()),
        }
    }

    pub async fn install_engine(&self, target: &str) -> VoiceResult<ManagedComponent> {
        let _guard = self.install_lock.lock().await;
        let asset = engine_asset_name(&self.app_version, target);
        let url = format!("{}/v{}/{}", self.release_base, self.app_version, asset);
        let checksum_url = format!("{url}.sha256");
        let checksum = self.fetch_small(&checksum_url, MAX_CHECKSUM_BYTES).await?;
        let digest = parse_checksum(&checksum, &asset)?;
        let directory = self.root.join("voice").join("engine");
        let destination = directory.join(&asset);
        let staged = directory.join(format!(".{asset}.candidate"));
        let mut component = self
            .download_verified("engine", &url, &staged, None, &digest, MAX_ENGINE_BYTES)
            .await?;
        set_executable(&staged).await?;
        clear_quarantine(&staged).await?;
        probe_engine(&staged, &self.app_version).await?;
        if cfg!(windows) {
            let _ = tokio::fs::remove_file(&destination).await;
        }
        tokio::fs::rename(&staged, &destination)
            .await
            .map_err(|error| VoiceError::Install(format!("engine publication failed: {error}")))?;
        component.id = asset;
        component.target = Some(target.to_owned());
        component.path = destination;
        self.publish(component.clone()).await?;
        Ok(component)
    }

    pub async fn install_model(&self, artifact: &ModelArtifact) -> VoiceResult<ManagedComponent> {
        let _guard = self.install_lock.lock().await;
        let destination = self.root.join("models").join(artifact.file_name);
        let component = self
            .download_verified(
                &format!("model:{:?}", artifact.model).to_lowercase(),
                artifact.url,
                &destination,
                Some(artifact.size_bytes),
                artifact.sha256,
                artifact.size_bytes,
            )
            .await?;
        self.publish(component.clone()).await?;
        Ok(component)
    }

    pub async fn manifest(&self) -> VoiceResult<ManagedManifest> {
        let bytes = match tokio::fs::read(self.manifest_path()).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ManagedManifest::default())
            }
            Err(error) => return Err(VoiceError::Install(error.to_string())),
        };
        serde_json::from_slice(&bytes)
            .map_err(|error| VoiceError::Install(format!("managed manifest is invalid: {error}")))
    }

    pub async fn verify(&self, component: &ManagedComponent) -> VoiceResult<()> {
        ensure_managed_path(&self.root, &component.path)?;
        let metadata = tokio::fs::metadata(&component.path)
            .await
            .map_err(|error| VoiceError::Corrupt(error.to_string()))?;
        if metadata.len() != component.size_bytes {
            return Err(VoiceError::Corrupt(format!(
                "{} size mismatch",
                component.id
            )));
        }
        let actual = hash_file(&component.path).await?;
        if actual != component.sha256 {
            return Err(VoiceError::Corrupt(format!(
                "{} checksum mismatch",
                component.id
            )));
        }
        Ok(())
    }

    pub async fn remove_managed(&self) -> VoiceResult<()> {
        let _guard = self.install_lock.lock().await;
        let manifest = self.manifest().await?;
        for component in manifest.components {
            ensure_managed_path(&self.root, &component.path)?;
            match tokio::fs::remove_file(&component.path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(VoiceError::Install(error.to_string())),
            }
        }
        match tokio::fs::remove_file(self.manifest_path()).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(VoiceError::Install(error.to_string())),
        }
    }

    async fn fetch_small(&self, url: &str, limit: u64) -> VoiceResult<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| VoiceError::Install(format!("download failed: {error}")))?
            .error_for_status()
            .map_err(|error| {
                VoiceError::Install(format!(
                    "exact-version release asset is unavailable: {error}"
                ))
            })?;
        if response
            .content_length()
            .is_some_and(|length| length > limit)
        {
            return Err(VoiceError::Install("response exceeds size limit".into()));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        if bytes.len() as u64 > limit {
            return Err(VoiceError::Install("response exceeds size limit".into()));
        }
        Ok(bytes.to_vec())
    }

    async fn download_verified(
        &self,
        kind: &str,
        url: &str,
        destination: &Path,
        exact_size: Option<u64>,
        digest: &str,
        maximum: u64,
    ) -> VoiceResult<ManagedComponent> {
        validate_digest(digest)?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| VoiceError::Install(error.to_string()))?;
        }
        let partial = destination.with_extension(format!(
            "{}partial",
            destination
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| format!("{value}."))
                .unwrap_or_default()
        ));
        let _ = tokio::fs::remove_file(&partial).await;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| VoiceError::Install(format!("download failed: {error}")))?
            .error_for_status()
            .map_err(|error| VoiceError::Install(format!("download failed: {error}")))?;
        let total = response.content_length();
        if total.is_some_and(|length| {
            length > maximum || exact_size.is_some_and(|expected| expected != length)
        }) {
            return Err(VoiceError::Install(
                "download Content-Length does not match catalog".into(),
            ));
        }
        let mut reporter = DownloadReporter::started(&self.events, kind, total.or(exact_size));
        let mut file = tokio::fs::File::create(&partial)
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        let mut stream = response.bytes_stream();
        let mut downloaded = 0_u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| VoiceError::Install(format!("download interrupted: {error}")))?;
            downloaded = downloaded.saturating_add(chunk.len() as u64);
            if downloaded > maximum || exact_size.is_some_and(|expected| downloaded > expected) {
                let _ = tokio::fs::remove_file(&partial).await;
                return Err(VoiceError::Install(
                    "download exceeded catalogued size".into(),
                ));
            }
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .map_err(|error| VoiceError::Install(error.to_string()))?;
            reporter.advanced(downloaded);
        }
        file.flush()
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        drop(file);
        if exact_size.is_some_and(|expected| downloaded != expected) {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(VoiceError::Install("download size mismatch".into()));
        }
        let actual = format!("{:x}", hasher.finalize());
        if actual != digest {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(VoiceError::Install("download checksum mismatch".into()));
        }
        if cfg!(windows) {
            let _ = tokio::fs::remove_file(destination).await;
        }
        tokio::fs::rename(&partial, destination)
            .await
            .map_err(|error| VoiceError::Install(format!("atomic publication failed: {error}")))?;
        reporter.finished(downloaded);
        Ok(ManagedComponent {
            kind: kind.into(),
            id: destination
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
            target: if kind == "engine" {
                super::current_target().map(str::to_owned)
            } else {
                None
            },
            size_bytes: downloaded,
            sha256: digest.into(),
            path: destination.to_owned(),
        })
    }

    async fn publish(&self, component: ManagedComponent) -> VoiceResult<()> {
        let mut manifest = self.manifest().await?;
        manifest
            .components
            .retain(|existing| existing.kind != component.kind);
        manifest.components.push(component);
        tokio::fs::create_dir_all(&self.root)
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        let temporary = self.manifest_path().with_extension("json.partial");
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        if cfg!(windows) {
            let _ = tokio::fs::remove_file(self.manifest_path()).await;
        }
        tokio::fs::rename(temporary, self.manifest_path())
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("voice-components.json")
    }
}

fn parse_checksum(bytes: &[u8], expected_name: &str) -> VoiceResult<String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| VoiceError::Install("checksum is not UTF-8".into()))?;
    let line = text
        .strip_suffix('\n')
        .unwrap_or(text)
        .strip_suffix('\r')
        .unwrap_or(text.strip_suffix('\n').unwrap_or(text));
    let (digest, name) = line
        .split_once("  ")
        .ok_or_else(|| VoiceError::Install("checksum must use sha256sum format".into()))?;
    validate_digest(digest)?;
    if name != expected_name || line.lines().count() != 1 {
        return Err(VoiceError::Install(
            "checksum names an unexpected asset".into(),
        ));
    }
    Ok(digest.to_ascii_lowercase())
}

fn validate_digest(digest: &str) -> VoiceResult<()> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(VoiceError::Install("invalid SHA-256 digest".into()));
    }
    Ok(())
}

fn ensure_managed_path(root: &Path, path: &Path) -> VoiceResult<()> {
    if !path.is_absolute()
        || !path.starts_with(root)
        || path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(VoiceError::Install(
            "manifest contains a path outside the managed root".into(),
        ));
    }
    Ok(())
}

async fn hash_file(path: &Path) -> VoiceResult<String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| VoiceError::Corrupt(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(unix)]
async fn set_executable(path: &Path) -> VoiceResult<()> {
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .await
        .map_err(|error| {
            VoiceError::Install(format!("setting executable permissions failed: {error}"))
        })
}
#[cfg(not(unix))]
async fn set_executable(_: &Path) -> VoiceResult<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
async fn clear_quarantine(path: &Path) -> VoiceResult<()> {
    let status = Command::new("/usr/bin/xattr")
        .arg("-d")
        .arg("com.apple.quarantine")
        .arg(path)
        .status()
        .await
        .map_err(|error| VoiceError::Install(format!("clearing quarantine failed: {error}")))?;
    // xattr returns nonzero when the attribute is absent; verify it is absent.
    if !status.success() {
        let check = Command::new("/usr/bin/xattr")
            .arg("-p")
            .arg("com.apple.quarantine")
            .arg(path)
            .status()
            .await;
        if check.is_ok_and(|status| status.success()) {
            return Err(VoiceError::Install("clearing quarantine failed".into()));
        }
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
async fn clear_quarantine(_: &Path) -> VoiceResult<()> {
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DistributionInfo {
    name: String,
    version: String,
    protocol_version: u32,
}

pub(crate) async fn probe_engine(path: &Path, version: &str) -> VoiceResult<()> {
    let output = Command::new(path)
        .arg("--distribution-info")
        .output()
        .await
        .map_err(|error| VoiceError::Install(format!("engine identity probe failed: {error}")))?;
    if !output.status.success() {
        return Err(VoiceError::Install("engine identity probe failed".into()));
    }
    let info: DistributionInfo = serde_json::from_slice(&output.stdout).map_err(|error| {
        VoiceError::Install(format!(
            "engine identity probe returned invalid JSON: {error}"
        ))
    })?;
    if info.name != "cairn-voice"
        || info.version != version
        || info.protocol_version != PROTOCOL_VERSION
    {
        return Err(VoiceError::Incompatible(
            "engine distribution identity does not match this app".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_parser_requires_exact_asset_and_format() {
        let digest = "a".repeat(64);
        assert_eq!(
            parse_checksum(format!("{digest}  engine\n").as_bytes(), "engine").unwrap(),
            digest
        );
        assert!(parse_checksum(format!("{digest} engine\n").as_bytes(), "engine").is_err());
        assert!(parse_checksum(format!("{digest}  other\n").as_bytes(), "engine").is_err());
    }

    fn recording_sink() -> (EventSink, Arc<std::sync::Mutex<Vec<InstallEvent>>>) {
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = recorded.clone();
        let events: EventSink = Arc::new(move |event| sink.lock().unwrap().push(event));
        (events, recorded)
    }

    fn operation_id(event: &InstallEvent) -> &str {
        match event {
            InstallEvent::Started { operation_id, .. }
            | InstallEvent::Progress { operation_id, .. }
            | InstallEvent::Finished { operation_id, .. } => operation_id,
        }
    }

    /// Streams `chunks` equal parts of `total` through one reporter, the way the
    /// download loop feeds it running byte counts.
    fn stream_download(events: &EventSink, total: u64, chunks: u64) {
        let mut reporter = DownloadReporter::started(events, "model:fast", Some(total));
        let mut downloaded = 0;
        for _ in 0..chunks {
            downloaded += total / chunks;
            reporter.advanced(downloaded);
        }
        reporter.finished(total);
    }

    #[test]
    fn progress_reports_on_a_whole_percent_or_the_time_step() {
        let total = Some(100_000);
        assert!(!should_report(999, 0, total, Duration::ZERO));
        assert!(should_report(1_000, 0, total, Duration::ZERO));
        assert!(should_report(1, 0, total, PROGRESS_INTERVAL));
        // A total the server never declared leaves only the time step.
        assert!(!should_report(9_000_000, 0, None, Duration::ZERO));
        assert!(should_report(1, 0, None, PROGRESS_INTERVAL));
    }

    /// The webview latches whichever operation starts while its own request is
    /// in flight, so one download's events must share one id and a later
    /// download must not reuse it.
    #[test]
    fn one_download_carries_one_fresh_operation_id_from_start_to_finish() {
        let (events, recorded) = recording_sink();
        stream_download(&events, 1_000_000, 500);
        let first = recorded.lock().unwrap().clone();
        recorded.lock().unwrap().clear();
        stream_download(&events, 1_000_000, 500);
        let second = recorded.lock().unwrap().clone();

        let id = operation_id(&first[0]).to_owned();
        assert!(matches!(first[0], InstallEvent::Started { .. }));
        assert!(first.iter().all(|event| operation_id(event) == id));
        assert!(matches!(
            first.last().expect("a download finishes"),
            InstallEvent::Finished { .. }
        ));
        assert_ne!(
            operation_id(&second[0]),
            id,
            "a second download is its own operation"
        );
    }

    /// A 547 MiB model arrives in tens of thousands of chunks; reporting each one
    /// would put that many WebSocket frames behind the bar it draws. The final
    /// count still has to be exact, or the bar stalls short of complete.
    #[test]
    fn a_large_download_reports_percent_steps_rather_than_chunks() {
        let total = 547 * 1024 * 1024;
        let (events, recorded) = recording_sink();
        stream_download(&events, total, 60_000);
        let recorded = recorded.lock().unwrap();

        let progress: Vec<_> = recorded
            .iter()
            .filter_map(|event| match event {
                InstallEvent::Progress {
                    downloaded_bytes, ..
                } => Some(*downloaded_bytes),
                _ => None,
            })
            .collect();
        assert!(
            (100..=400).contains(&progress.len()),
            "expected roughly one report per percent, got {}",
            progress.len()
        );
        assert_eq!(progress.last(), Some(&total));
    }

    #[test]
    fn managed_paths_cannot_escape_root() {
        let root = Path::new("/managed");
        assert!(ensure_managed_path(root, Path::new("/managed/models/model.bin")).is_ok());
        assert!(ensure_managed_path(root, Path::new("/tmp/model.bin")).is_err());
        assert!(ensure_managed_path(root, Path::new("/managed/../tmp/model.bin")).is_err());
    }
}
