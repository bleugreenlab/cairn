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
use tokio::sync::{Mutex, OwnedMutexGuard};
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
    /// The running runner's content identity, the `buildId` `/api/health`
    /// answers with. Every self-rebuild of one version publishes an engine under
    /// the same asset name, so this, not the name, is what ties a handed-off
    /// engine to the app it was built with.
    build_identity: String,
    client: reqwest::Client,
    events: EventSink,
    /// Shared so an engine commit can carry the lock into a task of its own and
    /// keep it until the commit settles, whoever stops waiting for it.
    install_lock: Arc<Mutex<()>>,
    #[cfg(test)]
    commit_gate: Option<Arc<CommitGate>>,
}

/// Holds an engine commit at its most delicate point, new bytes in place and the
/// manifest not yet written, so a test can cancel the caller exactly there.
#[cfg(test)]
#[derive(Default)]
struct CommitGate {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

/// What a self-rebuild leaves beside the engine it built and gated, once the app
/// built from the same revision has been verified installed. Written by
/// `handOffVoiceEngine` in `scripts/self-rebuild-lib.ts`; the two sides share no
/// code, so an unknown field is refused rather than silently dropped.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LocalEngineHandoff {
    build_id: String,
    asset: String,
}

const HANDOFF_FILE: &str = "handoff.json";

impl ComponentInstaller {
    pub fn new(
        root: PathBuf,
        release_base: impl Into<String>,
        app_version: impl Into<String>,
        build_identity: impl Into<String>,
        events: impl Fn(InstallEvent) + Send + Sync + 'static,
    ) -> Self {
        Self {
            root,
            release_base: release_base.into().trim_end_matches('/').to_owned(),
            app_version: app_version.into(),
            build_identity: build_identity.into(),
            client: reqwest::Client::new(),
            events: Arc::new(events),
            install_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            commit_gate: None,
        }
    }

    pub async fn install_engine(&self, target: &str) -> VoiceResult<ManagedComponent> {
        let guard = self.install_lock.clone().lock_owned().await;
        let asset = engine_asset_name(&self.app_version, target);
        recover_engine_backup(
            &self.root,
            &self.engine_dir().join(&asset),
            &self.engine_backup_path(&asset),
        )
        .await?;
        // Ahead of the already-current check: a handed-off engine carries the
        // same asset name as the engine it replaces, so that check would call
        // the stale engine current and never look further.
        if let Some(handoff) = self.local_engine_handoff().await {
            return self.install_local_engine(guard, target, handoff).await;
        }
        let manifest = self.manifest().await?;
        if let Some(component) = manifest
            .components
            .iter()
            .find(|component| component.kind == "engine" && component.id == asset)
        {
            if self.verify(component).await.is_ok() {
                return Ok(component.clone());
            }
        }
        let previous_engine = manifest
            .components
            .into_iter()
            .find(|component| component.kind == "engine");
        let url = format!("{}/v{}/{}", self.release_base, self.app_version, asset);
        let checksum_url = format!("{url}.sha256");
        let checksum = self.fetch_small(&checksum_url, MAX_CHECKSUM_BYTES).await?;
        let digest = parse_checksum(&checksum, &asset)?;
        let staged = self.staged_engine_path(&asset);
        let component = self
            .download_verified("engine", &url, &staged, None, &digest, MAX_ENGINE_BYTES)
            .await?;
        self.publish_engine(guard, &staged, target, component, previous_engine, None)
            .await
    }

    /// Whether a self-rebuild has handed off an engine built with this runner
    /// that has not been installed yet.
    pub async fn local_engine_pending(&self) -> bool {
        self.local_engine_handoff().await.is_some()
    }

    /// The handoff waiting for this runner, if any. One bound to another build
    /// is left where it is: it belongs to an app that is not running, and the
    /// next self-rebuild replaces it.
    async fn local_engine_handoff(&self) -> Option<LocalEngineHandoff> {
        let path = self.local_engine_dir().join(HANDOFF_FILE);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
            Err(error) => {
                log::warn!(
                    "Could not read voice engine handoff {}: {error}",
                    path.display()
                );
                return None;
            }
        };
        let handoff: LocalEngineHandoff = match serde_json::from_slice(&bytes) {
            Ok(handoff) => handoff,
            Err(error) => {
                log::warn!(
                    "Ignoring invalid voice engine handoff {}: {error}",
                    path.display()
                );
                return None;
            }
        };
        (handoff.build_id == self.build_identity).then_some(handoff)
    }

    /// Install a handed-off engine under the caller's `install_lock` guard.
    async fn install_local_engine(
        &self,
        guard: OwnedMutexGuard<()>,
        target: &str,
        handoff: LocalEngineHandoff,
    ) -> VoiceResult<ManagedComponent> {
        let result = self.adopt_local_engine(guard, target, &handoff).await;
        // Consumed whatever the outcome. Adopted, it is redundant; refused, it
        // must not be retried by every voice request that follows, which the
        // previous engine (untouched by a refusal) serves instead.
        match tokio::fs::remove_dir_all(self.local_engine_dir()).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => log::warn!("Could not remove voice engine handoff: {error}"),
        }
        if let Err(error) = &result {
            log::warn!("Refused handed-off voice engine {}: {error}", handoff.asset);
        }
        result
    }

    async fn adopt_local_engine(
        &self,
        guard: OwnedMutexGuard<()>,
        target: &str,
        handoff: &LocalEngineHandoff,
    ) -> VoiceResult<ManagedComponent> {
        let asset = engine_asset_name(&self.app_version, target);
        if handoff.asset != asset {
            return Err(VoiceError::Incompatible(format!(
                "handed-off engine {} is not this app's {asset}",
                handoff.asset
            )));
        }
        let source_dir = self.local_engine_dir();
        let checksum = read_small(
            &source_dir.join(format!("{asset}.sha256")),
            MAX_CHECKSUM_BYTES,
        )
        .await?;
        let digest = parse_checksum(&checksum, &asset)?;
        let previous_engine = self
            .manifest()
            .await?
            .components
            .into_iter()
            .find(|component| component.kind == "engine");
        let staged = self.staged_engine_path(&asset);
        let size_bytes =
            copy_verified(&source_dir.join(&asset), &staged, &digest, MAX_ENGINE_BYTES).await?;
        let component = ManagedComponent {
            kind: "engine".into(),
            id: asset,
            target: Some(target.to_owned()),
            size_bytes,
            sha256: digest,
            path: staged.clone(),
        };
        self.publish_engine(
            guard,
            &staged,
            target,
            component,
            previous_engine,
            Some(self.local_engine_dir()),
        )
        .await
    }

    /// The one way an engine becomes current, whichever source produced the
    /// checksum-verified candidate: made executable, released from quarantine,
    /// probed for this app's exact distribution identity, then committed by an
    /// [`EngineCommit`]. `handoff` is the handoff directory the commit consumes,
    /// when the candidate came from one.
    ///
    /// The commit runs as a task of its own, holding `guard` until it settles. A
    /// caller that stops waiting (a cancelled file transcription drops this
    /// future) therefore cannot stop it between replacing the engine and
    /// recording it, and nobody else installs until it has.
    async fn publish_engine(
        &self,
        guard: OwnedMutexGuard<()>,
        staged: &Path,
        target: &str,
        mut component: ManagedComponent,
        previous_engine: Option<ManagedComponent>,
        handoff: Option<PathBuf>,
    ) -> VoiceResult<ManagedComponent> {
        let asset = engine_asset_name(&self.app_version, target);
        let probed = async {
            set_executable(staged).await?;
            clear_quarantine(staged).await?;
            probe_engine(staged, &self.app_version).await
        }
        .await;
        if let Err(error) = probed {
            let _ = tokio::fs::remove_file(staged).await;
            return Err(error);
        }
        let destination = self.engine_dir().join(&asset);
        component.id = asset.clone();
        component.target = Some(target.to_owned());
        component.path = destination.clone();
        let commit = EngineCommit {
            root: self.root.clone(),
            staged: staged.to_owned(),
            destination,
            backup: self.engine_backup_path(&asset),
            component: component.clone(),
            superseded: previous_engine,
            handoff,
            #[cfg(test)]
            gate: self.commit_gate.clone(),
        };
        tokio::spawn(async move {
            let result = commit.run().await;
            drop(guard);
            result
        })
        .await
        .map_err(|error| {
            VoiceError::Install(format!("engine publication task failed: {error}"))
        })??;
        Ok(component)
    }

    pub fn engine_is_current(&self, component: &ManagedComponent, target: &str) -> bool {
        component.kind == "engine" && component.id == engine_asset_name(&self.app_version, target)
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
        read_manifest(&self.root).await
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
        publish_component(&self.root, component).await
    }

    fn manifest_path(&self) -> PathBuf {
        manifest_file(&self.root)
    }

    fn engine_dir(&self) -> PathBuf {
        self.root.join("voice").join("engine")
    }

    fn staged_engine_path(&self, asset: &str) -> PathBuf {
        self.engine_dir().join(format!(".{asset}.candidate"))
    }

    /// A second name for the published engine's bytes while a commit replaces
    /// them. See [`EngineCommit`].
    fn engine_backup_path(&self, asset: &str) -> PathBuf {
        self.engine_dir().join(format!(".{asset}.previous"))
    }

    /// Where a self-rebuild hands off the engine it built: the binary under its
    /// release asset name, the sibling `.sha256`, and `handoff.json`.
    fn local_engine_dir(&self) -> PathBuf {
        self.root.join("voice").join("local-engine")
    }
}

fn manifest_file(root: &Path) -> PathBuf {
    root.join("voice-components.json")
}

async fn read_manifest(root: &Path) -> VoiceResult<ManagedManifest> {
    let bytes = match tokio::fs::read(manifest_file(root)).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ManagedManifest::default())
        }
        Err(error) => return Err(VoiceError::Install(error.to_string())),
    };
    serde_json::from_slice(&bytes)
        .map_err(|error| VoiceError::Install(format!("managed manifest is invalid: {error}")))
}

/// Record `component` as the one installed component of its kind.
async fn publish_component(root: &Path, component: ManagedComponent) -> VoiceResult<()> {
    let mut manifest = read_manifest(root).await?;
    manifest
        .components
        .retain(|existing| existing.kind != component.kind);
    manifest.components.push(component);
    tokio::fs::create_dir_all(root)
        .await
        .map_err(|error| VoiceError::Install(error.to_string()))?;
    let temporary = manifest_file(root).with_extension("json.partial");
    let bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| VoiceError::Install(error.to_string()))?;
    tokio::fs::write(&temporary, bytes)
        .await
        .map_err(|error| VoiceError::Install(error.to_string()))?;
    if cfg!(windows) {
        let _ = tokio::fs::remove_file(manifest_file(root)).await;
    }
    tokio::fs::rename(temporary, manifest_file(root))
        .await
        .map_err(|error| VoiceError::Install(error.to_string()))
}

async fn remove_superseded_engine(
    root: &Path,
    previous: &ManagedComponent,
    current_path: &Path,
) -> VoiceResult<()> {
    if previous.kind != "engine" || previous.path == current_path {
        return Ok(());
    }
    ensure_managed_path(root, &previous.path)?;
    match tokio::fs::remove_file(&previous.path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(VoiceError::Install(format!(
            "removing superseded engine failed: {error}"
        ))),
    }
}

/// Replacing a managed engine and recording the replacement, as one operation
/// that either completes or leaves the previous engine and manifest as they
/// were.
///
/// The destination never disappears. Before the new bytes arrive, the published
/// ones gain a second name (`backup`, a hard link, so nothing is copied or moved
/// away); the new bytes then take the destination in one rename. The manifest
/// write is the commit point. If anything fails before it, the backup is renamed
/// back over the destination, again in one step.
struct EngineCommit {
    root: PathBuf,
    staged: PathBuf,
    destination: PathBuf,
    backup: PathBuf,
    component: ManagedComponent,
    /// The engine this one replaces, removed once the commit lands if it lived
    /// at another path.
    superseded: Option<ManagedComponent>,
    /// The handoff directory this engine came from, consumed whatever the outcome.
    handoff: Option<PathBuf>,
    #[cfg(test)]
    gate: Option<Arc<CommitGate>>,
}

impl EngineCommit {
    async fn run(self) -> VoiceResult<()> {
        let result = self.commit().await;
        let _ = tokio::fs::remove_file(&self.staged).await;
        if let Some(handoff) = &self.handoff {
            match tokio::fs::remove_dir_all(handoff).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => log::warn!("Could not remove voice engine handoff: {error}"),
            }
        }
        if result.is_ok() {
            if let Some(previous) = &self.superseded {
                if let Err(error) =
                    remove_superseded_engine(&self.root, previous, &self.destination).await
                {
                    log::warn!("Could not remove superseded voice engine: {error}");
                }
            }
        }
        result
    }

    async fn commit(&self) -> VoiceResult<()> {
        let backed_up = match tokio::fs::hard_link(&self.destination, &self.backup).await {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(VoiceError::Install(format!(
                    "keeping the previous engine failed: {error}"
                )))
            }
        };
        let committed = async {
            replace_file(&self.staged, &self.destination).await?;
            self.pause_before_recording().await;
            publish_component(&self.root, self.component.clone()).await
        }
        .await;
        match &committed {
            Ok(()) if backed_up => {
                let _ = tokio::fs::remove_file(&self.backup).await;
            }
            Ok(()) => {}
            Err(_) if backed_up => {
                match replace_file(&self.backup, &self.destination).await {
                    // Renaming one name of a file over another is a successful
                    // no-op, which is what happens when the new bytes never
                    // arrived; the backup is then still there, and redundant.
                    Ok(()) => {
                        let _ = tokio::fs::remove_file(&self.backup).await;
                    }
                    Err(restore) => log::error!(
                        "Could not restore the previous voice engine from {}: {restore}",
                        self.backup.display()
                    ),
                }
            }
            Err(_) => {
                let _ = tokio::fs::remove_file(&self.destination).await;
            }
        }
        committed
    }

    #[cfg(test)]
    async fn pause_before_recording(&self) {
        if let Some(gate) = &self.gate {
            gate.reached.notify_one();
            gate.release.notified().await;
        }
    }

    #[cfg(not(test))]
    async fn pause_before_recording(&self) {}
}

/// Settle a backup an earlier [`EngineCommit`] left because the process died
/// mid-commit. It may be the only copy of the bytes the manifest still
/// describes, so it is never simply deleted: it is restored when the manifest
/// names it, dropped only when the destination already matches the manifest
/// (the backup is then a redundant second name), and otherwise set aside under a
/// name of its own.
async fn recover_engine_backup(root: &Path, destination: &Path, backup: &Path) -> VoiceResult<()> {
    if !tokio::fs::try_exists(backup).await.unwrap_or(false) {
        return Ok(());
    }
    let recorded = read_manifest(root)
        .await?
        .components
        .into_iter()
        .find(|component| component.kind == "engine" && component.path == destination);
    if let Some(recorded) = recorded {
        if file_matches(destination, &recorded).await {
            let _ = tokio::fs::remove_file(backup).await;
            return Ok(());
        }
        if file_matches(backup, &recorded).await {
            log::warn!("Restoring the voice engine an interrupted replacement left behind");
            return replace_file(backup, destination).await;
        }
    }
    let orphan = backup.with_extension(format!("orphan-{}", Uuid::new_v4()));
    log::warn!(
        "Setting aside an unexplained voice engine backup as {}",
        orphan.display()
    );
    tokio::fs::rename(backup, &orphan).await.map_err(|error| {
        VoiceError::Install(format!("setting aside {}: {error}", backup.display()))
    })
}

async fn file_matches(path: &Path, component: &ManagedComponent) -> bool {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.len() == component.size_bytes => hash_file(path)
            .await
            .is_ok_and(|digest| digest == component.sha256),
        _ => false,
    }
}

/// Put `from` at `to` in one rename. Windows cannot rename over an existing
/// file, so there the old one goes first.
async fn replace_file(from: &Path, to: &Path) -> VoiceResult<()> {
    if cfg!(windows) {
        let _ = tokio::fs::remove_file(to).await;
    }
    tokio::fs::rename(from, to)
        .await
        .map_err(|error| VoiceError::Install(format!("engine publication failed: {error}")))
}

async fn read_small(path: &Path, limit: u64) -> VoiceResult<Vec<u8>> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| VoiceError::Install(format!("{}: {error}", path.display())))?;
    if metadata.len() > limit {
        return Err(VoiceError::Install(format!(
            "{} exceeds size limit",
            path.display()
        )));
    }
    tokio::fs::read(path)
        .await
        .map_err(|error| VoiceError::Install(format!("{}: {error}", path.display())))
}

/// Copy `source` to `destination`, hashing as it streams, and keep the copy only
/// if it is within `maximum` bytes and matches `digest`. Returns the size.
async fn copy_verified(
    source: &Path,
    destination: &Path,
    digest: &str,
    maximum: u64,
) -> VoiceResult<u64> {
    use tokio::io::AsyncReadExt;
    validate_digest(digest)?;
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
    }
    let copied = async {
        let mut input = tokio::fs::File::open(source).await.map_err(|error| {
            VoiceError::Install(format!("{} is unreadable: {error}", source.display()))
        })?;
        let mut output = tokio::fs::File::create(destination)
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        let mut copied = 0_u64;
        loop {
            let read = input
                .read(&mut buffer)
                .await
                .map_err(|error| VoiceError::Install(error.to_string()))?;
            if read == 0 {
                break;
            }
            copied = copied.saturating_add(read as u64);
            if copied > maximum {
                return Err(VoiceError::Install("engine exceeds size limit".into()));
            }
            hasher.update(&buffer[..read]);
            output
                .write_all(&buffer[..read])
                .await
                .map_err(|error| VoiceError::Install(error.to_string()))?;
        }
        output
            .flush()
            .await
            .map_err(|error| VoiceError::Install(error.to_string()))?;
        if format!("{:x}", hasher.finalize()) != digest {
            return Err(VoiceError::Install("engine checksum mismatch".into()));
        }
        Ok(copied)
    }
    .await;
    if copied.is_err() {
        let _ = tokio::fs::remove_file(destination).await;
    }
    copied
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

    /// The engine reports more about itself than the app checks (the embedded
    /// Parakeet library's provenance, for the release staging step). The identity
    /// probe must keep reading only what it needs, or an engine that grows its
    /// self-description would stop installing.
    #[test]
    fn the_identity_probe_tolerates_fields_it_does_not_read() {
        let info: DistributionInfo = serde_json::from_str(
            r#"{"name":"cairn-voice","version":"1.2.3","protocolVersion":1,
                "parakeet":{"commit":"abc","ggmlCommit":"def","patches":[],"device":"metal"}}"#,
        )
        .expect("unknown fields are ignored");
        assert_eq!(info.name, "cairn-voice");
        assert_eq!(info.version, "1.2.3");
        assert_eq!(info.protocol_version, 1);
    }

    #[test]
    fn managed_paths_cannot_escape_root() {
        let root = Path::new("/managed");
        assert!(ensure_managed_path(root, Path::new("/managed/models/model.bin")).is_ok());
        assert!(ensure_managed_path(root, Path::new("/tmp/model.bin")).is_err());
        assert!(ensure_managed_path(root, Path::new("/managed/../tmp/model.bin")).is_err());
    }

    #[tokio::test]
    async fn installing_an_already_current_engine_does_not_download_again() {
        let directory = tempfile::tempdir().unwrap();
        let target = super::super::current_target().expect("voice tests run on a supported target");
        let version = "1.2.3";
        let asset = engine_asset_name(version, target);
        let path = directory.path().join(&asset);
        let bytes = b"already installed";
        tokio::fs::write(&path, bytes).await.unwrap();
        let component = ManagedComponent {
            kind: "engine".into(),
            id: asset,
            target: Some(target.into()),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
            path,
        };
        let installer = ComponentInstaller::new(
            directory.path().to_path_buf(),
            "http://127.0.0.1:1",
            version,
            "test-build",
            |_| {},
        );
        installer.publish(component.clone()).await.unwrap();

        assert_eq!(
            installer.install_engine(target).await.unwrap().id,
            component.id,
            "the post-lock manifest check must make a concurrent second refresh a no-op"
        );
    }

    #[tokio::test]
    async fn replacing_an_engine_removes_only_the_superseded_binary() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let installer = ComponentInstaller::new(
            root.to_path_buf(),
            "http://127.0.0.1:1",
            "1.2.3",
            "test-build",
            |_| {},
        );
        let old_path = root.join("voice/engine/cairn-voice-v1.2.2-test");
        let new_path = root.join("voice/engine/cairn-voice-v1.2.3-test");
        let model_path = root.join("models/model-fast");
        let realtime_path = root.join("models/model-realtime.gguf");
        tokio::fs::create_dir_all(old_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::create_dir_all(model_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&old_path, b"old").await.unwrap();
        tokio::fs::write(&new_path, b"new").await.unwrap();
        tokio::fs::write(&model_path, b"model").await.unwrap();
        tokio::fs::write(&realtime_path, b"gguf").await.unwrap();
        let old_engine = ManagedComponent {
            kind: "engine".into(),
            id: "cairn-voice-v1.2.2-test".into(),
            target: Some("test".into()),
            size_bytes: 3,
            sha256: format!("{:x}", Sha256::digest(b"old")),
            path: old_path.clone(),
        };
        let model = ManagedComponent {
            kind: "model:fast".into(),
            id: "model-fast".into(),
            target: None,
            size_bytes: 5,
            sha256: format!("{:x}", Sha256::digest(b"model")),
            path: model_path.clone(),
        };
        let realtime = ManagedComponent {
            kind: "model:realtime".into(),
            id: "model-realtime.gguf".into(),
            target: None,
            size_bytes: 4,
            sha256: format!("{:x}", Sha256::digest(b"gguf")),
            path: realtime_path.clone(),
        };
        installer.publish(old_engine.clone()).await.unwrap();
        installer.publish(model.clone()).await.unwrap();
        installer.publish(realtime.clone()).await.unwrap();
        let current_engine = ManagedComponent {
            kind: "engine".into(),
            id: "cairn-voice-v1.2.3-test".into(),
            target: Some("test".into()),
            size_bytes: 3,
            sha256: format!("{:x}", Sha256::digest(b"new")),
            path: new_path.clone(),
        };

        installer.publish(current_engine.clone()).await.unwrap();
        remove_superseded_engine(root, &old_engine, &current_engine.path)
            .await
            .unwrap();

        assert!(!old_path.exists());
        assert!(new_path.exists());
        assert!(model_path.exists());
        assert!(
            realtime_path.exists(),
            "an engine refresh never touches models"
        );
        let manifest = installer.manifest().await.unwrap();
        assert!(manifest
            .components
            .iter()
            .any(|component| component.id == realtime.id));
        assert!(manifest
            .components
            .iter()
            .any(|component| component.id == current_engine.id));
        assert!(manifest
            .components
            .iter()
            .any(|component| component.id == model.id));
    }

    /// The cross-language contract: exactly what `handOffVoiceEngine` in
    /// `scripts/self-rebuild-lib.ts` writes, and nothing else.
    #[test]
    fn the_handoff_file_is_the_shape_the_self_rebuild_writes() {
        let handoff: LocalEngineHandoff = serde_json::from_str(
            r#"{"buildId":"content-v2:0123456789abcdef0123456789abcdef","asset":"cairn-voice-v1.2.3-aarch64-apple-darwin"}"#,
        )
        .expect("the self-rebuild's handoff parses");
        assert_eq!(
            handoff.build_id,
            "content-v2:0123456789abcdef0123456789abcdef"
        );
        assert_eq!(handoff.asset, "cairn-voice-v1.2.3-aarch64-apple-darwin");
        assert!(
            serde_json::from_str::<LocalEngineHandoff>(
                r#"{"buildId":"b","asset":"a","version":"1.2.3"}"#
            )
            .is_err(),
            "a field one side writes and the other ignores is a contract drifting apart"
        );
    }

    /// Adoption of an engine a self-rebuild handed off, against scripted
    /// engines that answer the identity probe the way a real one does.
    #[cfg(unix)]
    mod handoff {
        use super::*;
        use std::os::unix::fs::PermissionsExt;

        const VERSION: &str = "1.2.3";
        const THIS_RUNNER: &str = "content-v2:this-runner";

        /// A script that identifies itself as `name` at `version` and exits
        /// with `status`. `marker` only makes two engines' bytes differ.
        fn engine_script(name: &str, version: &str, status: i32, marker: &str) -> Vec<u8> {
            format!(
                "#!/bin/sh\n# {marker}\nif [ \"$1\" = '--distribution-info' ]; then\n  printf '{{\"name\":\"{name}\",\"version\":\"{version}\",\"protocolVersion\":{PROTOCOL_VERSION}}}\\n'\n  exit {status}\nfi\nexit 0\n"
            )
            .into_bytes()
        }

        fn component(
            kind: &str,
            path: PathBuf,
            bytes: &[u8],
            target: Option<&str>,
        ) -> ManagedComponent {
            ManagedComponent {
                kind: kind.into(),
                id: path.file_name().unwrap().to_string_lossy().into_owned(),
                target: target.map(str::to_owned),
                size_bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
                path,
            }
        }

        struct Fixture {
            directory: tempfile::TempDir,
            installer: ComponentInstaller,
            target: &'static str,
            asset: String,
            published: Vec<u8>,
            engine: ManagedComponent,
            model: ManagedComponent,
        }

        impl Fixture {
            /// An installer for this runner with a published engine and model,
            /// both real files the installer will verify.
            async fn new() -> Fixture {
                let directory = tempfile::tempdir().unwrap();
                let root = directory.path();
                let target =
                    crate::voice::current_target().expect("voice tests run on a supported target");
                let asset = engine_asset_name(VERSION, target);
                let installer = ComponentInstaller::new(
                    root.to_path_buf(),
                    "http://127.0.0.1:1",
                    VERSION,
                    THIS_RUNNER,
                    |_| {},
                );
                let engine_path = root.join("voice/engine").join(&asset);
                std::fs::create_dir_all(engine_path.parent().unwrap()).unwrap();
                let published = engine_script("cairn-voice", VERSION, 0, "published");
                std::fs::write(&engine_path, &published).unwrap();
                std::fs::set_permissions(&engine_path, std::fs::Permissions::from_mode(0o755))
                    .unwrap();
                let model_path = root.join("models/model-fast");
                std::fs::create_dir_all(model_path.parent().unwrap()).unwrap();
                std::fs::write(&model_path, b"model").unwrap();
                let engine = component("engine", engine_path, &published, Some(target));
                let model = component("model:fast", model_path, b"model", None);
                installer.publish(engine.clone()).await.unwrap();
                installer.publish(model.clone()).await.unwrap();
                Fixture {
                    directory,
                    installer,
                    target,
                    asset,
                    published,
                    engine,
                    model,
                }
            }

            fn handoff_dir(&self) -> PathBuf {
                self.directory.path().join("voice/local-engine")
            }

            fn backup(&self) -> PathBuf {
                self.installer.engine_backup_path(&self.asset)
            }

            /// The engine the manifest records, which must be the bytes on disk.
            async fn recorded_engine(&self) -> ManagedComponent {
                let recorded = self
                    .installer
                    .manifest()
                    .await
                    .unwrap()
                    .components
                    .into_iter()
                    .find(|component| component.kind == "engine")
                    .expect("an engine is recorded");
                self.installer
                    .verify(&recorded)
                    .await
                    .expect("the recorded engine is the one on disk");
                recorded
            }

            /// Lay out a handoff the way the self-rebuild does. `checksum_of`
            /// is the bytes the `.sha256` describes, normally `engine` itself.
            fn hand_off(&self, build_id: &str, engine: &[u8], checksum_of: &[u8]) {
                let dir = self.handoff_dir();
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join(&self.asset), engine).unwrap();
                std::fs::write(
                    dir.join(format!("{}.sha256", self.asset)),
                    format!("{:x}  {}\n", Sha256::digest(checksum_of), self.asset),
                )
                .unwrap();
                std::fs::write(
                    dir.join(HANDOFF_FILE),
                    serde_json::json!({ "buildId": build_id, "asset": self.asset }).to_string(),
                )
                .unwrap();
            }

            /// A refused handoff changes nothing but itself: the published
            /// engine's bytes and manifest entry stand, no candidate is left
            /// behind, and the handoff is consumed so it is not retried.
            async fn assert_refused_and_intact(&self, result: VoiceResult<ManagedComponent>) {
                let error = result.expect_err("the handed-off engine is refused");
                assert!(
                    matches!(error, VoiceError::Incompatible(_) | VoiceError::Install(_)),
                    "{error:?}"
                );
                assert_eq!(std::fs::read(&self.engine.path).unwrap(), self.published);
                let manifest = self.installer.manifest().await.unwrap();
                let engine = manifest
                    .components
                    .iter()
                    .find(|component| component.kind == "engine")
                    .expect("the published engine is still recorded");
                assert_eq!(engine.sha256, self.engine.sha256);
                self.installer
                    .verify(&self.engine)
                    .await
                    .expect("the previous engine still answers to its manifest entry");
                assert!(!self.installer.staged_engine_path(&self.asset).exists());
                assert!(!self.backup().exists());
                assert!(
                    !self.handoff_dir().exists(),
                    "a refused handoff is consumed"
                );
                assert!(!self.installer.local_engine_pending().await);
            }
        }

        #[tokio::test]
        async fn an_engine_reporting_another_version_is_refused() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-voice", "9.9.9", 0, "other version");
            fixture.hand_off(THIS_RUNNER, &engine, &engine);
            let result = fixture.installer.install_engine(fixture.target).await;
            assert!(
                matches!(result, Err(VoiceError::Incompatible(_))),
                "{result:?}"
            );
            fixture.assert_refused_and_intact(result).await;
        }

        #[tokio::test]
        async fn an_engine_reporting_another_name_is_refused() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-other", VERSION, 0, "other name");
            fixture.hand_off(THIS_RUNNER, &engine, &engine);
            let result = fixture.installer.install_engine(fixture.target).await;
            assert!(
                matches!(result, Err(VoiceError::Incompatible(_))),
                "{result:?}"
            );
            fixture.assert_refused_and_intact(result).await;
        }

        #[tokio::test]
        async fn an_engine_whose_probe_fails_is_refused() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-voice", VERSION, 3, "failing probe");
            fixture.hand_off(THIS_RUNNER, &engine, &engine);
            let result = fixture.installer.install_engine(fixture.target).await;
            fixture.assert_refused_and_intact(result).await;
        }

        #[tokio::test]
        async fn an_engine_that_does_not_match_its_checksum_is_refused() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-voice", VERSION, 0, "tampered");
            fixture.hand_off(THIS_RUNNER, &engine, b"the bytes that were gated");
            let result = fixture.installer.install_engine(fixture.target).await;
            fixture.assert_refused_and_intact(result).await;
        }

        /// The engine moves into place before the manifest is written; a
        /// manifest that cannot be written must not leave the old manifest
        /// describing bytes that are no longer there.
        #[tokio::test]
        async fn a_manifest_that_cannot_be_written_keeps_the_previous_engine() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-voice", VERSION, 0, "built from this revision");
            fixture.hand_off(THIS_RUNNER, &engine, &engine);
            // `publish` writes the manifest through this temporary path; a
            // directory there fails that write after every earlier step passed.
            std::fs::create_dir(
                fixture
                    .directory
                    .path()
                    .join("voice-components.json.partial"),
            )
            .unwrap();
            let result = fixture.installer.install_engine(fixture.target).await;
            assert!(matches!(result, Err(VoiceError::Install(_))), "{result:?}");
            fixture.assert_refused_and_intact(result).await;
        }

        /// A caller can stop waiting at any moment: a cancelled file
        /// transcription drops the install future. Dropped at the commit's most
        /// delicate point, with the new bytes in place and the manifest still
        /// describing the old ones, the commit must run on to a coherent end.
        #[tokio::test]
        async fn a_cancelled_caller_cannot_interrupt_an_engine_commit() {
            let mut fixture = Fixture::new().await;
            let gate = Arc::new(CommitGate::default());
            fixture.installer.commit_gate = Some(gate.clone());
            let engine = engine_script("cairn-voice", VERSION, 0, "built from this revision");
            fixture.hand_off(THIS_RUNNER, &engine, &engine);

            {
                let install = fixture.installer.install_engine(fixture.target);
                tokio::pin!(install);
                tokio::select! {
                    result = &mut install => panic!("the commit must pause at the gate: {result:?}"),
                    () = gate.reached.notified() => {}
                }
                assert_eq!(
                    std::fs::read(&fixture.engine.path).unwrap(),
                    engine,
                    "paused after the new bytes took the destination"
                );
            }
            gate.release.notify_one();
            // The commit holds the lock until it settles.
            drop(fixture.installer.install_lock.lock().await);

            let recorded = fixture.recorded_engine().await;
            assert_eq!(recorded.sha256, format!("{:x}", Sha256::digest(&engine)));
            assert!(!fixture.backup().exists());
            assert!(!fixture
                .installer
                .staged_engine_path(&fixture.asset)
                .exists());
            assert!(
                !fixture.handoff_dir().exists(),
                "the commit consumes the handoff"
            );
        }

        /// A process that died between replacing the engine and recording it
        /// leaves the manifest describing the backup; the next install puts it
        /// back rather than calling the engine corrupt.
        #[tokio::test]
        async fn a_backup_the_manifest_still_describes_is_restored() {
            let fixture = Fixture::new().await;
            std::fs::hard_link(&fixture.engine.path, fixture.backup()).unwrap();
            let unrecorded = engine_script("cairn-voice", VERSION, 0, "never recorded");
            let staged = fixture.installer.staged_engine_path(&fixture.asset);
            std::fs::write(&staged, &unrecorded).unwrap();
            std::fs::rename(&staged, &fixture.engine.path).unwrap();

            let installed = fixture
                .installer
                .install_engine(fixture.target)
                .await
                .expect("the recorded engine is current again");

            assert_eq!(installed.sha256, fixture.engine.sha256);
            assert_eq!(
                std::fs::read(&fixture.engine.path).unwrap(),
                fixture.published
            );
            assert!(!fixture.backup().exists());
        }

        /// A backup nothing explains is evidence: it is set aside, not deleted.
        #[tokio::test]
        async fn an_unexplained_backup_is_set_aside_rather_than_deleted() {
            let fixture = Fixture::new().await;
            std::fs::write(fixture.backup(), b"bytes nothing records").unwrap();
            std::fs::write(&fixture.engine.path, b"bytes nothing records either").unwrap();

            // The engine itself is corrupt, so this goes on to a download that
            // cannot succeed here; the backup is settled first either way.
            let _ = fixture.installer.install_engine(fixture.target).await;

            assert!(!fixture.backup().exists());
            let orphans: Vec<_> = std::fs::read_dir(fixture.installer.engine_dir())
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|name| name.contains(".orphan-"))
                .collect();
            assert_eq!(orphans.len(), 1, "{orphans:?}");
            assert_eq!(
                std::fs::read(fixture.installer.engine_dir().join(&orphans[0])).unwrap(),
                b"bytes nothing records"
            );
        }

        /// A restored, older, or newer app must not adopt an engine built for
        /// a runner other than itself.
        #[tokio::test]
        async fn a_handoff_for_another_build_is_ignored_and_kept() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-voice", VERSION, 0, "another runner");
            fixture.hand_off("content-v2:another-runner", &engine, &engine);
            assert!(!fixture.installer.local_engine_pending().await);

            let installed = fixture
                .installer
                .install_engine(fixture.target)
                .await
                .expect("the current engine stands without a download");

            assert_eq!(installed.sha256, fixture.engine.sha256);
            assert_eq!(
                std::fs::read(&fixture.engine.path).unwrap(),
                fixture.published
            );
            assert!(
                fixture.handoff_dir().join(HANDOFF_FILE).exists(),
                "another build's handoff is left for the next self-rebuild to replace"
            );
        }

        #[tokio::test]
        async fn a_matching_handoff_replaces_the_engine_in_place() {
            let fixture = Fixture::new().await;
            let engine = engine_script("cairn-voice", VERSION, 0, "built from this revision");
            fixture.hand_off(THIS_RUNNER, &engine, &engine);
            assert!(fixture.installer.local_engine_pending().await);

            let installed = fixture
                .installer
                .install_engine(fixture.target)
                .await
                .expect("the handed-off engine is adopted");

            assert_eq!(installed.path, fixture.engine.path, "same asset, same path");
            assert_eq!(installed.id, fixture.asset);
            assert_eq!(installed.target.as_deref(), Some(fixture.target));
            assert_eq!(installed.size_bytes, engine.len() as u64);
            assert_eq!(installed.sha256, format!("{:x}", Sha256::digest(&engine)));
            assert_eq!(std::fs::read(&installed.path).unwrap(), engine);
            fixture
                .installer
                .verify(&installed)
                .await
                .expect("the recorded digest is the installed file's own");
            let manifest = fixture.installer.manifest().await.unwrap();
            assert!(manifest.components.iter().any(
                |component| component.kind == "engine" && component.sha256 == installed.sha256
            ));
            assert!(
                manifest
                    .components
                    .iter()
                    .any(|component| component.kind == "model:fast"
                        && component.sha256 == fixture.model.sha256),
                "adopting an engine never touches models"
            );
            assert!(
                !fixture.handoff_dir().exists(),
                "an adopted handoff is consumed"
            );
            assert!(!fixture
                .installer
                .staged_engine_path(&fixture.asset)
                .exists());
            assert!(!fixture.installer.local_engine_pending().await);
        }
    }
}
