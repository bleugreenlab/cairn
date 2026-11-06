//! The engine, once per artifact kind: every test that installs runs against
//! both the Codex spec (an unpacked archive) and the Claude spec (a bare
//! executable), with packages served from memory.

use super::claude::{CLAUDE, CLAUDE_TARGETS};
use super::codex::{CODEX, CODEX_TARGETS};
use super::*;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Condvar;

const SPECS: [&ToolSpec; 2] = [&CODEX, &CLAUDE];

const RETRY_TEXT: &str = "It tries again when its pin changes, when you choose Check again in onboarding, and at every restart.";

/// With one worker, a plain spawn-and-join from a runtime task would park
/// the only worker with its core, and no timer or other task would run
/// until the download returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_download_from_a_worker_leaves_the_runtime_running() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = tokio::spawn({
        let ticks = ticks.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                ticks.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    // Spawned, not awaited in the test body: the body runs inside
    // `block_on`, not on the worker this needs to occupy.
    let ticked_during_call = tokio::spawn(async move {
        off_runtime(move || {
            let before = ticks.load(Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(300));
            Ok::<_, String>(ticks.load(Ordering::SeqCst) - before)
        })
    })
    .await
    .unwrap()
    .unwrap();
    ticker.abort();
    assert!(
        ticked_during_call > 0,
        "the runtime's other tasks must keep running while a worker waits on a download"
    );
}

#[tokio::test]
async fn a_download_from_a_current_thread_runtime_joins_directly() {
    assert_eq!(off_runtime(|| Ok::<_, String>(7)), Ok(7));
}

fn target(spec: &ToolSpec) -> &'static str {
    (spec.host_target)().unwrap_or(if spec.name == "codex" {
        CODEX_TARGETS[3]
    } else {
        CLAUDE_TARGETS[2]
    })
}

fn tar_gz(entries: &[(&str, &[u8], u32)]) -> Vec<u8> {
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    for (path, data, mode) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(*mode);
        builder.append_data(&mut header, path, *data).unwrap();
    }
    builder.into_inner().unwrap().finish().unwrap()
}

/// A package for `spec` whose CLI answers `--version` as `reports` would:
/// an OpenAI-shaped archive for Codex, a bare script for Claude.
fn package(spec: &ToolSpec, version: &str, reports: &str) -> Vec<u8> {
    if spec.name == "codex" {
        let manifest = serde_json::json!({
            "layoutVersion": 1,
            "version": version,
            "target": target(spec),
            "entrypoint": "bin/codex",
            "resourcesDir": "codex-resources",
            "pathDir": "codex-path",
        })
        .to_string();
        let script = format!("#!/bin/sh\necho 'codex-cli {reports}'\n");
        tar_gz(&[
            ("bin/codex", script.as_bytes(), 0o755),
            ("codex-package.json", manifest.as_bytes(), 0o644),
        ])
    } else {
        format!("#!/bin/sh\necho '{reports} (Claude Code)'\n").into_bytes()
    }
}

/// A managed Codex under `dir` whose active release's `bin/codex` runs
/// `script` (a `/bin/sh` body that must answer `--version` with
/// `version`), installed the way an operator's hand-dropped archive is.
#[cfg(unix)]
pub(crate) fn installed_script(dir: &Path, version: &str, script: &str) -> ManagedTool {
    let spec = &CODEX;
    let manifest = serde_json::json!({
        "version": version,
        "target": target(spec),
        "entrypoint": "bin/codex",
    })
    .to_string();
    let body = format!("#!/bin/sh\n{script}\n");
    let archive = tar_gz(&[
        ("bin/codex", body.as_bytes(), 0o755),
        ("codex-package.json", manifest.as_bytes(), 0o644),
    ]);
    let pin = pin_for(spec, version, &archive);
    let root = dir.join("tools").join("codex");
    let dropped = root
        .join("downloads")
        .join((spec.download_file_name)(target(spec), version));
    std::fs::create_dir_all(dropped.parent().unwrap()).unwrap();
    std::fs::write(&dropped, archive).unwrap();
    let managed = ManagedTool::new(
        spec,
        root,
        dir.join("bin"),
        Arc::new(RefusingSource),
        Arc::new(RefusingSource),
    )
    .with_target(Some(target(spec)))
    .with_embedded_pin(pin);
    managed.ensure().expect("the scripted Codex installs");
    managed
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn pin_for(spec: &ToolSpec, version: &str, archive: &[u8]) -> ToolPin {
    pin_with_digest(spec, version, &sha256(archive), archive.len() as u64)
}

/// A valid pin for `spec`: every target carries the same digest.
fn pin_with_digest(spec: &ToolSpec, version: &str, sha256: &str, size: u64) -> ToolPin {
    ToolPin {
        integration: spec.integration,
        version: version.to_string(),
        tag: (spec.name == "codex").then(|| format!("rust-v{version}")),
        reason: "test".to_string(),
        skip: Vec::new(),
        packages: spec
            .supported_targets
            .iter()
            .map(|target| {
                (
                    target.to_string(),
                    ToolPackage {
                        sha256: sha256.to_string(),
                        size,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    }
}

/// Holds every download at its midpoint until opened.
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, usize)>,
    changed: Condvar,
}

impl Gate {
    fn open(&self) {
        self.state.lock().unwrap().0 = true;
        self.changed.notify_all();
    }

    /// Block until a download is held at the gate.
    fn wait_for_a_download(&self) {
        let state = self.state.lock().unwrap();
        let (_state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |(_, held)| *held == 0)
            .unwrap();
        assert!(!timeout.timed_out(), "no download reached the gate");
    }

    fn pass(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 += 1;
        self.changed.notify_all();
        let _open = self.changed.wait_while(state, |(open, _)| !*open).unwrap();
    }
}

#[derive(Default)]
struct Served {
    archives: Mutex<HashMap<String, Vec<u8>>>,
    downloads: Mutex<Vec<String>>,
    /// Failures returned, in order, before anything is served.
    failures: Mutex<VecDeque<FetchError>>,
    gate: Mutex<Option<Arc<Gate>>>,
}

impl Served {
    fn serve(&self, spec: &ToolSpec, pin: &ToolPin, bytes: Vec<u8>) {
        self.archives
            .lock()
            .unwrap()
            .insert((spec.package_url)(pin, target(spec)), bytes);
    }
    fn fail_next(&self, failures: impl IntoIterator<Item = FetchError>) {
        self.failures.lock().unwrap().extend(failures);
    }
    fn gate(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    fn count(&self) -> usize {
        self.downloads.lock().unwrap().len()
    }
}

impl Fetch for Served {
    fn download(
        &self,
        url: &str,
        dest: &Path,
        max_bytes: u64,
        _stop: &Arc<AtomicBool>,
    ) -> Result<(), FetchError> {
        self.downloads.lock().unwrap().push(url.to_string());
        if let Some(failure) = self.failures.lock().unwrap().pop_front() {
            return Err(failure);
        }
        let bytes = self
            .archives
            .lock()
            .unwrap()
            .get(url)
            .cloned()
            .ok_or_else(|| FetchError::refused("HTTP status client error (404 Not Found)"))?;
        if bytes.len() as u64 > max_bytes {
            return Err(FetchError::refused(format!(
                "it is larger than the pinned {max_bytes} bytes"
            )));
        }
        let gate = self.gate.lock().unwrap().clone();
        let half = bytes.len() / 2;
        std::fs::write(dest, &bytes[..half]).map_err(|error| error.to_string())?;
        if let Some(gate) = gate {
            gate.pass();
        }
        std::fs::write(dest, bytes).map_err(|error| FetchError::refused(error.to_string()))
    }
}

enum Reply {
    Body(String),
    Unreachable,
}

struct Published {
    reply: Mutex<Reply>,
    etags: Mutex<Vec<Option<String>>>,
}

impl Published {
    fn new(reply: Reply) -> Arc<Self> {
        Arc::new(Self {
            reply: Mutex::new(reply),
            etags: Mutex::new(Vec::new()),
        })
    }
    fn publish(&self, pin: &ToolPin) {
        *self.reply.lock().unwrap() = Reply::Body(serde_json::to_string(pin).unwrap());
    }
    fn go_offline(&self) {
        *self.reply.lock().unwrap() = Reply::Unreachable;
    }
}

impl PinSource for Published {
    fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String> {
        self.etags.lock().unwrap().push(etag.map(str::to_string));
        match &*self.reply.lock().unwrap() {
            Reply::Unreachable => Err("connection refused".to_string()),
            Reply::Body(text) => {
                // Always a fresh body; `a_cached_pin_is_revalidated_with_its_etag`
                // covers the not-modified answer.
                Ok(PinFetch::Body {
                    text: text.clone(),
                    etag: Some(format!("\"{}\"", sha256(text.as_bytes()))),
                })
            }
        }
    }
}

struct Fixture {
    spec: &'static ToolSpec,
    _dir: tempfile::TempDir,
    root: PathBuf,
    bin: PathBuf,
    served: Arc<Served>,
    published: Arc<Published>,
}

impl Fixture {
    fn new(spec: &'static ToolSpec) -> Self {
        let dir = tempfile::tempdir().unwrap();
        Self {
            spec,
            root: dir.path().join("tools").join(spec.name),
            bin: dir.path().join("bin"),
            _dir: dir,
            served: Arc::new(Served::default()),
            published: Published::new(Reply::Unreachable),
        }
    }

    fn managed(&self, embedded: &ToolPin) -> ManagedTool {
        ManagedTool::new(
            self.spec,
            &self.root,
            &self.bin,
            self.published.clone(),
            self.served.clone(),
        )
        .with_target(Some(target(self.spec)))
        .with_embedded_pin(embedded.clone())
        .with_backoff(vec![Duration::ZERO])
    }

    fn package(&self, version: &str, reports: &str) -> Vec<u8> {
        package(self.spec, version, reports)
    }

    fn pin_for(&self, version: &str, archive: &[u8]) -> ToolPin {
        pin_for(self.spec, version, archive)
    }

    fn pin_with_digest(&self, version: &str, sha256: &str, size: u64) -> ToolPin {
        pin_with_digest(self.spec, version, sha256, size)
    }

    fn serve(&self, pin: &ToolPin, bytes: Vec<u8>) {
        self.served.serve(self.spec, pin, bytes);
    }

    fn release(&self, version: &str) -> PathBuf {
        self.root.join(format!("{version}-{}", target(self.spec)))
    }

    fn archive_path(&self, version: &str) -> PathBuf {
        self.root
            .join("downloads")
            .join((self.spec.download_file_name)(target(self.spec), version))
    }

    fn shim(&self) -> PathBuf {
        self.bin.join(self.spec.name)
    }

    /// Everything under the root other than releases, `current.json`,
    /// `pin.json`, and an empty `downloads`.
    fn leftovers(&self) -> Vec<String> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                found.push(name);
            }
        }
        for entry in std::fs::read_dir(self.root.join("downloads"))
            .into_iter()
            .flatten()
            .flatten()
        {
            found.push(format!("downloads/{}", entry.file_name().to_string_lossy()));
        }
        found
    }
}

fn codex_route_text(fixture: &Fixture, pin: &ToolPin) -> String {
    let target = target(&CODEX);
    format!(
        "To install by hand, download https://github.com/openai/codex/releases/download/rust-v{v}/codex-package-{target}.tar.gz, confirm its SHA-256 is {sha}, and save it as {archive}.",
        v = pin.version,
        sha = pin.package(target).unwrap().sha256,
        archive = fixture.archive_path(&pin.version).display(),
    )
}

#[cfg(unix)]
#[test]
fn a_digest_mismatch_is_refused_and_installs_nothing() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let good = fixture.package("9.1.0", "9.1.0");
        let tampered = fixture.package("9.1.0", "6.6.6");
        // The two archives compress to lengths that differ by build (the
        // workspace can unify a different flate2 backend in), so the pinned
        // size covers both: the download completes and only the checksum can
        // refuse it.
        let size = good.len().max(tampered.len()) as u64;
        let pin = fixture.pin_with_digest("9.1.0", &sha256(&good), size);
        fixture.serve(&pin, tampered.clone());
        let managed = fixture.managed(&pin);

        let error = managed.ensure().unwrap_err();
        if spec.name == "codex" {
            assert_eq!(
                error.to_string(),
                format!(
                    "Cairn could not install its pinned Codex CLI 9.1.0: the download of codex-package-{target}.tar.gz did not match its pinned checksum (expected sha256 {e}…, got {a}…). Cairn does not fall back to a `codex` found on your PATH. {RETRY_TEXT} {route}",
                    target = target(spec),
                    e = &sha256(&good)[..12],
                    a = &sha256(&tampered)[..12],
                    route = codex_route_text(&fixture, &pin),
                )
            );
        } else {
            assert!(
                error.to_string().starts_with(&format!(
                    "Cairn could not install its pinned Claude Code CLI 9.1.0: the download of {}/claude did not match its pinned checksum",
                    target(spec)
                )),
                "{error}"
            );
            assert!(error
                .to_string()
                .contains("Cairn does not fall back to a `claude` found on your PATH."));
        }
        assert!(matches!(error.failure, InstallFailure::Integrity { .. }));
        assert!(!error.is_transient());
        assert!(!fixture.release("9.1.0").exists());
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
        assert!(std::fs::symlink_metadata(fixture.shim()).is_err());
        assert!(managed.installed().is_none());
        assert_eq!(
            managed.status(),
            ManagedToolStatus::Failed(error.to_string())
        );
    }
}

/// The pinned size bounds the download: a package past it is refused as a
/// failed download, even one whose checksum would have matched, and nothing
/// is installed.
#[test]
fn an_archive_larger_than_its_pinned_size_is_refused_and_installs_nothing() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "9.1.0");
        let cap = archive.len() as u64 - 1;
        let pin = fixture.pin_with_digest("9.1.0", &sha256(&archive), cap);
        fixture.serve(&pin, archive);
        let managed = fixture.managed(&pin);

        let error = managed.ensure().unwrap_err();
        if spec.name == "codex" {
            assert_eq!(
                error.to_string(),
                format!(
                    "Cairn could not install its pinned Codex CLI 9.1.0: the download of codex-package-{target}.tar.gz failed (it is larger than the pinned {cap} bytes). Cairn does not fall back to a `codex` found on your PATH. {RETRY_TEXT} {route}",
                    target = target(spec),
                    route = codex_route_text(&fixture, &pin),
                )
            );
        }
        assert!(matches!(
            error.failure,
            InstallFailure::Download {
                transient: false,
                ..
            }
        ));
        assert!(!fixture.release("9.1.0").exists());
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
        assert!(managed.installed().is_none());
    }
}

#[cfg(unix)]
#[test]
fn a_failed_update_keeps_the_working_release() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let first = fixture.package("9.1.0", "9.1.0");
        let first_pin = fixture.pin_for("9.1.0", &first);
        fixture.serve(&first_pin, first);
        let managed = fixture.managed(&first_pin);
        let installed = managed.ensure().unwrap();

        let next = fixture.pin_with_digest("9.2.0", &"0".repeat(64), 1 << 20);
        fixture.serve(&next, fixture.package("9.2.0", "9.2.0"));
        fixture.published.publish(&next);
        let error = managed.update().unwrap_err();
        assert!(matches!(error.failure, InstallFailure::Integrity { .. }));
        assert_eq!(managed.installed(), Some(installed.clone()));
        if spec.shim {
            assert_eq!(std::fs::read_link(fixture.shim()).unwrap(), installed.path);
        } else {
            assert!(std::fs::symlink_metadata(fixture.shim()).is_err());
        }
        assert!(!fixture.release("9.2.0").exists());
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
        assert!(matches!(
            managed.status(),
            ManagedToolStatus::Installed { .. }
        ));
    }
}

/// After an upgrade (or a rollback) that changes the integration, the
/// release the other build installed is never launched, even when the
/// install for this integration fails.
#[cfg(unix)]
#[test]
fn a_release_installed_for_another_integration_is_refused_not_launched() {
    for spec in SPECS {
        for other in [spec.integration + 1, spec.integration - 1] {
            let fixture = Fixture::new(spec);
            let archive = fixture.package("9.1.0", "9.1.0");
            let old_pin = fixture.pin_for("9.1.0", &archive);
            fixture.serve(&old_pin, archive);
            let installed = fixture.managed(&old_pin).ensure().unwrap();
            assert!(installed.path.is_file());
            // Rewrite the record as the other build left it.
            let mut current = fixture.managed(&old_pin).read_current().unwrap();
            current.integration = other;
            write_json_atomically(&fixture.root, "current.json", &current).unwrap();

            // This build's pin cannot be fetched.
            let new_pin = fixture.pin_with_digest("9.2.0", &"c".repeat(64), 100);
            let managed = fixture.managed(&new_pin);
            assert_eq!(managed.installed(), None, "integration {other}");
            let error = managed.ensure().unwrap_err();
            assert!(
                matches!(error.failure, InstallFailure::Download { .. }),
                "integration {other}: {error}"
            );
            assert_eq!(managed.update(), Err(error.clone()));
            assert_eq!(
                managed.status(),
                ManagedToolStatus::Failed(error.to_string()),
                "onboarding must not report the other integration's release as installed"
            );
        }
    }
}

#[test]
fn a_failed_download_is_refused_with_the_manual_route() {
    let fixture = Fixture::new(&CODEX);
    let pin = fixture.pin_with_digest("9.1.0", &"a".repeat(64), 100);
    let managed = fixture.managed(&pin);
    let error = managed.ensure().unwrap_err();
    assert_eq!(
        error.to_string(),
        format!(
            "Cairn could not install its pinned Codex CLI 9.1.0: the download of codex-package-{target}.tar.gz failed (HTTP status client error (404 Not Found)). Cairn does not fall back to a `codex` found on your PATH. {RETRY_TEXT} {route}",
            target = target(&CODEX),
            route = codex_route_text(&fixture, &pin),
        )
    );
    assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());

    let fixture = Fixture::new(&CLAUDE);
    let pin = fixture.pin_with_digest("9.1.0", &"a".repeat(64), 100);
    let error = fixture.managed(&pin).ensure().unwrap_err();
    let target = target(&CLAUDE);
    assert!(
        error.to_string().ends_with(&format!(
            "To install by hand, download https://downloads.claude.ai/claude-code-releases/9.1.0/{target}/claude, confirm its SHA-256 is {}, and save it as {}.",
            "a".repeat(64),
            fixture.archive_path("9.1.0").display()
        )),
        "{error}"
    );
}

#[test]
fn an_unsupported_platform_is_refused() {
    for (spec, text) in [
        (
            &CODEX,
            "Cairn runs its own pinned Codex CLI, and OpenAI publishes no Codex package for this platform",
        ),
        (
            &CLAUDE,
            "Cairn runs its own pinned Claude Code CLI, and Anthropic publishes no Claude Code package for this platform",
        ),
    ] {
        let fixture = Fixture::new(spec);
        let managed = fixture.managed(&spec.embedded()).with_target(None);
        assert!(!managed.supported());
        let error = managed.ensure().unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "{text} ({}/{}), so {} sessions cannot run here. Cairn does not fall back to a `{}` found on your PATH.",
                std::env::consts::OS,
                std::env::consts::ARCH,
                spec.display_name,
                spec.name,
            )
        );
        assert_eq!(fixture.served.count(), 0);
    }
}

#[cfg(unix)]
#[test]
fn a_cli_reporting_another_version_is_refused_and_removed() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "0.0.1");
        let pin = fixture.pin_for("9.1.0", &archive);
        fixture.serve(&pin, archive);
        let managed = fixture.managed(&pin);
        let error = managed.ensure().unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "Cairn could not install its pinned {} CLI 9.1.0: the installed `{} --version` reported 0.0.1, not 9.1.0. Cairn does not fall back to a `{}` found on your PATH. {RETRY_TEXT}",
                spec.display_name, spec.name, spec.name
            )
        );
        assert!(!fixture.release("9.1.0").exists());
        assert!(managed.installed().is_none());
    }
}

#[cfg(unix)]
#[test]
fn a_published_pin_beats_the_embedded_one_in_production() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let embedded = fixture.pin_for("9.1.0", &fixture.package("9.1.0", "9.1.0"));
        let archive = fixture.package("9.2.0", "9.2.0");
        let published = fixture.pin_for("9.2.0", &archive);
        fixture.serve(&published, archive);
        fixture.published.publish(&published);
        let managed = fixture.managed(&embedded);
        assert_eq!(managed.ensure().unwrap().version, "9.2.0");
        assert_eq!(
            managed.status(),
            ManagedToolStatus::Installed {
                version: "9.2.0".to_string(),
                pin_source: PinOrigin::Published
            }
        );
    }
}

#[test]
fn an_unreachable_publication_falls_back_to_the_cached_then_the_embedded_pin() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let embedded = fixture.pin_with_digest("9.1.0", &"a".repeat(64), 1);
        let published = fixture.pin_with_digest("9.2.0", &"b".repeat(64), 1);
        let managed = fixture.managed(&embedded);

        assert_eq!(
            managed.effective_pin(),
            (embedded.clone(), PinOrigin::Embedded)
        );
        fixture.published.publish(&published);
        assert_eq!(
            managed.effective_pin(),
            (published.clone(), PinOrigin::Published)
        );
        fixture.published.go_offline();
        assert_eq!(managed.effective_pin(), (published, PinOrigin::Cached));
    }
}

#[test]
fn a_cached_pin_is_revalidated_with_its_etag() {
    struct NotModified;
    impl PinSource for NotModified {
        fn fetch(&self, etag: Option<&str>) -> Result<PinFetch, String> {
            assert_eq!(etag, Some("\"v2\""));
            Ok(PinFetch::NotModified)
        }
    }
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let published = fixture.pin_with_digest("9.2.0", &"b".repeat(64), 1);
        write_json_atomically(
            &fixture.root,
            "pin.json",
            &CachedPin {
                etag: Some("\"v2\"".to_string()),
                pin: published.clone(),
            },
        )
        .unwrap();
        let managed = ManagedTool::new(
            spec,
            &fixture.root,
            &fixture.bin,
            Arc::new(NotModified),
            fixture.served.clone(),
        )
        .with_target(Some(target(spec)));
        assert_eq!(managed.effective_pin(), (published, PinOrigin::Published));
    }
}

#[test]
fn a_published_pin_for_another_integration_is_ignored() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let embedded = fixture.pin_with_digest("9.1.0", &"a".repeat(64), 1);
        let mut other = fixture.pin_with_digest("9.9.0", &"b".repeat(64), 1);
        other.integration = spec.integration + 1;
        fixture.published.publish(&other);
        let managed = fixture.managed(&embedded);
        assert_eq!(managed.effective_pin(), (embedded, PinOrigin::Embedded));
        assert!(!fixture.root.join("pin.json").exists());
    }
}

#[test]
fn outside_production_the_embedded_pin_wins() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let embedded = fixture.pin_with_digest("9.1.0", &"a".repeat(64), 1);
        fixture
            .published
            .publish(&fixture.pin_with_digest("9.2.0", &"b".repeat(64), 1));
        let managed = fixture.managed(&embedded).prefer_embedded(true);
        assert_eq!(managed.effective_pin(), (embedded, PinOrigin::Embedded));
        assert!(fixture.published.etags.lock().unwrap().is_empty());
    }
}

#[cfg(unix)]
#[test]
fn update_swaps_to_a_new_pin_beside_the_old_and_prunes_the_one_before() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let pins: Vec<ToolPin> = ["9.1.0", "9.2.0", "9.3.0"]
            .iter()
            .map(|version| {
                let archive = fixture.package(version, version);
                let pin = fixture.pin_for(version, &archive);
                fixture.serve(&pin, archive);
                pin
            })
            .collect();
        let managed = fixture.managed(&pins[0]);
        let first = managed.ensure().unwrap();
        assert_eq!(first.version, "9.1.0");

        fixture.published.publish(&pins[1]);
        let UpdateOutcome::Installed { resolved, previous } = managed.update().unwrap() else {
            panic!("9.2.0 is installed");
        };
        assert_eq!(resolved.version, "9.2.0");
        assert_eq!(previous.as_deref(), Some("9.1.0"));
        assert_eq!(managed.installed(), Some(resolved.clone()));
        if spec.shim {
            assert_eq!(std::fs::read_link(fixture.shim()).unwrap(), resolved.path);
        } else {
            assert!(
                std::fs::symlink_metadata(fixture.shim()).is_err(),
                "{} has no forwarder",
                spec.name
            );
        }
        assert!(first.path.is_file(), "the previous release is kept");

        let downloads = fixture.served.count();
        assert_eq!(
            managed.update().unwrap(),
            UpdateOutcome::Current(resolved.clone())
        );
        assert_eq!(
            fixture.served.count(),
            downloads,
            "no download for the same pin"
        );

        fixture.published.publish(&pins[2]);
        let UpdateOutcome::Installed { resolved, .. } = managed.update().unwrap() else {
            panic!("9.3.0 is installed");
        };
        assert_eq!(resolved.version, "9.3.0");
        assert!(
            fixture.release("9.2.0").is_dir(),
            "the previous release is kept"
        );
        assert!(
            !fixture.release("9.1.0").exists(),
            "older releases are pruned"
        );
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
    }
}

#[cfg(unix)]
#[test]
fn a_matching_archive_dropped_by_hand_installs_without_a_download() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "9.1.0");
        let pin = fixture.pin_for("9.1.0", &archive);
        let dropped = fixture.archive_path("9.1.0");
        std::fs::create_dir_all(dropped.parent().unwrap()).unwrap();
        std::fs::write(&dropped, archive).unwrap();
        let managed = fixture.managed(&pin);
        assert_eq!(managed.ensure().unwrap().version, "9.1.0");
        assert_eq!(fixture.served.count(), 0);
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
    }
}

/// The executable is moved into its release, made executable, and named by
/// the marker the engine writes beside it.
#[cfg(unix)]
#[test]
fn an_executable_release_is_the_binary_and_its_marker() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new(&CLAUDE);
    let binary = fixture.package("9.1.0", "9.1.0");
    let pin = fixture.pin_for("9.1.0", &binary);
    fixture.serve(&pin, binary.clone());
    let resolved = fixture.managed(&pin).ensure().unwrap();
    assert_eq!(resolved.path, fixture.release("9.1.0").join("claude"));
    assert_eq!(std::fs::read(&resolved.path).unwrap(), binary);
    let mode = std::fs::metadata(&resolved.path)
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o755);
    let marker: serde_json::Value = serde_json::from_slice(
        &std::fs::read(fixture.release("9.1.0").join(RELEASE_MARKER)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        marker,
        serde_json::json!({
            "version": "9.1.0",
            "target": target(&CLAUDE),
            "entrypoint": "claude",
            "sha256": sha256(&binary),
        })
    );
}

/// A release missing its marker, naming another version, or missing its
/// binary is not launched, and the next install replaces it.
#[cfg(unix)]
#[test]
fn an_incomplete_release_is_not_launched() {
    let fixture = Fixture::new(&CLAUDE);
    let binary = fixture.package("9.1.0", "9.1.0");
    let pin = fixture.pin_for("9.1.0", &binary);
    fixture.serve(&pin, binary);
    let managed = fixture.managed(&pin);
    let resolved = managed.ensure().unwrap();
    let marker = fixture.release("9.1.0").join(RELEASE_MARKER);
    let original = std::fs::read(&marker).unwrap();

    std::fs::remove_file(&resolved.path).unwrap();
    assert_eq!(managed.installed(), None, "no binary");
    std::fs::write(&resolved.path, b"x").unwrap();
    std::fs::write(
        &marker,
        String::from_utf8(original.clone())
            .unwrap()
            .replace("9.1.0", "9.0.0"),
    )
    .unwrap();
    assert_eq!(managed.installed(), None, "another version");
    std::fs::remove_file(&marker).unwrap();
    assert_eq!(managed.installed(), None, "no marker");

    let reinstalled = managed.ensure().unwrap();
    assert_eq!(reinstalled, resolved);
    assert_eq!(fixture.served.count(), 2);
}

#[test]
fn entries_that_escape_the_package_are_refused() {
    fn raw_entry(name: &[u8], kind: tar::EntryType, link: Option<&[u8]>) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        let mut header = tar::Header::new_gnu();
        let gnu = header.as_gnu_mut().unwrap();
        gnu.name[..name.len()].copy_from_slice(name);
        if let Some(link) = link {
            gnu.linkname[..link.len()].copy_from_slice(link);
        }
        header.set_entry_type(kind);
        header.set_size(if link.is_some() { 0 } else { 4 });
        header.set_mode(0o644);
        header.set_cksum();
        let data: &[u8] = if link.is_some() { b"" } else { b"evil" };
        builder.append(&header, data).unwrap();
        builder.into_inner().unwrap().finish().unwrap()
    }
    let cases = [
        raw_entry(b"../evil", tar::EntryType::Regular, None),
        raw_entry(b"/tmp/evil", tar::EntryType::Regular, None),
        raw_entry(b"bin/../../evil", tar::EntryType::Regular, None),
        raw_entry(b"bin/link", tar::EntryType::Symlink, Some(b"/etc/passwd")),
        raw_entry(b"bin/link", tar::EntryType::Symlink, Some(b"../../escape")),
    ];
    for (index, bytes) in cases.iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("a.tar.gz");
        std::fs::write(&archive, bytes).unwrap();
        let dest = dir.path().join("dest");
        std::fs::create_dir_all(&dest).unwrap();
        let error = unpack_archive(&archive, &dest).unwrap_err();
        assert!(
            error.contains("outside the package"),
            "case {index}: {error}"
        );
        assert!(!dir.path().join("evil").exists());
    }
}

#[test]
fn the_version_is_read_from_version_output() {
    assert_eq!(parse_cli_version("codex-cli 0.153.3\n"), Some("0.153.3"));
    assert_eq!(parse_cli_version("0.37.1"), Some("0.37.1"));
    assert_eq!(
        parse_cli_version("codex-cli 0.154.0-alpha.2"),
        Some("0.154.0-alpha.2")
    );
    assert_eq!(
        parse_cli_version("2.1.311 (Claude Code)\n"),
        Some("2.1.311")
    );
    assert_eq!(parse_cli_version("codex-cli"), None);
    assert_eq!(parse_cli_version(""), None);
}

/// A launch issued while the first install is mid-download waits on it,
/// is told once where it stands, and starts on the release it lands.
#[cfg(unix)]
#[test]
fn launch_during_staged_install_waits_then_starts() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "9.1.0");
        let size = archive.len() as u64;
        let pin = fixture.pin_for("9.1.0", &archive);
        fixture.serve(&pin, archive);
        let gate = fixture.served.gate();
        let managed = Arc::new(fixture.managed(&pin));

        let installer = std::thread::spawn({
            let managed = managed.clone();
            move || managed.ensure()
        });
        gate.wait_for_a_download();

        let told = Arc::new(Mutex::new(Vec::new()));
        let waiter = std::thread::spawn({
            let managed = managed.clone();
            let told = told.clone();
            move || managed.ensure_with(|progress| told.lock().unwrap().push(progress), &|| true)
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while told.lock().unwrap().is_empty() {
            assert!(
                Instant::now() < deadline,
                "the waiting launch was never told"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished(), "the launch waits on the install");
        let ManagedToolStatus::Installing {
            version,
            downloaded,
            total,
            last_error,
        } = managed.status()
        else {
            panic!("{}: an install is in progress", spec.name);
        };
        assert_eq!(version, "9.1.0");
        assert_eq!(total, size);
        assert_eq!(downloaded, size / 2);
        assert_eq!(last_error, None);

        gate.open();
        let resolved = waiter.join().unwrap().expect("the waiting launch starts");
        assert_eq!(resolved, installer.join().unwrap().unwrap());
        let told = told.lock().unwrap();
        assert_eq!(told.len(), 1, "told exactly once");
        assert_eq!(told[0].version, "9.1.0");
        assert_eq!(told[0].total, size);
        assert!(told[0].waiting_message().starts_with(&format!(
            "Waiting for Cairn's pinned {} 9.1.0",
            spec.display_name
        )));
        assert!(managed.state_guard().refused.is_none());
        assert_eq!(fixture.served.count(), 1, "one install, not two");
    }
}

/// A network fault is retried inside the install: the launch waiting on it
/// gets its CLI, and no error surfaces.
#[cfg(unix)]
#[test]
fn transient_failures_keep_waiters_waiting() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "9.1.0");
        let pin = fixture.pin_for("9.1.0", &archive);
        fixture.serve(&pin, archive);
        fixture.served.fail_next([
            FetchError::transient("error sending request: connection refused"),
            FetchError::transient("connection reset by peer"),
        ]);
        let managed = Arc::new(fixture.managed(&pin));
        let resolved = managed
            .ensure_with(|progress| assert_eq!(progress.last_error, None), &|| true)
            .expect("the install outlasts the network fault");
        assert_eq!(resolved.version, "9.1.0");
        assert_eq!(fixture.served.count(), 3);
        assert!(managed.state_guard().refused.is_none());
        assert!(fixture.leftovers().is_empty(), "{:?}", fixture.leftovers());
    }
}

/// A digest mismatch is refused once: the next launch for the same pin is
/// refused at once with the same typed error, without another download,
/// until onboarding asks again or the pin changes.
#[cfg(unix)]
#[test]
fn digest_mismatch_refuses_typed_once() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let good = fixture.package("9.1.0", "9.1.0");
        let tampered = fixture.package("9.1.0", "6.6.6");
        let size = good.len().max(tampered.len()) as u64;
        let pin = fixture.pin_with_digest("9.1.0", &sha256(&good), size);
        fixture.serve(&pin, tampered);
        let managed = Arc::new(fixture.managed(&pin));

        let first = managed.ensure().unwrap_err();
        assert!(matches!(first.failure, InstallFailure::Integrity { .. }));
        assert_eq!(fixture.served.count(), 1);
        let told = AtomicUsize::new(0);
        let again = managed
            .ensure_with(
                |_| {
                    told.fetch_add(1, Ordering::SeqCst);
                },
                &|| true,
            )
            .unwrap_err();
        assert_eq!(again, first, "the same typed refusal");
        assert!(again.to_string().contains("To install by hand"));
        assert_eq!(told.load(Ordering::SeqCst), 0, "a refusal does not wait");
        assert_eq!(managed.update(), Err(first.clone()), "the follow loop too");
        assert_eq!(fixture.served.count(), 1, "never fetched again");

        // Onboarding's Check again tries once more.
        assert_eq!(managed.retry_if_idle(), Some(Err(first.clone())));
        assert_eq!(fixture.served.count(), 2);

        // A new pin is a new attempt.
        let next = fixture.package("9.2.0", "9.2.0");
        let next_pin = fixture.pin_for("9.2.0", &next);
        fixture.serve(&next_pin, next);
        fixture.published.publish(&next_pin);
        assert!(matches!(
            managed.update(),
            Ok(UpdateOutcome::Installed { .. })
        ));
        assert_eq!(fixture.served.count(), 3);
        assert!(managed.state_guard().refused.is_none());
    }
}

/// A package saved by hand at the manual-install path after a refusal is
/// picked up by the next launch, with no download; one that does not match
/// leaves the refusal standing.
#[cfg(unix)]
#[test]
fn a_manual_drop_after_a_refusal_installs_without_a_download() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let good = fixture.package("9.1.0", "9.1.0");
        let tampered = fixture.package("9.1.0", "6.6.6");
        let size = good.len().max(tampered.len()) as u64;
        let mut padded = good.clone();
        if spec.name != "codex" {
            // A shell script tolerates trailing blank lines; pad both to the
            // pinned size so the drop is the size the pin names.
            padded.resize(size as usize, b'\n');
        }
        let pin = fixture.pin_with_digest("9.1.0", &sha256(&padded), padded.len() as u64);
        fixture.serve(&pin, {
            let mut bad = tampered.clone();
            bad.resize(padded.len(), b'\n');
            bad
        });
        let managed = fixture.managed(&pin);
        let refusal = managed.ensure().unwrap_err();
        assert_eq!(fixture.served.count(), 1);

        let dropped = fixture.archive_path("9.1.0");
        let mut wrong = tampered.clone();
        wrong.resize(padded.len(), b' ');
        std::fs::write(&dropped, &wrong).unwrap();
        assert_eq!(managed.ensure().unwrap_err(), refusal, "a wrong drop");

        std::fs::write(&dropped, &padded).unwrap();
        let modified = SystemTime::now() + Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&dropped)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        assert_eq!(managed.ensure().unwrap().version, "9.1.0");
        assert_eq!(fixture.served.count(), 1, "no download");
    }
}

/// A refused download (HTTP 404) is genuine: refused once, not retried.
#[test]
fn refused_download_is_genuine() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let pin = fixture.pin_with_digest("9.1.0", &"a".repeat(64), 100);
        let managed = fixture.managed(&pin);
        let error = managed.ensure().unwrap_err();
        assert!(
            matches!(
                &error.failure,
                InstallFailure::Download { transient: false, reason, .. } if reason.contains("404")
            ),
            "{error}"
        );
        assert!(!error.is_transient());
        assert_eq!(managed.ensure().unwrap_err(), error);
        assert_eq!(fixture.served.count(), 1);
        assert_eq!(
            managed.status(),
            ManagedToolStatus::Failed(error.to_string())
        );
    }
}

#[test]
fn the_waiting_message_says_where_the_install_stands() {
    let progress = |downloaded, last_error: Option<&str>| InstallProgress {
        tool: CLAUDE.label(),
        version: "2.1.311".to_string(),
        downloaded,
        total: 290_000_000,
        last_error: last_error.map(str::to_string),
    };
    assert_eq!(
        progress(120_000_000, None).waiting_message(),
        "Waiting for Cairn's pinned Claude Code 2.1.311 to finish installing (downloading, 120 of 290 MB). The session starts when it lands."
    );
    assert_eq!(
        progress(0, Some("connection refused")).waiting_message(),
        "Waiting for Cairn's pinned Claude Code 2.1.311 to finish installing (starting a 290 MB download). The download was interrupted (connection refused) and retries by itself. The session starts when it lands."
    );
}

/// A transient fault that never clears: the network is down for good.
struct Offline {
    attempts: AtomicUsize,
}

impl Fetch for Offline {
    fn download(
        &self,
        _url: &str,
        _dest: &Path,
        _max_bytes: u64,
        _stop: &Arc<AtomicBool>,
    ) -> Result<(), FetchError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(FetchError::transient(
            "error sending request: connection refused",
        ))
    }
}

/// An offline first install retries forever while the host lives, but a
/// shutdown ends it, and every launch waiting on it, at once; nothing is
/// recorded as a refusal, so the next process start tries again.
#[test]
fn shutdown_ends_an_offline_install_and_its_waiters() {
    for spec in SPECS {
        let dir = tempfile::tempdir().unwrap();
        let offline = Arc::new(Offline {
            attempts: AtomicUsize::new(0),
        });
        let pin = pin_with_digest(spec, "9.1.0", &"a".repeat(64), 100);
        let managed = Arc::new(
            ManagedTool::new(
                spec,
                dir.path().join("tools"),
                dir.path().join("bin"),
                Arc::new(RefusingSource),
                offline.clone(),
            )
            .with_target(Some(target(spec)))
            .with_embedded_pin(pin)
            // Long enough that only a shutdown can end the wait in time.
            .with_backoff(vec![Duration::from_secs(600)]),
        );
        let installer = std::thread::spawn({
            let managed = managed.clone();
            move || managed.update()
        });
        let waiter = std::thread::spawn({
            let managed = managed.clone();
            move || managed.ensure()
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while offline.attempts.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "the install never began");
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert!(!installer.is_finished() && !waiter.is_finished());

        let asked = Instant::now();
        managed.shutdown();
        let installed = installer.join().unwrap();
        let waited = waiter.join().unwrap();
        assert!(
            asked.elapsed() < Duration::from_secs(5),
            "{:?}",
            asked.elapsed()
        );
        assert!(installed.unwrap_err().is_cancelled());
        assert!(waited.unwrap_err().is_cancelled());
        assert!(managed.state_guard().refused.is_none());
        assert_eq!(managed.status(), ManagedToolStatus::NotInstalled);
        assert!(
            managed.ensure().unwrap_err().is_cancelled(),
            "no new attempt"
        );
    }
}

/// A launch stopped while it waits behind another's install detaches at
/// once; the install goes on and lands for everyone else.
#[cfg(unix)]
#[test]
fn a_waiter_no_longer_wanted_detaches_and_the_install_lands() {
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "9.1.0");
        let pin = fixture.pin_for("9.1.0", &archive);
        fixture.serve(&pin, archive);
        let gate = fixture.served.gate();
        let managed = Arc::new(fixture.managed(&pin));
        let installer = std::thread::spawn({
            let managed = managed.clone();
            move || managed.ensure()
        });
        gate.wait_for_a_download();

        let wanted = Arc::new(AtomicBool::new(true));
        let waiter = std::thread::spawn({
            let (managed, wanted) = (managed.clone(), wanted.clone());
            move || managed.ensure_with(|_| {}, &|| wanted.load(Ordering::SeqCst))
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(!waiter.is_finished());
        let stopped = Instant::now();
        wanted.store(false, Ordering::SeqCst);
        let detached = waiter.join().unwrap().unwrap_err();
        assert!(detached.is_cancelled(), "{detached}");
        assert!(stopped.elapsed() < Duration::from_secs(3));
        assert!(!installer.is_finished(), "the shared install goes on");

        gate.open();
        assert_eq!(installer.join().unwrap().unwrap().version, "9.1.0");
        assert!(managed.state_guard().refused.is_none());
    }
}

/// The published-pin lookup that opens an install is part of it: a launch
/// arriving during the lookup is told it waits, rather than blocking
/// silently through the whole install.
#[cfg(unix)]
#[test]
fn a_launch_during_the_pin_lookup_is_told_it_waits() {
    struct SlowPin {
        gate: Arc<Gate>,
    }
    impl PinSource for SlowPin {
        fn fetch(&self, _etag: Option<&str>) -> Result<PinFetch, String> {
            self.gate.pass();
            Err("unreachable".to_string())
        }
    }
    for spec in SPECS {
        let fixture = Fixture::new(spec);
        let archive = fixture.package("9.1.0", "9.1.0");
        let pin = fixture.pin_for("9.1.0", &archive);
        fixture.serve(&pin, archive);
        let gate = Arc::new(Gate::default());
        let managed = Arc::new(
            ManagedTool::new(
                spec,
                &fixture.root,
                &fixture.bin,
                Arc::new(SlowPin { gate: gate.clone() }),
                fixture.served.clone(),
            )
            .with_target(Some(target(spec)))
            .with_embedded_pin(pin)
            .with_backoff(vec![Duration::ZERO]),
        );
        let follower = std::thread::spawn({
            let managed = managed.clone();
            move || managed.update()
        });
        gate.wait_for_a_download();

        let told = Arc::new(AtomicUsize::new(0));
        let waiter = std::thread::spawn({
            let (managed, told) = (managed.clone(), told.clone());
            move || {
                managed.ensure_with(
                    |progress| {
                        assert_eq!(progress.version, "9.1.0");
                        told.fetch_add(1, Ordering::SeqCst);
                    },
                    &|| true,
                )
            }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while told.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "{}: a launch during the pin lookup was never told it waits",
                spec.name
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!waiter.is_finished());
        assert!(matches!(
            managed.status(),
            ManagedToolStatus::Installing { .. }
        ));
        gate.open();
        assert!(follower.join().unwrap().is_ok());
        assert_eq!(waiter.join().unwrap().unwrap().version, "9.1.0");
        assert_eq!(told.load(Ordering::SeqCst), 1);
    }
}

/// The launch that found nothing installed does not own the install it
/// started: stopped, it detaches at once, and the install goes on until the
/// network returns or the host shuts down.
#[test]
fn a_launch_that_started_the_install_can_still_detach() {
    for spec in SPECS {
        let dir = tempfile::tempdir().unwrap();
        let offline = Arc::new(Offline {
            attempts: AtomicUsize::new(0),
        });
        let pin = pin_with_digest(spec, "9.1.0", &"a".repeat(64), 100);
        let managed = Arc::new(
            ManagedTool::new(
                spec,
                dir.path().join("tools"),
                dir.path().join("bin"),
                Arc::new(RefusingSource),
                offline.clone(),
            )
            .with_target(Some(target(spec)))
            .with_embedded_pin(pin)
            .with_backoff(vec![Duration::from_millis(10)]),
        );
        let wanted = Arc::new(AtomicBool::new(true));
        let launch = std::thread::spawn({
            let (managed, wanted) = (managed.clone(), wanted.clone());
            move || managed.ensure_with(|_| {}, &|| wanted.load(Ordering::SeqCst))
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while offline.attempts.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "{}: no install began", spec.name);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!launch.is_finished());

        let stopped = Instant::now();
        wanted.store(false, Ordering::SeqCst);
        let detached = launch.join().unwrap().unwrap_err();
        assert!(detached.is_cancelled(), "{detached}");
        assert!(
            stopped.elapsed() < Duration::from_secs(3),
            "{:?}",
            stopped.elapsed()
        );

        let before = offline.attempts.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            offline.attempts.load(Ordering::SeqCst) > before,
            "the install goes on without the launch that started it"
        );
        assert!(matches!(
            managed.status(),
            ManagedToolStatus::Installing { .. }
        ));

        managed.shutdown();
        let deadline = Instant::now() + Duration::from_secs(5);
        while managed.status() != ManagedToolStatus::NotInstalled {
            assert!(
                Instant::now() < deadline,
                "the install never ended at shutdown"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
