//! Shared `tursodb --sync-server` harness for the Turso Sync integration tests.
//!
//! Extracted from `turso_sync_roundtrip.rs` so the team-sync loop tests reuse
//! it. Honors `CAIRN_TEST_SYNC_URL` first; otherwise spawns the pinned `tursodb`
//! ([`tursodb_bin`]) and tears it down on drop. A process we own can be stopped
//! and restarted on the same address and backing file — the transient-outage
//! test needs that.
//!
//! Spawning `tursodb`, binding loopback, and writing temp files are all permitted
//! inside the worktree fence, so these suites run in every lane — provided the
//! binary exists at all, which is what `scripts/ensure-tursodb.ts` guarantees
//! (CAIRN-3300: nothing used to provision it, so these suites were permanently
//! red on a lane where no one had hand-installed it). An unreachable server is
//! therefore a broken environment rather than a test to skip, and
//! [`SyncServer::require`] fails instead of returning green.

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use cairn_core::internal::storage::{LocalDb, MigrationRunner, TEAM_MIGRATIONS};
use tempfile::{tempdir, TempDir};

/// A sync server endpoint for a test: either an externally provided URL
/// (`CAIRN_TEST_SYNC_URL`) or a `tursodb --sync-server` subprocess we own and
/// tear down on drop.
pub struct SyncServer {
    url: String,
    /// `Some` only for a process we spawned (and so can stop/restart).
    addr: Option<String>,
    db_path: Option<PathBuf>,
    child: Option<Child>,
    _dir: Option<TempDir>,
}

impl SyncServer {
    /// A sync server for `test`. Panics when none is reachable, because every
    /// Cairn lane has `tursodb` on PATH and the fence permits spawning it — so an
    /// unreachable server means the environment is broken, not that this test may
    /// quietly pass. (CAIRN-2170 is exactly how a sync misdiagnosis stood
    /// unrefuted behind such a green; CAIRN-3164 inverted the default.)
    ///
    /// `None` only on a machine that genuinely has no `tursodb` AND declares it
    /// for this run with `CAIRN_SYNC_TESTS_OPTIONAL=1`, which records a declared
    /// skip the runner permits for that run alone.
    pub fn require(test: &str) -> Option<Self> {
        if let Some(server) = Self::try_locate_or_spawn() {
            return Some(server);
        }
        assert!(
            sync_tests_optional(),
            "{test}: no sync server is reachable. These tests need the pinned `tursodb` \
             (built from the turso rev `src-tauri/os/cairn-db/Cargo.toml` pins), or a \
             `CAIRN_TEST_SYNC_URL`. Run `bun run ensure-tursodb` to build and cache it — \
             that is what `test:rust` does, and it works offline from the turso checkout \
             the workspace build already put in CARGO_HOME. Spawning it and binding \
             loopback are both permitted inside the worktree fence, so this suite runs in \
             every lane and a missing server is a broken environment — not a test to \
             skip. On a machine that genuinely cannot build it, declare that for this run \
             with CAIRN_SYNC_TESTS_OPTIONAL=1."
        );
        eprintln!("skipping {test}: no sync server, declared via CAIRN_SYNC_TESTS_OPTIONAL");
        super::record_skip(test, "sync-server-unavailable");
        None
    }

    /// A sync server this process OWNS, so it can be stopped and restarted to
    /// simulate an outage. `None` — with a declared skip — only when the operator
    /// pointed the suite at an external `CAIRN_TEST_SYNC_URL` we may not kill.
    pub fn require_owned(test: &str) -> Option<Self> {
        let server = Self::require(test)?;
        if server.is_owned() {
            return Some(server);
        }
        eprintln!(
            "skipping {test}: an external CAIRN_TEST_SYNC_URL cannot be stopped and restarted"
        );
        super::record_skip(test, "external-sync-server");
        None
    }

    fn try_locate_or_spawn() -> Option<Self> {
        if let Ok(url) = std::env::var("CAIRN_TEST_SYNC_URL") {
            if !url.is_empty() {
                return Some(Self {
                    url,
                    addr: None,
                    db_path: None,
                    child: None,
                    _dir: None,
                });
            }
        }
        if !tursodb_present() {
            return None;
        }
        let dir = tempdir().ok()?;
        let db_path = dir.path().join("sync-server.db");
        let port = free_port()?;
        let addr = format!("127.0.0.1:{port}");
        let child = spawn_tursodb(&db_path, &addr)?;
        wait_until_listening(&addr)?;
        Some(Self {
            url: format!("http://{addr}"),
            addr: Some(addr),
            db_path: Some(db_path),
            child: Some(child),
            _dir: Some(dir),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Whether this is a process we own (vs an external `CAIRN_TEST_SYNC_URL`).
    /// Only an owned server can be stopped and restarted.
    pub fn is_owned(&self) -> bool {
        self.addr.is_some()
    }

    /// Kill the owned `tursodb` process, simulating a sync-server outage. The
    /// backing DB file in the temp dir is preserved, so `restart` reattaches the
    /// same state. No-op for an external server.
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Respawn `tursodb` on the same address and backing DB file. Returns `false`
    /// if this server is not owned or the respawn/listen failed.
    pub fn restart(&mut self) -> bool {
        let (Some(addr), Some(db_path)) = (self.addr.clone(), self.db_path.clone()) else {
            return false;
        };
        self.stop();
        match spawn_tursodb(&db_path, &addr) {
            Some(child) => {
                self.child = Some(child);
                wait_until_listening(&addr).is_some()
            }
            None => false,
        }
    }
}

impl Drop for SyncServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_tursodb(db_path: &Path, addr: &str) -> Option<Child> {
    Command::new(tursodb_bin()?)
        .arg(db_path)
        .arg("--sync-server")
        .arg(addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

/// Whether the operator declared, for this run, that this machine has no
/// `tursodb` and the server-backed suites may record a skip instead of failing.
fn sync_tests_optional() -> bool {
    std::env::var_os("CAIRN_SYNC_TESTS_OPTIONAL").is_some()
}

/// Whether a team replica's schema can be established at all, probed by doing
/// exactly what every team-backed test does first: open a synced replica, run
/// `TEAM_MIGRATIONS`, and push the result to the server.
///
/// It currently CANNOT (CAIRN-3178). Note WHERE it fails, because it is not where
/// you would guess: the local apply SUCCEEDS — the plain engine runs statement by
/// statement — and `push` is what fails, replaying `turso_migrations/0118`'s
/// rename-then-copy to the server as one batch whose table names are resolved up
/// front. So the probe MUST include the push. A migrations-only probe reports the
/// schema as fine and every test then fails individually, which is exactly how
/// this helper was wrong on its first attempt.
///
/// The answer is a property of the migration set rather than of any one server,
/// so it is resolved once per test binary and shared.
async fn team_schema_available(url: &str) -> bool {
    static AVAILABLE: tokio::sync::OnceCell<bool> = tokio::sync::OnceCell::const_new();
    *AVAILABLE
        .get_or_init(|| async {
            let Ok(dir) = tempdir() else {
                return false;
            };
            let Ok(db) =
                LocalDb::open_synced(dir.path().join("team-schema-probe.db"), url, None).await
            else {
                return false;
            };
            if MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
                .run(&db)
                .await
                .is_err()
            {
                return false;
            }
            db.push().await.is_ok()
        })
        .await
}

/// Record a DECLARED skip and return `true` when a team replica's schema cannot
/// be established. This is a debt with an owner, not a silence: the skip is
/// counted in the verdict, `src-tauri/skip-manifest.toml` names CAIRN-3178 as the
/// issue that owes its removal, and the probe is a measured fact about the
/// product rather than a guess about the environment — so the day 0118 is fixed,
/// these 21 tests start running again on their own and the manifest entries
/// surface as retired.
pub async fn skip_if_team_schema_unavailable(test: &str, url: &str) -> bool {
    if team_schema_available(url).await {
        return false;
    }
    eprintln!("skipping {test}: a team replica's schema cannot be established (CAIRN-3178)");
    super::record_skip(test, "team-schema-replay");
    true
}

/// The database crate manifest that declares the `turso` git pin, baked in at compile
/// time. It is the same file `scripts/ensure-tursodb.ts` reads, so the revision
/// this harness DEMANDS and the revision the provisioner BUILDS cannot drift.
const DATABASE_MANIFEST: &str = include_str!("../../../../cairn-db/Cargo.toml");

/// The 40-hex `turso` git revision the workspace pins.
fn parse_pinned_rev(manifest: &str) -> Option<&str> {
    manifest
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("turso ") || line.starts_with("turso="))
        .filter(|line| line.contains("git ="))
        .find_map(|line| {
            let rev = line.split_once("rev = \"")?.1.split_once('"')?.0;
            (rev.len() == 40 && rev.chars().all(|c| c.is_ascii_hexdigit())).then_some(rev)
        })
}

/// Roots of the machine-level tool cache `scripts/ensure-tursodb.ts` populates:
/// the executor's (`CAIRN_TOOL_CACHE`), then `tool-cache` under `CAIRN_HOME` and
/// under the default `~/.cairn`. All are checked because a run may set
/// `CAIRN_HOME` elsewhere while the cache was built against the default home.
///
/// Machine-level rather than per-worktree on purpose: building tursodb from the
/// pinned turso revision costs minutes, and every build slot would repeat it.
fn cached_tursodb_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(cache) = std::env::var_os("CAIRN_TOOL_CACHE") {
        roots.push(PathBuf::from(cache));
    }
    if let Some(home) = std::env::var_os("CAIRN_HOME") {
        roots.push(PathBuf::from(home).join("tool-cache"));
    }
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".cairn").join("tool-cache"));
    }
    roots.into_iter().map(|root| root.join("tursodb")).collect()
}

fn tursodb_exe() -> &'static str {
    if cfg!(windows) {
        "tursodb.exe"
    } else {
        "tursodb"
    }
}

/// The binary a cache root holds for the pinned revision, or `None` when the pin
/// cannot be parsed.
///
/// The provisioner keys each build by the revision it came from
/// (`<root>/<rev>/bin/tursodb`) and publishes a revision's directory whole, so
/// looking up the pinned key IS the revision check, and running the binary is no
/// substitute for it: `tursodb --version` reports the release line
/// (`0.7.0-pre.10`), which is IDENTICAL across every git revision sharing it. So
/// [`runs`] proves a cached binary WORKS, never that it matches the pin. Another
/// revision's binary is exactly the silent ABI mismatch this design exists to
/// prevent (CAIRN-2147), so it is never resolved — and with no parseable pin no
/// cached binary is, because an unverifiable cache earns no trust.
fn pinned_bin_in(root: &Path, pinned: Option<&str>) -> Option<PathBuf> {
    Some(root.join(pinned?).join("bin").join(tursodb_exe()))
}

/// The revisions a cache root holds builds of, for a report that explains why
/// none of them was used. Staging directories are dot-prefixed and skipped.
fn cached_revisions(root: &Path) -> Vec<String> {
    let mut revisions: Vec<String> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| !name.starts_with('.'))
        .collect();
    revisions.sort();
    revisions
}

/// Whether `bin` is a working `tursodb`, proved by RUNNING it rather than by
/// testing for a file. A half-written cache entry or a wrong-architecture binary
/// is worse than an absent one: it would fail deep inside a sync test instead of
/// at resolution, where the report can name it.
fn runs(bin: &Path) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn resolve_tursodb() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("CAIRN_TURSODB_BIN") {
        let explicit = PathBuf::from(explicit);
        if runs(&explicit) {
            return Some(explicit);
        }
    }
    let pinned = parse_pinned_rev(DATABASE_MANIFEST);
    for root in cached_tursodb_roots() {
        if let Some(bin) = pinned_bin_in(&root, pinned).filter(|bin| runs(bin)) {
            return Some(bin);
        }
        // Say so rather than silently falling through: a cache that exists but
        // holds only other revisions is the case a reader most needs explained.
        let held = cached_revisions(&root);
        if !held.is_empty() {
            eprintln!(
                "ignoring cached tursodb under {}: it holds {}, workspace pins {} — run `bun run \
                 ensure-tursodb` to build it",
                root.display(),
                held.join(", "),
                pinned.unwrap_or("an unparseable revision"),
            );
        }
    }
    runs(Path::new("tursodb")).then(|| PathBuf::from("tursodb"))
}

/// The `tursodb` this test binary spawns, resolved once and reused.
///
/// Order: an explicit `CAIRN_TURSODB_BIN`, then the provisioned tool cache, then
/// PATH. Only the middle one is revision-verified, and that asymmetry is the
/// point: the cache is ours to guarantee, so it must prove it matches the pin
/// ([`cache_is_current`]) before it is preferred. The other two are a
/// developer's explicit choice of binary, accepted as given — an override that
/// was silently second-guessed would be useless, and PATH was the only source
/// this harness had before the cache existed.
///
/// Resolving explicitly rather than leaning on PATH is what lets a `cargo test`
/// find a provisioned binary in any shell, including one whose PATH has no
/// tursodb. It does NOT make the suite self-provisioning: a lane that has never
/// run `bun run ensure-tursodb` has nothing to resolve, and
/// [`SyncServer::require`] fails naming that command.
pub fn tursodb_bin() -> Option<&'static Path> {
    static RESOLVED: OnceLock<Option<PathBuf>> = OnceLock::new();
    RESOLVED.get_or_init(resolve_tursodb).as_deref()
}

pub fn tursodb_present() -> bool {
    tursodb_bin().is_some()
}

pub fn free_port() -> Option<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").ok()?;
    let port = listener.local_addr().ok()?.port();
    drop(listener);
    Some(port)
}

pub fn wait_until_listening(addr: &str) -> Option<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return Some(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

/// Guards the revision check that keeps a stale machine-wide cache from feeding
/// this suite a wrong-ABI `tursodb`. The cache outlives any one worktree, so
/// after a pin bump the previous revision's binary is still sitting there,
/// working and reporting the same `--version` — undetectable without its key.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_database_manifest_declares_a_parseable_turso_pin() {
        // If this fails, every cached binary is refused as unverifiable and the
        // suite falls back to PATH — so the parser breaking must be loud here
        // rather than quietly degrading resolution everywhere else.
        let rev = parse_pinned_rev(DATABASE_MANIFEST)
            .expect("src-tauri/os/cairn-db/Cargo.toml should declare a `turso` git rev pin");
        assert_eq!(rev.len(), 40);
        assert!(rev.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn parsing_ignores_a_turso_dependency_with_no_git_pin() {
        assert_eq!(parse_pinned_rev("turso = \"0.7\"\n"), None);
        assert_eq!(parse_pinned_rev("[dependencies]\nserde = \"1\"\n"), None);
    }

    #[test]
    fn parsing_rejects_a_rev_that_is_not_a_full_sha() {
        let manifest = "turso = { git = \"https://x/turso\", rev = \"496c24e\" }\n";
        assert_eq!(parse_pinned_rev(manifest), None);
    }

    #[test]
    fn the_pinned_revision_is_looked_up_under_its_own_key() {
        let pinned = "a".repeat(40);
        assert_eq!(
            pinned_bin_in(Path::new("/cache/tursodb"), Some(&pinned)),
            Some(
                Path::new("/cache/tursodb")
                    .join(&pinned)
                    .join("bin")
                    .join(tursodb_exe())
            )
        );
    }

    #[test]
    fn another_revisions_binary_is_never_resolved_for_the_pin() {
        // The pin-bump case: the old binary runs and reports the same release
        // line, so only its key distinguishes it.
        let root = tempdir().unwrap();
        let stale = root
            .path()
            .join("a".repeat(40))
            .join("bin")
            .join(tursodb_exe());
        std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
        std::fs::write(&stale, "").unwrap();
        let resolved = pinned_bin_in(root.path(), Some(&"b".repeat(40))).unwrap();
        assert_ne!(resolved, stale);
        assert!(!resolved.exists());
    }

    #[test]
    fn no_cached_binary_is_resolved_when_the_pin_cannot_be_parsed() {
        assert_eq!(pinned_bin_in(Path::new("/cache/tursodb"), None), None);
    }

    #[test]
    fn the_report_lists_published_revisions_and_skips_staging() {
        let root = tempdir().unwrap();
        assert!(cached_revisions(root.path()).is_empty());
        for name in ["b".repeat(40), "a".repeat(40), ".staging-x-1".into()] {
            std::fs::create_dir(root.path().join(name)).unwrap();
        }
        assert_eq!(
            cached_revisions(root.path()),
            vec!["a".repeat(40), "b".repeat(40)]
        );
    }
}
