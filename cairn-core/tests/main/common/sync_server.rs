//! Shared `tursodb --sync-server` harness for the Turso Sync integration tests.
//!
//! Extracted from `turso_sync_roundtrip.rs` so the team-sync loop tests reuse
//! it. Honors `CAIRN_TEST_SYNC_URL` first; otherwise spawns a `tursodb` from
//! PATH and tears it down on drop. A process we own can be stopped and restarted
//! on the same address and backing file — the transient-outage test needs that.

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
    /// Returns `None` (meaning "skip the test") when no server is reachable: no
    /// `CAIRN_TEST_SYNC_URL` set and no `tursodb` on PATH.
    ///
    /// In the unfenced sync lane (`CAIRN_REQUIRE_SYNC_TESTS=1`) a `None` is a
    /// HARD FAILURE instead: that lane installs the pinned `tursodb` precisely so
    /// these tests run for real, so an unreachable server means the lane is
    /// misconfigured, not that the test may be skipped. (CAIRN-2170.)
    pub fn locate_or_spawn() -> Option<Self> {
        let server = Self::try_locate_or_spawn();
        assert!(
            server.is_some() || !super::sync_tests_required(),
            "CAIRN_REQUIRE_SYNC_TESTS is set but no sync server is available: install the \
             pinned `tursodb` (turso rev 496c24e / 0.7.0-pre.10) on PATH and run UNFENCED. \
             A skip is NOT a pass in the unfenced sync lane (CAIRN-2170)."
        );
        server
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
    Command::new("tursodb")
        .arg(db_path)
        .arg("--sync-server")
        .arg(addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

pub fn tursodb_present() -> bool {
    Command::new("tursodb")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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
