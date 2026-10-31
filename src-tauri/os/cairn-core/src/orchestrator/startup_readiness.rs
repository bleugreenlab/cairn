//! The one readiness authority for a host's startup recovery.
//!
//! A runner bounce leaves the previous generation's streams, outbox, runs, and
//! waits for recovery to reconcile, and the agent callback gateway (`/api/mcp`)
//! refuses every call until that finishes. A turn that reaches a provider
//! process inside that window has no working verb, `waitFor` included, so every
//! cold agent launch is held in durable admission until the same latch opens.
//! The transport's 503 hold and the admission promoters read this one object,
//! so the two can never disagree about whether a turn could be served.
//!
//! The latch only ever moves from pending to ready, once per process. Hosts that
//! run no startup recovery (every test fixture, inert hosts) build it ready.

use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::watch;

#[derive(Clone)]
pub struct StartupReadiness {
    inner: Arc<Inner>,
}

struct Inner {
    ready: watch::Sender<bool>,
    /// Blocking-pool waiters (admission promotion runs off the async runtime,
    /// sometimes on a thread with no runtime at all) park here rather than on
    /// the watch channel.
    blocking: Mutex<()>,
    opened: Condvar,
}

impl Default for StartupReadiness {
    fn default() -> Self {
        Self::ready()
    }
}

impl std::fmt::Debug for StartupReadiness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartupReadiness")
            .field("ready", &self.is_ready())
            .finish()
    }
}

impl StartupReadiness {
    fn with(ready: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                ready: watch::Sender::new(ready),
                blocking: Mutex::new(()),
                opened: Condvar::new(),
            }),
        }
    }

    /// A latch for a host whose startup recovery has not finished.
    pub fn pending() -> Self {
        Self::with(false)
    }

    /// A latch for a host with no startup recovery to wait on.
    pub fn ready() -> Self {
        Self::with(true)
    }

    /// Open the latch. Returns `true` only for the call that opened it.
    pub fn mark_ready(&self) -> bool {
        let opened = !self.inner.ready.send_replace(true);
        // Taking the lock orders this wake after any blocking waiter's check of
        // the flag, so a waiter that read `false` is already parked on the
        // condvar and receives it.
        drop(
            self.inner
                .blocking
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        self.inner.opened.notify_all();
        opened
    }

    pub fn is_ready(&self) -> bool {
        *self.inner.ready.borrow()
    }

    /// Resolve once the latch is open.
    pub async fn wait_ready(&self) {
        let mut receiver = self.inner.ready.subscribe();
        // The sender lives in `inner`, which `self` keeps alive, so the channel
        // cannot close under this wait.
        let _ = receiver.wait_for(|ready| *ready).await;
    }

    /// Block the calling thread until the latch is open. For the blocking pool
    /// and plain OS threads only, never an async worker.
    pub fn wait_ready_blocking(&self) {
        let mut guard = self
            .inner
            .blocking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !self.is_ready() {
            guard = self
                .inner
                .opened
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_default_latch_is_ready() {
        assert!(StartupReadiness::default().is_ready());
        assert!(!StartupReadiness::pending().is_ready());
    }

    #[test]
    fn marking_ready_opens_the_latch_once_for_every_clone() {
        let latch = StartupReadiness::pending();
        let clone = latch.clone();
        assert!(clone.mark_ready());
        assert!(latch.is_ready());
        assert!(!latch.mark_ready(), "only the opening call reports it");
    }

    #[tokio::test]
    async fn waiters_resolve_whether_they_subscribed_before_or_after_the_flip() {
        let latch = StartupReadiness::pending();
        let early = tokio::spawn({
            let latch = latch.clone();
            async move { latch.wait_ready().await }
        });
        tokio::task::yield_now().await;
        assert!(!early.is_finished());
        latch.mark_ready();
        tokio::time::timeout(Duration::from_secs(5), early)
            .await
            .expect("an early waiter resolves on the flip")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), latch.wait_ready())
            .await
            .expect("a late waiter resolves at once");
    }

    /// `source` without its top-level inline test modules. Each
    /// `#[cfg(test)] mod x {` block is dropped through its closing brace, which
    /// rustfmt places alone at column 0; production code between and after such
    /// blocks is kept.
    fn strip_test_modules(source: &str) -> String {
        let mut kept = String::new();
        let mut lines = source.lines().peekable();
        while let Some(line) = lines.next() {
            let opens_test_module = line == "#[cfg(test)]"
                && lines
                    .peek()
                    .is_some_and(|next| next.starts_with("mod ") && next.trim_end().ends_with('{'));
            if opens_test_module {
                for skipped in lines.by_ref() {
                    if skipped == "}" {
                        break;
                    }
                }
                continue;
            }
            kept.push_str(line);
            kept.push('\n');
        }
        kept
    }

    /// Production source of every non-test file under `src/`, keyed by path
    /// relative to it. Files that are test modules in their own right are
    /// skipped.
    fn production_sources() -> Vec<(String, String)> {
        fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, String)>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, root, out);
                    continue;
                }
                let name = path.file_name().unwrap().to_string_lossy();
                if !name.ends_with(".rs") || name.ends_with("tests.rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).unwrap();
                let production = strip_test_modules(&source);
                let relative = path.strip_prefix(root).unwrap().to_string_lossy();
                out.push((relative.replace('\\', "/"), production));
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut out = Vec::new();
        walk(&root, &root, &mut out);
        out
    }

    fn files_calling(sources: &[(String, String)], call: &str, except: &str) -> Vec<String> {
        let mut files: Vec<String> = sources
            .iter()
            .filter(|(_, source)| source.replace(except, "").contains(call))
            .map(|(path, _)| path.clone())
            .collect();
        files.sort();
        files
    }

    /// cairn/4758: a provider process exists only if one of these few paths
    /// spawned it, and each is behind the readiness latch: ordinary launches
    /// through admission's promoters or the stateless deferral, calls through
    /// the call promoter or theirs. The process registry starts empty on every
    /// boot and only a backend's own `start_session` fills it, so no warm
    /// process can carry a turn before readiness either. A new path that spawns
    /// or registers a process fails here rather than resurfacing as a session
    /// whose every callback is refused.
    #[test]
    fn every_process_spawn_goes_through_a_readiness_gated_funnel() {
        let sources = production_sources();
        assert_eq!(
            files_calling(&sources, "start_agent_session(", "fn start_agent_session("),
            ["execution/jobs/calls.rs", "execution/jobs/mod.rs"],
            "start_ordinary_session and start_call_run_now are the only spawn entries"
        );
        // `start_ordinary_session` itself has no readiness check; its callers
        // (the stateless start and the leased start) sit behind the gate.
        assert_eq!(
            files_calling(
                &sources,
                "start_ordinary_session(",
                "fn start_ordinary_session("
            ),
            [
                "execution/jobs/mod.rs",
                "execution/jobs/ordinary_admission.rs"
            ]
        );
        assert_eq!(
            files_calling(&sources, "launch::start_session(", "fn start_session("),
            ["orchestrator/session.rs"]
        );
        assert_eq!(
            files_calling(&sources, ".start_session(", "fn start_session("),
            ["backends/launch.rs"]
        );
        assert_eq!(
            files_calling(&sources, "register_process(", "fn register_process("),
            [
                "backends/claude.rs",
                "backends/codex/backend_impl.rs",
                "backends/http_loop/mod.rs"
            ]
        );
    }

    #[test]
    fn a_blocking_waiter_wakes_on_the_flip() {
        let latch = StartupReadiness::pending();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn({
            let latch = latch.clone();
            move || {
                latch.wait_ready_blocking();
                tx.send(()).unwrap();
            }
        });
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        latch.mark_ready();
        rx.recv_timeout(Duration::from_secs(5))
            .expect("the blocking waiter wakes");
        waiter.join().unwrap();
        latch.wait_ready_blocking();
    }
}
