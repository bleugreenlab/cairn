//! Per-worktree language-server pool with lazy spawn and idle eviction.
//!
//! Mirrors `orchestrator::build_services` in shape: pure free functions take a
//! `&dyn ProcessSpawner` and config so they are unit-testable without a full
//! `Orchestrator`, and the [`LspManager`] holds the live instances.
//!
//! ## Idle eviction (sweep-on-access)
//!
//! The primary mechanism is a lazy last-used + TTL sweep run at the top of
//! [`LspManager::get_or_spawn`] — no background thread. The orchestrator also
//! calls [`LspManager::collect_idle`] from the existing warm-process eviction
//! cadence (`collect_warm_if_needed`) as a best-effort backstop. There is no
//! second GC loop or timer.
//!
//! ## Sandbox gating
//!
//! Each server is confined exactly like a worktree run: writable to its root and
//! a dedicated cache dir, reads broad minus the deny list. When no OS sandbox is
//! available the manager returns [`Unavailable`] rather than spawning an
//! unconfined server (so there is no LSP on Windows in v1, matching build
//! services' posture).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::build_services::Templates;
use crate::config::language_servers::LanguageServerConfig;
use crate::services::sandbox::{self, SandboxPolicy};
use crate::services::{ProcessSpawner, SpawnConfig};

use super::client::LspClient;
use super::{InstanceKey, Unavailable};

/// How long an instance may sit unused before the access-time sweep evicts it.
pub const IDLE_TTL: Duration = Duration::from_secs(600);

/// One pooled, running language-server instance.
pub struct LspInstance {
    pub key: InstanceKey,
    pub client: LspClient,
    last_used: Mutex<Instant>,
}

impl LspInstance {
    fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_used.lock().unwrap().elapsed()
    }

    fn alive(&self) -> bool {
        self.client.is_alive()
    }
}

/// Runtime status of one pooled instance, for the Phase-4 settings surface.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LspInstanceStatus {
    pub language: String,
    pub root: String,
    /// Whether the `initialize` handshake completed.
    pub handshake_ok: bool,
    /// Whether the server has signaled indexing-complete.
    pub ready: bool,
}

/// The language-server pool.
#[derive(Default)]
pub struct LspManager {
    instances: Mutex<HashMap<InstanceKey, Arc<LspInstance>>>,
}

impl LspManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Structural availability gate, pure for testing. `Err` when the indexing
    /// root is missing or no OS sandbox is available.
    pub fn availability_gate(root: &Path, sandbox_available: bool) -> Result<(), Unavailable> {
        if !root.exists() {
            return Err(Unavailable::new(format!(
                "indexing root does not exist: {}",
                root.display()
            )));
        }
        if !sandbox_available {
            return Err(Unavailable::new(
                "OS sandbox unavailable; language servers run only confined",
            ));
        }
        Ok(())
    }

    /// Evict instances idle past [`IDLE_TTL`] or whose process has died.
    pub fn collect_idle(&self) {
        let mut map = self.instances.lock().unwrap();
        let stale: Vec<InstanceKey> = map
            .iter()
            .filter(|(_, inst)| inst.idle_for() > IDLE_TTL || !inst.alive())
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            if let Some(inst) = map.remove(&key) {
                inst.client.stop();
            }
        }
    }

    /// Lazily spawn (or reuse) the server for `key`. Sweeps idle instances on
    /// access first, reuses a live instance, and otherwise confines and spawns a
    /// new one. Caches the instance even if its handshake degraded, so a server
    /// that is alive but slow to initialize is not re-spawned on every call.
    pub fn get_or_spawn(
        &self,
        spawner: &dyn ProcessSpawner,
        key: InstanceKey,
        cfg: &LanguageServerConfig,
        templates: &Templates,
        cache_dir: &Path,
        deny_read: Vec<PathBuf>,
    ) -> Result<Arc<LspInstance>, Unavailable> {
        self.collect_idle();

        // Reuse a live instance.
        {
            let map = self.instances.lock().unwrap();
            if let Some(inst) = map.get(&key) {
                if inst.alive() {
                    inst.touch();
                    return Ok(inst.clone());
                }
            }
        }

        Self::availability_gate(&key.root, sandbox::is_available())?;

        // The cache dir must exist before the (confined) spawn can write it.
        let _ = std::fs::create_dir_all(cache_dir);

        let config = lsp_spawn_config(cfg, templates, &key.root, cache_dir, deny_read)
            .ok_or_else(|| Unavailable::new("language server has an empty command"))?;

        let child = spawner
            .spawn(config)
            .map_err(|e| Unavailable::new(format!("spawn failed: {e}")))?;
        let client = LspClient::start(
            child,
            &key.language,
            &key.root,
            cfg.initialization_options.clone(),
        )
        .map_err(|e| Unavailable::new(format!("handshake failed: {e}")))?;

        let inst = Arc::new(LspInstance {
            key: key.clone(),
            client,
            last_used: Mutex::new(Instant::now()),
        });

        let mut map = self.instances.lock().unwrap();
        // Another thread may have inserted a live instance while we spawned.
        if let Some(existing) = map.get(&key) {
            if existing.alive() {
                existing.touch();
                inst.client.stop();
                return Ok(existing.clone());
            }
        }
        map.insert(key, inst.clone());
        Ok(inst)
    }

    /// Runtime status of every pooled instance.
    pub fn statuses(&self) -> Vec<LspInstanceStatus> {
        let map = self.instances.lock().unwrap();
        let mut out: Vec<LspInstanceStatus> = map
            .values()
            .map(|inst| LspInstanceStatus {
                language: inst.key.language.clone(),
                root: inst.key.root.to_string_lossy().to_string(),
                handshake_ok: inst.client.handshake_ok(),
                ready: inst.client.is_ready(),
            })
            .collect();
        out.sort_by(|a, b| (a.language.as_str(), a.root.as_str()).cmp(&(&b.language, &b.root)));
        out
    }

    /// A snapshot of every pooled instance, for passive read surfaces such as
    /// diagnostics aggregation. Sweeps nothing and spawns nothing; callers filter
    /// by root and read each instance's already-collected state.
    pub fn instances(&self) -> Vec<Arc<LspInstance>> {
        self.instances.lock().unwrap().values().cloned().collect()
    }

    /// Stop and drop every pooled instance (shutdown).
    pub fn stop_all(&self) {
        let mut map = self.instances.lock().unwrap();
        for (_, inst) in map.drain() {
            inst.client.stop();
        }
    }
}

/// Build the confined spawn config for a language server. Pure (no spawning),
/// so it can be asserted directly in tests. Returns `None` when the configured
/// command is empty. The server is confined to its indexing `root` plus a
/// dedicated `cache_dir`, with reads broad minus `deny_read`.
pub fn lsp_spawn_config(
    cfg: &LanguageServerConfig,
    templates: &Templates,
    root: &Path,
    cache_dir: &Path,
    deny_read: Vec<PathBuf>,
) -> Option<SpawnConfig> {
    let command = cfg.expanded_command(templates);
    let (program, args) = command.split_first()?;

    let sandbox = if sandbox::is_available() {
        Some(SandboxPolicy::for_run(
            root,
            &[cache_dir.to_string_lossy().to_string()],
            deny_read,
        ))
    } else {
        None
    };

    let mut config = SpawnConfig::new(program)
        .args(args.iter().cloned())
        .cwd(&root.to_string_lossy())
        .sandbox(sandbox)
        .stdin(true);
    config.capture_stdout = true;
    config.capture_stderr = true;
    for (k, v) in cfg.expanded_env(templates) {
        config = config.env(&k, &v);
    }
    Some(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::RecordingProcessSpawner;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn templates() -> Templates {
        Templates {
            home: PathBuf::from("/home/u"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            worktrees: PathBuf::from("/home/u/.cairn/worktrees"),
            worktree: None,
        }
    }

    fn rust_cfg() -> LanguageServerConfig {
        LanguageServerConfig {
            enabled: true,
            command: vec!["rust-analyzer".to_string()],
            extensions: vec!["rs".to_string()],
            root_markers: vec!["Cargo.toml".to_string()],
            container_separator: "::".to_string(),
            initialization_options: None,
            env: HashMap::new(),
        }
    }

    #[test]
    fn availability_gate_rejects_missing_root_and_no_sandbox() {
        let dir = tempdir().unwrap();
        assert!(LspManager::availability_gate(dir.path(), true).is_ok());
        assert!(LspManager::availability_gate(dir.path(), false).is_err());
        assert!(LspManager::availability_gate(&dir.path().join("nope"), true).is_err());
    }

    #[test]
    fn spawn_config_confines_to_root_and_cache_dir() {
        let root = tempdir().unwrap();
        let cache = tempdir().unwrap();
        let config =
            lsp_spawn_config(&rust_cfg(), &templates(), root.path(), cache.path(), vec![]).unwrap();
        assert_eq!(config.program, "rust-analyzer");
        assert!(config.capture_stdin);
        assert_eq!(
            config.cwd.as_deref(),
            Some(root.path().to_string_lossy().as_ref())
        );
        if sandbox::is_available() {
            let policy = config.sandbox.expect("sandbox should be applied");
            let writable = policy.writable_paths();
            assert!(writable.contains(&root.path().to_path_buf()));
            assert!(writable.contains(&cache.path().to_path_buf()));
        }
    }

    #[test]
    fn spawn_config_none_for_empty_command() {
        let mut cfg = rust_cfg();
        cfg.command.clear();
        let root = tempdir().unwrap();
        let cache = tempdir().unwrap();
        assert!(lsp_spawn_config(&cfg, &templates(), root.path(), cache.path(), vec![]).is_none());
    }

    #[test]
    fn get_or_spawn_spawns_once_and_reuses() {
        // Sandbox must be available for the manager to spawn at all; on a host
        // without it (e.g. CI Windows) this branch is unreachable, so skip.
        if !sandbox::is_available() {
            return;
        }
        let spawner = RecordingProcessSpawner::new();
        let manager = LspManager::new();
        let root = tempdir().unwrap();
        let cache = tempdir().unwrap();
        let key = InstanceKey::new("rust", root.path().to_path_buf());

        let first = manager.get_or_spawn(
            &spawner,
            key.clone(),
            &rust_cfg(),
            &templates(),
            cache.path(),
            vec![],
        );
        assert!(first.is_ok());
        let second = manager.get_or_spawn(
            &spawner,
            key,
            &rust_cfg(),
            &templates(),
            cache.path(),
            vec![],
        );
        assert!(second.is_ok());
        // The second call reused the cached instance: only one spawn happened.
        assert_eq!(spawner.spawn_count(), 1);
    }

    #[test]
    fn get_or_spawn_unavailable_when_root_missing() {
        let spawner = RecordingProcessSpawner::new();
        let manager = LspManager::new();
        let cache = tempdir().unwrap();
        let key = InstanceKey::new("rust", PathBuf::from("/no/such/root"));
        let result = manager.get_or_spawn(
            &spawner,
            key,
            &rust_cfg(),
            &templates(),
            cache.path(),
            vec![],
        );
        assert!(result.is_err());
        assert_eq!(spawner.spawn_count(), 0);
    }
}
