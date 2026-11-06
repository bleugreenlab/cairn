//! [`LocalSessionHost`]: sessions run as children of this process.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cairn_platform::services::process::{ChildProcess, ProcessSpawner, SpawnConfig};

use crate::interface::session::{Cli, CodexModels, SessionError, SessionHost};
use crate::interface::LOCAL_HOST;
use crate::managed::{InstallProgress, ManagedTools};
use crate::session::codex::{home, models};

/// The [`SessionHost`] for sessions whose processes run on this machine:
/// every CLI is the managed copy Cairn installed, spawned through the host's
/// [`ProcessSpawner`], and Codex runs in the `CODEX_HOME` the host names.
pub struct LocalSessionHost {
    process: Arc<dyn ProcessSpawner>,
    codex_home: PathBuf,
    managed: ManagedTools,
}

impl LocalSessionHost {
    /// Spawn through `process`, and keep Codex's home at `codex_home` (which
    /// must be the host's own, never the user's `~/.codex`; see [`home`]).
    /// Each CLI resolves to the one `managed` installs; a CLI on the `PATH`
    /// is never consulted.
    pub fn new(
        process: Arc<dyn ProcessSpawner>,
        codex_home: impl Into<PathBuf>,
        managed: ManagedTools,
    ) -> Self {
        Self {
            process,
            codex_home: codex_home.into(),
            managed,
        }
    }

    pub fn codex_home(&self) -> &Path {
        &self.codex_home
    }

    /// The managed CLIs this host launches.
    pub fn managed(&self) -> &ManagedTools {
        &self.managed
    }
}

impl SessionHost for LocalSessionHost {
    fn name(&self) -> &str {
        LOCAL_HOST
    }

    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError> {
        self.resolve_cli_waiting(cli, &mut |_| {}, &|| true)
    }

    fn resolve_cli_waiting(
        &self,
        cli: Cli,
        on_wait: &mut dyn FnMut(InstallProgress),
        still_wanted: &dyn Fn() -> bool,
    ) -> Result<String, SessionError> {
        self.managed
            .get(cli)
            .ensure_with(on_wait, still_wanted)
            .map(|resolved| resolved.path.to_string_lossy().into_owned())
            .map_err(|error| {
                // An abandoned wait is not a failed install: nothing for the
                // transcript to report as one.
                if error.is_cancelled() {
                    SessionError::Setup(error.to_string())
                } else {
                    SessionError::CliInstallFailed(error.to_string())
                }
            })
    }

    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        self.process.spawn(config)
    }

    fn prepare_codex_home(&self, models: CodexModels<'_>) -> Result<String, SessionError> {
        if let CodexModels::Refresh { login } = models {
            let codex_path = self.resolve_cli(Cli::Codex)?;
            models::refresh_codex_model_cache(
                &codex_path,
                &self.codex_home,
                login,
                &HashMap::new(),
            )
            .map_err(SessionError::Setup)?;
        }
        home::write_codex_config(&self.codex_home).map_err(SessionError::Setup)
    }

    fn place_file(&self, dir: &str, name: &str, contents: &[u8]) -> Result<String, String> {
        let dir = Path::new(dir);
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("Failed to create directory {dir:?}: {e}"))?;
        let path = dir.join(name);
        std::fs::write(&path, contents)
            .map_err(|e| format!("Failed to write file {path:?}: {e}"))?;
        log::debug!("Placed {:?} ({} bytes)", path, contents.len());
        Ok(path.to_string_lossy().into_owned())
    }
}
