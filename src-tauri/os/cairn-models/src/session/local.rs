//! [`LocalSessionHost`]: sessions run as children of this process.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cairn_platform::services::process::{ChildProcess, ProcessSpawner, SpawnConfig};

use crate::interface::session::{Cli, CodexModels, SessionError, SessionHost};
use crate::interface::LOCAL_HOST;
use crate::session::codex::{home, models, version};

/// How a host finds the Claude CLI. Cairn caches the answer; the default
/// searches the `PATH` each time.
pub type ClaudeResolver = Arc<dyn Fn() -> Result<String, String> + Send + Sync>;

/// The [`SessionHost`] for sessions whose processes run on this machine: CLIs
/// are found here, spawned through the host's [`ProcessSpawner`], and Codex
/// runs in the `CODEX_HOME` the host names.
pub struct LocalSessionHost {
    process: Arc<dyn ProcessSpawner>,
    codex_home: PathBuf,
    claude: ClaudeResolver,
}

impl LocalSessionHost {
    /// Spawn through `process`, and keep Codex's home at `codex_home` (which
    /// must be the host's own, never the user's `~/.codex`; see
    /// [`home`]). The Claude CLI is found on the `PATH`.
    pub fn new(process: Arc<dyn ProcessSpawner>, codex_home: impl Into<PathBuf>) -> Self {
        Self {
            process,
            codex_home: codex_home.into(),
            claude: Arc::new(|| cairn_platform::env::find_binary("claude")),
        }
    }

    /// Find the Claude CLI with `resolver` instead of searching the `PATH`.
    pub fn with_claude_resolver(mut self, resolver: ClaudeResolver) -> Self {
        self.claude = resolver;
        self
    }

    pub fn codex_home(&self) -> &Path {
        &self.codex_home
    }
}

impl SessionHost for LocalSessionHost {
    fn name(&self) -> &str {
        LOCAL_HOST
    }

    fn resolve_cli(&self, cli: Cli) -> Result<String, SessionError> {
        match cli {
            Cli::Claude => (self.claude)().map_err(SessionError::CliNotFound),
            Cli::Codex => {
                let path =
                    cairn_platform::env::find_binary("codex").map_err(SessionError::CliNotFound)?;
                version::check_codex_version(&path).map_err(SessionError::CliIncompatible)?;
                Ok(path)
            }
        }
    }

    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        self.process.spawn(config)
    }

    fn prepare_codex_home(&self, models: CodexModels<'_>) -> Result<String, SessionError> {
        if let CodexModels::Refresh { login } = models {
            let codex_path =
                cairn_platform::env::find_binary("codex").map_err(SessionError::CliNotFound)?;
            models::refresh_codex_model_cache(
                &codex_path,
                &self.codex_home,
                login.map(|login| login.expose().clone()),
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
