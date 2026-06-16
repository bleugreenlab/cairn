//! Managed Build Services: settings-configured, Cairn-supervised shared daemons.
//!
//! A build service is a long-lived helper (e.g. an sccache compile-cache server)
//! shared across every worktree agent. Cairn launches it under a **service
//! sandbox** (the worktree fence plus a configurable extra writable scope — e.g.
//! every worktree's `target/` tree) and injects **client env** into fenced agent
//! spawns so their tooling connects to the Cairn-owned daemon instead of
//! auto-starting its own (which would inherit one worktree's sandbox and then be
//! denied when serving any other worktree). See `docs/worktree-fence.md`.
//!
//! Build services are declared in user-owned `~/.cairn/settings.yaml` only — never
//! repo-checked config — because a service's `write` scope is a privilege grant
//! (it widens what a shared process may write across worktrees), and a repo
//! committer must not be able to declare a broadly-writable daemon.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// One Cairn-supervised build-service daemon.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BuildServiceConfig {
    /// Whether Cairn launches and supervises this service. Disabled entries stay
    /// in settings but are skipped at startup and contribute no client env.
    #[serde(default)]
    pub enabled: bool,
    /// Argv Cairn spawns (under the service sandbox) to start the daemon.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub start: Vec<String>,
    /// Reachability/health probe. Absent = assume healthy once spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<ReadyProbe>,
    /// The daemon's own writable cache/state dir (auto-added to its writable set
    /// so it never needs a broader grant just to write its own cache).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<String>,
    /// Extra writable scopes (absolute globs) beyond `state_dir` + temp — the
    /// explicit cross-worktree grant, e.g. `{worktrees}/**/target/**`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
    /// Env injected into fenced agent spawns so client tooling connects here.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
}

/// A health/reachability probe for a build service. YAML reads as
/// `ready: { tcp: "127.0.0.1:4226" }` or `ready: { command: [...] }`. A struct
/// (not an enum) so it maps directly onto that single-key-map YAML shape; `tcp`
/// is checked first when both are set.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReadyProbe {
    /// TCP connect to `host:port` succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp: Option<String>,
    /// A command exits 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
}

impl ReadyProbe {
    /// A TCP-connect probe to `addr`.
    pub fn tcp(addr: impl Into<String>) -> Self {
        Self {
            tcp: Some(addr.into()),
            command: None,
        }
    }
}

/// Template variables expanded in build-service config string values.
///
/// `{worktree}` is per-spawn (client env injection) and absent at daemon-launch
/// time; the other three are global. An unexpanded `{worktree}` is left literal
/// when no per-spawn worktree is in scope.
#[derive(Debug, Clone)]
pub struct Templates {
    pub home: PathBuf,
    pub cairn_home: PathBuf,
    pub worktrees: PathBuf,
    pub worktree: Option<PathBuf>,
}

impl Templates {
    /// Expand `{home}`, `{cairnHome}`, `{worktrees}`, and (when in scope)
    /// `{worktree}` in a string value.
    pub fn expand(&self, s: &str) -> String {
        let mut out = s
            .replace("{home}", &self.home.to_string_lossy())
            .replace("{cairnHome}", &self.cairn_home.to_string_lossy())
            .replace("{worktrees}", &self.worktrees.to_string_lossy());
        if let Some(wt) = &self.worktree {
            out = out.replace("{worktree}", &wt.to_string_lossy());
        }
        out
    }
}

impl BuildServiceConfig {
    /// The launch argv with templates expanded.
    pub fn expanded_start(&self, t: &Templates) -> Vec<String> {
        self.start.iter().map(|s| t.expand(s)).collect()
    }

    /// The extra writable globs with templates expanded.
    pub fn expanded_write(&self, t: &Templates) -> Vec<String> {
        self.write.iter().map(|s| t.expand(s)).collect()
    }

    /// The daemon's state dir with templates expanded, if configured.
    pub fn expanded_state_dir(&self, t: &Templates) -> Option<PathBuf> {
        self.state_dir.as_ref().map(|s| PathBuf::from(t.expand(s)))
    }

    /// The client env with templates expanded.
    pub fn expanded_env(&self, t: &Templates) -> HashMap<String, String> {
        self.env
            .iter()
            .map(|(k, v)| (k.clone(), t.expand(v)))
            .collect()
    }
}

/// The built-in default sccache build service, used when no `buildServices` are
/// configured. The supervisor only launches it when `sccache` is on `PATH`, so
/// it is a safe, zero-config default that fixes the cross-worktree sccache EPERM
/// out of the box. Values use templates so they resolve per host.
///
/// Port and cache dir mirror `scripts/cache-wrapper.sh`'s defaults (4226,
/// `$HOME/.cache/sccache`) so the Cairn-launched daemon and the client wrapper
/// agree without further configuration.
pub fn default_sccache_service() -> BuildServiceConfig {
    let mut env = HashMap::new();
    env.insert("SCCACHE_SERVER_PORT".to_string(), "4226".to_string());
    env.insert(
        "SCCACHE_DIR".to_string(),
        "{home}/.cache/sccache".to_string(),
    );
    BuildServiceConfig {
        enabled: true,
        start: vec!["sccache".to_string(), "--start-server".to_string()],
        ready: Some(ReadyProbe::tcp("127.0.0.1:4226")),
        state_dir: Some("{home}/.cache/sccache".to_string()),
        write: vec!["{worktrees}/**/target/**".to_string()],
        env,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn templates() -> Templates {
        Templates {
            home: PathBuf::from("/home/u"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            worktrees: PathBuf::from("/home/u/.cairn/worktrees"),
            worktree: Some(PathBuf::from("/home/u/.cairn/worktrees/CAIRN-1")),
        }
    }

    #[test]
    fn ready_probe_yaml_shapes_roundtrip() {
        let tcp: ReadyProbe = serde_yaml::from_str("tcp: \"127.0.0.1:4226\"").unwrap();
        assert_eq!(tcp.tcp.as_deref(), Some("127.0.0.1:4226"));
        assert_eq!(tcp.command, None);
        let cmd: ReadyProbe =
            serde_yaml::from_str("command: [\"sccache\", \"--show-stats\"]").unwrap();
        assert_eq!(
            cmd.command,
            Some(vec!["sccache".to_string(), "--show-stats".to_string()])
        );
        assert_eq!(cmd.tcp, None);
    }

    #[test]
    fn build_service_config_yaml_roundtrip() {
        let yaml = r#"
enabled: true
start: ["sccache", "--start-server"]
ready:
  tcp: "127.0.0.1:4226"
stateDir: "{cairnHome}/sccache"
write:
  - "{worktrees}/**/target/**"
env:
  SCCACHE_SERVER_PORT: "4226"
"#;
        let cfg: BuildServiceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.start, vec!["sccache", "--start-server"]);
        assert_eq!(cfg.ready, Some(ReadyProbe::tcp("127.0.0.1:4226")));
        assert_eq!(cfg.state_dir.as_deref(), Some("{cairnHome}/sccache"));
        assert_eq!(cfg.write, vec!["{worktrees}/**/target/**"]);
        assert_eq!(
            cfg.env.get("SCCACHE_SERVER_PORT").map(String::as_str),
            Some("4226")
        );

        // Re-serialize and re-parse to confirm a stable round trip.
        let serialized = serde_yaml::to_string(&cfg).unwrap();
        let reparsed: BuildServiceConfig = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(cfg, reparsed);
    }

    #[test]
    fn template_expansion_covers_all_vars() {
        let t = templates();
        assert_eq!(t.expand("{home}/.cache/sccache"), "/home/u/.cache/sccache");
        assert_eq!(
            t.expand("{worktrees}/**/target/**"),
            "/home/u/.cairn/worktrees/**/target/**"
        );
        assert_eq!(t.expand("{cairnHome}/sccache"), "/home/u/.cairn/sccache");
        assert_eq!(
            t.expand("{worktree}/target"),
            "/home/u/.cairn/worktrees/CAIRN-1/target"
        );
    }

    #[test]
    fn worktree_template_left_literal_when_out_of_scope() {
        let t = Templates {
            worktree: None,
            ..templates()
        };
        // No per-spawn worktree (daemon-launch time): `{worktree}` is untouched.
        assert_eq!(t.expand("{worktree}/x"), "{worktree}/x");
    }

    #[test]
    fn default_sccache_service_expands_to_concrete_paths() {
        let t = templates();
        let svc = default_sccache_service();
        assert_eq!(svc.expanded_start(&t), vec!["sccache", "--start-server"]);
        assert_eq!(
            svc.expanded_write(&t),
            vec!["/home/u/.cairn/worktrees/**/target/**"]
        );
        assert_eq!(
            svc.expanded_state_dir(&t),
            Some(PathBuf::from("/home/u/.cache/sccache"))
        );
        assert_eq!(
            svc.expanded_env(&t).get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache")
        );
    }
}
