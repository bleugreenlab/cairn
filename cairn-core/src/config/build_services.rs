//! Managed Build Services: settings-configured, Cairn-supervised shared daemons.
//!
//! A build service is a long-lived helper (e.g. an sccache compile-cache server)
//! shared across executor cells. Cairn launches it under a **service sandbox**
//! (the logical namespace fence plus configurable writable scopes such as cell
//! `target/` trees) and injects **client env** into the spawns that build inside
//! those scopes ([`MANAGED_BUILD_ROOTS`]) so their tooling connects to the
//! Cairn-owned daemon instead of auto-starting its own. See `docs/worktree-fence.md`.
//!
//! Build services are declared in user-owned `~/.cairn/settings.yaml` only — never
//! repo-checked config — because a service's `write` scope is a privilege grant
//! (it widens what a shared process may write across executor projections), and a repo
//! committer must not be able to declare a broadly-writable daemon.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Roots where Cairn materializes builds whose helper processes may use a
/// user-configured build service.
const MANAGED_BUILD_ROOTS: [&str; 2] = ["{worktrees}", "{cairnHome}/build-slots"];

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
    pub(crate) start: Vec<String>,
    /// Reachability/health probe. Absent = assume healthy once spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ready: Option<ReadyProbe>,
    /// The daemon's own writable cache/state dir (auto-added to its writable set
    /// so it never needs a broader grant just to write its own cache).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) state_dir: Option<String>,
    /// Extra writable scopes (absolute globs) beyond `state_dir` + temp — the
    /// explicit cross-worktree grant, e.g. `{worktrees}/**/target/**`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) write: Vec<String>,
    /// Env injected into spawns that build inside a managed build root, so their
    /// client tooling connects here. See [`MANAGED_BUILD_ROOTS`].
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub(crate) env: HashMap<String, String>,
    /// Env applied to the daemon **launch only**, never injected into client
    /// spawns. Daemon-only controls that must not leak into build tooling
    /// live here — e.g. sccache's `SCCACHE_START_SERVER`/`SCCACHE_NO_DAEMON`
    /// foreground-server switches (a client carrying `SCCACHE_START_SERVER` would
    /// try to run a server) and its `SCCACHE_ERROR_LOG`/`SCCACHE_LOG` diagnostics
    /// (which would otherwise spam build output). `env`, by contrast, is the
    /// client env that is also passed to the daemon.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub(crate) launch_env: HashMap<String, String>,
}

pub(crate) fn builds_in_managed_root(templates: &Templates, build_dir: &Path) -> bool {
    MANAGED_BUILD_ROOTS
        .iter()
        .map(|root| PathBuf::from(templates.expand(root)))
        .any(|root| build_dir.starts_with(root))
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
    pub(crate) tcp: Option<String>,
    /// A command exits 0. A cheap liveness check, run with no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) command: Option<Vec<String>>,
    /// A request/response health round-trip, run under a hard Rust-enforced
    /// deadline (see `orchestrator::build_services`). Unlike `command`, a
    /// deadline-exceeded run is treated as **wedged** (unhealthy) — this is what
    /// detects a listening-but-hung daemon that a bare TCP connect or an exit-0
    /// `command` can't distinguish from a healthy one. For sccache this is
    /// `sccache --show-stats`, a full round-trip that hangs identically against a
    /// wedged server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) round_trip: Option<Vec<String>>,
}

impl ReadyProbe {
    /// A TCP-connect probe to `addr`.
    pub fn tcp(addr: impl Into<String>) -> Self {
        Self {
            tcp: Some(addr.into()),
            command: None,
            round_trip: None,
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
    pub(crate) home: PathBuf,
    pub(crate) cairn_home: PathBuf,
    pub(crate) worktrees: PathBuf,
    pub(crate) worktree: Option<PathBuf>,
}

impl Templates {
    /// Expand `{home}`, `{cairnHome}`, `{worktrees}`, and (when in scope)
    /// `{worktree}` in a string value.
    pub(crate) fn expand(&self, s: &str) -> String {
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
    pub(crate) fn expanded_start(&self, t: &Templates) -> Vec<String> {
        self.start.iter().map(|s| t.expand(s)).collect()
    }

    /// The extra writable globs with templates expanded.
    pub(crate) fn expanded_write(&self, t: &Templates) -> Vec<String> {
        self.write.iter().map(|s| t.expand(s)).collect()
    }

    /// The daemon's state dir with templates expanded, if configured.
    pub(crate) fn expanded_state_dir(&self, t: &Templates) -> Option<PathBuf> {
        self.state_dir.as_ref().map(|s| PathBuf::from(t.expand(s)))
    }

    /// The client env with templates expanded.
    pub(crate) fn expanded_env(&self, t: &Templates) -> HashMap<String, String> {
        self.env
            .iter()
            .map(|(k, v)| (k.clone(), t.expand(v)))
            .collect()
    }

    /// The daemon-only launch env with templates expanded.
    pub(crate) fn expanded_launch_env(&self, t: &Templates) -> HashMap<String, String> {
        self.launch_env
            .iter()
            .map(|(k, v)| (k.clone(), t.expand(v)))
            .collect()
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
            worktree: Some(PathBuf::from("/home/u/.cairn/worktrees/cairn-1")),
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
            "/home/u/.cairn/worktrees/cairn-1/target"
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
    fn only_managed_build_roots_are_admitted_to_the_daemon() {
        let t = templates();
        // An agent's jj residence and an executor cell: Cairn materialized both,
        // and the daemon's grant reaches both.
        assert!(builds_in_managed_root(
            &t,
            Path::new("/home/u/.cairn/worktrees/cairn-1/src-tauri")
        ));
        assert!(builds_in_managed_root(
            &t,
            Path::new("/home/u/.cairn/build-slots/cairn/slot-3")
        ));
        // The developer's own checkout is the case the port split exists for: it
        // keeps sccache's defaults and its own unconfined server.
        assert!(!builds_in_managed_root(
            &t,
            Path::new("/home/u/projects/cairn")
        ));
        // A sibling of a managed root is not inside it.
        assert!(!builds_in_managed_root(
            &t,
            Path::new("/home/u/.cairn/worktrees-scratch/x")
        ));
    }
}
