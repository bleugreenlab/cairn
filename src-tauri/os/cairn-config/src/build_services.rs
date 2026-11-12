//! Managed Build Services: settings-configured, Cairn-supervised shared daemons.
//!
//! A build service is a long-lived helper (e.g. an sccache compile-cache server)
//! shared across executor cells. Cairn launches it under a **service sandbox**
//! and injects **client env** into the spawns that build inside the managed
//! build roots ([`managed_build_roots`]) so their tooling connects to the
//! Cairn-owned daemon instead of auto-starting its own. See `docs/worktree-fence.md`.
//!
//! The daemon's write grant and the roots admitted to it come from one place:
//! [`BuildServiceConfig::launch_grant`] grants `**/target/**` under every
//! managed root by construction, and admission resolves a build's root through
//! the same [`managed_build_roots`]. A configured `write` adds extras only.
//!
//! Build services are declared in user-owned `~/.cairn/settings.yaml` only — never
//! repo-checked config — because a service's `write` scope is a privilege grant
//! (it widens what a shared process may write across executor projections), and a repo
//! committer must not be able to declare a broadly-writable daemon.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

/// Roots where Cairn materializes builds whose helper processes may use a
/// user-configured build service: agent worktrees and executor cells.
const MANAGED_BUILD_ROOTS: [&str; 2] = ["{worktrees}", CELL_BUILD_ROOT];

/// The managed root executor cells live under (the colocated build root,
/// cairn/5286), not the Cairn home.
const CELL_BUILD_ROOT: &str = "{buildRoot}/build-slots";

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
    /// Extra writable scopes (absolute globs) beyond `state_dir` + temp.
    /// Extras only: every managed root's `target/` trees are granted by
    /// construction ([`BuildServiceConfig::launch_grant`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<String>,
    /// Env injected into spawns that build inside a managed build root, so their
    /// client tooling connects here. See [`managed_build_roots`].
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    /// Env applied to the daemon **launch only**, never injected into client
    /// spawns. Daemon-only controls that must not leak into build tooling
    /// live here — e.g. sccache's `SCCACHE_START_SERVER`/`SCCACHE_NO_DAEMON`
    /// foreground-server switches (a client carrying `SCCACHE_START_SERVER` would
    /// try to run a server) and its `SCCACHE_ERROR_LOG`/`SCCACHE_LOG` diagnostics
    /// (which would otherwise spam build output). `env`, by contrast, is the
    /// client env that is also passed to the daemon.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub launch_env: HashMap<String, String>,
}

/// `path` as the kernel resolves it, whether or not it exists yet.
///
/// The service sandbox judges real paths: a build volume reached through a
/// symlink, or macOS's `/var` and `/tmp` (really `/private/...`), would
/// otherwise be granted under one spelling and written under another. A path
/// that does not exist yet resolves through its nearest existing ancestor, with
/// the rest appended unchanged.
pub fn canonical_path(path: &Path) -> PathBuf {
    if let Ok(real) = std::fs::canonicalize(path) {
        return real;
    }
    let mut remainder = Vec::new();
    let mut cursor = path;
    while let Some(parent) = cursor.parent() {
        let Some(name) = cursor.file_name() else {
            return path.to_path_buf();
        };
        remainder.push(name.to_os_string());
        if let Ok(real) = std::fs::canonicalize(parent) {
            return remainder
                .iter()
                .rev()
                .fold(real, |resolved, name| resolved.join(name));
        }
        cursor = parent;
    }
    path.to_path_buf()
}

/// The roots Cairn materializes builds in, canonical and without duplicates.
///
/// The one list both sides of a build service read: the daemon is granted
/// `**/target/**` under each ([`BuildServiceConfig::launch_grant`]), and a build
/// is admitted to the daemon by the root it falls under
/// ([`managed_root_containing`]).
pub fn managed_build_roots(templates: &Templates) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();
    for root in MANAGED_BUILD_ROOTS {
        let root = canonical_path(Path::new(&templates.expand(root)));
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    roots
}

/// The managed root executor cells on this machine are materialized under.
pub fn cell_build_root(templates: &Templates) -> PathBuf {
    canonical_path(Path::new(&templates.expand(CELL_BUILD_ROOT)))
}

/// The write grant a daemon holds under one managed root: the `target/` trees
/// beneath it, and nothing broader — never the root itself, which holds
/// worktree source. The cache wrapper's output-directory gate mirrors this
/// shape (`scripts/cache-wrapper.sh`).
pub fn target_grant_glob(root: &Path) -> String {
    format!("{}/**/target/**", root.display())
}

/// The managed root `build_dir` lies under, compared canonically on both sides.
pub fn managed_root_containing(templates: &Templates, build_dir: &Path) -> Option<PathBuf> {
    let build_dir = canonical_path(build_dir);
    managed_build_roots(templates)
        .into_iter()
        .find(|root| build_dir.starts_with(root))
}

/// What a daemon's launch was made from, digested per part.
///
/// A running daemon keeps the argv, environment, and sandbox grant it was
/// launched with, so a change to any of them is a change only a relaunch can
/// apply. Per part rather than one digest, so the relaunch can say what
/// changed. Managed roots are deliberately absent: the grant record carries
/// them separately, because several runners with different roots share one
/// daemon and a root difference is not a configuration change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchFingerprint {
    pub start: String,
    pub launch_env: String,
    pub write: String,
    pub state_dir: String,
}

impl LaunchFingerprint {
    /// The settings names of the parts that differ, in a fixed order.
    pub fn changed_parts(&self, other: &Self) -> Vec<&'static str> {
        [
            ("start", self.start != other.start),
            ("launchEnv", self.launch_env != other.launch_env),
            ("write", self.write != other.write),
            ("stateDir", self.state_dir != other.state_dir),
        ]
        .into_iter()
        .filter_map(|(part, changed)| changed.then_some(part))
        .collect()
    }
}

fn digest<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
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
    /// A command exits 0. A cheap liveness check, run with no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    /// A request/response health round-trip, run under a hard Rust-enforced
    /// deadline (see `orchestrator::build_services`). Unlike `command`, a
    /// deadline-exceeded run is treated as **wedged** (unhealthy) — this is what
    /// detects a listening-but-hung daemon that a bare TCP connect or an exit-0
    /// `command` can't distinguish from a healthy one. For sccache this is
    /// `sccache --show-stats`, a full round-trip that hangs identically against a
    /// wedged server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_trip: Option<Vec<String>>,
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
/// time; the others are global. An unexpanded `{worktree}` is left literal
/// when no per-spawn worktree is in scope.
#[derive(Debug, Clone)]
pub struct Templates {
    pub home: PathBuf,
    /// Where the external build caches live: the home directory, or a build
    /// volume's cache home (`CAIRN_CACHE_HOME` to the processes Cairn runs).
    pub cache_home: PathBuf,
    pub cairn_home: PathBuf,
    /// Where the runner's own executor keeps its cells: the Cairn home, or a
    /// build volume configured apart from it.
    pub build_root: PathBuf,
    pub worktrees: PathBuf,
    pub worktree: Option<PathBuf>,
}

impl Templates {
    /// Expand `{home}`, `{cacheHome}`, `{cairnHome}`, `{buildRoot}`,
    /// `{worktrees}`, and (when in scope) `{worktree}` in a string value.
    pub fn expand(&self, s: &str) -> String {
        let mut out = s
            .replace("{home}", &self.home.to_string_lossy())
            .replace("{cacheHome}", &self.cache_home.to_string_lossy())
            .replace("{cairnHome}", &self.cairn_home.to_string_lossy())
            .replace("{buildRoot}", &self.build_root.to_string_lossy())
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

    /// Every glob a daemon launched for `roots` is granted: each root's
    /// `target/` trees ([`target_grant_glob`]), then the configured `write`
    /// extras, without duplicates.
    pub fn launch_grant(&self, t: &Templates, roots: &[PathBuf]) -> Vec<String> {
        let mut grant: Vec<String> = Vec::new();
        for glob in roots
            .iter()
            .map(|root| target_grant_glob(root))
            .chain(self.expanded_write(t))
        {
            if !grant.contains(&glob) {
                grant.push(glob);
            }
        }
        grant
    }

    /// The launch this configuration would make, as a [`LaunchFingerprint`].
    /// `env` is not part of it: it is the client env, and a change there does
    /// not call for replacing a running daemon.
    pub fn launch_fingerprint(&self, t: &Templates) -> LaunchFingerprint {
        let launch_env: BTreeMap<String, String> =
            self.expanded_launch_env(t).into_iter().collect();
        LaunchFingerprint {
            start: digest(&self.expanded_start(t)),
            launch_env: digest(&launch_env),
            write: digest(&self.expanded_write(t)),
            state_dir: digest(&self.expanded_state_dir(t)),
        }
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

    /// The daemon-only launch env with templates expanded.
    pub fn expanded_launch_env(&self, t: &Templates) -> HashMap<String, String> {
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
            cache_home: PathBuf::from("/Volumes/Builds/cairn/cache-home"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            build_root: PathBuf::from("/Volumes/Builds/cairn"),
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
        assert_eq!(
            t.expand("{cacheHome}/.cache/sccache-cairn"),
            "/Volumes/Builds/cairn/cache-home/.cache/sccache-cairn"
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

    /// Expected paths go through the same resolution as the code under test:
    /// on macOS `/home` itself resolves to `/System/Volumes/Data/home`.
    fn real(path: &str) -> PathBuf {
        canonical_path(Path::new(path))
    }

    #[test]
    fn only_managed_build_roots_are_admitted_to_the_daemon() {
        let t = templates();
        let admitted = |t: &Templates, dir: &str| managed_root_containing(t, Path::new(dir));
        // An agent's jj residence and an executor cell: Cairn materialized both,
        // and the daemon's grant reaches both.
        assert_eq!(
            admitted(&t, "/home/u/.cairn/worktrees/cairn-1/src-tauri"),
            Some(real("/home/u/.cairn/worktrees"))
        );
        assert_eq!(
            admitted(&t, "/Volumes/Builds/cairn/build-slots/cairn/slot-3"),
            Some(real("/Volumes/Builds/cairn/build-slots"))
        );
        // With the build root on its own volume, the home's old inventory is
        // nobody's cell any more.
        assert_eq!(
            admitted(&t, "/home/u/.cairn/build-slots/cairn/slot-3"),
            None
        );
        let home_layout = Templates {
            build_root: PathBuf::from("/home/u/.cairn"),
            ..templates()
        };
        assert_eq!(
            admitted(&home_layout, "/home/u/.cairn/build-slots/cairn/slot-3"),
            Some(real("/home/u/.cairn/build-slots"))
        );
        // The developer's own checkout is the case the port split exists for: it
        // keeps sccache's defaults and its own unconfined server.
        assert_eq!(admitted(&t, "/home/u/projects/cairn"), None);
        // A sibling of a managed root is not inside it.
        assert_eq!(admitted(&t, "/home/u/.cairn/worktrees-scratch/x"), None);
    }

    #[test]
    fn the_launch_grant_is_every_managed_roots_target_tree_then_the_extras() {
        let t = templates();
        let roots = managed_build_roots(&t);
        assert_eq!(
            roots,
            vec![
                real("/home/u/.cairn/worktrees"),
                real("/Volumes/Builds/cairn/build-slots")
            ]
        );
        let cfg = BuildServiceConfig {
            enabled: true,
            start: vec!["sccache".into()],
            ready: None,
            state_dir: None,
            // An extra that repeats a constructed grant is not granted twice.
            write: vec!["{worktrees}/**/target/**".into(), "/opt/extra/**".into()],
            env: HashMap::new(),
            launch_env: HashMap::new(),
        };
        let mut constructed: Vec<String> = roots.iter().map(|r| target_grant_glob(r)).collect();
        constructed.push("/opt/extra/**".into());
        let expanded_extra = t.expand("{worktrees}/**/target/**");
        if !constructed.contains(&expanded_extra) {
            // Where `/home` is a symlink the configured spelling differs from
            // the canonical one and is kept as the extra it was written as.
            constructed.insert(2, expanded_extra);
        }
        assert_eq!(cfg.launch_grant(&t, &roots), constructed);
        assert_eq!(
            target_grant_glob(Path::new("/b/build-slots")),
            "/b/build-slots/**/target/**"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_root_reached_through_a_symlink_is_granted_at_its_real_path() {
        let temp = tempfile::tempdir().unwrap();
        let real_root = temp.path().join("volume");
        std::fs::create_dir_all(real_root.join("build-slots")).unwrap();
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(&real_root, &link).unwrap();
        let t = Templates {
            build_root: link.clone(),
            worktrees: temp.path().join("worktrees"),
            ..templates()
        };
        let resolved = std::fs::canonicalize(real_root.join("build-slots")).unwrap();
        assert!(managed_build_roots(&t).contains(&resolved));
        assert_eq!(cell_build_root(&t), resolved);
        // A build named through either spelling is admitted under the real root,
        // even before its directories exist.
        assert_eq!(
            managed_root_containing(&t, &link.join("build-slots/p/slot-1/target/debug")),
            Some(resolved.clone())
        );
        // And a root that does not exist yet keeps its resolved ancestor.
        assert_eq!(
            canonical_path(&temp.path().join("worktrees")),
            std::fs::canonicalize(temp.path())
                .unwrap()
                .join("worktrees")
        );
    }

    #[test]
    fn the_launch_fingerprint_names_what_changed_and_ignores_client_env() {
        let t = templates();
        let cfg = BuildServiceConfig {
            enabled: true,
            start: vec!["sccache".into()],
            ready: None,
            state_dir: Some("{cacheHome}/sccache".into()),
            write: vec![],
            env: HashMap::from([("A".into(), "1".into())]),
            launch_env: HashMap::from([("B".into(), "1".into()), ("C".into(), "2".into())]),
        };
        let base = cfg.launch_fingerprint(&t);
        assert_eq!(
            base,
            cfg.clone().launch_fingerprint(&t),
            "stable across calls"
        );

        let mut client_only = cfg.clone();
        client_only.env.insert("D".into(), "1".into());
        assert!(client_only
            .launch_fingerprint(&t)
            .changed_parts(&base)
            .is_empty());

        let mut write = cfg.clone();
        write.write.push("/x/**".into());
        assert_eq!(
            write.launch_fingerprint(&t).changed_parts(&base),
            vec!["write"]
        );

        let mut start = cfg.clone();
        start.start.push("--x".into());
        assert_eq!(
            start.launch_fingerprint(&t).changed_parts(&base),
            vec!["start"]
        );

        let mut launch_env = cfg.clone();
        launch_env.launch_env.insert("B".into(), "2".into());
        assert_eq!(
            launch_env.launch_fingerprint(&t).changed_parts(&base),
            vec!["launchEnv"]
        );

        let mut state_dir = cfg;
        state_dir.state_dir = Some("{home}/elsewhere".into());
        assert_eq!(
            state_dir.launch_fingerprint(&t).changed_parts(&base),
            vec!["stateDir"]
        );
    }
}
