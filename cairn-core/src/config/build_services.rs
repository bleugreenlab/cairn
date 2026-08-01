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

/// The TCP port Cairn's supervised sccache daemon listens on. Deliberately one
/// above sccache's own default (4226): a build OUTSIDE the managed build roots —
/// the developer's own checkout, or CI — injects no service env and so falls
/// through to sccache's 4226 default, starting its OWN unconfined server, rather
/// than attaching to this confined daemon and being denied the server-side
/// miss-compile write into a `target/` the service sandbox doesn't cover. See
/// [`MANAGED_BUILD_ROOTS`] and `default_sccache_service`.
const CAIRN_SCCACHE_PORT: u16 = 4227;

/// The env var naming a spawn whose build tooling has been pointed at Cairn's
/// supervised daemons.
///
/// Its meaning to `scripts/cache-wrapper.sh` is exactly "attach, never start":
/// the only server that may hold Cairn's port is the one Cairn supervises, so a
/// client carrying this connects when the daemon is up and compiles directly
/// when it is not. Distinct from `CAIRN_SANDBOXED`, which says the *spawn* is
/// confined — a different reason for the same refusal to start a server, and no
/// longer the same set of spawns (see [`MANAGED_BUILD_ROOTS`]).
///
/// It is a claim about **one specific daemon**, so the service that owns that
/// daemon declares it in its own client `env` (see [`default_sccache_service`])
/// rather than the injection seam adding it to whatever a merge produced. Build
/// services are a generic facility: a workspace may enable a service that has
/// nothing to do with sccache, and inferring "Cairn named an sccache daemon"
/// from a non-empty merged env would tell that workspace's builds never to start
/// the server they should, leaving them uncached forever. Declaring it also
/// keeps it visible and editable, like the port, for an operator who replaces
/// the built-in entry with a hand-written one.
pub(crate) const BUILD_SERVICE_CLIENT_ENV: &str = "CAIRN_BUILD_SERVICE_CLIENT";

/// The env var that withdraws the invitation [`BUILD_SERVICE_CLIENT_ENV`]
/// extends: Cairn pointed this build at its daemon, and Cairn has since observed
/// that daemon failing the compiles it accepts.
///
/// A cache miss is compiled by the DAEMON, so a broken daemon does not cost a
/// build a cache hit — it fails the build outright, with a diagnostic naming
/// some third-party crate. The client cannot tell that apart from a real
/// compiler error: a server-side denied write arrives as exit 1 with rustc's own
/// error text, exactly like a type error (measured in CAIRN-3355). So the
/// judgment is made HERE, by the supervisor that watches the daemon's counters
/// across builds, and shipped to the wrapper as a fact rather than left to the
/// wrapper to infer from an exit status that cannot carry it.
///
/// Set only on POSITIVE evidence of unfitness, never on the absence of a healthy
/// observation — a daemon nothing has looked at yet is not a broken one. A build
/// carrying this compiles directly and uncached, which costs it the cache hits
/// the daemon was not going to provide anyway.
///
/// Deliberately a separate key rather than the removal of the sccache env: cargo
/// fingerprints `RUSTC_WRAPPER`, so withdrawing the wrapper to degrade would
/// invalidate every crate in every managed build root and turn a cache outage
/// into a machine-wide rebuild. The wrapper stays; only the routing decision
/// changes.
pub(crate) const BUILD_SERVICE_UNFIT_ENV: &str = "CAIRN_BUILD_SERVICE_UNFIT";

/// The directory roots Cairn materializes builds in, and therefore exactly the
/// builds the compile cache may serve: `{worktrees}` holds agent jj residences
/// and `{cairnHome}/build-slots` holds executor cells.
///
/// This is what decides whether a spawn is handed the daemon's client env — not
/// whether the spawn is fenced. The fence is a supervision policy and the cache
/// is a performance mechanism; what actually couples a build to this daemon is
/// whether the daemon may write where that build writes. The daemon runs each
/// cache-miss compile itself, so a build whose `target/` the service sandbox
/// does not cover does not merely miss the cache — rustc's output write is
/// kernel-denied and the compile fails with `Operation not permitted`. The
/// developer's own checkout is that case, which is why it keeps sccache's
/// defaults and its own unconfined server.
///
/// The daemon's writable grant is derived from these same roots (see
/// [`managed_build_write_globs`]), so the set a spawn is tested against and the
/// set the daemon may write cannot drift apart.
pub(crate) const MANAGED_BUILD_ROOTS: [&str; 2] = ["{worktrees}", "{cairnHome}/build-slots"];

/// Every Cairn home on the machine, as one glob.
///
/// One machine runs one compile daemon on one port ([`CAIRN_SCCACHE_PORT`]) with
/// one cache dir, both anchored at `{home}` rather than `{cairnHome}`. The
/// installed app and any number of dev instances each keep their own home —
/// `~/.cairn`, `~/.cairn-dev`, `~/.cairn-dev-<key>` — and each supervises that
/// same daemon. Whichever home's runner won the port, the daemon it launched
/// serves cells from all of them, so its grant must name all of them or a
/// cache-miss compile for another home's cell is denied. `*` does not span a
/// path separator, so this is the set of Cairn homes and nothing beneath them.
/// `scripts/cache-wrapper.sh` reasons about temp directories in the same terms.
const CAIRN_HOMES: &str = "{home}/.cairn*";

/// The daemon's cross-home writable grant: the `target/` tree of every managed
/// build root, on every Cairn home this machine has.
///
/// The [`MANAGED_BUILD_ROOTS`] counterpart, widened from this home to all of
/// them for the reason [`CAIRN_HOMES`] gives. `managed_roots_are_covered_by_the_daemon_grant`
/// pins that widening: every concrete root a spawn may be admitted for must be
/// matched by a glob the daemon was granted.
fn managed_build_write_globs() -> Vec<String> {
    vec![
        "{worktrees}/**/target/**".to_string(),
        format!("{CAIRN_HOMES}/build-slots/**/target/**"),
    ]
}

/// This host's [`MANAGED_BUILD_ROOTS`] as concrete paths.
pub(crate) fn managed_build_roots(templates: &Templates) -> Vec<PathBuf> {
    MANAGED_BUILD_ROOTS
        .iter()
        .map(|root| PathBuf::from(templates.expand(root)))
        .collect()
}

/// Whether a spawn that builds in `build_dir` builds inside a managed root, and
/// so may be pointed at the supervised compile-cache daemon.
pub(crate) fn builds_in_managed_root(templates: &Templates, build_dir: &Path) -> bool {
    managed_build_roots(templates)
        .iter()
        .any(|root| build_dir.starts_with(root))
}

/// The confined daemon's cache/state dir. Off sccache's default
/// `$HOME/.cache/sccache` (which an unmanaged build's own server keeps) so a
/// confined and an unconfined server never share one on-disk cache — sccache
/// assumes a single server per dir.
const CAIRN_SCCACHE_DIR: &str = "{home}/.cache/sccache-cairn";

/// The daemon's own temp directory, pinned explicitly at launch (see
/// `default_sccache_service`). A long-lived, machine-wide daemon keeps the
/// `TMPDIR` it was launched with for its entire life, so it must be a location
/// that outlives every build cell — never an inherited per-cell scratch path.
///
/// Deliberately a SIBLING of `CAIRN_SCCACHE_DIR`, not a subdirectory of it:
/// sccache treats everything under its cache dir as cache content to size and
/// evict, so a temp tree nested inside would be counted against the cache budget
/// and could be deleted out from under a live compile.
const CAIRN_SCCACHE_TEMP_DIR: &str = "{home}/.cache/sccache-cairn-tmp";

/// The built-in default sccache build service, used when no `buildServices` are
/// configured. The supervisor only launches it when `sccache` is on `PATH`, so
/// it is a safe, zero-config default that fixes the cross-worktree sccache EPERM
/// out of the box. Values use templates so they resolve per host.
///
/// Port and cache dir deliberately DIVERGE from sccache's own defaults (4226,
/// `$HOME/.cache/sccache`), which `scripts/cache-wrapper.sh` also falls back to
/// (see `CAIRN_SCCACHE_PORT` / `CAIRN_SCCACHE_DIR`). This confined daemon listens
/// on a Cairn-specific port and cache dir and injects them into every spawn that
/// builds inside a managed build root ([`MANAGED_BUILD_ROOTS`]), so their tooling
/// finds it; a build anywhere else injects nothing and keeps sccache's defaults,
/// starting its own unconfined server. Without that split a developer's own
/// checkout build would attach to this daemon (sccache is one-server-per-port)
/// and EPERM when the daemon's sandboxed miss-compile tried to write into the
/// checkout's `target/`.
///
/// The `RUSTC_WRAPPER` / `CARGO_BUILD_RUSTC_WRAPPER` env points every such
/// cargo invocation at the wrapper installed at `{cairnHome}/bin/cache-wrapper.sh`
/// (see `orchestrator::build_services::install_cache_wrapper`). That makes bare
/// `cargo` from an agent shell cache identically to the `bun run` scripts, and
/// gives every worktree one wrapper identity so cargo fingerprints never flip
/// between the two. Unix only — the wrapper is a shell script. `SCCACHE_CACHE_SIZE`
/// raises the daemon's max cache above the 10 GiB default (the daemon reads it
/// from this same env map at launch) so a warm multi-worktree workspace stops
/// evicting. `SCCACHE_IDLE_TIMEOUT=0` keeps the daemon alive indefinitely: sccache
/// defaults to exiting after 600 s idle, which killed the Cairn-supervised server
/// between builds — the client wrapper then silently fell back to uncached direct
/// compiles until the next runner restart. Like the cache size, the daemon reads
/// it from this env map at launch; it is inert in client env.
///
/// The daemon runs in the **foreground** as Cairn's supervised child rather than
/// forking a detached server: `launch_env` sets `SCCACHE_START_SERVER=1` (route
/// bare `sccache` to its in-process server) and `SCCACHE_NO_DAEMON=1` (skip the
/// `daemonize()` fork). The launched process then *is* the server and stays in
/// Cairn's process group, so a wedged server can be killed via its supervised
/// child handle and relaunched — its port stays occupied and `sccache
/// --stop-server` hangs against a wedged server, so a direct kill is the only
/// recovery. `launch_env` also points `SCCACHE_ERROR_LOG` at a file under the
/// (sandbox-writable) state dir with a `warn`-level `SCCACHE_LOG`, so the next
/// wedge/crash is diagnosable from disk, and pins `TMPDIR`/`TMP`/`TEMP` to
/// [`CAIRN_SCCACHE_TEMP_DIR`] so the daemon never inherits a temp directory that
/// can be reclaimed under it. These are daemon-only and never
/// injected into client spawns. On the client side, `SCCACHE_IGNORE_SERVER_IO_ERROR=1`
/// degrades a compile that can't reach the daemon (a mid-build death or wedge) to
/// a direct, uncached compile instead of failing or hanging the build.
///
/// Incremental compilation stays **on** for every managed build, and that is a
/// structural conclusion rather than a tuning preference (CAIRN-3348). sccache
/// hashes the compile's working directory into every Rust cache key
/// unconditionally — `compiler/rust.rs`, "The cwd of the compile. This will wind
/// up in the rlib" — so two slots at different paths can never share a
/// workspace-crate compile, whatever else is configured. `SCCACHE_BASEDIRS` does
/// not change that: sccache consults basedirs only when normalizing C/C++
/// preprocessor output and never on the Rust path. Turning incremental off would
/// therefore buy no cross-slot hit at all, while costing every slot the
/// incremental edit-test loop its agent depends on.
///
/// What the daemon does earn needs no policy to unlock, because cargo passes
/// `-C incremental` only to workspace members: registry dependencies are already
/// cacheable and already shared across every slot on the machine, their compile
/// cwd being the one shared `~/.cargo/registry` source dir. Accelerating a cold
/// slot mint is the cache's job, and it does that today. A warm slot compiling
/// only its own workspace crates is *expected* to be starved, and the compile
/// lane's third structural exclusion compounds it: `cargo clippy` reaches sccache
/// as `clippy-driver <absolute rustc path> …`, which it reads as multiple input
/// files, so the lint lane never touches the cache either. See
/// `docs/dev-instances.md` for the measurements behind all three.
pub(crate) fn default_sccache_service() -> BuildServiceConfig {
    let mut env = HashMap::new();
    env.insert(
        "SCCACHE_SERVER_PORT".to_string(),
        CAIRN_SCCACHE_PORT.to_string(),
    );
    env.insert("SCCACHE_DIR".to_string(), CAIRN_SCCACHE_DIR.to_string());
    env.insert("SCCACHE_CACHE_SIZE".to_string(), "50G".to_string());
    // Never idle out: the default 600 s idle timeout kills the supervised daemon
    // between builds, and the client wrapper silently degrades to uncached compiles.
    env.insert("SCCACHE_IDLE_TIMEOUT".to_string(), "0".to_string());
    // Client failover: if a compile can't reach the daemon (it died or wedged
    // mid-build), degrade THAT compile to a direct, uncached one instead of
    // failing or hanging the build. Client-side, so it rides in the injected env.
    env.insert(
        "SCCACHE_IGNORE_SERVER_IO_ERROR".to_string(),
        "1".to_string(),
    );
    // This service's own claim on the port above: a client of THIS daemon
    // attaches to it and never starts a server of its own. Declared here rather
    // than added by the injection seam, because it is true of this daemon and not
    // of build services in general (see `BUILD_SERVICE_CLIENT_ENV`). Inert on the
    // daemon's own launch env, which carries the same map: sccache never reads it.
    env.insert(BUILD_SERVICE_CLIENT_ENV.to_string(), "1".to_string());
    if cfg!(unix) {
        let wrapper = "{cairnHome}/bin/cache-wrapper.sh".to_string();
        env.insert("RUSTC_WRAPPER".to_string(), wrapper.clone());
        env.insert("CARGO_BUILD_RUSTC_WRAPPER".to_string(), wrapper);
    }

    // Daemon-only launch env. These MUST NOT leak into client spawns, so they
    // live in launch_env, not env (see `merge_client_env`).
    let mut launch_env = HashMap::new();
    // Run the server in the FOREGROUND as Cairn's supervised child instead of
    // letting sccache fork a detached daemon: SCCACHE_START_SERVER=1 routes bare
    // `sccache` to its in-process server, and SCCACHE_NO_DAEMON=1 skips the
    // daemonize() fork/setsid. The launched process then *is* the server, stays
    // in Cairn's process group, and is killed reliably via the supervised child
    // handle — the precondition for recovering a wedged server (whose port stays
    // occupied and which `sccache --stop-server` can't stop, hanging too).
    launch_env.insert("SCCACHE_START_SERVER".to_string(), "1".to_string());
    launch_env.insert("SCCACHE_NO_DAEMON".to_string(), "1".to_string());
    // Redirect the foreground server's stderr to a file so the next wedge/crash
    // is diagnosable from disk. create_error_log() runs before the daemonize
    // no-op, so this applies in foreground mode too. Kept under stateDir because
    // that is the only path the service sandbox lets the daemon write.
    launch_env.insert(
        "SCCACHE_ERROR_LOG".to_string(),
        format!("{CAIRN_SCCACHE_DIR}/sccache-error.log"),
    );
    launch_env.insert("SCCACHE_LOG".to_string(), "warn".to_string());
    // Pin the daemon's temp directory. sccache's server creates a temp dir per
    // cache-miss compile to stage rustc's output, and a daemon holds the TMPDIR
    // it was launched with for life. Left to inherit, a daemon started from a
    // build cell keeps that cell's ephemeral scratch path after the cell is
    // reclaimed and then fails EVERY compile it accepts, machine-wide, with
    // `Failed to create temp dir` — an error that surfaces in third-party crates
    // and looks unrelated to whatever change is under test. Daemon-only: a
    // client must keep its own cell-scoped temp dir.
    for key in ["TMPDIR", "TMP", "TEMP"] {
        launch_env.insert(key.to_string(), CAIRN_SCCACHE_TEMP_DIR.to_string());
    }

    BuildServiceConfig {
        enabled: true,
        // Bare `sccache`: SCCACHE_START_SERVER=1 (launch_env) selects the
        // in-process foreground server; `--start-server` would instead fork a
        // detached daemon Cairn could not supervise or kill by handle.
        start: vec!["sccache".to_string()],
        ready: Some(ReadyProbe {
            tcp: Some(format!("127.0.0.1:{CAIRN_SCCACHE_PORT}")),
            command: None,
            // A wedged sccache server still accepts the TCP connect, then blocks
            // the client's request read forever (no per-request timeout). The
            // deadlined round-trip detects that; --show-stats is a full
            // request/response that hangs identically against a wedged server.
            round_trip: Some(vec!["sccache".to_string(), "--show-stats".to_string()]),
        }),
        state_dir: Some(CAIRN_SCCACHE_DIR.to_string()),
        // Writable grant for the confined shared daemon. Its own pinned temp dir
        // comes first: it is a sibling of the state dir (see
        // `CAIRN_SCCACHE_TEMP_DIR`), so it needs an explicit grant of its own. The
        // rest is the cross-worktree grant — a cache-miss compile is run by the
        // server, so rustc must be able to write generated artifacts into every
        // managed build root, on every Cairn home sharing this one daemon.
        write: std::iter::once(format!("{CAIRN_SCCACHE_TEMP_DIR}/**"))
            .chain(managed_build_write_globs())
            .collect(),
        env,
        launch_env,
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
        // Bare `sccache`: the foreground server is selected via SCCACHE_START_SERVER
        // in launch_env, not a `--start-server` arg (which would fork a detached
        // daemon Cairn could not supervise).
        assert_eq!(svc.expanded_start(&t), vec!["sccache"]);
        assert_eq!(
            svc.expanded_write(&t),
            vec![
                "/home/u/.cache/sccache-cairn-tmp/**",
                "/home/u/.cairn/worktrees/**/target/**",
                "/home/u/.cairn*/build-slots/**/target/**",
            ]
        );
        assert_eq!(
            svc.expanded_state_dir(&t),
            Some(PathBuf::from("/home/u/.cache/sccache-cairn"))
        );
        assert_eq!(
            svc.expanded_env(&t).get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache-cairn")
        );
    }

    /// The load-bearing invariant of the whole injection rule: a spawn is
    /// admitted to the daemon because its build directory sits under a managed
    /// root, so every managed root's `target/` tree must be one the daemon was
    /// actually granted. A root the grant misses is not a missed cache hit — the
    /// daemon runs the miss-compile itself and rustc's write is kernel-denied,
    /// failing the build outright.
    #[test]
    fn managed_roots_are_covered_by_the_daemon_grant() {
        // Both the installed app's home and a dev instance's, because one
        // machine's homes share one daemon and only one of them launched it.
        for cairn_home in ["/home/u/.cairn", "/home/u/.cairn-dev-feature"] {
            let t = Templates {
                cairn_home: PathBuf::from(cairn_home),
                ..templates()
            };
            let granted: Vec<regex::Regex> = default_sccache_service()
                .expanded_write(&t)
                .iter()
                .map(|glob| regex::Regex::new(&cairn_sandbox::glob_to_regex(glob)).unwrap())
                .collect();
            for root in managed_build_roots(&t) {
                let artifact = root.join("CAIRN-1/src-tauri/target/debug/deps/x.rlib");
                let artifact = artifact.to_string_lossy();
                assert!(
                    granted.iter().any(|re| re.is_match(&artifact)),
                    "the daemon has no writable grant covering {artifact}, so a cache-miss \
                     compile for a spawn admitted from {root:?} would be denied"
                );
            }
        }
    }

    #[test]
    fn only_managed_build_roots_are_admitted_to_the_daemon() {
        let t = templates();
        // An agent's jj residence and an executor cell: Cairn materialized both,
        // and the daemon's grant reaches both.
        assert!(builds_in_managed_root(
            &t,
            Path::new("/home/u/.cairn/worktrees/CAIRN-1/src-tauri")
        ));
        assert!(builds_in_managed_root(
            &t,
            Path::new("/home/u/.cairn/build-slots/CAIRN/slot-3")
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

    #[test]
    fn default_sccache_service_raises_cache_size() {
        // The daemon reads SCCACHE_CACHE_SIZE from this same env map at launch;
        // it must be present on every platform so the server stops evicting.
        let env = default_sccache_service().expanded_env(&templates());
        assert_eq!(
            env.get("SCCACHE_CACHE_SIZE").map(String::as_str),
            Some("50G")
        );
    }

    #[test]
    fn default_sccache_service_never_idles_out() {
        // sccache's default 600 s idle timeout would kill the supervised daemon
        // between builds, silently degrading every later fenced build to uncached
        // compiles. The daemon reads SCCACHE_IDLE_TIMEOUT from this env map at
        // launch; 0 disables the timeout. Present on every platform.
        let env = default_sccache_service().expanded_env(&templates());
        assert_eq!(
            env.get("SCCACHE_IDLE_TIMEOUT").map(String::as_str),
            Some("0")
        );
    }

    #[cfg(unix)]
    #[test]
    fn default_sccache_service_injects_wrapper_env() {
        // Both cargo spellings point at the installed wrapper at {cairnHome}/bin,
        // so bare cargo and the bun scripts share one wrapper identity.
        let env = default_sccache_service().expanded_env(&templates());
        let wrapper = "/home/u/.cairn/bin/cache-wrapper.sh";
        assert_eq!(env.get("RUSTC_WRAPPER").map(String::as_str), Some(wrapper));
        assert_eq!(
            env.get("CARGO_BUILD_RUSTC_WRAPPER").map(String::as_str),
            Some(wrapper)
        );
    }

    #[test]
    fn default_sccache_service_runs_foreground_supervised() {
        // The daemon launches in the foreground as Cairn's supervised child so its
        // handle controls (and can kill) the server: SCCACHE_START_SERVER routes
        // bare `sccache` to its in-process server and SCCACHE_NO_DAEMON skips the
        // fork. Both are daemon-only launch env, NEVER injected into client spawns
        // (a client carrying SCCACHE_START_SERVER would try to run a server).
        let svc = default_sccache_service();
        let launch = svc.expanded_launch_env(&templates());
        assert_eq!(
            launch.get("SCCACHE_START_SERVER").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            launch.get("SCCACHE_NO_DAEMON").map(String::as_str),
            Some("1")
        );
        // Daemon-only launch env must not leak into the client env.
        let client = svc.expanded_env(&templates());
        assert!(!client.contains_key("SCCACHE_START_SERVER"));
        assert!(!client.contains_key("SCCACHE_NO_DAEMON"));
        assert!(!client.contains_key("SCCACHE_ERROR_LOG"));
        assert!(!client.contains_key("SCCACHE_LOG"));
    }

    #[test]
    fn default_sccache_service_diagnostics_log_under_state_dir() {
        // The error log must sit under stateDir — the only path the service sandbox
        // lets the daemon write — so create_error_log() succeeds and the file is
        // diagnosable from disk. A moderate SCCACHE_LOG level accompanies it.
        let svc = default_sccache_service();
        let launch = svc.expanded_launch_env(&templates());
        assert_eq!(
            launch.get("SCCACHE_ERROR_LOG").map(String::as_str),
            Some("/home/u/.cache/sccache-cairn/sccache-error.log")
        );
        let state = svc.expanded_state_dir(&templates()).unwrap();
        assert!(launch["SCCACHE_ERROR_LOG"].starts_with(state.to_str().unwrap()));
        assert_eq!(launch.get("SCCACHE_LOG").map(String::as_str), Some("warn"));
    }

    #[test]
    fn default_sccache_service_pins_a_stable_daemon_temp_dir() {
        // The launch env MUST name the daemon's temp dir explicitly. A daemon that
        // inherits one keeps it for life; inheriting a build cell's ephemeral
        // scratch path poisons every compile on the machine once that cell is
        // reclaimed. The pinned path is independent of any cell's lifetime, and it
        // is a SIBLING of the cache dir so sccache's own eviction never reaps it.
        let svc = default_sccache_service();
        let launch = svc.expanded_launch_env(&templates());
        let temp = "/home/u/.cache/sccache-cairn-tmp";
        for key in ["TMPDIR", "TMP", "TEMP"] {
            assert_eq!(
                launch.get(key).map(String::as_str),
                Some(temp),
                "{key} must be pinned in the daemon launch env"
            );
        }
        let cache = svc.expanded_state_dir(&templates()).unwrap();
        assert!(
            !std::path::Path::new(temp).starts_with(&cache),
            "the daemon temp dir must not nest inside the sccache cache dir"
        );
        // The service sandbox confines the daemon to its state dir plus the write
        // grants, so the pinned temp dir needs a grant of its own or every
        // cache-miss compile is denied.
        assert!(svc
            .expanded_write(&templates())
            .contains(&format!("{temp}/**")));
        // Daemon-only: a client keeps its own (cell-scoped) temp dir.
        let client = svc.expanded_env(&templates());
        for key in ["TMPDIR", "TMP", "TEMP"] {
            assert!(!client.contains_key(key), "{key} must not reach clients");
        }
    }

    #[test]
    fn default_sccache_service_client_fails_open_on_daemon_loss() {
        // A mid-compile daemon death/wedge must degrade THAT compile to a direct,
        // uncached one — never fail or hang the build. This is client-side, so it
        // rides in the injected client env.
        let client = default_sccache_service().expanded_env(&templates());
        assert_eq!(
            client
                .get("SCCACHE_IGNORE_SERVER_IO_ERROR")
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn default_sccache_service_claims_its_port_against_auto_start() {
        // The daemon this service describes is the only server that may hold its
        // port, so its clients attach and never start one. The claim belongs to
        // this service — a workspace running some other build service must not
        // inherit it and stop auto-starting the sccache server it should.
        let client = default_sccache_service().expanded_env(&templates());
        assert_eq!(
            client.get(BUILD_SERVICE_CLIENT_ENV).map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn default_sccache_service_uses_deadlined_round_trip_probe() {
        // Wedge detection needs a real request/response round-trip, not just a TCP
        // connect: a wedged server still accepts the connect. --show-stats is that
        // round-trip.
        let probe = default_sccache_service().ready.unwrap();
        assert_eq!(probe.tcp.as_deref(), Some("127.0.0.1:4227"));
        assert_eq!(
            probe.round_trip,
            Some(vec!["sccache".to_string(), "--show-stats".to_string()])
        );
    }

    #[test]
    fn default_sccache_service_keeps_incremental_on() {
        // Incremental compilation stays on for managed builds, and build-service
        // configuration is not where a decision about it would belong in any case:
        // this map is the daemon's identity (where it listens, what cache it keeps),
        // not per-build execution policy.
        //
        // The decision itself is settled and not a knob worth reopening: sccache
        // hashes the compile's cwd into every Rust key unconditionally, so sibling
        // slots at different paths cannot share a workspace-crate compile however
        // this is set. Disabling incremental would surrender the agent edit-test
        // loop to buy a cross-slot hit that does not exist. See the doc comment on
        // `default_sccache_service`.
        let env = default_sccache_service().expanded_env(&templates());
        assert_eq!(env.get("CARGO_INCREMENTAL"), None);
    }

    #[test]
    fn default_sccache_service_diverges_from_sccache_defaults() {
        // The confined daemon MUST NOT sit on sccache's own default port/cache dir
        // (4226, $HOME/.cache/sccache). An unfenced build — the developer's main
        // checkout, or CI — injects no service env and falls through to those
        // defaults, starting its OWN unconfined server. If this daemon shared them,
        // that build would attach to the confined daemon (one server per port) and
        // EPERM when its sandboxed miss-compile wrote into a target/ outside the
        // worktree/check-clone grant. See default_sccache_service.
        let env = default_sccache_service().expanded_env(&templates());
        assert_ne!(
            env.get("SCCACHE_SERVER_PORT").map(String::as_str),
            Some("4226")
        );
        assert_ne!(
            env.get("SCCACHE_DIR").map(String::as_str),
            Some("/home/u/.cache/sccache")
        );
    }
}
