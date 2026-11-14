//! Every directory Cairn writes under its home, and the retention rule that
//! bounds it.
//!
//! Tenet cairn://p/cairn/tenets/1446: every directory Cairn writes under its
//! home has a retention rule its writer enforces, and bytes the sweep measures
//! that no rule can reclaim are filed by name. This registry is that contract
//! for the filesystem, in the shape `TABLE_RETENTION` (cairn-db's
//! `storage::retention`) gives it for tables: one line per top-level entry,
//! naming the writer, the thread that owns it, and the rule. A writer with no
//! rule yet is [`HomeRule::Undecided`] and names the issue that will give it
//! one, so its absence is a recorded obligation rather than a silence.
//!
//! Three readers hold the registry to the code and the code to the registry:
//!
//! - the executor's categorized walk (`cairn-exec`'s storage accounting)
//!   classifies every top-level entry it prices here, and names the ones at or
//!   above [`UNRECLAIMABLE_THRESHOLD_BYTES`] that are unclaimed or undecided;
//! - this module's tests pin that a production-shaped home is fully claimed and
//!   that the registry is well formed (the `home-coverage` lane);
//! - a source scan (in the `source-scans` lane) refuses a literal joined onto a
//!   home root whose first component is not registered here, so a new writer
//!   cannot be born without a line.
//!
//! The registry describes the default layout, where the build root is the home.
//! When a build root is configured apart, the home's own `build-slots/`,
//! `managed-objects/`, `dependency-catalog/` and `tool-cache/` are a former
//! build root's, which no ladder rung reaches; the walk prices them as such
//! ([`FORMER_BUILD_ROOT_ENTRIES`]) and cairn/6762 retires them.

use std::path::{Path, PathBuf};

/// How large an unclaimed or undecided entry must be before the walk names it
/// as an unreclaimable holding. Small enough to catch a writer early, large
/// enough that a stray lock file or a Finder `.DS_Store` never raises one.
pub const UNRECLAIMABLE_THRESHOLD_BYTES: u64 = 1024 * 1024 * 1024;

/// How many unreclaimable holdings one walk reports by name. The true count
/// travels beside the bounded list.
pub const UNRECLAIMABLE_LIMIT: usize = 16;

/// The home entries that are a former build root's when the build root is
/// configured apart from the home: everything [`crate::executor_paths::ExecutorPaths`]
/// derives from the build root, other than the scratch trees (which the home
/// keeps for its own machine-authority cells).
pub const FORMER_BUILD_ROOT_ENTRIES: &[&str] = &[
    crate::runtime::BUILD_SLOTS_DIR,
    crate::executor_paths::MANAGED_OBJECTS_DIR,
    crate::executor_paths::DEPENDENCY_CATALOG_DIR,
    crate::executor_paths::TOOL_CACHE_DIR,
];

/// What bounds an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeRule {
    /// Removed once older than a fixed age.
    Window {
        max_age_secs: u64,
        enforcer: &'static str,
    },
    /// Held under a byte budget its enforcer reclaims down to.
    Budget { enforcer: &'static str },
    /// Removed with the thing it belongs to.
    Lifecycle {
        keyed_on: &'static str,
        enforcer: &'static str,
    },
    /// Tiered or evicted row by row (the database and its archive).
    Tiered { enforcer: &'static str },
    /// Rebuildable from, and bounded by, another entry.
    Derived { of: &'static str },
    /// Rewritten in place: a refresh replaces what was there.
    Replaced { enforcer: &'static str },
    /// One small piece of machine or process state, rewritten in place.
    Singleton(&'static str),
    /// Authored by the operator or by configuration, and removed with its
    /// declaration or by hand.
    Authored(&'static str),
    /// A retired layout; a one-time cleanup removes what is left.
    Legacy { cleanup: &'static str },
    /// No rule yet. Names the issue that will give it one, as a
    /// `cairn://p/<project>/<number>` URI.
    Undecided { issue: &'static str },
}

impl HomeRule {
    /// The issue an undecided entry names.
    pub fn issue(&self) -> Option<&'static str> {
        match self {
            Self::Undecided { issue } => Some(issue),
            _ => None,
        }
    }
}

/// How an entry's name matches what is on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeEntryShape {
    File,
    Dir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HomeEntry {
    pub name: &'static str,
    pub shape: HomeEntryShape,
    /// The code that creates it.
    pub writer: &'static str,
    /// The thread that owns its rule, as a label: the registry ships in the
    /// product, and another workspace has no such thread to address.
    pub owner: &'static str,
    pub rule: HomeRule,
    /// Entries one level down whose rule differs from this one's.
    pub children: &'static [HomeEntry],
}

impl HomeEntry {
    /// This entry's path under `home`.
    pub fn join(&self, home: &Path) -> PathBuf {
        home.join(self.name)
    }
}

const fn entry(
    name: &'static str,
    shape: HomeEntryShape,
    writer: &'static str,
    owner: &'static str,
    rule: HomeRule,
) -> HomeEntry {
    HomeEntry {
        name,
        shape,
        writer,
        owner,
        rule,
        children: &[],
    }
}

const fn dir(
    name: &'static str,
    writer: &'static str,
    owner: &'static str,
    rule: HomeRule,
) -> HomeEntry {
    entry(name, HomeEntryShape::Dir, writer, owner, rule)
}

const fn file(
    name: &'static str,
    writer: &'static str,
    owner: &'static str,
    rule: HomeRule,
) -> HomeEntry {
    entry(name, HomeEntryShape::File, writer, owner, rule)
}

const DAY_SECS: u64 = 24 * 60 * 60;

use HomeRule::{
    Authored, Budget, Derived, Legacy, Lifecycle, Replaced, Singleton, Tiered, Undecided, Window,
};

const BUILD_SLOTS_CHILDREN: &[HomeEntry] = &[
    dir(
        crate::executor_paths::AUTHORITY_DIR,
        "cairn-exec machine authority",
        "execution",
        Singleton("generation lock, owner record, cell epochs, storage-state.json"),
    ),
    dir(
        ".root-migrations",
        "cairn-exec build-root migration",
        "execution",
        Singleton("one marker per completed migration"),
    ),
    dir(
        ".compile-cache-probe",
        "cairn-core build_services compile-cache probe",
        "execution",
        Replaced {
            enforcer: "the probe reuses its one target",
        },
    ),
];

const TMP_CHILDREN: &[HomeEntry] = &[dir(
    "codex-login",
    "the Codex sign-in flow",
    "models",
    Lifecycle {
        keyed_on: "the sign-in",
        enforcer: "signin::codex removes its CODEX_HOME when the sign-in settles",
    },
)];

const REBUILDS_CHILDREN: &[HomeEntry] = &[
    dir(
        "tree",
        "scripts/rebuild-lib.ts release tree",
        "devops",
        Undecided {
            issue: "cairn://p/cairn/7023",
        },
    ),
    dir(
        "tree.lock",
        "scripts/rebuild-lib.ts lockReleaseTree",
        "devops",
        Singleton("the release tree's holder lock"),
    ),
];

const CLAUDE_PROFILES_CHILDREN: &[HomeEntry] = &[dir(
    "_transcripts",
    "the Claude CLI",
    "harness",
    Undecided {
        issue: "cairn://p/cairn/6759",
    },
)];

/// Every top-level entry Cairn writes under its home.
pub const HOME_RETENTION: &[HomeEntry] = &[
    // Executor state: cells, caches, scratch.
    HomeEntry {
        children: BUILD_SLOTS_CHILDREN,
        ..dir(
            crate::runtime::BUILD_SLOTS_DIR,
            "cairn-exec cells",
            "execution",
            Budget {
                enforcer: "cairn-exec storage ladder (idle retention, retired-cell rungs); \
                           quarantines and discards held for QUARANTINE_FORENSIC_WINDOW_MS",
            },
        )
    },
    dir(
        crate::executor_paths::MANAGED_OBJECTS_DIR,
        "cairn-exec managed object cache",
        "execution",
        Budget {
            enforcer: "sweep_managed_cache_budget (80 GiB, least recently used first)",
        },
    ),
    dir(
        crate::executor_paths::DEPENDENCY_CATALOG_DIR,
        "cairn-exec dependency catalog",
        "execution",
        Budget {
            enforcer: "prune_inactive_generations and the catalog-staging rung",
        },
    ),
    dir(
        crate::executor_paths::TOOL_CACHE_DIR,
        "cairn-exec tool cache, scripts/ensure-tursodb.ts",
        "execution",
        Undecided {
            issue: "cairn://p/cairn/7020",
        },
    ),
    dir(
        crate::executor_paths::CACHE_HOME_DIR,
        "external build caches under a build root",
        "execution",
        Budget {
            enforcer: "the external managed cache roots' reclamation",
        },
    ),
    HomeEntry {
        children: TMP_CHILDREN,
        ..dir(
            crate::executor_paths::SCRATCH_TREES_DIR,
            "cairn-exec cell scratch and node scratch replicas",
            "execution",
            Window {
                max_age_secs: DAY_SECS,
                enforcer:
                    "cairn-exec temporary-tree sweep (DEFAULT_TEMP_MAX_AGE) and scratch release",
            },
        )
    },
    dir(
        "warm-caches",
        "none in production code",
        "execution",
        Undecided {
            issue: "cairn://p/cairn/7019",
        },
    ),
    dir(
        "scratch",
        "cairn-core scratch residences",
        "performance",
        Window {
            max_age_secs: 7 * DAY_SECS,
            enforcer: "scratch::residences remove_quiet_unowned",
        },
    ),
    dir(
        "node-scratch-store",
        "cairn-core node_scratch::store",
        "performance",
        Lifecycle {
            keyed_on: "the node scratch",
            enforcer: "node_scratch::store::delete when the node closes",
        },
    ),
    // The orchestrator's database and its derivatives. Exact names: a
    // `cairn.db.backup` beside them is nothing TABLE_RETENTION can evict.
    file(
        crate::paths::RUNNER_DB_FILENAME,
        "the orchestrator database",
        "performance",
        Tiered {
            enforcer: "TABLE_RETENTION and the tiering of tenet 1303",
        },
    ),
    file(
        "cairn.db-wal",
        "the orchestrator database's write-ahead log",
        "performance",
        Derived { of: "cairn.db" },
    ),
    file(
        "cairn.db-log",
        "the orchestrator database's log",
        "performance",
        Derived { of: "cairn.db" },
    ),
    file(
        "cairn.search",
        "the orchestrator search index",
        "performance",
        Derived { of: "cairn.db" },
    ),
    dir(
        "archive",
        "cairn-db archival packs",
        "performance",
        Tiered {
            enforcer: "the archive pack catalog",
        },
    ),
    dir(
        "jj-stores",
        "cairn-vcs jj stores",
        "performance",
        Undecided {
            issue: "cairn://p/cairn/6757",
        },
    ),
    dir(
        "jj",
        "cairn-vcs managed jj config",
        "performance",
        Singleton("jj/config.toml, rewritten in place"),
    ),
    dir(
        "object-fetch-scratch",
        "cairn-core runner object fetch spool (open_runner_fetch)",
        "execution",
        Window {
            max_age_secs: DAY_SECS,
            enforcer: "each fetch removes its spool; reclaim_stale_fetch_scratch at the next fetch",
        },
    ),
    dir(
        "object-upload-staging",
        "cairn-transport object uploads, ObjectPlane::stage_delta",
        "performance",
        Undecided {
            issue: "cairn://p/cairn/7022",
        },
    ),
    dir(
        "state",
        "thread databases (paths::thread_db_dir)",
        "harness",
        Lifecycle {
            keyed_on: "the thread",
            enforcer: "threads::db_store::remove",
        },
    ),
    // Logs.
    dir(
        "logs",
        "every Cairn process's JSONL log",
        "devops",
        Window {
            max_age_secs: 7 * DAY_SECS,
            enforcer: "logging housekeeping (logRetentionDays, default 7)",
        },
    ),
    // The runner service's launchd logs, by their exact names: only the
    // stderr stream and its one archive are rotated.
    file(
        "orch.err.log",
        "launchd's capture of the runner's stderr (service::runner_stderr_log)",
        "devops",
        Budget {
            enforcer: "cairn-orch rotate_stderr_log_past (size cap, archived to .1)",
        },
    ),
    file(
        "orch.err.log.1",
        "cairn-orch rotate_stderr_log_past",
        "devops",
        Replaced {
            enforcer: "each rotation overwrites the one archive",
        },
    ),
    file(
        "orch.out.log",
        "launchd's capture of the runner's stdout",
        "devops",
        Undecided {
            issue: "cairn://p/cairn/7046",
        },
    ),
    // Builds of Cairn itself.
    HomeEntry {
        children: REBUILDS_CHILDREN,
        ..dir(
            "rebuilds",
            "cairn-core rebuilds, scripts/rebuild*.ts",
            "devops",
            Lifecycle {
                keyed_on: "the rebuild attempt",
                enforcer: "rebuild retention (cairn/6760)",
            },
        )
    },
    dir(
        "backups",
        "scripts/rebuild.ts app backups",
        "devops",
        Replaced {
            enforcer: "a verified rebuild prunes earlier rebuilds' backups (staleBackups)",
        },
    ),
    dir(
        "executor-artifacts",
        "cairn-core executor distribution",
        "devops",
        Undecided {
            issue: "cairn://p/cairn/6763",
        },
    ),
    dir(
        "turn-checks",
        "cairn-core turn-end check logs",
        "devops",
        Undecided {
            issue: "cairn://p/cairn/7025",
        },
    ),
    dir(
        "publications",
        "cairn-core publication leg logs",
        "devops",
        Undecided {
            issue: "cairn://p/cairn/7152",
        },
    ),
    dir(
        "check-slices",
        "cairn-core impact slices",
        "devops",
        Lifecycle {
            keyed_on: "the check slice",
            enforcer: "impact_slice removes its scratch on drop",
        },
    ),
    dir(
        "uv-cache",
        "cairn-platform uv_cache_dir",
        "devops",
        Undecided {
            issue: "cairn://p/cairn/7024",
        },
    ),
    dir(
        "bin",
        "CLI refresh and tool shims",
        "devops",
        Replaced {
            enforcer: "cli_refresh replaces in place",
        },
    ),
    dir(
        "tools",
        "managed tool installs",
        "devops",
        Replaced {
            enforcer: "tool_refresh sweep_trees",
        },
    ),
    dir(
        "runtime",
        "bundled runtimes (page-render)",
        "devops",
        Replaced {
            enforcer: "installed over in place",
        },
    ),
    dir(
        "shell_integration",
        "cairn-core shell integration scripts",
        "devops",
        Replaced {
            enforcer: "rewritten at startup",
        },
    ),
    // Providers and accounts.
    dir(
        "codex",
        "Cairn's CODEX_HOME and the Codex child",
        "providers",
        Lifecycle {
            keyed_on: "the session, 30 days idle",
            enforcer: "cairn-models session::codex::retention (cairn/6758)",
        },
    ),
    HomeEntry {
        children: CLAUDE_PROFILES_CHILDREN,
        ..dir(
            "claude-profiles",
            "claude_home per-account homes",
            "models",
            Singleton("one small home per account"),
        )
    },
    dir(
        "credential-leases",
        "cairn-credentials refresh leases",
        "models",
        Singleton("one lock file per credential scope"),
    ),
    dir(
        "inference",
        "cairn-exec inference deployments",
        "models",
        Undecided {
            issue: "cairn://p/cairn/7021",
        },
    ),
    dir(
        "voice",
        "cairn-core voice installer engines",
        "voice",
        Replaced {
            enforcer: "the voice installer replaces a component in place",
        },
    ),
    dir(
        "models",
        "cairn-core voice installer model artifacts",
        "voice",
        Replaced {
            enforcer: "the voice installer replaces a component in place",
        },
    ),
    file(
        "voice-components.json",
        "cairn-core voice installer manifest",
        "voice",
        Singleton("the installed-components manifest"),
    ),
    dir(
        crate::imports::IMPORTS_ROOT_DIR,
        "desktop attachment imports",
        "chat-ux",
        Window {
            max_age_secs: crate::imports::IMPORT_TTL.as_secs(),
            enforcer: "the desktop's sweep_stale_imports",
        },
    ),
    // Configuration: the workspace config repository and what it holds.
    dir(
        ".git",
        "the workspace config repository",
        "performance",
        Authored("the workspace config history"),
    ),
    file(
        ".gitignore",
        "workspace::repo::ensure_gitignore",
        "performance",
        Singleton("rewritten to the canonical ignore list"),
    ),
    file(
        "AGENTS.md",
        "the operator",
        "performance",
        Authored("workspace instructions"),
    ),
    file(
        "settings.yaml",
        "cairn-config settings",
        "performance",
        Authored("workspace settings"),
    ),
    file(
        "keybinds.json",
        "cairn-core keybinds",
        "performance",
        Authored("keybindings"),
    ),
    file(
        "credentials.yaml",
        "cairn-credentials store",
        "models",
        Authored("stored credentials"),
    ),
    file(
        "mcp_tools.json",
        "cairn-core MCP tool catalog",
        "performance",
        Singleton("the discovered tool catalog"),
    ),
    file(
        "hook-settings.json",
        "cairn-core memory hooks",
        "performance",
        Singleton("rewritten in place"),
    ),
    file(
        "tier-grid.retired.yaml",
        "cairn-config settings migration",
        "performance",
        Singleton("the retired tier grid, written once"),
    ),
    dir(
        "references",
        "cairn-core references",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "packs",
        "cairn-packs installs",
        "performance",
        Authored("removed when the pack is uninstalled"),
    ),
    dir(
        "plugin-data",
        "cairn-packs agent plugin runtimes",
        "performance",
        Authored("one directory per installed pack"),
    ),
    dir(
        "library",
        "cairn-packs library index cache",
        "performance",
        Replaced {
            enforcer: "each refresh replaces its source's verified index",
        },
    ),
    dir(
        "agents",
        "workspace agents",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "recipes",
        "workspace recipes",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "skills",
        "workspace skills",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "workflows",
        "workspace workflows",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "routes",
        "workspace routes",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "responses",
        "workspace responses",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "services",
        "workspace MCP services",
        "performance",
        Authored("removed with their declaration"),
    ),
    dir(
        "teams",
        "team workspaces",
        "identity",
        Authored("removed when the team is left"),
    ),
    dir(
        "served-api",
        "the served API's client keys",
        "models",
        Authored("one key per client"),
    ),
    // Machine and process singletons.
    file(
        "build-slot-residency-routes.json",
        "cairn-core orchestrator",
        "execution",
        Singleton("rewritten in place"),
    ),
    file(
        "keychain-read-health.json",
        "cairn-core keychain reads",
        "models",
        Singleton("rewritten in place"),
    ),
    file(
        crate::auth::MCP_SECRET_FILE,
        "cairn-common auth",
        "security",
        Singleton("the MCP callback secret"),
    ),
    // Spelled out rather than through `auth`'s constant: the registry names
    // the file and never reaches its value, and the operator-credential reach
    // audit (`cairn-core` `mcp::auth`) counts every file naming that constant.
    file(
        "operator_auth_secret",
        "cairn-common auth",
        "security",
        Singleton("the operator secret"),
    ),
    file(
        "orch.lock",
        "cairn-orch single owner",
        "execution",
        Singleton("the runner's owner lock"),
    ),
    dir(
        "executor-listener",
        "cairn-transport executor listener",
        "execution",
        Singleton("the listener certificate"),
    ),
    file(
        "executor_id",
        "cairn-exec",
        "execution",
        Singleton("the executor's identity"),
    ),
    file(
        "credential",
        "cairn-exec",
        "execution",
        Singleton("the executor's enrollment credential"),
    ),
    file(
        "executor.json",
        "cairn-exec",
        "execution",
        Singleton("the executor's configuration"),
    ),
    file(
        "update-attempt.json",
        "cairn-exec self update",
        "execution",
        Singleton("the last update attempt"),
    ),
    file(
        "cairn-exec.replaced",
        "cairn-exec self update",
        "execution",
        Replaced {
            enforcer: "self_update sweeps its siblings",
        },
    ),
    dir(
        "sock",
        "cairn runtime sockets",
        "execution",
        Singleton("recreated each launch"),
    ),
    file(
        ".cairn-runtime.lock",
        "cairn runtime",
        "execution",
        Singleton("recreated each launch"),
    ),
    file(
        crate::executor_paths::BUILD_ROOT_MARKER,
        "cairn-exec build root",
        "execution",
        Singleton("the build root's identity"),
    ),
    file(
        ".storage-reserve",
        "cairn-exec storage reserve",
        "execution",
        Singleton("a fixed-size reserve file"),
    ),
    // Retired layouts.
    dir(
        "worktrees",
        "none now",
        "performance",
        Legacy {
            cleanup: "cairn-transport state::cleanup_legacy_agent_residences",
        },
    ),
    file(
        "runner.lock",
        "the pre-orch runner",
        "execution",
        Legacy {
            cleanup: "retired_home recognizes it as a runner home's; the operator removes it",
        },
    ),
];

/// The registry entry `top` (a home-relative top-level name) belongs to,
/// refined by `second` (the name beneath it) where the entry's rule differs one
/// level down. Names match exactly.
pub fn classify(top: &str, second: Option<&str>) -> Option<&'static HomeEntry> {
    let entry = find(HOME_RETENTION, top)?;
    Some(
        second
            .and_then(|name| find(entry.children, name))
            .unwrap_or(entry),
    )
}

fn find(entries: &'static [HomeEntry], name: &str) -> Option<&'static HomeEntry> {
    entries.iter().find(|entry| entry.name == name)
}

/// The top-level names under `home` the registry does not claim, sorted.
pub fn unclaimed_entries(home: &Path) -> std::io::Result<Vec<String>> {
    let mut unclaimed = std::fs::read_dir(home)?
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| classify(name, None).is_none())
        .collect::<Vec<_>>();
    unclaimed.sort();
    Ok(unclaimed)
}

#[cfg(test)]
mod source_scan;
#[cfg(test)]
mod tests;
