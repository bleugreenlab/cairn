//! Resolve and introspect running `dev:instance`s for the `cairn://dev`
//! collection (`cairn://dev/db` and `cairn://dev/pid`).
//!
//! `bun run dev:instance` (scripts/dev-instance.ts) launches a branch-keyed dev
//! build whose home is `~/.cairn-dev-<key>` (key = slugified branch), database at
//! `<home>/cairn.db`, and — since the runner-daemon cutover — a `cairn-runner`
//! that owns that database and hosts the `/api/mcp` callback route on the runner
//! transport port `3849 + slot`, where the slot is persisted per branch in
//! `~/.cairn-dev-instances.json`. (Before the cutover this route lived on the
//! separate MCP callback port `3860 + slot`; the runner unified the two, so the
//! reachable port is now the runner port.) That running runner holds a process
//! lock on its own database file, so this module never opens the file directly:
//! it asks the instance's own runner to run the read-only `cairn://db` projection
//! over `/api/mcp` and relays the rows. The instance therefore re-validates the
//! read-only statement policy on its side, and a not-running instance surfaces as
//! an actionable error rather than a stale file read. See docs/dev-instances.md.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::future::join_all;

use cairn_common::paths;
use cairn_common::protocol::{CallbackRequest, CallbackResponse};
use cairn_common::query::{encode_query_params, QueryParam};
use cairn_common::read::ReadBatchEnvelope;

/// Ceiling on a dev instance's callback response. A running instance answers a
/// local read well under this; the cap just turns a hung/half-open port into a
/// prompt error instead of blocking the shared callback runtime.
const DEV_DB_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for a TCP connect when probing an instance's liveness.
const DEV_DB_PROBE_TIMEOUT: Duration = Duration::from_millis(300);

/// A registered `dev:instance`, resolved to everything needed to query it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DevInstance {
    /// Slug key; the instance home is `~/.cairn-dev-<key>`.
    pub key: String,
    /// Original branch name from the registry.
    pub branch: String,
    /// Instance home directory (holds the `mcp_auth_secret`).
    pub home: PathBuf,
    /// Runner transport port (`3849 + slot`); the instance's `cairn-runner`
    /// hosts the `/api/mcp` callback route on this port.
    pub runner_port: u16,
}

/// Why a selector could not be resolved to a single instance.
#[derive(Debug)]
pub(crate) enum ResolveError {
    /// No instances are registered at all.
    NoInstances,
    /// A selector matched no registered instance.
    NotFound {
        selector: String,
        available: Vec<String>,
    },
    /// No selector, several registered, and none are running.
    NoneRunning { available: Vec<String> },
    /// No selector, several registered, and more than one is running.
    AmbiguousRunning { running: Vec<String> },
}

impl ResolveError {
    pub fn message(&self) -> String {
        match self {
            ResolveError::NoInstances => "No dev instance is registered. Launch one from a worktree with `bun run dev:instance`, then query it with cairn://dev/db?sql=<read-only SQL>.".to_string(),
            ResolveError::NotFound { selector, available } => {
                if available.is_empty() {
                    format!("No dev instance matches '{selector}', and none are registered. Launch one with `bun run dev:instance --branch {selector}`.")
                } else {
                    format!("No dev instance matches '{selector}'. Registered: {}. Select one with ?at=<key>.", available.join(", "))
                }
            }
            ResolveError::NoneRunning { available } => format!("Several dev instances are registered ({}) but none are running. Start one with `bun run dev:instance`, or read cairn://dev to see their state.", available.join(", ")),
            ResolveError::AmbiguousRunning { running } => format!("Multiple dev instances are running ({}). Select one with ?at=<key>.", running.join(", ")),
        }
    }
}

/// Load the branch->slot registry written by the launcher. A missing or
/// malformed registry yields an empty map (treated as "no instances").
fn load_registry() -> BTreeMap<String, u32> {
    let Some(path) = paths::dev_instance_registry_path() else {
        return BTreeMap::new();
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return BTreeMap::new();
    };
    serde_json::from_str::<BTreeMap<String, u32>>(&text).unwrap_or_default()
}

/// Build the instance list from a home root and a branch->slot registry. Pure so
/// resolution is testable without touching the real home directory. An entry
/// whose branch slugifies to nothing or whose slot overflows the port space is
/// skipped; duplicate slug keys collapse to the first by sorted branch.
fn instances_from(home_root: &Path, registry: &BTreeMap<String, u32>) -> Vec<DevInstance> {
    let mut instances: Vec<DevInstance> = registry
        .iter()
        .filter_map(|(branch, slot)| {
            let key = paths::dev_instance_slug(branch)?;
            let port =
                paths::DEV_INSTANCE_RUNNER_PORT_BASE.checked_add(u16::try_from(*slot).ok()?)?;
            let home = home_root.join(format!("{}{key}", paths::DEV_INSTANCE_HOME_PREFIX));
            Some(DevInstance {
                key,
                branch: branch.clone(),
                home,
                runner_port: port,
            })
        })
        .collect();
    instances.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.branch.cmp(&b.branch)));
    instances.dedup_by(|a, b| a.key == b.key);
    instances
}

/// Every registered dev instance whose home directory still exists, sorted by
/// slug key. The home-exists filter drops dead registry entries (a branch whose
/// `~/.cairn-dev-<key>` was deleted) so they never appear in listings or count
/// toward ambiguity.
pub(crate) fn discover_instances() -> Vec<DevInstance> {
    let Some(home_root) = paths::os_home_dir() else {
        return Vec::new();
    };
    instances_from(&home_root, &load_registry())
        .into_iter()
        .filter(|inst| inst.home.is_dir())
        .collect()
}

fn keys(instances: &[DevInstance]) -> Vec<String> {
    instances.iter().map(|i| i.key.clone()).collect()
}

/// Pick an instance by selector (branch name or slug key).
fn select_by_key(instances: Vec<DevInstance>, selector: &str) -> Result<DevInstance, ResolveError> {
    let slug = paths::dev_instance_slug(selector);
    instances
        .iter()
        .find(|inst| slug.as_deref() == Some(inst.key.as_str()))
        .cloned()
        .ok_or_else(|| ResolveError::NotFound {
            selector: selector.to_string(),
            available: keys(&instances),
        })
}

/// Pick the instance to query when no selector is given. With one registered
/// instance, use it (the query reports if it is down); with several, use the
/// single running one (`running_keys`), else report an actionable ambiguity.
fn select_default(
    instances: Vec<DevInstance>,
    running_keys: &[String],
) -> Result<DevInstance, ResolveError> {
    match instances.len() {
        0 => Err(ResolveError::NoInstances),
        1 => Ok(instances.into_iter().next().unwrap()),
        _ => {
            let running: Vec<DevInstance> = instances
                .iter()
                .filter(|inst| running_keys.contains(&inst.key))
                .cloned()
                .collect();
            match running.len() {
                1 => Ok(running.into_iter().next().unwrap()),
                0 => Err(ResolveError::NoneRunning {
                    available: keys(&instances),
                }),
                _ => Err(ResolveError::AmbiguousRunning {
                    running: keys(&running),
                }),
            }
        }
    }
}

/// Resolve a selector (branch or slug key, or `None`) to one instance, probing
/// liveness only when needed to disambiguate several registered instances.
pub(crate) async fn resolve_instance(selector: Option<&str>) -> Result<DevInstance, ResolveError> {
    let instances = discover_instances();
    match selector {
        Some(sel) => select_by_key(instances, sel),
        None => {
            if instances.len() <= 1 {
                return select_default(instances, &[]);
            }
            let running = running_keys(&instances).await;
            select_default(instances, &running)
        }
    }
}

/// Probe every instance concurrently; return the slug keys that are running.
/// The registry accumulates an entry per branch ever launched, so a sequential
/// probe would be slow — these run together under one probe timeout.
async fn running_keys(instances: &[DevInstance]) -> Vec<String> {
    let probes = instances
        .iter()
        .map(|inst| async move { is_running(inst.runner_port).await.then(|| inst.key.clone()) });
    join_all(probes).await.into_iter().flatten().collect()
}

/// Best-effort liveness probe: a successful TCP connect to the instance's
/// callback port means its app process is up.
async fn is_running(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    matches!(
        tokio::time::timeout(DEV_DB_PROBE_TIMEOUT, tokio::net::TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

/// The inner `cairn://db` URI to run on the instance. Every component is
/// percent-encoded, so a `sql` value containing `&`/`=`/spaces round-trips
/// through the instance's query parser unambiguously.
fn build_db_uri(sql: &str, offset: usize, limit: usize) -> String {
    let params = vec![
        QueryParam {
            key: "offset".to_string(),
            value: offset.to_string(),
        },
        QueryParam {
            key: "limit".to_string(),
            value: limit.to_string(),
        },
        QueryParam {
            key: "sql".to_string(),
            value: sql.to_string(),
        },
    ];
    format!("cairn://db?{}", encode_query_params(&params))
}

/// Pull the row body out of a single-target `read_batch` envelope for
/// `cairn://db`. The composed text is `=== <uri> [..] ===\n<rows>\n\n<affordance>`;
/// SQL rows are escaped to single lines, so the first blank line marks the start
/// of the appended affordance. Everything between the frame header and that blank
/// line is the rows body (or the producer's single-line error message).
pub(crate) fn extract_db_body(text: &str) -> String {
    let after_header = match text.split_once('\n') {
        Some((first, rest)) if first.starts_with("=== ") && first.ends_with(" ===") => rest,
        // Single-line body (bare header / short error) or no frame: keep as-is.
        _ => text,
    };
    after_header
        .split("\n\n")
        .next()
        .unwrap_or(after_header)
        .trim_end()
        .to_string()
}

/// Run a read-only SQL projection against a running dev instance by relaying it
/// to that instance's own `cairn://db` over its MCP callback server.
pub(crate) async fn query_db(
    instance: &DevInstance,
    sql: &str,
    offset: usize,
    limit: usize,
) -> Result<String, String> {
    let token = cairn_common::auth::load_mcp_token_from(&instance.home).ok_or_else(|| {
        format!(
            "Could not read dev instance '{}' MCP secret at {}. Make sure the instance is running with `bun run dev:instance`.",
            instance.key,
            instance.home.join("mcp_auth_secret").display()
        )
    })?;

    let inner_uri = build_db_uri(sql, offset, limit);
    let request = CallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "read_batch".to_string(),
        payload: serde_json::json!({ "paths": [inner_uri] }),
        tool_use_id: None,
    };
    let url = format!("http://127.0.0.1:{}/api/mcp", instance.runner_port);

    let client = reqwest::Client::builder()
        .timeout(DEV_DB_HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("Failed to build HTTP client: {error}"))?;

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            format!(
                "Dev instance '{}' is not reachable on runner port {} ({error}). Start it from its worktree with `bun run dev:instance`, or pick another with cairn://dev/db?at=<key>.",
                instance.key, instance.runner_port
            )
        })?;

    let status = response.status();
    let body = response.text().await.map_err(|error| {
        format!(
            "Failed to read dev instance '{}' response: {error}",
            instance.key
        )
    })?;
    if !status.is_success() {
        return Err(format!(
            "Dev instance '{}' callback returned HTTP {}. Its MCP secret may have rotated; relaunch the instance.",
            instance.key,
            status.as_u16()
        ));
    }

    let callback: CallbackResponse = serde_json::from_str(&body).map_err(|error| {
        format!(
            "Could not parse dev instance '{}' response: {error}",
            instance.key
        )
    })?;
    let envelope: ReadBatchEnvelope = serde_json::from_str(&callback.result).map_err(|error| {
        format!(
            "Could not parse dev instance '{}' read envelope: {error}",
            instance.key
        )
    })?;
    Ok(extract_db_body(&envelope.text))
}

/// Ask a running dev instance for its own OS process id over its MCP callback
/// server. The instance answers the `process_info` tool with its
/// `std::process::id()` — authoritative and portable, no `lsof`, and it proves
/// liveness in the same round trip the db relay uses. Errors are short fragments
/// composed into a `pid_line`.
pub(crate) async fn query_pid(instance: &DevInstance) -> Result<u32, String> {
    let token = cairn_common::auth::load_mcp_token_from(&instance.home).ok_or_else(|| {
        format!(
            "MCP secret unreadable at {}",
            instance.home.join("mcp_auth_secret").display()
        )
    })?;

    let request = CallbackRequest {
        cwd: String::new(),
        run_id: None,
        tool: "process_info".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    };
    let url = format!("http://127.0.0.1:{}/api/mcp", instance.runner_port);

    let client = reqwest::Client::builder()
        .timeout(DEV_DB_HTTP_TIMEOUT)
        .build()
        .map_err(|error| format!("could not build HTTP client: {error}"))?;

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            format!(
                "not reachable on runner port {} ({error})",
                instance.runner_port
            )
        })?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| format!("response unreadable: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "callback returned HTTP {} (secret may have rotated; relaunch)",
            status.as_u16()
        ));
    }

    let callback: CallbackResponse =
        serde_json::from_str(&body).map_err(|error| format!("response unparseable: {error}"))?;
    parse_pid_body(&callback.result)
}

/// Parse the `process_info` callback result — a bare process id — into a pid.
fn parse_pid_body(result: &str) -> Result<u32, String> {
    result
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("returned an unexpected process id: {result:?}"))
}

/// Discovered instances partitioned into the running ones and a count of the
/// registered-but-stopped remainder, probing liveness once. Only running
/// instances can answer a query, and the registry accumulates every branch ever
/// launched, so stopped instances collapse to a single count.
async fn partition_live() -> (Vec<DevInstance>, usize) {
    let instances = discover_instances();
    if instances.is_empty() {
        return (Vec::new(), 0);
    }
    let running = running_keys(&instances).await;
    let total = instances.len();
    let live: Vec<DevInstance> = instances
        .into_iter()
        .filter(|inst| running.contains(&inst.key))
        .collect();
    let stopped = total - live.len();
    (live, stopped)
}

/// One `- key branch=… port=…` line for a running instance.
fn instance_line(inst: &DevInstance) -> String {
    format!(
        "- {} branch={} port={}",
        inst.key, inst.branch, inst.runner_port
    )
}

/// Render the `cairn://dev` collection entrypoint: the running instances plus
/// the available process-introspection sub-tools.
pub(crate) async fn render_collection() -> String {
    let (live, stopped) = partition_live().await;
    let mut lines = Vec::new();
    if live.is_empty() && stopped == 0 {
        lines.push("No dev instances are registered. Launch one from a worktree with `bun run dev:instance`.".to_string());
    } else if live.is_empty() {
        lines.push(format!(
            "No dev instances are running ({stopped} registered but stopped). Start one from its worktree with `bun run dev:instance`."
        ));
    } else {
        lines.push(format!(
            "{} running dev instance{}:",
            live.len(),
            if live.len() == 1 { "" } else { "s" }
        ));
        for inst in &live {
            lines.push(instance_line(inst));
        }
        if stopped > 0 {
            lines.push(format!(
                "({stopped} other registered instance{} stopped; not shown)",
                if stopped == 1 { " is" } else { "s are" }
            ));
        }
    }
    lines.push(String::new());
    lines.push("Process-introspection tools for a running instance you launched:".to_string());
    lines.push("- cairn://dev/db   read-only SQL against the instance database (?sql=<read-only SQL>, ?at=<branch-or-key>)".to_string());
    lines.push("- cairn://dev/pid  OS process id(s) of running instance(s), to target with external tools like Axon (?at=<branch-or-key>)".to_string());
    lines.join("\n")
}

/// Render the discovery listing for `read cairn://dev/db` (no `?sql`). Only
/// *running* instances are listed; stopped ones are summarized as a count.
pub(crate) async fn render_listing() -> String {
    let (live, stopped) = partition_live().await;
    if live.is_empty() && stopped == 0 {
        return "No dev instances are registered. Launch one from a worktree with `bun run dev:instance`, then query it with cairn://dev/db?sql=<read-only SQL>.".to_string();
    }
    if live.is_empty() {
        return format!(
            "No dev instances are running ({stopped} registered but stopped). Start one from its worktree with `bun run dev:instance`, then query cairn://dev/db?sql=<read-only SQL>."
        );
    }
    let mut lines = vec![format!(
        "{} running dev instance{} (query with cairn://dev/db?at=<key>&sql=<read-only SQL>):",
        live.len(),
        if live.len() == 1 { "" } else { "s" }
    )];
    for inst in &live {
        lines.push(instance_line(inst));
    }
    if stopped > 0 {
        lines.push(format!(
            "({stopped} other registered instance{} stopped; not shown)",
            if stopped == 1 { " is" } else { "s are" }
        ));
    }
    lines.push(
        "The selector accepts the branch name or its slug key. With exactly one running instance, ?at= is optional.".to_string(),
    );
    lines.join("\n")
}

/// Render `cairn://dev/pid`. With a selector, report that one instance's pid;
/// without, report the pid of every running instance. Each pid is the instance
/// reporting its own `std::process::id()` over its callback server.
pub(crate) async fn render_pids(selector: Option<&str>) -> String {
    let instances = discover_instances();
    if instances.is_empty() {
        return "No dev instance is registered. Launch one from a worktree with `bun run dev:instance`, then read cairn://dev/pid.".to_string();
    }
    if let Some(sel) = selector {
        return match select_by_key(instances, sel) {
            Ok(inst) => pid_line(&inst).await,
            Err(error) => error.message(),
        };
    }
    let running = running_keys(&instances).await;
    let live: Vec<&DevInstance> = instances
        .iter()
        .filter(|inst| running.contains(&inst.key))
        .collect();
    if live.is_empty() {
        return format!(
            "No dev instances are running ({} registered but stopped). Start one from its worktree with `bun run dev:instance`, then read cairn://dev/pid.",
            instances.len()
        );
    }
    join_all(live.iter().map(|inst| pid_line(inst)))
        .await
        .join("\n")
}

/// A single `cairn://dev/pid` line for one instance: its reported pid, or an
/// inline reason it could not be reached.
async fn pid_line(inst: &DevInstance) -> String {
    match query_pid(inst).await {
        Ok(pid) => format!(
            "- {} branch={} pid={} port={}",
            inst.key, inst.branch, pid, inst.runner_port
        ),
        Err(error) => format!(
            "- {} branch={} unavailable ({error})",
            inst.key, inst.branch
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(entries: &[(&str, u32)]) -> BTreeMap<String, u32> {
        entries
            .iter()
            .map(|(branch, slot)| (branch.to_string(), *slot))
            .collect()
    }

    #[test]
    fn instances_from_derives_key_home_and_port() {
        let root = Path::new("/home/u");
        let reg = registry(&[("agent/CAIRN-1928-builder-0", 3)]);
        let instances = instances_from(root, &reg);
        assert_eq!(instances.len(), 1);
        let inst = &instances[0];
        assert_eq!(inst.key, "agent-cairn-1928-builder-0");
        assert_eq!(inst.branch, "agent/CAIRN-1928-builder-0");
        assert_eq!(inst.runner_port, paths::DEV_INSTANCE_RUNNER_PORT_BASE + 3); // 3849 + slot 3
        assert_eq!(
            inst.home,
            Path::new("/home/u/.cairn-dev-agent-cairn-1928-builder-0")
        );
    }

    #[test]
    fn instances_from_skips_unsluggable_branches() {
        let reg = registry(&[("///", 1), ("main", 2)]);
        let instances = instances_from(Path::new("/h"), &reg);
        assert_eq!(keys(&instances), vec!["main".to_string()]);
    }

    #[test]
    fn select_by_key_accepts_branch_or_slug() {
        let reg = registry(&[("feature/Foo Bar", 1)]);
        let instances = instances_from(Path::new("/h"), &reg);
        // By original branch...
        assert!(select_by_key(instances.clone(), "feature/Foo Bar").is_ok());
        // ...and by the already-slugified key.
        assert!(select_by_key(instances.clone(), "feature-foo-bar").is_ok());
        // A miss reports what is available.
        let err = select_by_key(instances, "nope").unwrap_err();
        assert!(matches!(err, ResolveError::NotFound { .. }));
        assert!(err.message().contains("feature-foo-bar"));
    }

    #[test]
    fn select_default_rules() {
        // None registered.
        assert!(matches!(
            select_default(vec![], &[]).unwrap_err(),
            ResolveError::NoInstances
        ));

        let many = instances_from(Path::new("/h"), &registry(&[("a", 1), ("b", 2)]));
        // Exactly one registered -> used without probing.
        let one = instances_from(Path::new("/h"), &registry(&[("solo", 1)]));
        assert_eq!(select_default(one, &[]).unwrap().key, "solo");
        // Several registered, one running -> that one.
        assert_eq!(
            select_default(many.clone(), &["b".to_string()])
                .unwrap()
                .key,
            "b"
        );
        // Several registered, none running -> NoneRunning.
        assert!(matches!(
            select_default(many.clone(), &[]).unwrap_err(),
            ResolveError::NoneRunning { .. }
        ));
        // Several running -> AmbiguousRunning.
        assert!(matches!(
            select_default(many, &["a".to_string(), "b".to_string()]).unwrap_err(),
            ResolveError::AmbiguousRunning { .. }
        ));
    }

    #[test]
    fn build_db_uri_percent_encodes_sql() {
        let uri = build_db_uri("SELECT * FROM issues WHERE a & b", 5, 10);
        assert!(uri.starts_with("cairn://db?offset=5&limit=10&sql="));
        // Space and ampersand are encoded, not left literal, so the inner parser
        // keeps them inside the sql value.
        assert!(uri.contains("%20"));
        assert!(uri.contains("%26"));
        assert!(!uri.contains("a & b"));
    }

    #[test]
    fn extract_db_body_strips_frame_and_affordance() {
        let text = "=== cairn://db?sql=... [2 rows] ===\nid\tname\n1\tA\n2\tB\n\n## Live database SQL projection\n### filters\n- sql=...";
        assert_eq!(extract_db_body(text), "id\tname\n1\tA\n2\tB");
    }

    #[test]
    fn extract_db_body_keeps_single_line_error() {
        let text = "=== cairn://db?sql=... ===\nSQL query failed: no such table: bogus\n\n## Live database SQL projection";
        assert_eq!(
            extract_db_body(text),
            "SQL query failed: no such table: bogus"
        );
    }

    #[test]
    fn extract_db_body_handles_zero_rows() {
        let text = "=== cairn://db?sql=... [0 rows] ===\nid\tname\n(0 rows)\n\n## affordance";
        assert_eq!(extract_db_body(text), "id\tname\n(0 rows)");
    }

    #[test]
    fn parse_pid_body_reads_a_bare_pid() {
        // The `process_info` callback result is a bare process id, optionally
        // with surrounding whitespace.
        assert_eq!(parse_pid_body("12345").unwrap(), 12345);
        assert_eq!(parse_pid_body("  678\n").unwrap(), 678);
    }

    #[test]
    fn parse_pid_body_rejects_non_numeric() {
        assert!(parse_pid_body("").is_err());
        assert!(parse_pid_body("not a pid").is_err());
        // A framed/error body (not a bare pid) is rejected rather than silently
        // yielding a bogus pid.
        assert!(parse_pid_body("Unknown tool: process_info").is_err());
    }
}
