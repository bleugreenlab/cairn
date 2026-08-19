//! Text rendering for `cairn://p/{project}/map`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use cairn_common::executor_protocol::{EnrolledRemote, ExecutorHealthStatus, ExecutorInspection};
use cairn_common::query::QueryParam;

use crate::orchestrator::Orchestrator;
use crate::projects::codemap::{self, CodeMap, CodeMapFile};
use crate::storage::{LocalDb, RowExt};

use super::common::{connect_for_read, lookup_project_by_key};

const WAKE_SECONDS: i64 = 60 * 60;
const DEFAULT_LINES: usize = 25;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Activity {
    path: String,
    agent: String,
    action: String,
    at: i64,
    running: bool,
}

fn load_bar(value: f64) -> String {
    let filled = (value.clamp(0.0, 1.0) * 10.0).round() as usize;
    format!("{}{}", "▓".repeat(filled), "░".repeat(10 - filled))
}

#[derive(Debug, Clone, PartialEq)]
struct Machine {
    name: String,
    reachable: bool,
    silence_ms: u64,
    cpu: Option<f64>,
    memory: Option<f64>,
    agents: Vec<String>,
}

#[derive(Debug, Clone)]
enum MapState<'a> {
    Fresh(&'a CodeMap),
    Stale(&'a CodeMap),
    Computing(Option<&'a str>),
    Unavailable,
}

pub(crate) async fn read_project_map(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    let path = match map_path(params) {
        Ok(path) => path,
        Err(error) => return error,
    };
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project = match lookup_project_by_key(&conn, project_key).await {
        Ok(project) => project,
        Err(error) => return error,
    };
    let now = chrono::Utc::now().timestamp();
    let activity = load_activity(&conn, &project.project_id, now).await;
    drop(conn);

    let view = match codemap::current(orch, db, &project.project_id).await {
        Ok(view) => view,
        Err(_) => {
            return render_map(
                project_key,
                &path,
                MapState::Unavailable,
                &activity,
                &machines(orch),
                now,
            );
        }
    };
    let state = match (view.map.as_ref(), view.stale, view.unmappable) {
        (_, _, true) => MapState::Unavailable,
        (Some(map), true, false) => MapState::Stale(map),
        (Some(map), false, false) => MapState::Fresh(map),
        (None, false, false) => MapState::Computing(view.head.as_deref()),
        (None, true, false) => MapState::Computing(view.head.as_deref()),
    };
    render_map(project_key, &path, state, &activity, &machines(orch), now)
}

fn map_path(params: &[QueryParam]) -> Result<String, String> {
    let unsupported = params.iter().find(|param| param.key != "path");
    if let Some(param) = unsupported {
        return Err(format!(
            "Unsupported project map query parameter: {}",
            param.key
        ));
    }
    let path = params
        .iter()
        .find(|param| param.key == "path")
        .map(|param| param.value.trim_matches('/'))
        .unwrap_or("");
    if path.split('/').any(|part| part == "." || part == "..") {
        return Err("Project map path must stay within the project.".into());
    }
    Ok(path.to_string())
}

async fn load_activity(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    now: i64,
) -> Vec<Activity> {
    let mut rows = match conn
        .query(
            "SELECT a.file_path, COALESCE(j.node_name, j.uri_segment, 'agent'),              a.action, a.created_at, j.status = 'running'              FROM job_file_activity a JOIN jobs j ON j.id = a.job_id              WHERE j.project_id = ?1 AND a.created_at >= ?2              ORDER BY a.created_at DESC",
            cairn_db::turso::params![project_id, now - WAKE_SECONDS],
        )
        .await
    {
        Ok(rows) => rows,
        Err(_) => return Vec::new(),
    };
    let mut activity = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        let item = (|| -> cairn_db::storage::DbResult<Activity> {
            Ok(Activity {
                path: row.text(0)?,
                agent: row.text(1)?,
                action: row.text(2)?,
                at: row.i64(3)?,
                running: row.i64(4)? != 0,
            })
        })();
        if let Ok(item) = item {
            activity.push(item);
        }
    }
    activity
}

fn machines(orch: &Orchestrator) -> Vec<Machine> {
    let now = crate::fleet::unix_time_ms();
    let mut values: Vec<_> = orch
        .fleet
        .inspect_executors(now)
        .into_iter()
        .map(|executor| attached_machine(&executor))
        .collect();
    values.extend(
        orch.fleet
            .unattached_enrolled_remotes()
            .iter()
            .map(|remote| remote_machine(remote, now)),
    );
    values.sort_by(|a, b| a.name.cmp(&b.name));
    values
}

fn attached_machine(executor: &ExecutorInspection) -> Machine {
    let cpu = executor
        .health
        .machine
        .cpu
        .value()
        .map(|value| value.utilization);
    let memory = executor.health.machine.memory.value().and_then(|value| {
        (value.total_bytes > 0).then_some(value.used_bytes() as f64 / value.total_bytes as f64)
    });
    let mut agents = executor
        .occupancy
        .executing_requests
        .iter()
        .filter_map(|request| request.owner.as_ref())
        .map(display_owner)
        .collect::<Vec<_>>();
    agents.sort();
    agents.dedup();
    Machine {
        name: executor.name.clone(),
        reachable: executor.health.status == ExecutorHealthStatus::Online,
        silence_ms: executor.health.heartbeat_age_ms,
        cpu,
        memory,
        agents,
    }
}

fn display_owner(owner: &cairn_common::executor_protocol::CellOwnerRef) -> String {
    let role = owner.node_kind.as_deref().unwrap_or("agent");
    match owner.issue_number {
        Some(issue) => format!("{role}/{issue}"),
        None => role.to_string(),
    }
}

fn remote_machine(remote: &EnrolledRemote, now: u64) -> Machine {
    Machine {
        name: remote.name.clone(),
        reachable: false,
        silence_ms: remote
            .last_seen_unix_ms
            .map(|seen| now.saturating_sub(seen))
            .or_else(|| {
                remote
                    .last_attempt
                    .as_ref()
                    .map(|attempt| now.saturating_sub(attempt.attempted_at_unix_ms))
            })
            .unwrap_or_default(),
        cpu: None,
        memory: None,
        agents: Vec::new(),
    }
}

fn render_map(
    project: &str,
    path: &str,
    state: MapState<'_>,
    activity: &[Activity],
    machines: &[Machine],
    now: i64,
) -> String {
    let map = match state {
        MapState::Fresh(map) | MapState::Stale(map) => Some(map),
        MapState::Computing(_) | MapState::Unavailable => None,
    };
    let revision = map
        .map(|map| short(&map.base_commit_sha))
        .unwrap_or("unknown");
    let status = match state {
        MapState::Fresh(map) => {
            format!("codemap {} fresh", age(now - map.computed_at))
        }
        MapState::Stale(_) => "codemap stale (base advanced, recomputing)".into(),
        MapState::Computing(Some(head)) => {
            format!("codemap computing @ {}", short(head))
        }
        MapState::Computing(None) => "codemap computing".into(),
        MapState::Unavailable => "codemap unavailable".into(),
    };
    let active = activity
        .iter()
        .filter(|item| item.running)
        .map(|item| item.agent.as_str())
        .collect::<std::collections::HashSet<_>>()
        .len();
    let reachable = machines.iter().filter(|item| item.reachable).count();
    let identity = if path.is_empty() {
        format!("{} — map", display_identity(project, 16))
    } else {
        format!("{project} — map/{path}")
    };
    let mut out = String::new();
    let header_status = if path.is_empty() {
        format!(
            "{status} · @ {revision} · {active} {} · {reachable}/{} {}",
            plural(active, "agent", "agents"),
            machines.len(),
            plural(machines.len(), "machine", "machines")
        )
    } else {
        format!("@ {revision}")
    };
    push_identity_row(&mut out, "", &identity, &header_status);

    if let Some(map) = map {
        out.push('\n');
        if path.is_empty() {
            render_overview(&mut out, map, activity, now);
        } else {
            render_drilldown(&mut out, map, path, activity, now);
        }
    }
    render_fleet(&mut out, machines);
    debug_assert!(out.lines().all(|line| line.chars().count() <= 80));
    out
}

#[derive(Default)]
struct DirectoryNode {
    path: String,
    files: Vec<usize>,
    children: BTreeMap<String, DirectoryNode>,
}

impl DirectoryNode {
    fn insert(&mut self, path: &str, file_index: usize) {
        let mut node = self;
        let parts = path.split('/').collect::<Vec<_>>();
        for part in &parts[..parts.len().saturating_sub(1)] {
            node = node.children.entry((*part).to_string()).or_insert_with(|| {
                let child_path = if node.path.is_empty() {
                    (*part).to_string()
                } else {
                    format!("{}/{}", node.path, part)
                };
                DirectoryNode {
                    path: child_path,
                    ..Default::default()
                }
            });
        }
        node.files.push(file_index);
    }

    fn contains_file(&self, file: &str) -> bool {
        file.starts_with(&format!("{}/", self.path))
    }
}

fn render_overview(out: &mut String, map: &CodeMap, activity: &[Activity], now: i64) {
    let mut tree = DirectoryNode::default();
    for (index, file) in map.files.iter().enumerate() {
        tree.insert(&file.path, index);
    }
    let occupied = activity
        .iter()
        .map(|item| item.path.as_str())
        .collect::<HashSet<_>>();
    render_children(out, map, &tree, activity, &occupied, now, 0);
    if map.files.is_empty() {
        out.push_str("(empty code map)\n");
    }
}

fn render_children(
    out: &mut String,
    map: &CodeMap,
    parent: &DirectoryNode,
    activity: &[Activity],
    occupied: &HashSet<&str>,
    now: i64,
    depth: usize,
) {
    let children = coupling_order(map, parent.children.values().collect());
    for child in children {
        if out.lines().count() >= DEFAULT_LINES {
            return;
        }
        let files = descendant_files(map, child);
        let language = dominant_language(&files);
        let identity = format!("{}/", local_name(&child.path));
        let edge = heaviest_edge(map, &child.path);
        let trailing = format!(
            "── {} {} {} {}{}",
            display_identity(language, 10),
            heat(files.iter().map(|file| churn(file)).sum()),
            files.len(),
            plural(files.len(), "file", "files"),
            edge
        );
        push_identity_row(out, &"  ".repeat(depth), &identity, &trailing);

        let branch_is_occupied = occupied.iter().any(|path| child.contains_file(path));
        if !branch_is_occupied {
            continue;
        }

        for file_index in &child.files {
            let file = &map.files[*file_index];
            if !occupied.contains(file.path.as_str()) {
                continue;
            }
            render_activity_file(out, file, activity, now, depth + 1);
        }
        render_children(out, map, child, activity, occupied, now, depth + 1);
    }
}

fn render_activity_file(
    out: &mut String,
    file: &CodeMapFile,
    activity: &[Activity],
    now: i64,
    depth: usize,
) {
    let Some(item) = activity
        .iter()
        .filter(|item| item.path == file.path)
        .max_by_key(|item| item.at)
    else {
        return;
    };
    let marker = if item.running { "●" } else { "·" };
    let identity = display_identity(local_name(&file.path), 60usize.saturating_sub(depth * 2));
    let trailing = if item.running {
        format!(
            "{} {} {}",
            display_identity(&item.agent, 16),
            action_arrow(&item.action),
            age(now - item.at)
        )
    } else {
        age(now - item.at)
    };
    push_identity_row(
        out,
        &format!("{}{} ", "  ".repeat(depth), marker),
        &identity,
        &trailing,
    );
}

fn descendant_files<'a>(map: &'a CodeMap, node: &DirectoryNode) -> Vec<&'a CodeMapFile> {
    let prefix = format!("{}/", node.path);
    map.files
        .iter()
        .filter(|file| file.path.starts_with(&prefix))
        .collect()
}

fn coupling_order<'a>(map: &CodeMap, children: Vec<&'a DirectoryNode>) -> Vec<&'a DirectoryNode> {
    if children.len() < 2 {
        return children;
    }
    let mut weights = HashMap::<(usize, usize), usize>::new();
    for (from, to) in &map.imports {
        let left = children.iter().position(|child| child.contains_file(from));
        let right = children.iter().position(|child| child.contains_file(to));
        if let (Some(a), Some(b)) = (left, right) {
            if a != b {
                let pair = if a < b { (a, b) } else { (b, a) };
                *weights.entry(pair).or_default() += 1;
            }
        }
    }
    let Some((&seed, _)) = weights
        .iter()
        .max_by(|(a_pair, a_weight), (b_pair, b_weight)| {
            a_weight.cmp(b_weight).then_with(|| b_pair.cmp(a_pair))
        })
    else {
        return children;
    };
    let mut order = vec![seed.0, seed.1];
    let mut remaining = (0..children.len())
        .filter(|index| !order.contains(index))
        .collect::<BTreeSet<_>>();
    while !remaining.is_empty() {
        let next = *remaining
            .iter()
            .max_by(|a, b| {
                let affinity = |candidate: usize| {
                    order
                        .iter()
                        .map(|placed| {
                            let pair = if candidate < *placed {
                                (candidate, *placed)
                            } else {
                                (*placed, candidate)
                            };
                            weights.get(&pair).copied().unwrap_or(0)
                        })
                        .sum::<usize>()
                };
                affinity(**a)
                    .cmp(&affinity(**b))
                    .then_with(|| children[**b].path.cmp(&children[**a].path))
            })
            .expect("remaining is non-empty");
        remaining.remove(&next);
        order.push(next);
    }
    order.into_iter().map(|index| children[index]).collect()
}

fn heaviest_edge(map: &CodeMap, directory: &str) -> String {
    let prefix = format!("{directory}/");
    let mut weights: HashMap<String, usize> = HashMap::new();
    for (from, to) in &map.imports {
        let other = if from.starts_with(&prefix) && !to.starts_with(&prefix) {
            Some(to.as_str())
        } else if to.starts_with(&prefix) && !from.starts_with(&prefix) {
            Some(from.as_str())
        } else {
            None
        };
        if let Some(other) = other {
            let other_directory = other.rsplit_once('/').map(|(dir, _)| dir).unwrap_or(other);
            *weights.entry(other_directory.to_string()).or_default() += 1;
        }
    }
    weights
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
        .map(|(name, _)| format!(" ⇄ {}", compact_target(&name)))
        .unwrap_or_default()
}

fn render_drilldown(out: &mut String, map: &CodeMap, path: &str, activity: &[Activity], now: i64) {
    let prefix = format!("{}/", path.trim_matches('/'));
    let files: Vec<_> = map
        .files
        .iter()
        .filter(|file| file.path.starts_with(&prefix))
        .collect();
    if files.is_empty() {
        out.push_str("  no files at this path\n");
        return;
    }
    let mut tree = DirectoryNode::default();
    tree.path = path.trim_matches('/').to_string();
    for file in &files {
        tree.insert(
            file.path.strip_prefix(&prefix).unwrap_or(&file.path),
            map.files
                .iter()
                .position(|item| std::ptr::eq(item, *file))
                .unwrap(),
        );
    }
    render_drilldown_node(out, map, &tree, activity, now, 0);
    render_imports(out, map, &prefix);
}

fn render_drilldown_node(
    out: &mut String,
    map: &CodeMap,
    node: &DirectoryNode,
    activity: &[Activity],
    now: i64,
    depth: usize,
) {
    let mut files = node
        .files
        .iter()
        .map(|index| &map.files[*index])
        .collect::<Vec<_>>();
    files.sort_by_key(|file| std::cmp::Reverse(churn(file)));
    for file in files {
        let latest = activity
            .iter()
            .filter(|item| item.path == file.path)
            .max_by_key(|item| item.at);
        let mark = latest
            .map(|item| {
                if item.running {
                    format!("● {} {}", item.agent, action_arrow(&item.action))
                } else {
                    format!("· {}", age(now - item.at))
                }
            })
            .unwrap_or_default();
        push_identity_row(
            out,
            &"  ".repeat(depth + 1),
            &display_identity(local_name(&file.path), 32),
            &format!("{} {}L {mark}", heat(churn(file)), file.line_count),
        );
    }
    for child in coupling_order(map, node.children.values().collect()) {
        let descendants = descendant_files(map, child);
        push_identity_row(
            out,
            &"  ".repeat(depth + 1),
            &format!("{}/", local_name(&child.path)),
            &format!(
                "{} {} {}",
                heat(descendants.iter().map(|file| churn(file)).sum()),
                descendants.len(),
                plural(descendants.len(), "file", "files")
            ),
        );
        render_drilldown_node(out, map, child, activity, now, depth + 1);
    }
}

fn local_name(path: &str) -> &str {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(path)
}

fn compact_target(path: &str) -> &str {
    local_name(path)
}

fn plural<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

fn render_imports(out: &mut String, map: &CodeMap, prefix: &str) {
    let mut outbound = Vec::new();
    let mut inbound = 0;
    for (from, to) in &map.imports {
        if from.starts_with(prefix) && !to.starts_with(prefix) {
            outbound.push(to.as_str());
        }
        if to.starts_with(prefix) && !from.starts_with(prefix) {
            inbound += 1;
        }
    }
    outbound.sort_unstable();
    outbound.dedup();
    let has_outbound = !outbound.is_empty();
    if has_outbound {
        let shown = outbound.into_iter().take(3).collect::<Vec<_>>();
        out.push('\n');
        push_identity_row(
            out,
            "  imports → ",
            &display_identity(shown[0], 45),
            &shown[1..].join(", "),
        );
    }
    if inbound > 0 {
        if !has_outbound {
            out.push('\n');
        }
        out.push_str(&format!(
            "  imported by ← {inbound} {}\n",
            plural(inbound, "file", "files")
        ));
    }
}

fn render_fleet(out: &mut String, machines: &[Machine]) {
    out.push_str("\nfleet ─────────────────────────────────────────────\n");
    if machines.is_empty() {
        out.push_str("  no machines reporting\n");
    }
    for machine in machines {
        if machine.reachable {
            let cpu = machine
                .cpu
                .map(load_bar)
                .unwrap_or_else(|| "unknown".into());
            let memory = machine
                .memory
                .map(load_bar)
                .unwrap_or_else(|| "unknown".into());
            let residents = if machine.agents.is_empty() {
                String::new()
            } else {
                format!("  ● {}", machine.agents.join(", "))
            };
            push_identity_row(
                out,
                "  ",
                &display_identity(&machine.name, 20),
                &format!("cpu {cpu} mem {memory}{residents}"),
            );
        } else {
            push_identity_row(
                out,
                "  ",
                &display_identity(&machine.name, 42),
                &format!("✕ unreachable {}", age_ms(machine.silence_ms)),
            );
        }
    }
}

fn dominant_language<'a>(files: &[&'a CodeMapFile]) -> &'a str {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for file in files {
        *counts.entry(&file.language).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(language, _)| language)
        .unwrap_or("other")
}

fn churn(file: &CodeMapFile) -> i64 {
    file.churn_additions + file.churn_deletions
}

fn heat(value: i64) -> &'static str {
    match value {
        0 => "░░░░",
        1..=9 => "▓░░░",
        10..=99 => "▓▓░░",
        100..=999 => "▓▓▓░",
        _ => "▓▓▓▓",
    }
}

fn action_arrow(action: &str) -> &'static str {
    match action {
        "read" => "⌔",
        "edit" | "create" | "delete" => "→",
        _ => "·",
    }
}

fn display_identity(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let head = value
        .chars()
        .take(max.saturating_sub(1))
        .collect::<String>();
    format!("{head}…")
}

fn push_identity_row(out: &mut String, prefix: &str, identity: &str, trailing: &str) {
    let used = prefix.chars().count() + identity.chars().count();
    let available = 80usize.saturating_sub(used);
    out.push_str(prefix);
    out.push_str(identity);
    if available > 1 && !trailing.is_empty() {
        out.push(' ');
        out.push_str(&display_identity(trailing, available - 1));
    }
    out.push('\n');
}

fn age(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

fn age_ms(milliseconds: u64) -> String {
    age((milliseconds / 1_000) as i64)
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(8)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, churn: i64) -> CodeMapFile {
        CodeMapFile {
            path: path.into(),
            language: if path.ends_with(".rs") {
                "rust".into()
            } else {
                "ts".into()
            },
            line_count: 42,
            size_bytes: 100,
            churn_additions: churn,
            churn_deletions: 0,
        }
    }

    fn fixture() -> CodeMap {
        CodeMap {
            base_commit_sha: "1234567890abcdef".into(),
            computed_at: 940,
            files: vec![
                file("src/lib.ts", 5),
                file("src/map/view.ts", 20),
                file("rust/store.rs", 200),
            ],
            imports: vec![("rust/store.rs".into(), "src/lib.ts".into())],
        }
    }

    fn activity() -> Vec<Activity> {
        vec![
            Activity {
                path: "rust/store.rs".into(),
                agent: "builder/7".into(),
                action: "edit".into(),
                at: 980,
                running: true,
            },
            Activity {
                path: "src/lib.ts".into(),
                agent: "explorer/8".into(),
                action: "read".into(),
                at: 970,
                running: false,
            },
        ]
    }

    #[test]
    fn overview_collapses_quiet_subtrees_and_expands_activity() {
        let rendered = render_map(
            "demo",
            "",
            MapState::Fresh(&fixture()),
            &activity(),
            &[],
            1_000,
        );
        assert!(rendered.contains("rust/ ── rust"));
        assert!(rendered.contains("● store.rs builder/7 →"));
        assert!(rendered.contains("src/ ── ts"));
        assert!(rendered.contains("  · lib.ts 30s"));
        assert!(!rendered.contains("src/map/view.ts"));
    }

    #[test]
    fn drilldown_renders_files_glyphs_and_imports() {
        let rendered = render_map(
            "demo",
            "rust",
            MapState::Fresh(&fixture()),
            &activity(),
            &[],
            1_000,
        );
        assert!(rendered.contains("store.rs"));
        assert!(rendered.contains("● builder/7 →"));
        assert!(rendered.contains("imports → src/lib.ts"));
    }

    #[test]
    fn realistic_overview_matches_golden_render() {
        let rendered = render_map(
            "demo",
            "",
            MapState::Fresh(&fixture()),
            &activity(),
            &[Machine {
                name: "local".into(),
                reachable: true,
                silence_ms: 0,
                cpu: Some(0.2),
                memory: Some(0.5),
                agents: vec!["builder/7".into()],
            }],
            1_000,
        );
        assert_eq!(rendered, "demo — map codemap 1m fresh · @ 12345678 · 1 agent · 1/1 machine\n\nrust/ ── rust ▓▓▓░ 1 file ⇄ src\n  ● store.rs builder/7 → 20s\nsrc/ ── ts ▓▓░░ 2 files ⇄ rust\n  · lib.ts 30s\n  map/ ── ts ▓▓░░ 1 file\n\nfleet ─────────────────────────────────────────────\n  local cpu ▓▓░░░░░░░░ mem ▓▓▓▓▓░░░░░  ● builder/7\n");
    }

    #[test]
    fn nested_drilldown_matches_complete_golden_render() {
        let rendered = render_map(
            "demo",
            "src",
            MapState::Fresh(&fixture()),
            &activity(),
            &[],
            1_000,
        );
        assert_eq!(rendered, "demo — map/src @ 12345678\n\n  lib.ts ▓░░░ 42L · 30s\n  map/ ▓▓░░ 1 file\n    view.ts ▓▓░░ 42L \n\n  imported by ← 1 file\n\nfleet ─────────────────────────────────────────────\n  no machines reporting\n");
    }

    #[test]
    fn ages_use_plain_compact_units() {
        assert_eq!(age(59), "59s");
        assert_eq!(age(120), "2m");
        assert_eq!(age(7_200), "2h");
        assert_eq!(age(172_800), "2d");
    }

    #[test]
    fn codemap_states_are_honest() {
        let machines = Vec::new();
        let computing = render_map(
            "demo",
            "",
            MapState::Computing(Some("abcdef123")),
            &[],
            &machines,
            1_000,
        );
        let stale = render_map(
            "demo",
            "",
            MapState::Stale(&fixture()),
            &[],
            &machines,
            1_000,
        );
        let unavailable = render_map("demo", "", MapState::Unavailable, &[], &machines, 1_000);
        assert!(computing.contains("codemap computing @ abcdef12"));
        assert!(stale.contains("codemap stale (base advanced, recomputing)"));
        assert!(unavailable.contains("codemap unavailable"));
    }

    #[test]
    fn unreachable_machines_keep_their_silence_age() {
        let rendered = render_map(
            "demo",
            "",
            MapState::Unavailable,
            &[],
            &[Machine {
                name: "bbs".into(),
                reachable: false,
                silence_ms: 172_800_000,
                cpu: None,
                memory: None,
                agents: Vec::new(),
            }],
            1_000,
        );
        assert!(rendered.contains("bbs"));
        assert!(rendered.contains("✕ unreachable 2d"));
    }

    #[test]
    fn every_rendered_line_stays_within_terminal_width() {
        let rendered = render_map(
            "a-very-long-project-name",
            "",
            MapState::Fresh(&fixture()),
            &activity(),
            &[],
            1_000,
        );
        assert!(
            rendered.lines().all(|line| line.chars().count() <= 80),
            "{rendered}"
        );
    }

    #[test]
    fn glyphs_distinguish_reads_from_writes() {
        assert_eq!(action_arrow("read"), "⌔");
        assert_eq!(action_arrow("edit"), "→");
    }

    #[test]
    fn fleet_renders_telemetry_and_resident_agents() {
        let machine = Machine {
            name: "worker".into(),
            reachable: true,
            silence_ms: 0,
            cpu: Some(0.5),
            memory: Some(0.2),
            agents: vec!["builder/7".into()],
        };
        let mut rendered = String::new();
        render_fleet(&mut rendered, &[machine]);
        assert!(rendered.contains("cpu ▓▓▓▓▓░░░░░ mem ▓▓░░░░░░░░"));
        assert!(rendered.contains("● builder/7"));
    }

    #[test]
    fn occupied_nested_paths_expand_to_file_while_quiet_siblings_collapse() {
        let map = CodeMap {
            base_commit_sha: "abc".into(),
            computed_at: 1,
            files: vec![
                file("src/feature/deep/active.rs", 1),
                file("src/feature/deep/quiet.rs", 999),
                file("src/quiet/hidden.rs", 999),
            ],
            imports: vec![],
        };
        let activity = vec![Activity {
            path: "src/feature/deep/active.rs".into(),
            agent: "builder".into(),
            action: "edit".into(),
            at: 9,
            running: true,
        }];
        let rendered = render_map("demo", "", MapState::Fresh(&map), &activity, &[], 10);
        assert!(rendered.contains("  feature/"));
        assert!(rendered.contains("    deep/"));
        assert!(rendered.contains("● active.rs builder →"));
        assert!(rendered.contains("  quiet/"));
        assert!(!rendered.contains("src/quiet/hidden.rs"));
        assert!(!rendered.contains("src/feature/deep/quiet.rs"));
    }

    #[test]
    fn sibling_order_is_coupling_greedy_not_alphabetical_or_churn() {
        let map = CodeMap {
            base_commit_sha: "abc".into(),
            computed_at: 1,
            files: vec![
                file("alpha/a.rs", 999),
                file("beta/b.rs", 1),
                file("gamma/g.rs", 1),
            ],
            imports: vec![
                ("beta/b.rs".into(), "gamma/g.rs".into()),
                ("gamma/g.rs".into(), "beta/b.rs".into()),
            ],
        };
        let rendered = render_map("demo", "", MapState::Fresh(&map), &[], &[], 10);
        let beta = rendered.find("beta/").unwrap();
        let gamma = rendered.find("gamma/").unwrap();
        let alpha = rendered.find("alpha/").unwrap();
        assert!(beta < alpha && gamma < alpha, "{rendered}");
    }

    #[test]
    fn long_rows_preserve_project_file_and_machine_identity() {
        let project = "project-identity-that-must-survive";
        let file_path = "directory/file-identity-that-must-survive.rs";
        let map = CodeMap {
            base_commit_sha: "abcdef012345".into(),
            computed_at: 1,
            files: vec![file(file_path, 1)],
            imports: vec![],
        };
        let activity = vec![Activity {
            path: file_path.into(),
            agent: "agent-with-an-extremely-long-flexible-trailing-description".into(),
            action: "edit".into(),
            at: 9,
            running: true,
        }];
        let machine = Machine {
            name: "machine-identity-that-must-survive".into(),
            reachable: true,
            silence_ms: 0,
            cpu: Some(1.0),
            memory: Some(1.0),
            agents: vec!["resident-with-a-very-long-trailing-name".into()],
        };
        let rendered = render_map(
            project,
            "",
            MapState::Fresh(&map),
            &activity,
            &[machine],
            10,
        );
        assert!(rendered.contains("project-identit…"));
        assert!(rendered.contains("file-identity-that-must-survive.rs"));
        assert!(rendered.contains("machine-identity-"));
        assert!(
            rendered.lines().all(|line| line.chars().count() <= 80),
            "{rendered}"
        );
    }

    #[test]
    fn completed_activity_is_recent_history_not_occupancy() {
        let rendered = render_map(
            "demo",
            "",
            MapState::Fresh(&fixture()),
            &activity(),
            &[],
            1_000,
        );
        assert!(rendered.contains("1 agent"));
        assert!(rendered.contains("· lib.ts 30s"));
        assert!(!rendered.contains("● explorer/8"));
    }
}
