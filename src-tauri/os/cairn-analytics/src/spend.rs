//! Spend by thread: the Analytics cost total, attributed through the parent
//! edges ([`crate::attribution`]) and folded into a tree of threads, their
//! issues, and the issues' children.
//!
//! The roots reconcile with the Cost card ([`crate::cost_timeseries`]) because
//! both price the same `token_rollup` rows by the same rule before summing
//! them at different grains.

use std::collections::{HashMap, HashSet};

use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
use cairn_db::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::{Row, Value};

use super::attribution::{
    attribute, Attribution, IssueEdge, JobPlacement, OtherKind, ParentGraph, Root, ThreadInfo,
};
use super::queries::{self, JobCostComponentRow};
use super::types::{Bucket, ModelSpend, Scope, SpendByThread, SpendNode, SpendNodeKind, TimeRange};

/// Longest `parent_job_id` / `parent_issue_id` chain the graph loader follows,
/// matching the attribution walk's own cap.
const MAX_LOAD_DEPTH: usize = 64;

/// Price each job's share using the same row-level rule as the card. The query
/// has already separated unmetered usage before summing by job. Groups without
/// any billable-token row are still dropped, as the card's token-side join does.
pub(crate) fn price_job_rows(rows: Vec<JobCostComponentRow>) -> Vec<(JobCostComponentRow, f64)> {
    let key =
        |row: &JobCostComponentRow| (row.bucket_start, row.model.clone(), row.backend.clone());
    let token_groups: HashSet<_> = rows
        .iter()
        .filter(|row| row.token_rows > 0)
        .map(&key)
        .collect();
    rows.into_iter()
        .filter(|row| token_groups.contains(&key(row)))
        .map(|row| {
            let cost = super::cost::exact_or_priced(
                row.model.as_deref(),
                &row.unmetered_usage,
                row.exact_cost,
            );
            (row, cost)
        })
        .collect()
}

/// Spend for `scope` and `range`, attributed to threads. Pass the page's
/// `bucket` to preserve the card's inclusion of cost-only settlements.
pub async fn spend_by_thread(
    db: &LocalDb,
    scope: &Scope,
    range: &TimeRange,
    bucket: Bucket,
) -> DbResult<SpendByThread> {
    let priced = price_job_rows(queries::cost_components_by_job(db, scope, range, bucket).await?);
    let job_ids: HashSet<String> = priced
        .iter()
        .filter_map(|(r, _)| r.job_id.clone())
        .collect();
    let graph = load_graph(db, job_ids).await?;

    let mut tree = Tree::default();
    let mut attributions: HashMap<Option<String>, Attribution> = HashMap::new();
    for (row, cost) in &priced {
        let attribution = attributions
            .entry(row.job_id.clone())
            .or_insert_with(|| attribute(row.job_id.as_deref(), &graph));
        let model = row
            .model
            .as_deref()
            .filter(|m| !m.is_empty())
            .unwrap_or("unknown");
        tree.add(attribution, &graph, model, *cost, row.billable);
    }

    let project_ids: Vec<String> = tree.projects.iter().cloned().collect();
    let keys: HashMap<String, String> = by_ids(
        db,
        "SELECT id, key FROM projects WHERE id IN",
        &project_ids,
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await?
    .into_iter()
    .collect();
    Ok(tree.finish(&graph, &keys))
}

/// Run `select` (ending in `... IN`) over `ids` in bound chunks.
async fn by_ids<T>(
    db: &LocalDb,
    select: &str,
    ids: &[String],
    map: fn(&Row) -> DbResult<T>,
) -> DbResult<Vec<T>>
where
    T: Send + 'static,
{
    let mut out = Vec::new();
    for chunk in ids.chunks(IN_LIST_CHUNK) {
        let sql = format!("{select} ({})", placeholders(chunk.len()));
        let params: Vec<Value> = chunk.iter().cloned().map(Value::Text).collect();
        out.extend(db.query_all(sql, params, map).await?);
    }
    Ok(out)
}

/// Load the jobs named in the rows plus their spawner chains, which of them ran
/// a workflow, the issues they sit on plus those issues' parents, and every
/// thread either names.
async fn load_graph(db: &LocalDb, job_ids: HashSet<String>) -> DbResult<ParentGraph> {
    let mut graph = ParentGraph::default();

    let mut frontier: Vec<String> = job_ids.into_iter().collect();
    for _ in 0..MAX_LOAD_DEPTH {
        if frontier.is_empty() {
            break;
        }
        let rows = by_ids(
            db,
            "SELECT id, issue_id, thread_id, parent_job_id FROM jobs WHERE id IN",
            &frontier,
            |row| {
                Ok((
                    row.text(0)?,
                    JobPlacement {
                        issue_id: row.opt_text(1)?,
                        thread_id: row.opt_text(2)?,
                        parent_job_id: row.opt_text(3)?,
                        is_workflow: false,
                    },
                ))
            },
        )
        .await?;
        let mut next: HashSet<String> = HashSet::new();
        for (id, placement) in rows {
            // Only a job with no placement of its own walks to its spawner.
            if placement.issue_id.is_none() && placement.thread_id.is_none() {
                next.extend(placement.parent_job_id.clone());
            }
            graph.jobs.insert(id, placement);
        }
        frontier = next
            .into_iter()
            .filter(|id| !graph.jobs.contains_key(id))
            .collect();
    }

    let loaded: Vec<String> = graph.jobs.keys().cloned().collect();
    let workflow_jobs = by_ids(
        db,
        "SELECT DISTINCT job_id FROM workflow_run WHERE job_id IN",
        &loaded,
        |row| row.text(0),
    )
    .await?;
    for id in workflow_jobs {
        if let Some(placement) = graph.jobs.get_mut(&id) {
            placement.is_workflow = true;
        }
    }

    let mut frontier: Vec<String> = graph
        .jobs
        .values()
        .filter_map(|p| p.issue_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    for _ in 0..MAX_LOAD_DEPTH {
        if frontier.is_empty() {
            break;
        }
        let rows = by_ids(
            db,
            "SELECT id, project_id, number, title, parent_issue_id, parent_thread_id
             FROM issues WHERE id IN",
            &frontier,
            |row| {
                Ok((
                    row.text(0)?,
                    IssueEdge {
                        project_id: row.text(1)?,
                        number: row.opt_i64(2)?.unwrap_or_default(),
                        title: row.opt_text(3)?.unwrap_or_default(),
                        parent_issue_id: row.opt_text(4)?,
                        parent_thread_id: row.opt_text(5)?,
                    },
                ))
            },
        )
        .await?;
        let mut next: HashSet<String> = HashSet::new();
        for (id, issue) in rows {
            next.extend(issue.parent_issue_id.clone());
            graph.issues.insert(id, issue);
        }
        frontier = next
            .into_iter()
            .filter(|id| !graph.issues.contains_key(id))
            .collect();
    }

    let thread_ids: Vec<String> = graph
        .jobs
        .values()
        .filter_map(|p| p.thread_id.clone())
        .chain(
            graph
                .issues
                .values()
                .filter_map(|i| i.parent_thread_id.clone()),
        )
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let threads = by_ids(
        db,
        "SELECT id, project_id, name FROM threads WHERE id IN",
        &thread_ids,
        |row| {
            Ok((
                row.text(0)?,
                ThreadInfo {
                    project_id: row.text(1)?,
                    name: row.text(2)?,
                },
            ))
        },
    )
    .await?;
    graph.threads.extend(threads);
    Ok(graph)
}

/// Cost, tokens, and the per-model split of some set of rows.
#[derive(Debug, Default, Clone)]
struct Totals {
    cost: f64,
    billable: i64,
    models: HashMap<String, (f64, i64)>,
}

impl Totals {
    fn add(&mut self, model: &str, cost: f64, billable: i64) {
        self.cost += cost;
        self.billable += billable;
        let entry = self.models.entry(model.to_string()).or_default();
        entry.0 += cost;
        entry.1 += billable;
    }

    fn absorb(&mut self, node: &SpendNode) {
        self.cost += node.cost_usd;
        self.billable += node.billable_tokens;
        for m in &node.models {
            let entry = self.models.entry(m.model.clone()).or_default();
            entry.0 += m.cost_usd;
            entry.1 += m.billable_tokens;
        }
    }

    fn is_empty(&self) -> bool {
        self.cost == 0.0 && self.billable == 0
    }

    fn models(&self) -> Vec<ModelSpend> {
        let mut models: Vec<ModelSpend> = self
            .models
            .iter()
            .filter(|(_, (cost, billable))| *cost != 0.0 || *billable != 0)
            .map(|(model, (cost_usd, billable_tokens))| ModelSpend {
                model: model.clone(),
                cost_usd: *cost_usd,
                billable_tokens: *billable_tokens,
            })
            .collect();
        models.sort_by(|a, b| {
            b.cost_usd
                .total_cmp(&a.cost_usd)
                .then_with(|| a.model.cmp(&b.model))
        });
        models
    }
}

/// A thread or issue while folding: its direct spend and its child issues.
#[derive(Debug, Default)]
struct Branch {
    own: Totals,
    children: HashMap<String, Branch>,
}

#[derive(Debug, Default)]
struct Tree {
    threads: HashMap<String, Branch>,
    unparented: Branch,
    other: HashMap<OtherKind, Totals>,
    /// Every project a thread or issue in the tree belongs to.
    projects: HashSet<String>,
}

impl Tree {
    fn add(
        &mut self,
        attribution: &Attribution,
        graph: &ParentGraph,
        model: &str,
        cost: f64,
        billable: i64,
    ) {
        let mut branch = match &attribution.root {
            Root::Thread(thread) => {
                if let Some(info) = graph.threads.get(thread) {
                    self.projects.insert(info.project_id.clone());
                }
                self.threads.entry(thread.clone()).or_default()
            }
            Root::Unparented => &mut self.unparented,
            Root::Other(kind) => {
                self.other
                    .entry(*kind)
                    .or_default()
                    .add(model, cost, billable);
                return;
            }
        };
        for issue in &attribution.issue_path {
            if let Some(edge) = graph.issues.get(issue) {
                self.projects.insert(edge.project_id.clone());
            }
            branch = branch.children.entry(issue.clone()).or_default();
        }
        branch.own.add(model, cost, billable);
    }

    fn finish(self, graph: &ParentGraph, keys: &HashMap<String, String>) -> SpendByThread {
        let ctx = Ctx {
            graph,
            keys,
            qualify: self.projects.len() > 1,
        };
        let mut roots: Vec<SpendNode> = Vec::new();
        for (thread, branch) in self.threads {
            let info = graph.threads.get(&thread);
            let key = info.and_then(|i| keys.get(&i.project_id)).cloned();
            let name = info.map_or_else(|| thread.clone(), |i| i.name.clone());
            let id = match &key {
                Some(key) => format!("cairn://p/{key}/{name}"),
                None => format!("thread:{thread}"),
            };
            let label = match (&key, ctx.qualify) {
                (Some(key), true) => format!("{key}/{name}"),
                _ => name,
            };
            roots.push(ctx.branch_node(
                Head {
                    id,
                    kind: SpendNodeKind::Thread,
                    label,
                    project_key: key,
                    number: None,
                    title: None,
                },
                branch,
            ));
        }
        roots.push(ctx.branch_node(
            Head::plain("unparented", SpendNodeKind::Unparented, "unparented"),
            self.unparented,
        ));
        let mut other_children: Vec<SpendNode> = self
            .other
            .into_iter()
            .map(|(kind, totals)| {
                let (slug, label) = match kind {
                    OtherKind::Workflow => ("workflow", "workflow"),
                    OtherKind::Unplaced => ("unplaced", "unplaced job"),
                    OtherKind::NoJob => ("no-job", "no job"),
                };
                leaf(
                    Head::plain(&format!("other/{slug}"), SpendNodeKind::Other, label),
                    &totals,
                )
            })
            .collect();
        prune_and_sort(&mut other_children);
        roots.push(parent(
            Head::plain("other", SpendNodeKind::Other, "other"),
            Totals::default(),
            other_children,
        ));
        prune_and_sort(&mut roots);

        let mut total = Totals::default();
        for root in &roots {
            total.absorb(root);
        }
        SpendByThread {
            total_cost_usd: total.cost,
            billable_tokens: total.billable,
            roots,
        }
    }
}

/// The identity half of a [`SpendNode`].
struct Head {
    id: String,
    kind: SpendNodeKind,
    label: String,
    project_key: Option<String>,
    number: Option<i64>,
    title: Option<String>,
}

impl Head {
    fn plain(id: &str, kind: SpendNodeKind, label: &str) -> Self {
        Self {
            id: id.to_string(),
            kind,
            label: label.to_string(),
            project_key: None,
            number: None,
            title: None,
        }
    }
}

struct Ctx<'a> {
    graph: &'a ParentGraph,
    keys: &'a HashMap<String, String>,
    /// Qualify thread labels with their project key, because thread names
    /// such as `general` repeat across projects.
    qualify: bool,
}

impl Ctx<'_> {
    fn issue_head(&self, issue_id: &str) -> Head {
        let edge = self.graph.issues.get(issue_id);
        let key = edge.and_then(|e| self.keys.get(&e.project_id)).cloned();
        let number = edge.map(|e| e.number);
        let title = edge.map(|e| e.title.clone());
        let id = match (&key, number) {
            (Some(key), Some(n)) => format!("cairn://p/{key}/{n}"),
            _ => format!("issue:{issue_id}"),
        };
        let label = match (number, &title) {
            (Some(n), Some(t)) if !t.is_empty() => format!("#{n} {t}"),
            (Some(n), _) => format!("#{n}"),
            _ => issue_id.to_string(),
        };
        Head {
            id,
            kind: SpendNodeKind::Issue,
            label,
            project_key: key,
            number,
            title,
        }
    }

    /// A thread, issue, or the unparented root: its child issues, plus its
    /// direct spend as an `own` child when it also has children.
    fn branch_node(&self, head: Head, branch: Branch) -> SpendNode {
        let mut children: Vec<SpendNode> = branch
            .children
            .into_iter()
            .map(|(issue_id, child)| self.branch_node(self.issue_head(&issue_id), child))
            .collect();
        prune_and_sort(&mut children);
        if children.is_empty() {
            return leaf(head, &branch.own);
        }
        if !branch.own.is_empty() {
            let own_label = if head.kind == SpendNodeKind::Thread {
                "thread session"
            } else {
                "this issue's runs"
            };
            children.push(leaf(
                Head {
                    id: format!("{}#own", head.id),
                    kind: SpendNodeKind::Own,
                    label: own_label.to_string(),
                    project_key: head.project_key.clone(),
                    number: None,
                    title: None,
                },
                &branch.own,
            ));
            sort(&mut children);
        }
        parent(head, Totals::default(), children)
    }
}

fn leaf(head: Head, totals: &Totals) -> SpendNode {
    SpendNode {
        id: head.id,
        kind: head.kind,
        label: head.label,
        project_key: head.project_key,
        number: head.number,
        title: head.title,
        cost_usd: totals.cost,
        billable_tokens: totals.billable,
        models: totals.models(),
        children: Vec::new(),
    }
}

/// A node whose totals are `base` plus the sum of `children`.
fn parent(head: Head, mut base: Totals, children: Vec<SpendNode>) -> SpendNode {
    for child in &children {
        base.absorb(child);
    }
    SpendNode {
        children,
        ..leaf(head, &base)
    }
}

fn prune_and_sort(nodes: &mut Vec<SpendNode>) {
    nodes.retain(|n| n.cost_usd != 0.0 || n.billable_tokens != 0);
    sort(nodes);
}

fn sort(nodes: &mut [SpendNode]) {
    nodes.sort_by(|a, b| {
        b.cost_usd
            .total_cmp(&a.cost_usd)
            .then_with(|| b.billable_tokens.cmp(&a.billable_tokens))
            .then_with(|| a.label.cmp(&b.label))
    });
}
