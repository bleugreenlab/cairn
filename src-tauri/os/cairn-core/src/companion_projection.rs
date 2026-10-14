//! A small, wire-neutral read model for companion endpoints.
//!
//! This module owns presentation bounds and continuity. Lifecycle interpretation
//! remains in the canonical execution, job, check, and post queries it calls.

use std::{collections::BTreeMap, sync::Arc};

use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::storage::{LocalDb, RowExt};

pub const MAX_ISSUES: usize = 20;
pub const MAX_EXECUTIONS: usize = 40;
pub const MAX_NODES: usize = 160;
pub const MAX_CHECKS: usize = 320;
pub const MAX_FEED_ITEMS: usize = 30;
pub const MAX_TEXT_CHARS: usize = 1_000;
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024;
pub const MAX_DELTA_FRAME_BYTES: usize = 48 * 1024;

// IOS_WIRE_DESCRIPTION_BEGIN
pub struct CompanionProjectionWireDescription {
    pub entities: &'static [(&'static str, &'static [&'static str])],
    pub collections: &'static [&'static str],
    pub delta_fields: &'static [&'static str],
}

pub const COMPANION_PROJECTION_WIRE: CompanionProjectionWireDescription =
    CompanionProjectionWireDescription {
        entities: &[
            ("CompanionIssue", &["id", "number", "title", "status"]),
            (
                "CompanionExecution",
                &["id", "issueId", "sequence", "status", "startedAt"],
            ),
            (
                "CompanionNode",
                &["id", "executionId", "name", "status", "activity"],
            ),
            (
                "CompanionCheck",
                &["id", "nodeId", "name", "verdict", "ranAt"],
            ),
            (
                "CompanionFeedItem",
                &["id", "title", "content", "createdAt"],
            ),
        ],
        collections: &["issues", "executions", "nodes", "checks", "feed"],
        delta_fields: &["upsert", "remove"],
    };
// IOS_WIRE_DESCRIPTION_END

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionIssue {
    pub id: String,
    pub number: i32,
    pub title: String,
    pub status: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionExecution {
    pub id: String,
    pub issue_id: String,
    pub sequence: i32,
    pub status: String,
    pub started_at: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionNode {
    pub id: String,
    pub execution_id: String,
    pub name: String,
    pub status: String,
    pub activity: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionCheck {
    pub id: String,
    pub node_id: String,
    pub name: String,
    pub verdict: String,
    pub ran_at: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionFeedItem {
    pub id: String,
    pub title: Option<String>,
    pub content: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionSnapshot {
    pub issues: BTreeMap<String, CompanionIssue>,
    pub executions: BTreeMap<String, CompanionExecution>,
    pub nodes: BTreeMap<String, CompanionNode>,
    pub checks: BTreeMap<String, CompanionCheck>,
    pub feed: BTreeMap<String, CompanionFeedItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EntityDelta<T> {
    pub upsert: Vec<T>,
    pub remove: Vec<String>,
}

impl<T> Default for EntityDelta<T> {
    fn default() -> Self {
        Self {
            upsert: Vec::new(),
            remove: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CompanionDelta {
    pub issues: EntityDelta<CompanionIssue>,
    pub executions: EntityDelta<CompanionExecution>,
    pub nodes: EntityDelta<CompanionNode>,
    pub checks: EntityDelta<CompanionCheck>,
    pub feed: EntityDelta<CompanionFeedItem>,
}

fn bounded(value: String) -> String {
    value.chars().take(MAX_TEXT_CHARS).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompanionSections(u8);

impl CompanionSections {
    pub const GRAPH: Self = Self(1);
    pub const CHECKS: Self = Self(2);
    pub const FEED: Self = Self(4);
    pub const ALL: Self = Self(7);
    pub fn contains(self, section: Self) -> bool {
        self.0 & section.0 != 0
    }
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

const ACTIVE_GRAPH_CTES: &str = "WITH ranked_issues AS (
 SELECT i.id, i.number, i.title, i.status, MAX(e.started_at) AS latest_started
 FROM issues i JOIN executions e ON e.issue_id=i.id
 WHERE e.status IN ('running','paused') GROUP BY i.id
 ORDER BY latest_started DESC, i.id LIMIT ?1
), selected_executions AS (
 SELECT e.id, e.issue_id, COALESCE(e.seq,0) AS seq, e.status, e.started_at
 FROM executions e JOIN ranked_issues i ON i.id=e.issue_id
 WHERE e.status IN ('running','paused') ORDER BY e.started_at DESC, e.id LIMIT ?2
), selected_jobs AS (
 SELECT j.id, j.execution_id, COALESCE(j.uri_segment,j.recipe_node_id,j.id) AS name,
        j.status, j.created_at, j.current_turn_id
 FROM jobs j JOIN selected_executions e ON e.id=j.execution_id
 WHERE j.status <> 'cancelled' ORDER BY e.started_at DESC, j.created_at DESC, j.id LIMIT ?3
) ";

#[cfg(test)]
static GRAPH_READS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static CHECK_READS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static FEED_READS: AtomicUsize = AtomicUsize::new(0);

async fn read_graph(
    db: &LocalDb,
) -> Result<
    (
        BTreeMap<String, CompanionIssue>,
        BTreeMap<String, CompanionExecution>,
        BTreeMap<String, CompanionNode>,
    ),
    String,
> {
    #[cfg(test)]
    GRAPH_READS.fetch_add(1, Ordering::SeqCst);
    db.read(|conn| Box::pin(async move {
        let mut issues = BTreeMap::new();
        let mut rows = conn.query(&format!("{ACTIVE_GRAPH_CTES} SELECT id,number,title,status FROM ranked_issues"), params![MAX_ISSUES as i64]).await?;
        while let Some(r)=rows.next().await? { let id=r.text(0)?; issues.insert(id.clone(), CompanionIssue{id,number:r.i64(1)? as i32,title:bounded(r.text(2)?),status:r.text(3)?}); }
        let mut executions = BTreeMap::new();
        let mut rows = conn.query(&format!("{ACTIVE_GRAPH_CTES} SELECT id,issue_id,seq,status,started_at FROM selected_executions"), params![MAX_ISSUES as i64, MAX_EXECUTIONS as i64]).await?;
        while let Some(r)=rows.next().await? { let id=r.text(0)?; executions.insert(id.clone(), CompanionExecution{id,issue_id:r.text(1)?,sequence:r.i64(2)? as i32,status:r.text(3)?,started_at:r.i64(4)?}); }
        let mut nodes = BTreeMap::new();
        let sql=format!("{ACTIVE_GRAPH_CTES} SELECT j.id,j.execution_id,j.name,j.status,
          CASE WHEN EXISTS(SELECT 1 FROM prompts p WHERE p.turn_id=j.current_turn_id AND p.response IS NULL)
                 OR EXISTS(SELECT 1 FROM permission_requests pr LEFT JOIN runs r ON r.id=pr.run_id WHERE COALESCE(pr.job_id,r.job_id)=j.id AND pr.status='pending') THEN 'awaitinginput'
               WHEN t.state IN ('running','starting') THEN 'running' ELSE 'idle' END
          FROM selected_jobs j LEFT JOIN turns t ON t.id=COALESCE(j.current_turn_id,(SELECT id FROM turns WHERE job_id=j.id ORDER BY created_at DESC,sequence DESC LIMIT 1))");
        let mut rows=conn.query(&sql,params![MAX_ISSUES as i64, MAX_EXECUTIONS as i64, MAX_NODES as i64]).await?;
        while let Some(r)=rows.next().await? { let id=r.text(0)?; nodes.insert(id.clone(),CompanionNode{id,execution_id:r.text(1)?,name:bounded(r.text(2)?),status:r.text(3)?,activity:r.text(4)?}); }
        Ok((issues,executions,nodes))
    })).await.map_err(|e|e.to_string())
}

async fn read_checks(db: &LocalDb) -> Result<BTreeMap<String, CompanionCheck>, String> {
    #[cfg(test)]
    CHECK_READS.fetch_add(1, Ordering::SeqCst);
    db.read(|conn| Box::pin(async move {
        let sql=format!("{ACTIVE_GRAPH_CTES}, latest AS (
          SELECT c.*, ROW_NUMBER() OVER(PARTITION BY c.job_id,c.check_name ORDER BY c.ran_at DESC,c.tree_hash DESC,c.input_hash DESC) AS rn
          FROM check_result_cache c JOIN selected_jobs j ON j.id=c.job_id)
          SELECT job_id,check_name,passed,failure_kind,ran_at FROM latest WHERE rn=1 ORDER BY ran_at DESC,job_id,check_name LIMIT ?4");
        let mut rows=conn.query(&sql,params![MAX_ISSUES as i64,MAX_EXECUTIONS as i64,MAX_NODES as i64,MAX_CHECKS as i64]).await?;
        let mut checks=BTreeMap::new();
        while let Some(r)=rows.next().await? { let node_id=r.text(0)?; let name=r.text(1)?; let id=format!("{node_id}:{name}"); let passed=r.i64(2)? != 0; checks.insert(id.clone(),CompanionCheck{id,node_id,name:bounded(name),verdict:if passed{"passed".into()}else{r.opt_text(3)?.unwrap_or_else(||"failed".into())},ran_at:r.i64(4)?}); }
        Ok(checks)
    })).await.map_err(|e|e.to_string())
}

async fn read_feed(db: &LocalDb) -> Result<BTreeMap<String, CompanionFeedItem>, String> {
    #[cfg(test)]
    FEED_READS.fetch_add(1, Ordering::SeqCst);
    db.read(|conn| Box::pin(async move {
        let mut rows=conn.query("SELECT id,title,content,created_at FROM posts ORDER BY created_at DESC,id DESC LIMIT ?1",params![MAX_FEED_ITEMS as i64]).await?;
        let mut feed=BTreeMap::new();
        while let Some(r)=rows.next().await? { let id=r.i64(0)?.to_string(); feed.insert(id.clone(),CompanionFeedItem{id,title:r.opt_text(1)?.map(bounded),content:bounded(r.text(2)?),created_at:r.i64(3)?}); }
        Ok(feed)
    })).await.map_err(|e|e.to_string())
}

/// Refresh selected runner-global sections into `baseline`. SQL applies all
/// ranking and cardinality bounds before rows cross the database boundary.
pub async fn refresh_companion_snapshot(
    db: Arc<LocalDb>,
    baseline: &CompanionSnapshot,
    sections: CompanionSections,
) -> Result<CompanionSnapshot, String> {
    let mut next = baseline.clone();
    if sections.contains(CompanionSections::GRAPH) {
        let (i, e, n) = read_graph(&db).await?;
        next.issues = i;
        next.executions = e;
        next.nodes = n;
        // Graph membership selects the nodes whose checks are visible. Refresh
        // checks with it so newly admitted nodes converge without a later check event.
        next.checks = read_checks(&db).await?;
        next.checks
            .retain(|_, c| next.nodes.contains_key(&c.node_id));
    }
    if sections.contains(CompanionSections::CHECKS) && !sections.contains(CompanionSections::GRAPH)
    {
        next.checks = read_checks(&db).await?;
        next.checks
            .retain(|_, c| next.nodes.contains_key(&c.node_id));
    }
    if sections.contains(CompanionSections::FEED) {
        next.feed = read_feed(&db).await?;
    }
    bound_companion_snapshot(&mut next, |snapshot| {
        serde_json::to_vec(snapshot)
            .map(|bytes| bytes.len())
            .map_err(|e| e.to_string())
    })?;
    Ok(next)
}

pub async fn read_companion_snapshot(db: Arc<LocalDb>) -> Result<CompanionSnapshot, String> {
    refresh_companion_snapshot(db, &CompanionSnapshot::default(), CompanionSections::ALL).await
}

fn map_delta<T: Clone + PartialEq>(
    old: &BTreeMap<String, T>,
    new: &BTreeMap<String, T>,
) -> EntityDelta<T> {
    EntityDelta {
        upsert: new
            .iter()
            .filter(|(k, v)| old.get(*k) != Some(*v))
            .map(|(_, v)| v.clone())
            .collect(),
        remove: old
            .keys()
            .filter(|k| !new.contains_key(*k))
            .cloned()
            .collect(),
    }
}
pub fn diff(old: &CompanionSnapshot, new: &CompanionSnapshot) -> CompanionDelta {
    CompanionDelta {
        issues: map_delta(&old.issues, &new.issues),
        executions: map_delta(&old.executions, &new.executions),
        nodes: map_delta(&old.nodes, &new.nodes),
        checks: map_delta(&old.checks, &new.checks),
        feed: map_delta(&old.feed, &new.feed),
    }
}

/// Trim a snapshot to an encoded representation while preserving the projection's
/// global newest-first ranking. The caller supplies the real wire encoder so the
/// bound includes every transport envelope byte.
pub fn bound_companion_snapshot(
    s: &mut CompanionSnapshot,
    mut encoded_len: impl FnMut(&CompanionSnapshot) -> Result<usize, String>,
) -> Result<(), String> {
    while encoded_len(s)? > MAX_SNAPSHOT_BYTES {
        if let Some(id) = s
            .feed
            .values()
            .min_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| b.id.cmp(&a.id))
            })
            .map(|v| v.id.clone())
        {
            s.feed.remove(&id);
            continue;
        }
        if let Some(id) = s
            .checks
            .values()
            .min_by(|a, b| a.ran_at.cmp(&b.ran_at).then_with(|| b.id.cmp(&a.id)))
            .map(|v| v.id.clone())
        {
            s.checks.remove(&id);
            continue;
        }
        if let Some(id) = s
            .nodes
            .values()
            .min_by(|a, b| {
                let at = |node: &CompanionNode| {
                    s.executions.get(&node.execution_id).map(|e| e.started_at)
                };
                at(a).cmp(&at(b)).then_with(|| b.id.cmp(&a.id))
            })
            .map(|v| v.id.clone())
        {
            s.nodes.remove(&id);
            s.checks.retain(|_, check| check.node_id != id);
            continue;
        }
        if let Some(id) = s
            .executions
            .values()
            .min_by(|a, b| {
                a.started_at
                    .cmp(&b.started_at)
                    .then_with(|| b.id.cmp(&a.id))
            })
            .map(|v| v.id.clone())
        {
            let issue_id = s
                .executions
                .remove(&id)
                .expect("selected execution exists")
                .issue_id;
            let removed_nodes: Vec<_> = s
                .nodes
                .values()
                .filter(|node| node.execution_id == id)
                .map(|node| node.id.clone())
                .collect();
            s.nodes.retain(|_, node| node.execution_id != id);
            s.checks
                .retain(|_, check| !removed_nodes.contains(&check.node_id));
            if !s
                .executions
                .values()
                .any(|execution| execution.issue_id == issue_id)
            {
                s.issues.remove(&issue_id);
            }
            continue;
        }
        return Err("snapshot cannot fit byte bound".into());
    }
    Ok(())
}

/// Greedily chunks entity operations. Every returned JSON value is independently
/// decodable and no larger than `ceiling`; an individually oversized entity is rejected.
pub fn chunk_delta(delta: &CompanionDelta, ceiling: usize) -> Result<Vec<CompanionDelta>, String> {
    let mut chunks = Vec::new();
    let mut current = CompanionDelta::default();
    macro_rules! push {
        ($field:ident,$member:ident,$value:expr) => {{
            let mut candidate = current.clone();
            candidate.$field.$member.push($value);
            if serde_json::to_vec(&candidate)
                .map_err(|e| e.to_string())?
                .len()
                > ceiling
            {
                if current == CompanionDelta::default() {
                    return Err("single companion delta entity exceeds frame ceiling".into());
                }
                chunks.push(current);
                current = CompanionDelta::default();
                current
                    .$field
                    .$member
                    .push(candidate.$field.$member.pop().unwrap());
                if serde_json::to_vec(&current)
                    .map_err(|e| e.to_string())?
                    .len()
                    > ceiling
                {
                    return Err("single companion delta entity exceeds frame ceiling".into());
                }
            } else {
                current = candidate;
            }
        }};
    }
    for v in delta.issues.upsert.clone() {
        push!(issues, upsert, v)
    }
    for v in delta.issues.remove.clone() {
        push!(issues, remove, v)
    }
    for v in delta.executions.upsert.clone() {
        push!(executions, upsert, v)
    }
    for v in delta.executions.remove.clone() {
        push!(executions, remove, v)
    }
    for v in delta.nodes.upsert.clone() {
        push!(nodes, upsert, v)
    }
    for v in delta.nodes.remove.clone() {
        push!(nodes, remove, v)
    }
    for v in delta.checks.upsert.clone() {
        push!(checks, upsert, v)
    }
    for v in delta.checks.remove.clone() {
        push!(checks, remove, v)
    }
    for v in delta.feed.upsert.clone() {
        push!(feed, upsert, v)
    }
    for v in delta.feed.remove.clone() {
        push!(feed, remove, v)
    }
    if current != CompanionDelta::default() {
        chunks.push(current)
    }
    Ok(chunks)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continuity {
    Current,
    Recover,
}
#[derive(Debug, Default)]
pub struct ProjectionSequence {
    next: u64,
    valid: bool,
}
impl ProjectionSequence {
    pub fn new() -> Self {
        Self {
            next: 1,
            valid: false,
        }
    }
    pub fn establish_baseline(&mut self) -> u64 {
        let n = self.next;
        self.next += 1;
        self.valid = true;
        n
    }
    pub fn next_delta(&mut self) -> Result<u64, Continuity> {
        if !self.valid {
            return Err(Continuity::Recover);
        }
        let n = self.next;
        self.next += 1;
        Ok(n)
    }
    pub fn invalidate(&mut self) {
        self.valid = false
    }
    pub fn observe_client_request(&mut self, recovery: bool) -> Continuity {
        if recovery {
            self.invalidate();
            Continuity::Recover
        } else {
            Continuity::Current
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn projection_db() -> Arc<LocalDb> {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(LocalDb::open(dir.join("projection.db")).await.unwrap());
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn reset_read_counts() {
        GRAPH_READS.store(0, Ordering::SeqCst);
        CHECK_READS.store(0, Ordering::SeqCst);
        FEED_READS.store(0, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn localized_refresh_only_reads_requested_section() {
        let db = projection_db().await;
        reset_read_counts();
        refresh_companion_snapshot(
            db.clone(),
            &CompanionSnapshot::default(),
            CompanionSections::FEED,
        )
        .await
        .unwrap();
        assert_eq!(
            (
                GRAPH_READS.load(Ordering::SeqCst),
                CHECK_READS.load(Ordering::SeqCst),
                FEED_READS.load(Ordering::SeqCst)
            ),
            (0, 0, 1)
        );
        reset_read_counts();
        refresh_companion_snapshot(
            db.clone(),
            &CompanionSnapshot::default(),
            CompanionSections::CHECKS,
        )
        .await
        .unwrap();
        assert_eq!(
            (
                GRAPH_READS.load(Ordering::SeqCst),
                CHECK_READS.load(Ordering::SeqCst),
                FEED_READS.load(Ordering::SeqCst)
            ),
            (0, 1, 0)
        );
        reset_read_counts();
        refresh_companion_snapshot(db, &CompanionSnapshot::default(), CompanionSections::GRAPH)
            .await
            .unwrap();
        assert_eq!(
            (
                GRAPH_READS.load(Ordering::SeqCst),
                CHECK_READS.load(Ordering::SeqCst),
                FEED_READS.load(Ordering::SeqCst)
            ),
            (1, 1, 0)
        );
    }

    #[tokio::test]
    async fn graph_refresh_converges_checks_after_membership_change() {
        let db = projection_db().await;
        db.execute_script("INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','w',1,1); INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','p','p','/tmp/p',1,1); INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'issue','active',1,1); INSERT INTO executions(id,recipe_id,issue_id,project_id,seq,status,started_at) VALUES('e','build','i','p',1,'running',1);").await.unwrap();
        let baseline = read_companion_snapshot(db.clone()).await.unwrap();
        db.execute_script("INSERT INTO jobs(id,project_id,execution_id,name,status,uri_segment,created_at,updated_at) VALUES('j','p','e','node','running','node',2,2); INSERT INTO check_result_cache(project_id,tree_hash,input_hash,check_name,exit_code,passed,output_tail,duration_ms,ran_at,job_id) VALUES('p','tree','input','focused',0,1,'',1,2,'j');").await.unwrap();

        let localized = refresh_companion_snapshot(db.clone(), &baseline, CompanionSections::GRAPH)
            .await
            .unwrap();
        let canonical = read_companion_snapshot(db).await.unwrap();

        assert_eq!(localized, canonical);
        assert!(localized.checks.contains_key("j:focused"));
    }

    #[tokio::test]
    async fn many_projects_are_ranked_and_bounded_globally() {
        let db = projection_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','w',1,1)",
        )
        .await
        .unwrap();
        for project in 0..(MAX_ISSUES + 15) {
            let sql = format!("INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p{project}','w','p{project}','p{project}','/tmp/p{project}',1,1); INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i{project}','p{project}',1,'{}','active',1,{project}); INSERT INTO executions(id,recipe_id,issue_id,project_id,seq,status,started_at) VALUES('e{project}','build','i{project}','p{project}',1,'running',{project}); INSERT INTO jobs(id,project_id,execution_id,status,uri_segment,created_at,updated_at) VALUES('j{project}','p{project}','e{project}','running','node',{project},1);", "x".repeat(MAX_TEXT_CHARS));
            db.execute_script(&sql).await.unwrap();
        }
        let snapshot = read_companion_snapshot(db).await.unwrap();
        assert_eq!(snapshot.issues.len(), MAX_ISSUES);
        assert!(snapshot
            .issues
            .contains_key(&format!("i{}", MAX_ISSUES + 14)));
        assert!(!snapshot.issues.contains_key("i0"));
        assert!(snapshot.executions.len() <= MAX_EXECUTIONS && snapshot.nodes.len() <= MAX_NODES);
        assert!(serde_json::to_vec(&snapshot).unwrap().len() <= MAX_SNAPSHOT_BYTES);
        assert!(snapshot
            .executions
            .values()
            .all(|e| snapshot.issues.contains_key(&e.issue_id)));
        assert!(snapshot
            .nodes
            .values()
            .all(|n| snapshot.executions.contains_key(&n.execution_id)));
    }
    fn issue(id: &str, title: &str) -> CompanionIssue {
        CompanionIssue {
            id: id.into(),
            number: 1,
            title: title.into(),
            status: "active".into(),
        }
    }
    #[test]
    fn diff_is_proportional() {
        let mut a = CompanionSnapshot::default();
        a.issues.insert("a".into(), issue("a", "old"));
        a.issues.insert("gone".into(), issue("gone", "x"));
        let mut b = CompanionSnapshot::default();
        b.issues.insert("a".into(), issue("a", "new"));
        let d = diff(&a, &b);
        assert_eq!(d.issues.upsert, vec![issue("a", "new")]);
        assert_eq!(d.issues.remove, vec!["gone"]);
        assert!(d.nodes.upsert.is_empty())
    }
    #[test]
    fn chunks_stay_below_ceiling() {
        let mut d = CompanionDelta::default();
        for n in 0..20 {
            d.issues.upsert.push(issue(&n.to_string(), &"x".repeat(80)))
        }
        let chunks = chunk_delta(&d, 500).unwrap();
        assert!(chunks.len() > 1);
        assert!(chunks
            .iter()
            .all(|c| serde_json::to_vec(c).unwrap().len() <= 500));
    }
    #[test]
    fn oversized_entity_forces_recovery() {
        let mut d = CompanionDelta::default();
        d.issues.upsert.push(issue("a", &"x".repeat(500)));
        assert!(chunk_delta(&d, 100).is_err())
    }
    #[test]
    fn lag_and_client_recovery_discard_baseline() {
        let mut s = ProjectionSequence::new();
        assert_eq!(s.next_delta(), Err(Continuity::Recover));
        assert_eq!(s.establish_baseline(), 1);
        assert_eq!(s.next_delta(), Ok(2));
        s.invalidate();
        assert_eq!(s.next_delta(), Err(Continuity::Recover));
        s.establish_baseline();
        assert_eq!(s.observe_client_request(true), Continuity::Recover);
        assert_eq!(s.next_delta(), Err(Continuity::Recover));
    }
}
