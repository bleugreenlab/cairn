//! The two authorities issue parentage confers, gated-artifact confirmation and
//! permission-answer ancestry, run on the same edge fixtures and must agree.
//! Both apply `FilingParent::proof_sql`; this pins that neither drifts to
//! its own reading of an edge.

use cairn_db::lineage::FilingParent;

use crate::mcp::handlers::permission::answer_authorization::is_ancestor;
use crate::storage::{migrated_test_db, LocalDb};

/// How an issue's parent edge came to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edge {
    /// Filed by a job on the parent, never changed since.
    Proven,
    /// Filed by a job on the parent, then stamped revoked, as migration 0406
    /// stamps every edge that predates it.
    Revoked,
    /// The parent column with no recorded filing job: the shape of a child
    /// filed before filing proof existed, or routed rather than filed.
    Legacy,
    /// Filed by a job on the parent, orphaned, then adopted back by the same
    /// parent through ordinary updates.
    Reparented,
    /// Filed under the parent by a job that does not sit on it.
    ForeignFiler,
}

const EDGES: [Edge; 5] = [
    Edge::Proven,
    Edge::Revoked,
    Edge::Legacy,
    Edge::Reparented,
    Edge::ForeignFiler,
];

const ARMS: [FilingParent; 2] = [FilingParent::Issue, FilingParent::Thread];

/// The parent column an arm writes, the parent it names, and the job that
/// sits on that parent (the one that may answer for it).
fn arm_parts(arm: FilingParent) -> (&'static str, &'static str, &'static str) {
    match arm {
        FilingParent::Issue => ("parent_issue_id", "'i-2'", "job-coord"),
        FilingParent::Thread => ("parent_thread_id", "'thread-design'", "job-design"),
    }
}

/// The INSERT of issue `id`, filed on `arm` as `edge` says. `edge: None`
/// files it with no parent at all.
fn issue_sql(id: &str, number: i64, edge: Option<(FilingParent, Edge)>) -> String {
    let (mut parent_issue, mut parent_thread, mut filer, mut revoked) =
        ("NULL", "NULL", "NULL".to_string(), "NULL");
    if let Some((arm, edge)) = edge {
        let (_, parent, owner) = arm_parts(arm);
        match arm {
            FilingParent::Issue => parent_issue = parent,
            FilingParent::Thread => parent_thread = parent,
        }
        let other_arm = match arm {
            FilingParent::Issue => FilingParent::Thread,
            FilingParent::Thread => FilingParent::Issue,
        };
        filer = match edge {
            Edge::Legacy => "NULL".to_string(),
            Edge::ForeignFiler => format!("'{}'", arm_parts(other_arm).2),
            _ => format!("'{owner}'"),
        };
        if edge == Edge::Revoked {
            revoked = "1";
        }
    }
    format!(
        "INSERT INTO issues(id, project_id, number, title, status, attention, parent_issue_id, parent_thread_id, parent_job_id, filing_parent_revoked_at, created_at, updated_at)
           VALUES ('{id}', 'p-fp', {number}, '{id}', 'active', 'none', {parent_issue}, {parent_thread}, {filer}, {revoked}, 1, 1);"
    )
}

/// Thread `design` and its session job `job-design`; issue `i-2` (filed as
/// `mission` says) driven by `job-coord`; and child `i-1` (filed as `child`
/// says) whose node `job-req` holds pending permission `perm-row`. Both
/// answerers exist before either issue, so each can be named as a filer.
async fn seed(
    name: &str,
    mission: Option<(FilingParent, Edge)>,
    child: (FilingParent, Edge),
) -> LocalDb {
    let db = migrated_test_db(name).await;
    let mut sql = String::from(
        "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-fp', 'default', 'Filing proof', 'fp', '/tmp/fp', 1, 1);
         INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
           VALUES ('thread-design', 'p-fp', 'design', 'active', 'idle', 1, 1);
         INSERT INTO executions(id, recipe_id, project_id, status, started_at)
           VALUES ('host-design', 'host-job-design', 'p-fp', 'running', 1);
         INSERT INTO jobs(id, execution_id, thread_id, project_id, node_name, status, uri_segment, created_at, updated_at)
           VALUES ('job-design', 'host-design', 'thread-design', 'p-fp', 'Thread', 'running', 'thread', 1, 1);
         INSERT INTO executions(id, recipe_id, project_id, status, started_at)
           VALUES ('exec-2', 'coordinator', 'p-fp', 'running', 1);
         INSERT INTO jobs(id, execution_id, project_id, node_name, status, uri_segment, created_at, updated_at)
           VALUES ('job-coord', 'exec-2', 'p-fp', 'coordinator', 'running', 'coordinator', 1, 1);",
    );
    sql.push_str(&issue_sql("i-2", 2, mission));
    // `job-coord` drives `i-2`, which must exist before the job can name it.
    sql.push_str(
        "UPDATE executions SET issue_id = 'i-2', seq = 1 WHERE id = 'exec-2';
         UPDATE jobs SET issue_id = 'i-2' WHERE id = 'job-coord';",
    );
    sql.push_str(&issue_sql("i-1", 1, Some(child)));
    sql.push_str(
        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('exec-1', 'recipe-1', 'i-1', 'p-fp', 'running', 1, 1);
         INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
           VALUES ('job-req', 'exec-1', 'i-1', 'p-fp', 'builder', 'running', 's-req', 'builder', 1, 1);
         INSERT INTO sessions(id, job_id, status, created_at, updated_at)
           VALUES ('s-req', 'job-req', 'active', 1, 1);
         INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
           VALUES ('run-req', 'p-fp', 'i-1', 'job-req', 'live', 's-req', 1, 1, 'resume');
         INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
           VALUES ('perm-row', 'run-req', 'job-req', 'toolu-1', 'read', '{}', 'pending', 1, 'perm-1');",
    );
    db.execute_script(&sql).await.unwrap();
    for (issue, edge) in [("i-2", mission), ("i-1", Some(child))] {
        if let Some((arm, Edge::Reparented)) = edge {
            let (column, parent, _) = arm_parts(arm);
            db.execute_script(&format!(
                "UPDATE issues SET {column} = NULL WHERE id = '{issue}';
                 UPDATE issues SET {column} = {parent} WHERE id = '{issue}';"
            ))
            .await
            .unwrap();
        }
    }
    db
}

/// The confirm check's verdict on `issue`'s `arm` edge.
async fn confirm_proves(db: &LocalDb, issue: &'static str, arm: FilingParent) -> bool {
    db.read(move |conn| {
        Box::pin(async move { super::filing_edge_proven_conn(conn, issue, arm).await })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn confirmation_and_permission_ancestry_agree_on_every_parent_edge() {
    for arm in ARMS {
        for edge in EDGES {
            let case = format!("{arm:?} {edge:?}");
            let db = seed(
                &format!("filing-proof-{arm:?}-{edge:?}.db"),
                None,
                (arm, edge),
            )
            .await;
            let confirms = confirm_proves(&db, "i-1", arm).await;
            let answers = is_ancestor(&db, "perm-row", arm_parts(arm).2)
                .await
                .unwrap();
            assert_eq!(confirms, answers, "{case}: the two authorities disagree");
            assert_eq!(
                confirms,
                edge == Edge::Proven,
                "{case}: only an edge filed by its parent and never changed is proven"
            );
        }
    }
}

/// Ancestry crosses `i-1 -> i-2 -> thread design`. The first hop is proven;
/// the thread answers only when the second hop is proven too, by the same
/// rule the confirm check applies to that edge on its own.
#[tokio::test]
async fn ancestry_applies_the_filing_proof_at_every_hop() {
    for edge in EDGES {
        let db = seed(
            &format!("filing-proof-hop-{edge:?}.db"),
            Some((FilingParent::Thread, edge)),
            (FilingParent::Issue, Edge::Proven),
        )
        .await;
        let first = confirm_proves(&db, "i-1", FilingParent::Issue).await;
        let second = confirm_proves(&db, "i-2", FilingParent::Thread).await;
        assert!(first, "{edge:?}: the first hop is filed");
        assert_eq!(second, edge == Edge::Proven, "{edge:?}");
        assert_eq!(
            is_ancestor(&db, "perm-row", "job-design").await.unwrap(),
            first && second,
            "{edge:?}: the grandparent thread answers only through two proven hops"
        );
        // The first hop alone still makes the coordinator an ancestor.
        assert!(
            is_ancestor(&db, "perm-row", "job-coord").await.unwrap(),
            "{edge:?}"
        );
    }
}
