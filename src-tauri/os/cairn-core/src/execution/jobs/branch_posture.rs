//! Whether a job is on the branch its turns and commits belong on.
//!
//! A recipe node's job exists, and already owns a session, from the moment its
//! execution creates it. Its branch exists only once [`super::prepare_job`] has
//! run its start. Between the two, the job's write coordinate
//! (`mcp::handlers::branch::resolve_job_rev`) falls through to its base branch,
//! which for most work is the project's default. Only an execution filed with
//! `branch: base` declares the base as its target. So "no branch" means one of
//! two very different things, and this module is the one place that tells them
//! apart. The resume funnel asks it before opening a turn and the commit gate
//! asks it before sealing, so the two cannot disagree about where a job's work
//! lands.

use cairn_db::turso::params;

use crate::storage::{LocalDb, RowExt};

/// Where a job's work lands, as its own row and its execution declare it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BranchPosture {
    /// A thread's own work. A thread owns no branch, and its commits are
    /// refused by [`crate::threads::commit_refusal_for_job`], not here.
    Thread,
    /// The execution was filed with `branch: base`: it works on its base
    /// branch by declaration, with no branch of its own.
    DeclaredBase,
    /// The job carries a branch: minted at its start, inherited from an
    /// upstream node, or copied from the job that delegated it.
    Own {
        branch: String,
        /// The branch is the project's default branch.
        is_default: bool,
    },
    /// No branch of its own and no declaration that it should have none. Its
    /// reads and writes fall through to `base_branch`.
    Unminted {
        base_branch: String,
        /// The job is a recipe node, whose branch only its start creates.
        recipe_node: bool,
        /// The job has ever had a run. `prepare_job` inserts a node's first
        /// run, so a node with none has never been through its start.
        has_run: bool,
    },
}

impl BranchPosture {
    /// Why a `commit_msg` from this job may not seal, or `None` when it may.
    /// `node` names the job in the refusal.
    pub(crate) fn commit_refusal(&self, node: &str) -> Option<String> {
        match self {
            BranchPosture::Thread | BranchPosture::DeclaredBase => None,
            BranchPosture::Own {
                is_default: false, ..
            } => None,
            BranchPosture::Own {
                branch,
                is_default: true,
            } => Some(format!(
                "Refusing to commit from {node}: its branch is `{branch}`, the project's default \
                 branch, and its execution was not filed with `branch: base`. A commit here would \
                 land on `{branch}` directly, with no pull request and no review. Nothing was \
                 written. Stop and report this on your issue; do not retry the commit."
            )),
            BranchPosture::Unminted {
                base_branch,
                recipe_node,
                ..
            } => {
                let why = if *recipe_node {
                    "A node's branch is created when it starts, and this one was given a turn \
                     without it."
                } else {
                    "It was created without a branch, so it may read but not commit."
                };
                Some(format!(
                    "Refusing to commit from {node}: it has no branch of its own, so this commit \
                     would land directly on `{base_branch}` with no pull request and no review. \
                     {why} Nothing was written. Stop and report this on your issue; do not retry \
                     the commit."
                ))
            }
        }
    }
}

/// `job_id`'s posture, or `None` when no such job exists.
pub(crate) async fn branch_posture(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<BranchPosture>, String> {
    // An unreadable or pre-target snapshot ran under the only behaviour there
    // was, `new`, matching `ExecutionSnapshot::branch_target`'s serde default.
    db.query_opt(
        "SELECT j.branch,
                COALESCE(j.base_branch, p.default_branch, 'main'),
                j.thread_id IS NOT NULL,
                j.recipe_node_id IS NOT NULL,
                CASE WHEN json_valid(e.snapshot)
                     THEN json_extract(e.snapshot, '$.branchTarget') END,
                COALESCE(p.default_branch, 'main'),
                EXISTS(SELECT 1 FROM runs r WHERE r.job_id = j.id)
         FROM jobs j
         LEFT JOIN executions e ON e.id = j.execution_id
         LEFT JOIN projects p ON p.id = j.project_id
         WHERE j.id = ?1",
        params![job_id],
        |row| {
            let branch = row.opt_text(0)?;
            let base_branch = row.text(1)?;
            let thread = row.i64(2)? != 0;
            let recipe_node = row.i64(3)? != 0;
            let declared_base = row.opt_text(4)?.as_deref() == Some("base");
            let default_branch = row.text(5)?;
            let has_run = row.i64(6)? != 0;
            Ok(if thread {
                BranchPosture::Thread
            } else if declared_base {
                BranchPosture::DeclaredBase
            } else if let Some(branch) = branch.filter(|branch| !branch.is_empty()) {
                BranchPosture::Own {
                    is_default: branch == default_branch,
                    branch,
                }
            } else {
                BranchPosture::Unminted {
                    base_branch,
                    recipe_node,
                    has_run,
                }
            })
        },
    )
    .await
    .map_err(|error| format!("could not read job {job_id}'s branch: {error}"))
}

/// Why `job_id` may not take a turn yet: it is a recipe node that has not been
/// through its start, so it has no branch. `None` when it may.
///
/// Fails closed: a posture that cannot be read refuses, because the turn it
/// would admit is one whose commits land on the base.
pub(crate) async fn unstarted_node_refusal(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<String>, String> {
    let posture = branch_posture(db, job_id).await.map_err(|error| {
        format!("Refusing to open a turn: whether this node has started is unknown ({error}).")
    })?;
    let Some(BranchPosture::Unminted {
        base_branch,
        recipe_node: true,
        has_run: false,
    }) = posture
    else {
        return Ok(None);
    };
    Ok(Some(format!(
        "This node has not started yet, so it has no branch of its own. A node's branch is \
         created when it starts; a turn opened before then would commit straight onto \
         `{base_branch}`. Its messages stay queued and reach it once it has started. To start \
         it now, use Start now on its execution."
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('p','default','P','prj','/tmp/p','main',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
               VALUES ('i','p',1,'Issue','active','none',1,1),
                      ('i-base','p',2,'Mission','active','none',1,1),
                      ('i-legacy','p',3,'Older','active','none',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('t','p','topic','active','none',1,1);
             INSERT INTO executions(id, issue_id, project_id, recipe_id, status, started_at, seq, snapshot)
               VALUES ('e-new','i','p','build','running',1,1,'{\"branchTarget\":\"new\"}'),
                      ('e-base','i-base','p','coordinator','running',1,1,'{\"branchTarget\":\"base\"}'),
                      ('e-legacy','i-legacy','p','build','running',1,1,'{}');
             INSERT INTO jobs(id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, node_name, created_at, updated_at)
               VALUES ('held','e-new','builder','i','p','pending',NULL,'main','builder','Builder',1,1),
                      ('legacy-held','e-legacy','builder','i-legacy','p','pending',NULL,'main','builder','Builder',1,1),
                      ('started','e-new','builder','i','p','running','agent/prj-1-builder-0','main','started','Builder',1,1),
                      ('on-default','e-new','builder','i','p','running','main','main','on-default','Builder',1,1),
                      ('base','e-base','coordinator','i-base','p','running',NULL,'main','coordinator','Coordinator',1,1),
                      ('call','e-new',NULL,'i','p','running',NULL,NULL,'call','call',1,1),
                      ('ran-branchless','e-new','builder','i','p','running',NULL,'main','ran','Builder',1,1);
             INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
               VALUES ('r-ran','p','ran-branchless','live',1,1);
             INSERT INTO jobs(id, thread_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('thread','t','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();
        db
    }

    async fn posture(db: &LocalDb, job: &str) -> BranchPosture {
        branch_posture(db, job).await.unwrap().expect("seeded job")
    }

    /// The two meanings of "no branch" are told apart by the execution's
    /// declaration, not by the job row alone.
    #[tokio::test]
    async fn a_missing_branch_is_unminted_unless_the_execution_declared_base() {
        let db = seed("branch-posture-kinds.db").await;
        let unminted_node = BranchPosture::Unminted {
            base_branch: "main".into(),
            recipe_node: true,
            has_run: false,
        };
        assert_eq!(posture(&db, "held").await, unminted_node);
        assert_eq!(
            posture(&db, "legacy-held").await,
            unminted_node,
            "a snapshot written before branch targets existed ran under `new`"
        );
        assert_eq!(posture(&db, "base").await, BranchPosture::DeclaredBase);
        assert_eq!(posture(&db, "thread").await, BranchPosture::Thread);
        assert_eq!(
            posture(&db, "call").await,
            BranchPosture::Unminted {
                base_branch: "main".into(),
                recipe_node: false,
                has_run: false,
            },
            "a branchless call falls through to the project default"
        );
        assert_eq!(
            posture(&db, "started").await,
            BranchPosture::Own {
                branch: "agent/prj-1-builder-0".into(),
                is_default: false
            }
        );
        assert!(branch_posture(&db, "ghost").await.unwrap().is_none());
    }

    /// cairn/7323: a NULL-branch job's commit is refused by name, before
    /// anything is written; a declared-base job and a started node still commit.
    #[tokio::test]
    async fn only_a_job_on_its_own_branch_or_a_declared_base_may_commit() {
        let db = seed("branch-posture-commit.db").await;
        let node = "cairn://p/prj/1/1/builder";

        let refusal = posture(&db, "held")
            .await
            .commit_refusal(node)
            .expect("an unstarted node may not commit");
        assert!(
            refusal.contains(node)
                && refusal.contains("no branch of its own")
                && refusal.contains("`main`")
                && refusal.contains("Nothing was written"),
            "{refusal}"
        );
        let refusal = posture(&db, "on-default")
            .await
            .commit_refusal(node)
            .expect("a job whose branch is the default may not commit");
        assert!(refusal.contains("default branch"), "{refusal}");
        assert!(posture(&db, "call")
            .await
            .commit_refusal(node)
            .is_some_and(|refusal| refusal.contains("may read but not commit")));

        assert!(
            posture(&db, "ran-branchless")
                .await
                .commit_refusal(node)
                .is_some(),
            "a node that ran without its branch still may not commit"
        );
        assert_eq!(posture(&db, "base").await.commit_refusal(node), None);
        assert_eq!(posture(&db, "started").await.commit_refusal(node), None);
    }

    /// Only an unstarted recipe node is refused a turn: a branchless call, a
    /// declared-base node, and a node that has already run take turns as before.
    #[tokio::test]
    async fn only_an_unstarted_recipe_node_is_refused_a_turn() {
        let db = seed("branch-posture-turn.db").await;
        let refusal = unstarted_node_refusal(&db, "held")
            .await
            .unwrap()
            .expect("a node that never started takes no turn");
        assert!(
            refusal.contains("has not started yet") && refusal.contains("stay queued"),
            "{refusal}"
        );
        // A node that has run is past its start; whether it may commit is the
        // commit gate's question, not this one's.
        for job in [
            "started",
            "ran-branchless",
            "base",
            "call",
            "thread",
            "ghost",
        ] {
            assert_eq!(
                unstarted_node_refusal(&db, job).await.unwrap(),
                None,
                "{job} may take a turn"
            );
        }
    }
}
