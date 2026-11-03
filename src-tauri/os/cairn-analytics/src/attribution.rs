//! Spend attribution: which thread a job's cost belongs to.
//!
//! A job's cost belongs to the thread reached by following parent edges upward
//! from the issue or thread its run belongs to. An issue's parent is an issue
//! (`parent_issue_id`, checked first so nesting is kept) or a thread
//! (`parent_thread_id`); a job with neither an issue nor a thread inherits its
//! spawner's placement through `parent_job_id`. An issue with no thread above
//! it is `Unparented`; a job with no issue and no thread is `Other`, labelled by
//! kind. The graph is read from the current edges at query time, so
//! reparenting an issue moves its history with it.
//!
//! This is deliberately not `cairn-attention`'s coordinator resolution: that
//! picks which live job to wake and skips closed threads, while spend still
//! counts toward a closed thread.

use std::collections::{HashMap, HashSet};

/// Longest parent chain followed before the walk gives up. Nothing in the
/// schema forbids a cycle or bounds depth, so both walks are capped.
const MAX_DEPTH: usize = 64;

/// Where a job sits: the placement columns on `jobs`, plus whether a
/// `workflow_run` names it.
#[derive(Debug, Clone, Default)]
pub(crate) struct JobPlacement {
    pub issue_id: Option<String>,
    pub thread_id: Option<String>,
    pub parent_job_id: Option<String>,
    pub is_workflow: bool,
}

/// An issue's identity and its two parent edges.
#[derive(Debug, Clone, Default)]
pub(crate) struct IssueEdge {
    pub project_id: String,
    pub number: i64,
    pub title: String,
    pub parent_issue_id: Option<String>,
    pub parent_thread_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ThreadInfo {
    pub project_id: String,
    pub name: String,
}

/// The slice of the job/issue/thread graph attribution walks, keyed by id.
#[derive(Debug, Clone, Default)]
pub(crate) struct ParentGraph {
    pub jobs: HashMap<String, JobPlacement>,
    pub issues: HashMap<String, IssueEdge>,
    pub threads: HashMap<String, ThreadInfo>,
}

/// Why a job's spend has no issue or thread to land on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum OtherKind {
    /// The job, or one of its spawners, ran a workflow.
    Workflow,
    /// A job with no issue, no thread, and no workflow (one-shots, responses).
    Unplaced,
    /// A rollup row with no job at all.
    NoJob,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Root {
    Thread(String),
    Unparented,
    Other(OtherKind),
}

/// Where one job's spend lands: a root, and the issues between that root and
/// the job, top-most first. The last issue is the job's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attribution {
    pub root: Root,
    pub issue_path: Vec<String>,
}

impl Attribution {
    fn other(kind: OtherKind) -> Self {
        Self {
            root: Root::Other(kind),
            issue_path: Vec::new(),
        }
    }
}

pub(crate) fn attribute(job: Option<&str>, graph: &ParentGraph) -> Attribution {
    let Some(mut job_id) = job else {
        return Attribution::other(OtherKind::NoJob);
    };
    let mut seen: HashSet<&str> = HashSet::new();
    let mut workflow = false;
    while seen.len() < MAX_DEPTH && seen.insert(job_id) {
        let Some(placement) = graph.jobs.get(job_id) else {
            break;
        };
        workflow |= placement.is_workflow;
        if let Some(issue) = placement.issue_id.as_deref() {
            if let Some(attribution) = walk_issue(issue, graph) {
                return attribution;
            }
            break;
        }
        if let Some(thread) = &placement.thread_id {
            return Attribution {
                root: Root::Thread(thread.clone()),
                issue_path: Vec::new(),
            };
        }
        match placement.parent_job_id.as_deref() {
            Some(parent) => job_id = parent,
            None => break,
        }
    }
    Attribution::other(if workflow {
        OtherKind::Workflow
    } else {
        OtherKind::Unplaced
    })
}

/// Walk up from the job's issue. `None` when the starting issue is not in the
/// graph (a dangling `issue_id`), so the caller falls back to `Other`.
fn walk_issue(start: &str, graph: &ParentGraph) -> Option<Attribution> {
    // Bottom-up while walking; reversed to top-first on return.
    let mut path: Vec<String> = Vec::new();
    let mut current = start;
    loop {
        if let Some(at) = path.iter().position(|seen| seen == current) {
            // A cycle: the first issue that repeats becomes the top, and the
            // issues walked past it are dropped so the path still ends at the
            // job's own issue.
            path.truncate(at + 1);
            return Some(unparented(path));
        }
        let Some(issue) = graph.issues.get(current) else {
            // The previous issue's parent dangles; it is the top.
            return (!path.is_empty()).then(|| unparented(path));
        };
        path.push(current.to_string());
        if path.len() >= MAX_DEPTH {
            return Some(unparented(path));
        }
        if let Some(parent) = issue.parent_issue_id.as_deref() {
            current = parent;
            continue;
        }
        path.reverse();
        return Some(match &issue.parent_thread_id {
            Some(thread) => Attribution {
                root: Root::Thread(thread.clone()),
                issue_path: path,
            },
            None => Attribution {
                root: Root::Unparented,
                issue_path: path,
            },
        });
    }
}

fn unparented(mut bottom_up: Vec<String>) -> Attribution {
    bottom_up.reverse();
    Attribution {
        root: Root::Unparented,
        issue_path: bottom_up,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(issue: Option<&str>, thread: Option<&str>, parent: Option<&str>) -> JobPlacement {
        JobPlacement {
            issue_id: issue.map(str::to_string),
            thread_id: thread.map(str::to_string),
            parent_job_id: parent.map(str::to_string),
            is_workflow: false,
        }
    }

    fn issue(number: i64, parent_issue: Option<&str>, parent_thread: Option<&str>) -> IssueEdge {
        IssueEdge {
            project_id: "p".into(),
            number,
            title: format!("issue {number}"),
            parent_issue_id: parent_issue.map(str::to_string),
            parent_thread_id: parent_thread.map(str::to_string),
        }
    }

    /// Thread `models` with child issue `i1`, grandchild `i2` under `i1`, an
    /// unparented issue `u1`, and jobs placed at each level.
    fn graph() -> ParentGraph {
        let mut g = ParentGraph::default();
        g.threads.insert(
            "t-models".into(),
            ThreadInfo {
                project_id: "p".into(),
                name: "models".into(),
            },
        );
        g.issues
            .insert("i1".into(), issue(1, None, Some("t-models")));
        g.issues.insert("i2".into(), issue(2, Some("i1"), None));
        g.issues.insert("u1".into(), issue(3, None, None));
        // The thread's own session, and a task it spawned (0167 copies the
        // thread down).
        g.jobs
            .insert("thread-session".into(), job(None, Some("t-models"), None));
        g.jobs.insert(
            "thread-task".into(),
            job(None, Some("t-models"), Some("thread-session")),
        );
        g.jobs
            .insert("child-builder".into(), job(Some("i1"), None, None));
        g.jobs
            .insert("grandchild-builder".into(), job(Some("i2"), None, None));
        // A sub-agent task under the child's builder, with copied placement…
        g.jobs.insert(
            "task-copied".into(),
            job(Some("i1"), None, Some("child-builder")),
        );
        // …and a stale one that has to walk parent_job_id.
        g.jobs
            .insert("task-stale".into(), job(None, None, Some("child-builder")));
        g.jobs
            .insert("unparented-builder".into(), job(Some("u1"), None, None));
        g.jobs.insert("one-shot".into(), job(None, None, None));
        g.jobs.insert(
            "workflow".into(),
            JobPlacement {
                is_workflow: true,
                ..job(None, None, None)
            },
        );
        g.jobs
            .insert("workflow-call".into(), job(None, None, Some("workflow")));
        g
    }

    fn thread(name: &str, path: &[&str]) -> Attribution {
        Attribution {
            root: Root::Thread(name.into()),
            issue_path: path.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn a_threads_own_session_and_its_tasks_belong_to_the_thread() {
        let g = graph();
        assert_eq!(
            attribute(Some("thread-session"), &g),
            thread("t-models", &[])
        );
        assert_eq!(attribute(Some("thread-task"), &g), thread("t-models", &[]));
    }

    #[test]
    fn a_child_issue_rolls_up_to_its_thread() {
        let g = graph();
        assert_eq!(
            attribute(Some("child-builder"), &g),
            thread("t-models", &["i1"])
        );
    }

    #[test]
    fn a_grandchild_keeps_its_nesting_through_the_issue_parent() {
        let g = graph();
        assert_eq!(
            attribute(Some("grandchild-builder"), &g),
            thread("t-models", &["i1", "i2"])
        );
    }

    #[test]
    fn a_sub_agent_task_belongs_to_its_spawners_issue() {
        let g = graph();
        assert_eq!(
            attribute(Some("task-copied"), &g),
            thread("t-models", &["i1"])
        );
        assert_eq!(
            attribute(Some("task-stale"), &g),
            thread("t-models", &["i1"])
        );
    }

    #[test]
    fn an_issue_with_no_thread_above_it_is_unparented() {
        let g = graph();
        assert_eq!(
            attribute(Some("unparented-builder"), &g),
            Attribution {
                root: Root::Unparented,
                issue_path: vec!["u1".into()],
            }
        );
    }

    #[test]
    fn a_run_with_no_issue_is_other_by_kind() {
        let g = graph();
        assert_eq!(attribute(None, &g), Attribution::other(OtherKind::NoJob));
        assert_eq!(
            attribute(Some("one-shot"), &g),
            Attribution::other(OtherKind::Unplaced)
        );
        assert_eq!(
            attribute(Some("workflow"), &g),
            Attribution::other(OtherKind::Workflow)
        );
        assert_eq!(
            attribute(Some("workflow-call"), &g),
            Attribution::other(OtherKind::Workflow)
        );
        // A job id the graph never loaded (deleted) is unplaced, not a panic.
        assert_eq!(
            attribute(Some("missing"), &g),
            Attribution::other(OtherKind::Unplaced)
        );
    }

    #[test]
    fn an_issue_parent_cycle_is_unparented_at_the_first_repeat() {
        let mut g = graph();
        // leaf -> a -> b -> a
        g.issues.insert("a".into(), issue(10, Some("b"), None));
        g.issues.insert("b".into(), issue(11, Some("a"), None));
        g.issues.insert("leaf".into(), issue(12, Some("a"), None));
        g.jobs
            .insert("cyclic".into(), job(Some("leaf"), None, None));
        assert_eq!(
            attribute(Some("cyclic"), &g),
            Attribution {
                root: Root::Unparented,
                issue_path: vec!["a".into(), "leaf".into()],
            }
        );
        // A job-parent cycle terminates too.
        g.jobs.insert("j1".into(), job(None, None, Some("j2")));
        g.jobs.insert("j2".into(), job(None, None, Some("j1")));
        assert_eq!(
            attribute(Some("j1"), &g),
            Attribution::other(OtherKind::Unplaced)
        );
    }
}
