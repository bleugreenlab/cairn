//! First-class thread resource rendering, descendant normalization, and
//! migration-address resolution.

use super::common::{connect_for_read, lookup_project_by_key};
use cairn_common::uri::{is_reserved_node_segment, CairnResource, DEFAULT_BROWSER_SLUG};
use cairn_db::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

/// Why a branch-shaped sub-resource has no meaning under a thread.
///
/// A thread session is commit-fenced by construction: it has no branch and no
/// PR, so `diff`, `changed`, `rebase`, and the session's own `checks` are not
/// merely unimplemented there — they name something that does not exist. Saying
/// so is a better answer than the silent coercion into an artifact of that name
/// this replaced.
fn branchless_refusal(name: &str, segment: &str) -> String {
    format!(
        "A thread session has no branch, so '{segment}' does not apply to thread '{name}'. \
         Branch-shaped resources (diff, changed, rebase, and a session's own checks) belong to \
         an execution node working on an issue."
    )
}

fn parse_seq(kind: &str, value: &str) -> Result<i32, String> {
    value
        .parse()
        .map_err(|_| format!("Invalid thread {kind}: {value}"))
}

/// Normalize a thread-session descendant onto the job-owned resource family that
/// already answers for it.
///
/// This is the ONE interpretation of a thread sub-path, shared by the read
/// dispatcher and the write dispatcher. Both sides previously carried their own
/// interpreter and the two had drifted: the write side knew `tasks` and the read
/// side did not, the read side knew `chat/turn/N` and the write side did not, and
/// neither knew `terminal/<slug>` — which is exactly why a thread agent could
/// spawn a task it could not then read, and could not open a terminal at all.
///
/// The mapping is driven by what the PARSER resolves under a node, not by a list
/// maintained here; `a_thread_session_addresses_every_resource_a_node_does` fails
/// until a newly parseable node segment is answered for here too.
///
/// After this runs, a thread descendant IS a node-family resource: the existing
/// renderers, dispatch arms, contract entries, and affordance blocks all apply
/// unchanged, per CAIRN-3697's generalized-ownership ruling. The reserved
/// `(0, 0, thread-name)` coordinate it produces is defined by
/// [`cairn_common::uri::NodeAddress`], which is also what renders it back.
///
/// `Err` is a purposeful refusal carrying its reason, not a failure to map.
pub(crate) fn delegate_thread_descendant(resource: CairnResource) -> Result<CairnResource, String> {
    let CairnResource::Thread {
        project,
        name,
        path,
    } = resource
    else {
        return Ok(resource);
    };

    // Every node-family variant carries `project, number, exec_seq, node_id` in
    // the same shape, so the mapping reads as one line per addressable segment
    // instead of eight lines of coordinate rebuilding each.
    macro_rules! at_thread {
        ($variant:ident $(, $field:ident : $value:expr)* $(,)?) => {
            CairnResource::$variant {
                project: project.clone(),
                number: 0,
                exec_seq: 0,
                node_id: name.clone(),
                $($field: $value,)*
            }
        };
    }

    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    let delegated = match segments.as_slice() {
        // The thread itself, which is not a node-family resource.
        [] => {
            return Ok(CairnResource::Thread {
                project,
                name,
                path,
            })
        }

        // Branch-shaped, and a thread has no branch. Refused with the reason at
        // both levels rather than silently becoming an artifact of that name.
        ["diff"] | ["changed"] | ["checks"] | ["rebase"] => {
            return Err(branchless_refusal(&name, segments[0]))
        }
        ["task", _, segment @ ("diff" | "changed" | "rebase")] => {
            return Err(branchless_refusal(&name, segment))
        }

        // ---- Session level ----
        ["chat"] => at_thread!(NodeChat),
        ["chat", "raw"] => at_thread!(NodeChatRaw),
        ["chat", "turn", turn] => {
            at_thread!(NodeChatTurn, turn_seq: parse_seq("chat turn", turn)?)
        }
        ["chat", run, event] => at_thread!(
            NodeChatEvent,
            run_seq: parse_seq("chat run", run)?,
            event_seq: parse_seq("chat event", event)?,
        ),
        ["artifact"] => at_thread!(NodeArtifact, name: None),
        ["symbols"] => at_thread!(NodeSymbols, symbol: None),
        ["symbols", symbol] => at_thread!(NodeSymbols, symbol: Some((*symbol).to_string())),
        ["todos"] => at_thread!(JobTodos, task_name: None),
        // A session's feed IS the thread's: the cursor keys on the thread row,
        // so it is the same reading position across every session.
        ["feed"] => at_thread!(HomeFeed, task_name: None),
        ["tasks"] => at_thread!(NodeTasks),
        ["calls"] => at_thread!(NodeCalls),
        ["wakes"] => at_thread!(NodeWakes),
        ["questions"] => at_thread!(NodeQuestions),
        ["questions", segment] => at_thread!(NodeQuestion, segment: (*segment).to_string()),
        ["permissions"] => at_thread!(NodePermissions),
        ["permissions", segment] => at_thread!(NodePermission, segment: (*segment).to_string()),
        ["messages"] => at_thread!(NodeMessages),
        ["progress"] => at_thread!(NodeProgress),
        ["memories"] => at_thread!(NodeMemories),
        ["memories", seq] => at_thread!(NodeMemory, memory_seq: parse_seq("memory sequence", seq)?),
        ["terminal", slug] => at_thread!(NodeTerminal, slug: (*slug).to_string()),
        ["repl", slug] => at_thread!(NodeRepl, slug: (*slug).to_string()),
        ["browser"] => at_thread!(NodeBrowser, slug: DEFAULT_BROWSER_SLUG.to_string()),
        ["browser", slug] => at_thread!(NodeBrowser, slug: (*slug).to_string()),
        ["browser", slug, "network", request_id] => at_thread!(
            NodeBrowserNetworkRequest,
            slug: (*slug).to_string(),
            request_id: (*request_id).to_string(),
        ),
        // A trailing non-reserved segment names an artifact type, exactly as it
        // does under a node. `arc` resolves here. The guard is
        // `is_reserved_node_segment` at both levels now: the ad-hoc four-name
        // exclusion list this replaced is why `browser`, `checks`, and `diff`
        // from a thread session became artifacts of that name (CAIRN-3760).
        [artifact] if !is_reserved_node_segment(artifact) => {
            at_thread!(NodeArtifact, name: Some((*artifact).to_string()))
        }

        // ---- Task level ----
        // A thread's task is a job like any other, so it addresses the same
        // families a node's task does, reached through the same reserved
        // coordinate with the task's own segment carried alongside.
        ["task", task] => at_thread!(Task, task_name: (*task).to_string()),
        ["task", task, "todos"] => at_thread!(
            JobTodos,
            task_name: Some((*task).to_string()),
        ),
        ["task", task, "feed"] => at_thread!(
            HomeFeed,
            task_name: Some((*task).to_string()),
        ),
        ["task", task, "messages"] => at_thread!(TaskMessages, task_name: (*task).to_string()),
        // A task's `checks` is the task JOB's turn-end verdicts, addressed the
        // way every other task-level segment is, and the read-only
        // `TASK_CHECKS_CONTRACT` is what answers a mutation of it. Refusing it
        // as branch-shaped alongside `diff`/`changed`/`rebase` — which describe
        // a branch's CONTENTS and cannot be answered without one — advertised a
        // mutation the contract does not offer and sent a live task looping
        // through addresses (CAIRN-3874).
        ["task", task, "checks"] => at_thread!(TaskChecks, task_name: (*task).to_string()),
        ["task", task, "artifact"] => at_thread!(
            TaskArtifact,
            task_name: (*task).to_string(),
            name: None,
        ),
        ["task", task, "permissions"] => {
            at_thread!(TaskPermissions, task_name: (*task).to_string())
        }
        ["task", task, "permissions", segment] => at_thread!(
            TaskPermission,
            task_name: (*task).to_string(),
            segment: (*segment).to_string(),
        ),
        ["task", task, "chat"] => at_thread!(TaskChat, task_name: (*task).to_string()),
        ["task", task, "chat", "raw"] => at_thread!(TaskChatRaw, task_name: (*task).to_string()),
        ["task", task, "chat", "turn", turn] => at_thread!(
            TaskChatTurn,
            task_name: (*task).to_string(),
            turn_seq: parse_seq("task chat turn", turn)?,
        ),
        ["task", task, "chat", run, event] => at_thread!(
            TaskChatEvent,
            task_name: (*task).to_string(),
            run_seq: parse_seq("task chat run", run)?,
            event_seq: parse_seq("task chat event", event)?,
        ),
        ["task", task, "terminal", slug] => at_thread!(
            TaskTerminal,
            task_name: (*task).to_string(),
            slug: (*slug).to_string(),
        ),
        ["task", task, "browser"] => at_thread!(
            TaskBrowser,
            task_name: (*task).to_string(),
            slug: DEFAULT_BROWSER_SLUG.to_string(),
        ),
        ["task", task, "browser", slug] => at_thread!(
            TaskBrowser,
            task_name: (*task).to_string(),
            slug: (*slug).to_string(),
        ),
        ["task", task, "browser", slug, "network", request_id] => at_thread!(
            TaskBrowserNetworkRequest,
            task_name: (*task).to_string(),
            slug: (*slug).to_string(),
            request_id: (*request_id).to_string(),
        ),
        ["task", task, artifact] if !is_reserved_node_segment(artifact) => at_thread!(
            TaskArtifact,
            task_name: (*task).to_string(),
            name: Some((*artifact).to_string()),
        ),

        _ => {
            return Ok(CairnResource::Thread {
                project,
                name,
                path,
            })
        }
    };
    Ok(delegated)
}

pub(super) async fn resolve_migrated_thread_uri(
    db: &LocalDb,
    identity: &str,
) -> Result<Option<(String, String)>, String> {
    let parts: Vec<&str> = identity
        .strip_prefix("cairn://p/")
        .unwrap_or("")
        .split('/')
        .collect();
    if parts.len() < 2 {
        return Ok(None);
    }
    let Ok(_number) = parts[1].parse::<i64>() else {
        return Ok(None);
    };
    let conn = connect_for_read(db).await?;
    // The alias is a best-effort fallback: a project (or thread) that does not
    // resolve means "no alias here", never a new failure path. Erroring out
    // would preempt the canonical handling downstream — e.g. the contract
    // gate's "Unsupported resource mutation" rejection, which must win even
    // when the database knows nothing about the project in the URI.
    let alias = format!("cairn://p/{}/{}", parts[0], parts[1]);
    let Some((_, _, name)) = crate::threads::resolve_parent_thread_uri_conn(&conn, &alias)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let canonical = format!("cairn://p/{}/{}", parts[0], name);
    let mapped = match parts.as_slice() {
        [_, _, _, "thread", tail @ ..] if !tail.is_empty() => {
            format!("{canonical}/{}", tail.join("/"))
        }
        _ => canonical.clone(),
    };
    Ok(Some((mapped, canonical)))
}

pub(super) async fn read_project_threads(db: &LocalDb, project_key: &str) -> String {
    let conn = match connect_for_read(db).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let project = match lookup_project_by_key(&conn, project_key).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    // Active only. This collection is what an agent reads to find the threads it
    // can address, so a closed thread must not appear in it — while its own
    // canonical URI keeps resolving, transcript and children intact, so it can be
    // read and reopened.
    let mut rows = match conn.query("SELECT name, jurisdiction, status, attention, updated_at FROM threads WHERE project_id = ?1 AND status = 'active' ORDER BY name", params![project.project_id.as_str()]).await {
        Ok(v) => v, Err(e) => return format!("Failed to load project threads: {e}")
    };
    let mut out = format!("# Threads — {}\n\n", project.project_key);
    let mut count = 0;
    while let Ok(Some(row)) = rows.next().await {
        let (Ok(name), Ok(jurisdiction), Ok(status), Ok(attention), Ok(updated_at)) = (
            row.text(0),
            row.opt_text(1),
            row.text(2),
            row.text(3),
            row.i64(4),
        ) else {
            continue;
        };
        count += 1;
        let last_activity = crate::clock::age(updated_at);
        out.push_str(&format!("- [{name}](cairn://p/{}/{name})\n  {}Status: {status}. Attention: {attention}. Last activity: {last_activity}.\n", project.project_key, jurisdiction.map(|v| format!("Jurisdiction: {v}. ")).unwrap_or_default()));
    }
    if count == 0 {
        out.push_str("No threads.\n");
    }
    out
}

/// Render a thread's overview.
///
/// Only the overview: every addressable descendant is normalized onto its
/// node-family resource by [`delegate_thread_descendant`] before dispatch
/// reaches here, so there is no sub-path interpretation left in this function.
pub(super) async fn read_thread(
    db: &LocalDb,
    project_key: &str,
    name: &str,
    path: &[String],
) -> String {
    let conn = match connect_for_read(db).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let project = match lookup_project_by_key(&conn, project_key).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut rows = match conn.query("SELECT id, jurisdiction, status, attention, definition FROM threads WHERE project_id = ?1 AND name = ?2 LIMIT 1", params![project.project_id.as_str(), name]).await {
        Ok(v) => v, Err(e) => return format!("Failed to load thread: {e}")
    };
    let Some(row) = rows.next().await.ok().flatten() else {
        return format!("Thread '{}' not found in {}", name, project.project_key);
    };
    let (Ok(id), Ok(jurisdiction), Ok(status), Ok(attention), Ok(definition)) = (
        row.text(0),
        row.opt_text(1),
        row.text(2),
        row.text(3),
        row.opt_text(4),
    ) else {
        return "Failed to decode thread".into();
    };
    let mut jobs = match conn
        .query(
            &format!(
                "SELECT j.id, j.uri_segment, j.status FROM jobs j
                 WHERE j.thread_id = ?1 AND {}
                 ORDER BY j.created_at DESC, j.rowid DESC LIMIT 1",
                crate::threads::SESSION_JOB_SHAPE
            ),
            params![id.as_str()],
        )
        .await
    {
        Ok(v) => v,
        Err(e) => return format!("Failed to load thread session: {e}"),
    };
    let session = jobs
        .next()
        .await
        .ok()
        .flatten()
        .and_then(|row| Some((row.text(0).ok()?, row.opt_text(1).ok()?, row.text(2).ok()?)));
    // Every addressable descendant was normalized onto its node-family resource
    // by `delegate_thread_descendant` before dispatch reached here, so anything
    // still arriving with a path is a shape nothing answers for.
    if !path.is_empty() {
        return format!(
            "Unknown thread session resource '{}'. Read {} for the thread overview.",
            path.join("/"),
            cairn_common::uri::build_thread_uri(&project.project_key, name)
        );
    }
    let canonical = format!("cairn://p/{}/{}", project.project_key, name);
    let mut out = format!("# {name}\n\n");
    if let Some(jurisdiction) = jurisdiction {
        out.push_str(&format!("Jurisdiction: {jurisdiction}\n\n"));
    }
    out.push_str(&format!("Status: {status} · Attention: {attention}\n\n"));
    if let Some(definition) = definition {
        out.push_str(&format!("Definition: {definition}\n\n"));
    }
    out.push_str(&format!("- [arc]({canonical}/arc)\n"));
    match session {
        Some((job_id, segment, session_status)) => out.push_str(&format!(
            "- Live session: {}{} ({})\n",
            job_id,
            segment.map(|v| format!(" / {v}")).unwrap_or_default(),
            session_status
        )),
        None => out.push_str("- Live session: not started\n"),
    }
    let mut children = match conn
        .query(
            "SELECT number, title FROM issues WHERE parent_thread_id = ?1 ORDER BY number",
            params![id.as_str()],
        )
        .await
    {
        Ok(v) => v,
        Err(_) => return out,
    };
    out.push_str("\n## Children\n\n");
    let mut any = false;
    while let Ok(Some(row)) = children.next().await {
        let (Ok(number), Ok(title)) = (row.i64(0), row.text(1)) else {
            continue;
        };
        any = true;
        out.push_str(&format!(
            "- [{number} — {title}](cairn://p/{}/{number})\n",
            project.project_key
        ));
    }
    if !any {
        out.push_str("None.\n");
    }
    out
}

#[cfg(test)]
mod delegation_tests {
    use super::delegate_thread_descendant;
    use cairn_common::contract::ResourceKind;
    use cairn_common::uri::{parse_uri, CairnResource};

    fn delegate(path: &str) -> Result<CairnResource, String> {
        let uri = format!("cairn://p/cairn/design-review/{path}");
        delegate_thread_descendant(parse_uri(&uri).expect("a thread path always parses"))
    }

    /// The reserved coordinate carries the thread in the node slot and, for a
    /// task, its own segment beside it — exactly as an issue task is named
    /// beside its node.
    #[test]
    fn delegation_carries_the_reserved_thread_coordinate() {
        assert!(matches!(
            delegate("arc").unwrap(),
            CairnResource::NodeArtifact { number: 0, exec_seq: 0, ref node_id, name: Some(ref name), .. }
                if node_id == "design-review" && name == "arc"
        ));
        assert!(matches!(
            delegate("task/probe/return").unwrap(),
            CairnResource::TaskArtifact { number: 0, exec_seq: 0, ref node_id, ref task_name, name: Some(ref name), .. }
                if node_id == "design-review" && task_name == "probe" && name == "return"
        ));
        assert!(matches!(
            delegate("terminal/smoke").unwrap(),
            CairnResource::NodeTerminal { number: 0, exec_seq: 0, ref node_id, ref slug, .. }
                if node_id == "design-review" && slug == "smoke"
        ));
        // A bare thread stays a thread.
        assert_eq!(
            delegate_thread_descendant(parse_uri("cairn://p/cairn/design-review").unwrap())
                .unwrap()
                .kind(),
            ResourceKind::Thread
        );
    }

    /// Branch-shaped segments are refused with the reason. They are not
    /// unimplemented: a thread session has no branch, so they name something
    /// that does not exist. Before this they became artifacts of that name,
    /// which read back as "no artifact 'diff' found".
    #[test]
    fn branch_shaped_segments_are_refused_with_a_reason() {
        for path in [
            "diff",
            "changed",
            "checks",
            "rebase",
            "task/probe/diff",
            "task/probe/changed",
            "task/probe/rebase",
        ] {
            let error = delegate(path).expect_err("branch-shaped segments are refused");
            assert!(error.contains("no branch"), "{path}: {error}");
            assert!(error.contains("design-review"), "{path}: {error}");
        }
    }

    /// A task's `checks` is the task job's own read-only resource, not a
    /// branch-shaped one.
    ///
    /// `diff`, `changed`, and `rebase` describe a branch's CONTENTS and cannot
    /// be answered without one; a job's turn-end verdicts are job-shaped, so a
    /// thread's task addresses them exactly as a node's task does and the
    /// read-only contract is what answers a mutation. Refusing it as
    /// branch-shaped advertised a mutation the contract does not offer
    /// (CAIRN-3874).
    #[test]
    fn a_thread_tasks_checks_is_the_task_jobs_read_only_resource() {
        assert!(matches!(
            delegate("task/probe/checks").expect("a task's checks delegates"),
            CairnResource::TaskChecks { number: 0, exec_seq: 0, ref node_id, ref task_name, .. }
                if node_id == "design-review" && task_name == "probe"
        ));
    }

    /// A thread addresses every resource an execution node addresses.
    ///
    /// Driven by the PARSER rather than by a list written here: for every segment
    /// the parser resolves under a node, the same segment under a thread must
    /// resolve to the same resource kind. A hand-written case list cannot fail
    /// when a case is MISSING, which is how `permissions` — a sub-resource with
    /// its own parser arm, omitted from the shared reserved-keyword list — was
    /// once accepted as an artifact type-name. Adding a node sub-resource to the
    /// parser now fails here until the thread mapping learns it too.
    #[test]
    fn a_thread_addresses_every_resource_a_node_does() {
        // Branch-shaped, and deliberately refused rather than mapped: a thread
        // has no branch. This is an assertion carrying its reason, not a silent
        // exemption — `branch_shaped_segments_are_refused_with_a_reason` pins it.
        // `checks` is refused for the SESSION, whose own turn-end verdicts are
        // what a thread has no branch to produce, and mapped for a TASK, whose
        // verdicts belong to the task job like every other task-level segment.
        for (node_prefix, thread_prefix, level, refused) in [
            (
                "cairn://p/cairn/1/1/builder",
                "cairn://p/cairn/design-review",
                "node",
                &["diff", "changed", "checks", "rebase"][..],
            ),
            (
                "cairn://p/cairn/1/1/builder/task/probe",
                "cairn://p/cairn/design-review/task/probe",
                "task",
                &["diff", "changed", "rebase"][..],
            ),
        ] {
            let mut checked = 0;
            for segment in cairn_common::uri::RESERVED_NODE_SEGMENTS
                .iter()
                .copied()
                .chain(["return", "arc"])
            {
                // What the parser does with this segment under a node is the
                // specification; a segment it resolves to nothing there is not
                // one a thread owes an answer for either.
                let Some(node) = parse_uri(&format!("{node_prefix}/{segment}")) else {
                    continue;
                };
                let thread = parse_uri(&format!("{thread_prefix}/{segment}"))
                    .expect("a thread path always parses");
                let delegated = delegate_thread_descendant(thread);

                if refused.contains(&segment) {
                    assert!(
                        delegated.is_err(),
                        "{level} {segment} is branch-shaped and must be refused, not mapped"
                    );
                    continue;
                }
                assert_eq!(
                    delegated.expect("a mapped segment delegates").kind(),
                    node.kind(),
                    "a thread {level}'s {segment} must address what a node {level}'s {segment} does"
                );
                checked += 1;
            }
            assert!(
                checked >= 6,
                "the parser resolves several {level} sub-resources; only {checked} were compared, \
                 so this test is not exercising what it claims"
            );
        }
    }

    /// The multi-segment shapes the flat reserved-segment sweep above cannot
    /// reach. Same rule, same specification: the parser decides.
    #[test]
    fn multi_segment_thread_paths_address_what_a_node_does() {
        for (node_path, thread_path) in [
            ("chat/raw", "chat/raw"),
            ("chat/turn/3", "chat/turn/3"),
            ("chat/2/7", "chat/2/7"),
            ("symbols/build_widget", "symbols/build_widget"),
            ("memories/4", "memories/4"),
            ("questions/q-1", "questions/q-1"),
            ("permissions/perm-1", "permissions/perm-1"),
            ("terminal/smoke", "terminal/smoke"),
            ("repl/analysis", "repl/analysis"),
            ("browser/default", "browser/default"),
            ("browser/main/network/req-1", "browser/main/network/req-1"),
            ("task/probe/chat/raw", "task/probe/chat/raw"),
            ("task/probe/chat/turn/2", "task/probe/chat/turn/2"),
            ("task/probe/chat/1/5", "task/probe/chat/1/5"),
            ("task/probe/terminal/build", "task/probe/terminal/build"),
            ("task/probe/browser/default", "task/probe/browser/default"),
            (
                "task/probe/permissions/perm-1",
                "task/probe/permissions/perm-1",
            ),
        ] {
            let node = parse_uri(&format!("cairn://p/cairn/1/1/builder/{node_path}"))
                .unwrap_or_else(|| panic!("node path {node_path} must parse"));
            let delegated = delegate(thread_path)
                .unwrap_or_else(|error| panic!("{thread_path} must delegate: {error}"));
            assert_eq!(delegated.kind(), node.kind(), "{thread_path}");
        }
    }

    /// The union of what the two former interpreters knew, answered by the one
    /// that replaced them.
    ///
    /// These are the exact paths the split produced: `tasks` and the task write
    /// families only the WRITE side knew (so `read cairn:~/tasks` reported "no
    /// artifact 'tasks' found"), `chat/turn/N` only the READ side knew, and
    /// `terminal/<slug>` neither knew (so a thread's terminal create was judged
    /// against the thread resource itself and rejected). One function answers
    /// for all of them now, and both dispatchers call it.
    #[test]
    fn the_one_normalizer_answers_what_each_side_used_to_know_alone() {
        for (path, kind) in [
            // Write knew these; read read them as artifact names.
            ("tasks", ResourceKind::NodeTasks),
            ("todos", ResourceKind::JobTodos),
            ("questions", ResourceKind::NodeQuestions),
            ("task/probe", ResourceKind::Task),
            ("task/probe/return", ResourceKind::TaskArtifact),
            // Read knew these; write had no arm at all.
            ("chat/turn/3", ResourceKind::NodeChatTurn),
            ("chat/2/7", ResourceKind::NodeChatEvent),
            ("repl/analysis", ResourceKind::NodeRepl),
            // Neither side knew these.
            ("terminal/smoke", ResourceKind::NodeTerminal),
            ("browser", ResourceKind::NodeBrowser),
            ("task/probe/terminal/build", ResourceKind::TaskTerminal),
            ("progress", ResourceKind::NodeProgress),
            ("calls", ResourceKind::NodeCalls),
            ("symbols", ResourceKind::NodeSymbols),
            ("permissions", ResourceKind::NodePermissions),
            ("artifact", ResourceKind::NodeArtifact),
        ] {
            assert_eq!(
                delegate(path)
                    .unwrap_or_else(|error| panic!("{path} must delegate: {error}"))
                    .kind(),
                kind,
                "{path}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> LocalDb {
        let db = crate::storage::migrated_test_db("thread-resources.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-threads', 'default', 'Threads', 'thr', '/tmp/threads', 1, 1);
             INSERT INTO threads (id, project_id, name, jurisdiction, migrated_from_number, created_at, updated_at)
             VALUES ('t-design', 'p-threads', 'design-review', 'Own architecture decisions', 3404, 2, 3);
             INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, parent_thread_id, created_at, updated_at)
             VALUES ('i-child', 'p-threads', 12, 'Child issue', '', 'backlog', 'backlog', 'none', 0, 't-design', 4, 4);",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn collection_and_overview_render_first_class_thread_fields() {
        let db = fixture().await;
        let collection = read_project_threads(&db, "thr").await;
        assert!(collection.contains("[design-review](cairn://p/thr/design-review)"));
        assert!(collection.contains("Jurisdiction: Own architecture decisions"));
        assert!(collection.contains("Status: active"));
        assert!(collection.contains("Attention: none"));
        // A thread's last activity reads as an age with its absolute anchor
        // beside it, never as the bare `updated_at` epoch it is stored as.
        assert!(
            collection.contains("Last activity: ") && collection.contains(" ago ("),
            "{collection}"
        );
        assert!(
            !collection.contains("Last activity: 3."),
            "the stored epoch must not reach the surface: {collection}"
        );

        let overview = read_thread(&db, "thr", "design-review", &[]).await;
        // A thread's heading is its one identifier, the same string its URI
        // carries and its pane header shows.
        assert!(overview.contains("# design-review"));
        assert!(overview.contains("[arc](cairn://p/thr/design-review/arc)"));
        assert!(overview.contains("[12 — Child issue](cairn://p/thr/12)"));
        assert!(overview.contains("Live session: not started"));
    }

    /// The collection is an ACTIVE listing and the canonical URI is not. That
    /// asymmetry is the whole shape of dormancy: a closed thread stops being
    /// offered to agents while staying fully readable at its own address, which
    /// is what makes closing reversible from the outside.
    #[tokio::test]
    async fn the_collection_hides_a_closed_thread_that_its_own_uri_still_renders() {
        let db = fixture().await;
        db.execute_script(
            "INSERT INTO threads (id, project_id, name, jurisdiction, status, created_at, updated_at)
             VALUES ('t-retired', 'p-threads', 'retired-topic', 'Old ground', 'closed', 2, 3);
             INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, parent_thread_id, created_at, updated_at)
             VALUES ('i-retired-child', 'p-threads', 44, 'Still open child', '', 'backlog', 'backlog', 'none', 0, 't-retired', 4, 4);",
        )
        .await
        .unwrap();

        let collection = read_project_threads(&db, "thr").await;
        assert!(collection.contains("[design-review](cairn://p/thr/design-review)"));
        assert!(
            !collection.contains("retired-topic"),
            "a closed thread is not offered as somewhere to address: {collection}"
        );

        let overview = read_thread(&db, "thr", "retired-topic", &[]).await;
        assert!(overview.contains("# retired-topic"));
        assert!(overview.contains("Status: closed"));
        assert!(
            overview.contains("[44 — Still open child](cairn://p/thr/44)"),
            "closing routes attention, it does not disown children: {overview}"
        );
    }

    #[tokio::test]
    async fn migrated_number_resolves_only_when_no_issue_owns_it() {
        let db = fixture().await;
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404")
                .await
                .unwrap(),
            Some((
                "cairn://p/thr/design-review".into(),
                "cairn://p/thr/design-review".into()
            ))
        );
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404/1/thread/chat")
                .await
                .unwrap(),
            Some((
                "cairn://p/thr/design-review/chat".into(),
                "cairn://p/thr/design-review".into()
            ))
        );

        db.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
             VALUES ('i-real', 'p-threads', 3404, 'Real issue', '', 'backlog', 'backlog', 'none', 0, 5, 5)",
            (),
        )
        .await
        .unwrap();
        assert_eq!(
            resolve_migrated_thread_uri(&db, "cairn://p/thr/3404")
                .await
                .unwrap(),
            None
        );
    }

    /// A thread's descendants are read by the ORDINARY node renderers, reached
    /// through the reserved coordinate the normalizer produces.
    ///
    /// Driving the node renderers directly is what this proves: no thread-specific
    /// read path is involved, and the URIs they render address the thread rather
    /// than a `0/0` placeholder.
    #[tokio::test]
    async fn thread_descendants_read_through_the_ordinary_node_renderers() {
        let db = fixture().await;
        let job_id = crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute(
            r#"INSERT INTO artifacts (id, job_id, artifact_type, output_name, data, created_at, updated_at) VALUES ('a-arc', ?1, 'document', 'arc', '{"content":"Canonical arc"}', 5, 5)"#,
            (job_id.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO memories (id, name, project_id, content, scope, scope_value, job_id, node_seq, created_at, updated_at) VALUES ('m-thread', 'Thread fact', 'p-threads', 'Remember this detail', 'project', 'p-threads', ?1, 1, 6, 6)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, parent_job_id, project_id, status, node_name, uri_segment, created_at, updated_at) VALUES ('j-task', ?1, 'p-threads', 'complete', 'research', 'research', 7, 8)",
            (job_id.as_str(),),
        )
        .await
        .unwrap();

        // The session job is what a thread coordinate resolves to, and a task it
        // spawned resolves beneath it.
        assert_eq!(
            crate::jobs::queries::job_id_for_node_coordinate(
                &db,
                "thr",
                0,
                0,
                "design-review",
                None
            )
            .await
            .unwrap()
            .as_deref(),
            Some(job_id.as_str())
        );
        assert_eq!(
            crate::jobs::queries::job_id_for_node_coordinate(
                &db,
                "thr",
                0,
                0,
                "design-review",
                Some("research")
            )
            .await
            .unwrap()
            .as_deref(),
            Some("j-task")
        );

        // The node renderers, driven at the thread coordinate.
        let wakes = super::super::node::read_node_wakes(&db, "thr", 0, 0, "design-review").await;
        assert!(wakes.contains("# Wakes — design-review"), "{wakes}");
        // `ensure_thread_session` seeds the default system subscriptions so a
        // thread session hears messages from birth.
        assert!(wakes.contains("peer `*`"), "{wakes}");
        assert!(wakes.contains("user `*`"), "{wakes}");
        // The rendered address is the thread's, not a 0/0 placeholder.
        assert!(
            wakes.contains("cairn://p/thr/design-review/wakes"),
            "{wakes}"
        );
        assert!(!wakes.contains("/0/0/"), "{wakes}");

        let todos =
            super::super::node::read_job_todos(&db, "thr", 0, 0, "design-review", None).await;
        assert!(!todos.contains("/0/0/"), "{todos}");

        let tasks = super::super::node::read_node_tasks(&db, "thr", 0, 0, "design-review").await;
        assert!(tasks.contains("research"), "{tasks}");
        assert!(!tasks.contains("/0/0/"), "{tasks}");

        let task =
            super::super::node::read_task(&db, "thr", 0, 0, "design-review", "research").await;
        assert!(task.contains("research"), "{task}");
        assert!(!task.contains("/0/0/"), "{task}");

        let memories = super::super::memories::render_job_memories(
            &db,
            "design-review",
            "cairn://p/thr/design-review/memories",
            &job_id,
        )
        .await;
        assert!(
            memories.contains("cairn://p/thr/design-review/memories/1"),
            "{memories}"
        );
        assert_eq!(
            crate::memories::db::resolve_node_memory_id(&db, "thr", 0, 0, "design-review", 1)
                .await
                .unwrap()
                .as_deref(),
            Some("m-thread"),
            "a thread's memories are individually addressable"
        );
    }

    /// A read of a dormant thread degrades to its overview and, crucially, does
    /// not bring the session job into existence.
    #[tokio::test]
    async fn reading_a_thread_without_a_session_creates_nothing() {
        let db = fixture().await;
        async fn job_count(db: &LocalDb) -> i64 {
            db.query_opt("SELECT COUNT(*) FROM jobs", (), |row| row.i64(0))
                .await
                .unwrap()
                .unwrap()
        }
        assert_eq!(job_count(&db).await, 0);

        for body in [
            super::super::node::read_node_wakes(&db, "thr", 0, 0, "design-review").await,
            super::super::node::read_job_todos(&db, "thr", 0, 0, "design-review", None).await,
            super::super::node::read_node_tasks(&db, "thr", 0, 0, "design-review").await,
        ] {
            assert!(body.contains("has no session yet"), "{body}");
            assert!(body.contains("cairn://p/thr/design-review"), "{body}");
        }

        assert_eq!(
            job_count(&db).await,
            0,
            "a read must never mint a thread's session job"
        );

        let overview = read_thread(&db, "thr", "design-review", &[]).await;
        assert!(overview.contains("Live session: not started"), "{overview}");
    }
}
