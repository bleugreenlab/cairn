//! What a thread's sub-path means.
//!
//! The parser stops every thread URI at `CairnResource::Thread { path }` and
//! hands the path on intact; [`delegate_thread_descendant`] is the one reader
//! that names the variant, shared by read dispatch, write dispatch, `run`, and
//! (compiled to WebAssembly) the frontend. It is pure path matching: no store
//! is consulted, so an address means the same thing on every side.

use crate::{is_reserved_node_segment, CairnResource, DEFAULT_BROWSER_SLUG};

/// The path segment under every `/db/` that never names a table. Under a thread
/// it is the generated schema projection (`.../db/schema`); under `cairn://db`
/// it is held back so the two roots keep one grammar.
pub const RESERVED_TABLE_SEGMENT: &str = "schema";

/// The refusal for a table or row address whose table segment is the reserved
/// one. `db_root` is the store's canonical root URI.
pub fn reserved_table_segment_refusal(db_root: &str) -> String {
    format!(
        "`{RESERVED_TABLE_SEGMENT}` is a reserved segment under {db_root} and never names a table. \
         Read a table called `{RESERVED_TABLE_SEGMENT}` with {db_root}?sql=SELECT * FROM \"{RESERVED_TABLE_SEGMENT}\" \
         or {db_root}?table={RESERVED_TABLE_SEGMENT}."
    )
}

/// Why an address under `procedures` or `automations` resolves to nothing, and
/// what replaced it: a thread runs code without a turn only through a service
/// (tenet 1313). Read, write, and `run` all reach this through
/// [`delegate_thread_descendant`], so each says the same thing.
fn retired_surface_refusal(project: &str, thread: &str, surface: &str) -> String {
    let services = format!("{}/services", crate::build_thread_uri(project, thread));
    let shape = "write({changes:[{target:\"cairn:~/services/<name>\",mode:\"create\",\
         payload:{files:{\"main.ts\":\"import { serve } from '@cairn/sdk/service'; \
         serve({methods:{refresh: async (params, ctx) => { /* ctx.cairn.run([...]), \
         ctx.cairn.write([...]) */ }}});\"}, manifest:{schedule:[{every:\"15m\",\
         method:\"refresh\"}]}}}]})";
    let lead = if surface == "automations" {
        "Thread automations are retired (tenet 1313): scheduled thread code is a `schedule` \
         entry in a service's manifest. The runner calls the method every interval as this \
         thread, with no turn, and wakes the thread only when a failure streak begins."
    } else {
        "Thread procedures are retired (tenet 1313): thread code that runs without a turn is a \
         method of a service, in its `main.ts` under `serve({methods})`. A former procedure's \
         batch runs inside the method as `ctx.cairn.run([...])`; call the method with \
         write({changes:[{target:\"cairn:~/services/<name>\",mode:\"append\",payload:{method:\
         \"refresh\"}}]}), or schedule it with `manifest.schedule`."
    };
    format!(
        "{lead} Write a service at {services}/<name>: {shape}. Its next due time and last \
         outcome are on {services}/<name>/status. See cairn://skills/pages (Services) and \
         docs/services.md."
    )
}

/// The refusal for `resource` when it addresses a retired thread surface
/// (`procedures` or `automations`, at session or task level).
pub fn retired_surface(resource: &CairnResource) -> Option<String> {
    let CairnResource::Thread {
        project,
        name,
        path,
    } = resource
    else {
        return None;
    };
    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    match segments.as_slice() {
        [surface @ ("procedures" | "automations"), ..]
        | ["task", _, surface @ ("procedures" | "automations"), ..] => {
            Some(retired_surface_refusal(project, name, surface))
        }
        _ => None,
    }
}

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

/// A thread database table or primary-key segment, percent-decoded as
/// `build_db_table_uri`/`build_db_row_uri` encoded it.
fn decode_db_segment(kind: &str, value: &str) -> Result<String, String> {
    crate::decode_path_segment(value)
        .map_err(|error| format!("Invalid thread database {kind} segment '{value}': {error}"))
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
/// [`crate::NodeAddress`], which is also what renders it back.
///
/// `Err` is a purposeful refusal carrying its reason, not a failure to map.
pub fn delegate_thread_descendant(resource: CairnResource) -> Result<CairnResource, String> {
    if let Some(refusal) = retired_surface(&resource) {
        return Err(refusal);
    }
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
        ["queue"] => CairnResource::ThreadQueue {
            project,
            thread: name,
        },

        ["db"] | ["task", _, "db"] => {
            return Ok(CairnResource::ThreadDb { project, name });
        }
        // The schema projection keeps its arm ahead of the row grammar: `schema`
        // is reserved under `/db/`, so it never reaches the table arms below.
        ["db", "schema"] | ["task", _, "db", "schema"] => {
            return Ok(CairnResource::ThreadDbSchema { project, name });
        }
        ["db", "schema", _] | ["task", _, "db", "schema", _] => {
            return Err(reserved_table_segment_refusal(&format!(
                "{}/db",
                crate::build_thread_uri(&project, &name)
            )));
        }
        ["db", table] | ["task", _, "db", table] => {
            return Ok(CairnResource::ThreadDbTable {
                project,
                name,
                table: decode_db_segment("table", table)?,
            });
        }
        ["db", table, pk] | ["task", _, "db", table, pk] => {
            return Ok(CairnResource::ThreadDbRow {
                project,
                name,
                table: decode_db_segment("table", table)?,
                pk: decode_db_segment("primary key", pk)?,
            });
        }
        // A thread's services are the thread's, from its session and its tasks
        // alike, as its database is.
        ["services"] | ["task", _, "services"] => {
            return Ok(CairnResource::ThreadServices { project, name });
        }
        ["services", service] | ["task", _, "services", service] => {
            return Ok(CairnResource::ThreadService {
                project,
                name,
                service: (*service).to_string(),
            });
        }
        ["services", service, "status"] | ["task", _, "services", service, "status"] => {
            return Ok(CairnResource::ThreadServiceStatus {
                project,
                name,
                service: (*service).to_string(),
            });
        }
        ["services", service, "log"] | ["task", _, "services", service, "log"] => {
            return Ok(CairnResource::ThreadServiceLog {
                project,
                name,
                service: (*service).to_string(),
            });
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
        // A thread agent's `run` hands back `cairn:~/batches/<id>` like any
        // node's does, so the handle has to resolve from a thread too.
        ["batches"] => at_thread!(NodeBatches),
        ["batches", request_id] => at_thread!(NodeBatch, request_id: (*request_id).to_string()),
        ["wakes"] => at_thread!(NodeWakes),
        ["questions"] => at_thread!(NodeQuestions),
        ["questions", segment] => at_thread!(NodeQuestion, segment: (*segment).to_string()),
        ["permissions"] => at_thread!(NodePermissions),
        ["permissions", segment] => at_thread!(NodePermission, segment: (*segment).to_string()),
        ["messages"] => at_thread!(NodeMessages),
        ["progress"] => at_thread!(NodeProgress),
        ["memories"] => at_thread!(NodeMemories),
        ["memories", seq] => at_thread!(NodeMemory, memory_seq: parse_seq("memory sequence", seq)?),
        ["eras"] => at_thread!(NodeEras),
        ["eras", seq] => at_thread!(NodeEra, era_seq: parse_seq("era sequence", seq)?),
        ["pages"] => at_thread!(NodePages),
        ["pages", page] => at_thread!(NodePage, name: (*page).to_string()),
        ["pages", page, "log"] => at_thread!(NodePageLog, name: (*page).to_string()),
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
        // does under a node, where a thread session's own write is refused (a
        // thread keeps no artifacts) and its tasks' return. The guard is
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
        ["task", task, "batches"] => at_thread!(TaskBatches, task_name: (*task).to_string()),
        ["task", task, "batches", request_id] => at_thread!(
            TaskBatch,
            task_name: (*task).to_string(),
            request_id: (*request_id).to_string(),
        ),
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

#[cfg(test)]
mod tests {
    use super::delegate_thread_descendant;
    use crate::ResourceKind;
    use crate::{parse_uri, CairnResource};

    fn delegate(path: &str) -> Result<CairnResource, String> {
        let uri = format!("cairn://p/cairn/design-review/{path}");
        delegate_thread_descendant(parse_uri(&uri).expect("a thread path always parses"))
    }

    /// Every `CairnResource` variant survives being written as a URI and read
    /// back — the whole trip, through both readers that share the grammar.
    ///
    /// `to_uri` and `parse_uri` are the grammar; the meaning of a thread
    /// sub-path lives here. That split is deliberate (one resolver for thread
    /// sub-paths), but it is only safe while the composition is TOTAL, and
    /// nothing on either side of the crate boundary can see the whole of it
    /// alone. This is where the halves meet: `delegate_thread_descendant` is the
    /// identity on everything that is not a thread sub-path, so one assertion
    /// covers the parser's own families and the resolver's alike.
    ///
    /// The sample table is exhaustive over the enum by construction, so a
    /// variant added without a case fails to COMPILE in
    /// `crate::samples`. That is the guard that was missing when
    /// `build.rs` learned to render `procedures/<name>` and nothing learned to
    /// read it, killing every procedure invocation and scheduled automation for
    /// fifteen days (CAIRN-4725).
    #[test]
    fn every_resource_variant_survives_its_own_uri() {
        for sample in crate::samples::every_resource_variant() {
            let uri = sample.resource.to_uri();
            let parsed = parse_uri(&uri).unwrap_or_else(|| panic!("{uri} must parse"));
            let resolved = delegate_thread_descendant(parsed)
                .unwrap_or_else(|error| panic!("{uri} must resolve: {error}"));
            assert_eq!(
                resolved, sample.resource,
                "{uri} must read back as the variant that built it"
            );
        }
    }

    #[test]
    fn a_thread_database_delegates_to_its_first_class_resource() {
        let resource = delegate("db").expect("a thread owns its database");
        assert_eq!(resource.kind(), ResourceKind::ThreadDb);
        assert_eq!(resource.to_uri(), "cairn://p/cairn/design-review/db");
        assert_eq!(resource.project(), Some("cairn"));
        assert_eq!(resource.to_route(), None);
    }

    #[test]
    fn a_thread_database_schema_delegates_to_its_read_only_resource() {
        let resource = delegate("db/schema").expect("a thread database exposes its schema");
        assert_eq!(resource.kind(), ResourceKind::ThreadDbSchema);
        assert_eq!(resource.to_uri(), "cairn://p/cairn/design-review/db/schema");
        assert_eq!(resource.project(), Some("cairn"));
        assert_eq!(resource.to_route(), None);
    }

    #[test]
    fn a_task_database_resolves_to_the_owning_thread_store() {
        let resource = delegate("task/probe/db").expect("a task uses its owning thread database");
        assert_eq!(resource.kind(), ResourceKind::ThreadDb);
        assert_eq!(resource.to_uri(), "cairn://p/cairn/design-review/db");
    }

    /// Every address under the retired surfaces, at session and task level,
    /// is refused with the service shape that replaced it.
    #[test]
    fn procedures_and_automations_are_refused_with_the_service_shape() {
        for path in [
            "procedures",
            "procedures/collect",
            "task/probe/procedures",
            "task/probe/procedures/collect",
            "automations",
            "automations/auto-1",
            "task/probe/automations",
        ] {
            let refusal = delegate(path).expect_err(path);
            assert!(refusal.contains("tenet 1313"), "{path}: {refusal}");
            assert!(refusal.contains("serve({methods"), "{path}: {refusal}");
            assert!(refusal.contains("schedule"), "{path}: {refusal}");
            assert!(
                refusal.contains("cairn://p/cairn/design-review/services"),
                "{path}: {refusal}"
            );
        }
        assert!(delegate("automations")
            .unwrap_err()
            .starts_with("Thread automations are retired"));
        assert!(delegate("procedures/collect")
            .unwrap_err()
            .starts_with("Thread procedures are retired"));
    }

    /// The reserved coordinate carries the thread in the node slot and, for a
    /// task, its own segment beside it — exactly as an issue task is named
    /// beside its node.
    #[test]
    fn delegation_carries_the_reserved_thread_coordinate() {
        assert!(matches!(
            delegate("plan").unwrap(),
            CairnResource::NodeArtifact { number: 0, exec_seq: 0, ref node_id, name: Some(ref name), .. }
                if node_id == "design-review" && name == "plan"
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
            for segment in crate::RESERVED_NODE_SEGMENTS
                .iter()
                .copied()
                .chain(["return", "notes"])
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
            ("eras/7", "eras/7"),
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
