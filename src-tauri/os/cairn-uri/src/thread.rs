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
mod tests;
