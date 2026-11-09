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
}

#[test]
fn a_thread_database_schema_delegates_to_its_read_only_resource() {
    let resource = delegate("db/schema").expect("a thread database exposes its schema");
    assert_eq!(resource.kind(), ResourceKind::ThreadDbSchema);
    assert_eq!(resource.to_uri(), "cairn://p/cairn/design-review/db/schema");
    assert_eq!(resource.project(), Some("cairn"));
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
        ("batches/req-1", "batches/req-1"),
        ("task/probe/batches/req-1", "task/probe/batches/req-1"),
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
        ("batches", ResourceKind::NodeBatches),
        ("task/probe/batches", ResourceKind::TaskBatches),
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
