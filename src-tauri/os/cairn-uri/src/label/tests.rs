use super::LabelContext;
use crate::samples::every_resource_variant;
use crate::{delegate_thread_descendant, parse_uri};

fn label(uri: &str, ctx: &LabelContext) -> String {
    let resource = parse_uri(uri)
        .and_then(|resource| delegate_thread_descendant(resource).ok())
        .unwrap_or_else(|| panic!("{uri} must resolve"));
    resource.label(ctx)
}

fn named(node: &str, task: &str) -> LabelContext {
    LabelContext {
        node_label: Some(node.to_string()),
        task_label: Some(task.to_string()),
    }
}

/// The chip labels the frontend showed before it adopted the Rust grammar,
/// plus branches it never reached. The frontend now renders every label
/// through `label` in the wasm module, so this holds them at their source.
#[test]
fn the_frontend_labels_are_reproduced() {
    let plain = LabelContext::default();
    let sha = "a".repeat(64);
    let legacy = format!("cairn://p/CAIRN/images/{sha}");
    let batches = "cairn://p/CAIRN/42/1/builder";
    let task_batches = "cairn://p/CAIRN/42/1/builder/task/review";
    let cases: Vec<(String, LabelContext, &str)> = vec![
        // Stored images, in every scope and both address forms.
        (
            "cairn://p/cairn/ui/images/1".into(),
            plain.clone(),
            "cairn/ui / image 1",
        ),
        (
            "cairn://p/CAIRN/roadmap-2/images".into(),
            plain.clone(),
            "cairn/roadmap-2 / images",
        ),
        (
            "cairn://p/CAIRN/3242/images".into(),
            plain.clone(),
            "cairn/3242 / images",
        ),
        (
            "cairn://p/CAIRN/images".into(),
            plain.clone(),
            "cairn / images",
        ),
        (legacy, plain.clone(), "cairn / image"),
        (
            "cairn://p/CAIRN/3242/images/1".into(),
            plain.clone(),
            "cairn/3242 / image 1",
        ),
        (
            "cairn://p/CAIRN/images/7".into(),
            plain.clone(),
            "cairn / image 7",
        ),
        // Threads: the thread, then its last path segment.
        (
            "cairn://p/CAIRN/general".into(),
            plain.clone(),
            "cairn/general",
        ),
        (
            "cairn://p/CAIRN/general/chat".into(),
            plain.clone(),
            "cairn/general / chat",
        ),
        (
            "cairn://p/CAIRN/general/task/review/create-pr".into(),
            plain.clone(),
            "cairn/general / create-pr",
        ),
        (
            "cairn://p/CAIRN/general/chat/turn/60".into(),
            plain.clone(),
            "cairn/general / 60",
        ),
        (
            "cairn://p/CAIRN/general/questions".into(),
            plain.clone(),
            "cairn/general / questions",
        ),
        // Project-level.
        ("cairn://p/CAIRN".into(), plain.clone(), "cairn"),
        ("cairn://p/CAIRN/messages".into(), plain.clone(), "cairn"),
        ("cairn://p/CAIRN/home".into(), plain.clone(), "cairn / home"),
        (
            "cairn://p/CAIRN/headlines".into(),
            plain.clone(),
            "cairn / headlines",
        ),
        (
            "cairn://p/CAIRN/threads".into(),
            plain.clone(),
            "cairn / threads",
        ),
        (
            "cairn://p/CAIRN/posts".into(),
            plain.clone(),
            "cairn / posts",
        ),
        (
            "cairn://p/CAIRN/tenets".into(),
            plain.clone(),
            "cairn / tenets",
        ),
        (
            "cairn://p/CAIRN/tenets/12".into(),
            plain.clone(),
            "cairn / tenet #12",
        ),
        (
            "cairn://p/CAIRN/tenets/one-grammar".into(),
            plain.clone(),
            "cairn / tenet one-grammar",
        ),
        (
            "cairn://p/CAIRN/terminal/dev".into(),
            plain.clone(),
            "cairn / dev",
        ),
        (
            "cairn://p/CAIRN/camps".into(),
            plain.clone(),
            "cairn / camps",
        ),
        (
            "cairn://p/CAIRN/camps/base".into(),
            plain.clone(),
            "cairn / base camp",
        ),
        (
            "cairn://p/CAIRN/camps/rebuild".into(),
            plain.clone(),
            "cairn / camp rebuild",
        ),
        (
            "cairn://p/CAIRN/camps/rebuild/members".into(),
            plain.clone(),
            "cairn / camp rebuild / members",
        ),
        (
            "cairn://p/CAIRN/camps/rebuild/posts".into(),
            plain.clone(),
            "cairn / rebuild camp posts",
        ),
        (
            "cairn://p/CAIRN/camps/base/posts".into(),
            plain.clone(),
            "cairn / base camp posts",
        ),
        // Workspace-level.
        ("cairn://posts".into(), plain.clone(), "Posts"),
        ("cairn://posts/25".into(), plain.clone(), "Post 25"),
        ("cairn://requests".into(), plain.clone(), "Requests"),
        (
            "cairn://requests/request_1-abc".into(),
            plain.clone(),
            "Request request_1-abc",
        ),
        // Issues.
        ("cairn://p/CAIRN/123".into(), plain.clone(), "cairn/123"),
        (
            "cairn://p/CAIRN/123/messages".into(),
            plain.clone(),
            "cairn/123",
        ),
        (
            "cairn://p/CAIRN/123/changed".into(),
            plain.clone(),
            "cairn/123",
        ),
        // Nodes: the raw segment unless the caller names it.
        (
            "cairn://p/CAIRN/42/1/builder".into(),
            plain.clone(),
            "cairn/42 / builder",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/chat".into(),
            named("Builder", "-"),
            "cairn/42 / Builder",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/chat/turn/3".into(),
            plain.clone(),
            "cairn/42 / builder",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/chat/1/7".into(),
            plain.clone(),
            "cairn/42 / builder",
        ),
        (
            "cairn://p/CAIRN/123/1/Builder/artifact".into(),
            plain.clone(),
            "cairn/123 / Builder / artifact",
        ),
        (
            "cairn://p/CAIRN/42/1/Builder/board".into(),
            plain.clone(),
            "cairn/42 / Builder / board",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/diff".into(),
            plain.clone(),
            "cairn/42 / builder / diff",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/questions/q-1".into(),
            plain.clone(),
            "cairn/42 / builder / question",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/permissions/perm-1".into(),
            plain.clone(),
            "cairn/42 / builder / permission",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/terminal/smoke".into(),
            plain.clone(),
            "cairn/42 / builder / smoke",
        ),
        // Tasks.
        (
            "cairn://p/CAIRN/42/1/builder-node/task/explore-2/chat".into(),
            named("Builder", "Explore"),
            "cairn/42 / Builder / Explore",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/task/review/chat".into(),
            plain.clone(),
            "cairn/42 / builder / review",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/task/review/create-pr".into(),
            plain.clone(),
            "cairn/42 / builder / review / create-pr",
        ),
        (
            "cairn://p/CAIRN/42/1/builder/task/review/artifact".into(),
            plain.clone(),
            "cairn/42 / builder / review / artifact",
        ),
        // Batches, in both scopes, with and without display names.
        (
            format!("{batches}/batches"),
            plain.clone(),
            "cairn/42 / builder / batches",
        ),
        (
            format!("{batches}/batches/run-batch-123"),
            plain.clone(),
            "cairn/42 / builder / batch run-batch-123",
        ),
        (
            format!("{task_batches}/batches"),
            plain.clone(),
            "cairn/42 / builder / review / batches",
        ),
        (
            format!("{task_batches}/batches/run-batch-123"),
            plain.clone(),
            "cairn/42 / builder / review / batch run-batch-123",
        ),
        (
            format!("{batches}/batches"),
            named("Builder", "Review"),
            "cairn/42 / Builder / batches",
        ),
        (
            format!("{task_batches}/batches"),
            named("Builder", "Review"),
            "cairn/42 / Builder / Review / batches",
        ),
        (
            format!("{task_batches}/batches/run-batch-123"),
            named("Builder", "Review"),
            "cairn/42 / Builder / Review / batch run-batch-123",
        ),
    ];
    for (uri, ctx, expected) in cases {
        assert_eq!(label(&uri, &ctx), expected, "{uri}");
    }
}

#[test]
fn the_wider_style_holds_for_kinds_the_frontend_never_labelled() {
    let plain = LabelContext::default();
    for (uri, expected) in [
        ("cairn://executors", "Executors"),
        ("cairn://executors/bbs", "Executor bbs"),
        ("cairn://skills/testing", "Skill testing"),
        ("cairn://models", "Models"),
        ("cairn://posts/12/comments/3", "Post 12 / comment 3"),
        (
            "cairn://p/cairn/6241/1/builder/questions",
            "cairn/6241 / builder / questions",
        ),
        (
            "cairn://p/cairn/6241/1/builder/checks",
            "cairn/6241 / builder / checks",
        ),
        (
            "cairn://p/cairn/6241/1/builder/tasks",
            "cairn/6241 / builder / tasks",
        ),
        (
            "cairn://p/cairn/6241/1/builder/messages",
            "cairn/6241 / builder / messages",
        ),
        (
            "cairn://p/cairn/uri-parity/questions",
            "cairn/uri-parity / questions",
        ),
        (
            "cairn://p/cairn/uri-parity/db/attempt%20log",
            "cairn/uri-parity / attempt log",
        ),
        ("cairn://p/cairn/42/comments/3", "cairn/42 / comment 3"),
    ] {
        assert_eq!(label(uri, &plain), expected, "{uri}");
    }
}

#[test]
fn every_sample_has_a_finished_label() {
    for ctx in [LabelContext::default(), named("Builder", "Review")] {
        for sample in every_resource_variant() {
            let label = sample.resource.label(&ctx);
            let uri = sample.resource.to_uri();
            assert!(!label.trim().is_empty(), "{uri} has an empty label");
            assert!(!label.contains('{'), "{uri} has an unfilled label: {label}");
            assert!(
                !label.contains("cairn://"),
                "{uri} labels as an address: {label}"
            );
        }
    }
}
