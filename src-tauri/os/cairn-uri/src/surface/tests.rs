use super::Surface;
use crate::samples::every_resource_variant;
use crate::{delegate_thread_descendant, parse_uri};

fn surface(uri: &str) -> Surface {
    parse_uri(uri)
        .and_then(|resource| delegate_thread_descendant(resource).ok())
        .unwrap_or_else(|| panic!("{uri} must resolve"))
        .surface()
}

fn read(uri: &str) -> Surface {
    Surface::Read {
        uri: uri.to_string(),
    }
}

fn node_chat() -> Surface {
    Surface::NodeChat {
        project: "cairn".into(),
        number: 42,
        exec: 1,
        node: "builder".into(),
    }
}

/// The views the frontend's `toRoute` and `useResourceNavigation` open
/// today, kind by kind, including where this deliberately differs.
#[test]
fn each_address_opens_the_view_the_app_has_for_it() {
    let p = || "cairn".to_string();
    let thread = |name: &str| Surface::ThreadChat {
        project: p(),
        name: name.into(),
    };
    for (uri, expected) in [
        ("cairn://p/cairn", Surface::ProjectIssues { project: p() }),
        (
            "cairn://p/cairn/messages",
            Surface::ProjectIssues { project: p() },
        ),
        (
            "cairn://p/cairn/threads",
            Surface::ProjectThreads { project: p() },
        ),
        (
            "cairn://p/cairn/home",
            Surface::ProjectHome { project: p() },
        ),
        (
            "cairn://p/cairn/tenets",
            Surface::ProjectTenets { project: p() },
        ),
        (
            "cairn://p/cairn/tenets/12",
            Surface::ProjectTenets { project: p() },
        ),
        (
            "cairn://p/cairn/pages",
            Surface::ProjectPages { project: p() },
        ),
        (
            "cairn://p/cairn/posts",
            Surface::Posts {
                project: Some(p()),
                post: None,
            },
        ),
        (
            "cairn://posts",
            Surface::Posts {
                project: None,
                post: None,
            },
        ),
        (
            "cairn://posts/25",
            Surface::Posts {
                project: None,
                post: Some(25),
            },
        ),
        (
            "cairn://p/cairn/terminal/dev",
            Surface::ProjectTerminal {
                project: p(),
                slug: "dev".into(),
            },
        ),
        ("cairn://p/cairn/images/7", Surface::Inline),
        ("cairn://p/cairn/3242/images/1", Surface::Inline),
        (
            "cairn://p/cairn/42",
            Surface::IssueOverview {
                project: p(),
                number: 42,
            },
        ),
        (
            "cairn://p/cairn/42/changed",
            Surface::IssueOverview {
                project: p(),
                number: 42,
            },
        ),
        ("cairn://p/cairn/42/1/builder", node_chat()),
        ("cairn://p/cairn/42/1/builder/chat", node_chat()),
        ("cairn://p/cairn/42/1/builder/chat/turn/3", node_chat()),
        (
            "cairn://p/cairn/42/1/builder/diff",
            Surface::NodeDiff {
                project: p(),
                number: 42,
                exec: 1,
                node: "builder".into(),
            },
        ),
        (
            "cairn://p/cairn/42/1/builder/plan",
            Surface::NodeArtifact {
                project: p(),
                number: 42,
                exec: 1,
                node: "builder".into(),
                name: Some("plan".into()),
            },
        ),
        (
            "cairn://p/cairn/42/1/builder/terminal/smoke",
            Surface::NodeTerminal {
                project: p(),
                number: 42,
                exec: 1,
                node: "builder".into(),
                slug: "smoke".into(),
            },
        ),
        (
            "cairn://p/cairn/42/1/builder/task/review/chat",
            Surface::TaskChat {
                project: p(),
                number: 42,
                exec: 1,
                node: "builder".into(),
                task: "review".into(),
            },
        ),
        // Deliberate: the frontend sends these to the node's chat, which
        // does not show one question or permission request by itself.
        (
            "cairn://p/cairn/42/1/builder/questions/q-1",
            read("cairn://p/cairn/42/1/builder/questions/q-1"),
        ),
        (
            "cairn://p/cairn/42/1/builder/permissions/perm-1",
            read("cairn://p/cairn/42/1/builder/permissions/perm-1"),
        ),
        (
            "cairn://p/cairn/42/1/builder/batches",
            read("cairn://p/cairn/42/1/builder/batches"),
        ),
        // Threads.
        ("cairn://p/cairn/general", thread("general")),
        ("cairn://p/cairn/general/chat", thread("general")),
        (
            "cairn://p/cairn/general/task/review/chat",
            thread("general"),
        ),
        (
            "cairn://p/cairn/general/tenets",
            Surface::ThreadTenets {
                project: p(),
                name: "general".into(),
            },
        ),
        (
            "cairn://p/cairn/general/pages/board",
            Surface::ThreadPage {
                project: p(),
                name: "general".into(),
                page: "board".into(),
            },
        ),
        // Deliberate: the frontend opens the thread's chat for these.
        (
            "cairn://p/cairn/general/questions",
            read("cairn://p/cairn/general/questions"),
        ),
        (
            "cairn://p/cairn/general/todos",
            read("cairn://p/cairn/general/todos"),
        ),
        // Workspace views.
        ("cairn://requests", Surface::Requests { request: None }),
        (
            "cairn://requests/request_1-abc",
            Surface::Requests {
                request: Some("request_1-abc".into()),
            },
        ),
        ("cairn://rebuilds", Surface::Builds { rebuild: None }),
        (
            "cairn://rebuilds/req-1",
            Surface::Builds {
                rebuild: Some("req-1".into()),
            },
        ),
        ("cairn://executors", Surface::Fleet { machine: None }),
        (
            "cairn://executors/bbs",
            Surface::Fleet {
                machine: Some("bbs".into()),
            },
        ),
        ("cairn://settings", Surface::Settings { project: None }),
        ("cairn://db", Surface::Database),
        ("cairn://logs", Surface::Logs),
        ("cairn://models", read("cairn://models")),
    ] {
        assert_eq!(surface(uri), expected, "{uri}");
    }
}

/// A `Read` pane reads the canonical address back, so it must name the
/// same resource.
#[test]
fn a_read_surface_names_its_own_resource() {
    for sample in every_resource_variant() {
        if let Surface::Read { uri } = sample.resource.surface() {
            assert_eq!(uri, sample.resource.to_uri());
        }
    }
}
