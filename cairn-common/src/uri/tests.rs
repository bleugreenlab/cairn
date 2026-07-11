use super::*;
use crate::contract::ResourceKind;
use crate::query::QueryParam;

#[test]
fn parses_symbol_resources_all_forms() {
    assert_eq!(
        parse_uri("cairn://p/cairn/12/1/builder/symbols"),
        Some(CairnResource::NodeSymbols {
            project: "CAIRN".to_string(),
            number: 12,
            exec_seq: 1,
            node_id: "builder".to_string(),
            symbol: None,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/12/1/builder/symbols/build_widget"),
        Some(CairnResource::NodeSymbols {
            project: "CAIRN".to_string(),
            number: 12,
            exec_seq: 1,
            node_id: "builder".to_string(),
            symbol: Some("build_widget".to_string()),
        })
    );
    // A `::`-qualified symbol survives as one segment.
    assert_eq!(
        parse_uri("cairn://p/cairn/12/1/builder/symbols/Foo::bar"),
        Some(CairnResource::NodeSymbols {
            project: "CAIRN".to_string(),
            number: 12,
            exec_seq: 1,
            node_id: "builder".to_string(),
            symbol: Some("Foo::bar".to_string()),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/symbols"),
        Some(CairnResource::ProjectSymbols {
            project: "CAIRN".to_string(),
            symbol: None,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/symbols/build_widget"),
        Some(CairnResource::ProjectSymbols {
            project: "CAIRN".to_string(),
            symbol: Some("build_widget".to_string()),
        })
    );
}

#[test]
fn symbols_segment_is_reserved_not_an_artifact() {
    assert!(is_reserved_node_segment("symbols"));
    // `.../node/symbols` must parse as NodeSymbols, never a NodeArtifact named "symbols".
    assert!(matches!(
        parse_uri("cairn://p/cairn/12/1/builder/symbols"),
        Some(CairnResource::NodeSymbols { .. })
    ));
}

#[test]
fn symbol_uris_round_trip() {
    for uri in [
        "cairn://p/CAIRN/12/1/builder/symbols",
        "cairn://p/CAIRN/12/1/builder/symbols/build_widget",
        "cairn://p/CAIRN/symbols",
        "cairn://p/CAIRN/symbols/build_widget",
    ] {
        assert_eq!(parse_uri(uri).unwrap().to_uri(), uri, "round-trip {uri}");
    }
}

#[test]
fn parses_canonical_project_resources() {
    assert_eq!(
        parse_uri("cairn://p/cairn/123/changed"),
        Some(CairnResource::Changed {
            project: "CAIRN".to_string(),
            number: 123,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/issues"),
        Some(CairnResource::ProjectIssues {
            project: "CAIRN".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/messages"),
        Some(CairnResource::ProjectMessages {
            project: "CAIRN".to_string(),
        })
    );
}

#[test]
fn parses_and_roundtrips_node_and_task_messages() {
    // Node `/messages` is the canonical node messaging target.
    let node = parse_uri("cairn://p/cairn/42/2/builder/messages");
    assert_eq!(
        node,
        Some(CairnResource::NodeMessages {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
        })
    );
    assert_eq!(
        node.unwrap().to_uri(),
        "cairn://p/CAIRN/42/2/builder/messages"
    );

    // Task `/messages` is the sub-agent analogue.
    let task = parse_uri("cairn://p/cairn/42/2/builder/task/review/messages");
    assert_eq!(
        task,
        Some(CairnResource::TaskMessages {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "review".to_string(),
        })
    );
    assert_eq!(
        task.unwrap().to_uri(),
        "cairn://p/CAIRN/42/2/builder/task/review/messages"
    );

    // `/messages` is not mistaken for a type-named artifact.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/messages").map(|r| r.kind()),
        Some(ResourceKind::NodeMessages)
    );
}

#[test]
fn parses_and_roundtrips_settings_family() {
    assert_eq!(parse_uri("cairn://settings"), Some(CairnResource::Settings));
    assert_eq!(CairnResource::Settings.to_uri(), "cairn://settings");
    assert_eq!(CairnResource::Settings.kind(), ResourceKind::Settings);
    assert_eq!(CairnResource::Settings.project(), None);
    assert_eq!(CairnResource::Settings.to_route(), None);

    assert_eq!(parse_uri("cairn://projects"), Some(CairnResource::Projects));
    assert_eq!(CairnResource::Projects.to_uri(), "cairn://projects");
    assert_eq!(CairnResource::Projects.kind(), ResourceKind::Projects);

    let ps = parse_uri("cairn://p/cairn/settings");
    assert_eq!(
        ps,
        Some(CairnResource::ProjectSettings {
            project: "CAIRN".to_string(),
        })
    );
    let ps = ps.unwrap();
    assert_eq!(ps.to_uri(), "cairn://p/CAIRN/settings");
    assert_eq!(ps.kind(), ResourceKind::ProjectSettings);
    assert_eq!(ps.project(), Some("CAIRN"));
    assert_eq!(ps.issue_number(), None);
    assert_eq!(ps.to_route(), None);

    // `settings` is not parsed as an issue number.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/settings").map(|r| r.kind()),
        Some(ResourceKind::ProjectSettings)
    );
}

#[test]
fn parses_and_roundtrips_websearch() {
    assert_eq!(
        parse_uri("cairn://websearch"),
        Some(CairnResource::WebSearch)
    );
    assert_eq!(CairnResource::WebSearch.to_uri(), "cairn://websearch");
    assert_eq!(CairnResource::WebSearch.kind(), ResourceKind::WebSearch);
    assert_eq!(CairnResource::WebSearch.project(), None);
    assert_eq!(CairnResource::WebSearch.issue_number(), None);
    // The query rides in ?q=; parse_uri ignores the query like every resource.
    assert_eq!(
        parse_uri("cairn://websearch?q=rust async"),
        Some(CairnResource::WebSearch)
    );
}

#[test]
fn parses_and_roundtrips_help() {
    assert_eq!(parse_uri("cairn://help"), Some(CairnResource::Help));
    assert_eq!(CairnResource::Help.to_uri(), "cairn://help");
    assert_eq!(CairnResource::Help.kind(), ResourceKind::Help);
    assert_eq!(CairnResource::Help.project(), None);
}

#[test]
fn parses_and_roundtrips_logs() {
    assert_eq!(parse_uri("cairn://logs"), Some(CairnResource::Logs));
    assert_eq!(CairnResource::Logs.to_uri(), "cairn://logs");
    assert_eq!(CairnResource::Logs.kind(), ResourceKind::Logs);
    assert_eq!(CairnResource::Logs.project(), None);
    // Read-only logical resource: not a UI deep-link.
    assert_eq!(CairnResource::Logs.to_route(), None);
    // Query strings do not affect resource identity.
    assert_eq!(
        parse_uri("cairn://logs?process=mcp&grep=ERROR"),
        Some(CairnResource::Logs)
    );
}

#[test]
fn parses_type_named_node_artifact() {
    // A trailing non-reserved segment is a type-named artifact.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/plan"),
        Some(CairnResource::NodeArtifact {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            name: Some("plan".to_string()),
        })
    );
    // Round-trips back to the same type-named URI.
    assert_eq!(
        CairnResource::NodeArtifact {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            name: Some("plan".to_string()),
        }
        .to_uri(),
        "cairn://p/CAIRN/42/2/builder/plan"
    );
    // The literal `artifact` keyword is the generic (name: None) alias.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/artifact"),
        Some(CairnResource::NodeArtifact {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            name: None,
        })
    );
}

#[test]
fn reserved_segments_are_not_artifacts() {
    // `chat` is reserved and must parse as NodeChat, never an artifact named "chat".
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/chat"),
        Some(CairnResource::NodeChat {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
        })
    );
    assert!(is_reserved_node_segment("chat"));
    assert!(is_reserved_node_segment("todos"));
    assert!(!is_reserved_node_segment("plan"));
    assert!(!is_reserved_node_segment("pr"));
}

#[test]
fn parses_type_named_task_artifact() {
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/result"),
        Some(CairnResource::TaskArtifact {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            name: Some("result".to_string()),
        })
    );
}

#[test]
fn parses_and_roundtrips_task_base() {
    // The task job base — the analogue of a node base. Regression: this used
    // to fall through to `None`, so a sub-agent's home URI was rejected and
    // `cairn:~/...` shorthand could not resolve.
    let uri = "cairn://p/CAIRN/1174/1/planner/task/cairn-1171";
    let parsed = parse_uri(uri);
    assert_eq!(
        parsed,
        Some(CairnResource::Task {
            project: "CAIRN".to_string(),
            number: 1174,
            exec_seq: 1,
            node_id: "planner".to_string(),
            task_name: "cairn-1171".to_string(),
        })
    );
    // Round-trips back to the same string (distinct from the artifact form).
    assert_eq!(parsed.unwrap().to_uri(), uri);
}

#[test]
fn task_base_built_by_build_job_base_uri_parses() {
    // The exact path the orchestrator uses to stamp CAIRN_HOME_URI for a task.
    let built = build_job_base_uri("CAIRN", 1174, 1, "cairn-1171", Some("planner"));
    assert!(
        parse_uri(&built).is_some(),
        "task home URI must parse: {built}"
    );
}

#[test]
fn chat_full_uri_no_longer_parses() {
    // The `full` segment was renamed to `raw`; the old spelling must 404 so
    // the removal is deliberate rather than a silent dual-name.
    assert!(parse_uri("cairn://p/CAIRN/42/2/builder/chat/full").is_none());
    assert!(parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/chat/full").is_none());
}

#[test]
fn parses_and_roundtrips_browsers() {
    let node = CairnResource::NodeBrowser {
        project: "CAIRN".to_string(),
        number: 42,
        exec_seq: 2,
        node_id: "builder".to_string(),
        slug: "main".to_string(),
    };
    assert_eq!(node.to_uri(), "cairn://p/CAIRN/42/2/builder/browser/main");
    assert_eq!(parse_uri(&node.to_uri()), Some(node.clone()));
    assert_eq!(node.kind(), ResourceKind::NodeBrowser);
    assert_eq!(
        node.to_route(),
        Some("/p/cairn/i/42/2/builder?browserId=main".to_string())
    );

    let task = CairnResource::TaskBrowser {
        project: "CAIRN".to_string(),
        number: 42,
        exec_seq: 2,
        node_id: "builder".to_string(),
        task_name: "Explore".to_string(),
        slug: "main".to_string(),
    };
    assert_eq!(
        task.to_uri(),
        "cairn://p/CAIRN/42/2/builder/task/Explore/browser/main"
    );
    assert_eq!(parse_uri(&task.to_uri()), Some(task.clone()));
    assert_eq!(task.kind(), ResourceKind::TaskBrowser);

    let project = CairnResource::ProjectBrowser {
        project: "CAIRN".to_string(),
        slug: "main".to_string(),
    };
    assert_eq!(project.to_uri(), "cairn://p/CAIRN/browser/main");
    assert_eq!(parse_uri(&project.to_uri()), Some(project.clone()));
    assert_eq!(project.kind(), ResourceKind::ProjectBrowser);
    assert_eq!(
        project.to_route(),
        Some("/p/cairn/browser?browserId=main".to_string())
    );
}

#[test]
fn bare_browser_defaults_slug() {
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/browser"),
        Some(CairnResource::NodeBrowser {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            slug: "default".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/browser"),
        Some(CairnResource::TaskBrowser {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "default".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/browser"),
        Some(CairnResource::ProjectBrowser {
            project: "CAIRN".to_string(),
            slug: "default".to_string(),
        })
    );
}

#[test]
fn project_browser_arm_precedes_issue_arm() {
    // A numeric project-browser slug must resolve as ProjectBrowser, not as
    // an issue: the `[p, project, "browser", slug]` arm must precede the
    // `[p, project, number]` issue arm.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/browser/123"),
        Some(CairnResource::ProjectBrowser {
            project: "CAIRN".to_string(),
            slug: "123".to_string(),
        })
    );
    // And a bare numeric segment is still an issue.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/123"),
        Some(CairnResource::Issue {
            project: "CAIRN".to_string(),
            number: 123,
        })
    );
}

#[test]
fn parses_node_and_task_resources() {
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/chat/raw"),
        Some(CairnResource::NodeChatRaw {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/chat/turn/3"),
        Some(CairnResource::TaskChatTurn {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            turn_seq: 3,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/chat/1/0"),
        Some(CairnResource::NodeChatEvent {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            run_seq: 1,
            event_seq: 0,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/chat/1/0"),
        Some(CairnResource::TaskChatEvent {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            run_seq: 1,
            event_seq: 0,
        })
    );
}

#[test]
fn parses_and_roundtrips_node_tasks_and_questions() {
    let cases = [
        (
            "cairn://p/CAIRN/42/2/builder/tasks",
            CairnResource::NodeTasks {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
        ),
        (
            "cairn://p/CAIRN/42/2/builder/questions",
            CairnResource::NodeQuestions {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
        ),
        (
            "cairn://p/CAIRN/42/2/builder/questions/q-1",
            CairnResource::NodeQuestion {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                segment: "q-1".to_string(),
            },
        ),
        (
            "cairn://p/CAIRN/42/2/builder/permissions",
            CairnResource::NodePermissions {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
            },
        ),
        (
            "cairn://p/CAIRN/42/2/builder/permissions/perm-1",
            CairnResource::NodePermission {
                project: "CAIRN".to_string(),
                number: 42,
                exec_seq: 2,
                node_id: "builder".to_string(),
                segment: "perm-1".to_string(),
            },
        ),
    ];
    for (uri, expected) in cases {
        assert_eq!(parse_uri(uri), Some(expected.clone()));
        assert_eq!(expected.to_uri(), uri);
    }
}

#[test]
fn parse_uri_keeps_path_only_compatibility_with_queries() {
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/terminal/dev?full=true"),
        Some(CairnResource::NodeTerminal {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            slug: "dev".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci?new=true"),
        Some(CairnResource::TaskTerminal {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "ci".to_string(),
        })
    );
}

#[test]
fn parses_and_roundtrips_task_terminal() {
    let uri = "cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci";
    let parsed = parse_uri(uri);
    assert_eq!(
        parsed,
        Some(CairnResource::TaskTerminal {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "ci".to_string(),
        })
    );
    let resource = parsed.unwrap();
    assert_eq!(resource.to_uri(), uri);
    assert_eq!(resource.kind(), ResourceKind::TaskTerminal);
    assert_eq!(resource.project(), Some("CAIRN"));
    assert_eq!(
        resource.to_route(),
        Some("/p/cairn/i/42/2/builder/task/Explore?terminalId=ci".to_string())
    );
}

#[test]
fn parse_resource_uri_preserves_ordered_query_params() {
    let parsed = parse_resource_uri("cairn://p/cairn/issues?limit=10&status=backlog").unwrap();
    assert_eq!(
        parsed,
        Some(CairnResourceUri {
            resource: CairnResource::ProjectIssues {
                project: "CAIRN".to_string(),
            },
            params: vec![
                QueryParam {
                    key: "limit".to_string(),
                    value: "10".to_string(),
                },
                QueryParam {
                    key: "status".to_string(),
                    value: "backlog".to_string(),
                },
            ],
        })
    );
    assert_eq!(
        parsed.unwrap().to_uri(),
        "cairn://p/CAIRN/issues?limit=10&status=backlog"
    );
}

#[test]
fn parse_resource_uri_encodes_canonical_query_params() {
    // `+` is literal in a value (not form-decoded to a space), so it
    // canonicalizes to `%2B`; a space encodes as `%20`. `&status=` still
    // splits because `status` is a recognized key.
    let parsed =
        parse_resource_uri("cairn://p/cairn/issues?label=needs+review&status=backlog%2Cactive")
            .unwrap()
            .unwrap();
    assert_eq!(
        parsed.to_uri(),
        "cairn://p/CAIRN/issues?label=needs%2Breview&status=backlog%2Cactive"
    );
}

#[test]
fn parse_resource_uri_rejects_invalid_query_encoding() {
    let err = parse_resource_uri("cairn://p/CAIRN/issues?status=%ZZ").unwrap_err();
    assert!(err.contains("Invalid percent escape"));
}

#[test]
fn rejects_legacy_roots_and_invalid_paths() {
    assert!(parse_uri("cairn://CAIRN/42").is_none());
    assert!(parse_uri("cairn://ws/skills").is_none());
    assert!(parse_uri("cairn://p").is_none());
    assert!(parse_uri("cairn://p/CAIRN/comments").is_none());
    assert!(parse_uri("cairn://p/CAIRN/42/pr").is_none());
    // Note: a trailing non-reserved segment like `.../builder/diff` is now a
    // valid type-named artifact (see parses_type_named_node_artifact), not an error.
    assert!(parse_uri("cairn://p/CAIRN/42/0/builder").is_none());
    assert!(parse_uri("cairn://").is_none());
}

#[test]
fn serializes_canonical_uris() {
    assert_eq!(build_project_uri("cairn"), "cairn://p/CAIRN");
    assert_eq!(build_project_issues_uri("cairn"), "cairn://p/CAIRN/issues");
    assert_eq!(build_issue_uri("cairn", 42), "cairn://p/CAIRN/42");
    assert_eq!(
        build_node_terminal_uri("cairn", 42, 2, "builder", "dev"),
        "cairn://p/CAIRN/42/2/builder/terminal/dev"
    );
    assert_eq!(
        build_task_terminal_uri("cairn", 42, 2, "builder", "Explore", "ci"),
        "cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci"
    );
    assert_eq!(
        build_task_artifact_uri("cairn", 42, 2, "builder", "Explore"),
        "cairn://p/CAIRN/42/2/builder/task/Explore/artifact"
    );
}

#[test]
fn parses_issue_executions_collection() {
    assert_eq!(
        parse_uri("cairn://p/cairn/42/executions"),
        Some(CairnResource::IssueExecutions {
            project: "CAIRN".to_string(),
            number: 42,
        })
    );
    assert_eq!(
        build_issue_executions_uri("CAIRN", 42),
        "cairn://p/CAIRN/42/executions"
    );
}

#[test]
fn parses_issue_comments_collection_and_member() {
    assert_eq!(
        parse_uri("cairn://p/cairn/12/comments"),
        Some(CairnResource::IssueComments {
            project: "CAIRN".to_string(),
            number: 12,
        })
    );
    assert_eq!(
        build_issue_comments_uri("CAIRN", 12),
        "cairn://p/CAIRN/12/comments"
    );

    let member = parse_uri("cairn://p/cairn/12/comments/3").unwrap();
    assert_eq!(
        member,
        CairnResource::IssueComment {
            project: "CAIRN".to_string(),
            number: 12,
            comment_seq: 3,
        }
    );
    assert_eq!(member.kind(), ResourceKind::IssueComment);
    assert_eq!(member.project(), Some("CAIRN"));
    assert_eq!(member.issue_number(), Some(12));
    assert_eq!(member.to_uri(), "cairn://p/CAIRN/12/comments/3");
    // A non-numeric comment tail is not a valid member URI.
    assert_eq!(parse_uri("cairn://p/CAIRN/12/comments/not-a-number"), None);

    let collection = parse_uri("cairn://p/CAIRN/12/comments").unwrap();
    assert_eq!(collection.kind(), ResourceKind::IssueComments);
    assert_eq!(collection.issue_number(), Some(12));
}

#[test]
fn parses_single_execution_snapshot() {
    let resource = parse_uri("cairn://p/cairn/42/executions/2").unwrap();
    assert_eq!(
        resource,
        CairnResource::IssueExecution {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
        }
    );
    assert_eq!(resource.kind(), ResourceKind::IssueExecution);
    assert_eq!(resource.project(), Some("CAIRN"));
    assert_eq!(resource.issue_number(), Some(42));
    assert_eq!(resource.to_route(), None);
    assert_eq!(
        build_issue_execution_uri("CAIRN", 42, 2),
        "cairn://p/CAIRN/42/executions/2"
    );
    assert_eq!(resource.to_uri(), "cairn://p/CAIRN/42/executions/2");
}

/// `.../42/executions/2` and the node shape `.../42/2/builder` are both
/// 5-segment URIs; the literal `executions` in the 4th slot must resolve to
/// a single execution, never a node whose exec_seq parsed there.
#[test]
fn single_execution_does_not_shadow_node() {
    assert_eq!(
        parse_uri("cairn://p/cairn/42/executions/2"),
        Some(CairnResource::IssueExecution {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/42/2/builder"),
        Some(CairnResource::Node {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
        })
    );
    // A non-numeric exec_seq under `executions` is malformed, not a node.
    assert_eq!(parse_uri("cairn://p/cairn/42/executions/abc"), None);
}

#[test]
fn round_trips_every_resource_family() {
    let resources = vec![
        CairnResource::Project {
            project: "CAIRN".to_string(),
        },
        CairnResource::ProjectIssues {
            project: "CAIRN".to_string(),
        },
        CairnResource::Issue {
            project: "CAIRN".to_string(),
            number: 1,
        },
        CairnResource::ProjectMessages {
            project: "CAIRN".to_string(),
        },
        CairnResource::ProjectTerminal {
            project: "CAIRN".to_string(),
            slug: "build".to_string(),
        },
        CairnResource::IssueMessages {
            project: "CAIRN".to_string(),
            number: 1,
        },
        CairnResource::Changed {
            project: "CAIRN".to_string(),
            number: 1,
        },
        CairnResource::IssueExecutions {
            project: "CAIRN".to_string(),
            number: 1,
        },
        CairnResource::IssueComments {
            project: "CAIRN".to_string(),
            number: 1,
        },
        CairnResource::IssueComment {
            project: "CAIRN".to_string(),
            number: 1,
            comment_seq: 1,
        },
        CairnResource::IssueExecution {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
        },
        CairnResource::Node {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
        },
        CairnResource::NodeChat {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
        },
        CairnResource::NodeChatRaw {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
        },
        CairnResource::NodeChatTurn {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            turn_seq: 0,
        },
        CairnResource::NodeChatEvent {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            run_seq: 1,
            event_seq: 5,
        },
        CairnResource::NodeArtifact {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            name: None,
        },
        CairnResource::NodeDiff {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
        },
        CairnResource::NodeTerminal {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            slug: "dev".to_string(),
        },
        CairnResource::TaskTerminal {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            slug: "ci".to_string(),
        },
        CairnResource::TaskChat {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
        },
        CairnResource::TaskChatRaw {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
        },
        CairnResource::TaskChatTurn {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            turn_seq: 2,
        },
        CairnResource::TaskChatEvent {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            run_seq: 1,
            event_seq: 3,
        },
        CairnResource::TaskArtifact {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            name: None,
        },
        CairnResource::JobTodos {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: None,
        },
        CairnResource::JobTodos {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: Some("Explore".to_string()),
        },
        CairnResource::TaskPermissions {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
        },
        CairnResource::TaskPermission {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: "Explore".to_string(),
            segment: "perm-1".to_string(),
        },
        CairnResource::Bug,
    ];

    for resource in resources {
        assert_eq!(parse_uri(&resource.to_uri()), Some(resource.clone()));
    }
}

#[test]
fn parses_job_todos_node_and_task_forms() {
    assert_eq!(
        parse_uri("cairn://p/cairn/42/2/builder/todos"),
        Some(CairnResource::JobTodos {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: None,
        })
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/task/Explore/todos"),
        Some(CairnResource::JobTodos {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: Some("Explore".to_string()),
        })
    );
    assert_eq!(
        build_job_todos_uri("cairn", 42, 2, "builder", None),
        "cairn://p/CAIRN/42/2/builder/todos"
    );
    assert_eq!(
        build_job_todos_uri("cairn", 42, 2, "builder", Some("Explore")),
        "cairn://p/CAIRN/42/2/builder/task/Explore/todos"
    );
}

#[test]
fn job_todos_uri_keeps_path_only_compatibility_with_queries() {
    // parse_uri strips the query; query rejection is enforced at the handler.
    assert_eq!(
        parse_uri("cairn://p/CAIRN/42/2/builder/todos?limit=3"),
        Some(CairnResource::JobTodos {
            project: "CAIRN".to_string(),
            number: 42,
            exec_seq: 2,
            node_id: "builder".to_string(),
            task_name: None,
        })
    );
}

#[test]
fn parses_bug_resource() {
    assert_eq!(parse_uri("cairn://bug"), Some(CairnResource::Bug));
    assert_eq!(build_bug_uri(), "cairn://bug");
    assert_eq!(CairnResource::Bug.project(), None);
    assert_eq!(CairnResource::Bug.to_route(), None);
}

#[test]
fn parses_and_round_trips_dev_resources() {
    // The collection entrypoint and its two sub-tools parse and round-trip.
    assert_eq!(parse_uri("cairn://dev"), Some(CairnResource::Dev));
    assert_eq!(parse_uri("cairn://dev/db"), Some(CairnResource::DevDb));
    assert_eq!(parse_uri("cairn://dev/pid"), Some(CairnResource::DevPid));
    assert_eq!(CairnResource::Dev.to_uri(), "cairn://dev");
    assert_eq!(CairnResource::DevDb.to_uri(), "cairn://dev/db");
    assert_eq!(CairnResource::DevPid.to_uri(), "cairn://dev/pid");
    // The flat legacy name is replaced outright, not aliased.
    assert_eq!(parse_uri("cairn://dev-db"), None);
    // An unknown sub-tool does not parse.
    assert_eq!(parse_uri("cairn://dev/bogus"), None);
    // None of them carry project or navigation scope.
    for resource in [
        CairnResource::Dev,
        CairnResource::DevDb,
        CairnResource::DevPid,
    ] {
        assert_eq!(resource.project(), None);
        assert_eq!(resource.to_route(), None);
    }
}

#[test]
fn parses_and_round_trips_mcp_resources() {
    // Top-level: list servers.
    assert_eq!(
        parse_uri("cairn://mcp"),
        Some(CairnResource::Mcp {
            server: None,
            resource: None,
        })
    );
    // Server scope: list tools/resources.
    assert_eq!(
        parse_uri("cairn://mcp/playwright"),
        Some(CairnResource::Mcp {
            server: Some("playwright".to_string()),
            resource: None,
        })
    );
    // Tool target (for run).
    assert_eq!(
        parse_uri("cairn://mcp/playwright/browser_navigate"),
        Some(CairnResource::Mcp {
            server: Some("playwright".to_string()),
            resource: Some("browser_navigate".to_string()),
        })
    );
    // External resource URI tail kept intact, including '/' and '://'.
    let r = parse_uri("cairn://mcp/linear/issue://ABC-1/sub").unwrap();
    assert_eq!(
        r,
        CairnResource::Mcp {
            server: Some("linear".to_string()),
            resource: Some("issue://ABC-1/sub".to_string()),
        }
    );
    assert_eq!(r.to_uri(), "cairn://mcp/linear/issue://ABC-1/sub");
    // The external resource tail may carry its own '?query', which must NOT
    // be consumed as Cairn-side query params.
    let q = parse_uri("cairn://mcp/linear/https://api.example.com/items?limit=10").unwrap();
    assert_eq!(
        q,
        CairnResource::Mcp {
            server: Some("linear".to_string()),
            resource: Some("https://api.example.com/items?limit=10".to_string()),
        }
    );
    assert_eq!(
        q.to_uri(),
        "cairn://mcp/linear/https://api.example.com/items?limit=10"
    );
    // Round-trip the simpler forms.
    for uri in [
        "cairn://mcp",
        "cairn://mcp/playwright",
        "cairn://mcp/playwright/browser_navigate",
    ] {
        assert_eq!(parse_uri(uri).unwrap().to_uri(), uri);
    }
    assert_eq!(parse_uri("cairn://mcp").unwrap().project(), None);
    assert_eq!(parse_uri("cairn://mcp").unwrap().kind(), ResourceKind::Mcp);
}

#[test]
fn parses_skill_resources() {
    assert_eq!(parse_uri("cairn://skills"), Some(CairnResource::Skills));
    assert_eq!(
        parse_uri("cairn://skills/ui"),
        Some(CairnResource::Skill {
            skill_id: "ui".to_string(),
            path: vec![],
        })
    );
    assert_eq!(
        parse_uri("cairn://skills/ui/SKILL.md"),
        Some(CairnResource::Skill {
            skill_id: "ui".to_string(),
            path: vec!["SKILL.md".to_string()],
        })
    );
    assert_eq!(
        parse_uri("cairn://skills/ui/references/a/b.md"),
        Some(CairnResource::Skill {
            skill_id: "ui".to_string(),
            path: vec![
                "references".to_string(),
                "a".to_string(),
                "b.md".to_string()
            ],
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/skills"),
        Some(CairnResource::ProjectSkills {
            project: "CAIRN".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/skills/ui/scripts/run.sh"),
        Some(CairnResource::ProjectSkill {
            project: "CAIRN".to_string(),
            skill_id: "ui".to_string(),
            path: vec!["scripts".to_string(), "run.sh".to_string()],
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/references"),
        Some(CairnResource::ProjectReferences {
            project: "CAIRN".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/references/openpnp"),
        Some(CairnResource::ProjectReference {
            project: "CAIRN".to_string(),
            name: "openpnp".to_string(),
        })
    );
}

#[test]
fn round_trips_skill_resources() {
    let resources = vec![
        CairnResource::Skills,
        CairnResource::Skill {
            skill_id: "ui".to_string(),
            path: vec![],
        },
        CairnResource::Skill {
            skill_id: "ui".to_string(),
            path: vec!["SKILL.md".to_string()],
        },
        CairnResource::Skill {
            skill_id: "ui".to_string(),
            path: vec![
                "references".to_string(),
                "a".to_string(),
                "b.md".to_string(),
            ],
        },
        CairnResource::ProjectSkills {
            project: "CAIRN".to_string(),
        },
        CairnResource::ProjectSkill {
            project: "CAIRN".to_string(),
            skill_id: "ui".to_string(),
            path: vec!["scripts".to_string(), "run.sh".to_string()],
        },
        CairnResource::ProjectReferences {
            project: "CAIRN".to_string(),
        },
        CairnResource::ProjectReference {
            project: "CAIRN".to_string(),
            name: "openpnp".to_string(),
        },
    ];
    for resource in resources {
        assert_eq!(parse_uri(&resource.to_uri()), Some(resource.clone()));
    }
}

#[test]
fn round_trips_workflow_resources() {
    let resources = vec![
        CairnResource::Workflows,
        CairnResource::Workflow {
            workflow_id: "deep-research".to_string(),
        },
        CairnResource::ProjectWorkflows {
            project: "CAIRN".to_string(),
        },
        CairnResource::ProjectWorkflow {
            project: "CAIRN".to_string(),
            workflow_id: "deep-research".to_string(),
        },
    ];
    for resource in resources {
        assert_eq!(parse_uri(&resource.to_uri()), Some(resource.clone()));
        assert!(crate::contract::contract_for(resource.kind()).is_some());
    }
}

#[test]
fn workflow_resources_project_and_route() {
    assert_eq!(CairnResource::Workflows.project(), None);
    assert_eq!(CairnResource::Workflows.to_route(), None);
    assert_eq!(
        CairnResource::ProjectWorkflows {
            project: "CAIRN".to_string(),
        }
        .project(),
        Some("CAIRN")
    );
    assert_eq!(
        CairnResource::ProjectWorkflow {
            project: "CAIRN".to_string(),
            workflow_id: "deep-research".to_string(),
        }
        .to_route(),
        None
    );
    // Identity-only: workflows carry no issue number.
    assert_eq!(CairnResource::Workflows.issue_number(), None);
}

#[test]
fn project_reference_resources_report_project_and_kind() {
    let collection = parse_uri("cairn://p/cairn/references").unwrap();
    assert_eq!(collection.project(), Some("CAIRN"));
    assert_eq!(collection.issue_number(), None);
    assert_eq!(collection.kind(), ResourceKind::ProjectReferences);

    let member = parse_uri("cairn://p/cairn/references/openpnp").unwrap();
    assert_eq!(member.project(), Some("CAIRN"));
    assert_eq!(member.issue_number(), None);
    assert_eq!(member.kind(), ResourceKind::ProjectReference);
}

#[test]
fn parses_only_node_memory_resources() {
    assert_eq!(parse_uri("cairn://memories"), None);
    assert_eq!(parse_uri("cairn://memories/abc-123"), None);
    assert_eq!(parse_uri("cairn://p/CAIRN/memories"), None);
    assert_eq!(parse_uri("cairn://p/CAIRN/memories/abc-123"), None);

    let resource = CairnResource::NodeMemory {
        project: "CAIRN".to_string(),
        number: 1498,
        exec_seq: 1,
        node_id: "builder".to_string(),
        memory_seq: 2,
    };
    let uri = "cairn://p/CAIRN/1498/1/builder/memories/2";
    assert_eq!(parse_uri(uri), Some(resource.clone()));
    assert_eq!(resource.to_uri(), uri);
    assert_eq!(resource.project(), Some("CAIRN"));
    assert_eq!(
        resource.to_route(),
        Some("/p/cairn/i/1498/1/builder/memories/2".to_string())
    );
}

#[test]
fn parses_and_round_trips_recipe_resources() {
    let cases = [
        ("cairn://recipes", CairnResource::Recipes),
        (
            "cairn://recipes/default-flow",
            CairnResource::Recipe {
                recipe_id: "default-flow".to_string(),
            },
        ),
        (
            "cairn://p/CAIRN/recipes",
            CairnResource::ProjectRecipes {
                project: "CAIRN".to_string(),
            },
        ),
        (
            "cairn://p/CAIRN/recipes/default-flow",
            CairnResource::ProjectRecipe {
                project: "CAIRN".to_string(),
                recipe_id: "default-flow".to_string(),
            },
        ),
    ];
    for (uri, expected) in cases {
        assert_eq!(parse_uri(uri), Some(expected.clone()));
        assert_eq!(expected.to_uri(), uri);
        assert_eq!(expected.kind(), expected.clone().kind());
    }
    // Project canonicalization on parse.
    assert_eq!(
        parse_uri("cairn://p/cairn/recipes"),
        Some(CairnResource::ProjectRecipes {
            project: "CAIRN".to_string(),
        })
    );
    assert_eq!(
        parse_uri("cairn://p/cairn/recipes/default-flow"),
        Some(CairnResource::ProjectRecipe {
            project: "CAIRN".to_string(),
            recipe_id: "default-flow".to_string(),
        })
    );
    assert_eq!(CairnResource::Recipes.project(), None);
    assert_eq!(CairnResource::Recipes.issue_number(), None);
    assert_eq!(
        CairnResource::ProjectRecipe {
            project: "CAIRN".to_string(),
            recipe_id: "x".to_string(),
        }
        .project(),
        Some("CAIRN")
    );
    assert_eq!(CairnResource::Recipes.to_route(), None);
    assert_eq!(CairnResource::Recipes.kind(), ResourceKind::Recipes);
}

#[test]
fn skill_resources_have_no_project_or_route() {
    assert_eq!(CairnResource::Skills.project(), None);
    assert_eq!(CairnResource::Skills.to_route(), None);
    assert_eq!(
        CairnResource::ProjectSkills {
            project: "CAIRN".to_string(),
        }
        .project(),
        Some("CAIRN")
    );
    assert_eq!(
        CairnResource::ProjectSkill {
            project: "CAIRN".to_string(),
            skill_id: "ui".to_string(),
            path: vec![],
        }
        .to_route(),
        None
    );
}

#[test]
fn resource_contracts_include_project_issue_collection() {
    use crate::contract::RESOURCE_CONTRACTS;
    assert!(RESOURCE_CONTRACTS
        .iter()
        .any(|contract| contract.uri_template == "cairn://p/{project}/issues"));
}

#[test]
fn every_resource_kind_round_trips_through_kind() {
    use crate::contract::ResourceKind;
    // kind() must agree with the table: every kind a resource reports has a contract.
    for resource in [
        CairnResource::Project {
            project: "CAIRN".to_string(),
        },
        CairnResource::Bug,
        CairnResource::Skills,
    ] {
        assert!(crate::contract::contract_for(resource.kind()).is_some());
    }
    assert_eq!(
        CairnResource::Issue {
            project: "CAIRN".to_string(),
            number: 1,
        }
        .kind(),
        ResourceKind::Issue
    );
}

#[test]
fn routes_only_navigate_supported_resources() {
    assert_eq!(
        CairnResource::Project {
            project: "CAIRN".to_string(),
        }
        .to_route(),
        Some("/p/cairn/issues".to_string())
    );
    assert_eq!(
        CairnResource::ProjectIssues {
            project: "CAIRN".to_string(),
        }
        .to_route(),
        None
    );
    assert_eq!(
        CairnResource::ProjectTerminal {
            project: "CAIRN".to_string(),
            slug: "build".to_string(),
        }
        .to_route(),
        Some("/p/cairn/terminal?terminalId=build".to_string())
    );
    assert_eq!(
        CairnResource::NodeChatRaw {
            project: "CAIRN".to_string(),
            number: 1,
            exec_seq: 2,
            node_id: "builder".to_string(),
        }
        .to_route(),
        None
    );
}

#[test]
fn browser_network_request_uris_round_trip_in_all_scopes() {
    let cases = [
        "cairn://p/CAIRN/browser/default/network/realm-1",
        "cairn://p/CAIRN/42/2/builder/browser/dev/network/realm_2.3~x",
        "cairn://p/CAIRN/42/2/builder/task/review/browser/default/network/realm-4",
    ];
    for uri in cases {
        let resource = parse_uri(uri).expect(uri);
        assert_eq!(resource.to_uri(), uri);
        assert!(matches!(
            resource.kind(),
            ResourceKind::ProjectBrowserNetworkRequest
                | ResourceKind::NodeBrowserNetworkRequest
                | ResourceKind::TaskBrowserNetworkRequest
        ));
    }
    assert_eq!(parse_uri("cairn://p/CAIRN/browser/default/network/"), None);
    assert_eq!(
        parse_uri("cairn://p/CAIRN/browser/default/network/not%2Fsafe"),
        None
    );
    assert_eq!(
        parse_uri("cairn://p/CAIRN/browser/default/network/has space"),
        None
    );
}
