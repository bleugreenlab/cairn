use super::*;

const NODE: &str = "cairn://p/proj/2/1/builder";
const TURN: &str = "cairn://p/proj/2/1/builder/chat/turn/1";

fn event_data(event_type: &str, content: &str) -> String {
    serde_json::json!({"eventType": event_type, "isError": false, "content": content}).to_string()
}

/// Issue `proj-2` ("Child"), execution 1 with its `builder` job and one
/// session, a thread `execution`, and one stored turn per `turns` entry, each a
/// list of `(event_type, content)`.
async fn fixture(name: &str, turns: &[&[(&str, &str)]]) -> Orchestrator {
    let db = crate::storage::migrated_test_db(name).await;
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','proj','/tmp',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,description,created_at,updated_at)
           VALUES('issue-1','p',2,'Child','active','active','none','## Why

Because the lease drifts. And more.',1,50);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('exec-1','r','issue-1','p','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,uri_segment,status,created_at,updated_at)
           VALUES('child-job','p','issue-1','exec-1','builder','running',1,60);
         INSERT INTO sessions(id,job_id,status,created_at,updated_at) VALUES('sess','child-job','active',1,1);
         INSERT INTO runs(id,project_id,job_id,issue_id,session_id,created_at,updated_at) VALUES('run-1','p','child-job','issue-1','sess',1,1);
         INSERT INTO comments(id,issue_id,content,source,created_at,seq) VALUES('c-1','issue-1','Looks right to me.','agent',70,1);
         INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('thread-1','p','execution',1,80);
         UPDATE threads SET jurisdiction='Where work physically runs.' WHERE id='thread-1';",
    )
    .await
    .unwrap();
    for (index, events) in turns.iter().enumerate() {
        let sequence = index as i64 + 1;
        let turn_id = format!("turn-{sequence}");
        let at = 100 * sequence;
        db.execute(
            "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,created_at,updated_at)
             VALUES(?1,'sess','run-1','child-job',?2,'completed',?3,?3)",
            params![turn_id.as_str(), sequence, at],
        )
        .await
        .unwrap();
        for (offset, (event_type, content)) in events.iter().enumerate() {
            db.execute(
                "INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,turn_id,created_at)
                 VALUES(?1,'run-1','sess',?2,?3,?4,?5,?6,?3)",
                params![
                    format!("{turn_id}-{offset}"),
                    sequence * 100 + offset as i64,
                    at + offset as i64,
                    *event_type,
                    event_data(event_type, content),
                    turn_id.as_str()
                ],
            )
            .await
            .unwrap();
        }
    }
    test_orchestrator(db)
}

fn quotation(entry: &ReferenceEntry) -> (&str, &str, Option<i64>, &str, bool) {
    match entry {
        ReferenceEntry::Quotation {
            resource,
            author,
            at,
            text,
            elided,
            ..
        } => (resource, author, *at, text, *elided),
        other => panic!("expected a quotation, got {other:?}"),
    }
}

#[tokio::test]
async fn a_chat_turn_quotes_the_operator_without_the_resume_machinery() {
    let orch = fixture(
        "references-turn.db",
        &[&[
            (
                "user",
                "[Fri 09:12 PST — resumed]\n\n<system-reminder>noise</system-reminder>\nShip the lease change.\n\nThen look at the panel.",
            ),
            ("assistant", "On it."),
        ]],
    )
    .await;
    let entry = resolve_reference(&orch, TURN).await.unwrap();
    assert_eq!(
        quotation(&entry),
        (
            "node-chat-turn",
            "the operator",
            Some(100),
            "Ship the lease change.\n\nThen look at the panel.",
            false
        )
    );
}

#[tokio::test]
async fn a_delivered_message_is_attributed_to_its_sender_and_a_launch_to_the_agent() {
    let orch = fixture(
        "references-turn-authors.db",
        &[
            &[(
                "user",
                "[Direct message from cairn://p/proj/execution] Take the lane.\nTo reply, use the message tool with to: \"cairn://p/proj/execution/messages\"",
            )],
            &[
                ("user:launch", "Composed from the issue, typed by nobody."),
                ("assistant", "Reading the issue first."),
            ],
        ],
    )
    .await;
    let first = resolve_reference(&orch, TURN).await.unwrap();
    let (_, author, _, text, _) = quotation(&first);
    assert_eq!(
        (author, text),
        ("cairn://p/proj/execution", "Take the lane.")
    );

    let second = resolve_reference(&orch, &format!("{NODE}/chat/turn/2"))
        .await
        .unwrap();
    let (_, author, _, text, _) = quotation(&second);
    assert_eq!((author, text), (NODE, "Reading the issue first."));
}

#[tokio::test]
async fn a_long_turn_is_elided_and_its_own_references_are_not_expanded() {
    let long = format!(
        "{} see cairn://p/proj/2\n\n## References\n\n- [issue](cairn://p/proj/2): pasted",
        "word ".repeat(200)
    );
    let orch = fixture("references-turn-long.db", &[&[("user", long.as_str())]]).await;
    let text = format!("Quoting {TURN} here.");
    let references = resolve_references(&orch, &text).await;
    assert_eq!(references.entries.len(), 1, "{references:?}");
    let (_, _, _, quoted, elided) = quotation(&references.entries[0]);
    assert!(elided);
    assert!(!quoted.contains("## References"), "{quoted}");
    let block = references_block(&orch, &text).await.unwrap();
    assert!(
        block.contains(&format!("(elided; read {TURN} for the rest)")),
        "{block}"
    );
    assert!(
        block.matches("\n- [").count() == 1 && !block.contains("(cairn://p/proj/2)"),
        "a URI inside a quotation is not itself expanded: {block}"
    );
}

#[tokio::test]
async fn previews_name_the_issue_node_thread_and_comment_in_one_line() {
    let orch = fixture("references-previews.db", &[]).await;
    let text = format!(
        "cairn://p/proj/2, {NODE}, cairn://p/proj/execution, cairn://p/proj/2/comments/1, \
         file:src/lib.rs:12 and cairn://p/proj/2 again."
    );
    let references = resolve_references(&orch, &text).await;
    assert_eq!(
        references.entries,
        vec![
            ReferenceEntry::Preview {
                uri: "cairn://p/proj/2".into(),
                resource: "issue".into(),
                title: "Child".into(),
                status: Some("active".into()),
                line: Some("Because the lease drifts. And more.".into()),
                at: Some(50),
            },
            ReferenceEntry::Preview {
                uri: NODE.into(),
                resource: "node".into(),
                title: "Child".into(),
                status: Some("running".into()),
                line: None,
                at: Some(60),
            },
            ReferenceEntry::Preview {
                uri: "cairn://p/proj/execution".into(),
                resource: "thread".into(),
                title: "execution".into(),
                status: Some("active".into()),
                line: Some("Where work physically runs.".into()),
                at: Some(80),
            },
            ReferenceEntry::Quotation {
                uri: "cairn://p/proj/2/comments/1".into(),
                resource: "issue-comment".into(),
                author: "an agent".into(),
                at: Some(70),
                text: "Looks right to me.".into(),
                elided: false,
            },
            ReferenceEntry::File {
                uri: "file:src/lib.rs:12".into(),
                path: "src/lib.rs".into(),
                lines: Some("line 12".into()),
            },
        ]
    );
    assert_eq!(references.more, 0);
}

/// A check observation stores `ran_at` in epoch milliseconds, the way the
/// production writer stamps it, while a preview's `at` is epoch seconds. An
/// observation written through the cache's own path must preview at the time it
/// ran, not some fifty-eight thousand years later.
#[tokio::test]
async fn a_check_observation_previews_at_the_second_it_ran() {
    let orch = fixture("references-check.db", &[]).await;
    let mut observation =
        crate::execution::cache::tests::observation("obs-preview", "commit-1", "env-a");
    observation.project_id = "p".into();
    observation.ran_at = chrono::Utc::now().timestamp_millis();
    let ran_at_seconds = observation.ran_at / 1_000;
    let uri = format!(
        "cairn://p/proj/check-observations/{}",
        observation.public_handle
    );
    crate::execution::cache::record_fresh_check_observation(orch.db.local.clone(), observation)
        .unwrap();

    let Some(ReferenceEntry::Preview { title, at, .. }) = resolve_reference(&orch, &uri).await
    else {
        panic!("{uri} resolves to a preview");
    };
    assert_eq!(title, "rust");
    let at = at.expect("a check observation states when it ran");
    assert!(
        (at - ran_at_seconds).abs() <= 2,
        "previewed at {at}, but the observation ran at {ran_at_seconds} (epoch seconds)"
    );
}

#[tokio::test]
async fn an_unresolvable_uri_gets_no_entry_and_the_cap_counts_the_rest() {
    let orch = fixture("references-caps.db", &[]).await;
    assert!(
        resolve_reference(&orch, "cairn://p/proj/99")
            .await
            .is_none(),
        "a missing issue resolves to nothing"
    );
    assert!(resolve_reference(&orch, "cairn://p/proj/2/1/nobody")
        .await
        .is_none());
    assert!(references_block(&orch, "cairn://p/proj/99 only")
        .await
        .is_none());

    let files: Vec<String> = (1..=11).map(|n| format!("file:src/f{n}.rs")).collect();
    let text = format!("cairn://p/proj/99 {}", files.join(" "));
    let references = resolve_references(&orch, &text).await;
    assert_eq!(references.entries.len(), MAX_REFERENCE_ENTRIES);
    assert_eq!(references.entries[0].uri(), "file:src/f1.rs");
    assert_eq!(references.more, 11 - MAX_REFERENCE_ENTRIES);
    let block = references_block(&orch, &text).await.unwrap();
    assert!(
        block.ends_with("- and 3 more references not shown"),
        "{block}"
    );
}

fn push(key: &str, content_ref: &str) -> crate::orchestrator::attention_push::Push {
    crate::orchestrator::attention_push::Push {
        id: "push".into(),
        recipient: "watcher".into(),
        content_ref: content_ref.into(),
        wake: crate::orchestrator::attention_push::Wake::Passive,
        boundary: crate::orchestrator::attention_push::Boundary::Event,
        key: key.into(),
        created_at: 1,
        delivered_event_id: None,
    }
}

async fn artifact(orch: &Orchestrator, name: &str, version: i64, data: &str, at: i64) {
    orch.db
        .local
        .execute(
            "INSERT INTO artifacts(id,job_id,artifact_type,data,version,output_name,created_at,updated_at)
             VALUES(?1,'child-job',?2,?3,?4,?2,?5,?5)",
            params![format!("{name}-v{version}"), name, data, version, at],
        )
        .await
        .unwrap();
}

/// A review push's line is the resolver's artifact preview: the newest version
/// of the named artifact, the `/artifact` alias read as the PR rather than the
/// newest artifact of any name, and never the body.
#[tokio::test]
async fn a_review_push_names_the_artifact_title_and_summary_not_its_body() {
    use crate::orchestrator::attention_delivery::render_push_resolved;
    let orch = fixture("references-review.db", &[]).await;
    artifact(
        &orch,
        "create-pr",
        1,
        r#"{"title":"Rebase onto main (draft)","body":"BODY sentinel"}"#,
        10,
    )
    .await;
    artifact(
        &orch,
        "create-pr",
        2,
        r#"{"title":"Rebase onto main","body":"BODY sentinel"}"#,
        20,
    )
    .await;
    artifact(
        &orch,
        "plan",
        1,
        r#"{"title":"Plan the rebase","summary":"Reorders the tests,\nthen rebases.","content":"BODY sentinel"}"#,
        30,
    )
    .await;
    let header = |uri: &str| format!("Attention update (passive): {uri}");

    let plan = format!("{NODE}/plan");
    assert_eq!(
        render_push_resolved(&orch, &push(&format!("review:{plan}"), &plan)).await,
        format!(
            "{}\n\nWork product ready for review: Plan the rebase\n\
             Reorders the tests, then rebases.\n\
             Read {plan} for the current content.",
            header(&plan)
        )
    );
    for uri in [format!("{NODE}/create-pr"), format!("{NODE}/artifact")] {
        let rendered = render_push_resolved(&orch, &push(&format!("review:{uri}"), &uri)).await;
        assert_eq!(
            rendered,
            format!(
                "{}\n\nWork product ready for review: Rebase onto main\n\
                 Read {uri} for the current content.",
                header(&uri)
            )
        );
    }
}

/// The acceptance shape: a direct message that cites a chat turn reaches its
/// recipient as the header, the content, and the reply hint, then the block
/// quoting the turn with its author and time, with nothing pasted by the sender.
#[tokio::test]
async fn a_direct_message_citing_a_turn_delivers_the_quoted_words_after_the_reply_hint() {
    use crate::orchestrator::attention_delivery::render_push_resolved;
    let orch = fixture(
        "references-direct.db",
        &[&[(
            "user",
            "[Fri 09:12 PST — resumed]\n\nHold the lease change until Monday.",
        )]],
    )
    .await;
    orch.db
        .local
        .execute(
            "INSERT INTO messages(id,channel_type,channel_id,sender_run_id,sender_name,recipient_run_id,content,created_at)
             VALUES('m-1','direct',NULL,NULL,'cairn://p/proj/execution','run-1',?1,5)",
            params![format!("The operator ruled in {TURN}; plan around it.")],
        )
        .await
        .unwrap();

    let rendered = render_push_resolved(&orch, &push("direct:m-1", NODE)).await;

    let reply = "To reply, use the message tool with to: \"cairn://p/proj/execution/messages\"";
    let stamp = crate::clock::stamp(100).unwrap();
    let expected_tail = format!(
        "[Direct message from cairn://p/proj/execution] The operator ruled in {TURN}; plan around it.\n\
         {reply}\n\n\
         ## References\n\n\
         - [Transcript turn]({TURN}): quoted from the operator, {stamp}\n\
         \x20 > Hold the lease change until Monday."
    );
    assert!(rendered.ends_with(&expected_tail), "{rendered}");
}
