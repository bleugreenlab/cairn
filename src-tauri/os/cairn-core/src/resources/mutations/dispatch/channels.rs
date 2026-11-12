use super::super::{build_failure, ResourceMutationResult};
use super::append_payload;
use crate::channels::{agent_message, provider_notify_fit};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use crate::resources::channels::{
    admit_reached, check_send_length, reach_conversation, set_purpose, ChannelReader,
};
use cairn_uri::CairnResource;

fn required_text<'a>(
    index: usize,
    item: &ChangeItem,
    payload: &'a serde_json::Value,
    key: &str,
) -> ResourceMutationResult<&'a str> {
    payload
        .get(key)
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("payload.{key} is required and must be a non-empty string"),
            )
        })
}

/// The channels resources' writes: an agent sending free text to a
/// conversation it reaches, the verified operator setting a provider's
/// purpose, and a label on an ambient routing.
pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    match (resource, item.mode) {
        (CairnResource::ChannelsConversation { provider, path }, ChangeMode::Append) => {
            send(orch, request, index, item, dry_run, provider, path)
                .await
                .map(Some)
        }
        (CairnResource::Channels, ChangeMode::Patch) => {
            let payload = item.payload.clone().unwrap_or(serde_json::Value::Null);
            set_purpose(orch, request, &payload, dry_run)
                .await
                .map(Some)
                .map_err(|error| build_failure(index, item, error))
        }
        (CairnResource::ChannelsAmbientFeedback, ChangeMode::Append) => {
            label(orch, request, index, item, dry_run).await.map(Some)
        }
        _ => Ok(None),
    }
}

/// A thread or the operator labelling an ambient routing. Refusals come in a
/// fixed order, each a sentence naming its reason: the payload's shape, then
/// the labeller (a chartered thread, never a task or node; or a run-less
/// operator write), then the event, then the re-route target.
async fn label(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
) -> ResourceMutationResult<String> {
    use crate::channels::ambient_feedback::{apply_label, labeller, LabelPayload};
    let payload = append_payload(index, item)?;
    let payload =
        LabelPayload::parse(payload).map_err(|error| build_failure(index, item, error))?;
    let labeller = labeller(orch, request)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    apply_label(orch, request, &labeller, &payload, dry_run)
        .await
        .map_err(|error| build_failure(index, item, error))
}

/// An agent sending free text to the conversation its target URI names.
///
/// Every refusal happens before the intent is ledgered, in a fixed order: the
/// payload shape, then that the sender is a live agent node (attribution comes
/// from the authenticated run, never the payload), then that the sender reaches
/// the conversation, then that it is sendable, then the provider's length
/// limit. A successful write reports the enqueue and never claims delivery:
/// the router sends it, and a failure lands on the `channel_outbound` row, which
/// the conversation's resource shows.
async fn send(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    provider: &str,
    path: &[String],
) -> ResourceMutationResult<String> {
    let payload = append_payload(index, item)?;
    let content = required_text(index, item, payload, "content")?;
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            build_failure(
                index,
                item,
                format!("Only a live agent node may send a channel message: {error}"),
            )
        })?;
    let reader = ChannelReader::from_home(&agent.home, &agent.job_id)
        .map_err(|error| build_failure(index, item, error))?;
    let reached = reach_conversation(orch, &reader, provider, path)
        .await
        .map_err(|error| build_failure(index, item, error))?;
    let admitted = admit_reached(&reached).map_err(|error| build_failure(index, item, error))?;
    let provider = admitted.address.provider().id();
    // Budget the message as it will be rendered: the sender's attribution line
    // shares some providers' limits, so the body alone is not the measure.
    let header = agent_message::context_header(&agent.home);
    let (overflow, limit) = provider_notify_fit(provider, &header, content).unwrap_or((0, None));
    check_send_length(provider, overflow, limit)
        .map_err(|error| build_failure(index, item, error))?;
    let label = &admitted.row.label;
    let uri = &reached.uri;
    let warning = admitted
        .row
        .last_error
        .as_deref()
        .map(|error| {
            format!(
                " Warning: {provider}'s most recent outbound delivery failed ({error}); this message may fail the same way."
            )
        })
        .unwrap_or_default();

    if dry_run {
        return Ok(format!(
            "Would send a message to {label} ({uri}) as {}.{warning}",
            agent.home
        ));
    }
    let id = agent_message::enqueue(
        &orch.db.local,
        &admitted.address,
        &agent.home,
        Some(&agent.job_id),
        content,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;
    Ok(format!(
        "Queued message {id} to {label} (pending); read {uri} to see it sent, or why it failed.{warning}"
    ))
}

#[cfg(test)]
mod tests {
    use super::super::dispatch_resource_change;
    use crate::db::DbState;
    use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    /// A project `prj` with threads `general` (whose session has a live run)
    /// and `other`, Discord enabled, and three bindings: `general`'s and
    /// `other`'s channels and the project's ambient channel.
    async fn orchestrator() -> Orchestrator {
        use crate::channels::bindings::{
            BindingKind, MESSAGE_CLASSES_ALL, MESSAGE_CLASS_AGENT_TEXT,
        };
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        local
            .execute_script(
                "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
                 INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES ('p','w','P','prj','/tmp/p',1,1);
                 INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-general', 'p', 'general', 'active', 1, 1),
                        ('t-other', 'p', 'other', 'active', 1, 1);",
            )
            .await
            .unwrap();
        let session = crate::threads::ensure_thread_session(&local, "t-general")
            .await
            .unwrap();
        local
            .execute_script(format!(
                "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                 VALUES ('r-live', 'p', '{session}', 'live', 1, 1);"
            ))
            .await
            .unwrap();
        for (conversation, target, kind, classes) in [
            (
                "discord:1/2",
                "cairn://p/prj/general",
                BindingKind::Structural,
                MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            ),
            (
                "discord:1/3",
                "cairn://p/prj/other",
                BindingKind::Structural,
                MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            ),
            (
                "discord:1/9",
                "cairn://p/prj",
                BindingKind::Ambient,
                MESSAGE_CLASS_AGENT_TEXT,
            ),
        ] {
            assert!(crate::channels::ledger::bind_target(
                &local,
                "discord",
                conversation,
                target,
                kind,
                classes,
                1,
                0
            )
            .await
            .unwrap());
        }
        let config_dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(
            crate::config::settings::get_settings_path(&config_dir),
            "channels:\n  discord:\n    enabled: true\n    guildId: '1'\n",
        )
        .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build()
    }

    fn request(run_id: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: run_id.map(str::to_string),
            tool: "change".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            origin: None,
        }
    }

    fn send(target: &str) -> ChangeItem {
        ChangeItem {
            target: target.into(),
            mode: ChangeMode::Append,
            payload: Some(serde_json::json!({ "content": "hello" })),
        }
    }

    async fn refusal(orch: &Orchestrator, run_id: Option<&str>, target: &str) -> String {
        let error = dispatch_resource_change(orch, &request(run_id), 0, &send(target), false)
            .await
            .unwrap_err()
            .error;
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
                .await
                .unwrap(),
            Some(0),
            "a refused send ledgers nothing: {error}"
        );
        error
    }

    #[tokio::test]
    async fn a_send_without_a_live_agent_run_is_refused_before_admission() {
        let orch = orchestrator().await;
        let own = "cairn://channels/discord/prj/general";
        for run in [None, Some("r-gone")] {
            let error = refusal(&orch, run, own).await;
            assert!(
                error.contains("Only a live agent node may send a channel message"),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn a_thread_may_not_send_to_another_threads_channel() {
        let orch = orchestrator().await;
        let error = refusal(&orch, Some("r-live"), "cairn://channels/discord/prj/other").await;
        assert!(
            error.contains("#other · prj · Discord")
                && error.contains("is not a conversation cairn://p/prj/general can reach"),
            "{error}"
        );
        assert!(!error.contains("discord:"), "{error}");
    }

    #[tokio::test]
    async fn a_send_to_an_unbound_conversation_names_what_it_should_look_like() {
        let orch = orchestrator().await;
        let error = refusal(&orch, Some("r-live"), "cairn://channels/discord/prj/nope").await;
        assert!(
            error.contains("No Discord channel is bound to cairn://p/prj/nope"),
            "{error}"
        );
        let error = refusal(&orch, Some("r-live"), "cairn://channels/discord/prj/a/b").await;
        assert!(error.contains("PROJECT/THREAD_OR_ISSUE"), "{error}");
    }

    /// The purpose line is the verified desktop operator's: the channels door
    /// sets and clears it, and an agent's or a run-less caller's patch is
    /// refused.
    #[tokio::test]
    async fn only_the_verified_operator_sets_a_providers_purpose() {
        let orch = orchestrator().await;
        let patch = |purpose: &str| ChangeItem {
            target: "cairn://channels".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({ "provider": "telegram", "purpose": purpose })),
        };
        for run in [Some("r-live"), None] {
            let refused = dispatch_resource_change(&orch, &request(run), 0, &patch("x"), false)
                .await
                .unwrap_err()
                .error;
            assert!(refused.contains("Only the operator"), "{run:?}: {refused}");
        }
        let mut machine = request(None);
        machine.tool = "write".into();
        machine.origin = Some(cairn_common::protocol::DispatchOrigin::UnverifiedLocal);
        assert!(
            dispatch_resource_change(&orch, &machine, 0, &patch("x"), false)
                .await
                .is_err()
        );
        assert_eq!(
            crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .telegram
                .purpose,
            None
        );

        let set = crate::channels::write_as_operator(
            &orch,
            "cairn://channels",
            "patch",
            Some(serde_json::json!({ "provider": "telegram", "purpose": "urgent: my phone" })),
        )
        .await
        .unwrap();
        assert!(set.contains("Set Telegram's purpose"), "{set}");
        let settings = crate::config::settings::load_settings(&orch.config_dir).channels;
        assert_eq!(
            settings.telegram.purpose.as_deref(),
            Some("urgent: my phone")
        );
        assert!(
            settings.discord.enabled,
            "the rest of the document survives"
        );
        let rendered = crate::channels::read_as_operator(&orch, "cairn://channels")
            .await
            .unwrap();
        assert!(
            rendered.contains("- purpose: urgent: my phone"),
            "{rendered}"
        );

        crate::channels::write_as_operator(
            &orch,
            "cairn://channels",
            "patch",
            Some(serde_json::json!({ "provider": "telegram", "purpose": "" })),
        )
        .await
        .unwrap();
        assert_eq!(
            crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .telegram
                .purpose,
            None
        );
        let elsewhere = crate::channels::write_as_operator(
            &orch,
            "cairn://p/prj/general/messages",
            "append",
            Some(serde_json::json!({ "content": "x" })),
        )
        .await
        .unwrap_err();
        assert!(elsewhere.contains("not a channels resource"), "{elsewhere}");
    }

    /// The verified operator reaches every binding: the listing names another
    /// thread's channel that no thread but its own reaches.
    #[tokio::test]
    async fn the_verified_operator_lists_every_binding_and_narrows_by_project() {
        let orch = orchestrator().await;
        let listing = crate::channels::read_as_operator(&orch, "cairn://channels/discord")
            .await
            .unwrap();
        for label in [
            "#general · prj · Discord",
            "#other · prj · Discord",
            "#ambient · prj · Discord",
        ] {
            assert!(listing.contains(label), "{label}: {listing}");
        }
        assert!(listing.contains("the operator can reach"), "{listing}");
        let narrowed =
            crate::channels::read_as_operator(&orch, "cairn://channels/discord?project=zzz")
                .await
                .unwrap();
        assert!(
            narrowed.starts_with("# Discord conversations\n\n0 conversations"),
            "{narrowed}"
        );
        let history =
            crate::channels::read_as_operator(&orch, "cairn://channels/discord/prj/other")
                .await
                .unwrap();
        assert!(history.starts_with("# #other · prj · Discord"), "{history}");
        assert!(crate::channels::read_as_operator(&orch, "cairn://p/prj")
            .await
            .unwrap_err()
            .contains("not a channels resource"));
    }

    /// Only the channels door may stamp a request as the verified desktop
    /// operator, so no other path can read or write the channels tree as one.
    #[test]
    fn only_the_channels_door_builds_a_desktop_operator_origin() {
        fn visit(dir: &std::path::Path, found: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, found);
                } else if path.extension().is_some_and(|ext| ext == "rs")
                    && std::fs::read_to_string(&path)
                        .unwrap()
                        .contains(concat!("DispatchOrigin::", "DesktopOperator)"))
                {
                    found.push(path.display().to_string());
                }
            }
        }
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = Vec::new();
        visit(&src, &mut found);
        assert!(
            found
                .iter()
                .all(|path| path.ends_with("resources/channels.rs")),
            "{found:#?}"
        );
    }

    /// A thread reaches its own channel and its project's ambient channel; it
    /// is refused there only because no Discord runtime is running in a test.
    #[tokio::test]
    async fn a_reachable_conversation_is_still_refused_while_its_provider_is_down() {
        let orch = orchestrator().await;
        for target in [
            "cairn://channels/discord/prj/general",
            "cairn://channels/discord/prj",
        ] {
            let error = refusal(&orch, Some("r-live"), target).await;
            assert!(
                error.contains("admitted only while the provider is running"),
                "{target}: {error}"
            );
        }
    }
}
