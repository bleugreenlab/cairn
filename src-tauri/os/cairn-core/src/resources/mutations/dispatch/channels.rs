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
/// the labeller (a chartered thread, never a task or node; or the verified
/// desktop operator through the channels door), then the event, then the
/// re-route target.
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

/// The message a send's `replyTo` names, resolved within the conversation the
/// sender already reached: its Cairn id, and the provider id a native reply
/// references (`None` when the provider has no native reply). Every refusal
/// names what to do instead.
async fn reply_target<'a>(
    orch: &Orchestrator,
    address: &crate::channels::ConversationAddress,
    label: &str,
    uri: &str,
    id: &'a str,
) -> Result<(&'a str, Option<String>), String> {
    use crate::channels::ledger::{self, ReplyTarget};
    let provider = address.provider();
    let target =
        ledger::reply_target(&orch.db.local, provider.id(), &address.to_string(), id).await?;
    let guid = match target {
        None => {
            return Err(format!(
                "replyTo '{id}' is not a message in {label} ({uri}); reply to an `in` or `out` id that conversation's view shows."
            ))
        }
        Some(ReplyTarget::Outbound {
            provider_guid: None,
            status,
        }) => {
            return Err(format!(
                "message {id} has not been delivered yet ({status}), so there is nothing to reply to; read {uri} and reply once it reads sent."
            ))
        }
        Some(ReplyTarget::Inbound { message_guid: None }) => {
            return Err(format!(
                "message {id} was recorded before Cairn kept provider message ids, so {} cannot reply to it natively; send without replyTo.",
                provider.display_name()
            ))
        }
        Some(ReplyTarget::Inbound {
            message_guid: Some(guid),
        })
        | Some(ReplyTarget::Outbound {
            provider_guid: Some(guid),
            ..
        }) => guid,
    };
    Ok((
        id,
        crate::channels::native_reply(provider.id()).then_some(guid),
    ))
}

/// An agent sending free text to the conversation its target URI names,
/// optionally as a reply (`replyTo`) to a message that conversation shows.
///
/// Every refusal happens before the intent is ledgered, in a fixed order: the
/// payload shape, then that the sender is a live agent node (attribution comes
/// from the authenticated run, never the payload), then that the sender reaches
/// the conversation, then that it is sendable, then the provider's length
/// limit, then the reply target. A successful write reports the enqueue and never claims delivery:
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
    let reply_to = match payload.get("replyTo") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(id)) if !id.trim().is_empty() => Some(id.trim()),
        Some(_) => {
            return Err(build_failure(
                index,
                item,
                "payload.replyTo must be a message id string: an `in` or `out` id the conversation's view shows".to_string(),
            ))
        }
    };
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
    let reply = match reply_to {
        Some(id) => Some(
            reply_target(orch, &admitted.address, label, uri, id)
                .await
                .map_err(|error| build_failure(index, item, error))?,
        ),
        None => None,
    };
    let replying = match &reply {
        Some((id, Some(_))) => format!(", replying to {id}"),
        Some((id, None)) => format!(
            ", replying to {id}. {} cannot reply natively, so this is sent as a plain message",
            admitted.address.provider().display_name()
        ),
        None => String::new(),
    };
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
            "Would send a message to {label} ({uri}) as {}{replying}.{warning}",
            agent.home
        ));
    }
    let id = agent_message::enqueue(
        &orch.db.local,
        &admitted.address,
        &agent.home,
        Some(&agent.job_id),
        content,
        reply
            .as_ref()
            .map(|(to, guid)| crate::channels::ledger::OutboundReply {
                to,
                guid: guid.as_deref(),
            }),
    )
    .await
    .map_err(|error| build_failure(index, item, error))?;
    Ok(format!(
        "Queued message {id} to {label} (pending){replying}; read {uri} to see it sent, or why it failed.{warning}"
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

    /// The default project is the verified desktop operator's too: the door
    /// sets it to an existing project, refuses an unknown one by name, clears
    /// it back to the oldest project, and drops the retired `defaultThread`.
    #[tokio::test]
    async fn only_the_verified_operator_sets_the_default_project() {
        let orch = orchestrator().await;
        let patch = |key: &str| ChangeItem {
            target: "cairn://channels".into(),
            mode: ChangeMode::Patch,
            payload: Some(serde_json::json!({ "defaultProject": key })),
        };
        let default_project = || {
            crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .default_project
        };
        for run in [Some("r-live"), None] {
            let refused = dispatch_resource_change(&orch, &request(run), 0, &patch("prj"), false)
                .await
                .unwrap_err()
                .error;
            assert!(refused.contains("Only the operator"), "{run:?}: {refused}");
        }
        assert_eq!(default_project(), None);

        let operator = |key: &str| {
            crate::channels::write_as_operator(
                &orch,
                "cairn://channels",
                "patch",
                Some(serde_json::json!({ "defaultProject": key })),
            )
        };
        let unknown = operator("zzz").await.unwrap_err();
        assert!(
            unknown.contains("Unknown project 'zzz'") && unknown.contains("known: prj"),
            "{unknown}"
        );
        assert_eq!(default_project(), None);

        let path = crate::config::settings::get_settings_path(&orch.config_dir);
        let mut yaml = std::fs::read_to_string(&path).unwrap();
        yaml.push_str("  defaultThread: cairn://p/CAIRN/3404\n");
        std::fs::write(&path, yaml).unwrap();
        let set = operator("PRJ").await.unwrap();
        assert!(set.contains("Set the default project to prj"), "{set}");
        assert_eq!(default_project().as_deref(), Some("prj"));
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(!written.contains("defaultThread"), "{written}");
        assert!(
            crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .discord
                .enabled,
            "the rest of the document survives"
        );

        let cleared = operator("").await.unwrap();
        assert!(cleared.contains("Cleared the default project"), "{cleared}");
        assert_eq!(default_project(), None);
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

    /// `general`'s channel with a running Discord runtime (replying natively
    /// or not) and its messages: `in-1` (with its own provider id), `in-old`
    /// (recorded before ids were kept), `out-pending` (unsent) and `out-sent`
    /// in `discord:1/2`, and `in-other` in `#other`.
    async fn reply_fixture(native_reply: bool) -> (Orchestrator, impl Drop) {
        use crate::channels::ledger;
        let orch = orchestrator().await;
        let db = &orch.db.local;
        for (id, conversation, guid) in [
            ("in-1", "discord:1/2", Some("2:50")),
            ("in-old", "discord:1/2", None),
            ("in-other", "discord:1/3", Some("3:70")),
        ] {
            assert!(ledger::insert_inbound(
                db,
                &ledger::InboundRecord {
                    id: id.into(),
                    channel: "discord".into(),
                    provider_guid: None,
                    sender: "1234".into(),
                    text: "hi".into(),
                    received_at: 1,
                    rejection_reason: None,
                    acknowledged_at: None,
                    conversation: Some(conversation.into()),
                    target_uri: Some("cairn://p/prj/general".into()),
                    binding_kind: Some("structural".into()),
                    addressed_by: None,
                    message_guid: guid.map(str::to_string),
                },
            )
            .await
            .unwrap());
        }
        for id in ["out-pending", "out-sent"] {
            assert!(ledger::insert_intent(
                db,
                &ledger::NewOutbound {
                    id,
                    channel: "discord",
                    kind: "message",
                    binding_ref: &format!("message:{id}:cairn://p/prj/general"),
                    conversation: "discord:1/2",
                    job_id: None,
                    rendered_text: "earlier",
                    rendering: "text",
                    created_at: 1,
                },
            )
            .await
            .unwrap());
        }
        db.execute(
            "UPDATE channel_outbound SET status = 'sent', provider_guid = '2:60', sent_at = 2 WHERE id = 'out-sent'",
            (),
        )
        .await
        .unwrap();
        let runtime = crate::channels::register_test_runtime(
            "discord",
            crate::channels::titles::tests::TitledProvider::replying(native_reply),
        );
        (orch, runtime)
    }

    fn reply(reply_to: serde_json::Value) -> ChangeItem {
        ChangeItem {
            target: "cairn://channels/discord/prj/general".into(),
            mode: ChangeMode::Append,
            payload: Some(serde_json::json!({ "content": "answer", "replyTo": reply_to })),
        }
    }

    /// `(reply_to, reply_to_guid)` of the one agent message `answer` queued.
    async fn queued_reply(orch: &Orchestrator) -> (Option<String>, Option<String>) {
        orch.db
            .local
            .query_opt(
                "SELECT reply_to, reply_to_guid FROM channel_outbound WHERE rendered_text = 'answer'",
                (),
                |row| {
                    use crate::storage::RowExt;
                    Ok((row.opt_text(0)?, row.opt_text(1)?))
                },
            )
            .await
            .unwrap()
            .expect("one reply was queued")
    }

    #[tokio::test]
    #[serial_test::serial(channel_runtime)]
    async fn a_reply_to_a_message_the_view_shows_records_its_native_reference() {
        for (target, guid) in [("in-1", "2:50"), ("out-sent", "2:60")] {
            let (orch, _runtime) = reply_fixture(true).await;
            let applied = dispatch_resource_change(
                &orch,
                &request(Some("r-live")),
                0,
                &reply(serde_json::json!(target)),
                false,
            )
            .await
            .unwrap()
            .summary;
            assert!(
                applied.contains(&format!("replying to {target};")),
                "{applied}"
            );
            assert_eq!(
                queued_reply(&orch).await,
                (Some(target.to_string()), Some(guid.to_string()))
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(channel_runtime)]
    async fn a_reply_without_native_support_is_sent_plainly_and_says_so() {
        let (orch, _runtime) = reply_fixture(false).await;
        let applied = dispatch_resource_change(
            &orch,
            &request(Some("r-live")),
            0,
            &reply(serde_json::json!("in-1")),
            false,
        )
        .await
        .unwrap()
        .summary;
        assert!(
            applied.contains("Discord cannot reply natively, so this is sent as a plain message"),
            "{applied}"
        );
        assert_eq!(queued_reply(&orch).await, (Some("in-1".to_string()), None));
    }

    #[tokio::test]
    #[serial_test::serial(channel_runtime)]
    async fn a_reply_to_nothing_this_conversation_can_answer_is_refused_by_name() {
        let (orch, _runtime) = reply_fixture(true).await;
        let cases = [
            (
                serde_json::json!("nope"),
                "replyTo 'nope' is not a message in #general · prj · Discord (cairn://channels/discord/prj/general)",
            ),
            (
                serde_json::json!("in-other"),
                "replyTo 'in-other' is not a message in #general · prj · Discord",
            ),
            (
                serde_json::json!("out-pending"),
                "message out-pending has not been delivered yet (pending), so there is nothing to reply to",
            ),
            (
                serde_json::json!("in-old"),
                "message in-old was recorded before Cairn kept provider message ids, so Discord cannot reply to it natively",
            ),
            (
                serde_json::json!(7),
                "payload.replyTo must be a message id string",
            ),
        ];
        let count = || async {
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM channel_outbound", ())
                .await
                .unwrap()
        };
        let before = count().await;
        for (reply_to, expected) in cases {
            let error = dispatch_resource_change(
                &orch,
                &request(Some("r-live")),
                0,
                &reply(reply_to.clone()),
                false,
            )
            .await
            .unwrap_err()
            .error;
            assert!(error.contains(expected), "{reply_to}: {error}");
            assert_eq!(count().await, before, "{reply_to} ledgered nothing");
        }
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
