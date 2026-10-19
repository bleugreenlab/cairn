//! Relay catch-up: the install's relay channel carries the GitHub App's
//! webhooks and every ingress endpoint's events on one shared cursor.
//!
//! Each event is handled on its own. A GitHub event becomes a route fact (or
//! a default-branch sync); an endpoint event goes to [`crate::ingress`]. An
//! event that cannot be handled is recorded and skipped, and the cursor moves
//! past it, so one poison event never parks the feed for the events behind
//! it. The one exception is a key mismatch, which means every later event is
//! unreadable too: that pauses the channel until it is repaired.

use std::{collections::BTreeMap, time::Duration};

use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    github::{credentials, crypto},
    orchestrator::Orchestrator,
    projects::remote::find_project_by_remote_full_name,
    routes::RouteFact,
};

/// The api's relay, which every install's channel lives on.
pub const DEFAULT_RELAY_URL: &str = "https://api.cairn.computer/relay";

const POLL_INTERVAL: Duration = Duration::from_secs(15);
const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Deserialize)]
struct RelayResponse {
    events: Vec<RelayEvent>,
    #[serde(default)]
    has_more: bool,
}

fn is_dead_epoch_event(event: &RelayEvent, rotated_at: Option<&str>) -> bool {
    let Some(rotated_at) = rotated_at else {
        return false;
    };
    let Ok(received_at) = chrono::DateTime::parse_from_rfc3339(&event.received_at) else {
        return false;
    };
    let Ok(rotated_at) = chrono::DateTime::parse_from_rfc3339(rotated_at) else {
        return false;
    };
    received_at < rotated_at
}

fn failure_backoff(consecutive_failures: u32) -> Duration {
    let multiplier = 1_u32
        .checked_shl(consecutive_failures.saturating_sub(1).min(31))
        .unwrap_or(u32::MAX);
    POLL_INTERVAL
        .checked_mul(multiplier)
        .unwrap_or(MAX_FAILURE_BACKOFF)
        .min(MAX_FAILURE_BACKOFF)
}

fn decode_cursor(cursor: &str) -> (&str, Option<i64>) {
    let Some((received_at, id)) = cursor.rsplit_once('|') else {
        return (cursor, None);
    };
    match id.parse() {
        Ok(id) => (received_at, Some(id)),
        Err(_) => (cursor, None),
    }
}

fn encode_cursor(received_at: &str, id: i64) -> String {
    format!("{received_at}|{id}")
}

fn event_cursor(event: &RelayEvent) -> String {
    encode_cursor(&event.received_at, event.id)
}

#[derive(Debug, Default, Deserialize)]
pub struct RelayEvent {
    pub id: i64,
    pub event_type: String,
    pub payload: Value,
    pub received_at: String,
    #[serde(default)]
    pub encrypted: bool,
    /// Set only on an ingress endpoint's event; a GitHub webhook has none.
    #[serde(default)]
    pub endpoint_id: Option<String>,
    #[serde(default)]
    pub delivery_id: Option<String>,
    #[serde(default)]
    pub verification: Option<String>,
    #[serde(default)]
    pub sender: Option<Value>,
}

#[derive(Debug, PartialEq)]
enum RelayPayloadError {
    KeyMismatch,
    Malformed(String),
}

fn decrypt_event_payload(payload: &Value, private_key: &str) -> Result<Value, RelayPayloadError> {
    let encrypted = payload.as_str().ok_or_else(|| {
        RelayPayloadError::Malformed("encrypted relay payload was not a string".into())
    })?;
    let plaintext = crypto::decrypt_payload(encrypted, private_key).map_err(|error| {
        if error == "Decryption failed" {
            RelayPayloadError::KeyMismatch
        } else {
            RelayPayloadError::Malformed(error)
        }
    })?;
    serde_json::from_str(&plaintext).map_err(|error| {
        RelayPayloadError::Malformed(format!("invalid GitHub webhook payload: {error}"))
    })
}

fn record_key_mismatch(creds: &mut credentials::GitHubCredentials, event: &RelayEvent, now: &str) {
    let failures = creds
        .relay_consecutive_failures
        .unwrap_or(0)
        .saturating_add(1);
    creds.relay_health_state = Some(
        if failures >= 2 {
            "key_mismatch"
        } else {
            "suspect"
        }
        .into(),
    );
    creds.relay_health_reason = Some("relay_key_mismatch".into());
    creds
        .relay_first_failure_at
        .get_or_insert_with(|| now.to_owned());
    creds.relay_last_failure_at = Some(now.to_owned());
    creds.relay_consecutive_failures = Some(failures);
    creds.relay_failing_event_id = Some(event.id.to_string());
    creds.relay_failing_event_at = Some(event.received_at.clone());
}

/// Record an event the consumer skipped. The feed keeps moving, so this is not
/// a failure state like `key_mismatch`: it is `skipping`, with the skipped
/// event and the reason, until the next successful delivery clears it. The
/// consecutive-failure count belongs to key mismatches and is left alone.
fn record_skip(
    creds: &mut credentials::GitHubCredentials,
    event_id: i64,
    received_at: &str,
    reason: &str,
    now: &str,
) {
    creds.relay_health_state = Some("skipping".into());
    creds.relay_health_reason = Some(reason.into());
    creds
        .relay_first_failure_at
        .get_or_insert_with(|| now.to_owned());
    creds.relay_last_failure_at = Some(now.to_owned());
    creds.relay_failing_event_id = Some(event_id.to_string());
    creds.relay_failing_event_at = Some(received_at.to_owned());
    creds.relay_undeliverable_since_healthy =
        creds.relay_undeliverable_since_healthy.saturating_add(1);
}

fn record_successful_delivery(creds: &mut credentials::GitHubCredentials, now: &str) {
    creds.relay_undeliverable_since_healthy = 0;
    creds.relay_health_state = Some("healthy".into());
    creds.relay_health_reason = None;
    creds.relay_first_failure_at = None;
    creds.relay_last_failure_at = None;
    creds.relay_consecutive_failures = Some(0);
    creds.relay_failing_event_id = None;
    creds.relay_failing_event_at = None;
    creds.relay_last_successful_delivery_at = Some(now.to_owned());
}

#[derive(Debug, PartialEq)]
pub enum TriggerDecision {
    Ignore,
    Comment(CommentTrigger),
}

#[derive(Debug, PartialEq)]
pub struct CommentTrigger {
    pub comment_id: i64,
    pub repository: String,
    pub number: i64,
    pub kind: &'static str,
    pub author: String,
    pub url: String,
    pub body: String,
}

/// Maps only newly-created issue comments that explicitly address this app.
/// GitHub uses `issue_comment` for both issue and pull-request conversation.
pub fn trigger_for_event(event_type: &str, payload: &Value, app_slug: &str) -> TriggerDecision {
    if event_type != "issue_comment" || payload["action"].as_str() != Some("created") {
        return TriggerDecision::Ignore;
    }
    let Some(body) = payload["comment"]["body"].as_str() else {
        return TriggerDecision::Ignore;
    };
    if !contains_mention(body, app_slug) {
        return TriggerDecision::Ignore;
    }
    let author = payload["comment"]["user"]["login"]
        .as_str()
        .unwrap_or_default();
    let author_type = payload["comment"]["user"]["type"]
        .as_str()
        .unwrap_or_default();
    if author.is_empty()
        || author_type.eq_ignore_ascii_case("bot")
        || author.eq_ignore_ascii_case(app_slug)
        || author.eq_ignore_ascii_case(&format!("{app_slug}[bot]"))
    {
        return TriggerDecision::Ignore;
    }
    let Some(repository) = payload["repository"]["full_name"].as_str() else {
        return TriggerDecision::Ignore;
    };
    let Some(number) = payload["issue"]["number"].as_i64() else {
        return TriggerDecision::Ignore;
    };
    let Some(comment_id) = payload["comment"]["id"].as_i64() else {
        return TriggerDecision::Ignore;
    };
    TriggerDecision::Comment(CommentTrigger {
        comment_id,
        repository: repository.to_owned(),
        number,
        kind: if payload["issue"].get("pull_request").is_some() {
            "pull_request"
        } else {
            "issue"
        },
        author: author.to_owned(),
        url: payload["comment"]["html_url"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        body: body.to_owned(),
    })
}

fn contains_mention(body: &str, app_slug: &str) -> bool {
    let mention = format!("@{}", app_slug.to_ascii_lowercase());
    body.split(|character: char| {
        !(character.is_ascii_alphanumeric()
            || character == '-'
            || character == '_'
            || character == '@')
    })
    .any(|word| word.to_ascii_lowercase() == mention)
}

/// The issue description a mention becomes: where the comment came from, then
/// the comment itself. The fact log rebuilds a comment fact's `body` with this
/// from its stored fields, so the two can never disagree.
pub(crate) fn comment_context(
    repository: &str,
    kind: &str,
    number: &str,
    author: &str,
    url: &str,
    text: &str,
) -> String {
    format!("Repository: {repository}\nGitHub {kind}: #{number}\nAuthor: @{author}\nURL: {url}\n\n{text}")
}

pub(crate) fn comment_fact(
    comment: &CommentTrigger,
    project: &str,
    origin: crate::issues::crud::IssueAuthorship,
) -> RouteFact {
    let title = format!(
        "GitHub {} #{} mention from @{}",
        comment.kind.replace('_', " "),
        comment.number,
        comment.author
    );
    let context = comment_context(
        &comment.repository,
        comment.kind,
        &comment.number.to_string(),
        &comment.author,
        &comment.url,
        &comment.body,
    );
    RouteFact {
        source: "github_comment".into(),
        identity: format!("github-comment:{}", comment.comment_id),
        fields: BTreeMap::from([
            ("project".into(), json!(project)),
            ("repository".into(), json!(comment.repository)),
            ("number".into(), json!(comment.number.to_string())),
            ("kind".into(), json!(comment.kind)),
            ("author".into(), json!(comment.author)),
            ("url".into(), json!(comment.url)),
            ("title".into(), json!(title)),
            ("body".into(), json!(context)),
            ("text".into(), json!(comment.body)),
        ]),
        origin: Some(origin),
        summary: Some(title),
        route_provenance: None,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessEventOutcome {
    Ignored,
    UnmappedMention,
    Routed,
    /// A push to a repository's default branch, handed to the default-branch
    /// sync in the background.
    DefaultBranchPush,
}

/// The `push` fields the default-branch sync needs, when this is a push to the
/// repository's own default branch.
fn default_branch_push(event_type: &str, payload: &Value) -> Option<DefaultBranchPush> {
    if event_type != "push" || payload["deleted"].as_bool() == Some(true) {
        return None;
    }
    let push = DefaultBranchPush {
        repository: payload["repository"]["full_name"].as_str()?.to_string(),
        git_ref: payload["ref"].as_str()?.to_string(),
        default_branch: payload["repository"]["default_branch"]
            .as_str()?
            .to_string(),
        after: payload["after"].as_str()?.to_string(),
    };
    crate::projects::remote::is_default_branch_push(&push.git_ref, &push.default_branch)
        .then_some(push)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DefaultBranchPush {
    repository: String,
    git_ref: String,
    default_branch: String,
    after: String,
}

/// Map a default-branch push onto its project, fast-forward the operator's
/// checkout when that is safe, and tell the default-branch sync where origin
/// now points.
async fn handle_default_branch_push(
    orch: &Orchestrator,
    push: &DefaultBranchPush,
) -> Result<crate::projects::remote::PushRemoteSyncOutcome, String> {
    let outcome = crate::projects::remote::pull_project_on_default_branch_push(
        &orch.db.local,
        orch.services.git.as_ref(),
        &push.repository,
        &push.git_ref,
        &push.default_branch,
    )
    .await
    .map_err(|error| error.to_string())?;
    if let crate::projects::remote::PushRemoteSyncOutcome::Pulled { project_id, .. } = &outcome {
        crate::orchestrator::default_branch_sync::observe_remote_tip(
            orch,
            project_id,
            &push.after,
            crate::orchestrator::default_branch_sync::Source::Push,
        )
        .await;
    }
    Ok(outcome)
}

pub async fn process_event(
    orch: &Orchestrator,
    event_id: i64,
    event_type: &str,
    payload: &Value,
    app_slug: &str,
) -> Result<ProcessEventOutcome, String> {
    if let Some(push) = default_branch_push(event_type, payload) {
        // Like minting, the relay's obligation ends at handing the push off:
        // the checkout pull and store sync run detached, so a slow fetch can
        // never hold the relay cursor.
        let orch = orch.clone();
        tokio::spawn(async move {
            match handle_default_branch_push(&orch, &push).await {
                Ok(outcome) => log::debug!(
                    "GitHub relay event {event_id}: default-branch push to {} at {}: {outcome:?}",
                    push.repository,
                    push.after
                ),
                Err(error) => log::warn!(
                    "GitHub relay event {event_id}: default-branch push to {} failed: {error}",
                    push.repository
                ),
            }
        });
        return Ok(ProcessEventOutcome::DefaultBranchPush);
    }
    let TriggerDecision::Comment(comment) = trigger_for_event(event_type, payload, app_slug) else {
        log::debug!(
            "Ignoring GitHub relay event {event_id}: type={event_type}, action={}",
            payload["action"].as_str().unwrap_or("<none>")
        );
        return Ok(ProcessEventOutcome::Ignored);
    };
    let Some(project) = find_project_by_remote_full_name(
        &orch.db.local,
        orch.services.git.as_ref(),
        &comment.repository,
    )
    .await
    .map_err(|error| error.to_string())?
    else {
        let now = chrono::Utc::now().to_rfc3339();
        log::info!(
            "Dropping GitHub app mention from relay event {event_id}: type={event_type}, action={}, repository={} maps to no Cairn project",
            payload["action"].as_str().unwrap_or("<none>"),
            comment.repository
        );
        credentials::update_github_credentials(&orch.db.local, |creds| {
            creds.relay_unmapped_mention_count =
                creds.relay_unmapped_mention_count.saturating_add(1);
            creds.relay_last_unmapped_event_id = Some(event_id.to_string());
            creds.relay_last_unmapped_repository = Some(comment.repository.clone());
            creds.relay_last_unmapped_at = Some(now);
        })
        .await?;
        return Ok(ProcessEventOutcome::UnmappedMention);
    };
    // Minting is the relay's whole obligation: routes run later, from the
    // fact log, so a route that fails or hangs cannot stall the relay cursor.
    // A failed insert still propagates and the event is retried; the source's
    // dedupe absorbs the retry.
    crate::routes::mint(
        orch,
        crate::routes::Mint {
            fact: comment_fact(
                &comment,
                &project.key,
                crate::routes::installation_machine_origin(orch)?,
            ),
            record_ref: comment.url.clone(),
            record_id: Some(comment.comment_id.to_string()),
            project_id: Some(&project.id),
            presence: crate::routes::log::operator_presence().await,
            actor_job_id: None,
        },
    )
    .await?;
    Ok(ProcessEventOutcome::Routed)
}

/// What became of one relay event.
#[derive(Debug, PartialEq, Eq)]
enum EventOutcome {
    /// Opened and handled: evidence the channel delivers.
    Delivered,
    /// Passed over without being opened: a dead-epoch event, or the skip
    /// marker the relay sends in place of an oversized payload.
    PassedOver,
    /// Could not be handled. Recorded, and the cursor still advances.
    Skipped {
        reason: &'static str,
        detail: String,
    },
}

/// What every event on one sync shares.
struct PageContext<'a> {
    private_key: &'a str,
    relay_key_rotated_at: Option<&'a str>,
    app_slug: &'a str,
}

/// Handle one event. `Err` only for a key mismatch outside a dead epoch,
/// which is recorded first and pauses the channel.
async fn handle_event(
    orch: &Orchestrator,
    event: &RelayEvent,
    ctx: &PageContext<'_>,
) -> Result<EventOutcome, String> {
    // `Ok(None)` is a plaintext event: a skip marker in place of a payload.
    let opened: Result<Option<Value>, String> = if event.encrypted {
        match decrypt_event_payload(&event.payload, ctx.private_key) {
            Ok(payload) => Ok(Some(payload)),
            Err(RelayPayloadError::KeyMismatch) => {
                if is_dead_epoch_event(event, ctx.relay_key_rotated_at) {
                    log::warn!(
                        "skipping dead-epoch relay event {} (pre-rotation)",
                        event.id
                    );
                    return Ok(EventOutcome::PassedOver);
                }
                let now = chrono::Utc::now().to_rfc3339();
                credentials::update_github_credentials(&orch.db.local, |creds| {
                    record_key_mismatch(creds, event, &now);
                })
                .await?;
                return Err(format!(
                    "relay key mismatch at event {}; cursor remains paused",
                    event.id
                ));
            }
            Err(RelayPayloadError::Malformed(error)) => Err(error),
        }
    } else {
        Ok(None)
    };

    if let Some(endpoint_id) = &event.endpoint_id {
        let body = match &opened {
            Ok(Some(payload)) => Ok(payload.clone()),
            Ok(None) => Err(format!(
                "the relay delivered no sealed body ({})",
                event.payload
            )),
            Err(error) => Err(format!("the sealed body could not be opened: {error}")),
        };
        let consumed = crate::ingress::consume::consume(
            orch,
            crate::ingress::consume::EndpointEvent {
                relay_event_id: event.id,
                endpoint_id: endpoint_id.clone(),
                delivery_id: event.delivery_id.clone(),
                event_type: event.event_type.clone(),
                verification: event.verification.clone(),
                sender: event.sender.clone(),
                received_at: event.received_at.clone(),
                body,
            },
        )
        .await;
        return Ok(match consumed {
            crate::ingress::consume::Consumed::Delivered { .. } => EventOutcome::Delivered,
            crate::ingress::consume::Consumed::Undeliverable { reason, detail } => {
                log::warn!(
                    "Skipping relay event {} for ingress endpoint {endpoint_id}: {reason}: {detail}",
                    event.id
                );
                EventOutcome::Skipped { reason, detail }
            }
        });
    }

    let (payload, decrypted) = match opened {
        Ok(Some(payload)) => (payload, true),
        Ok(None) => (event.payload.clone(), false),
        Err(error) => {
            log::warn!(
                "Skipping malformed GitHub relay event {}: {error}",
                event.id
            );
            return Ok(EventOutcome::Skipped {
                reason: "relay_event_malformed",
                detail: error,
            });
        }
    };
    match process_event(orch, event.id, &event.event_type, &payload, ctx.app_slug).await {
        Ok(_) if decrypted => Ok(EventOutcome::Delivered),
        Ok(_) => Ok(EventOutcome::PassedOver),
        Err(error) => {
            log::warn!(
                "Skipping GitHub relay event {} ({}): {error}",
                event.id,
                event.event_type
            );
            Ok(EventOutcome::Skipped {
                reason: "relay_event_processing_failed",
                detail: error,
            })
        }
    }
}

/// Handle a page of events in order, persisting the cursor and the health
/// evidence after each. Returns the cursor after the last event handled.
async fn apply_page(
    orch: &Orchestrator,
    events: Vec<RelayEvent>,
    ctx: &PageContext<'_>,
) -> Result<Option<String>, String> {
    let mut cursor = None;
    for event in events {
        let event_cursor = event_cursor(&event);
        let outcome = handle_event(orch, &event, ctx).await?;
        let now = chrono::Utc::now().to_rfc3339();
        credentials::update_github_credentials(&orch.db.local, |creds| {
            creds.last_event_sync = Some(event_cursor.clone());
            match &outcome {
                EventOutcome::Delivered => record_successful_delivery(creds, &now),
                EventOutcome::PassedOver => {}
                EventOutcome::Skipped { reason, .. } => {
                    record_skip(creds, event.id, &event.received_at, reason, &now)
                }
            }
        })
        .await?;
        cursor = Some(event_cursor);
    }
    Ok(cursor)
}

async fn sync_once(orch: &Orchestrator, relay_url: &str) -> Result<(), String> {
    let _relay_operation = credentials::relay_operation_lock().lock().await;
    let creds = credentials::get_github_credentials(&orch.db.local).await?;
    let (Some(channel_id), Some(secret), Some(app_slug), Some(encrypted_private_key)) = (
        creds.relay_channel_id,
        creds.relay_secret,
        creds.app_slug,
        creds.relay_private_key_encrypted,
    ) else {
        return Ok(());
    };
    let private_key =
        crypto::decrypt_private_key(&encrypted_private_key, &crypto::get_machine_id())?;
    let client = reqwest::Client::new();
    let events_url = format!("{}/events/{channel_id}", relay_url.trim_end_matches('/'));
    let mut cursor = creds.last_event_sync;
    let relay_key_rotated_at = creds.relay_key_rotated_at;

    loop {
        let mut request = client.get(&events_url).bearer_auth(&secret);
        if let Some(cursor) = cursor.as_deref() {
            let (since, after_id) = decode_cursor(cursor);
            request = request.query(&[("since", since)]);
            if let Some(after_id) = after_id {
                request = request.query(&[("after_id", after_id)]);
            }
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("relay event sync failed: {error}"))?;
        if !response.status().is_success() {
            return Err(format!("relay event sync returned {}", response.status()));
        }
        let response: RelayResponse = response
            .json()
            .await
            .map_err(|error| format!("invalid relay response: {error}"))?;

        let ctx = PageContext {
            private_key: &private_key,
            relay_key_rotated_at: relay_key_rotated_at.as_deref(),
            app_slug: &app_slug,
        };
        if let Some(last) = apply_page(orch, response.events, &ctx).await? {
            cursor = Some(last);
        }

        if !response.has_more {
            return Ok(());
        }
    }
}

pub fn spawn_relay_sync(orch: Orchestrator, relay_url: &'static str) {
    tokio::spawn(async move {
        let mut consecutive_failures = 0;
        let mut repair_generation = credentials::relay_repair_generation();
        loop {
            let delay = match sync_once(&orch, relay_url).await {
                Ok(()) => {
                    consecutive_failures = 0;
                    POLL_INTERVAL
                }
                Err(error) => {
                    consecutive_failures += 1;
                    let delay = failure_backoff(consecutive_failures);
                    log::warn!(
                        "GitHub relay sync failed: {error}; retrying in {} seconds",
                        delay.as_secs()
                    );
                    delay
                }
            };
            if credentials::relay_repair_generation() != repair_generation {
                repair_generation = credentials::relay_repair_generation();
                consecutive_failures = 0;
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = credentials::relay_repair_notified() => {
                    repair_generation = credentials::relay_repair_generation();
                    consecutive_failures = 0;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::migrated_test_db;
    use std::sync::Arc;

    async fn test_orchestrator(name: &str) -> (Orchestrator, Arc<crate::storage::LocalDb>) {
        use crate::db::DbState;
        use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
        use crate::storage::SearchIndex;

        let temp = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(migrated_test_db(name).await);
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(
                TestServicesBuilder::new()
                    .with_process(RecordingProcessSpawner::new().clone())
                    .build(),
            ),
            temp.join("config"),
        )
        .build();
        (orch, db)
    }

    fn comment(author: &str, author_type: &str, body: &str) -> Value {
        json!({
            "action": "created",
            "repository": {"full_name": "cairn/cairn"},
            "issue": {"number": 42, "pull_request": {"url": "https://api.github.test/pulls/42"}},
            "comment": {"id": 99, "body": body, "html_url": "https://github.test/cairn/cairn/pull/42#issuecomment-99", "user": {"login": author, "type": author_type}}
        })
    }

    #[tokio::test]
    async fn process_event_reports_ignored_non_mentions() {
        let (orch, _) = test_orchestrator("relay-ignored-event.db").await;

        let outcome = process_event(
            &orch,
            100,
            "issue_comment",
            &comment("alice", "User", "hello"),
            "cairn-app",
        )
        .await
        .unwrap();

        assert_eq!(outcome, ProcessEventOutcome::Ignored);
    }

    #[tokio::test]
    async fn process_event_records_mentions_for_unmapped_repositories() {
        let (orch, db) = test_orchestrator("relay-unmapped-mention.db").await;
        let mut payload = comment("alice", "User", "@cairn-app please investigate");
        payload["repository"]["full_name"] = json!("unknown/repository");

        let outcome = process_event(&orch, 101, "issue_comment", &payload, "cairn-app")
            .await
            .unwrap();

        assert_eq!(outcome, ProcessEventOutcome::UnmappedMention);
        let credentials = credentials::get_github_credentials(&db).await.unwrap();
        assert_eq!(credentials.relay_unmapped_mention_count, 1);
        assert_eq!(
            credentials.relay_last_unmapped_event_id.as_deref(),
            Some("101")
        );
        assert_eq!(
            credentials.relay_last_unmapped_repository.as_deref(),
            Some("unknown/repository")
        );
        assert!(credentials.relay_last_unmapped_at.is_some());
    }

    fn push(git_ref: &str, repository: &str) -> Value {
        json!({
            "ref": git_ref,
            "after": "9e77a2750000000000000000000000000000000b",
            "deleted": false,
            "repository": {"full_name": repository, "default_branch": "main"}
        })
    }

    #[test]
    fn only_a_push_to_the_default_branch_is_a_default_branch_push() {
        let push_event =
            default_branch_push("push", &push("refs/heads/main", "acme/repo")).unwrap();
        assert_eq!(push_event.after, "9e77a2750000000000000000000000000000000b");
        assert_eq!(push_event.repository, "acme/repo");
        assert_eq!(
            default_branch_push("push", &push("refs/heads/feature", "acme/repo")),
            None
        );
        let mut deleted = push("refs/heads/main", "acme/repo");
        deleted["deleted"] = json!(true);
        assert_eq!(default_branch_push("push", &deleted), None);
        assert_eq!(
            default_branch_push("issue_comment", &push("refs/heads/main", "acme/repo")),
            None
        );
    }

    #[tokio::test]
    async fn a_default_branch_push_for_a_mapped_project_reaches_the_sync() {
        use crate::services::testing::MockGitClient;

        let db = Arc::new(migrated_test_db("relay-default-push.db").await);
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('proj', 'default', 'Project', 'PROJ', '/repos/proj', 'main', 1, 1)",
            (),
        )
        .await
        .unwrap();
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .returning(|_| Ok("git@github.com:acme/repo.git".into()));
        // A dirty operator checkout is left alone; the store sync still runs.
        git.expect_status()
            .returning(|_| Ok(" M src/lib.rs".into()));
        git.expect_fetch_origin_branch()
            .returning(|_, _| Err("git fetch origin failed: offline".into()));
        let temp = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(crate::db::DbState::new(
                db.clone(),
                Arc::new(crate::storage::SearchIndex::open_or_create(temp.join("search")).unwrap()),
            )),
            Arc::new(
                crate::services::testing::TestServicesBuilder::new()
                    .with_git(git)
                    .build(),
            ),
            temp.join("config"),
        )
        .build();

        let mapped = default_branch_push("push", &push("refs/heads/main", "acme/repo")).unwrap();
        let outcome = handle_default_branch_push(&orch, &mapped).await.unwrap();
        assert!(matches!(
            outcome,
            crate::projects::remote::PushRemoteSyncOutcome::Pulled { ref project_id, .. } if project_id == "proj"
        ));
        let state = orch.default_branch_sync.snapshot("proj").unwrap();
        assert_eq!(
            state.remote_tip.as_deref(),
            Some("9e77a2750000000000000000000000000000000b")
        );
        assert_eq!(
            state.remote_tip_source,
            Some(crate::orchestrator::default_branch_sync::Source::Push)
        );

        let unmapped = default_branch_push("push", &push("refs/heads/main", "other/repo")).unwrap();
        assert_eq!(
            handle_default_branch_push(&orch, &unmapped).await.unwrap(),
            crate::projects::remote::PushRemoteSyncOutcome::NoMatchingProject
        );
    }

    #[tokio::test]
    async fn process_event_hands_a_default_branch_push_off_and_still_routes_mentions() {
        let (orch, _) = test_orchestrator("relay-push-outcome.db").await;
        let outcome = process_event(
            &orch,
            102,
            "push",
            &push("refs/heads/main", "acme/repo"),
            "cairn-app",
        )
        .await
        .unwrap();
        assert_eq!(outcome, ProcessEventOutcome::DefaultBranchPush);
        let outcome = process_event(
            &orch,
            103,
            "push",
            &push("refs/heads/feature", "acme/repo"),
            "cairn-app",
        )
        .await
        .unwrap();
        assert_eq!(outcome, ProcessEventOutcome::Ignored);
    }

    #[test]
    fn maps_an_explicit_app_mention_with_actionable_context() {
        let TriggerDecision::Comment(trigger) = trigger_for_event(
            "issue_comment",
            &comment("alice", "User", "@cairn-app please investigate"),
            "cairn-app",
        ) else {
            panic!("expected comment trigger")
        };
        assert_eq!(trigger.repository, "cairn/cairn");
        assert_eq!(trigger.number, 42);
        assert_eq!(trigger.kind, "pull_request");
        assert_eq!(trigger.author, "alice");
        assert_eq!(trigger.comment_id, 99);
    }

    #[test]
    fn ignores_non_mentions_and_bot_authored_comments() {
        assert_eq!(
            trigger_for_event(
                "issue_comment",
                &comment("alice", "User", "hello"),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
        assert_eq!(
            trigger_for_event(
                "issue_comment",
                &comment("dependabot[bot]", "Bot", "@cairn-app hello"),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
        assert_eq!(
            trigger_for_event(
                "issue_comment",
                &comment("cairn-app[bot]", "User", "@cairn-app hello"),
                "cairn-app"
            ),
            TriggerDecision::Ignore
        );
    }

    #[test]
    fn cursor_is_stable_with_equal_timestamps_and_accepts_legacy_values() {
        let first = encode_cursor("2026-08-10T20:00:00.000Z", 41);
        let second = encode_cursor("2026-08-10T20:00:00.000Z", 42);
        assert_ne!(first, second);
        assert_eq!(
            decode_cursor(&first),
            ("2026-08-10T20:00:00.000Z", Some(41))
        );
        assert_eq!(
            decode_cursor("2026-08-10T20:00:00.000Z"),
            ("2026-08-10T20:00:00.000Z", None)
        );
    }

    #[test]
    fn relay_skip_marker_is_ignored_and_carries_its_composite_cursor() {
        let marker: RelayEvent = serde_json::from_value(json!({
            "id": 67241,
            "event_type": "cairn.relay_skipped",
            "payload": { "error": "payload_exceeds_hard_ceiling" },
            "received_at": "2026-07-08T17:35:12.732Z",
            "encrypted": false
        }))
        .unwrap();

        assert_eq!(
            trigger_for_event(&marker.event_type, &marker.payload, "cairn-app"),
            TriggerDecision::Ignore
        );
        assert_eq!(event_cursor(&marker), "2026-07-08T17:35:12.732Z|67241");
    }

    #[test]
    fn relay_response_controls_immediate_page_draining() {
        let more: RelayResponse = serde_json::from_value(json!({
            "events": [],
            "has_more": true
        }))
        .unwrap();
        let final_page: RelayResponse = serde_json::from_value(json!({"events": []})).unwrap();
        assert!(more.has_more);
        assert!(!final_page.has_more);
    }

    fn encrypted_event(id: i64) -> RelayEvent {
        RelayEvent {
            id,
            event_type: "issue_comment".into(),
            payload: json!("ciphertext"),
            received_at: "2026-08-12T21:30:00Z".into(),
            encrypted: true,
            ..Default::default()
        }
    }

    #[test]
    fn malformed_encrypted_payload_is_not_key_mismatch_evidence() {
        assert!(matches!(
            decrypt_event_payload(&json!({"ciphertext": "not-a-string"}), "unused"),
            Err(RelayPayloadError::Malformed(message))
                if message == "encrypted relay payload was not a string"
        ));
        assert!(matches!(
            decrypt_event_payload(&json!("not base64"), "unused"),
            Err(RelayPayloadError::Malformed(message)) if message.starts_with("Invalid base64:")
        ));
    }

    #[test]
    fn only_pre_rotation_mismatches_belong_to_a_dead_epoch() {
        let mut event = encrypted_event(73);
        event.received_at = "2026-08-12T21:30:00Z".into();
        assert!(is_dead_epoch_event(&event, Some("2026-08-12T21:31:00Z")));
        event.received_at = "2026-08-12T21:32:00Z".into();
        assert!(!is_dead_epoch_event(&event, Some("2026-08-12T21:31:00Z")));
        assert!(!is_dead_epoch_event(&event, None));
    }

    #[test]
    fn repeated_key_mismatch_promotes_suspect_and_preserves_first_failure() {
        let event = encrypted_event(73);
        let mut creds = credentials::GitHubCredentials {
            last_event_sync: Some("previous|72".into()),
            ..Default::default()
        };

        record_key_mismatch(&mut creds, &event, "2026-08-12T21:31:00Z");
        assert_eq!(creds.relay_health_state.as_deref(), Some("suspect"));
        assert_eq!(creds.relay_consecutive_failures, Some(1));
        assert_eq!(creds.last_event_sync.as_deref(), Some("previous|72"));

        record_key_mismatch(&mut creds, &event, "2026-08-12T21:32:00Z");
        assert_eq!(creds.relay_health_state.as_deref(), Some("key_mismatch"));
        assert_eq!(
            creds.relay_health_reason.as_deref(),
            Some("relay_key_mismatch")
        );
        assert_eq!(creds.relay_consecutive_failures, Some(2));
        assert_eq!(
            creds.relay_first_failure_at.as_deref(),
            Some("2026-08-12T21:31:00Z")
        );
        assert_eq!(
            creds.relay_last_failure_at.as_deref(),
            Some("2026-08-12T21:32:00Z")
        );
        assert_eq!(creds.relay_failing_event_id.as_deref(), Some("73"));
        assert_eq!(
            creds.relay_failing_event_at.as_deref(),
            Some(event.received_at.as_str())
        );
        assert_eq!(creds.last_event_sync.as_deref(), Some("previous|72"));
    }

    #[test]
    fn successful_delivery_clears_evidence_and_finishes_recovery() {
        let mut creds = credentials::GitHubCredentials {
            relay_health_state: Some("recovering".into()),
            relay_health_reason: Some("relay_key_mismatch".into()),
            relay_first_failure_at: Some("first".into()),
            relay_last_failure_at: Some("last".into()),
            relay_consecutive_failures: Some(4),
            relay_failing_event_id: Some("73".into()),
            relay_failing_event_at: Some("event-time".into()),
            ..Default::default()
        };

        record_successful_delivery(&mut creds, "2026-08-12T21:35:00Z");

        assert_eq!(creds.relay_health_state.as_deref(), Some("healthy"));
        assert_eq!(creds.relay_consecutive_failures, Some(0));
        assert_eq!(
            creds.relay_last_successful_delivery_at.as_deref(),
            Some("2026-08-12T21:35:00Z")
        );
        assert!(creds.relay_health_reason.is_none());
        assert!(creds.relay_first_failure_at.is_none());
        assert!(creds.relay_last_failure_at.is_none());
        assert!(creds.relay_failing_event_id.is_none());
        assert!(creds.relay_failing_event_at.is_none());
    }

    /// The install's relay key, a bugs endpoint mirrored for project `p`, and a
    /// way to seal events to the key the way the api does.
    struct SealedFeed {
        public_key: String,
        private_key: String,
    }

    impl SealedFeed {
        async fn new(db: &crate::storage::LocalDb) -> Self {
            crate::ingress::upsert_endpoint(
                db,
                &crate::ingress::EndpointRecord {
                    id: "src_bugs".into(),
                    channel_id: "ch-1".into(),
                    project_id: Some("p".into()),
                    name: "bugs".into(),
                    provider: "first-party".into(),
                    topic: Some("cairn.bugs".into()),
                    status: "active".into(),
                    granted_at: Some("2026-10-18T00:00:00Z".into()),
                    created_at: 1,
                    revoked_at: None,
                },
            )
            .await
            .unwrap();
            let (public_key, private_key) = crypto::generate_keypair();
            Self {
                public_key,
                private_key,
            }
        }

        fn ctx(&self) -> PageContext<'_> {
            PageContext {
                private_key: &self.private_key,
                relay_key_rotated_at: None,
                app_slug: "cairn-app",
            }
        }

        /// A mention whose repository name can never be resolved, so
        /// `process_event` fails on every attempt.
        fn poison_github_event(&self, id: i64) -> RelayEvent {
            let mut payload = comment("alice", "User", "@cairn-app please look");
            payload["repository"]["full_name"] = json!("not-a-repository-name");
            RelayEvent {
                id,
                event_type: "issue_comment".into(),
                payload: json!(crypto::seal_payload_for_test(
                    &payload.to_string(),
                    &self.public_key
                )),
                received_at: format!("2026-10-18T20:00:{id:02}.000Z"),
                encrypted: true,
                ..Default::default()
            }
        }

        fn bug_event(&self, id: i64, endpoint_id: &str, report: i64) -> RelayEvent {
            RelayEvent {
                id,
                event_type: "bug.reported".into(),
                payload: json!(crypto::seal_payload_for_test(
                    &json!({"id": report, "title": "Wakes stopped"}).to_string(),
                    &self.public_key
                )),
                received_at: format!("2026-10-18T20:00:{id:02}.000Z"),
                encrypted: true,
                endpoint_id: Some(endpoint_id.into()),
                delivery_id: Some(format!("bug-report:{report}")),
                verification: Some("first-party".into()),
                sender: None,
            }
        }
    }

    async fn count(db: &crate::storage::LocalDb, sql: &str) -> i64 {
        db.query_opt_i64(sql, ()).await.unwrap().unwrap_or(0)
    }

    /// A GitHub event that fails at `process_event` on every attempt is
    /// recorded and passed, and a bug event queued behind it on the shared
    /// cursor is still delivered and minted.
    #[tokio::test]
    async fn a_persistently_failing_github_event_does_not_park_the_bug_event_behind_it() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let poison = feed.poison_github_event(1);
        let poison_cursor = event_cursor(&poison);

        // Delivered alone first, the way a page boundary could leave it.
        let cursor = apply_page(&orch, vec![poison], &feed.ctx()).await.unwrap();
        assert_eq!(cursor.as_deref(), Some(poison_cursor.as_str()));
        let creds = credentials::get_github_credentials(&db).await.unwrap();
        assert_eq!(
            creds.last_event_sync.as_deref(),
            Some(poison_cursor.as_str())
        );
        assert_eq!(creds.relay_health_state.as_deref(), Some("skipping"));
        assert_eq!(
            creds.relay_health_reason.as_deref(),
            Some("relay_event_processing_failed")
        );
        assert_eq!(creds.relay_failing_event_id.as_deref(), Some("1"));
        assert_eq!(
            creds.relay_failing_event_at.as_deref(),
            Some("2026-10-18T20:00:01.000Z")
        );
        assert_eq!(creds.relay_undeliverable_since_healthy, 1);
        assert!(creds.relay_first_failure_at.is_some());
        // Skipping is not a key failure: the channel's failure streak is untouched.
        assert_eq!(creds.relay_consecutive_failures, None);

        // The same poison again, then a bug report, on one page.
        let bug = feed.bug_event(3, "src_bugs", 7);
        let bug_cursor = event_cursor(&bug);
        let cursor = apply_page(&orch, vec![feed.poison_github_event(2), bug], &feed.ctx())
            .await
            .unwrap();
        assert_eq!(cursor.as_deref(), Some(bug_cursor.as_str()));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE status = 'ok' AND delivery_id = 'bug-report:7'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'"
            )
            .await,
            1
        );
        let creds = credentials::get_github_credentials(&db).await.unwrap();
        assert_eq!(creds.last_event_sync.as_deref(), Some(bug_cursor.as_str()));
        assert_eq!(creds.relay_health_state.as_deref(), Some("healthy"));
        assert_eq!(creds.relay_undeliverable_since_healthy, 0);
        assert!(creds.relay_failing_event_id.is_none());
        assert!(creds.relay_last_successful_delivery_at.is_some());
    }

    /// An endpoint event this install cannot deliver is recorded undeliverable,
    /// counted, and passed.
    #[tokio::test]
    async fn an_undeliverable_ingress_event_is_recorded_and_the_cursor_advances() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let stray = feed.bug_event(4, "src_unknown", 8);
        let stray_cursor = event_cursor(&stray);

        let cursor = apply_page(&orch, vec![stray], &feed.ctx()).await.unwrap();

        assert_eq!(cursor.as_deref(), Some(stray_cursor.as_str()));
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM ingress_events WHERE status = 'undeliverable' AND endpoint_id = 'src_unknown'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM fact_log WHERE source = 'ingress'"
            )
            .await,
            0
        );
        let creds = credentials::get_github_credentials(&db).await.unwrap();
        assert_eq!(
            creds.last_event_sync.as_deref(),
            Some(stray_cursor.as_str())
        );
        assert_eq!(creds.relay_health_state.as_deref(), Some("skipping"));
        assert_eq!(
            creds.relay_health_reason.as_deref(),
            Some("ingress_unknown_endpoint")
        );
        assert_eq!(creds.relay_undeliverable_since_healthy, 1);
    }

    /// A key mismatch still pauses the whole channel: every later event is
    /// unreadable too, so the cursor must not move past it.
    #[tokio::test]
    async fn a_key_mismatch_still_pauses_the_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = crate::routes::fixture::orchestrator(temp.path(), &[]).await;
        let feed = SealedFeed::new(&db).await;
        let (other_public, _) = crypto::generate_keypair();
        let mut foreign = feed.bug_event(5, "src_bugs", 9);
        foreign.payload = json!(crypto::seal_payload_for_test("{}", &other_public));

        let error = apply_page(
            &orch,
            vec![foreign, feed.bug_event(6, "src_bugs", 10)],
            &feed.ctx(),
        )
        .await
        .unwrap_err();

        assert!(error.contains("key mismatch"), "{error}");
        let creds = credentials::get_github_credentials(&db).await.unwrap();
        assert_eq!(creds.last_event_sync, None);
        assert_eq!(creds.relay_health_state.as_deref(), Some("suspect"));
        assert_eq!(count(&db, "SELECT COUNT(*) FROM ingress_events").await, 0);
    }

    #[test]
    fn failure_backoff_grows_exponentially_and_is_capped() {
        assert_eq!(failure_backoff(1), Duration::from_secs(15));
        assert_eq!(failure_backoff(2), Duration::from_secs(30));
        assert_eq!(failure_backoff(3), Duration::from_secs(60));
        assert_eq!(failure_backoff(7), MAX_FAILURE_BACKOFF);
        assert_eq!(failure_backoff(100), MAX_FAILURE_BACKOFF);
    }
}
