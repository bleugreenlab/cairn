//! GitHub relay catch-up and conversion into declarative route facts.

use std::{collections::BTreeMap, path::Path, time::Duration};

use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    github::{credentials, crypto},
    orchestrator::Orchestrator,
    projects::remote::find_project_by_remote_full_name,
    routes::{Presence, RouteContext, RouteFact},
};

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

#[derive(Debug, Deserialize)]
pub struct RelayEvent {
    pub id: i64,
    pub event_type: String,
    pub payload: Value,
    pub received_at: String,
    #[serde(default)]
    pub encrypted: bool,
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

fn record_successful_delivery(creds: &mut credentials::GitHubCredentials, now: &str) {
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

fn comment_fact(
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
    let context = format!(
        "Repository: {}\nGitHub {}: #{}\nAuthor: @{}\nURL: {}\n\n{}",
        comment.repository, comment.kind, comment.number, comment.author, comment.url, comment.body
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

pub async fn process_event(
    orch: &Orchestrator,
    event_type: &str,
    payload: &Value,
    app_slug: &str,
) -> Result<(), String> {
    let TriggerDecision::Comment(comment) = trigger_for_event(event_type, payload, app_slug) else {
        return Ok(());
    };
    let Some(project) = find_project_by_remote_full_name(
        &orch.db.local,
        orch.services.git.as_ref(),
        &comment.repository,
    )
    .await
    .map_err(|error| error.to_string())?
    else {
        return Ok(());
    };
    let presence = crate::channels::operator_presence_status().await.presence;
    let submissions = crate::routes::dispatch(
        orch,
        comment_fact(
            &comment,
            &project.key,
            crate::routes::installation_machine_origin(orch)?,
        ),
        if presence == crate::channels::OperatorPresence::Present {
            Presence::Active
        } else {
            Presence::Away
        },
        RouteContext {
            project_id: Some(&project.id),
            project_path: Some(Path::new(&project.repo_path)),
        },
    )
    .await?;
    for submission in submissions {
        if let Err(submission) = crate::channels::submit_route(submission) {
            crate::routes::record_channel_outcome(
                orch,
                &submission,
                Err("channel runtime is unavailable".into()),
            )
            .await?;
        }
    }
    Ok(())
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

        for mut event in response.events {
            let event_cursor = event_cursor(&event);
            let mut decrypted = false;
            if event.encrypted {
                match decrypt_event_payload(&event.payload, &private_key) {
                    Ok(payload) => {
                        event.payload = payload;
                        decrypted = true;
                    }
                    Err(RelayPayloadError::KeyMismatch) => {
                        if is_dead_epoch_event(&event, relay_key_rotated_at.as_deref()) {
                            log::warn!(
                                "skipping dead-epoch relay event {} (pre-rotation)",
                                event.id
                            );
                            credentials::update_github_credentials(&orch.db.local, |creds| {
                                creds.last_event_sync = Some(event_cursor.clone());
                            })
                            .await?;
                            cursor = Some(event_cursor);
                            continue;
                        }
                        let now = chrono::Utc::now().to_rfc3339();
                        credentials::update_github_credentials(&orch.db.local, |creds| {
                            record_key_mismatch(creds, &event, &now);
                        })
                        .await?;
                        return Err(format!(
                            "relay key mismatch at event {}; cursor remains paused",
                            event.id
                        ));
                    }
                    Err(RelayPayloadError::Malformed(error)) => {
                        log::warn!(
                            "Skipping malformed GitHub relay event {}: {error}",
                            event.id
                        );
                        credentials::update_github_credentials(&orch.db.local, |creds| {
                            creds.last_event_sync = Some(event_cursor.clone());
                        })
                        .await?;
                        cursor = Some(event_cursor);
                        continue;
                    }
                }
            }
            process_event(orch, &event.event_type, &event.payload, &app_slug).await?;
            let now = chrono::Utc::now().to_rfc3339();
            credentials::update_github_credentials(&orch.db.local, |creds| {
                creds.last_event_sync = Some(event_cursor.clone());
                if decrypted {
                    record_successful_delivery(creds, &now);
                }
            })
            .await?;
            cursor = Some(event_cursor);
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

    fn comment(author: &str, author_type: &str, body: &str) -> Value {
        json!({
            "action": "created",
            "repository": {"full_name": "cairn/cairn"},
            "issue": {"number": 42, "pull_request": {"url": "https://api.github.test/pulls/42"}},
            "comment": {"id": 99, "body": body, "html_url": "https://github.test/cairn/cairn/pull/42#issuecomment-99", "user": {"login": author, "type": author_type}}
        })
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

    #[test]
    fn failure_backoff_grows_exponentially_and_is_capped() {
        assert_eq!(failure_backoff(1), Duration::from_secs(15));
        assert_eq!(failure_backoff(2), Duration::from_secs(30));
        assert_eq!(failure_backoff(3), Duration::from_secs(60));
        assert_eq!(failure_backoff(7), MAX_FAILURE_BACKOFF);
        assert_eq!(failure_backoff(100), MAX_FAILURE_BACKOFF);
    }
}
