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

#[derive(Debug, Deserialize)]
struct RelayResponse {
    events: Vec<RelayEvent>,
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

fn comment_fact(comment: &CommentTrigger, project: &str) -> RouteFact {
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
        comment_fact(&comment, &project.key),
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
    let mut request = reqwest::Client::new()
        .get(format!(
            "{}/events/{channel_id}",
            relay_url.trim_end_matches('/')
        ))
        .bearer_auth(secret);
    if let Some(cursor) = creds.last_event_sync.as_deref() {
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
        let cursor = encode_cursor(&event.received_at, event.id);
        if event.encrypted {
            let payload = event
                .payload
                .as_str()
                .ok_or_else(|| "encrypted relay payload was not a string".to_string())
                .and_then(|encrypted| crypto::decrypt_payload(encrypted, &private_key))
                .and_then(|payload| {
                    serde_json::from_str(&payload)
                        .map_err(|error| format!("invalid GitHub webhook payload: {error}"))
                });
            match payload {
                Ok(payload) => event.payload = payload,
                Err(error) => {
                    log::warn!(
                        "Skipping malformed GitHub relay event {}: {error}",
                        event.id
                    );
                    credentials::update_github_credentials(&orch.db.local, |creds| {
                        creds.last_event_sync = Some(cursor);
                    })
                    .await?;
                    continue;
                }
            }
        }
        process_event(orch, &event.event_type, &event.payload, &app_slug).await?;
        credentials::update_github_credentials(&orch.db.local, |creds| {
            creds.last_event_sync = Some(cursor);
        })
        .await?;
    }
    Ok(())
}

pub fn spawn_relay_sync(orch: Orchestrator, relay_url: &'static str) {
    tokio::spawn(async move {
        loop {
            if let Err(error) = sync_once(&orch, relay_url).await {
                log::warn!("GitHub relay sync failed: {error}");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
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
}
