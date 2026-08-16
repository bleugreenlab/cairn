use cairn_db::turso::{params, Row};

use crate::storage::{DbResult, LocalDb, RowExt};

const OUTBOUND_COLUMNS: &str = "id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, options_json, status, provider_guid, caption_guid, created_at, sent_at, resolved_at, last_error";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOutbound<'a> {
    pub id: &'a str,
    pub channel: &'a str,
    pub kind: &'a str,
    pub binding_ref: &'a str,
    pub conversation: &'a str,
    pub job_id: Option<&'a str>,
    pub rendered_text: &'a str,
    pub rendering: &'a str,
    pub created_at: i64,
}

/// Loads question choices staged by one channel conversation. Staged choices are
/// deliberately kept in `channel_outbound`; only a complete response is allowed
/// to enter the canonical prompt-level resolution table.
pub async fn staged_answers_for_prompt(
    db: &LocalDb,
    channel: &str,
    conversation: &str,
    prompt_id: &str,
) -> Result<Vec<(usize, String)>, String> {
    let prefix = format!("{prompt_id}:%");
    db.read(|conn| {
        let channel = channel.to_string();
        let conversation = conversation.to_string();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT binding_ref, json_extract(options_json, '$.answer')
                       FROM channel_outbound
                      WHERE channel = ?1 AND conversation = ?2
                        AND binding_ref LIKE ?3 AND kind = 'question'
                        AND status = 'resolved'
                        AND json_extract(options_json, '$.answer') IS NOT NULL
                   ORDER BY binding_ref",
                    params![channel, conversation, prefix],
                )
                .await?;
            let mut answers = Vec::new();
            while let Some(row) = rows.next().await? {
                let binding = row.text(0)?;
                let index = binding
                    .rsplit_once(':')
                    .and_then(|(_, value)| value.parse().ok())
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!("invalid question binding: {binding}"))
                    })?;
                answers.push((index, row.text(1)?));
            }
            Ok(answers)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn resolution_for_action(
    db: &LocalDb,
    action_ref: &str,
) -> Result<Option<AskResolution>, String> {
    db.query_opt(
        "SELECT resolution_id, binding_ref, action_ref, kind, answer, winner_provider, winner_conversation, winner_surface, winner_actor, domain_resolved_at IS NOT NULL FROM channel_ask_resolution WHERE action_ref = ?1 ORDER BY binding_ref LIMIT 1",
        (action_ref.to_string(),),
        |row| Ok(AskResolution {
            resolution_id: row.text(0)?,
            binding_ref: row.text(1)?,
            action_ref: row.text(2)?,
            kind: row.text(3)?,
            answer: row.text(4)?,
            winner_provider: row.text(5)?,
            winner_conversation: row.text(6)?,
            winner_surface: row.text(7)?,
            winner_actor: row.text(8)?,
            domain_resolved: row.i64(9)? != 0,
        }),
    )
    .await
    .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveBinding {
    pub provider: String,
    pub conversation: String,
    pub target_uri: String,
    pub message_classes: i64,
}

/// List every persisted binding that currently carries at least one outbound
/// message class. This is a read-only registry projection; callers layer
/// provider configuration and runtime health onto it.
pub async fn list_active_bindings(db: &LocalDb) -> Result<Vec<ActiveBinding>, String> {
    db.query_all(
        "SELECT provider, conversation, target_uri, message_classes
           FROM channel_conversation_binding
          WHERE message_classes != 0
          ORDER BY provider, conversation, target_uri",
        (),
        |row| {
            Ok(ActiveBinding {
                provider: row.text(0)?,
                conversation: row.text(1)?,
                target_uri: row.text(2)?,
                message_classes: row.i64(3)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn record_cleanup_failure(db: &LocalDb, id: &str, error: &str) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET last_error = ?2 WHERE id = ?1 AND status = 'cleanup_pending'",
        params![id.to_string(), error.to_string()],
    )
    .await
    .map(|n| n == 1)
    .map_err(|error| error.to_string())
}

pub async fn pending_ask_actions(db: &LocalDb) -> Result<Vec<(String, String)>, String> {
    db.query_all(
        "SELECT action_ref, kind FROM channel_ask_action WHERE domain_resolved_at IS NULL ORDER BY action_ref",
        (),
        |row| Ok((row.text(0)?, row.text(1)?)),
    ).await.map_err(|error| error.to_string())
}

pub async fn answers_for_action(
    db: &LocalDb,
    action_ref: &str,
) -> Result<Vec<(String, String)>, String> {
    db.query_all(
        "SELECT binding_ref, answer FROM channel_ask_resolution WHERE action_ref = ?1 ORDER BY binding_ref",
        (action_ref.to_string(),),
        |row| Ok((row.text(0)?, row.text(1)?)),
    ).await.map_err(|error| error.to_string())
}

pub async fn try_lease_ask_action(
    db: &LocalDb,
    action_ref: &str,
    now: i64,
    lease_ms: i64,
) -> Result<Option<DomainActionLease>, String> {
    let token = uuid::Uuid::new_v4().to_string();
    let changed = db.execute(
        "UPDATE channel_ask_action SET lease_token = ?2, lease_until = ?3, attempt_count = attempt_count + 1, last_error = NULL WHERE action_ref = ?1 AND domain_resolved_at IS NULL AND (lease_until IS NULL OR lease_until <= ?4)",
        params![action_ref.to_string(), token.clone(), now + lease_ms, now],
    ).await.map_err(|error| error.to_string())?;
    if changed == 0 {
        return Ok(None);
    }

    db.query_opt(
        "SELECT action_ref, kind FROM channel_ask_action WHERE action_ref = ?1 AND lease_token = ?2",
        params![action_ref.to_string(), token.clone()],
        move |row| Ok(DomainActionLease { action_ref: row.text(0)?, kind: row.text(1)?, token: token.clone() }),
    ).await.map_err(|error| error.to_string())
}

pub async fn release_ask_action(
    db: &LocalDb,
    lease: &DomainActionLease,
    error: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_ask_action SET lease_token = NULL, lease_until = NULL, last_error = ?3 WHERE action_ref = ?1 AND lease_token = ?2 AND domain_resolved_at IS NULL",
        params![lease.action_ref.clone(), lease.token.clone(), error.to_string()],
    ).await.map(|n| n == 1).map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainActionLease {
    pub action_ref: String,
    pub kind: String,
    pub token: String,
}

pub async fn remove_home_relative_focus(db: &LocalDb, channel: &str) -> Result<bool, String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    db.execute(
        "DELETE FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND selected_at IS NOT NULL
            AND (target_uri = 'cairn:~' OR target_uri LIKE 'cairn:~/%')",
        params![channel.to_string(), conversation],
    )
    .await
    .map(|changed| changed > 0)
    .map_err(|error| error.to_string())
}

pub async fn lookup_conversation_target(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
) -> Result<Option<String>, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.query_opt(
        "SELECT target_uri FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND binding_kind = 'structural'",
        params![provider.to_string(), conversation],
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn set_message_classes(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    message_classes: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    db.execute(
        "UPDATE channel_conversation_binding SET message_classes = ?4
          WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
        params![
            provider.to_string(),
            conversation,
            uri,
            message_classes.max(0)
        ],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

fn canonical_persisted_target(uri: &str) -> Result<String, String> {
    let uri = cairn_common::uri::canonicalize_uri_identity(uri);
    if uri == "cairn:~" || uri.starts_with("cairn:~/") {
        return Err(format!(
            "home-relative channel target must be resolved before persistence: {uri}"
        ));
    }

    super::bindings::FollowTarget::parse(&uri)?;
    Ok(uri)
}

pub async fn latest_send_error(db: &LocalDb, channel: &str) -> Result<Option<String>, String> {
    db.query_opt(
        "SELECT last_error FROM channel_outbound WHERE channel = ?1 AND status = 'failed' AND rowid = (SELECT rowid FROM channel_outbound WHERE channel = ?1 ORDER BY created_at DESC, rowid DESC LIMIT 1)",
        (channel.to_string(),),
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

/// Claim the only provider attempt for an intent before crossing the external
/// side-effect boundary. `failed` is the durable ambiguous-outcome state: if the
/// process dies after the provider sends but before `mark_sent`, later sweeps do
/// not put the same intent on the wire again.
pub async fn begin_delivery(db: &LocalDb, id: &str) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'failed', last_error = 'delivery outcome unknown' WHERE id = ?1 AND status = 'pending'",
        (id.to_string(),),
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn set_pending_route_submission(
    db: &LocalDb,
    id: &str,
    submission_json: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND kind = 'route' AND status = 'pending'",
        params![id.to_string(), submission_json.to_string()],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn is_target_followed(db: &LocalDb, channel: &str, uri: &str) -> Result<bool, String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    let uri = canonical_persisted_target(uri)?;
    db.query_opt_i64(
        "SELECT 1 FROM channel_conversation_binding WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3 AND binding_kind = 'follow' AND message_classes != 0",
        params![channel.to_string(), conversation, uri],
    ).await.map(|value| value.is_some()).map_err(|error| error.to_string())
}

pub async fn is_bound(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    binding_kind: &str,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    db.query_opt_i64(
        "SELECT 1 FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3 AND binding_kind = ?4",
        params![
            provider.to_string(),
            conversation,
            uri,
            binding_kind.to_string()
        ],
    )
    .await
    .map(|value| value.is_some())
    .map_err(|error| error.to_string())
}

/// Claims provider cleanup for any sent outbound bubble. Question cleanup uses
/// the stricter helper above because it also carries answer-resolution meaning;
/// thread-update retraction only needs the exactly-once side-effect claim.
pub async fn claim_outbound_cleanup(
    db: &LocalDb,
    id: &str,
    resolved_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE id = ?1 AND status = 'sent'",
        params![id.to_string(), resolved_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn advance_follow_cursor(
    db: &LocalDb,
    channel: &str,
    uri: &str,
    cursor_rowid: i64,
) -> Result<(), String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    advance_binding_cursor(db, channel, &conversation, uri, cursor_rowid).await
}

pub async fn advance_binding_cursor(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    cursor_rowid: i64,
) -> Result<(), String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    db.execute(
        "UPDATE channel_conversation_binding SET cursor_rowid = MAX(cursor_rowid, ?4)
          WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
        params![provider.to_string(), conversation, uri, cursor_rowid.max(0)],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Follow {
    pub uri: String,
    pub followed_at: i64,
    pub cursor_rowid: i64,
}

pub async fn follow_target(
    db: &LocalDb,
    channel: &str,
    uri: &str,
    followed_at: i64,
    cursor_rowid: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    bind_target(
        db,
        channel,
        &conversation,
        uri,
        super::bindings::BindingKind::Follow,
        super::bindings::MESSAGE_CLASSES_ALL,
        followed_at,
        cursor_rowid,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn bind_target(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    binding_kind: super::bindings::BindingKind,
    message_classes: i64,
    followed_at: i64,
    cursor_rowid: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    db.execute(
        "INSERT OR IGNORE INTO channel_conversation_binding
          (provider, conversation, target_uri, binding_kind, message_classes, followed_at, cursor_rowid)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            provider.to_string(),
            conversation,
            uri,
            binding_kind.as_str(),
            message_classes.max(0),
            followed_at,
            cursor_rowid.max(0)
        ],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// Move a follow (and the focus, when it points there) onto the canonical URI
/// for the same target, merging with a canonical row that already exists.
///
/// A follow's URI is its identity everywhere downstream: it is the primary key
/// here, the poll's checkmark lookup, and the prefix of the `binding_ref` that
/// makes stream delivery once-only. So an alias and its canonical form are two
/// identities for one conversation — the poll shows the target unfollowed while
/// it is streaming, following it again adds a second row, and every update then
/// arrives twice under two distinct binding refs. Canonicalizing the row is what
/// keeps one target to one identity.
///
/// The merged cursor is the LATER of the two. Events between the two cursors
/// were already delivered under the row that had read further, so taking the
/// maximum leaves no gap and repeats nothing; the earlier `followed_at` is kept
/// because that is when the operator actually started following.
pub async fn canonicalize_follow(
    db: &LocalDb,
    channel: &str,
    from_uri: &str,
    to_uri: &str,
) -> Result<(), String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    canonicalize_binding(db, channel, &conversation, from_uri, to_uri).await
}

pub async fn canonicalize_binding(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    from_uri: &str,
    to_uri: &str,
) -> Result<(), String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let provider = provider.to_string();
    let from_uri = cairn_common::uri::canonicalize_uri_identity(from_uri);
    let to_uri = cairn_common::uri::canonicalize_uri_identity(to_uri);
    db.write(move |conn| {
        let provider = provider.clone();
        let conversation = conversation.clone();
        let from_uri = from_uri.clone();
        let to_uri = to_uri.clone();
        Box::pin(async move {
            let mut focus_rows = conn.query(
                "SELECT MAX(selected_at) FROM channel_conversation_binding
                  WHERE provider = ?1 AND conversation = ?2 AND target_uri IN (?3, ?4)",
                params![provider.as_str(), conversation.as_str(), from_uri.as_str(), to_uri.as_str()],
            ).await?;
            let selected_at = match focus_rows.next().await? {
                Some(row) => row.opt_i64(0)?,
                None => None,
            };
            conn.execute(
                "UPDATE channel_conversation_binding SET selected_at = NULL
                  WHERE provider = ?1 AND conversation = ?2 AND target_uri IN (?3, ?4)",
                params![provider.as_str(), conversation.as_str(), from_uri.as_str(), to_uri.as_str()],
            ).await?;
            conn.execute(
                "INSERT INTO channel_conversation_binding
                   (provider, conversation, target_uri, binding_kind, message_classes, followed_at, cursor_rowid, suppressed_updates, selected_at)
                 SELECT ?1, ?2, ?4, binding_kind, message_classes, followed_at, cursor_rowid, suppressed_updates, selected_at
                   FROM channel_conversation_binding
                  WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3
                 ON CONFLICT(provider, conversation, target_uri) DO UPDATE SET
                   cursor_rowid = MAX(cursor_rowid, excluded.cursor_rowid),
                   followed_at = MIN(followed_at, excluded.followed_at),
                   suppressed_updates = MAX(suppressed_updates, excluded.suppressed_updates),
                   selected_at = COALESCE(channel_conversation_binding.selected_at, excluded.selected_at)",
                params![provider.as_str(), conversation.as_str(), from_uri.as_str(), to_uri.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM channel_conversation_binding
                  WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
                params![provider.as_str(), conversation.as_str(), from_uri.as_str()],
            )
            .await?;
            if let Some(selected_at) = selected_at {
                conn.execute(
                    "UPDATE channel_conversation_binding SET selected_at = ?4
                      WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
                    params![provider.as_str(), conversation.as_str(), to_uri.as_str(), selected_at],
                ).await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn unfollow_target(db: &LocalDb, channel: &str, uri: &str) -> Result<bool, String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    unbind_target(db, channel, &conversation, uri, Some("follow")).await
}

pub async fn unbind_target(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    binding_kind: Option<&str>,
) -> Result<bool, String> {
    let provider = provider.to_string();
    let conversation = super::bindings::canonical_conversation(&provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    let binding_kind = binding_kind.map(str::to_string);
    db.write(|conn| {
        let provider = provider.clone();
        let conversation = conversation.clone();
        let uri = uri.clone();
        let binding_kind = binding_kind.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "DELETE FROM channel_conversation_binding
                      WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3
                        AND (?4 IS NULL OR binding_kind = ?4)",
                    params![
                        provider.as_str(),
                        conversation.as_str(),
                        uri.as_str(),
                        binding_kind.as_deref()
                    ],
                )
                .await?;
            conn.execute(
                "UPDATE channel_conversation_binding SET selected_at = ?4
                  WHERE provider = ?1 AND conversation = ?2 AND target_uri = (
                    SELECT target_uri FROM channel_conversation_binding
                     WHERE provider = ?1 AND conversation = ?2 AND binding_kind = 'follow'
                     ORDER BY followed_at DESC, target_uri ASC LIMIT 1
                  ) AND NOT EXISTS (
                    SELECT 1 FROM channel_conversation_binding
                     WHERE provider = ?1 AND conversation = ?2 AND selected_at IS NOT NULL
                  )",
                params![
                    provider.as_str(),
                    conversation.as_str(),
                    uri.as_str(),
                    chrono::Utc::now().timestamp_millis()
                ],
            )
            .await?;
            Ok(changed == 1)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_follows(db: &LocalDb, channel: &str) -> Result<Vec<Follow>, String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    list_stream_subscriptions(db, channel, &conversation).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleBinding {
    pub conversation: String,
    pub target_uri: String,
    pub binding_kind: String,
}

/// Enumerate every conversation whose binding carries `message_class` for the
/// canonical target. Each result becomes an independent outbound rendering.
/// Stable projection generation for fields that affect gate routing. This is one
/// small private-ledger query per channel sweep, independent of project count;
/// unrelated private database writes leave the value unchanged.
pub async fn binding_generation(db: &LocalDb) -> Result<String, String> {
    db.query_opt_text(
        "SELECT group_concat(encoded, char(30)) FROM (
           SELECT provider || char(31) || conversation || char(31) || target_uri || char(31) ||
                  binding_kind || char(31) || message_classes AS encoded
             FROM channel_conversation_binding
            ORDER BY provider, conversation, target_uri, binding_kind
         )",
        (),
    )
    .await
    .map(|value| value.unwrap_or_default())
    .map_err(|error| error.to_string())
}

pub async fn list_eligible_bindings(
    db: &LocalDb,
    provider: &str,
    target_uri: &str,
    message_class: i64,
) -> Result<Vec<EligibleBinding>, String> {
    let target_uri = canonical_persisted_target(target_uri)?;
    if message_class <= 0 || message_class & !super::bindings::MESSAGE_CLASSES_ALL != 0 {
        return Err(format!(
            "invalid channel message class bit: {message_class}"
        ));
    }
    db.query_all(
        "SELECT conversation, target_uri, binding_kind
           FROM channel_conversation_binding
          WHERE provider = ?1 AND target_uri = ?2
            AND (message_classes & ?3) != 0
          ORDER BY conversation, binding_kind",
        params![provider.to_string(), target_uri, message_class],
        |row| {
            Ok(EligibleBinding {
                conversation: row.text(0)?,
                target_uri: row.text(1)?,
                binding_kind: row.text(2)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_stream_subscriptions(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
) -> Result<Vec<Follow>, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.query_all(
        "SELECT target_uri, followed_at, cursor_rowid FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND binding_kind = 'follow' AND message_classes != 0
          ORDER BY followed_at DESC, target_uri ASC",
        params![provider.to_string(), conversation],
        |row| {
            Ok(Follow {
                uri: row.text(0)?,
                followed_at: row.i64(1)?,
                cursor_rowid: row.i64(2)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn set_focus(
    db: &LocalDb,
    channel: &str,
    uri: &str,
    selected_at: i64,
) -> Result<(), String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    select_focus(db, channel, &conversation, uri, selected_at).await
}

pub async fn select_focus(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    selected_at: i64,
) -> Result<(), String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    let provider = provider.to_string();
    db.write(move |conn| {
        let provider = provider.clone();
        let conversation = conversation.clone();
        let uri = uri.clone();
        Box::pin(async move {
        conn.execute(
            "INSERT OR IGNORE INTO channel_conversation_binding (provider, conversation, target_uri, binding_kind, message_classes, followed_at, cursor_rowid) VALUES (?1, ?2, ?3, 'follow', 0, 0, 0)",
            params![provider.as_str(), conversation.as_str(), uri.as_str()],
        ).await?;
        conn.execute(
            "UPDATE channel_conversation_binding SET selected_at = NULL
              WHERE provider = ?1 AND conversation = ?2 AND selected_at IS NOT NULL",
            params![provider.as_str(), conversation.as_str()],
        ).await?;
        let changed = conn.execute(
            "UPDATE channel_conversation_binding SET selected_at = ?4
              WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3 AND binding_kind = 'follow'",
            params![provider.as_str(), conversation.as_str(), uri.as_str(), selected_at],
        ).await?;
        if changed != 1 {
            return Err(crate::storage::DbError::internal("focused target is not followed in this conversation"));
        }
        Ok(())
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn get_focus(db: &LocalDb, channel: &str) -> Result<Option<String>, String> {
    let conversation = super::bindings::legacy_conversation(channel)?;
    get_conversation_focus(db, channel, &conversation).await
}

pub async fn get_conversation_focus(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
) -> Result<Option<String>, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    db.query_opt(
        "SELECT target_uri FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND selected_at IS NOT NULL",
        params![provider.to_string(), conversation],
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

/// Claims an inbound answer against a still-sent rendering. This legacy rendering
/// race is independent of the canonical domain-action lease.
pub async fn claim_question_answer(
    db: &LocalDb,
    id: &str,
    answer: &str,
    resolved_at: i64,
) -> Result<bool, String> {
    let answer_json = serde_json::json!({ "answer": answer }).to_string();
    db.execute(
        "UPDATE channel_outbound SET status = 'resolved', options_json = ?2, resolved_at = ?3, last_error = NULL WHERE id = ?1 AND kind = 'question' AND status = 'sent'",
        params![id.to_string(), answer_json, resolved_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

/// Preserves an inbound answer when a simultaneous live-snapshot cleanup claimed
/// resolution first. Only the first inbound answer may fill the resolved row.
pub async fn record_answer_after_cleanup_claim(
    db: &LocalDb,
    id: &str,
    answer: &str,
) -> Result<bool, String> {
    let answer_json = serde_json::json!({ "answer": answer }).to_string();
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND kind = 'question' AND status = 'resolved' AND json_extract(options_json, '$.answer') IS NULL",
        params![id.to_string(), answer_json],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// Claims ownership of question cleanup while recording its durable resolution.
/// Only the path that observes `sent` may perform provider side effects.
pub async fn claim_question_cleanup(
    db: &LocalDb,
    id: &str,
    resolved_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE id = ?1 AND kind = 'question' AND status = 'sent'",
        params![id.to_string(), resolved_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn mark_expired(db: &LocalDb, id: &str, expired_at: i64) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'expired', resolved_at = ?2 WHERE id = ?1 AND status = 'pending'",
        params![id.to_string(), expired_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRecord {
    pub id: String,
    pub channel: String,
    pub kind: String,
    pub binding_ref: String,
    pub conversation: String,
    pub job_id: Option<String>,
    pub rendered_text: String,
    pub rendering: String,
    pub options_json: Option<String>,
    pub status: String,
    pub provider_guid: Option<String>,
    pub caption_guid: Option<String>,
    pub created_at: i64,
    pub sent_at: Option<i64>,
    pub resolved_at: Option<i64>,
    pub last_error: Option<String>,
}

impl OutboundRecord {
    fn from_row(row: &Row) -> DbResult<Self> {
        Ok(Self {
            id: row.text(0)?,
            channel: row.text(1)?,
            kind: row.text(2)?,
            binding_ref: row.text(3)?,
            conversation: row.text(4)?,
            job_id: row.opt_text(5)?,
            rendered_text: row.text(6)?,
            rendering: row.text(7)?,
            options_json: row.opt_text(8)?,
            status: row.text(9)?,
            provider_guid: row.opt_text(10)?,
            caption_guid: row.opt_text(11)?,
            created_at: row.i64(12)?,
            sent_at: row.opt_i64(13)?,
            resolved_at: row.opt_i64(14)?,
            last_error: row.opt_text(15)?,
        })
    }
}

/// Insert a pending delivery intent. Returns `false` when the channel/kind/binding
/// uniqueness fence proves the gate was already observed by an earlier sweep.
pub async fn insert_intent(db: &LocalDb, intent: &NewOutbound<'_>) -> Result<bool, String> {
    let binding_ref = cairn_common::uri::canonicalize_uri_identity(intent.binding_ref);
    let intent = NewOutbound {
        id: intent.id,
        channel: intent.channel,
        kind: intent.kind,
        binding_ref: intent.binding_ref,
        conversation: intent.conversation,
        job_id: intent.job_id,
        rendered_text: intent.rendered_text,
        rendering: intent.rendering,
        created_at: intent.created_at,
    };
    db.execute(
        "INSERT OR IGNORE INTO channel_outbound (id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, status, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', ?9)",
        params![intent.id, intent.channel, intent.kind, binding_ref, intent.conversation, intent.job_id, intent.rendered_text, intent.rendering, intent.created_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn get_by_conversation_binding(
    db: &LocalDb,
    channel: &str,
    conversation: &str,
    kind: &str,
    action_ref: &str,
) -> Result<Option<OutboundRecord>, String> {
    let action_ref = cairn_common::uri::canonicalize_uri_identity(action_ref);
    db.query_opt(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND conversation = ?2 AND kind = ?3 AND binding_ref = ?4"),
        params![channel.to_string(), conversation.to_string(), kind.to_string(), action_ref],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn get_by_binding(
    db: &LocalDb,
    channel: &str,
    kind: &str,
    binding_ref: &str,
) -> Result<Option<OutboundRecord>, String> {
    let binding_ref = cairn_common::uri::canonicalize_uri_identity(binding_ref);
    db.query_opt(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND kind = ?2 AND binding_ref = ?3"),
        params![channel.to_string(), kind.to_string(), binding_ref],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

/// Resolve either identifier a provider may expose as the reply target.
pub async fn get_by_provider_guid(
    db: &LocalDb,
    channel: &str,
    guid: &str,
) -> Result<Option<OutboundRecord>, String> {
    db.query_opt(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND (provider_guid = ?2 OR caption_guid = ?2)"),
        params![channel.to_string(), guid.to_string()],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn list_unresolved(db: &LocalDb, channel: &str) -> Result<Vec<OutboundRecord>, String> {
    db.query_all(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND status IN ('pending', 'sent', 'failed') ORDER BY created_at"),
        params![channel.to_string()],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn mark_sent(
    db: &LocalDb,
    id: &str,
    provider_guid: &str,
    caption_guid: Option<&str>,
    options_json: Option<&str>,
    sent_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'sent', provider_guid = ?2, caption_guid = ?3, options_json = ?4, sent_at = ?5, last_error = NULL WHERE id = ?1 AND status IN ('pending', 'failed')",
        params![id.to_string(), provider_guid.to_string(), caption_guid.map(str::to_string), options_json.map(str::to_string), sent_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn update_options(db: &LocalDb, id: &str, options_json: &str) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND status = 'sent'",
        params![id.to_string(), options_json.to_string()],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskResolution {
    pub resolution_id: String,
    pub binding_ref: String,
    pub action_ref: String,
    pub kind: String,
    pub answer: String,
    pub winner_provider: String,
    pub winner_conversation: String,
    pub winner_surface: String,
    pub winner_actor: String,
    pub domain_resolved: bool,
}

impl AskResolution {
    pub fn resolution_provenance(
        &self,
    ) -> Result<crate::turns::queries::ResolutionProvenance, String> {
        let surface =
            serde_json::from_value(serde_json::Value::String(self.winner_surface.clone()))
                .map_err(|error| {
                    format!(
                        "invalid stored ask winner surface {:?}: {error}",
                        self.winner_surface
                    )
                })?;
        let present = |value: &str| (!value.is_empty()).then(|| value.to_string());
        Ok(crate::turns::queries::ResolutionProvenance {
            id: self.resolution_id.clone(),
            surface,
            provider: present(&self.winner_provider),
            conversation: present(&self.winner_conversation),
            actor: present(&self.winner_actor),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AskClaim {
    Won(AskResolution),
    Existing(AskResolution),
}

/// Globally claims a canonical ask before any provider invokes its domain action.
/// SQLite's primary-key insert is the serialization point across all surfaces.
///
/// The explicit provider, conversation, ask, and action identities are the
/// transaction boundary; grouping them would make accidental cross-surface
/// aliasing easier rather than clarifying this write.
#[allow(clippy::too_many_arguments)]
pub async fn claim_ask_resolution(
    db: &LocalDb,
    binding_ref: &str,
    answer: &str,
    transport: cairn_common::identity::AppearanceTransport,
    provider: Option<&str>,
    conversation: Option<&str>,
    actor: Option<&str>,
    kind: &str,
    action_ref: &str,
    resolved_at: i64,
) -> Result<AskClaim, String> {
    let surface = serde_json::to_string(&transport)
        .map_err(|error| error.to_string())?
        .trim_matches('"')
        .to_string();
    if matches!(
        transport,
        cairn_common::identity::AppearanceTransport::ChannelReply
            | cairn_common::identity::AppearanceTransport::ResourcePatch
    ) && actor.is_none_or(|actor| actor.trim().is_empty())
    {
        return Err(format!(
            "{surface} resolution requires an authenticated actor"
        ));
    }
    let binding_ref = cairn_common::uri::canonicalize_uri_identity(binding_ref);
    let action_ref = cairn_common::uri::canonicalize_uri_identity(action_ref);
    let binding_for_insert = binding_ref.clone();
    let inserted = db.write(move |conn| {
        let binding_ref = binding_for_insert.clone();
        let action_ref = action_ref.clone();
        let answer = answer.to_string();
        let provider = provider.unwrap_or_default().to_string();
        let conversation = conversation.unwrap_or_default().to_string();
        let actor = actor.unwrap_or_default().to_string();
        let surface = surface.clone();
        let resolution_id = uuid::Uuid::new_v4().to_string();
        let kind = kind.to_string();
        Box::pin(async move {
            conn.execute("INSERT OR IGNORE INTO channel_ask_action (action_ref, kind) VALUES (?1, ?2)", params![action_ref.clone(), kind.clone()]).await?;
            Ok(conn.execute(
                "INSERT OR IGNORE INTO channel_ask_resolution (binding_ref, action_ref, kind, answer, winner_provider, winner_conversation, resolved_at, resolution_id, winner_surface, winner_actor) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![binding_ref, action_ref, kind, answer, provider, conversation, resolved_at, resolution_id, surface, actor],
            ).await?)
        })
    }).await.map_err(|error| error.to_string())? == 1;
    let resolution = db.query_opt(
        "SELECT resolution_id, binding_ref, action_ref, kind, answer, winner_provider, winner_conversation, winner_surface, winner_actor, domain_resolved_at IS NOT NULL FROM channel_ask_resolution WHERE binding_ref = ?1",
        (binding_ref,),
        |row| Ok(AskResolution {
            resolution_id: row.text(0)?,
            binding_ref: row.text(1)?,
            action_ref: row.text(2)?,
            kind: row.text(3)?,
            answer: row.text(4)?,
            winner_provider: row.text(5)?,
            winner_conversation: row.text(6)?,
            winner_surface: row.text(7)?,
            winner_actor: row.text(8)?,
            domain_resolved: row.i64(9)? != 0,
        }),
    ).await.map_err(|error| error.to_string())?
        .ok_or_else(|| "ask resolution claim disappeared".to_string())?;
    Ok(if inserted {
        AskClaim::Won(resolution)
    } else {
        AskClaim::Existing(resolution)
    })
}

/// Records successful domain resolution and fans the winning receipt out to all
/// provider renderings. Offline providers discover these rows on their next sweep.
pub async fn finalize_ask_resolution(
    db: &LocalDb,
    action_ref: &str,
    receipt: &str,
    resolved_at: i64,
) -> Result<u64, String> {
    let action_ref = cairn_common::uri::canonicalize_uri_identity(action_ref);
    let options = serde_json::json!({ "receipt": receipt }).to_string();
    db.write(move |conn| {
        let action_ref = action_ref.clone();
        let options = options.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE channel_ask_action SET domain_resolved_at = COALESCE(domain_resolved_at, ?2), lease_token = NULL, lease_until = NULL, last_error = NULL WHERE action_ref = ?1",
                params![action_ref.clone(), resolved_at],
            ).await?;
            conn.execute("UPDATE channel_ask_resolution SET domain_resolved_at = COALESCE(domain_resolved_at, ?2) WHERE action_ref = ?1", params![action_ref.clone(), resolved_at]).await?;
            let changed = conn.execute(
                "UPDATE channel_outbound SET status = 'cleanup_pending', options_json = ?2, resolved_at = ?3, last_error = NULL WHERE EXISTS (SELECT 1 FROM channel_ask_resolution resolution WHERE resolution.action_ref = ?1 AND (channel_outbound.binding_ref = resolution.binding_ref OR (resolution.kind = 'question' AND channel_outbound.binding_ref LIKE resolution.action_ref || ':%'))) AND status = 'sent'",
                params![action_ref.clone(), options, resolved_at],
            ).await?;
            conn.execute(
                "UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE EXISTS (SELECT 1 FROM channel_ask_resolution resolution WHERE resolution.action_ref = ?1 AND (channel_outbound.binding_ref = resolution.binding_ref OR (resolution.kind = 'question' AND channel_outbound.binding_ref LIKE resolution.action_ref || ':%'))) AND status IN ('pending', 'failed')",
                params![action_ref, resolved_at],
            ).await?;
            Ok(changed)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn list_cleanup_pending(
    db: &LocalDb,
    channel: &str,
) -> Result<Vec<OutboundRecord>, String> {
    db.query_all(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND status = 'cleanup_pending' ORDER BY created_at"),
        (channel.to_string(),),
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn acknowledge_cleanup(db: &LocalDb, id: &str) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'resolved', last_error = NULL WHERE id = ?1 AND status = 'cleanup_pending'",
        (id.to_string(),),
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn answered_for_prompt(
    db: &LocalDb,
    _channel: &str,
    prompt_id: &str,
) -> Result<Vec<(usize, String)>, String> {
    let prefix = format!("{prompt_id}:%");
    db.read(|conn| {
        let prefix = prefix.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT binding_ref, answer FROM (
                   SELECT binding_ref, answer FROM channel_ask_resolution WHERE binding_ref LIKE ?1
                   UNION
                   SELECT binding_ref, json_extract(options_json, '$.answer') AS answer
                     FROM channel_outbound
                    WHERE binding_ref LIKE ?1 AND kind = 'question' AND status = 'resolved'
                      AND json_extract(options_json, '$.answer') IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM channel_ask_resolution resolution
                         WHERE resolution.binding_ref = channel_outbound.binding_ref
                      )
                 ) ORDER BY binding_ref",
                    (prefix,),
                )
                .await?;
            let mut answers = Vec::new();
            while let Some(row) = rows.next().await? {
                let binding = row.text(0)?;
                let index = binding
                    .rsplit_once(':')
                    .and_then(|(_, value)| value.parse().ok())
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!("invalid question binding: {binding}"))
                    })?;
                answers.push((index, row.text(1)?));
            }
            Ok(answers)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn mark_resolved(db: &LocalDb, id: &str, resolved_at: i64) -> Result<bool, String> {
    db.execute("UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE id = ?1 AND status != 'resolved'", params![id.to_string(), resolved_at])
        .await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

/// Abandons every intent this channel never delivered. The channel is a live tap
/// on the ask event, so an intent that had not reached the provider by the time
/// the session ended is dead rather than owed: re-sending it after a restart
/// texts the operator prompts their agents have long since moved past. `sent`
/// rows are deliberately left alone, because a reply to an ask that DID go out
/// still binds to its provider GUID however long the operator takes.
pub async fn expire_undelivered(
    db: &LocalDb,
    channel: &str,
    expired_at: i64,
) -> Result<u64, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'expired', resolved_at = ?2 WHERE channel = ?1 AND status IN ('pending', 'failed')",
        params![channel.to_string(), expired_at],
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn mark_failed(db: &LocalDb, id: &str, error: &str) -> Result<bool, String> {
    db.execute("UPDATE channel_outbound SET status = 'failed', last_error = ?2 WHERE id = ?1 AND status != 'resolved'", params![id.to_string(), error.to_string()])
        .await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn get_cursor(db: &LocalDb, channel: &str) -> Result<Option<i64>, String> {
    db.query_opt_i64(
        "SELECT since_rowid FROM channel_cursor WHERE channel = ?1",
        params![channel.to_string()],
    )
    .await
    .map_err(|error| error.to_string())
}

/// Advance a cursor monotonically so duplicate or reordered watch events cannot
/// move restart recovery backwards.
pub async fn advance_cursor(db: &LocalDb, channel: &str, since_rowid: i64) -> Result<i64, String> {
    db.execute(
        "INSERT INTO channel_cursor (channel, since_rowid) VALUES (?1, ?2) ON CONFLICT(channel) DO UPDATE SET since_rowid = MAX(channel_cursor.since_rowid, excluded.since_rowid)",
        params![channel.to_string(), since_rowid],
    ).await.map_err(|error| error.to_string())?;
    get_cursor(db, channel)
        .await?
        .ok_or_else(|| "cursor upsert returned no row".to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRecord {
    pub id: String,
    pub channel: String,
    pub provider_guid: Option<String>,
    pub sender: String,
    pub text: String,
    pub received_at: i64,
    pub rejection_reason: Option<String>,
    pub acknowledged_at: Option<i64>,
}

impl InboundRecord {
    fn from_row(row: &Row) -> DbResult<Self> {
        Ok(Self {
            id: row.text(0)?,
            channel: row.text(1)?,
            provider_guid: row.opt_text(2)?,
            sender: row.text(3)?,
            text: row.text(4)?,
            received_at: row.i64(5)?,
            rejection_reason: row.opt_text(6)?,
            acknowledged_at: row.opt_i64(7)?,
        })
    }
}

pub async fn insert_inbound(db: &LocalDb, record: &InboundRecord) -> Result<bool, String> {
    db.execute(
        "INSERT OR IGNORE INTO channel_inbound (id, channel, provider_guid, sender, text, received_at, rejection_reason, acknowledged_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![record.id.clone(), record.channel.clone(), record.provider_guid.clone(), record.sender.clone(), record.text.clone(), record.received_at, record.rejection_reason.clone(), record.acknowledged_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn list_inbound(
    db: &LocalDb,
    channel: &str,
    limit: i64,
) -> Result<Vec<InboundRecord>, String> {
    db.query_all(
        "SELECT id, channel, provider_guid, sender, text, received_at, rejection_reason, acknowledged_at FROM channel_inbound WHERE channel = ?1 ORDER BY received_at DESC LIMIT ?2",
        params![channel.to_string(), limit.max(0)],
        InboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn mark_inbound_acknowledged(
    db: &LocalDb,
    id: &str,
    acknowledged_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_inbound SET acknowledged_at = COALESCE(acknowledged_at, ?2) WHERE id = ?1",
        params![id.to_string(), acknowledged_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    fn stored_winner(
        surface: &str,
        provider: &str,
        conversation: &str,
        actor: &str,
    ) -> AskResolution {
        AskResolution {
            resolution_id: "winner-resolution".to_string(),
            binding_ref: "prompt:0".to_string(),
            action_ref: "prompt".to_string(),
            kind: "question".to_string(),
            answer: "Winner".to_string(),
            winner_provider: provider.to_string(),
            winner_conversation: conversation.to_string(),
            winner_surface: surface.to_string(),
            winner_actor: actor.to_string(),
            domain_resolved: false,
        }
    }

    #[test]
    fn direct_lease_winner_executes_with_stored_claim_winner_provenance() {
        let provenance = stored_winner(
            "channel_reply",
            "telegram",
            "chat:winning",
            "sender:winning",
        )
        .resolution_provenance()
        .unwrap();

        assert_eq!(provenance.id, "winner-resolution");
        assert_eq!(
            provenance.surface,
            cairn_common::identity::AppearanceTransport::ChannelReply
        );
        assert_eq!(provenance.provider.as_deref(), Some("telegram"));
        assert_eq!(provenance.conversation.as_deref(), Some("chat:winning"));
        assert_eq!(provenance.actor.as_deref(), Some("sender:winning"));
    }

    #[test]
    fn recovery_lease_winner_preserves_stored_non_channel_provenance() {
        let provenance = stored_winner("resource_patch", "", "", "cairn://p/CAIRN/1/1/builder")
            .resolution_provenance()
            .unwrap();

        assert_eq!(
            provenance.surface,
            cairn_common::identity::AppearanceTransport::ResourcePatch
        );
        assert_eq!(provenance.provider, None);
        assert_eq!(provenance.conversation, None);
        assert_eq!(
            provenance.actor.as_deref(),
            Some("cairn://p/CAIRN/1/1/builder")
        );
    }

    #[tokio::test]
    async fn active_bindings_are_carrying_rows_in_canonical_order() {
        let db = migrated_test_db("channel-ledger-active-bindings.db").await;
        bind_target(
            &db,
            "telegram",
            "telegram:20",
            "cairn://p/cairn/general",
            super::super::bindings::BindingKind::Follow,
            super::super::bindings::MESSAGE_CLASS_NOTIFY,
            1,
            0,
        )
        .await
        .unwrap();
        bind_target(
            &db,
            "imessage",
            "imessage:a@example.com",
            "cairn://p/cairn/4026",
            super::super::bindings::BindingKind::Follow,
            super::super::bindings::MESSAGE_CLASS_QUESTION
                | super::super::bindings::MESSAGE_CLASS_PERMISSION,
            2,
            0,
        )
        .await
        .unwrap();
        bind_target(
            &db,
            "imessage",
            "imessage:a@example.com",
            "cairn://p/cairn/general",
            super::super::bindings::BindingKind::Follow,
            0,
            3,
            0,
        )
        .await
        .unwrap();

        assert_eq!(
            list_active_bindings(&db).await.unwrap(),
            vec![
                ActiveBinding {
                    provider: "imessage".into(),
                    conversation: "imessage:a@example.com".into(),
                    target_uri: "cairn://p/cairn/4026".into(),
                    message_classes: 3,
                },
                ActiveBinding {
                    provider: "telegram".into(),
                    conversation: "telegram:20".into(),
                    target_uri: "cairn://p/cairn/general".into(),
                    message_classes: 4,
                },
            ]
        );
    }

    #[tokio::test]
    async fn followed_state_reflects_follow_and_unfollow() {
        let db = migrated_test_db("channel-ledger-follow-state.db").await;
        let thread = "cairn://p/cairn/3404";

        assert!(!is_target_followed(&db, "imessage", thread).await.unwrap());
        assert!(follow_target(&db, "imessage", thread, 10, 20)
            .await
            .unwrap());
        assert!(is_target_followed(&db, "imessage", thread).await.unwrap());
        assert!(unfollow_target(&db, "imessage", thread).await.unwrap());
        assert!(!is_target_followed(&db, "imessage", thread).await.unwrap());
    }

    #[tokio::test]
    async fn home_relative_targets_never_cross_the_ledger_boundary() {
        let db = migrated_test_db("channel-ledger-relative-target.db").await;

        assert!(follow_target(&db, "imessage", "cairn:~/", 10, 20)
            .await
            .unwrap_err()
            .contains("must be resolved"));
        assert!(set_focus(&db, "imessage", "cairn:~", 10)
            .await
            .unwrap_err()
            .contains("must be resolved"));
        assert!(list_follows(&db, "imessage").await.unwrap().is_empty());
        assert_eq!(get_focus(&db, "imessage").await.unwrap(), None);
    }

    fn intent<'a>(id: &'a str) -> NewOutbound<'a> {
        NewOutbound {
            id,
            channel: "imessage",
            kind: "question",
            binding_ref: "prompt-1:0",
            conversation: "+15551234567",
            job_id: Some("job-1"),
            rendered_text: "Choose",
            rendering: "poll",
            created_at: 10,
        }
    }

    #[tokio::test]
    async fn latest_send_error_tracks_the_latest_provider_attempt() {
        let db = migrated_test_db("channel-ledger-latest-send-error.db").await;
        let mut failed = intent("failed");
        failed.created_at = 10;
        assert!(insert_intent(&db, &failed).await.unwrap());
        assert!(mark_failed(&db, "failed", "send failure").await.unwrap());
        assert_eq!(
            latest_send_error(&db, "imessage").await.unwrap().as_deref(),
            Some("send failure")
        );

        let mut sent = intent("sent");
        sent.binding_ref = "prompt-2:0";
        sent.created_at = 20;
        assert!(insert_intent(&db, &sent).await.unwrap());
        assert!(mark_sent(&db, "sent", "guid", None, None, 21)
            .await
            .unwrap());
        assert_eq!(latest_send_error(&db, "imessage").await.unwrap(), None);
        assert_eq!(latest_send_error(&db, "telegram").await.unwrap(), None);
    }

    #[tokio::test]
    async fn only_one_resolver_can_claim_question_cleanup() {
        let db = migrated_test_db("channel-ledger-cleanup-claim.db").await;
        assert!(insert_intent(&db, &intent("question")).await.unwrap());
        assert!(mark_sent(&db, "question", "guid", None, None, 11)
            .await
            .unwrap());

        let (first, second) = tokio::join!(
            claim_question_cleanup(&db, "question", 12),
            claim_question_cleanup(&db, "question", 12),
        );

        assert_ne!(first.unwrap(), second.unwrap());
    }

    #[tokio::test]
    async fn prompt_answers_ignore_cleanup_only_resolved_siblings() {
        let db = migrated_test_db("channel-ledger-mixed-question-resolution.db").await;
        let mut answered = intent("answered");
        answered.binding_ref = "prompt-1:0";
        let mut cleanup_only = intent("cleanup-only");
        cleanup_only.binding_ref = "prompt-1:1";
        assert!(insert_intent(&db, &answered).await.unwrap());
        assert!(insert_intent(&db, &cleanup_only).await.unwrap());
        assert!(mark_sent(&db, "answered", "guid-a", None, None, 11)
            .await
            .unwrap());
        assert!(mark_sent(&db, "cleanup-only", "guid-b", None, None, 11)
            .await
            .unwrap());
        assert!(claim_question_answer(&db, "answered", "Ship it", 12)
            .await
            .unwrap());
        assert!(claim_question_cleanup(&db, "cleanup-only", 12)
            .await
            .unwrap());

        assert_eq!(
            answered_for_prompt(&db, "imessage", "prompt-1")
                .await
                .unwrap(),
            vec![(0, "Ship it".into())]
        );
    }

    #[tokio::test]
    async fn first_answer_is_immutable_and_only_one_worker_holds_the_domain_lease() {
        let db = migrated_test_db("channel-ledger-domain-lease.db").await;
        let first = claim_ask_resolution(
            &db,
            "prompt:0",
            "First",
            cairn_common::identity::AppearanceTransport::ChannelReply,
            Some("imessage"),
            Some("phone"),
            Some("sender:first"),
            "question",
            "prompt",
            10,
        )
        .await
        .unwrap();
        assert!(matches!(first, AskClaim::Won(_)));
        let second = claim_ask_resolution(
            &db,
            "prompt:0",
            "Second",
            cairn_common::identity::AppearanceTransport::ChannelReply,
            Some("discord"),
            Some("room"),
            Some("sender:second"),
            "question",
            "prompt",
            11,
        )
        .await
        .unwrap();
        assert!(
            matches!(second, AskClaim::Existing(AskResolution { answer, .. }) if answer == "First")
        );

        let (a, b) = tokio::join!(
            try_lease_ask_action(&db, "prompt", 12, 100),
            try_lease_ask_action(&db, "prompt", 12, 100),
        );
        let leases = [a.unwrap(), b.unwrap()];
        assert_eq!(leases.iter().filter(|lease| lease.is_some()).count(), 1);
    }

    #[tokio::test]
    async fn concurrent_transports_share_one_winner_and_one_domain_lease() {
        let db = migrated_test_db("all-surface-ask-race.db").await;
        let (desktop, resource) = tokio::join!(
            claim_ask_resolution(
                &db,
                "prompt:0",
                "Desktop",
                cairn_common::identity::AppearanceTransport::AuthenticatedDesktop,
                None,
                None,
                None,
                "question",
                "prompt",
                10,
            ),
            claim_ask_resolution(
                &db,
                "prompt:0",
                "Resource",
                cairn_common::identity::AppearanceTransport::ResourcePatch,
                None,
                None,
                Some("cairn://p/CAIRN/1/1/builder"),
                "question",
                "prompt",
                10,
            ),
        );
        let claims = [desktop.unwrap(), resource.unwrap()];
        assert_eq!(
            claims
                .iter()
                .filter(|claim| matches!(claim, AskClaim::Won(_)))
                .count(),
            1
        );
        let answers = claims
            .iter()
            .map(|claim| match claim {
                AskClaim::Won(value) | AskClaim::Existing(value) => value.answer.as_str(),
            })
            .collect::<Vec<_>>();
        assert_eq!(answers[0], answers[1]);

        let (first, second) = tokio::join!(
            try_lease_ask_action(&db, "prompt", 11, 100),
            try_lease_ask_action(&db, "prompt", 11, 100),
        );
        assert_eq!(
            [first.unwrap(), second.unwrap()].iter().flatten().count(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_multi_question_submissions_share_one_complete_winner() {
        let db = migrated_test_db("multi-question-answer-race.db").await;
        let desktop_response = "Question 1: Desktop A\nQuestion 2: Desktop B";
        let resource_response = "Question 1: Resource A\nQuestion 2: Resource B";

        let (desktop, resource) = tokio::join!(
            claim_ask_resolution(
                &db,
                "prompt",
                desktop_response,
                cairn_common::identity::AppearanceTransport::AuthenticatedDesktop,
                None,
                None,
                None,
                "question",
                "prompt",
                10,
            ),
            claim_ask_resolution(
                &db,
                "prompt",
                resource_response,
                cairn_common::identity::AppearanceTransport::ResourcePatch,
                None,
                None,
                Some("cairn://p/CAIRN/1/1/builder"),
                "question",
                "prompt",
                10,
            ),
        );
        let claims = [desktop.unwrap(), resource.unwrap()];
        let winners = claims
            .iter()
            .map(|claim| match claim {
                AskClaim::Won(winner) | AskClaim::Existing(winner) => winner,
            })
            .collect::<Vec<_>>();

        assert_eq!(winners[0].answer, winners[1].answer);
        assert!(winners[0].answer == desktop_response || winners[0].answer == resource_response);
        assert_eq!(winners[0].resolution_id, winners[1].resolution_id);
        assert_eq!(winners[0].winner_surface, winners[1].winner_surface);
        assert!(matches!(
            winners[0].winner_surface.as_str(),
            "authenticated_desktop" | "resource_patch"
        ));
        assert_eq!(winners[0].winner_actor, winners[1].winner_actor);
    }

    #[tokio::test]
    async fn desktop_and_channel_share_one_complete_prompt_winner() {
        let db = migrated_test_db("desktop-channel-prompt-race.db").await;
        let desktop = "Question 1: Desktop A\nQuestion 2: Desktop B";
        let channel = "Question 1: Channel A\nQuestion 2: Channel B";

        let (desktop_claim, channel_claim) = tokio::join!(
            claim_ask_resolution(
                &db,
                "prompt",
                desktop,
                cairn_common::identity::AppearanceTransport::AuthenticatedDesktop,
                None,
                None,
                None,
                "question",
                "prompt",
                10,
            ),
            claim_ask_resolution(
                &db,
                "prompt",
                channel,
                cairn_common::identity::AppearanceTransport::ChannelReply,
                Some("imessage"),
                Some("phone"),
                Some("sender:channel"),
                "question",
                "prompt",
                10,
            ),
        );
        let claims = [desktop_claim.unwrap(), channel_claim.unwrap()];
        let winners = claims.map(|claim| match claim {
            AskClaim::Won(winner) | AskClaim::Existing(winner) => winner,
        });

        assert_eq!(winners[0].resolution_id, winners[1].resolution_id);
        assert_eq!(winners[0].answer, winners[1].answer);
        assert!(winners[0].answer == desktop || winners[0].answer == channel);
    }

    #[tokio::test]
    async fn two_channels_cannot_form_a_hybrid_multi_question_winner() {
        let db = migrated_test_db("two-channel-prompt-race.db").await;
        let imessage = "Question 1: iMessage A\nQuestion 2: iMessage B";
        let discord = "Question 1: Discord A\nQuestion 2: Discord B";

        let (imessage_claim, discord_claim) = tokio::join!(
            claim_ask_resolution(
                &db,
                "prompt",
                imessage,
                cairn_common::identity::AppearanceTransport::ChannelReply,
                Some("imessage"),
                Some("phone"),
                Some("sender:imessage"),
                "question",
                "prompt",
                10,
            ),
            claim_ask_resolution(
                &db,
                "prompt",
                discord,
                cairn_common::identity::AppearanceTransport::ChannelReply,
                Some("discord"),
                Some("room"),
                Some("sender:discord"),
                "question",
                "prompt",
                10,
            ),
        );
        let claims = [imessage_claim.unwrap(), discord_claim.unwrap()];
        let winners = claims.map(|claim| match claim {
            AskClaim::Won(winner) | AskClaim::Existing(winner) => winner,
        });

        assert_eq!(winners[0].resolution_id, winners[1].resolution_id);
        assert_eq!(winners[0].answer, winners[1].answer);
        assert!(winners[0].answer == imessage || winners[0].answer == discord);
    }

    #[tokio::test]
    async fn late_transport_returns_stored_winner_without_reopening_domain_action() {
        let db = migrated_test_db("all-surface-ask-late.db").await;
        claim_ask_resolution(
            &db,
            "permission",
            "allow",
            cairn_common::identity::AppearanceTransport::LocalInvoke,
            None,
            None,
            None,
            "permission",
            "permission",
            10,
        )
        .await
        .unwrap();
        finalize_ask_resolution(&db, "permission", "answered", 11)
            .await
            .unwrap();

        let late = claim_ask_resolution(
            &db,
            "permission",
            "deny",
            cairn_common::identity::AppearanceTransport::AuthenticatedOperator,
            None,
            None,
            Some("operator:late"),
            "permission",
            "permission",
            12,
        )
        .await
        .unwrap();
        assert!(
            matches!(late, AskClaim::Existing(AskResolution { answer, domain_resolved: true, .. }) if answer == "allow")
        );
        assert!(try_lease_ask_action(&db, "permission", 13, 100)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn failed_or_abandoned_domain_lease_is_recoverable_after_restart() {
        let db = migrated_test_db("channel-ledger-domain-retry.db").await;
        claim_ask_resolution(
            &db,
            "permission",
            "Approve",
            cairn_common::identity::AppearanceTransport::ChannelReply,
            Some("imessage"),
            Some("phone"),
            Some("sender:first"),
            "permission",
            "permission",
            10,
        )
        .await
        .unwrap();
        let lease = try_lease_ask_action(&db, "permission", 11, 100)
            .await
            .unwrap()
            .unwrap();
        assert!(release_ask_action(&db, &lease, "temporary failure")
            .await
            .unwrap());
        assert!(try_lease_ask_action(&db, "permission", 12, 100)
            .await
            .unwrap()
            .is_some());

        let db = migrated_test_db("channel-ledger-domain-expiry.db").await;
        claim_ask_resolution(
            &db,
            "prompt:0",
            "Ship",
            cairn_common::identity::AppearanceTransport::ChannelReply,
            Some("discord"),
            Some("room"),
            Some("sender:first"),
            "question",
            "prompt",
            10,
        )
        .await
        .unwrap();
        assert!(try_lease_ask_action(&db, "prompt", 11, 10)
            .await
            .unwrap()
            .is_some());
        assert!(try_lease_ask_action(&db, "prompt", 22, 10)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            pending_ask_actions(&db).await.unwrap(),
            vec![("prompt".into(), "question".into())]
        );
    }

    #[tokio::test]
    async fn finalization_cleans_sent_renderings_and_retires_unsent_renderings() {
        let db = migrated_test_db("channel-ledger-offline-finalization.db").await;
        claim_ask_resolution(
            &db,
            "prompt:0",
            "Ship",
            cairn_common::identity::AppearanceTransport::ChannelReply,
            Some("discord"),
            Some("room"),
            Some("sender:first"),
            "question",
            "prompt",
            10,
        )
        .await
        .unwrap();

        let mut sent = intent("sent-rendering");
        sent.channel = "discord";
        sent.conversation = "discord:1/10";
        sent.binding_ref = "prompt:0";
        let mut pending = intent("pending-rendering");
        pending.channel = "imessage";
        pending.conversation = "imessage:+15551234567";
        pending.binding_ref = "prompt:0";
        let mut failed = intent("failed-rendering");
        failed.channel = "telegram";
        failed.conversation = "telegram:42";
        failed.binding_ref = "prompt:0";
        assert!(insert_intent(&db, &sent).await.unwrap());
        assert!(insert_intent(&db, &pending).await.unwrap());
        assert!(insert_intent(&db, &failed).await.unwrap());
        assert!(mark_sent(&db, sent.id, "message", None, None, 11)
            .await
            .unwrap());
        assert!(mark_failed(&db, failed.id, "offline").await.unwrap());

        assert_eq!(
            finalize_ask_resolution(&db, "prompt", "✓ answered: Ship", 12)
                .await
                .unwrap(),
            1
        );
        assert_eq!(list_cleanup_pending(&db, "discord").await.unwrap().len(), 1);
        assert!(list_unresolved(&db, "imessage").await.unwrap().is_empty());
        assert!(list_unresolved(&db, "telegram").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn intent_fence_and_lifecycle_are_durable() {
        let db = migrated_test_db("channel-ledger-lifecycle.db").await;
        assert!(insert_intent(&db, &intent("first")).await.unwrap());
        assert!(!insert_intent(&db, &intent("duplicate")).await.unwrap());
        assert!(mark_sent(
            &db,
            "first",
            "poll-guid",
            Some("caption-guid"),
            Some("{}"),
            11
        )
        .await
        .unwrap());
        assert_eq!(
            get_by_provider_guid(&db, "imessage", "caption-guid")
                .await
                .unwrap()
                .unwrap()
                .id,
            "first"
        );
        assert!(mark_resolved(&db, "first", 12).await.unwrap());
        assert!(list_unresolved(&db, "imessage").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn expiry_abandons_undelivered_intents_and_spares_delivered_ones() {
        let db = migrated_test_db("channel-ledger-expiry.db").await;
        for (id, binding) in [
            ("never-sent", "prompt-1:0"),
            ("send-errored", "prompt-2:0"),
            ("on-the-phone", "prompt-3:0"),
            ("answered", "prompt-4:0"),
        ] {
            let mut intent = intent(id);
            intent.binding_ref = binding;
            assert!(insert_intent(&db, &intent).await.unwrap());
        }
        assert!(mark_failed(&db, "send-errored", "executor offline")
            .await
            .unwrap());
        assert!(mark_sent(&db, "on-the-phone", "guid-a", None, None, 11)
            .await
            .unwrap());
        assert!(mark_sent(&db, "answered", "guid-b", None, None, 11)
            .await
            .unwrap());
        assert!(mark_resolved(&db, "answered", 12).await.unwrap());

        assert_eq!(expire_undelivered(&db, "imessage", 20).await.unwrap(), 2);

        let unresolved = list_unresolved(&db, "imessage").await.unwrap();
        assert_eq!(
            unresolved
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["on-the-phone"],
            "only an ask that actually reached the phone stays live"
        );
        // An expired intent keeps its fence, so the sweep can never resurrect it.
        assert!(!insert_intent(&db, &intent("replay")).await.unwrap());
        // A reply to an ask that did go out still binds after the session ends.
        assert_eq!(
            get_by_provider_guid(&db, "imessage", "guid-a")
                .await
                .unwrap()
                .unwrap()
                .id,
            "on-the-phone"
        );
    }

    #[tokio::test]
    async fn cursor_never_regresses_and_inbound_provider_guid_deduplicates() {
        let db = migrated_test_db("channel-ledger-inbound.db").await;
        assert_eq!(advance_cursor(&db, "imessage", 42).await.unwrap(), 42);
        assert_eq!(advance_cursor(&db, "imessage", 9).await.unwrap(), 42);
        let inbound = InboundRecord {
            id: "in-1".into(),
            channel: "imessage".into(),
            provider_guid: Some("message-guid".into()),
            sender: "+15551234567".into(),
            text: "hello".into(),
            received_at: 20,
            rejection_reason: Some("allowlist".into()),
            acknowledged_at: None,
        };
        assert!(insert_inbound(&db, &inbound).await.unwrap());
        let mut duplicate = inbound.clone();
        duplicate.id = "in-2".into();
        assert!(!insert_inbound(&db, &duplicate).await.unwrap());
        assert!(mark_inbound_acknowledged(&db, "in-1", 21).await.unwrap());
        assert_eq!(
            list_inbound(&db, "imessage", 10).await.unwrap()[0].acknowledged_at,
            Some(21)
        );
    }

    #[tokio::test]
    async fn thread_focus_tracks_the_most_recent_selection_and_follows_survive() {
        let db = migrated_test_db("channel-ledger-thread-focus.db").await;
        assert!(follow_target(&db, "imessage", "cairn://p/cairn/1", 10, 42)
            .await
            .unwrap());
        set_focus(&db, "imessage", "cairn://p/cairn/1", 10)
            .await
            .unwrap();
        set_focus(&db, "imessage", "cairn://p/cairn/2", 11)
            .await
            .unwrap();
        assert_eq!(
            get_focus(&db, "imessage").await.unwrap().as_deref(),
            Some("cairn://p/cairn/2")
        );
        assert_eq!(
            list_follows(&db, "imessage").await.unwrap()[0].cursor_rowid,
            42
        );
    }

    #[tokio::test]
    async fn restart_rebases_surviving_follow_to_the_current_live_edge() {
        let db = migrated_test_db("channel-ledger-thread-restart.db").await;
        assert!(follow_target(&db, "imessage", "cairn://p/cairn/1", 10, 5)
            .await
            .unwrap());

        advance_follow_cursor(&db, "imessage", "cairn://p/cairn/1", 99)
            .await
            .unwrap();

        assert_eq!(
            list_follows(&db, "imessage").await.unwrap()[0].cursor_rowid,
            99,
            "events accumulated before the restarted session's live edge are skipped"
        );
    }

    #[tokio::test]
    async fn refollow_starts_at_the_new_live_edge_instead_of_replaying_backlog() {
        let db = migrated_test_db("channel-ledger-thread-refollow.db").await;
        assert!(follow_target(&db, "imessage", "cairn://p/cairn/1", 10, 5)
            .await
            .unwrap());
        assert!(unfollow_target(&db, "imessage", "cairn://p/cairn/1")
            .await
            .unwrap());
        assert!(follow_target(&db, "imessage", "cairn://p/cairn/1", 20, 99)
            .await
            .unwrap());
        assert_eq!(
            list_follows(&db, "imessage").await.unwrap()[0].cursor_rowid,
            99
        );
    }
    #[tokio::test]
    async fn eligible_bindings_filter_by_target_and_effective_message_class() {
        let db = migrated_test_db("channel-ledger-eligible-bindings.db").await;
        let target = "cairn://p/cairn/4006";
        for (conversation, classes) in [
            ("discord:42/7", super::super::bindings::MESSAGE_CLASSES_ALL),
            ("discord:42/8", super::super::bindings::MESSAGE_CLASS_NOTIFY),
        ] {
            assert!(bind_target(
                &db,
                "discord",
                conversation,
                target,
                super::super::bindings::BindingKind::Structural,
                classes,
                10,
                0,
            )
            .await
            .unwrap());
        }
        assert!(bind_target(
            &db,
            "discord",
            "discord:42/9",
            "cairn://p/cairn/4007",
            super::super::bindings::BindingKind::Structural,
            super::super::bindings::MESSAGE_CLASSES_ALL,
            10,
            0,
        )
        .await
        .unwrap());

        assert_eq!(
            list_eligible_bindings(
                &db,
                "discord",
                target,
                super::super::bindings::MESSAGE_CLASS_QUESTION,
            )
            .await
            .unwrap()
            .into_iter()
            .map(|binding| binding.conversation)
            .collect::<Vec<_>>(),
            vec!["discord:42/7"]
        );
        assert_eq!(
            list_eligible_bindings(
                &db,
                "discord",
                target,
                super::super::bindings::MESSAGE_CLASS_NOTIFY,
            )
            .await
            .unwrap()
            .len(),
            2
        );
    }

    #[tokio::test]
    async fn provider_ledgers_isolate_cursors_bindings_and_message_ids() {
        let db = migrated_test_db("channel-ledger-provider-isolation.db").await;
        assert_eq!(advance_cursor(&db, "imessage", 42).await.unwrap(), 42);
        assert_eq!(advance_cursor(&db, "telegram", 7).await.unwrap(), 7);

        let mut imessage = intent("imessage-intent");
        imessage.channel = "imessage";
        let mut telegram = intent("telegram-intent");
        telegram.channel = "telegram";
        assert!(insert_intent(&db, &imessage).await.unwrap());
        assert!(insert_intent(&db, &telegram).await.unwrap());
        assert!(
            mark_sent(&db, "imessage-intent", "shared-id", None, None, 10)
                .await
                .unwrap()
        );
        assert!(
            mark_sent(&db, "telegram-intent", "shared-id", None, None, 11)
                .await
                .unwrap()
        );

        assert_eq!(get_cursor(&db, "imessage").await.unwrap(), Some(42));
        assert_eq!(get_cursor(&db, "telegram").await.unwrap(), Some(7));
        assert_eq!(
            get_by_provider_guid(&db, "imessage", "shared-id")
                .await
                .unwrap()
                .unwrap()
                .id,
            "imessage-intent"
        );
        assert_eq!(
            get_by_provider_guid(&db, "telegram", "shared-id")
                .await
                .unwrap()
                .unwrap()
                .id,
            "telegram-intent"
        );
    }
}
