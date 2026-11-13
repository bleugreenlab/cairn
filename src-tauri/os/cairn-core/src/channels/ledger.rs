use cairn_db::turso::{params, Row};

use crate::storage::{DbResult, LocalDb, RowExt};

const OUTBOUND_COLUMNS: &str = "id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, options_json, status, provider_guid, caption_guid, created_at, sent_at, resolved_at, last_error, attempts, next_attempt_at, last_attempt_at, reply_to, reply_to_guid";

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

pub async fn get_by_id(db: &LocalDb, id: &str) -> Result<Option<OutboundRecord>, String> {
    db.query_opt(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE id = ?1"),
        (id.to_string(),),
        OutboundRecord::from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Atomically takes one scheduled retry. Clearing `next_attempt_at` is the
/// retry-side claim, so concurrent sweeps cannot both cross the provider boundary;
/// `now` stamps the attempt (see [`begin_delivery`]).
pub async fn begin_retry_delivery(
    db: &LocalDb,
    id: &str,
    now: i64,
    max_attempts: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET last_error = 'delivery outcome unknown', next_attempt_at = NULL, last_attempt_at = ?2 WHERE id = ?1 AND status = 'failed' AND attempts < ?3 AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?2",
        params![id.to_string(), now, max_attempts],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
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
    pub binding_kind: String,
}

/// List every persisted binding that currently carries at least one outbound
/// message class -- operator attention or agent text. This is a read-only
/// registry projection; callers layer provider configuration and runtime health
/// onto it. A zero-class row carries nothing and is not listed.
pub async fn list_active_bindings(db: &LocalDb) -> Result<Vec<ActiveBinding>, String> {
    db.query_all(
        "SELECT provider, conversation, target_uri, message_classes, binding_kind
           FROM channel_conversation_binding
          WHERE message_classes != 0
          ORDER BY provider, conversation, target_uri",
        (),
        active_binding,
    )
    .await
    .map_err(|error| error.to_string())
}

fn active_binding(row: &Row) -> DbResult<ActiveBinding> {
    Ok(ActiveBinding {
        provider: row.text(0)?,
        conversation: row.text(1)?,
        target_uri: row.text(2)?,
        message_classes: row.i64(3)?,
        binding_kind: row.text(4)?,
    })
}

/// The active binding of `kind` that targets `target_uri`, newest first when a
/// guild change left more than one: the conversation
/// `cairn://channels/discord/{project}/...` names.
pub async fn binding_for_target(
    db: &LocalDb,
    provider: &str,
    target_uri: &str,
    kind: super::bindings::BindingKind,
) -> Result<Option<ActiveBinding>, String> {
    db.query_opt(
        "SELECT provider, conversation, target_uri, message_classes, binding_kind
           FROM channel_conversation_binding
          WHERE provider = ?1 AND target_uri = ?2 AND binding_kind = ?3
            AND message_classes != 0
          ORDER BY bound_at DESC, conversation
          LIMIT 1",
        params![
            provider.to_string(),
            target_uri.to_string(),
            kind.as_str().to_string()
        ],
        active_binding,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every active binding one conversation carries, in target order.
pub async fn bindings_for_conversation(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
) -> Result<Vec<ActiveBinding>, String> {
    db.query_all(
        "SELECT provider, conversation, target_uri, message_classes, binding_kind
           FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND message_classes != 0
          ORDER BY target_uri",
        params![provider.to_string(), conversation.to_string()],
        active_binding,
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

/// The domain effects a cadence sweep is allowed to attempt.
///
/// A parked action is deliberately absent: its refusal is waiting on an
/// operator, not on time, so re-attempting it on a timer can only produce the
/// same refusal and the same log line (CAIRN-4364). It comes back the moment
/// something leases it, which is what an operator answering does.
pub async fn pending_ask_actions(db: &LocalDb) -> Result<Vec<(String, String)>, String> {
    db.query_all(
        "SELECT action_ref, kind FROM channel_ask_action WHERE domain_resolved_at IS NULL AND parked_at IS NULL ORDER BY action_ref",
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

/// Takes the right to run one domain effect.
///
/// A parked action is unleasable, and that is the whole guarantee: parking is
/// worth nothing if a caller holding a snapshot from before the park can still
/// take the lease. Two routers sweep concurrently and both list the same action;
/// the one that loses the race arrives with a lease attempt that is already
/// stale, and letting it through would clear the park, re-attempt the same
/// refusal, and put the action back in the loop it was parked to leave.
///
/// Only a new answer lifts a park, in [`claim_ask_resolution`] -- an event
/// nobody can produce by racing.
pub async fn try_lease_ask_action(
    db: &LocalDb,
    action_ref: &str,
    now: i64,
    lease_ms: i64,
) -> Result<Option<DomainActionLease>, String> {
    let token = uuid::Uuid::new_v4().to_string();
    let changed = db.execute(
        "UPDATE channel_ask_action SET lease_token = ?2, lease_until = ?3, attempt_count = attempt_count + 1, last_error = NULL WHERE action_ref = ?1 AND domain_resolved_at IS NULL AND parked_at IS NULL AND (lease_until IS NULL OR lease_until <= ?4)",
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

/// Marks a refusal that is TERMINAL for the answer which earned it: nothing but
/// an operator acting can change the outcome, so no amount of retrying will.
///
/// Every resolver failure reaches this ledger as `Err(String)`, which is why the
/// retry machinery could not tell "the disk was full, try again" from "this
/// answer is not allowed to approve this prompt". It retried both, and a single
/// unapprovable answer produced ~29,000 identical log lines in a day
/// (CAIRN-4364). The vocabulary lives here, next to the machinery that acts on
/// it, and the resolver that mints such a refusal speaks it -- so what a failure
/// means travels with the failure instead of being re-derived from prose by
/// every caller that has to decide what to do about it.
pub const OPERATOR_PENDING_REFUSAL: &str = "Refused (pending operator):";

/// Whether a failure is a refusal only an operator event can lift.
///
/// Matched anywhere in the message rather than at its start, because a refusal
/// is routinely wrapped by the layer that reports it.
pub fn is_operator_pending_refusal(error: &str) -> bool {
    error.contains(OPERATOR_PENDING_REFUSAL)
}

/// What became of a domain action whose effect failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskActionFailure {
    /// Parked by this call: no cadence attempt follows, and the answer that
    /// earned the refusal has been withdrawn from the gate.
    Parked,
    /// Released for a later attempt, because time can plausibly change it.
    Retryable,
    /// The lease was already gone, so this attempt speaks for nothing.
    Stale,
}

/// Records the outcome of a failed domain effect, and decides here — once —
/// whether it is worth attempting again.
///
/// The park-or-retry choice is deliberately NOT the caller's. Three surfaces now
/// drive domain effects through this ledger, and a single one of them mistaking
/// "this surface may not decide that" for "try again shortly" restores the whole
/// defect: an unapprovable answer sitting on the gate, retried forever, with the
/// operator locked out by their own replayed answer. One door, one rule.
pub async fn record_ask_action_failure(
    db: &LocalDb,
    lease: &DomainActionLease,
    error: &str,
    now: i64,
) -> Result<AskActionFailure, String> {
    if is_operator_pending_refusal(error) {
        return Ok(if park_ask_action(db, lease, error, now).await? {
            AskActionFailure::Parked
        } else {
            AskActionFailure::Stale
        });
    }
    Ok(if release_ask_action(db, lease, error).await? {
        AskActionFailure::Retryable
    } else {
        AskActionFailure::Stale
    })
}

async fn release_ask_action(
    db: &LocalDb,
    lease: &DomainActionLease,
    error: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_ask_action SET lease_token = NULL, lease_until = NULL, last_error = ?3 WHERE action_ref = ?1 AND lease_token = ?2 AND domain_resolved_at IS NULL",
        params![lease.action_ref.clone(), lease.token.clone(), error.to_string()],
    ).await.map(|n| n == 1).map_err(|error| error.to_string())
}

/// Parks a domain action whose refusal only an operator can lift, and withdraws
/// the answer that earned it.
///
/// The withdrawal is the half that keeps "still pending for the operator" true.
/// A channel reply claims the first-answer-wins resolution BEFORE anything asks
/// whether that surface may make this decision, so an answer the domain then
/// refuses would otherwise sit on the gate forever: an operator answering the
/// same request afterwards replays the stored winner, is refused for the same
/// reason, and cannot get their own decision recorded. A refused answer resolved
/// nothing, minted nothing, and woke nothing, so it holds no claim either --
/// deleting it leaves the gate genuinely open for whoever can answer it.
///
/// The refusal survives as `parked_reason`, which names the surface that
/// answered and why it could not.
async fn park_ask_action(
    db: &LocalDb,
    lease: &DomainActionLease,
    reason: &str,
    parked_at: i64,
) -> Result<bool, String> {
    let action_ref = lease.action_ref.clone();
    let token = lease.token.clone();
    let reason = reason.to_string();
    db.write(move |conn| {
        let action_ref = action_ref.clone();
        let token = token.clone();
        let reason = reason.clone();
        Box::pin(async move {
            let parked = conn.execute(
                "UPDATE channel_ask_action SET lease_token = NULL, lease_until = NULL, parked_at = ?3, parked_reason = ?4, last_error = ?4 WHERE action_ref = ?1 AND lease_token = ?2 AND domain_resolved_at IS NULL",
                params![action_ref.clone(), token, parked_at, reason],
            ).await?;
            if parked == 1 {
                conn.execute(
                    "DELETE FROM channel_ask_resolution WHERE action_ref = ?1 AND domain_resolved_at IS NULL",
                    params![action_ref],
                ).await?;
            }
            Ok(parked)
        })
    })
    .await
    .map(|parked| parked == 1)
    .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainActionLease {
    pub action_ref: String,
    pub kind: String,
    pub token: String,
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

/// Grant or revoke ONE message class on one binding, leaving every other bit as
/// it was. Returns whether the binding exists.
///
/// Single-bit rather than a whole-mask replace because the mask has several
/// writers: the reconciler installs a structural row's defaults, migrations
/// backfill new grants, and the operator toggles one grant at a time. A replace
/// computed from a mask read moments earlier would silently undo whichever of
/// those landed in between.
pub async fn set_message_class(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    class: i64,
    granted: bool,
) -> Result<bool, String> {
    if class <= 0 || class & (class - 1) != 0 {
        return Err(format!("not a single message class bit: {class}"));
    }
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    // Ask the row its kind before validating the target: an ambient row's
    // target is a project root, which the thread-or-issue check below refuses
    // with an error that would hide why the change is impossible.
    let kind = db
        .query_opt_text(
            "SELECT binding_kind FROM channel_conversation_binding
              WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
            params![
                provider.to_string(),
                conversation.clone(),
                cairn_uri::canonicalize_uri_identity(uri)
            ],
        )
        .await
        .map_err(|error| error.to_string())?;
    if kind.as_deref() == Some(super::bindings::BindingKind::Ambient.as_str()) {
        return Err(AMBIENT_CLASSES_FIXED.to_string());
    }
    let uri = canonical_persisted_target(uri)?;
    let (sql, mask) = if granted {
        (
            "UPDATE channel_conversation_binding SET message_classes = message_classes | ?4
              WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
            class,
        )
    } else {
        (
            "UPDATE channel_conversation_binding SET message_classes = message_classes & ?4
              WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3",
            !class,
        )
    };
    db.execute(sql, params![provider.to_string(), conversation, uri, mask])
        .await
        .map(|changed| changed == 1)
        .map_err(|error| error.to_string())
}

/// Why an ambient binding's classes never change: the CHECK pins them to agent
/// text, and the operator's switch shows this instead of a constraint failure.
pub const AMBIENT_CLASSES_FIXED: &str =
    "ambient conversations carry only agent text; their classes can't be changed";

fn canonical_persisted_target(uri: &str) -> Result<String, String> {
    canonical_binding_target(uri, super::bindings::BindingKind::Structural)
}

/// The canonical target a binding of `kind` persists. A structural binding
/// targets a thread or issue; an ambient binding targets exactly a project
/// root.
fn canonical_binding_target(
    uri: &str,
    kind: super::bindings::BindingKind,
) -> Result<String, String> {
    let uri = cairn_uri::canonicalize_uri_identity(uri);
    if uri == "cairn:~" || uri.starts_with("cairn:~/") || uri.starts_with("cairn:@") {
        return Err(format!(
            "deictic channel target must be resolved before persistence: {uri}"
        ));
    }

    match kind {
        super::bindings::BindingKind::Ambient => {
            ambient_target_project(&uri)
                .ok_or_else(|| format!("an ambient binding targets a project root: {uri}"))?;
        }
        super::bindings::BindingKind::Structural => {
            super::bindings::BindingTarget::parse(&uri)?;
        }
    }
    Ok(uri)
}

/// The project key an ambient binding's target names, when it is a project
/// root.
fn ambient_target_project(uri: &str) -> Option<String> {
    match cairn_uri::parse_uri(uri)? {
        cairn_uri::CairnResource::Project { project } => Some(project),
        _ => None,
    }
}

/// The project an ambient conversation belongs to, or `None` when the
/// conversation has no ambient binding.
pub async fn ambient_project(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
) -> Result<Option<String>, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let target = db
        .query_opt_text(
            "SELECT target_uri FROM channel_conversation_binding
              WHERE provider = ?1 AND conversation = ?2 AND binding_kind = 'ambient'",
            params![provider.to_string(), conversation],
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(target.as_deref().and_then(ambient_target_project))
}

/// Every ambient conversation of `provider`, with its project key, in
/// conversation order.
pub async fn list_ambient_conversations(
    db: &LocalDb,
    provider: &str,
) -> Result<Vec<(String, String)>, String> {
    let rows = db
        .query_all(
            "SELECT conversation, target_uri FROM channel_conversation_binding
              WHERE provider = ?1 AND binding_kind = 'ambient'
              ORDER BY conversation",
            params![provider.to_string()],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .filter_map(|(conversation, target)| {
            ambient_target_project(&target).map(|project| (conversation, project))
        })
        .collect())
}

/// When a failed row last failed, as a SQL expression: the start of its latest
/// attempt, overwritten by the moment that attempt's failure was recorded. A row
/// failed before attempts were stamped falls back to what it carries: the retry
/// it scheduled (its attempt's start plus backoff) or, with none, its creation.
const FAILED_AT: &str = "COALESCE(last_attempt_at, next_attempt_at, created_at)";

/// The `failed` rows that are a channel's current status rather than its
/// history, as a WHERE fragment binding `?1` to the channel: those whose latest
/// attempt is later than the channel's latest successful send. A failure the
/// provider has since sent past keeps its own `last_error` on the row but no
/// longer degrades the channel. Every retry restamps its row, so a delivery that
/// is still failing is current again with each attempt until its budget is spent.
fn current_failure() -> String {
    format!(
        "status = 'failed' AND {FAILED_AT} > COALESCE((SELECT MAX(sent.sent_at) FROM channel_outbound sent WHERE sent.channel = ?1), -1)"
    )
}

/// The error of the channel's most recent current failure (see
/// [`current_failure`]).
pub async fn latest_send_error(db: &LocalDb, channel: &str) -> Result<Option<String>, String> {
    db.query_opt(
        format!(
            "SELECT last_error FROM channel_outbound WHERE channel = ?1 AND last_error IS NOT NULL AND {} ORDER BY {FAILED_AT} DESC, rowid DESC LIMIT 1",
            current_failure()
        ),
        (channel.to_string(),),
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

/// Claim the only provider attempt for an intent before crossing the external
/// side-effect boundary. `failed` is the durable ambiguous-outcome state: if the
/// process dies after the provider sends but before `mark_sent`, later sweeps do
/// not put the same intent on the wire again. `now` stamps the attempt, so such
/// a failure is dated to the attempt rather than to the intent's creation.
pub async fn begin_delivery(db: &LocalDb, id: &str, now: i64) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'failed', last_error = 'delivery outcome unknown', last_attempt_at = ?2 WHERE id = ?1 AND status = 'pending'",
        params![id.to_string(), now],
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

/// Every `(conversation, target_uri)` key bound for `provider`: the identity
/// `bind_target` inserts under, so a sweep can bind only what is missing.
pub async fn bound_conversation_targets(
    db: &LocalDb,
    provider: &str,
) -> Result<std::collections::HashSet<(String, String)>, String> {
    db.query_all(
        "SELECT conversation, target_uri FROM channel_conversation_binding WHERE provider = ?1",
        (provider.to_string(),),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map(|rows| rows.into_iter().collect())
    .map_err(|error| error.to_string())
}

pub async fn bind_target(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    binding_kind: super::bindings::BindingKind,
    message_classes: i64,
    bound_at: i64,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_binding_target(uri, binding_kind)?;
    db.execute(
        "INSERT OR IGNORE INTO channel_conversation_binding
          (provider, conversation, target_uri, binding_kind, message_classes, bound_at)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            provider.to_string(),
            conversation,
            uri,
            binding_kind.as_str(),
            message_classes.max(0),
            bound_at
        ],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn unbind_target(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    uri: &str,
    binding_kind: Option<&str>,
) -> Result<bool, String> {
    let conversation = super::bindings::canonical_conversation(provider, conversation)?;
    let uri = canonical_persisted_target(uri)?;
    db.execute(
        "DELETE FROM channel_conversation_binding
          WHERE provider = ?1 AND conversation = ?2 AND target_uri = ?3
            AND (?4 IS NULL OR binding_kind = ?4)",
        params![
            provider.to_string(),
            conversation,
            uri,
            binding_kind.map(str::to_string)
        ],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
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

/// Re-key a pending intent onto the conversation its delivery now addresses, so
/// the row, its send, and any later lookup by `(conversation, binding)` agree.
/// The caller first establishes that no other row already holds that key.
pub async fn adopt_pending_conversation(
    db: &LocalDb,
    id: &str,
    conversation: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET conversation = ?2 WHERE id = ?1 AND status = 'pending'",
        params![id.to_string(), conversation.to_string()],
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

/// Expire a row still owed a delivery (`pending`, or `failed` awaiting a
/// retry) that can never be sent, recording why in `last_error`. Unlike
/// [`mark_failed`] it spends no attempt and schedules nothing: no sweep picks
/// the row up again.
pub async fn expire_undeliverable(
    db: &LocalDb,
    id: &str,
    error: &str,
    expired_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound
            SET status = 'expired', resolved_at = ?3, last_error = ?2, next_attempt_at = NULL
          WHERE id = ?1 AND status IN ('pending', 'failed')",
        params![id.to_string(), error.to_string(), expired_at],
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
    pub attempts: i64,
    pub next_attempt_at: Option<i64>,
    pub last_attempt_at: Option<i64>,
    /// The Cairn message id this replies to.
    pub reply_to: Option<String>,
    /// The provider id it references natively; `None` when sent plainly.
    pub reply_to_guid: Option<String>,
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
            attempts: row.i64(16)?,
            next_attempt_at: row.opt_i64(17)?,
            last_attempt_at: row.opt_i64(18)?,
            reply_to: row.opt_text(19)?,
            reply_to_guid: row.opt_text(20)?,
        })
    }
}

/// Insert a pending delivery intent. Returns `false` when the channel/kind/binding
/// uniqueness fence proves the gate was already observed by an earlier sweep.
pub async fn insert_intent(db: &LocalDb, intent: &NewOutbound<'_>) -> Result<bool, String> {
    insert_intent_with_status(db, intent, "pending").await
}

/// What an outbound replies to: a Cairn message id in the same conversation,
/// and the provider id the send references natively (`None` when the provider
/// has no native reply and it goes out plainly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundReply<'a> {
    pub to: &'a str,
    pub guid: Option<&'a str>,
}

/// [`insert_intent`] for a send that replies to an earlier message, recording
/// both reply columns in the same insert.
pub async fn insert_reply_intent(
    db: &LocalDb,
    intent: &NewOutbound<'_>,
    reply: OutboundReply<'_>,
) -> Result<bool, String> {
    write_intent(db, intent, "pending", Some(reply)).await
}

/// Insert a delivery intent either as owed work (`pending`) or as a gate sealed
/// at a genuine session boundary (`expired`).
pub async fn insert_intent_with_status(
    db: &LocalDb,
    intent: &NewOutbound<'_>,
    status: &str,
) -> Result<bool, String> {
    write_intent(db, intent, status, None).await
}

async fn write_intent(
    db: &LocalDb,
    intent: &NewOutbound<'_>,
    status: &str,
    reply: Option<OutboundReply<'_>>,
) -> Result<bool, String> {
    if !matches!(status, "pending" | "expired") {
        return Err(format!(
            "outbound intent insert status must be pending or expired, got {status:?}"
        ));
    }
    let binding_ref = cairn_uri::canonicalize_uri_identity(intent.binding_ref);
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
        "INSERT OR IGNORE INTO channel_outbound (id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, status, created_at, resolved_at, reply_to, reply_to_guid) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CASE WHEN ?9 = 'expired' THEN ?10 END, ?11, ?12)",
        params![intent.id, intent.channel, intent.kind, binding_ref, intent.conversation, intent.job_id, intent.rendered_text, intent.rendering, status, intent.created_at, reply.map(|reply| reply.to), reply.and_then(|reply| reply.guid)],
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
    let action_ref = cairn_uri::canonicalize_uri_identity(action_ref);
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
    let binding_ref = cairn_uri::canonicalize_uri_identity(binding_ref);
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

/// Intents of one kind that a sweep owes a delivery attempt now: every `pending`
/// row, and every `failed` row whose scheduled retry is due and whose attempts
/// are not exhausted. A `failed` row with no schedule is an ambiguous outcome
/// (the process died mid-send) and is deliberately never re-sent.
pub async fn list_due_deliveries(
    db: &LocalDb,
    channel: &str,
    kind: &str,
    now: i64,
    max_attempts: i64,
) -> Result<Vec<OutboundRecord>, String> {
    db.query_all(
        format!(
            "SELECT {OUTBOUND_COLUMNS} FROM channel_outbound
              WHERE channel = ?1 AND kind = ?2
                AND (status = 'pending'
                     OR (status = 'failed' AND attempts < ?4
                         AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?3))
              ORDER BY created_at"
        ),
        params![channel.to_string(), kind.to_string(), now, max_attempts],
        OutboundRecord::from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Whether any route intent on this channel is still owed a delivery.
pub async fn has_pending_route(db: &LocalDb, channel: &str) -> Result<bool, String> {
    db.query_opt_i64(
        "SELECT EXISTS(SELECT 1 FROM channel_outbound WHERE channel = ?1 AND kind = 'route' AND status = 'pending')",
        params![channel.to_string()],
    )
    .await
    .map(|exists| exists == Some(1))
    .map_err(|error| error.to_string())
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
    let binding_ref = cairn_uri::canonicalize_uri_identity(binding_ref);
    let action_ref = cairn_uri::canonicalize_uri_identity(action_ref);
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
            let inserted = conn.execute(
                "INSERT OR IGNORE INTO channel_ask_resolution (binding_ref, action_ref, kind, answer, winner_provider, winner_conversation, resolved_at, resolution_id, winner_surface, winner_actor) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![binding_ref, action_ref.clone(), kind, answer, provider, conversation, resolved_at, resolution_id, surface, actor],
            ).await?;
            if inserted == 1 {
                // Winning the gate is the event a parked action was waiting for.
                // Parking vacated the gate precisely so a new answer could take
                // it, and this claim is that answer -- so the park lifts here,
                // where a decision was actually made, and nowhere else.
                conn.execute(
                    "UPDATE channel_ask_action SET parked_at = NULL, parked_reason = NULL, last_error = NULL WHERE action_ref = ?1 AND domain_resolved_at IS NULL",
                    params![action_ref],
                ).await?;
            }
            Ok(inserted)
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
    let action_ref = cairn_uri::canonicalize_uri_identity(action_ref);
    let options = serde_json::json!({ "receipt": receipt }).to_string();
    db.write(move |conn| {
        let action_ref = action_ref.clone();
        let options = options.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE channel_ask_action SET domain_resolved_at = COALESCE(domain_resolved_at, ?2), lease_token = NULL, lease_until = NULL, last_error = NULL, parked_at = NULL, parked_reason = NULL WHERE action_ref = ?1",
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

/// Record a provider attempt that failed at `failed_at`, spending one attempt.
/// `next_attempt_at` schedules the retry; `None` fails the row with none, so no
/// sweep ever re-sends it.
pub async fn mark_failed(
    db: &LocalDb,
    id: &str,
    error: &str,
    failed_at: i64,
    next_attempt_at: Option<i64>,
) -> Result<bool, String> {
    db.execute("UPDATE channel_outbound SET status = 'failed', last_error = ?2, last_attempt_at = ?3, next_attempt_at = ?4, attempts = attempts + 1 WHERE id = ?1 AND status = 'failed'", params![id.to_string(), error.to_string(), failed_at, next_attempt_at])
        .await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UndeliveredCounts {
    pub pending: i64,
    pub failed: i64,
}

/// Queued rows, and the failed rows that are current (see [`current_failure`]).
pub async fn undelivered_counts(db: &LocalDb, channel: &str) -> Result<UndeliveredCounts, String> {
    db.query_one(
        format!(
            "SELECT COUNT(*) FILTER (WHERE status = 'pending'), COUNT(*) FILTER (WHERE {}) FROM channel_outbound WHERE channel = ?1 AND status IN ('pending', 'failed')",
            current_failure()
        ),
        (channel.to_string(),),
        |row| Ok(UndeliveredCounts { pending: row.i64(0)?, failed: row.i64(1)? }),
    )
    .await
    .map_err(|error| error.to_string())
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
    /// For an admitted message, the canonical conversation it arrived in.
    pub conversation: Option<String>,
    /// For an admitted message, the target it was delivered into (the project
    /// root for unaddressed ambient text). Set exactly when the message was
    /// admitted.
    pub target_uri: Option<String>,
    /// For an admitted message, the kind of binding that resolved its target:
    /// `structural`, `ambient`, or `none`.
    pub binding_kind: Option<String>,
    /// Why the message reached its target, as [`InboundAddress::token`]
    /// writes it. Read it through [`InboundAddress::parse`].
    pub addressed_by: Option<String>,
    /// The message's own provider id, which a native reply to it references.
    /// (`provider_guid` is the id of the message this one replied to.)
    pub message_guid: Option<String>,
}

const INBOUND_COLUMNS: &str = "id, channel, provider_guid, sender, text, received_at, rejection_reason, acknowledged_at, conversation, target_uri, binding_kind, addressed_by, message_guid";

/// Why an admitted inbound message reached the target it did: the one encoder
/// and decoder of `channel_inbound.addressed_by` and of a `channel_message`
/// fact's `addressedBy` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundAddress {
    /// The leading addresses an ambient message opened with, as typed
    /// (`@channels <#123>`), which the router consumed before delivering the
    /// rest.
    Typed(String),
    /// A native reply to the outbound row `outbound`, delivered to its sender.
    Reply { outbound: String },
    /// Unaddressed text, delivered to the sender of the outbound row
    /// `outbound`: the latest operator message on the transport within
    /// [`UNADDRESSED_WINDOW_MS`].
    RecentOperatorMessage { outbound: String },
    /// Unaddressed text with no recent operator message, delivered to the
    /// default project's general thread.
    DefaultProject { project: String },
}

impl InboundAddress {
    const REPLY: &'static str = "reply:";
    const RECENT: &'static str = "recent:";
    const DEFAULT: &'static str = "default:";

    /// The stored form. A typed address always starts with `@` or `<#`, so it
    /// can never collide with the three prefixed reasons.
    pub fn token(&self) -> String {
        match self {
            Self::Typed(typed) => typed.clone(),
            Self::Reply { outbound } => format!("{}{outbound}", Self::REPLY),
            Self::RecentOperatorMessage { outbound } => format!("{}{outbound}", Self::RECENT),
            Self::DefaultProject { project } => format!("{}{project}", Self::DEFAULT),
        }
    }

    /// Read a stored token back. Anything that is not one of the prefixed
    /// reasons is a typed address.
    pub fn parse(token: &str) -> Self {
        if let Some(outbound) = token.strip_prefix(Self::REPLY) {
            Self::Reply {
                outbound: outbound.to_string(),
            }
        } else if let Some(outbound) = token.strip_prefix(Self::RECENT) {
            Self::RecentOperatorMessage {
                outbound: outbound.to_string(),
            }
        } else if let Some(project) = token.strip_prefix(Self::DEFAULT) {
            Self::DefaultProject {
                project: project.to_string(),
            }
        } else {
            Self::Typed(token.to_string())
        }
    }
}

/// How long an operator message keeps the operator's unaddressed text on its
/// transport: a message sent this long ago or longer no longer does.
pub const UNADDRESSED_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

/// The most recent operator message on `channel` sent into `conversation`
/// after `since_ms` (exclusive). Answered messages count, since the
/// conversation continues.
///
/// Matched by conversation, never by sender: a Telegram group's operator
/// row names the chat (`-100123`) while its inbound sender is the user who
/// typed (`7`). An iMessage inbound's conversation is its sender's handle, so
/// for iMessage this is the handle match.
pub async fn latest_operator_send(
    db: &LocalDb,
    channel: &str,
    conversation: &str,
    since_ms: i64,
) -> Result<Option<OutboundRecord>, String> {
    let conversation = conversation_key(channel, conversation);
    let sent = db
        .query_all(
            format!(
                "SELECT {OUTBOUND_COLUMNS} FROM channel_outbound
                  WHERE kind = ?1 AND channel = ?2 AND sent_at IS NOT NULL AND sent_at > ?3
                  ORDER BY sent_at DESC, rowid DESC"
            ),
            params![super::operator_message::KIND, channel.to_string(), since_ms],
            OutboundRecord::from_row,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(sent
        .into_iter()
        .find(|record| conversation_key(channel, &record.conversation) == conversation))
}

/// One conversation's comparable form on `channel`, whether it is written
/// canonically (`telegram:-100123`, as inbound events carry it) or as the bare
/// destination an operator row stores (`-100123`, `+15551234567`). An address
/// that does not parse compares as written.
fn conversation_key(channel: &str, conversation: &str) -> String {
    let qualified = if conversation.starts_with(&format!("{channel}:")) {
        conversation.to_string()
    } else {
        format!("{channel}:{conversation}")
    };
    qualified
        .parse::<super::ConversationAddress>()
        .map(|address| address.to_string())
        .unwrap_or_else(|_| conversation.to_string())
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
            conversation: row.opt_text(8)?,
            target_uri: row.opt_text(9)?,
            binding_kind: row.opt_text(10)?,
            addressed_by: row.opt_text(11)?,
            message_guid: row.opt_text(12)?,
        })
    }
}

pub async fn insert_inbound(db: &LocalDb, record: &InboundRecord) -> Result<bool, String> {
    db.execute(
        format!("INSERT OR IGNORE INTO channel_inbound ({INBOUND_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"),
        params![record.id.clone(), record.channel.clone(), record.provider_guid.clone(), record.sender.clone(), record.text.clone(), record.received_at, record.rejection_reason.clone(), record.acknowledged_at, record.conversation.clone(), record.target_uri.clone(), record.binding_kind.clone(), record.addressed_by.clone(), record.message_guid.clone()],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

/// A message a send may reply to, found by its Cairn id within one
/// conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyTarget {
    /// An inbound message, with its own provider id when it was recorded.
    Inbound { message_guid: Option<String> },
    /// An outbound message, with the provider id its send produced, if sent.
    Outbound {
        provider_guid: Option<String>,
        status: String,
    },
}

/// The message `id` names among `conversation`'s rows on `provider`, inbound
/// or outbound, or `None`. An id in any other conversation is `None` too, so a
/// caller learns nothing about rows it does not reach.
pub async fn reply_target(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    id: &str,
) -> Result<Option<ReplyTarget>, String> {
    let inbound = db
        .query_opt(
            "SELECT message_guid FROM channel_inbound WHERE id = ?1 AND channel = ?2 AND conversation = ?3",
            params![id.to_string(), provider.to_string(), conversation.to_string()],
            |row| row.opt_text(0),
        )
        .await
        .map_err(|error| error.to_string())?;
    if let Some(message_guid) = inbound {
        return Ok(Some(ReplyTarget::Inbound { message_guid }));
    }
    db.query_opt(
        "SELECT provider_guid, status FROM channel_outbound WHERE id = ?1 AND channel = ?2 AND conversation IN (?3, ?4)",
        params![
            id.to_string(),
            provider.to_string(),
            conversation.to_string(),
            bare_alias(conversation).to_string()
        ],
        |row| {
            Ok(ReplyTarget::Outbound {
                provider_guid: row.opt_text(0)?,
                status: row.text(1)?,
            })
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// One inbound row by id, admitted or not.
pub async fn get_inbound(db: &LocalDb, id: &str) -> Result<Option<InboundRecord>, String> {
    db.query_opt(
        format!("SELECT {INBOUND_COLUMNS} FROM channel_inbound WHERE id = ?1"),
        params![id.to_string()],
        InboundRecord::from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// A provider's inbound messages that were NOT admitted: rejected and
/// unsolicited rows, newest first. Admitted messages carry a target and are
/// left out, so ordinary traffic never floods this list.
pub async fn list_inbound(
    db: &LocalDb,
    channel: &str,
    limit: i64,
) -> Result<Vec<InboundRecord>, String> {
    db.query_all(
        format!("SELECT {INBOUND_COLUMNS} FROM channel_inbound WHERE channel = ?1 AND target_uri IS NULL ORDER BY received_at DESC LIMIT ?2"),
        params![channel.to_string(), limit.max(0)],
        InboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

/// Which way a conversation message travelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDirection {
    In,
    Out,
}

/// One row of a conversation's history: a `channel_inbound` or a
/// `channel_outbound` row, projected onto the columns both directions share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationMessage {
    pub direction: MessageDirection,
    pub id: String,
    /// `received_at` inbound, `created_at` outbound (epoch milliseconds).
    pub at: i64,
    /// The provider sender id inbound; the outbound `kind`.
    pub sender: String,
    pub text: String,
    /// The rejection reason inbound (NULL when admitted); the outbound status.
    pub state: Option<String>,
    /// The target delivered into inbound; the outbound `binding_ref`.
    pub reference: Option<String>,
    pub binding_kind: Option<String>,
    /// The address an ambient message opened with inbound; the last delivery
    /// error outbound.
    pub detail: Option<String>,
    pub attempts: Option<i64>,
    pub next_attempt_at: Option<i64>,
    pub sent_at: Option<i64>,
    pub resolved_at: Option<i64>,
    pub job_id: Option<String>,
    /// Outbound only: the Cairn message id it replied to.
    pub reply_to: Option<String>,
    /// Outbound only: the provider id it referenced natively; `None` with a
    /// `reply_to` means it was sent plainly.
    pub reply_to_guid: Option<String>,
}

/// Which directions a conversation read takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectionFilter {
    In,
    Out,
    All,
}

impl DirectionFilter {
    fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
            Self::All => "all",
        }
    }
}

/// An outbound row projected onto [`ConversationMessage`]'s columns.
const OUTBOUND_MESSAGE_COLUMNS: &str = "'out', id, created_at, kind, rendered_text, status, binding_ref, NULL, last_error, attempts, next_attempt_at, sent_at, resolved_at, job_id, reply_to, reply_to_guid";

fn conversation_message(row: &Row) -> DbResult<ConversationMessage> {
    Ok(ConversationMessage {
        direction: if row.text(0)? == "in" {
            MessageDirection::In
        } else {
            MessageDirection::Out
        },
        id: row.text(1)?,
        at: row.i64(2)?,
        sender: row.text(3)?,
        text: row.text(4)?,
        state: row.opt_text(5)?,
        reference: row.opt_text(6)?,
        binding_kind: row.opt_text(7)?,
        detail: row.opt_text(8)?,
        attempts: row.opt_i64(9)?,
        next_attempt_at: row.opt_i64(10)?,
        sent_at: row.opt_i64(11)?,
        resolved_at: row.opt_i64(12)?,
        job_id: row.opt_text(13)?,
        reply_to: row.opt_text(14)?,
        reply_to_guid: row.opt_text(15)?,
    })
}

/// The bare form some outbound rows of `conversation` are keyed under: a route
/// row's Telegram chat id or iMessage handle. Discord rows carry the address.
fn bare_alias(conversation: &str) -> &str {
    match conversation.split_once(':') {
        Some(("telegram" | "imessage", bare)) => bare,
        _ => conversation,
    }
}

/// A conversation's newest `limit` messages at or after `since_ms`, both
/// directions interleaved, oldest first. `conversation` is the canonical
/// address. Every row is held to `provider`: a bare alias such as a phone
/// number can be both an iMessage handle and a Telegram chat id.
pub async fn conversation_messages(
    db: &LocalDb,
    provider: &str,
    conversation: &str,
    since_ms: i64,
    direction: DirectionFilter,
    limit: i64,
) -> Result<Vec<ConversationMessage>, String> {
    let mut rows = db
        .query_all(
            format!(
                "SELECT direction, id, at, sender, text, state, reference, binding_kind, detail,
                        attempts, next_attempt_at, sent_at, resolved_at, job_id,
                        reply_to, reply_to_guid
                   FROM (
                     SELECT 'in' AS direction, id, received_at AS at, sender, text,
                            rejection_reason AS state, target_uri AS reference, binding_kind,
                            addressed_by AS detail, NULL AS attempts, NULL AS next_attempt_at,
                            NULL AS sent_at, NULL AS resolved_at, NULL AS job_id,
                            NULL AS reply_to, NULL AS reply_to_guid
                       FROM channel_inbound
                      WHERE channel = ?6 AND conversation = ?1 AND received_at >= ?3
                        AND ?4 IN ('in', 'all')
                     UNION ALL
                     SELECT {OUTBOUND_MESSAGE_COLUMNS}
                       FROM channel_outbound
                      WHERE channel = ?6 AND conversation IN (?1, ?2) AND created_at >= ?3
                        AND ?4 IN ('out', 'all')
                   )
                  ORDER BY at DESC, id DESC
                  LIMIT ?5"
            ),
            params![
                conversation.to_string(),
                bare_alias(conversation).to_string(),
                since_ms,
                direction.as_str().to_string(),
                limit.max(0),
                provider.to_string()
            ],
            conversation_message,
        )
        .await
        .map_err(|error| error.to_string())?;
    rows.reverse();
    Ok(rows)
}

/// The acknowledgement a route firing sent, found from the firing itself and
/// never from a page of messages: by the outbound id a firing recorded as
/// `channel_outbound:{id}`, or by the route's delivery key
/// (`route:{route}:channel-message:{inbound}`, with a `:{position}` suffix past
/// the first sink) under the conversation a channel sink names. Held to
/// `provider`, as [`conversation_messages`] is.
pub async fn route_acknowledgement(
    db: &LocalDb,
    provider: &str,
    firing: &InboundFiring,
) -> Result<Option<ConversationMessage>, String> {
    let Some(sink) = firing.sink_ref.as_deref() else {
        return Ok(None);
    };
    if let Some(id) = sink.strip_prefix("channel_outbound:") {
        return db
            .query_opt(
                format!(
                    "SELECT {OUTBOUND_MESSAGE_COLUMNS} FROM channel_outbound
                      WHERE id = ?1 AND channel = ?2"
                ),
                params![id.to_string(), provider.to_string()],
                conversation_message,
            )
            .await
            .map_err(|error| error.to_string());
    }
    if firing.sink_kind != "channel" {
        return Ok(None);
    }
    let key = format!(
        "route:{}:channel-message:{}",
        firing.route_id, firing.inbound_id
    );
    db.query_opt(
        format!(
            "SELECT {OUTBOUND_MESSAGE_COLUMNS} FROM channel_outbound
              WHERE channel = ?1 AND conversation IN (?2, ?3) AND kind = 'route'
                AND (binding_ref = ?4 OR (binding_ref > ?4 || ':' AND binding_ref < ?4 || ';'))
              ORDER BY created_at, id
              LIMIT 1"
        ),
        params![
            provider.to_string(),
            sink.to_string(),
            bare_alias(sink).to_string(),
            key
        ],
        conversation_message,
    )
    .await
    .map_err(|error| error.to_string())
}

/// The newest activity on each of a provider's conversations, in epoch
/// milliseconds: its latest inbound receipt or outbound send (or, unsent, its
/// creation). Keys are the conversation strings rows carry, which for a route
/// row on Telegram or iMessage is the bare chat id or handle.
pub async fn last_activity(
    db: &LocalDb,
    provider: &str,
) -> Result<std::collections::HashMap<String, i64>, String> {
    let rows = db
        .query_all(
            "SELECT conversation, MAX(at) FROM (
                 SELECT conversation, received_at AS at FROM channel_inbound
                  WHERE channel = ?1 AND conversation IS NOT NULL
                 UNION ALL
                 SELECT conversation, COALESCE(sent_at, created_at) FROM channel_outbound
                  WHERE channel = ?1
               )
              GROUP BY conversation",
            params![provider.to_string()],
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows.into_iter().collect())
}

/// Operator sends on `channel` still awaiting a read receipt: sent at or after
/// `since_ms`, with a provider guid and no `read_at`, as
/// `(binding_ref, provider_guid)`.
pub async fn unread_operator_sends(
    db: &LocalDb,
    channel: &str,
    since_ms: i64,
) -> Result<Vec<(String, String)>, String> {
    db.query_all(
        "SELECT binding_ref, provider_guid FROM channel_outbound
          WHERE channel = ?1 AND kind = 'operator' AND status = 'sent'
            AND read_at IS NULL AND provider_guid IS NOT NULL AND sent_at >= ?2
          ORDER BY sent_at",
        params![channel.to_string(), since_ms],
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|error| error.to_string())
}

/// Stamp the transport's read receipt on the row `provider_guid` names, once.
/// Status and attempts are not touched: a receipt says nothing about delivery.
pub async fn mark_read(
    db: &LocalDb,
    channel: &str,
    provider_guid: &str,
    read_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET read_at = ?3
          WHERE channel = ?1 AND provider_guid = ?2 AND read_at IS NULL",
        params![channel.to_string(), provider_guid.to_string(), read_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// The transport's read receipt on one outbound row, epoch milliseconds.
pub async fn read_at(db: &LocalDb, id: &str) -> Result<Option<i64>, String> {
    db.query_opt(
        "SELECT read_at FROM channel_outbound WHERE id = ?1",
        params![id.to_string()],
        |row| row.opt_i64(0),
    )
    .await
    .map(Option::flatten)
    .map_err(|error| error.to_string())
}

/// When the operator last wrote on a provider: its newest admitted inbound.
pub async fn last_operator_message(db: &LocalDb, provider: &str) -> Result<Option<i64>, String> {
    db.query_opt(
        "SELECT MAX(received_at) FROM channel_inbound
          WHERE channel = ?1 AND target_uri IS NOT NULL",
        params![provider.to_string()],
        |row| row.opt_i64(0),
    )
    .await
    .map(Option::flatten)
    .map_err(|error| error.to_string())
}

/// One route firing a `channel_message` fact caused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundFiring {
    /// The `channel_inbound` id whose fact fired it.
    pub inbound_id: String,
    pub route_id: String,
    pub status: String,
    pub drop_reason: Option<String>,
    pub error: Option<String>,
    pub sink_kind: String,
    pub sink_ref: Option<String>,
}

/// The route firings each inbound message's `channel_message` fact caused, in
/// firing order. A fact's identity is `channel-message:{inbound id}`.
pub async fn firings_for_inbound(
    db: &LocalDb,
    inbound_ids: &[String],
) -> Result<Vec<InboundFiring>, String> {
    let mut firings = Vec::new();
    for chunk in inbound_ids.chunks(cairn_db::sql::IN_LIST_CHUNK) {
        let binds = chunk
            .iter()
            .map(|id| cairn_db::turso::Value::Text(format!("channel-message:{id}")))
            .collect::<Vec<_>>();
        let sql = format!(
            "SELECT fact_identity, route_id, status, drop_reason, error, sink_kind, sink_ref
               FROM route_firings
              WHERE fact_identity IN ({})
              ORDER BY created_at, seq",
            cairn_db::sql::placeholders(chunk.len())
        );
        firings.extend(
            db.query_all(sql, binds, |row| {
                let identity = row.text(0)?;
                Ok(InboundFiring {
                    inbound_id: identity
                        .strip_prefix("channel-message:")
                        .unwrap_or(&identity)
                        .to_string(),
                    route_id: row.text(1)?,
                    status: row.text(2)?,
                    drop_reason: row.opt_text(3)?,
                    error: row.opt_text(4)?,
                    sink_kind: row.text(5)?,
                    sink_ref: row.opt_text(6)?,
                })
            })
            .await
            .map_err(|error| error.to_string())?,
        );
    }
    Ok(firings)
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
        use super::super::bindings::{
            BindingKind, MESSAGE_CLASS_NOTIFY, MESSAGE_CLASS_PERMISSION, MESSAGE_CLASS_QUESTION,
        };
        let db = migrated_test_db("channel-ledger-active-bindings.db").await;
        for (provider, conversation, target, classes) in [
            (
                "telegram",
                "telegram:20",
                "cairn://p/cairn/general",
                MESSAGE_CLASS_NOTIFY,
            ),
            (
                "imessage",
                "imessage:a@example.com",
                "cairn://p/cairn/4026",
                MESSAGE_CLASS_QUESTION | MESSAGE_CLASS_PERMISSION,
            ),
            (
                "imessage",
                "imessage:b@example.com",
                "cairn://p/cairn/general",
                0,
            ),
        ] {
            bind_target(
                &db,
                provider,
                conversation,
                target,
                BindingKind::Structural,
                classes,
                1,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            list_active_bindings(&db).await.unwrap(),
            vec![
                ActiveBinding {
                    provider: "imessage".into(),
                    conversation: "imessage:a@example.com".into(),
                    target_uri: "cairn://p/cairn/4026".into(),
                    message_classes: 3,
                    binding_kind: "structural".into(),
                },
                ActiveBinding {
                    provider: "telegram".into(),
                    conversation: "telegram:20".into(),
                    target_uri: "cairn://p/cairn/general".into(),
                    message_classes: 4,
                    binding_kind: "structural".into(),
                },
            ]
        );
    }

    /// An ambient binding targets exactly a project root, is never a structural
    /// target, never carries operator attention, and refuses a class change
    /// instructively rather than with a target or CHECK error.
    #[tokio::test]
    async fn ambient_bindings_name_a_project_and_never_carry_attention() {
        use super::super::bindings::{
            BindingKind, MESSAGE_CLASSES_ALL, MESSAGE_CLASS_AGENT_TEXT, MESSAGE_CLASS_NOTIFY,
            MESSAGE_CLASS_PERMISSION, MESSAGE_CLASS_QUESTION,
        };
        let db = migrated_test_db("channel-ledger-ambient.db").await;
        assert!(bind_target(
            &db,
            "discord",
            "discord:1/9",
            "cairn://p/cairn/general",
            BindingKind::Ambient,
            MESSAGE_CLASS_AGENT_TEXT,
            1,
        )
        .await
        .unwrap_err()
        .contains("project root"));
        assert!(bind_target(
            &db,
            "discord",
            "discord:1/9",
            "cairn://p/cairn",
            BindingKind::Structural,
            MESSAGE_CLASS_AGENT_TEXT,
            1,
        )
        .await
        .is_err());
        assert!(bind_target(
            &db,
            "discord",
            "discord:1/9",
            "cairn://p/cairn",
            BindingKind::Ambient,
            MESSAGE_CLASS_AGENT_TEXT,
            1,
        )
        .await
        .unwrap());
        assert!(bind_target(
            &db,
            "discord",
            "discord:1/8",
            "cairn://p/cairn/general",
            BindingKind::Structural,
            MESSAGE_CLASSES_ALL,
            2,
        )
        .await
        .unwrap());

        assert_eq!(
            ambient_project(&db, "discord", "discord:1/9")
                .await
                .unwrap(),
            Some("cairn".to_string())
        );
        assert_eq!(
            ambient_project(&db, "discord", "discord:1/10")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            list_ambient_conversations(&db, "discord").await.unwrap(),
            vec![("discord:1/9".to_string(), "cairn".to_string())]
        );
        assert_eq!(
            lookup_conversation_target(&db, "discord", "discord:1/9")
                .await
                .unwrap(),
            None
        );
        for class in [
            MESSAGE_CLASS_QUESTION,
            MESSAGE_CLASS_PERMISSION,
            MESSAGE_CLASS_NOTIFY,
        ] {
            // An attention gate's target is a thread or an issue; a project root
            // is never one, so no gate can reach the ambient row.
            assert!(
                list_eligible_bindings(&db, "discord", "cairn://p/cairn", class)
                    .await
                    .is_err()
            );
            assert!(
                list_eligible_bindings(&db, "discord", "cairn://p/cairn/general", class)
                    .await
                    .unwrap()
                    .iter()
                    .all(|binding| binding.binding_kind != "ambient"),
                "class {class}"
            );
        }
        assert!(list_active_bindings(&db)
            .await
            .unwrap()
            .iter()
            .any(|binding| binding.conversation == "discord:1/9"
                && binding.binding_kind == "ambient"
                && binding.message_classes == MESSAGE_CLASS_AGENT_TEXT));

        for (class, granted) in [
            (MESSAGE_CLASS_AGENT_TEXT, false),
            (MESSAGE_CLASS_AGENT_TEXT, true),
            (MESSAGE_CLASS_QUESTION, true),
        ] {
            assert_eq!(
                set_message_class(
                    &db,
                    "discord",
                    "discord:1/9",
                    "cairn://p/cairn",
                    class,
                    granted
                )
                .await
                .unwrap_err(),
                AMBIENT_CLASSES_FIXED
            );
        }
    }

    #[tokio::test]
    async fn home_relative_targets_never_cross_the_ledger_boundary() {
        let db = migrated_test_db("channel-ledger-relative-target.db").await;

        assert!(bind_target(
            &db,
            "imessage",
            "imessage:a@example.com",
            "cairn:~/",
            super::super::bindings::BindingKind::Structural,
            super::super::bindings::MESSAGE_CLASSES_ALL,
            10,
        )
        .await
        .unwrap_err()
        .contains("must be resolved"));
        assert!(list_active_bindings(&db).await.unwrap().is_empty());
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

    async fn status(db: &LocalDb) -> (Option<String>, i64) {
        (
            latest_send_error(db, "imessage").await.unwrap(),
            undelivered_counts(db, "imessage").await.unwrap().failed,
        )
    }

    async fn insert_at(db: &LocalDb, id: &'static str, binding_ref: &'static str, created_at: i64) {
        let mut row = intent(id);
        row.binding_ref = binding_ref;
        row.created_at = created_at;
        assert!(insert_intent(db, &row).await.unwrap());
    }

    #[tokio::test]
    async fn a_failure_is_status_until_the_provider_sends_past_it() {
        let db = migrated_test_db("channel-ledger-failure-status.db").await;
        insert_at(&db, "failed", "prompt-1:0", 10).await;
        assert!(begin_delivery(&db, "failed", 11).await.unwrap());
        assert!(mark_failed(&db, "failed", "thread is locked", 12, Some(17))
            .await
            .unwrap());
        assert_eq!(
            status(&db).await,
            (Some("thread is locked".to_string()), 1),
            "with no success since, a failure is the channel's status"
        );
        assert_eq!(latest_send_error(&db, "telegram").await.unwrap(), None);

        insert_at(&db, "sent", "prompt-2:0", 20).await;
        assert!(mark_sent(&db, "sent", "guid", None, None, 21)
            .await
            .unwrap());
        assert_eq!(
            status(&db).await,
            (None, 0),
            "a failure the provider has since sent past is history"
        );
        assert_eq!(
            get_by_id(&db, "failed")
                .await
                .unwrap()
                .unwrap()
                .last_error
                .as_deref(),
            Some("thread is locked"),
            "the row keeps its own error as the archaeology record"
        );

        // Its retry is claimed after that success and the process stops before
        // the outcome is recorded. The claim dates the attempt, so the
        // unresolved delivery is current again.
        assert!(begin_retry_delivery(&db, "failed", 30, 6).await.unwrap());
        assert_eq!(
            status(&db).await,
            (Some("delivery outcome unknown".to_string()), 1)
        );

        assert!(
            mark_failed(&db, "failed", "thread is still locked", 31, Some(40))
                .await
                .unwrap()
        );
        assert_eq!(
            status(&db).await,
            (Some("thread is still locked".to_string()), 1)
        );
        let record = get_by_id(&db, "failed").await.unwrap().unwrap();
        assert_eq!(record.attempts, 2);
        assert_eq!(record.last_attempt_at, Some(31));
    }

    #[tokio::test]
    async fn the_latest_failure_is_the_latest_attempt_not_the_latest_intent() {
        let db = migrated_test_db("channel-ledger-latest-failure.db").await;
        insert_at(&db, "older-intent", "prompt-1:0", 10).await;
        insert_at(&db, "newer-intent", "prompt-2:0", 20).await;
        assert!(begin_delivery(&db, "newer-intent", 21).await.unwrap());
        assert!(
            mark_failed(&db, "newer-intent", "earlier failure", 22, None)
                .await
                .unwrap()
        );
        assert!(begin_delivery(&db, "older-intent", 30).await.unwrap());
        assert!(mark_failed(&db, "older-intent", "latest failure", 31, None)
            .await
            .unwrap());
        assert_eq!(status(&db).await, (Some("latest failure".to_string()), 2));
    }

    #[tokio::test]
    async fn an_unstamped_failure_falls_back_to_its_schedule_then_its_creation() {
        let db = migrated_test_db("channel-ledger-unstamped-failure.db").await;
        // Failed before attempts were stamped: one with a retry schedule, one
        // ambiguous outcome without.
        insert_at(&db, "scheduled", "prompt-1:0", 10).await;
        insert_at(&db, "ambiguous", "prompt-2:0", 20).await;
        db.execute(
            "UPDATE channel_outbound SET status = 'failed', last_error = 'offline', next_attempt_at = 50 WHERE id = ?1",
            ("scheduled",),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE channel_outbound SET status = 'failed', last_error = 'delivery outcome unknown' WHERE id = ?1",
            ("ambiguous",),
        )
        .await
        .unwrap();
        insert_at(&db, "sent", "prompt-3:0", 30).await;
        assert!(mark_sent(&db, "sent", "guid", None, None, 40)
            .await
            .unwrap());
        assert_eq!(
            status(&db).await,
            (Some("offline".to_string()), 1),
            "a schedule after the latest success may hide a failure after it; one before it may not"
        );

        db.execute(
            "UPDATE channel_outbound SET sent_at = 60 WHERE id = ?1",
            ("sent",),
        )
        .await
        .unwrap();
        assert_eq!(
            status(&db).await,
            (None, 0),
            "an orphaned schedule ages out once a send succeeds after it"
        );
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

    /// Two transports answering one ask share one winner, and two routers
    /// leasing its action get one lease. Looped, from OS threads.
    #[test]
    fn concurrent_transports_share_one_winner_and_one_domain_lease() {
        let runtime = crate::storage::race_runtime();
        let db = runtime.block_on(migrated_test_db("all-surface-ask-race.db"));
        for round in 0..20 {
            let action = format!("prompt-{round}");
            let binding = format!("{action}:0");
            let (rt, db, action, binding) = (&runtime, &db, action.as_str(), binding.as_str());
            let claims = crate::storage::race_on_threads(
                [
                    (
                        "Desktop",
                        cairn_common::identity::AppearanceTransport::AuthenticatedDesktop,
                        None,
                    ),
                    (
                        "Resource",
                        cairn_common::identity::AppearanceTransport::ResourcePatch,
                        Some("cairn://p/CAIRN/1/1/builder"),
                    ),
                ]
                .map(|(answer, transport, actor)| {
                    move || {
                        rt.block_on(claim_ask_resolution(
                            db, binding, answer, transport, None, None, actor, "question", action,
                            10,
                        ))
                        .unwrap()
                    }
                }),
            );
            assert_eq!(
                claims
                    .iter()
                    .filter(|claim| matches!(claim, AskClaim::Won(_)))
                    .count(),
                1,
                "round {round}"
            );
            let answers = claims
                .iter()
                .map(|claim| match claim {
                    AskClaim::Won(value) | AskClaim::Existing(value) => value.answer.as_str(),
                })
                .collect::<Vec<_>>();
            assert_eq!(answers[0], answers[1]);

            let leases = crate::storage::race_on_threads((0..2).map(|_| {
                move || {
                    rt.block_on(try_lease_ask_action(db, action, 11, 100))
                        .unwrap()
                }
            }));
            assert_eq!(leases.iter().flatten().count(), 1, "round {round}");
        }
    }

    #[tokio::test]
    async fn concurrent_multi_question_submissions_share_one_complete_winner() {
        let db = migrated_test_db("multi-question-answer-race.db").await;
        let desktop_response = &stored_answers("Desktop A", "Desktop B");
        let resource_response = &stored_answers("Resource A", "Resource B");

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
        assert!(&winners[0].answer == desktop_response || &winners[0].answer == resource_response);
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
        let desktop = &stored_answers("Desktop A", "Desktop B");
        let channel = &stored_answers("Channel A", "Channel B");

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
        assert!(&winners[0].answer == desktop || &winners[0].answer == channel);
    }

    /// A two-question prompt's answers in their stored, index-keyed form: what
    /// every surface claims with.
    fn stored_answers(first: &str, second: &str) -> String {
        crate::models::question::PromptAnswers::new(
            2,
            [first, second]
                .into_iter()
                .enumerate()
                .map(|(index, answer)| crate::models::question::QuestionAnswer {
                    index,
                    answer: answer.to_string(),
                })
                .collect(),
        )
        .unwrap()
        .to_stored()
    }

    #[tokio::test]
    async fn two_channels_cannot_form_a_hybrid_multi_question_winner() {
        let db = migrated_test_db("two-channel-prompt-race.db").await;
        let imessage = &stored_answers("iMessage A", "iMessage B");
        let discord = &stored_answers("Discord A", "Discord B");

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
        assert!(&winners[0].answer == imessage || &winners[0].answer == discord);
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

    /// A refusal in the shape the resolver mints when only an operator may
    /// decide, quoted through the ledger's own constant so a reworded refusal
    /// cannot leave these tests asserting about a shape nothing produces.
    fn operator_pending() -> String {
        format!("{OPERATOR_PENDING_REFUSAL} only an operator can approve that")
    }

    /// Parking takes an action out of the cadence sweep's reach AND takes the
    /// answer that earned the refusal off the gate, which is what lets the
    /// operator it is waiting for actually answer it.
    #[tokio::test]
    async fn a_parked_domain_action_leaves_both_the_sweep_and_the_gate() {
        let db = migrated_test_db("channel-ledger-domain-park.db").await;
        claim_ask_resolution(
            &db,
            "permission",
            "allow",
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
        assert_eq!(
            record_ask_action_failure(&db, &lease, &operator_pending(), 12)
                .await
                .unwrap(),
            AskActionFailure::Parked
        );

        assert!(
            pending_ask_actions(&db).await.unwrap().is_empty(),
            "a cadence sweep never sees a parked action"
        );
        assert!(
            answers_for_action(&db, "permission")
                .await
                .unwrap()
                .is_empty(),
            "an answer the domain refused does not go on holding the gate"
        );

        // The operator answering is the event that resumes it: their claim takes
        // the vacated gate, which lifts the park and makes the action leasable.
        let claim = claim_ask_resolution(
            &db,
            "permission",
            "deny",
            cairn_common::identity::AppearanceTransport::AuthenticatedOperator,
            None,
            None,
            Some("operator:desk"),
            "permission",
            "permission",
            13,
        )
        .await
        .unwrap();
        assert!(matches!(claim, AskClaim::Won(_)));
        assert!(try_lease_ask_action(&db, "permission", 14, 100)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            pending_ask_actions(&db).await.unwrap(),
            vec![("permission".to_string(), "permission".to_string())]
        );
    }

    /// Parking is worth nothing if a caller that listed the action BEFORE the
    /// park can still lease it afterwards.
    ///
    /// Every provider sweeps on its own cadence, so two routers routinely hold
    /// the same pending action. One of them leases it, gets the refusal, and
    /// parks; the other's lease attempt is by then answering a question that has
    /// already been settled. Honoring it would clear the park, re-attempt the
    /// same refusal against an answer that parking withdrew, and land the action
    /// back in the loop -- now failing for missing provenance instead, once per
    /// lease expiry, which is the same spin wearing a different message.
    #[tokio::test]
    async fn a_stale_sweep_cannot_resurrect_a_parked_domain_action() {
        let db = migrated_test_db("channel-ledger-domain-park-race.db").await;
        claim_ask_resolution(
            &db,
            "permission",
            "allow",
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

        // Two routers list the same action before either has leased it.
        let first = pending_ask_actions(&db).await.unwrap();
        let second = pending_ask_actions(&db).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 1);

        let lease = try_lease_ask_action(&db, "permission", 11, 100)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record_ask_action_failure(&db, &lease, &operator_pending(), 12)
                .await
                .unwrap(),
            AskActionFailure::Parked
        );

        // The second router acts on its snapshot after the park, and again after
        // the lease it would have taken would have expired.
        for now in [13, 200] {
            assert!(
                try_lease_ask_action(&db, "permission", now, 100)
                    .await
                    .unwrap()
                    .is_none(),
                "a parked action is unleasable, however stale the caller's view"
            );
        }
        assert_eq!(
            db.query_one(
                "SELECT attempt_count FROM channel_ask_action WHERE action_ref = 'permission'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            1,
            "the refusal was attempted once and stayed attempted once"
        );
        assert_eq!(
            db.query_one(
                "SELECT parked_at IS NOT NULL FROM channel_ask_action WHERE action_ref = 'permission'",
                (),
                |row| row.i64(0)
            )
            .await
            .unwrap(),
            1,
            "no sweep may clear a park it did not earn"
        );

        // A real answer still resumes it.
        assert!(matches!(
            claim_ask_resolution(
                &db,
                "permission",
                "deny",
                cairn_common::identity::AppearanceTransport::AuthenticatedOperator,
                None,
                None,
                Some("operator:desk"),
                "permission",
                "permission",
                201,
            )
            .await
            .unwrap(),
            AskClaim::Won(_)
        ));
        assert!(try_lease_ask_action(&db, "permission", 202, 100)
            .await
            .unwrap()
            .is_some());
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
        assert_eq!(
            record_ask_action_failure(&db, &lease, "tempdir: no storage space", 11)
                .await
                .unwrap(),
            AskActionFailure::Retryable,
            "a failure time can change stays in the retry loop"
        );
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
        assert!(begin_delivery(&db, failed.id, 19).await.unwrap());
        assert!(mark_failed(&db, failed.id, "offline", 20, Some(25))
            .await
            .unwrap());

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
    async fn sealed_intent_is_expired_at_insert_without_an_error() {
        let db = migrated_test_db("channel-ledger-sealed-intent.db").await;
        assert!(insert_intent_with_status(&db, &intent("sealed"), "expired")
            .await
            .unwrap());

        let sealed = get_by_id(&db, "sealed").await.unwrap().unwrap();
        assert_eq!(sealed.status, "expired");
        assert_eq!(sealed.resolved_at, Some(10));
        assert_eq!(sealed.last_error, None);
        assert_eq!(sealed.attempts, 0);
        assert_eq!(sealed.next_attempt_at, None);
        assert!(!insert_intent(&db, &intent("duplicate")).await.unwrap());
    }

    #[tokio::test]
    async fn failed_attempts_schedule_retry_without_changing_ambiguous_delivery() {
        let db = migrated_test_db("channel-ledger-retry-state.db").await;
        let mut counted = intent("counted");
        counted.binding_ref = "prompt-2:0";
        assert!(insert_intent(&db, &intent("ambiguous")).await.unwrap());
        assert!(insert_intent(&db, &counted).await.unwrap());
        assert!(begin_delivery(&db, "ambiguous", 11).await.unwrap());
        assert!(begin_delivery(&db, "counted", 11).await.unwrap());

        let ambiguous = get_by_id(&db, "ambiguous").await.unwrap().unwrap();
        assert_eq!(ambiguous.attempts, 0);
        assert_eq!(ambiguous.next_attempt_at, None);
        assert_eq!(ambiguous.last_attempt_at, Some(11));
        assert_eq!(
            ambiguous.last_error.as_deref(),
            Some("delivery outcome unknown")
        );

        assert!(mark_failed(&db, "counted", "offline", 12, Some(25))
            .await
            .unwrap());
        let counted = get_by_id(&db, "counted").await.unwrap().unwrap();
        assert_eq!(counted.attempts, 1);
        assert_eq!(counted.next_attempt_at, Some(25));
        assert_eq!(counted.last_attempt_at, Some(12));
        assert_eq!(counted.last_error.as_deref(), Some("offline"));

        // Every recorded failure is an attempt the provider was called for, so
        // each one spends; a failure with no schedule is never re-sent.
        assert!(mark_failed(&db, "counted", "thread is locked", 30, None)
            .await
            .unwrap());
        let terminal = get_by_id(&db, "counted").await.unwrap().unwrap();
        assert_eq!(terminal.attempts, 2);
        assert_eq!(terminal.next_attempt_at, None);
        assert_eq!(terminal.last_attempt_at, Some(30));
    }

    #[tokio::test]
    async fn retry_claim_requires_a_schedule_is_atomic_and_stamps_the_attempt() {
        let db = migrated_test_db("channel-ledger-retry-claim.db").await;
        let mut scheduled = intent("scheduled");
        scheduled.binding_ref = "prompt-2:0";
        assert!(insert_intent(&db, &intent("ambiguous")).await.unwrap());
        assert!(insert_intent(&db, &scheduled).await.unwrap());
        assert!(begin_delivery(&db, "ambiguous", 11).await.unwrap());
        assert!(begin_delivery(&db, "scheduled", 11).await.unwrap());
        assert!(
            mark_failed(&db, "scheduled", "bridge degraded", 12, Some(25))
                .await
                .unwrap()
        );

        assert!(
            !begin_retry_delivery(&db, "ambiguous", 25, 6).await.unwrap(),
            "an ambiguous failed delivery without next_attempt_at must never be retried"
        );
        let (first, second) = tokio::join!(
            begin_retry_delivery(&db, "scheduled", 25, 6),
            begin_retry_delivery(&db, "scheduled", 25, 6),
        );
        assert_eq!(
            [first.unwrap(), second.unwrap()]
                .into_iter()
                .filter(|claimed| *claimed)
                .count(),
            1,
            "clearing next_attempt_at must atomically admit exactly one retry"
        );

        let claimed = get_by_id(&db, "scheduled").await.unwrap().unwrap();
        assert_eq!(claimed.attempts, 1, "the claim itself spends nothing");
        assert_eq!(claimed.next_attempt_at, None);
        assert_eq!(
            claimed.last_attempt_at,
            Some(25),
            "the claim dates the attempt"
        );
        assert_eq!(
            claimed.last_error.as_deref(),
            Some("delivery outcome unknown")
        );
    }

    #[tokio::test]
    async fn undelivered_counts_separate_queued_and_failed_rows() {
        let db = migrated_test_db("channel-ledger-undelivered-counts.db").await;
        let mut failed = intent("failed-count");
        failed.binding_ref = "prompt-2:0";
        assert!(insert_intent(&db, &intent("pending-count")).await.unwrap());
        assert!(insert_intent(&db, &failed).await.unwrap());
        assert!(begin_delivery(&db, failed.id, 11).await.unwrap());

        assert_eq!(
            undelivered_counts(&db, "imessage").await.unwrap(),
            UndeliveredCounts {
                pending: 1,
                failed: 1,
            }
        );
        assert_eq!(
            undelivered_counts(&db, "telegram").await.unwrap(),
            UndeliveredCounts::default()
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
            conversation: None,
            target_uri: None,
            binding_kind: None,
            addressed_by: None,
            message_guid: None,
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

    /// Agent text is a grant to speak in a conversation, not operator attention:
    /// it makes the row an active binding, but no gate can ask for it and it
    /// never makes the row eligible for an attention class.
    #[tokio::test]
    async fn agent_text_is_a_binding_grant_and_never_operator_attention() {
        use super::super::bindings::{
            BindingKind, MESSAGE_CLASS_AGENT_TEXT, MESSAGE_CLASS_QUESTION,
        };
        let db = migrated_test_db("channel-ledger-agent-text.db").await;
        assert!(bind_target(
            &db,
            "telegram",
            "telegram:0",
            "cairn://p/cairn/2",
            BindingKind::Structural,
            MESSAGE_CLASS_AGENT_TEXT,
            10,
        )
        .await
        .unwrap());

        assert_eq!(
            list_active_bindings(&db)
                .await
                .unwrap()
                .into_iter()
                .map(|binding| binding.target_uri)
                .collect::<Vec<_>>(),
            vec!["cairn://p/cairn/2"]
        );
        assert!(list_eligible_bindings(
            &db,
            "telegram",
            "cairn://p/cairn/2",
            MESSAGE_CLASS_QUESTION
        )
        .await
        .unwrap()
        .is_empty());
        assert!(list_eligible_bindings(
            &db,
            "telegram",
            "cairn://p/cairn/2",
            MESSAGE_CLASS_AGENT_TEXT
        )
        .await
        .is_err());
    }

    /// The operator's toggle moves exactly one bit and refuses a mask or a
    /// binding that does not exist.
    #[tokio::test]
    async fn operator_class_toggle_moves_exactly_one_bit() {
        use super::super::bindings::{
            BindingKind, MESSAGE_CLASSES_ALL, MESSAGE_CLASS_AGENT_TEXT, MESSAGE_CLASS_NOTIFY,
        };
        let db = migrated_test_db("channel-ledger-operator-classes.db").await;
        let classes = || async {
            db.query_opt_i64(
                "SELECT message_classes FROM channel_conversation_binding WHERE target_uri = 'cairn://p/cairn/1'",
                (),
            )
            .await
            .unwrap()
        };
        assert!(bind_target(
            &db,
            "telegram",
            "telegram:0",
            "cairn://p/cairn/1",
            BindingKind::Structural,
            MESSAGE_CLASSES_ALL,
            10,
        )
        .await
        .unwrap());

        for (class, granted, expected) in [
            (
                MESSAGE_CLASS_AGENT_TEXT,
                true,
                MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            ),
            (
                MESSAGE_CLASS_NOTIFY,
                false,
                (MESSAGE_CLASSES_ALL & !MESSAGE_CLASS_NOTIFY) | MESSAGE_CLASS_AGENT_TEXT,
            ),
        ] {
            assert!(set_message_class(
                &db,
                "telegram",
                "telegram:0",
                "cairn://p/cairn/1",
                class,
                granted,
            )
            .await
            .unwrap());
            assert_eq!(classes().await, Some(expected));
        }

        assert!(!set_message_class(
            &db,
            "telegram",
            "telegram:0",
            "cairn://p/cairn/2",
            MESSAGE_CLASS_AGENT_TEXT,
            true,
        )
        .await
        .unwrap());
        assert!(set_message_class(
            &db,
            "telegram",
            "telegram:0",
            "cairn://p/cairn/1",
            MESSAGE_CLASSES_ALL,
            true,
        )
        .await
        .is_err());
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
