use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

use super::types::*;

#[derive(Clone, Debug)]
struct DerivedThreadScope {
    source_ref: String,
    fact_kinds: Vec<String>,
}

pub(crate) async fn seed_default_job_subscriptions_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<()> {
    let now = chrono::Utc::now().timestamp();
    for source_kind in ["user", "peer"] {
        conn.execute(
            "INSERT OR IGNORE INTO wake_subscriptions
             (id, job_id, source_kind, source_ref, fact_kinds_json, state,
              created_by, created_at, updated_at, one_shot)
             VALUES (?1, ?2, ?3, NULL, NULL, 'active', 'system', ?4, ?4, 0)",
            params![uuid::Uuid::new_v4().to_string(), job_id, source_kind, now],
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn rebuild_derived_thread_subscriptions_conn(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    triggers: &[crate::routes::TriggerClause],
) -> DbResult<()> {
    let scopes = compile_derived_thread_scopes(triggers).map_err(crate::storage::DbError::Row)?;
    conn.execute(
        "DELETE FROM wake_subscriptions WHERE job_id = ?1 AND id LIKE 'derived:thread:%'",
        params![job_id],
    )
    .await?;
    let now = chrono::Utc::now().timestamp();
    for scope in scopes {
        let id = format!("{DERIVED_THREAD_ID_PREFIX}{}", uuid::Uuid::new_v4());
        let fact_kinds_json = serde_json::to_string(&scope.fact_kinds)
            .map_err(|error| crate::storage::DbError::Row(error.to_string()))?;
        // Persisted provenance cannot use created_by=derived: the shipped,
        // team-synced table constrains that field to agent/user/system and cannot
        // be destructively rebuilt without breaking sync triggers. The reserved
        // ID namespace is therefore the durable ownership marker; system
        // preserves default-row matching precedence.
        conn.execute(
            "INSERT INTO wake_subscriptions
             (id, job_id, source_kind, source_ref, fact_kinds_json, state,
              created_by, created_at, updated_at, one_shot)
             VALUES (?1, ?2, 'issue', ?3, ?4, 'active', 'system', ?5, ?5, 0)",
            params![
                id.as_str(),
                job_id,
                scope.source_ref.as_str(),
                fact_kinds_json.as_str(),
                now
            ],
        )
        .await?;
    }
    Ok(())
}

pub(super) const DERIVED_THREAD_ID_PREFIX: &str = "derived:thread:";

/// Rebuild the durable index for a thread definition's standing triggers.
///
/// Route predicates are richer than wake rows. Compile only predicates whose
/// meaning the current wake schema can preserve exactly; rejecting an
/// unrepresentable clause is safer than waking a thread for facts it did not
/// select.
pub(crate) fn validate_derived_thread_triggers(
    triggers: &[crate::routes::TriggerClause],
) -> Result<(), String> {
    compile_derived_thread_scopes(triggers).map(|_| ())
}

fn compile_derived_thread_scopes(
    triggers: &[crate::routes::TriggerClause],
) -> Result<Vec<DerivedThreadScope>, String> {
    let mut scopes = Vec::new();
    for clause in triggers {
        let fact = clause.get("fact").and_then(serde_json::Value::as_str);
        if fact != Some("attention") {
            return Err(format!(
                "thread trigger fact '{}' is not backed by wake subscriptions",
                fact.unwrap_or("<missing>")
            ));
        }
        let source_ref = clause
            .get("detailUri")
            .and_then(serde_json::Value::as_str)
            .filter(|value| {
                matches!(
                    cairn_common::uri::parse_uri(value),
                    Some(cairn_common::uri::CairnResource::Issue { .. })
                )
            })
            .ok_or_else(|| {
                "thread attention triggers require an exact canonical issue detailUri".to_string()
            })?;
        let statuses = clause
            .get("status")
            .map(|value| match value {
                serde_json::Value::String(value) => vec![value.as_str()],
                serde_json::Value::Array(values) => values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect(),
                _ => Vec::new(),
            })
            .unwrap_or_default();
        let statuses = statuses
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if statuses != std::collections::BTreeSet::from(["closed", "failed", "merged"])
            || clause
                .keys()
                .any(|key| !matches!(key.as_str(), "fact" | "detailUri" | "status"))
        {
            return Err(
                "thread attention triggers currently require exact detailUri and all terminal statuses (merged, closed, and failed)"
                    .to_string(),
            );
        }
        scopes.push(DerivedThreadScope {
            source_ref: source_ref.to_string(),
            fact_kinds: vec!["resolved".to_string()],
        });
    }
    scopes.sort_by(|left, right| left.source_ref.cmp(&right.source_ref));
    scopes.dedup_by(|left, right| {
        left.source_ref == right.source_ref && left.fact_kinds == right.fact_kinds
    });
    Ok(scopes)
}

pub async fn list_subscriptions_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<WakeSubscription>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot, match_phrase
                     FROM wake_subscriptions
                     WHERE job_id = ?1
                     ORDER BY created_at ASC, id ASC",
                    params![job_id.as_str()],
                )
                .await?;
            let mut subscriptions = Vec::new();
            while let Some(row) = rows.next().await? {
                subscriptions.push(subscription_from_row(&row)?);
            }
            Ok(subscriptions)
        })
    })
    .await
    .map_err(|error| format!("Failed to list wake subscriptions: {error}"))
}

async fn exact_subscription(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
) -> Result<Option<WakeSubscription>, String> {
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kinds_json = fact_kinds_json(fact_kinds);
    db.read(|conn| {
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kinds_json = fact_kinds_json.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot, match_phrase
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kinds_json, '') = COALESCE(?4, '')
                     LIMIT 1",
                    params![
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref()
                    ],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| subscription_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(|error| format!("Failed to read wake subscription: {error}"))
}

pub(crate) async fn peek_pending_suppressed_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<SuppressedWake>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { select_pending_suppressed(conn, &job_id).await })
    })
    .await
    .map_err(|error| format!("Failed to peek suppressed wakes: {error}"))
}

async fn select_pending_suppressed(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> DbResult<Vec<SuppressedWake>> {
    let mut rows = conn
        .query(
            "SELECT id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                    occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at
             FROM suppressed_wakes
             WHERE job_id = ?1 AND delivered_at IS NULL
               AND (subscription_id IS NOT NULL OR content IS NULL)
             ORDER BY created_at ASC, id ASC",
            params![job_id],
        )
        .await?;
    let mut notices = Vec::new();
    while let Some(row) = rows.next().await? {
        notices.push(suppressed_from_row(&row)?);
    }
    Ok(notices)
}

async fn subscribe_scope(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    subscribe_scope_inner(db, job_id, scope, created_by, false).await
}

async fn subscribe_scope_inner(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    created_by: &str,
    one_shot: bool,
) -> Result<WakeSubscription, String> {
    upsert_subscription(
        db,
        job_id,
        scope.source.kind(),
        scope.source.reference(),
        scope.fact_kinds.as_deref(),
        WakeSubscriptionState::Active,
        None,
        None,
        created_by,
        one_shot,
        None,
    )
    .await
}

pub(super) async fn seed_scope(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    if let Some(existing) = exact_subscription(
        db,
        job_id,
        scope.source.kind(),
        scope.source.reference(),
        scope.fact_kinds.as_deref(),
    )
    .await?
    {
        return Ok(existing);
    }
    subscribe_scope(db, job_id, scope, created_by).await
}

pub(crate) async fn subscribe(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let scope = WakeScope::new(source, fact_kinds.map(|values| values.to_vec()));
    subscribe_scope(db, job_id, &scope, created_by).await
}

/// Subscribe a one-shot wake: the subscription is consumed (deleted) the first
/// time a matching wake routes to it. Used for terminal-exit subscriptions,
/// which fire exactly once.
pub(crate) async fn subscribe_one_shot(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let scope = WakeScope::new(source, fact_kinds.map(|values| values.to_vec()));
    subscribe_scope_inner(db, job_id, &scope, created_by, true).await
}

/// Subscribe a one-shot phrase watcher on a terminal's output. Persists a
/// `process` source keyed on the canonical terminal URI, carrying both the
/// `terminal_output` and `terminal_exit` fact kinds: it fires when the phrase
/// appears (routed by the live read loop) OR when the terminal exits first, so a
/// build that dies before printing the phrase still wakes the waiting agent
/// instead of stranding it. Re-subscribing the same terminal replaces the phrase
/// (the unique scope index collapses to one output watcher per job+terminal).
pub async fn subscribe_terminal_output_one_shot(
    db: &LocalDb,
    job_id: &str,
    terminal_uri: &str,
    phrase: &str,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let fact_kinds = vec![
        FACT_KIND_TERMINAL_OUTPUT.to_string(),
        FACT_KIND_TERMINAL_EXIT.to_string(),
    ];
    upsert_subscription(
        db,
        job_id,
        SOURCE_KIND_PROCESS,
        Some(terminal_uri),
        Some(&fact_kinds),
        WakeSubscriptionState::Active,
        None,
        None,
        created_by,
        true,
        Some(phrase),
    )
    .await
}

/// Load the active one-shot output-phrase watchers persisted for a terminal's
/// canonical URI, returned as `(subscription_id, job_id, phrase)` tuples. A
/// (re)starting PTY session calls this to re-attach the in-memory watchers its
/// read loop scans, so an output subscription is durable across sessions:
/// it survives the worktree-fence approval respawn (which tears down the
/// original session) and a subscribe made while no session was live is honored
/// by the next session. The `wake_subscriptions` row is the source of truth;
/// the in-memory watcher list is only a per-session cache.
pub(crate) async fn list_terminal_output_watchers(
    db: &LocalDb,
    terminal_uri: &str,
) -> Result<Vec<(String, String, String, String)>, String> {
    let terminal_uri = terminal_uri.to_string();
    db.read(|conn| {
        let terminal_uri = terminal_uri.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, match_phrase, source_ref
                     FROM wake_subscriptions
                     WHERE source_kind = ?1 AND source_ref = ?2
                       AND state = 'active' AND match_phrase IS NOT NULL",
                    params![SOURCE_KIND_PROCESS, terminal_uri.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let Some(phrase) = row.opt_text(2)? else {
                    continue;
                };
                out.push((row.text(0)?, row.text(1)?, phrase, row.text(3)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to list terminal output watchers: {error}"))
}

/// Like `list_terminal_output_watchers` but resolved by the owning job and the
/// terminal's slug rather than the full canonical URI. The interactive terminal
/// reader knows `job_id` + `slug` (not the canonical node URI) and uses this to
/// hydrate its watcher registry at session start. Subscriptions are always
/// created in the caller's own job scope, so `job_id` plus the trailing
/// `/terminal/<slug>` segment uniquely identify the terminal. Returns
/// `(subscription_id, job_id, phrase, terminal_uri)` tuples.
pub async fn list_terminal_output_watchers_for_job_terminal(
    db: &LocalDb,
    job_id: &str,
    slug: &str,
) -> Result<Vec<(String, String, String, String)>, String> {
    let job_id = job_id.to_string();
    let like = format!("%/terminal/{slug}");
    db.read(|conn| {
        let job_id = job_id.clone();
        let like = like.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, job_id, match_phrase, source_ref
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND state = 'active' AND match_phrase IS NOT NULL
                       AND source_ref LIKE ?3",
                    params![job_id.as_str(), SOURCE_KIND_PROCESS, like.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let Some(phrase) = row.opt_text(2)? else {
                    continue;
                };
                out.push((row.text(0)?, row.text(1)?, phrase, row.text(3)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to list terminal output watchers: {error}"))
}

/// Whether any job holds an active subscription on this exact source.
///
/// A gate, not a router: it exists so an edge that must BUILD an expensive
/// payload before it can route (the checks-settlement snapshot reads the
/// repository) can find out first that nobody is listening. Every node's every
/// turn end passes through that edge, and almost none of them are watched.
pub(crate) async fn any_active_subscriber(
    db: &LocalDb,
    source_kind: &str,
    source_ref: &str,
) -> Result<bool, String> {
    let (source_kind, source_ref) = (source_kind.to_string(), source_ref.to_string());
    db.read(|conn| {
        let (source_kind, source_ref) = (source_kind.clone(), source_ref.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM wake_subscriptions
                     WHERE source_kind = ?1 AND source_ref = ?2 AND state = 'active'
                     LIMIT 1",
                    params![source_kind.as_str(), source_ref.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub(crate) async fn seed_default_job_subscriptions(
    db: &LocalDb,
    job_id: &str,
) -> Result<(), String> {
    seed_scope(
        db,
        job_id,
        &WakeScope::new(WakeSource::User, None),
        "system",
    )
    .await?;
    seed_scope(
        db,
        job_id,
        &WakeScope::new(WakeSource::Peer { reference: None }, None),
        "system",
    )
    .await?;
    Ok(())
}

async fn mute_scope(
    db: &LocalDb,
    job_id: &str,
    scope: &WakeScope,
    until: Option<&WakeSource>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    upsert_subscription(
        db,
        job_id,
        scope.source.kind(),
        scope.source.reference(),
        scope.fact_kinds.as_deref(),
        WakeSubscriptionState::Muted,
        until.map(WakeSource::kind),
        until.and_then(WakeSource::reference),
        created_by,
        false,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn mute(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    until_kind: Option<&str>,
    until_ref: Option<&str>,
    created_by: &str,
) -> Result<WakeSubscription, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let until = match until_kind {
        Some(kind) => Some(WakeSource::from_parts(kind, until_ref)?),
        None => None,
    };
    let scope = WakeScope::new(source, fact_kinds.map(|values| values.to_vec()));
    mute_scope(db, job_id, &scope, until.as_ref(), created_by).await
}

pub(crate) async fn unmute_matching(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
) -> Result<usize, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    update_state_matching(
        db,
        job_id,
        source.kind(),
        source.reference(),
        WakeSubscriptionState::Active,
    )
    .await
}

/// Unsubscribe a job from a source.
///
/// Flips every matching row to `unsubscribed`, and when a scoped source matched
/// nothing it *records* the opt-out as a row instead of reporting a silent no-op.
/// That tombstone is what makes opting out of a **derived** watch possible: a
/// coordinator's child-attention watch is derived from the parent edge and has no
/// row to flip, so without a recorded refusal the derivation would keep
/// re-adding it (CAIRN-3293). An unscoped unsubscribe (`ref` omitted, meaning
/// "every row of this kind") stays a pure state flip — there is no single source
/// to tombstone.
pub(crate) async fn unsubscribe_matching(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    created_by: &str,
) -> Result<usize, String> {
    let source = WakeSource::from_parts(source_kind, source_ref)?;
    let changed = update_state_matching(
        db,
        job_id,
        source.kind(),
        source.reference(),
        WakeSubscriptionState::Unsubscribed,
    )
    .await?;
    if changed > 0 {
        return Ok(changed);
    }
    let Some(reference) = source.reference() else {
        return Ok(0);
    };
    upsert_subscription(
        db,
        job_id,
        source.kind(),
        Some(reference),
        None,
        WakeSubscriptionState::Unsubscribed,
        None,
        None,
        created_by,
        false,
        None,
    )
    .await?;
    Ok(1)
}

async fn update_state_matching(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    state: WakeSubscriptionState,
) -> Result<usize, String> {
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let now = chrono::Utc::now().timestamp();
    let state_str = state.as_str().to_string();
    db.write(|conn| {
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let state_str = state_str.clone();
        Box::pin(async move {
            let changed = match source_ref.as_deref() {
                Some(source_ref) => {
                    conn.execute(
                        "UPDATE wake_subscriptions SET state = ?1, updated_at = ?2
                         WHERE job_id = ?3 AND source_kind = ?4 AND source_ref = ?5",
                        params![
                            state_str.as_str(),
                            now,
                            job_id.as_str(),
                            source_kind.as_str(),
                            source_ref
                        ],
                    )
                    .await?
                }
                None => {
                    conn.execute(
                        "UPDATE wake_subscriptions SET state = ?1, updated_at = ?2
                         WHERE job_id = ?3 AND source_kind = ?4",
                        params![
                            state_str.as_str(),
                            now,
                            job_id.as_str(),
                            source_kind.as_str()
                        ],
                    )
                    .await?
                }
            };
            Ok(changed as usize)
        })
    })
    .await
    .map_err(|error| format!("Failed to update wake subscriptions: {error}"))
}

#[allow(clippy::too_many_arguments)]
async fn upsert_subscription(
    db: &LocalDb,
    job_id: &str,
    source_kind: &str,
    source_ref: Option<&str>,
    fact_kinds: Option<&[String]>,
    state: WakeSubscriptionState,
    until_kind: Option<&str>,
    until_ref: Option<&str>,
    created_by: &str,
    one_shot: bool,
    match_phrase: Option<&str>,
) -> Result<WakeSubscription, String> {
    let id = uuid::Uuid::new_v4().to_string();
    let job_id = job_id.to_string();
    let source_kind = source_kind.to_string();
    let source_ref = source_ref.map(ToString::to_string);
    let fact_kinds_json = fact_kinds_json(fact_kinds);
    let until_kind = until_kind.map(ToString::to_string);
    let until_ref = until_ref.map(ToString::to_string);
    let created_by = created_by.to_string();
    let state_str = state.as_str().to_string();
    let match_phrase = match_phrase.map(ToString::to_string);
    let one_shot_int: i64 = if one_shot { 1 } else { 0 };
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let id = id.clone();
        let job_id = job_id.clone();
        let source_kind = source_kind.clone();
        let source_ref = source_ref.clone();
        let fact_kinds_json = fact_kinds_json.clone();
        let until_kind = until_kind.clone();
        let until_ref = until_ref.clone();
        let created_by = created_by.clone();
        let state_str = state_str.clone();
        let match_phrase = match_phrase.clone();
        Box::pin(async move {
            let mut existing = conn
                .query(
                    "SELECT id
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kinds_json, '') = COALESCE(?4, '')
                     LIMIT 1",
                    params![
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref()
                    ],
                )
                .await?;
            let existing_id = existing.next().await?.map(|row| row.text(0)).transpose()?;
            drop(existing);
            if let Some(existing_id) = existing_id {
                conn.execute(
                    "UPDATE wake_subscriptions
                     SET state = ?1, mute_until_kind = ?2, mute_until_ref = ?3, updated_at = ?4,
                         one_shot = ?6, match_phrase = ?7
                     WHERE id = ?5",
                    params![
                        state_str.as_str(),
                        until_kind.as_deref(),
                        until_ref.as_deref(),
                        now,
                        existing_id.as_str(),
                        one_shot_int,
                        match_phrase.as_deref()
                    ],
                )
                .await?;
            } else {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                     (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                      mute_until_kind, mute_until_ref, created_by, created_at, updated_at, one_shot,
                      match_phrase)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12)",
                    params![
                        id.as_str(),
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref(),
                        state_str.as_str(),
                        until_kind.as_deref(),
                        until_ref.as_deref(),
                        created_by.as_str(),
                        now,
                        one_shot_int,
                        match_phrase.as_deref()
                    ],
                )
                .await?;
            }
            let mut rows = conn
                .query(
                    "SELECT id, job_id, source_kind, source_ref, fact_kinds_json, state,
                            mute_until_kind, mute_until_ref, created_by, created_at, updated_at,
                            one_shot, match_phrase
                     FROM wake_subscriptions
                     WHERE job_id = ?1 AND source_kind = ?2
                       AND COALESCE(source_ref, '') = COALESCE(?3, '')
                       AND COALESCE(fact_kinds_json, '') = COALESCE(?4, '')
                     LIMIT 1",
                    params![
                        job_id.as_str(),
                        source_kind.as_str(),
                        source_ref.as_deref(),
                        fact_kinds_json.as_deref()
                    ],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                crate::storage::DbError::Row("missing wake subscription".to_string())
            })?;
            subscription_from_row(&row)
        })
    })
    .await
    .map_err(|error| format!("Failed to upsert wake subscription: {error}"))
}
