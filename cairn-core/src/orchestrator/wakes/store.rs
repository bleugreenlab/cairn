use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

use super::types::*;

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

pub async fn peek_pending_suppressed_for_job(
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

pub async fn subscribe_scope(
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

pub async fn subscribe(
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
pub async fn subscribe_one_shot(
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
pub async fn list_terminal_output_watchers(
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

pub async fn seed_default_job_subscriptions(db: &LocalDb, job_id: &str) -> Result<(), String> {
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

pub async fn mute_scope(
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
pub async fn mute(
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

pub async fn unmute_matching(
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

pub async fn unsubscribe_matching(
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
        WakeSubscriptionState::Unsubscribed,
    )
    .await
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
