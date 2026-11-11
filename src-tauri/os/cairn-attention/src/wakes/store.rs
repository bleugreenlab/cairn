use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

use super::types::*;

#[derive(Clone, Debug)]
struct DerivedThreadScope {
    source_kind: &'static str,
    source_ref: String,
    fact_kinds: Vec<String>,
}

pub async fn seed_default_job_subscriptions_conn(
    conn: &cairn_db::storage::TrackedConnection,
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

pub async fn rebuild_derived_thread_subscriptions_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
    triggers: &[std::collections::BTreeMap<String, serde_json::Value>],
) -> DbResult<()> {
    let scopes = compile_derived_thread_scopes(triggers).map_err(crate::storage::DbError::Row)?;
    // A rebuild re-derives the same scopes far more often than it changes
    // them, so a state the node set on a derived row (a mute, an opt-out)
    // carries over to the row that re-derives its scope.
    let mut kept_states = std::collections::HashMap::new();
    {
        let mut rows = conn
            .query(
                "SELECT source_kind, COALESCE(source_ref, ''), COALESCE(fact_kinds_json, ''), state
                   FROM wake_subscriptions WHERE job_id = ?1 AND id LIKE 'derived:thread:%'",
                params![job_id],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            kept_states.insert((row.text(0)?, row.text(1)?, row.text(2)?), row.text(3)?);
        }
    }
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
        // A scope the node already holds as a row of its own (an explicit or
        // migrated Posts watch is exactly `{fact: post_created}`) is governed
        // by that row, mute and opt-out included, as an explicit issue row
        // governs the derived parent watch. Inserting a second would also
        // violate the unique scope index.
        let held = {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM wake_subscriptions
                      WHERE job_id = ?1 AND source_kind = ?2
                        AND COALESCE(source_ref, '') = ?3 AND COALESCE(fact_kinds_json, '') = ?4
                      LIMIT 1",
                    params![
                        job_id,
                        scope.source_kind,
                        scope.source_ref.as_str(),
                        fact_kinds_json.as_str()
                    ],
                )
                .await?;
            rows.next().await?.is_some()
        };
        if held {
            continue;
        }
        let state = kept_states
            .get(&(
                scope.source_kind.to_string(),
                scope.source_ref.clone(),
                fact_kinds_json.clone(),
            ))
            .cloned()
            .unwrap_or_else(|| "active".to_string());
        // Persisted provenance cannot use created_by=derived: the shipped,
        // team-synced table constrains that field to agent/user/system and cannot
        // be destructively rebuilt without breaking sync triggers. The reserved
        // ID namespace is therefore the durable ownership marker; system
        // preserves default-row matching precedence.
        conn.execute(
            "INSERT INTO wake_subscriptions
             (id, job_id, source_kind, source_ref, fact_kinds_json, state,
              created_by, created_at, updated_at, one_shot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?7, 'system', ?6, ?6, 0)",
            params![
                id.as_str(),
                job_id,
                scope.source_kind,
                scope.source_ref.as_str(),
                fact_kinds_json.as_str(),
                now,
                state.as_str()
            ],
        )
        .await?;
    }
    Ok(())
}

pub const DERIVED_THREAD_ID_PREFIX: &str = "derived:thread:";

/// Check that a thread definition's standing triggers compile to wake rows.
/// Field vocabulary is the fact registry's to judge, and the definition
/// validates against it before this runs.
pub fn validate_derived_thread_triggers(
    triggers: &[std::collections::BTreeMap<String, serde_json::Value>],
) -> Result<(), String> {
    compile_derived_thread_scopes(triggers).map(|_| ())
}

/// The issue a clause watches for resolution, when the clause is exactly that
/// watch: attention on one canonical issue, at every terminal status. That
/// shape keeps its issue-sourced `resolved` row, the same subscription an
/// agent's explicit issue watch holds, with its push semantics.
fn issue_resolution_watch(
    clause: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Option<String> {
    if clause.get("fact").and_then(serde_json::Value::as_str) != Some("attention")
        || clause
            .keys()
            .any(|key| !matches!(key.as_str(), "fact" | "detailUri" | "status"))
    {
        return None;
    }
    let issue = clause
        .get("detailUri")
        .and_then(serde_json::Value::as_str)
        .filter(|value| {
            matches!(
                cairn_uri::parse_uri(value),
                Some(cairn_uri::CairnResource::Issue { .. })
            )
        })?;
    let statuses = match clause.get("status")? {
        serde_json::Value::String(value) => vec![value.as_str()],
        serde_json::Value::Array(values) => values
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect(),
        _ => return None,
    };
    (statuses
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        == std::collections::BTreeSet::from(["closed", "failed", "merged"]))
    .then(|| cairn_uri::canonicalize_uri_identity(issue))
}

/// The reference a `fact` subscription is stored and delivered under: the
/// clause's canonical JSON, keys sorted. One canonicalization, so two
/// spellings of one clause are one row, and the fact log worker's wake names
/// exactly the row it matched.
pub fn fact_clause_ref(
    clause: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<String, String> {
    let mut canonical = clause.clone();
    if clause.get("fact").and_then(serde_json::Value::as_str) == Some("request") {
        let raw = clause
            .get("request")
            .and_then(serde_json::Value::as_str)
            .ok_or("request fact subscription requires a request URI")?;
        let Some(resource @ cairn_uri::CairnResource::Request { .. }) = cairn_uri::parse_uri(raw)
        else {
            return Err("request fact subscription requires cairn://requests/<id>".into());
        };
        let uri = resource.to_uri();
        if raw != uri {
            return Err(
                "request fact subscription requires a canonical URI without query or alias".into(),
            );
        }
        canonical.insert("request".into(), serde_json::Value::String(uri));
    }
    serde_json::to_string(&canonical).map_err(|error| error.to_string())
}

fn compile_derived_thread_scopes(
    triggers: &[std::collections::BTreeMap<String, serde_json::Value>],
) -> Result<Vec<DerivedThreadScope>, String> {
    let mut scopes = Vec::new();
    for clause in triggers {
        let fact = clause
            .get("fact")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "each thread trigger requires a scalar 'fact'".to_string())?;
        if let Some(issue) = issue_resolution_watch(clause) {
            scopes.push(DerivedThreadScope {
                source_kind: SOURCE_KIND_ISSUE,
                source_ref: issue,
                fact_kinds: vec!["resolved".to_string()],
            });
            continue;
        }
        // Every other clause is a standing trigger over the fact log.
        scopes.push(DerivedThreadScope {
            source_kind: SOURCE_KIND_FACT,
            source_ref: fact_clause_ref(clause)?,
            fact_kinds: vec![fact.to_string()],
        });
    }
    scopes.sort_by(|left, right| {
        (left.source_kind, &left.source_ref).cmp(&(right.source_kind, &right.source_ref))
    });
    scopes.dedup_by(|left, right| {
        left.source_kind == right.source_kind
            && left.source_ref == right.source_ref
            && left.fact_kinds == right.fact_kinds
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
    let source_ref = source_ref.map(cairn_uri::canonicalize_uri_identity);
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
    conn: &cairn_db::storage::TrackedConnection,
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

pub async fn seed_scope(
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

/// Whether any job holds an active subscription on this exact source.
///
/// A gate, not a router: it exists so an edge that must BUILD an expensive
/// payload before it can route (the checks-settlement snapshot reads the
/// repository) can find out first that nobody is listening. Every node's every
/// turn end passes through that edge, and almost none of them are watched.
pub async fn any_active_subscriber(
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
pub async fn unsubscribe_matching(
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
    let source_ref = source_ref.map(cairn_uri::canonicalize_uri_identity);
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
    let source_ref = source_ref.map(cairn_uri::canonicalize_uri_identity);
    let fact_kinds_json = fact_kinds_json(fact_kinds);
    let until_kind = until_kind.map(ToString::to_string);
    let until_ref = until_ref.map(cairn_uri::canonicalize_uri_identity);
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

// ── Delivery receipts ──
//
// One row per (fact, stable recipient), in the database where its delivery
// settles. See `0423_delivery_receipts.sql` for the column contract and
// `docs/wakes.md` for the settlement protocol.

/// Receipt states. Only `queued` may change level or reasons; `delivering`
/// is bound to the message its settlement writes; the other three are final.
pub mod receipt_state {
    pub const QUEUED: &str = "queued";
    pub const DELIVERING: &str = "delivering";
    pub const DELIVERED: &str = "delivered";
    pub const WITHDRAWN: &str = "withdrawn";
    pub const DROPPED: &str = "dropped";
}

/// One delivery receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub id: String,
    pub fact_key: String,
    pub recipient: super::recipients::StableRecipient,
    pub project_id: Option<String>,
    pub fact_row_id: Option<i64>,
    pub level: crate::push::Wake,
    pub reasons: Vec<super::recipients::Reached>,
    pub state: String,
    pub wake_id: Option<i64>,
    pub message_id: Option<String>,
    pub delivered_job_id: Option<String>,
    pub push_id: Option<String>,
    pub last_refusal: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// The structured facts of a fact with no row of its own to render from
    /// (a one-shot subscription's firing); `None` for a logged fact or a push.
    pub fact_json: Option<String>,
}

const RECEIPT_COLUMNS: &str = "id, fact_key, recipient_kind, recipient_id, project_id, fact_row_id, level, reasons_json, state, wake_id, message_id, delivered_job_id, push_id, last_refusal, created_at, updated_at, fact_json";

fn receipt_from_row(row: &cairn_db::turso::Row) -> DbResult<Receipt> {
    let kind = row.text(2)?;
    let id = row.text(3)?;
    let recipient =
        super::recipients::StableRecipient::from_parts(&kind, &id).ok_or_else(|| {
            crate::storage::DbError::Row(format!("unknown receipt recipient kind {kind:?}"))
        })?;
    let level_text = row.text(6)?;
    let level = crate::push::Wake::from_db(&level_text).ok_or_else(|| {
        crate::storage::DbError::Row(format!("unknown receipt level {level_text:?}"))
    })?;
    let reasons = serde_json::from_str(&row.text(7)?)
        .map_err(|error| crate::storage::DbError::Row(format!("receipt reasons: {error}")))?;
    Ok(Receipt {
        id: row.text(0)?,
        fact_key: row.text(1)?,
        recipient,
        project_id: row.opt_text(4)?,
        fact_row_id: row.opt_i64(5)?,
        level,
        reasons,
        state: row.text(8)?,
        wake_id: row.opt_i64(9)?,
        message_id: row.opt_text(10)?,
        delivered_job_id: row.opt_text(11)?,
        push_id: row.opt_text(12)?,
        last_refusal: row.opt_text(13)?,
        created_at: row.i64(14)?,
        updated_at: row.i64(15)?,
        fact_json: row.opt_text(16)?,
    })
}

fn receipt_now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A receipt to write for one resolved candidate.
#[derive(Clone, Debug)]
pub struct NewReceipt<'a> {
    pub fact_key: &'a str,
    pub candidate: &'a super::recipients::Candidate,
    pub project_id: Option<&'a str>,
    /// The logged row this fact is, or the row it is delivered beside: a
    /// recipient's receipts sharing a row go in one message.
    pub fact_row_id: Option<i64>,
    /// See [`Receipt::fact_json`].
    pub fact_json: Option<&'a str>,
}

/// What writing a receipt found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptWrite {
    /// No receipt existed; this one is new and queued.
    Inserted(Receipt),
    /// A queued receipt existed; its level and reasons now include this
    /// derivation's.
    Raised(Receipt),
    /// The receipt was already consumed (delivered, withdrawn, dropped) or is
    /// bound to a settlement: a second derivation or a re-mint never delivers
    /// twice, and a consumed receipt is never re-opened.
    Consumed(Receipt),
}

impl ReceiptWrite {
    pub fn receipt(&self) -> &Receipt {
        match self {
            Self::Inserted(receipt) | Self::Raised(receipt) | Self::Consumed(receipt) => receipt,
        }
    }

    /// Whether the receipt is still owed.
    pub fn is_owed(&self) -> bool {
        !matches!(self, Self::Consumed(_))
    }
}

pub async fn receipt_conn(
    conn: &crate::storage::TrackedConnection,
    fact_key: &str,
    recipient: &super::recipients::StableRecipient,
) -> DbResult<Option<Receipt>> {
    let mut rows = conn
        .query(
            format!(
                "SELECT {RECEIPT_COLUMNS} FROM delivery_receipts
                 WHERE fact_key = ?1 AND recipient_kind = ?2 AND recipient_id = ?3 LIMIT 1"
            ),
            cairn_db::turso::params![fact_key, recipient.kind(), recipient.id()],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(receipt_from_row(&row)?)),
        None => Ok(None),
    }
}

/// Write the receipt `new` owes, or fold its reasons into the queued one that
/// already exists: `level = max(level, new)`, reasons unioned. A receipt past
/// `queued` is left exactly as it is.
pub async fn upsert_receipt_conn(
    conn: &crate::storage::TrackedConnection,
    new: &NewReceipt<'_>,
) -> DbResult<ReceiptWrite> {
    let recipient = &new.candidate.recipient;
    let now = receipt_now_ms();
    let existing = receipt_conn(conn, new.fact_key, recipient).await?;
    let Some(existing) = existing else {
        let id = uuid::Uuid::new_v4().to_string();
        let reasons = serde_json::to_string(&new.candidate.reasons)
            .map_err(|error| crate::storage::DbError::Row(error.to_string()))?;
        conn.execute(
            "INSERT INTO delivery_receipts
               (id, fact_key, recipient_kind, recipient_id, project_id, fact_row_id, level,
                reasons_json, state, created_at, updated_at, fact_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', ?9, ?9, ?10)",
            cairn_db::turso::params![
                id.as_str(),
                new.fact_key,
                recipient.kind(),
                recipient.id(),
                new.project_id,
                new.fact_row_id,
                new.candidate.level.as_str(),
                reasons.as_str(),
                now,
                new.fact_json
            ],
        )
        .await?;
        return Ok(ReceiptWrite::Inserted(Receipt {
            id,
            fact_key: new.fact_key.to_string(),
            recipient: recipient.clone(),
            project_id: new.project_id.map(str::to_owned),
            fact_row_id: new.fact_row_id,
            level: new.candidate.level,
            reasons: new.candidate.reasons.clone(),
            state: receipt_state::QUEUED.to_string(),
            wake_id: None,
            message_id: None,
            delivered_job_id: None,
            push_id: None,
            last_refusal: None,
            created_at: now,
            updated_at: now,
            fact_json: new.fact_json.map(str::to_owned),
        }));
    };
    if existing.state != receipt_state::QUEUED {
        return Ok(ReceiptWrite::Consumed(existing));
    }
    let mut reasons = existing.reasons.clone();
    for reached in &new.candidate.reasons {
        match reasons
            .iter_mut()
            .find(|known| known.reason == reached.reason)
        {
            Some(known) => known.level = known.level.max(reached.level),
            None => reasons.push(reached.clone()),
        }
    }
    let level = existing.level.max(new.candidate.level);
    if reasons == existing.reasons && level == existing.level {
        return Ok(ReceiptWrite::Raised(existing));
    }
    let reasons_json = serde_json::to_string(&reasons)
        .map_err(|error| crate::storage::DbError::Row(error.to_string()))?;
    conn.execute(
        "UPDATE delivery_receipts SET level = ?2, reasons_json = ?3, updated_at = ?4
          WHERE id = ?1 AND state = 'queued'",
        cairn_db::turso::params![existing.id.as_str(), level.as_str(), reasons_json, now],
    )
    .await?;
    Ok(ReceiptWrite::Raised(Receipt {
        level,
        reasons,
        updated_at: now,
        ..existing
    }))
}

/// Write the receipt a logged fact owes one recipient and fold the fact's row
/// into that recipient's pending wake, in one transaction: a receipt never
/// exists without the wake that carries it. A row is folded only when its
/// receipt is new, so indexing a row again (a restart mid-index) neither
/// duplicates the receipt nor counts the row twice; a receipt that gained a
/// louder reason raises its pending wake. Returns the receipt's write and the
/// wake that carries it.
pub async fn queue_fact_receipt(
    db: &LocalDb,
    new: &NewReceipt<'_>,
    matched: &cairn_db::storage::WakeMatch,
    window: cairn_db::storage::WakeWindow,
) -> DbResult<(ReceiptWrite, Option<i64>)> {
    let fact_key = new.fact_key.to_string();
    let candidate = new.candidate.clone();
    let project_id = new.project_id.map(str::to_owned);
    let fact_row_id = new.fact_row_id;
    let fact_json = new.fact_json.map(str::to_owned);
    let matched = matched.clone();
    db.write(move |conn| {
        let (fact_key, candidate, project_id, fact_json, matched) = (
            fact_key.clone(),
            candidate.clone(),
            project_id.clone(),
            fact_json.clone(),
            matched.clone(),
        );
        Box::pin(async move {
            let new = NewReceipt {
                fact_key: &fact_key,
                candidate: &candidate,
                project_id: project_id.as_deref(),
                fact_row_id,
                fact_json: fact_json.as_deref(),
            };
            queue_receipt_conn(conn, &new, &matched, window, None).await
        })
    })
    .await
}

/// [`queue_fact_receipt`] inside the caller's transaction, so a producer can
/// commit the receipt with the state change that owes it (a one-shot
/// subscription spent as its firing is written).
///
/// A new receipt that shares its row with a receipt the recipient already has
/// queued joins that receipt's pending wake rather than `matched`'s, so both
/// go in one message at their maximum level whichever was written first.
/// `not_before_ms` keeps the carrying wake from coming due before then: the
/// drain's pacing for a burst of turn-launching deliveries.
pub async fn queue_receipt_conn(
    conn: &crate::storage::TrackedConnection,
    new: &NewReceipt<'_>,
    matched: &cairn_db::storage::WakeMatch,
    window: cairn_db::storage::WakeWindow,
    not_before_ms: Option<i64>,
) -> DbResult<(ReceiptWrite, Option<i64>)> {
    let write = upsert_receipt_conn(conn, new).await?;
    let wake_id = match &write {
        ReceiptWrite::Inserted(receipt) => {
            let wake_id = match pending_sibling_wake_conn(conn, receipt).await? {
                Some(wake_id) => {
                    cairn_db::storage::raise_fact_wake_level_conn(
                        conn,
                        wake_id,
                        receipt.level.as_str(),
                        &matched.clause_ref,
                    )
                    .await?;
                    wake_id
                }
                None => cairn_db::storage::coalesce_fact_wake_conn(conn, matched, window).await?,
            };
            attach_receipt_to_wake_conn(conn, &receipt.id, wake_id).await?;
            Some(wake_id)
        }
        ReceiptWrite::Raised(receipt) => {
            if let Some(wake_id) = receipt.wake_id {
                cairn_db::storage::raise_fact_wake_level_conn(
                    conn,
                    wake_id,
                    receipt.level.as_str(),
                    &matched.clause_ref,
                )
                .await?;
            }
            receipt.wake_id
        }
        ReceiptWrite::Consumed(receipt) => receipt.wake_id,
    };
    if let (Some(wake_id), Some(at), true) = (wake_id, not_before_ms, write.is_owed()) {
        cairn_db::storage::hold_fact_wake_until_conn(conn, wake_id, at).await?;
    }
    Ok((write, wake_id))
}

/// The pending wake carrying another queued receipt of the same recipient for
/// the same row, if there is one.
async fn pending_sibling_wake_conn(
    conn: &crate::storage::TrackedConnection,
    receipt: &Receipt,
) -> DbResult<Option<i64>> {
    let Some(row_id) = receipt.fact_row_id else {
        return Ok(None);
    };
    let mut rows = conn
        .query(
            "SELECT r.wake_id FROM delivery_receipts r JOIN fact_log_wakes w ON w.id = r.wake_id
              WHERE r.fact_row_id = ?1 AND r.recipient_kind = ?2 AND r.recipient_id = ?3
                AND r.id != ?4 AND r.state = 'queued' AND w.delivered_at IS NULL
              ORDER BY w.id LIMIT 1",
            cairn_db::turso::params![
                row_id,
                receipt.recipient.kind(),
                receipt.recipient.id(),
                receipt.id.as_str()
            ],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.i64(0)?)),
        None => Ok(None),
    }
}

/// Whether a queued receipt was already carried by a message that told its
/// recipient about the same row at least as loudly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Coverage {
    /// Nothing delivered covers it; it is owed.
    Owed,
    /// A delivered receipt for the same row and recipient, at a level at
    /// least this one's: delivering again would repeat that message.
    Covered(Box<Receipt>),
    /// Such a receipt is mid-settlement; ask again once it settles.
    InFlight,
}

/// Judge `receipt` against the recipient's other receipts for its row. A
/// louder receipt is never covered by a quieter one: a thread that read a
/// camp line passively is still woken by the wake it subscribed to.
pub async fn receipt_coverage(db: &LocalDb, receipt: &Receipt) -> DbResult<Coverage> {
    let Some(row_id) = receipt.fact_row_id else {
        return Ok(Coverage::Owed);
    };
    let siblings = db
        .query_all(
            format!(
                "SELECT {RECEIPT_COLUMNS} FROM delivery_receipts
                  WHERE fact_row_id = ?1 AND recipient_kind = ?2 AND recipient_id = ?3
                    AND id != ?4 AND state IN ('delivering', 'delivered')
                  ORDER BY created_at, id"
            ),
            (
                row_id,
                receipt.recipient.kind().to_string(),
                receipt.recipient.id().to_string(),
                receipt.id.clone(),
            ),
            receipt_from_row,
        )
        .await?;
    let mut in_flight = false;
    for sibling in siblings {
        if sibling.level < receipt.level {
            continue;
        }
        if sibling.state == receipt_state::DELIVERED {
            return Ok(Coverage::Covered(Box::new(sibling)));
        }
        in_flight = true;
    }
    Ok(if in_flight {
        Coverage::InFlight
    } else {
        Coverage::Owed
    })
}

/// Bind a receipt to the coalesced wake that will carry it.
pub async fn attach_receipt_to_wake_conn(
    conn: &crate::storage::TrackedConnection,
    receipt_id: &str,
    wake_id: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE delivery_receipts SET wake_id = ?2 WHERE id = ?1 AND state = 'queued'",
        cairn_db::turso::params![receipt_id, wake_id],
    )
    .await?;
    Ok(())
}

/// The receipts a wake still owes or is settling, oldest first.
pub async fn receipts_for_wake(db: &LocalDb, wake_id: i64) -> DbResult<Vec<Receipt>> {
    db.query_all(
        format!(
            "SELECT {RECEIPT_COLUMNS} FROM delivery_receipts INDEXED BY idx_delivery_receipts_wake
              WHERE wake_id = ?1 AND state IN ('queued', 'delivering')
              ORDER BY created_at, id"
        ),
        (wake_id,),
        receipt_from_row,
    )
    .await
}

/// Rewrite a queued receipt's reasons after re-validation at delivery.
pub async fn relevel_receipt(
    db: &LocalDb,
    receipt_id: &str,
    level: crate::push::Wake,
    reasons: &[super::recipients::Reached],
) -> DbResult<()> {
    let reasons = serde_json::to_string(reasons)
        .map_err(|error| crate::storage::DbError::Row(error.to_string()))?;
    db.execute(
        "UPDATE delivery_receipts SET level = ?2, reasons_json = ?3, updated_at = ?4
          WHERE id = ?1 AND state = 'queued'",
        cairn_db::turso::params![
            receipt_id.to_string(),
            level.as_str(),
            reasons,
            receipt_now_ms()
        ],
    )
    .await?;
    Ok(())
}

/// [`close_receipts`] inside the caller's transaction.
pub async fn close_receipts_conn(
    conn: &crate::storage::TrackedConnection,
    receipt_ids: &[String],
    state: &str,
    why: &str,
) -> DbResult<()> {
    let now = receipt_now_ms();
    for id in receipt_ids {
        conn.execute(
            "UPDATE delivery_receipts SET state = ?2, last_refusal = ?3, updated_at = ?4, settled_at = ?4
              WHERE id = ?1 AND state IN ('queued', 'delivering')",
            cairn_db::turso::params![id.as_str(), state, why, now],
        )
        .await?;
    }
    Ok(())
}

/// Settle receipts that will never deliver: `withdrawn` (every reason is gone)
/// or `dropped` (the recipient is closed, fenced or gone). Final.
pub async fn close_receipts(
    db: &LocalDb,
    receipt_ids: &[String],
    state: &str,
    why: &str,
) -> DbResult<()> {
    let now = receipt_now_ms();
    for id in receipt_ids {
        db.execute(
            "UPDATE delivery_receipts SET state = ?2, last_refusal = ?3, updated_at = ?4, settled_at = ?4
              WHERE id = ?1 AND state IN ('queued', 'delivering')",
            cairn_db::turso::params![id.clone(), state.to_string(), why.to_string(), now],
        )
        .await?;
    }
    Ok(())
}

/// Record why a queued receipt was not delivered on this attempt, leaving it
/// queued: what `/wakes` shows for a receipt held past its bound.
pub async fn note_receipt_refusal(db: &LocalDb, receipt_ids: &[String], why: &str) -> DbResult<()> {
    let now = receipt_now_ms();
    for id in receipt_ids {
        db.execute(
            "UPDATE delivery_receipts SET last_refusal = ?2, updated_at = ?3
              WHERE id = ?1 AND state = 'queued'",
            cairn_db::turso::params![id.clone(), why.to_string(), now],
        )
        .await?;
    }
    Ok(())
}

/// Mark receipts `delivering`, bound to the message id their settlement will
/// write. Written before the settlement transaction; a retry of a bound
/// receipt re-settles exactly that message.
pub async fn bind_receipts(db: &LocalDb, receipt_ids: &[String], message_id: &str) -> DbResult<()> {
    let ids = receipt_ids.to_vec();
    let message_id = message_id.to_string();
    db.write(move |conn| {
        let ids = ids.clone();
        let message_id = message_id.clone();
        Box::pin(async move {
            let now = receipt_now_ms();
            for id in &ids {
                conn.execute(
                    "UPDATE delivery_receipts SET state = 'delivering', message_id = ?2, updated_at = ?3
                      WHERE id = ?1 AND state = 'queued'",
                    cairn_db::turso::params![id.as_str(), message_id.as_str(), now],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

/// Settle a queued receipt that `by`'s message already carried: delivered to
/// whatever `by` was delivered to, in that message, noting which receipt
/// covered it. Nothing is invented for a value `by` does not hold.
pub async fn settle_covered_receipt(db: &LocalDb, receipt_id: &str, by: &Receipt) -> DbResult<()> {
    let now = receipt_now_ms();
    db.execute(
        "UPDATE delivery_receipts
            SET state = 'delivered', delivered_job_id = ?2, message_id = ?3,
                last_refusal = ?4, updated_at = ?5, settled_at = ?5
          WHERE id = ?1 AND state = 'queued'",
        cairn_db::turso::params![
            receipt_id.to_string(),
            by.delivered_job_id.clone(),
            by.message_id.clone(),
            format!("carried by receipt {}", by.id),
            now
        ],
    )
    .await?;
    Ok(())
}

/// Return receipts a settlement bound but whose delivery was refused (nothing
/// was written) to `queued`, so a receipt that waits out the refusal is owed
/// again rather than bound to a message that will never exist.
pub async fn unbind_receipts(db: &LocalDb, receipt_ids: &[String]) -> DbResult<()> {
    let now = receipt_now_ms();
    for id in receipt_ids {
        db.execute(
            "UPDATE delivery_receipts SET state = 'queued', message_id = NULL, updated_at = ?2
              WHERE id = ?1 AND state = 'delivering'",
            cairn_db::turso::params![id.clone(), now],
        )
        .await?;
    }
    Ok(())
}

/// Mark receipts `delivered` inside the settlement's own transaction.
pub async fn settle_receipts_conn(
    conn: &crate::storage::TrackedConnection,
    receipt_ids: &[String],
    delivered_job_id: &str,
    message_id: Option<&str>,
    push_id: Option<&str>,
) -> DbResult<()> {
    let now = receipt_now_ms();
    for id in receipt_ids {
        conn.execute(
            "UPDATE delivery_receipts
                SET state = 'delivered', delivered_job_id = ?2,
                    message_id = COALESCE(?3, message_id), push_id = COALESCE(?4, push_id),
                    updated_at = ?5, settled_at = ?5
              WHERE id = ?1 AND state IN ('queued', 'delivering')",
            cairn_db::turso::params![id.as_str(), delivered_job_id, message_id, push_id, now],
        )
        .await?;
    }
    Ok(())
}

/// [`settle_receipts_conn`] as its own transaction, for a settlement whose
/// message committed in another database.
pub async fn settle_receipts(
    db: &LocalDb,
    receipt_ids: &[String],
    delivered_job_id: &str,
    message_id: Option<&str>,
    push_id: Option<&str>,
) -> DbResult<()> {
    let ids = receipt_ids.to_vec();
    let (job, message, push) = (
        delivered_job_id.to_string(),
        message_id.map(str::to_owned),
        push_id.map(str::to_owned),
    );
    db.write(move |conn| {
        let (ids, job, message, push) = (ids.clone(), job.clone(), message.clone(), push.clone());
        Box::pin(async move {
            settle_receipts_conn(conn, &ids, &job, message.as_deref(), push.as_deref()).await
        })
    })
    .await
}

/// The message id one delivery attempt writes: UUIDv5 over the recipient and
/// the sorted fact keys it carries. A retry of the same set writes the same
/// message, so it lands once however often the settlement is re-driven.
pub fn delivery_message_id(
    recipient: &super::recipients::StableRecipient,
    fact_keys: &[&str],
) -> String {
    const NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x6391_a1de_11e5_4e2c_9a5b_d00d_0000_6391);
    let mut keys = fact_keys.to_vec();
    keys.sort_unstable();
    keys.dedup();
    let name = format!("{}\n{}", recipient.key(), keys.join("\n"));
    uuid::Uuid::new_v5(&NAMESPACE, name.as_bytes()).to_string()
}

/// How long a receipt may stay queued before its recipient's `/wakes` lists
/// it as held.
pub const RECEIPT_OVERDUE_MS: i64 = 10 * 60_000;

/// Queued receipts of `recipient` older than `older_than_ms`, or that a drain
/// attempt already refused: what is held, shown on the recipient's `/wakes`
/// so nothing waits invisibly.
pub async fn overdue_receipts(
    db: &LocalDb,
    recipient: &super::recipients::StableRecipient,
    older_than_ms: i64,
) -> DbResult<Vec<Receipt>> {
    db.query_all(
        format!(
            "SELECT {RECEIPT_COLUMNS} FROM delivery_receipts
              WHERE recipient_kind = ?1 AND recipient_id = ?2 AND state = 'queued'
                AND (created_at < ?3 OR last_refusal IS NOT NULL)
              ORDER BY created_at, id"
        ),
        (
            recipient.kind().to_string(),
            recipient.id().to_string(),
            older_than_ms,
        ),
        receipt_from_row,
    )
    .await
}

/// `recipient`'s receipts under `key_prefix` dropped since `since_ms`, newest
/// first: what a recipient was owed and will now never receive, kept visible
/// on its `/wakes` for a while after the drop.
pub async fn dropped_receipts(
    db: &LocalDb,
    recipient: &super::recipients::StableRecipient,
    key_prefix: &str,
    since_ms: i64,
) -> DbResult<Vec<Receipt>> {
    db.query_all(
        format!(
            "SELECT {RECEIPT_COLUMNS} FROM delivery_receipts
              WHERE recipient_kind = ?1 AND recipient_id = ?2 AND state = 'dropped'
                AND substr(fact_key, 1, length(?3)) = ?3 AND settled_at >= ?4
              ORDER BY settled_at DESC, id"
        ),
        (
            recipient.kind().to_string(),
            recipient.id().to_string(),
            key_prefix.to_string(),
            since_ms,
        ),
        receipt_from_row,
    )
    .await
}

#[cfg(test)]
mod request_subscription_tests {
    use super::*;

    #[test]
    fn request_fact_subscription_identity_is_sorted_and_has_no_selectors() {
        let clause = std::collections::BTreeMap::from([
            ("request".into(), serde_json::json!("cairn://requests/r")),
            ("outcome".into(), serde_json::json!("done")),
            ("fact".into(), serde_json::json!("request")),
        ]);
        assert_eq!(
            fact_clause_ref(&clause).unwrap(),
            r#"{"fact":"request","outcome":"done","request":"cairn://requests/r"}"#
        );
        for uri in [
            "cairn://requests",
            "cairn://requests/r?outcome=accepted",
            "cairn://p/cairn/1",
            "cairn://requests/r/",
        ] {
            let mut invalid = clause.clone();
            invalid.insert("request".into(), serde_json::json!(uri));
            assert!(fact_clause_ref(&invalid).is_err(), "{uri}");
        }
        let scopes = compile_derived_thread_scopes(&[clause.clone(), clause]).unwrap();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].source_kind, SOURCE_KIND_FACT);
        assert_eq!(scopes[0].fact_kinds, ["request"]);
    }
}
