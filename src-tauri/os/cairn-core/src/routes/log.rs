//! Minting facts into the durable fact log, and restoring a row to the fact it
//! recorded.
//!
//! A producer that observes something routable calls [`mint`] (or
//! [`spawn_mint`] where it must not wait at all) instead of dispatching it. The
//! mint is one local insert: the row lands, the fact log worker is poked, and
//! the producer returns. Matching, thread wakes, and route dispatch all happen
//! later, in the worker, off the producer's path.
//!
//! A row holds only the registry's stored fields, bounded. Content fields are
//! declared `stored: false` and restored by [`hydrate`] from the canonical
//! record the row points at, so the log never becomes a second copy of the
//! transcript or of a file. The one exception is a source with no local
//! record at all (a relayed GitHub comment): its row stores the content once,
//! bounded only by the external record's own limit, because clipping it would
//! hand routes less than the producer saw.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{FactRegistry, Presence, RouteFact, SourceSpec};
use crate::orchestrator::Orchestrator;
use crate::storage::{FactRow, NewFact};

/// Longest stored scalar, matching the firing journal's snapshot bound.
const MAX_STORED_CHARS: usize = crate::storage::SNAPSHOT_CHARS;
/// Longest stored list.
const MAX_STORED_LIST: usize = 64;
/// Longest stored summary.
const MAX_SUMMARY_CHARS: usize = 280;

/// One fact to mint, as its producer decided it.
pub struct Mint<'a> {
    pub fact: RouteFact,
    /// A navigable pointer to the canonical record this fact is about.
    pub record_ref: String,
    /// The canonical record's raw key, which outlives any change to the
    /// resource grammar `record_ref` is written in.
    pub record_id: Option<String>,
    /// The producer's project, from its own context. `None` only for a fact
    /// that resolves to no project.
    pub project_id: Option<&'a str>,
    /// Operator presence when the fact was decided.
    pub presence: Presence,
    /// The job whose action caused this fact. Its own thread is never woken by it.
    pub actor_job_id: Option<&'a str>,
}

/// [`Mint`] with owned fields, for [`spawn_mint`]. `presence: None` resolves
/// operator presence inside the spawned task, so a producer that must not
/// await anything does not await that either.
pub struct MintOwned {
    pub fact: RouteFact,
    pub record_ref: String,
    pub record_id: Option<String>,
    pub project_id: Option<String>,
    pub presence: Option<Presence>,
    pub actor_job_id: Option<String>,
}

pub(crate) fn presence_str(presence: Presence) -> &'static str {
    match presence {
        Presence::Active => "active",
        Presence::Away => "away",
    }
}

pub(crate) fn presence_from_str(value: &str) -> Presence {
    match value {
        "active" => Presence::Active,
        _ => Presence::Away,
    }
}

/// The operator's single, provider-independent presence.
pub(crate) async fn operator_presence() -> Presence {
    if crate::channels::operator_presence_status().await.presence
        == crate::channels::OperatorPresence::Present
    {
        Presence::Active
    } else {
        Presence::Away
    }
}

fn bound_text(text: &str, limit: usize) -> String {
    let mut bounded: String = text.chars().take(limit).collect();
    if bounded.chars().count() < text.chars().count() {
        bounded.push('…');
    }
    bounded
}

fn bound_value(value: Value, max_chars: usize, max_list: usize) -> Value {
    match value {
        Value::String(text) => Value::String(bound_text(&text, max_chars)),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .take(max_list)
                .map(|item| match item {
                    Value::String(text) => Value::String(bound_text(&text, max_chars)),
                    other => other,
                })
                .collect(),
        ),
        other => other,
    }
}

/// The fields a row stores: registry fields marked stored, each bounded.
/// Anything the registry does not declare, and every hydrated field, is
/// dropped here, so no producer can widen what the log holds.
fn stored_fields(spec: &SourceSpec, fields: BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    let max_chars = spec.max_stored_chars.unwrap_or(MAX_STORED_CHARS);
    let max_list = if spec.whole_lists {
        usize::MAX
    } else {
        MAX_STORED_LIST
    };
    fields
        .into_iter()
        .filter(|(name, _)| spec.fields.get(name.as_str()).is_some_and(|f| f.stored))
        .map(|(name, value)| (name, bound_value(value, max_chars, max_list)))
        .collect()
}

/// Insert one fact into the log. Returns the new row id, or `None` when the
/// source's dedupe rule recognised the fact as one already recorded.
pub async fn mint(orch: &Orchestrator, mint: Mint<'_>) -> Result<Option<i64>, String> {
    let registry = FactRegistry::default();
    let spec = registry
        .source(&mint.fact.source)
        .ok_or_else(|| format!("unknown fact source '{}'", mint.fact.source))?;
    let (origin_principal_json, origin_appearance_json) = match &mint.fact.origin {
        Some(origin) => {
            let (principal, appearance) = crate::issues::crud::encode_issue_authorship(origin)
                .map_err(|error| error.to_string())?;
            (Some(principal), Some(appearance))
        }
        None => (None, None),
    };
    let dedupe = spec.dedupe;
    let fields_json = serde_json::to_string(&stored_fields(spec, mint.fact.fields))
        .map_err(|error| error.to_string())?;
    let new = NewFact {
        project_id: mint.project_id.map(str::to_owned),
        identity: cairn_common::uri::canonicalize_uri_identity(&mint.fact.identity),
        source: mint.fact.source,
        record_ref: mint.record_ref,
        record_id: mint.record_id,
        fields_json,
        summary: mint
            .fact
            .summary
            .as_deref()
            .map(str::trim)
            .filter(|summary| !summary.is_empty())
            .map(|summary| bound_text(summary, MAX_SUMMARY_CHARS)),
        presence: presence_str(mint.presence).to_string(),
        actor_job_id: mint.actor_job_id.map(str::to_owned),
        origin_principal_json,
        origin_appearance_json,
        // A fact minted while a route's sink runs is that route's output, so
        // the loop guard holds for every minter without each one threading it.
        route_provenance: mint
            .fact
            .route_provenance
            .or_else(super::current_provenance),
        minted_at: chrono::Utc::now().timestamp_millis(),
    };
    let id = crate::storage::insert_fact(&orch.db.local, new, dedupe)
        .await
        .map_err(|error| error.to_string())?;
    if id.is_some() {
        orch.fact_log.notify.notify_one();
    }
    Ok(id)
}

/// Mint without waiting for anything, not even the insert. The route
/// provenance of the calling task is captured before the spawn, because the
/// spawned task does not inherit task-locals.
pub fn spawn_mint(orch: &Orchestrator, mint: MintOwned) {
    spawn_mint_deferred(orch, async move { Ok(mint) });
}

/// [`spawn_mint`] for a producer whose fact needs reads of its own (the
/// writing job's project and node): `build` runs inside the spawned task, so
/// the producer waits on none of it.
pub fn spawn_mint_deferred<F>(orch: &Orchestrator, build: F)
where
    F: std::future::Future<Output = Result<MintOwned, String>> + Send + 'static,
{
    let provenance = super::current_provenance();
    let orch = orch.clone();
    tokio::spawn(async move {
        let mint = match build.await {
            Ok(mint) => mint,
            Err(error) => {
                log::warn!("could not build a fact to mint into the fact log: {error}");
                return;
            }
        };
        let mut fact = mint.fact;
        if fact.route_provenance.is_none() {
            fact.route_provenance = provenance;
        }
        let source = fact.source.clone();
        let presence = match mint.presence {
            Some(presence) => presence,
            None => operator_presence().await,
        };
        if let Err(error) = self::mint(
            &orch,
            Mint {
                fact,
                record_ref: mint.record_ref,
                record_id: mint.record_id,
                project_id: mint.project_id.as_deref(),
                presence,
                actor_job_id: mint.actor_job_id.as_deref(),
            },
        )
        .await
        {
            log::warn!("could not mint a {source} fact into the fact log: {error}");
        }
    });
}

/// A row restored to the fact it recorded.
pub struct Hydrated {
    pub fact: RouteFact,
    pub presence: Presence,
    /// When the canonical record says the fact happened, in milliseconds, for
    /// sources whose record carries a time the log row does not.
    pub occurred_at_ms: Option<i64>,
}

/// The row's stored fact, without hydrated fields.
pub fn stored_fact(row: &FactRow) -> Result<RouteFact, String> {
    let fields: BTreeMap<String, Value> =
        serde_json::from_str(&row.fields_json).map_err(|error| error.to_string())?;
    let origin = crate::issues::crud::decode_issue_authorship(
        row.origin_principal_json.clone(),
        row.origin_appearance_json.clone(),
    )
    .map_err(|error| error.to_string())?;
    Ok(RouteFact {
        source: row.source.clone(),
        identity: row.identity.clone(),
        fields,
        origin,
        summary: row.summary.clone(),
        route_provenance: row.route_provenance.clone(),
    })
}

/// Restore a row's hydrated fields from the canonical record it points at.
/// One arm per source that declares a hydrated field; every other source's row
/// already is its whole fact.
pub async fn hydrate(orch: &Orchestrator, row: &FactRow) -> Result<Hydrated, String> {
    let mut fact = stored_fact(row)?;
    let mut occurred_at_ms = None;
    if row.source == "thread_stream" {
        let rowid: i64 = row
            .record_id
            .as_deref()
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| format!("thread_stream row {} has no transcript rowid", row.id))?;
        let project = fact
            .fields
            .get("project")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("thread_stream row {} has no project", row.id))?
            .to_owned();
        let db = orch.db.for_project(&project).await;
        let (data, created_at) = db
            .query_opt(
                "SELECT data, created_at FROM events WHERE rowid = ?1",
                (rowid,),
                |row| {
                    Ok((
                        crate::storage::RowExt::text(row, 0)?,
                        crate::storage::RowExt::i64(row, 1)?,
                    ))
                },
            )
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("transcript event {rowid} no longer exists"))?;
        let text = crate::channels::router::assistant_text(&data)
            .ok_or_else(|| format!("transcript event {rowid} carries no assistant text"))?;
        fact.fields.insert("text".into(), Value::String(text));
        // Transcript `created_at` is whole seconds.
        occurred_at_ms = Some(created_at.saturating_mul(1000));
    }
    if row.source == "github_comment" {
        let field = |name: &str| {
            fact.fields
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let body = crate::github::relay::comment_context(
            &field("repository"),
            &field("kind"),
            &field("number"),
            &field("author"),
            &field("url"),
            &field("text"),
        );
        fact.fields.insert("body".into(), Value::String(body));
    }
    Ok(Hydrated {
        fact,
        presence: presence_from_str(&row.presence),
        occurred_at_ms,
    })
}

/// Whether matching any of these clauses needs a field the row does not store.
pub(crate) fn clauses_need_hydration<'c>(
    spec: &SourceSpec,
    clauses: impl IntoIterator<Item = &'c super::TriggerClause>,
) -> bool {
    clauses.into_iter().any(|clause| {
        clause.keys().any(|key| {
            let field = key.strip_suffix("Prefix").unwrap_or(key);
            spec.is_hydrated(field)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_fields_drop_hydrated_and_unregistered_fields_and_bound_the_rest() {
        let registry = FactRegistry::default();
        let spec = registry.source("thread_stream").unwrap();
        let fields = BTreeMap::from([
            (
                "text".to_string(),
                Value::String("the whole utterance".into()),
            ),
            ("mystery".to_string(), Value::Bool(true)),
            (
                "context".to_string(),
                Value::String("x".repeat(MAX_STORED_CHARS + 5)),
            ),
        ]);
        let stored = stored_fields(spec, fields);
        assert_eq!(stored.keys().collect::<Vec<_>>(), vec!["context"]);
        assert_eq!(
            stored["context"].as_str().unwrap().chars().count(),
            MAX_STORED_CHARS + 1
        );

        let attention = registry.source("attention").unwrap();
        let labels = Value::Array((0..100).map(|n| Value::String(n.to_string())).collect());
        let stored = stored_fields(
            attention,
            BTreeMap::from([("label".to_string(), labels.clone())]),
        );
        assert_eq!(stored["label"].as_array().unwrap().len(), MAX_STORED_LIST);

        // A reference set the lane resolves or matches is never cut.
        for source in ["thread_stream", "post_created", "post_commented"] {
            let spec = registry.source(source).unwrap();
            let stored = stored_fields(
                spec,
                BTreeMap::from([("mentions".to_string(), labels.clone())]),
            );
            assert_eq!(
                stored["mentions"].as_array().unwrap().len(),
                100,
                "{source}"
            );
        }
    }
}
