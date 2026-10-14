//! The source-keyed trigger index the fact log worker matches rows against.
//!
//! Two kinds of consumer declare interest in a fact source: enabled route
//! triggers (route YAML) and thread standing triggers (persisted as `fact`
//! wake subscriptions). Both are loaded once and held keyed by source, so a
//! row only visits consumers that declared its source, and route YAML is read
//! when it changes rather than once per fact.
//!
//! Route freshness has two inputs: every writer of route files calls
//! [`invalidate_trigger_index`], and a cached route set expires after
//! [`INDEX_TTL`] regardless, which bounds how long a hand edit of a route file
//! goes unseen. Thread consumers are rebuilt deep inside thread session setup,
//! where no orchestrator is at hand, so they validate themselves instead: each
//! pass reads the live `fact` subscription rows and reuses the parsed
//! consumers only when those rows are exactly the ones they were parsed from.
//! A count or rowid fingerprint is not enough: a rebuild deletes and reinserts
//! rows, and an edit that keeps the clause count can reuse the same rowids.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use super::predicate::{scope_exclusion, ThreadScope};
use super::{Fact, TriggerClause};
use crate::config::{routes::FileRoute, ConfigResult};
use crate::orchestrator::Orchestrator;

pub(crate) const INDEX_TTL: Duration = Duration::from_secs(30);

/// The orchestrator's handle on the fact log: the two lanes' wake signals and
/// the trigger index both lanes read.
#[derive(Clone, Default)]
pub struct FactLogHandle {
    /// Poked by every mint; wakes the index lane.
    pub(crate) notify: Arc<Notify>,
    /// Poked whenever the index lane queues a row for routes.
    pub(crate) route_notify: Arc<Notify>,
    pub(crate) index: Arc<TriggerIndex>,
}

/// Drop every cached consumer list so the next row reloads it. Route writers
/// and thread-trigger writers call this; it is the one invalidation seam.
pub fn invalidate_trigger_index(orch: &Orchestrator) {
    orch.fact_log.index.invalidate();
}

/// The enabled routes in one route scope, keyed by the fact source each
/// route's triggers name.
pub struct RouteSet {
    routes: Vec<FileRoute>,
    by_source: HashMap<String, Vec<usize>>,
}

impl RouteSet {
    fn build(results: Vec<ConfigResult<FileRoute>>) -> Self {
        let routes: Vec<FileRoute> = results
            .into_iter()
            .filter_map(|result| match result {
                ConfigResult::Ok(route) if route.definition.enabled => Some(route),
                _ => None,
            })
            .collect();
        let mut by_source: HashMap<String, Vec<usize>> = HashMap::new();
        for (position, route) in routes.iter().enumerate() {
            let mut sources: Vec<&str> = route
                .definition
                .triggers()
                .filter_map(|clause| clause.get("fact").and_then(serde_json::Value::as_str))
                .collect();
            sources.sort_unstable();
            sources.dedup();
            for source in sources {
                by_source
                    .entry(source.to_owned())
                    .or_default()
                    .push(position);
            }
        }
        Self { routes, by_source }
    }

    /// Routes with at least one trigger on `source`, in load order.
    pub(crate) fn routes_for(&self, source: &str) -> impl Iterator<Item = &FileRoute> {
        self.by_source
            .get(source)
            .into_iter()
            .flatten()
            .map(|&position| &self.routes[position])
    }

    /// Every trigger clause on `source` across those routes.
    pub(crate) fn clauses_for<'s>(
        &'s self,
        source: &'s str,
    ) -> impl Iterator<Item = &'s TriggerClause> + 's {
        self.routes_for(source).flat_map(move |route| {
            route.definition.triggers().filter(move |clause| {
                clause.get("fact").and_then(serde_json::Value::as_str) == Some(source)
            })
        })
    }
}

/// A thread whose definition declares a standing trigger, compiled to one
/// `fact` wake subscription.
#[derive(Debug, Clone)]
pub(crate) struct ThreadConsumer {
    pub(crate) job_id: String,
    /// The project the consumer's job belongs to, read once with the
    /// subscription so jurisdiction is a comparison rather than a per-row
    /// lookup. `None` when the job's project cannot be resolved.
    pub(crate) project_id: Option<String>,
    pub(crate) project_key: Option<String>,
    /// The subscription's `source_ref`: the clause's canonical JSON, which is
    /// what the wake carries so delivery resolves this exact subscription.
    pub(crate) clause_ref: String,
    pub(crate) clause: TriggerClause,
}

impl ThreadConsumer {
    pub(crate) fn scope<'a>(&'a self, row_project_id: Option<&'a str>) -> ThreadScope<'a> {
        ThreadScope {
            home_project_id: self.project_id.as_deref(),
            home_project_key: self.project_key.as_deref(),
            row_project_id,
        }
    }

    /// Whether `fact`, from a row in `row_project_id`, is within this
    /// consumer's jurisdiction (see [`scope_exclusion`]).
    pub(crate) fn admits(&self, fact: &Fact<'_>, row_project_id: Option<&str>) -> bool {
        scope_exclusion(
            self.clause.get("project"),
            fact,
            &self.scope(row_project_id),
        )
        .is_none()
    }
}

pub(crate) type ThreadConsumers = HashMap<String, Vec<ThreadConsumer>>;

/// The live `fact` subscriptions as (job id, clause JSON): what a cached set of
/// consumers was parsed from, and the check that it still applies.
type SubscriptionFingerprint = Vec<(String, String)>;
type CachedThreads = (SubscriptionFingerprint, Arc<ThreadConsumers>);

struct Cached<T> {
    generation: u64,
    loaded_at: Instant,
    value: Arc<T>,
}

impl<T> Cached<T> {
    fn fresh(&self, generation: u64) -> Option<Arc<T>> {
        (self.generation == generation && self.loaded_at.elapsed() < INDEX_TTL)
            .then(|| self.value.clone())
    }
}

#[derive(Default)]
pub struct TriggerIndex {
    generation: AtomicU64,
    routes: Mutex<HashMap<Option<PathBuf>, Cached<RouteSet>>>,
    threads: tokio::sync::Mutex<Option<CachedThreads>>,
}

impl TriggerIndex {
    pub(crate) fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// The route set for a fact's project (`None` for a project-less fact):
    /// project routes shadow workspace routes, which shadow the bundled ones,
    /// exactly as `config::routes::list_routes` decides.
    pub(crate) fn routes(
        &self,
        config_dir: &Path,
        project_path: Option<&Path>,
    ) -> Result<Arc<RouteSet>, String> {
        let generation = self.generation.load(Ordering::SeqCst);
        let key = project_path.map(Path::to_owned);
        if let Some(set) = self
            .routes
            .lock()
            .expect("route index poisoned")
            .get(&key)
            .and_then(|cached| cached.fresh(generation))
        {
            return Ok(set);
        }
        let set = Arc::new(RouteSet::build(crate::config::routes::list_routes(
            config_dir,
            project_path,
        )?));
        self.routes.lock().expect("route index poisoned").insert(
            key,
            Cached {
                generation,
                loaded_at: Instant::now(),
                value: set.clone(),
            },
        );
        Ok(set)
    }

    pub(crate) async fn threads(
        &self,
        db: &crate::storage::LocalDb,
    ) -> Result<Arc<ThreadConsumers>, String> {
        // The freshness check runs once per row, so it reads the subscription
        // table alone; the project join runs only when the cache is rebuilt. A
        // job's project never changes, so the check need not include it.
        let live = db
            .query_all(
                "SELECT job_id, source_ref FROM wake_subscriptions
                  WHERE source_kind = 'fact' AND state != 'unsubscribed' AND source_ref IS NOT NULL
                  ORDER BY job_id, id",
                (),
                |row| {
                    Ok((
                        crate::storage::RowExt::text(row, 0)?,
                        crate::storage::RowExt::text(row, 1)?,
                    ))
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut slot = self.threads.lock().await;
        if let Some((cached, consumers)) = slot.as_ref() {
            if *cached == live {
                return Ok(consumers.clone());
            }
        }
        let rows = db
            .query_all(
                // A job's project is its issue's, or a thread session's own: the
                // same COALESCE that names a job's home.
                "SELECT s.job_id, p.id, p.key, s.source_ref
                   FROM wake_subscriptions s
                   LEFT JOIN jobs j ON j.id = s.job_id
                   LEFT JOIN issues i ON i.id = j.issue_id
                   LEFT JOIN projects p ON p.id = COALESCE(i.project_id, j.project_id)
                  WHERE s.source_kind = 'fact' AND s.state != 'unsubscribed' AND s.source_ref IS NOT NULL
                  ORDER BY s.job_id, s.id",
                (),
                |row| {
                    Ok((
                        crate::storage::RowExt::text(row, 0)?,
                        crate::storage::RowExt::opt_text(row, 1)?,
                        crate::storage::RowExt::opt_text(row, 2)?,
                        crate::storage::RowExt::text(row, 3)?,
                    ))
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        // Fingerprint what was actually parsed: a subscription that changed
        // between the two reads then fails the next check and rebuilds.
        let parsed: SubscriptionFingerprint = rows
            .iter()
            .map(|(job_id, _, _, clause_ref)| (job_id.clone(), clause_ref.clone()))
            .collect();
        let mut consumers: ThreadConsumers = HashMap::new();
        for (job_id, project_id, project_key, clause_ref) in rows {
            let Ok(clause) = serde_json::from_str::<TriggerClause>(&clause_ref) else {
                log::warn!("skipping unreadable fact trigger subscription for job {job_id}");
                continue;
            };
            let Some(source) = clause
                .get("fact")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            consumers.entry(source).or_default().push(ThreadConsumer {
                job_id,
                project_id,
                project_key,
                clause_ref,
                clause,
            });
        }
        let consumers = Arc::new(consumers);
        *slot = Some((parsed, consumers.clone()));
        Ok(consumers)
    }
}
