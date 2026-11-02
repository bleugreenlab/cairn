//! Model requests: waiting work that leases a model resident, loading it if
//! it is absent, and what each one waits on while it waits.
//!
//! A request names one deployment, chosen when it is made from the
//! deployments serving the model's real name, and is never served by another
//! model. While it waits it leases the resident: a resident with a waiting
//! request is never evicted. Once answered it counts as demand, so the
//! resident's idle window runs from the last request it answered.

use std::collections::{BTreeMap, BTreeSet};

use cairn_inference::DeploymentId;
use serde::{Deserialize, Serialize};

use super::rules::{Acquirable, Resident};
use super::settings::{relative, Ms};
use crate::fleet::pools::PoolKey;

/// A request's id, assigned by the store.
pub type RequestId = i64;

/// One caller's request for a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub id: RequestId,
    /// The pool the deployment was in when the request was made.
    pub pool: PoolKey,
    pub deployment: DeploymentId,
    /// The model's real name, as the request door resolved it.
    pub model: String,
    pub requester: String,
    pub at: Ms,
    pub state: RequestState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum RequestState {
    Waiting,
    /// The deployment was healthy at `at`.
    Ready {
        at: Ms,
    },
    Failed {
        at: Ms,
        reason: String,
    },
    Cancelled {
        at: Ms,
    },
}

impl RequestState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Ready { .. } => "ready",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
        }
    }
}

/// Why a resident cannot give its machines up yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum HoldCause {
    /// A standing lease: never idle, never evicted for room.
    Pinned { holder: String },
    /// Requests wait on it.
    Wanted { requests: u32 },
    /// It is loading.
    Loading,
    /// Its minimum residency has not run.
    Residency,
    /// It was requested within its idle window.
    RecentRequest,
}

/// A resident holding machines something else needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hold {
    pub deployment: DeploymentId,
    pub model: String,
    pub because: HoldCause,
    /// When the hold ends if nothing changes; `None` while it is pinned or
    /// requests wait on it.
    pub until: Option<Ms>,
}

impl Hold {
    pub fn describe(&self, now: Ms) -> String {
        let until = self
            .until
            .map(|at| format!(", until {}", relative(at, now)))
            .unwrap_or_default();
        let model = &self.model;
        match &self.because {
            HoldCause::Pinned { holder } => format!("{model} is pinned by {holder}"),
            HoldCause::Wanted { requests } => format!(
                "{model} keeps its machines while {} wait on it",
                plural(*requests, "request", "requests")
            ),
            HoldCause::Loading => format!("{model} is loading{until}"),
            HoldCause::Residency => {
                format!("{model} is inside its minimum residency{until}")
            }
            HoldCause::RecentRequest => format!(
                "{model} was requested within its idle window and keeps its machines{until}"
            ),
        }
    }
}

/// What a waiting request waits on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Wait {
    /// Its deployment is resident; others placement released still occupy
    /// its machines while they stop.
    Unloading { deployments: Vec<DeploymentId> },
    /// Its deployment is resident and nothing is in the way; the start is
    /// asked of the act side on this pass.
    Starting,
    /// Its machines are reserved and the start has not been sent: placement
    /// waits for every gang machine to have the memory the model needs
    /// (`waiting_on`, in the drain's words) and fails the start if it never
    /// fits.
    Fitting {
        since: Ms,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        waiting_on: Option<String>,
    },
    /// The head is loading it.
    Loading { since: Ms, bound_at: Ms },
    /// The act side is still stopping its previous run.
    Stopping,
    /// Residents hold machines it needs.
    Held { by: Vec<Hold> },
    /// An earlier request waits for machines it needs.
    Queued {
        behind: RequestId,
        deployment: DeploymentId,
    },
}

fn ids_label(ids: &[DeploymentId]) -> String {
    ids.iter()
        .map(DeploymentId::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn plural(count: u32, one: &str, many: &str) -> String {
    if count == 1 {
        format!("1 {one}")
    } else {
        format!("{count} {many}")
    }
}

impl Wait {
    /// What the request waits on, in a sentence fragment.
    pub fn describe(&self, now: Ms) -> String {
        match self {
            Self::Unloading { deployments } => {
                format!("unloading {} from its machines", ids_label(deployments))
            }
            Self::Starting => "nothing in the way; placement is starting it".to_string(),
            Self::Fitting { since, waiting_on } => {
                let mut said = format!(
                    "waiting to fit since {}, its machines' room reserved for it",
                    relative(*since, now)
                );
                if let Some(on) = waiting_on {
                    said.push_str(&format!("; {on}"));
                }
                let gives_up = since
                    + Ms::try_from(cairn_inference::control::FIT_WAIT_SECS).unwrap_or(0) * 1_000;
                said.push_str(&format!(
                    "; the start fails {} if it never fits",
                    relative(gives_up, now)
                ));
                said
            }
            Self::Loading { since, bound_at } => format!(
                "loading since {}, given up {} if it is not healthy by then",
                relative(*since, now),
                relative(*bound_at, now)
            ),
            Self::Stopping => "the act side is still stopping its previous run".to_string(),
            Self::Held { by } if by.is_empty() => "the models on its machines".to_string(),
            Self::Held { by } => by
                .iter()
                .map(|hold| hold.describe(now))
                .collect::<Vec<_>>()
                .join("; "),
            Self::Queued { behind, deployment } => format!(
                "queued behind request #{behind} for {deployment}, which asked first for the same machines"
            ),
        }
    }
}

/// A waiting request in one line, naming the model, the deployment, the pool
/// and what it waits on.
pub fn describe_waiting(request: &Request, wait: &Wait, now: Ms) -> String {
    format!(
        "{} (deployment {}) on pool {} for {}, waiting {}: {}",
        request.model,
        request.deployment,
        request.pool,
        request.requester,
        relative(request.at, now).trim_end_matches(" ago"),
        wait.describe(now)
    )
}

/// The deployment a request for `model` leases: one whose profile's real name
/// is `model` (case-insensitive, exact: an alias or a partial name is never
/// matched, and another model never serves it), narrowed to `pool` when the
/// caller names one. A deployment already resident or loading is preferred,
/// then one on a pool holding no other resident, then the lowest id. A
/// resident serves requests whether or not its machines form a detected pool
/// now; only acquiring a model anew needs one.
pub fn choose_deployment(
    catalog: &BTreeMap<DeploymentId, Acquirable>,
    residents: &BTreeMap<DeploymentId, Resident>,
    model: &str,
    pool: Option<&PoolKey>,
) -> Result<DeploymentId, String> {
    let model = model.trim();
    if model.is_empty() {
        return Err("name the model to load".to_string());
    }
    let serving: Vec<&Acquirable> = catalog
        .values()
        .filter(|candidate| candidate.model.eq_ignore_ascii_case(model))
        .collect();
    if serving.is_empty() {
        return Err(format!(
            "no deployment serves '{model}'; models go by their real names, and cairn://inference/deployments lists them"
        ));
    }
    let pool_of = |candidate: &Acquirable| {
        residents
            .get(&candidate.deployment)
            .map(|resident| resident.pool.clone())
            .or_else(|| candidate.pool.clone())
    };
    let in_pool: Vec<&Acquirable> = serving
        .iter()
        .copied()
        .filter(|candidate| pool.is_none_or(|pool| pool_of(candidate).as_ref() == Some(pool)))
        .collect();
    if in_pool.is_empty() {
        return Err(format!(
            "no deployment of '{model}' runs on pool {}; it is deployed on {}",
            pool.map(PoolKey::to_string).unwrap_or_default(),
            serving
                .iter()
                .map(|candidate| candidate.deployment.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let eligible: Vec<&Acquirable> = in_pool
        .iter()
        .copied()
        .filter(|candidate| {
            candidate.pool.is_some() || residents.contains_key(&candidate.deployment)
        })
        .collect();
    if eligible.is_empty() {
        return Err(format!(
            "'{model}' cannot run now: {}",
            in_pool
                .iter()
                .map(|candidate| format!(
                    "{} {}",
                    candidate.deployment,
                    candidate
                        .refused
                        .as_deref()
                        .unwrap_or("is in no detected pool")
                ))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    let occupied: BTreeSet<&PoolKey> = residents.values().map(|r| &r.pool).collect();
    eligible
        .into_iter()
        .min_by_key(|candidate| {
            let resident = residents.contains_key(&candidate.deployment);
            let pool_free = pool_of(candidate).is_some_and(|pool| !occupied.contains(&pool));
            (!resident, !pool_free, candidate.deployment.clone())
        })
        .map(|candidate| candidate.deployment.clone())
        .ok_or_else(|| format!("no deployment serves '{model}'"))
}
