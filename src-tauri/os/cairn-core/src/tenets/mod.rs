//! Tenets: the human checkpoint for threads.
//!
//! A thread proposes a statement of how the project does things, and the
//! operator answers `yes`, `no`, or `your_call`. `your_call` approves exactly
//! as `yes` does; it is kept apart because it records that the operator simply
//! trusted the thread, which calibrates how far a thread is trusted.
//!
//! The statement is the one sentence the operator answers at a glance. What a
//! decision rests on (why, the alternatives ruled out, and where it was
//! argued) goes in the optional rationale beneath it, so a yes records the
//! rejection without burying the line the verdict is given on.
//!
//! A tenet is immutable once proposed. The only transitions are the proposing
//! thread withdrawing an unanswered proposal, the operator answering one, and a
//! later approved tenet superseding an earlier approved one. There is no
//! revise: a changed mind is a withdrawal and a new proposal, or a successor.
//!
//! This module is the only writer of the `tenets` table. Every write emits a
//! `db-change` for `tenets` and, when it can change what a held execution's
//! gate reads, nudges the gate sweep.

pub mod evidence;
pub(crate) mod filing;
mod policy;
mod read;
mod ruling_import;
mod verdict;
mod write;

#[cfg(test)]
mod gate_tests;
#[cfg(test)]
mod tests;

use cairn_common::identity::PrincipalRef;
use serde::{Deserialize, Serialize};

use crate::services::EventEmitter;

pub use policy::{
    load_policy, operator_only_policy_refusal, release_label, set_gated_recipes, set_release,
    TenetPolicy, TenetRelease, OPERATOR_ONLY_POLICY_REFUSAL, RATIFIED_REFUSAL,
};
pub use read::{
    for_thread, get, list, pending_count, resolve_number, status_filter, thread_named,
    TenetEvidence, TenetFilter, TenetGate, TenetView,
};
pub(crate) use read::{lookup_conn, row_conn, TenetLookup};
pub use ruling_import::{import_database, import_thread_rulings, ImportReport, ProjectImport};
pub(crate) use verdict::ensure_decision_comment;
pub(crate) use write::release_deleted_thread_conn;
pub use write::{allocate_number_conn, insert_conn, NewTenet};
pub use write::{answer, propose, withdraw, Answer, Answered, Proposal, Proposer};

/// The table this module owns, as `db-change` names it.
pub const TABLE: &str = "tenets";

/// The gate fact that names a tenet in `execution_gates.predicate_json`, as
/// `{fact: "tenet_verdict", tenet: <canonical tenet URI>, ...}`. The read
/// projection's `gates` field finds executions through it.
pub const TENET_GATE_FACT: &str = "tenet_verdict";

/// The ceiling on a proposal's statement, in characters: one sentence the
/// operator answers at a glance.
pub const MAX_STATEMENT_CHARS: usize = 200;

/// The ceiling on a proposal's rationale, in characters. Detail beyond it
/// belongs in the issue or chat turn the rationale cites.
pub const MAX_RATIONALE_CHARS: usize = 1500;

pub const OVER_LENGTH_REFUSAL: &str = "a tenet statement is one sentence the operator answers at a glance (≤200 chars); put the reasoning, rejected alternatives, and provenance links in `rationale`";

pub const RATIONALE_OVER_LENGTH_REFUSAL: &str = "a tenet's rationale is at most 1500 characters; cite the issue or chat turn where it was argued instead of restating it";

/// The refusal for any attempt to change a tenet's content.
pub const IMMUTABLE_REFUSAL: &str = "tenets are immutable; withdraw and propose again";

/// Why an agent cannot record a verdict, and where the operator records one.
pub const AGENT_VERDICT_REFUSAL: &str =
    "verdicts belong to the operator: a tenet is answered in the desktop app, from the proposing thread's Tenets facet, the proposal's card in the chat, or the project Tenets tab, never by an agent";

/// The refusal for a proposal in the retired question-with-options shape,
/// with `prefix` naming where the proposal sits in the payload (`payload.`,
/// `payload.execution.proposal.`).
pub fn retired_proposal_shape_refusal(prefix: &str) -> String {
    format!(
        "a tenet is one statement the operator answers yes or no: send {prefix}statement, \
         \"this is how we do things\", in place of `question` and `options`, which are retired. \
         Alternatives you ruled out belong in {prefix}rationale."
    )
}

/// The refusal for changing a tenet that already has a verdict: the change is
/// a new tenet that supersedes it.
pub fn successor_refusal(number: i64) -> String {
    format!(
        "tenet #{number} already has a verdict; propose a successor with `supersedes: {number}`"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TenetStatus {
    Proposed,
    Approved,
    Declined,
    Withdrawn,
    Superseded,
}

impl TenetStatus {
    pub const ALL: [TenetStatus; 5] = [
        TenetStatus::Proposed,
        TenetStatus::Approved,
        TenetStatus::Declined,
        TenetStatus::Withdrawn,
        TenetStatus::Superseded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TenetStatus::Proposed => "proposed",
            TenetStatus::Approved => "approved",
            TenetStatus::Declined => "declined",
            TenetStatus::Withdrawn => "withdrawn",
            TenetStatus::Superseded => "superseded",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|status| status.as_str() == value)
    }

    /// Whether the operator has answered this tenet. A superseded tenet was
    /// approved before its successor was, so it keeps its verdict.
    pub fn has_verdict(self) -> bool {
        matches!(
            self,
            TenetStatus::Approved | TenetStatus::Declined | TenetStatus::Superseded
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenetOrigin {
    /// Proposed as the condition of a gated filing.
    #[serde(rename = "filing-gate")]
    FilingGate,
    /// Proposed on its own through the tenets collection.
    #[serde(rename = "standing")]
    Standing,
    /// Imported from a thread's pre-tenet rulings.
    #[serde(rename = "migrated-ruling")]
    MigratedRuling,
}

impl TenetOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            TenetOrigin::FilingGate => "filing-gate",
            TenetOrigin::Standing => "standing",
            TenetOrigin::MigratedRuling => "migrated-ruling",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        [
            TenetOrigin::FilingGate,
            TenetOrigin::Standing,
            TenetOrigin::MigratedRuling,
        ]
        .into_iter()
        .find(|origin| origin.as_str() == value)
    }
}

/// How a verdict was attributed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnswerSource {
    /// A signed-in person, recorded as their Human principal.
    Principal,
    /// The desktop with no signed-in person, recorded as this machine's
    /// device identity.
    Device,
    /// A ruling imported with its verdict already given.
    Migrated,
}

impl AnswerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            AnswerSource::Principal => "principal",
            AnswerSource::Device => "device",
            AnswerSource::Migrated => "migrated",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        [
            AnswerSource::Principal,
            AnswerSource::Device,
            AnswerSource::Migrated,
        ]
        .into_iter()
        .find(|source| source.as_str() == value)
    }

    /// Which source an answering principal records as. Only the operator
    /// answers: a signed-in person, or the desktop's own device identity.
    pub fn for_answerer(principal: &PrincipalRef) -> Result<Self, String> {
        match principal {
            PrincipalRef::Human { .. } => Ok(AnswerSource::Principal),
            PrincipalRef::Machine { .. } => Ok(AnswerSource::Device),
            PrincipalRef::Agent { .. } | PrincipalRef::External { .. } => {
                Err(AGENT_VERDICT_REFUSAL.to_string())
            }
        }
    }
}

/// The operator's answer to a proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// This is how we do things: the tenet is approved.
    Yes,
    /// It is not: the tenet is declined.
    No,
    /// Approved on the thread's judgment rather than the operator's own.
    YourCall,
}

impl Verdict {
    pub const ALL: [Verdict; 3] = [Verdict::Yes, Verdict::No, Verdict::YourCall];

    /// The stored `verdict` column, and how every payload spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Yes => "yes",
            Verdict::No => "no",
            Verdict::YourCall => "your_call",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|verdict| verdict.as_str() == value)
    }

    /// The status this verdict gives a proposal.
    pub fn status(self) -> TenetStatus {
        match self {
            Verdict::Yes | Verdict::YourCall => TenetStatus::Approved,
            Verdict::No => TenetStatus::Declined,
        }
    }

    /// A verdict as a payload spells it: `"yes"`, `"no"`, or `"your_call"`.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        value
            .as_str()
            .and_then(|text| Self::parse(text.trim()))
            .ok_or_else(|| "verdict must be \"yes\", \"no\", or \"your_call\"".to_string())
    }
}

/// One stored tenet, exactly as persisted.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenetRow {
    pub id: String,
    pub project_id: String,
    pub number: i64,
    pub proposing_thread_id: String,
    pub slug: Option<String>,
    /// What the tenet says: how the project does things, in one sentence.
    pub statement: String,
    /// Why, the alternatives ruled out, and where it was argued. `None` on a
    /// tenet proposed without one, and on every tenet that predates the field.
    pub rationale: Option<String>,
    pub status: TenetStatus,
    pub verdict: Option<Verdict>,
    pub comment: Option<String>,
    pub answered_by: Option<PrincipalRef>,
    pub answer_source: Option<AnswerSource>,
    pub answered_at: Option<i64>,
    pub supersedes: Option<i64>,
    pub origin: TenetOrigin,
    pub legacy: Option<serde_json::Value>,
    pub contradiction_alerted_at: Option<i64>,
    pub proposer: PrincipalRef,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Record the operator's verdict ([`answer`]) and rouse the proposing thread
/// when the verdict wants it awake. This is the operator's answering act for
/// any caller holding an orchestrator.
pub async fn answer_and_wake(
    orch: &crate::orchestrator::Orchestrator,
    db: &crate::storage::LocalDb,
    project_id: &str,
    number: i64,
    verdict: Answer,
) -> Result<Answered, String> {
    let answered = answer(
        db,
        orch.services.emitter.as_ref(),
        orch.services.clock.now(),
        project_id,
        number,
        verdict,
    )
    .await?;
    // The operator just answered and is looking at the badge the answer clears,
    // so the thread rollup rebuilds now rather than after the cadence floor.
    orch.acknowledge_thread_status(project_id, "tenet-answered");
    if let Some(session) = answered.wake.as_deref() {
        if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            session,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            tracing::warn!(%error, "tenet #{number}'s verdict is queued, but rousing its thread failed");
        }
    }
    Ok(answered)
}

/// Announce a change to the tenets table to the desktop.
///
/// The frame names its project because the thread status rollup reads this
/// table: a scoped frame rebuilds that one project's sidebar, where an unscoped
/// one would rebuild every project's.
pub fn emit_changed(emitter: &dyn EventEmitter, project_id: &str, action: &str) {
    if let Err(error) = emitter.emit(
        "db-change",
        serde_json::json!({"table": TABLE, "projectId": project_id, "action": action}),
    ) {
        tracing::warn!(%error, "failed to emit tenets db-change notification");
    }
}

/// The canonical address of tenet `number` in `project_key`.
pub fn tenet_uri(project_key: &str, number: i64) -> String {
    cairn_common::uri::build_project_tenet_uri(project_key, &number.to_string())
}
