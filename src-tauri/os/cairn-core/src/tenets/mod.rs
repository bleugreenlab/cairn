//! Tenets: the human checkpoint for threads.
//!
//! A thread proposes a question with one to five options, option 1 being its
//! own recommendation, and the operator answers with an option, `none`, or
//! `your_call` (which adopts option 1). The answered tenet's text is
//! `options[selected_option]` and its rejected alternatives are the other
//! options; neither is stored twice.
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

pub const MAX_OPTIONS: usize = 5;

/// The ceiling on a proposal's question plus all of its options, in
/// characters. A proposal is read by the operator in one glance; the reasoning
/// behind it belongs in the issue it gates.
pub const MAX_PROPOSAL_CHARS: usize = 1500;

pub const OVER_LENGTH_REFUSAL: &str =
    "proposals are written for the operator; put detail in the issue description";

/// The refusal for any attempt to change a tenet's content.
pub const IMMUTABLE_REFUSAL: &str = "tenets are immutable; withdraw and propose again";

/// Why an agent cannot record a verdict, and where the operator records one.
pub const AGENT_VERDICT_REFUSAL: &str =
    "verdicts belong to the operator: a tenet is answered in the Tenets tab of the desktop app, never by an agent";

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Option `k`, 1-based as the operator reads it.
    Option(usize),
    /// None of the options: the tenet is declined.
    None,
    /// The operator defers to the thread, which adopts its recommendation,
    /// option 1.
    YourCall,
}

impl Verdict {
    /// The stored `verdict` column.
    pub fn kind(self) -> &'static str {
        match self {
            Verdict::Option(_) => "option",
            Verdict::None => "none",
            Verdict::YourCall => "your_call",
        }
    }

    /// A verdict as a payload spells it: an option number (`2` or `"2"`),
    /// `"none"`, or `"your_call"`.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        const SHAPE: &str =
            "verdict must be an option number (1-based), \"none\", or \"your_call\"";
        match value {
            serde_json::Value::Number(number) => number
                .as_u64()
                .filter(|k| *k >= 1)
                .map(|k| Verdict::Option(k as usize))
                .ok_or_else(|| SHAPE.to_string()),
            serde_json::Value::String(text) => match text.trim() {
                "none" => Ok(Verdict::None),
                "your_call" => Ok(Verdict::YourCall),
                digits => digits
                    .parse::<usize>()
                    .ok()
                    .filter(|k| *k >= 1)
                    .map(Verdict::Option)
                    .ok_or_else(|| SHAPE.to_string()),
            },
            _ => Err(SHAPE.to_string()),
        }
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
    pub question: String,
    pub options: Vec<String>,
    pub status: TenetStatus,
    pub verdict: Option<String>,
    /// 0-based.
    pub selected_option: Option<usize>,
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

impl TenetRow {
    /// The adopted text: the selected option, when one was selected.
    pub fn text(&self) -> Option<&str> {
        self.selected_option
            .and_then(|index| self.options.get(index))
            .map(String::as_str)
    }

    /// Every option the verdict did not adopt. For a declined tenet that is
    /// all of them; for an unanswered one, none yet.
    pub fn rejected(&self) -> Vec<&str> {
        if !self.status.has_verdict() {
            return Vec::new();
        }
        self.options
            .iter()
            .enumerate()
            .filter(|(index, _)| Some(*index) != self.selected_option)
            .map(|(_, option)| option.as_str())
            .collect()
    }
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
pub fn emit_changed(emitter: &dyn EventEmitter, action: &str) {
    if let Err(error) = emitter.emit(
        "db-change",
        serde_json::json!({"table": TABLE, "action": action}),
    ) {
        tracing::warn!(%error, "failed to emit tenets db-change notification");
    }
}

/// The canonical address of tenet `number` in `project_key`.
pub fn tenet_uri(project_key: &str, number: i64) -> String {
    cairn_common::uri::build_project_tenet_uri(project_key, &number.to_string())
}
