use serde::{Deserialize, Serialize};

use crate::storage::{DbResult, RowExt};
use cairn_domain::DeliveryUrgency;

pub const SOURCE_KIND_ISSUE: &str = "issue";
pub const SOURCE_KIND_PEER: &str = "peer";
const SOURCE_KIND_USER: &str = "user";
pub const SOURCE_KIND_PROCESS: &str = "process";
const SOURCE_KIND_RESOURCE: &str = "resource";
pub const SOURCE_KIND_CONDITION: &str = "condition";
/// A thread standing trigger over the fact log. The row's `source_ref` is the
/// trigger clause's canonical JSON and its fact kinds name the clause's fact
/// source; the fact log worker delivers a matching row as a targeted wake.
pub const SOURCE_KIND_FACT: &str = "fact";
/// The clause-ref namespace of a fact wake owed to a recipient the row itself
/// names (a mention, a post's author) rather than to a standing trigger. The
/// full ref is `addressed:{source}.{field}`. No row is stored for it: with no
/// persisted subscription of the same ref, routing synthesizes an active one
/// ([`addressed_fact_subscription`]), so a persisted row on the ref is how a
/// node mutes or refuses one addressee field.
pub const ADDRESSED_FACT_REF_PREFIX: &str = "addressed:";
pub const SOURCE_KIND_ISSUE_COMMENT: &str = "issue_comment";
pub const SOURCE_KIND_ISSUE_MESSAGE: &str = "issue_message";
pub const FACT_KIND_MESSAGE: &str = "message";
pub const FACT_KIND_TERMINAL_EXIT: &str = "terminal_exit";
pub const FACT_KIND_TERMINAL_OUTPUT: &str = "terminal_output";
/// A node's project check lanes reaching a state nothing will move further.
/// Carried on a `condition` source keyed by the node's canonical `/checks` URI.
pub const FACT_KIND_CHECKS_SETTLED: &str = "checks_settled";
/// A child node that still claims to be working holds no session that could do
/// it (CAIRN-4609). Issue-sourced, like every other child fact, so it reaches
/// the node currently driving the parent through the same derived watch.
pub const FACT_KIND_CHILD_STALLED: &str = "child_stalled";

/// Whether a job status is one the projection will never move again on its own.
///
/// Stated once, because two places decide it and they must agree: child-stall
/// detection asks it to rule a dead child out of scope, and the answerability
/// predicate asks it to rule a dead watcher out of holding an ask. Two copies of
/// this list disagreeing means an ask is held by a job that ended — the silent
/// stranding the ask rule exists to prevent.
pub fn job_is_terminal(status: &str) -> bool {
    matches!(status, "failed" | "cancelled" | "complete")
}

// CAIRN-1647: the attention ledger collapses the old `agent_idle_with_work` +
// `pr_state_change` fan-out into a single `review` item kind. Default child
// subscriptions carry the new vocabulary; legacy `agent_idle_with_work` /
// `pr_state_change` subscription rows still match `review` items via
// `fact_kind_matches` and `REVIEW_LEGACY_FACT_KINDS`, so old rows keep working.
pub const DEFAULT_CHILD_FACT_KINDS: &[&str] = &[
    "question",
    "permission",
    "review",
    "resolved",
    FACT_KIND_MESSAGE,
    FACT_KIND_CHILD_STALLED,
];
pub const REVIEW_LEGACY_FACT_KINDS: &[&str] = &["agent_idle_with_work", "pr_state_change"];

/// `created_by` marker on the synthetic parent-axis subscription returned by
/// [`derived_child_subscription`]. It never reaches the database: the recipient
/// is resolved live on every wake, so there is no row to go stale.
pub const CREATED_BY_DERIVED: &str = "derived";

/// The parent-axis child-attention watch as a subscription value, so the derived
/// coordinator flows through the same matching, mute, and routing rules as a
/// persisted row. Constructed only when the coordinating node holds no explicit
/// row of its own (`child::subscriptions_governing_issue`).
pub fn derived_child_subscription(job_id: &str, issue_uri: &str) -> WakeSubscription {
    WakeSubscription {
        id: format!("derived:parent:{job_id}:{issue_uri}"),
        job_id: job_id.to_string(),
        source_kind: SOURCE_KIND_ISSUE.to_string(),
        source_ref: Some(issue_uri.to_string()),
        fact_kinds: Some(
            DEFAULT_CHILD_FACT_KINDS
                .iter()
                .map(|kind| (*kind).to_string())
                .collect(),
        ),
        state: WakeSubscriptionState::Active,
        mute_until_kind: None,
        mute_until_ref: None,
        created_by: CREATED_BY_DERIVED.to_string(),
        created_at: 0,
        updated_at: 0,
        one_shot: false,
        match_phrase: None,
    }
}

/// The derived subscription a fact wake carries when the row addresses its
/// recipient directly (see [`ADDRESSED_FACT_REF_PREFIX`]). Like
/// [`derived_child_subscription`], it never reaches the database.
pub fn addressed_fact_subscription(
    job_id: &str,
    clause_ref: &str,
    fact_kind: &str,
) -> WakeSubscription {
    WakeSubscription {
        id: format!("derived:addressed:{job_id}:{clause_ref}"),
        job_id: job_id.to_string(),
        source_kind: SOURCE_KIND_FACT.to_string(),
        source_ref: Some(clause_ref.to_string()),
        fact_kinds: Some(vec![fact_kind.to_string()]),
        state: WakeSubscriptionState::Active,
        mute_until_kind: None,
        mute_until_ref: None,
        created_by: CREATED_BY_DERIVED.to_string(),
        created_at: 0,
        updated_at: 0,
        one_shot: false,
        match_phrase: None,
    }
}

/// Typed source taxonomy for every external wake a job can subscribe to.
///
/// Time is deliberately absent: wakes are event-routed, not polled/scheduled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WakeSource {
    Issue {
        reference: String,
    },
    Peer {
        reference: Option<String>,
    },
    User,
    Process {
        reference: String,
    },
    Resource {
        reference: String,
    },
    Condition {
        reference: String,
    },
    /// A fact log row that matched a thread standing trigger. The reference
    /// is the trigger clause's canonical JSON, compared verbatim.
    Fact {
        clause_ref: String,
    },
}

impl WakeSource {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Issue { .. } => SOURCE_KIND_ISSUE,
            Self::Peer { .. } => SOURCE_KIND_PEER,
            Self::User => SOURCE_KIND_USER,
            Self::Process { .. } => SOURCE_KIND_PROCESS,
            Self::Resource { .. } => SOURCE_KIND_RESOURCE,
            Self::Condition { .. } => SOURCE_KIND_CONDITION,
            Self::Fact { .. } => SOURCE_KIND_FACT,
        }
    }

    pub fn reference(&self) -> Option<&str> {
        match self {
            Self::Issue { reference }
            | Self::Process { reference }
            | Self::Resource { reference }
            | Self::Condition { reference } => Some(reference.as_str()),
            Self::Fact { clause_ref } => Some(clause_ref.as_str()),
            Self::Peer { reference } => reference.as_deref(),
            Self::User => None,
        }
    }

    pub fn from_parts(kind: &str, reference: Option<&str>) -> Result<Self, String> {
        match kind {
            SOURCE_KIND_ISSUE => Ok(Self::Issue { reference: required_ref(kind, reference)? }),
            SOURCE_KIND_PEER => Ok(Self::Peer {
                reference: reference
                    .filter(|value| !value.is_empty())
                    .map(cairn_common::uri::canonicalize_uri_identity),
            }),
            SOURCE_KIND_USER => {
                if reference.is_some() {
                    return Err("wake source kind 'user' must not include ref".to_string());
                }
                Ok(Self::User)
            }
            SOURCE_KIND_PROCESS => Ok(Self::Process { reference: required_ref(kind, reference)? }),
            SOURCE_KIND_RESOURCE => Ok(Self::Resource { reference: required_ref(kind, reference)? }),
            SOURCE_KIND_CONDITION => Ok(Self::Condition { reference: required_ref(kind, reference)? }),
            // A clause is JSON, not a URI: it is compared exactly as compiled.
            SOURCE_KIND_FACT => Ok(Self::Fact {
                clause_ref: reference
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| format!("wake source kind '{kind}' requires ref"))?,
            }),
            _ => Err(format!(
                "unknown wake source kind '{kind}' (expected issue, peer, user, process, resource, condition, or fact)"
            )),
        }
    }
}

/// Whether a row of this kind with no `source_ref` is a **standing watch** over
/// every reference of the kind, rather than an under-specified scope.
///
/// Only `peer` says yes: its unscoped row is the only form the subscription
/// has, and a ref-less `peer` row watches every peer, so a fact carrying a
/// concrete peer URI still has to find it. Every other kind requires a ref at
/// subscribe time, so a NULL there would be a row that cannot match anything.
pub fn unscoped_row_is_a_standing_watch(kind: &str) -> bool {
    kind == SOURCE_KIND_PEER
}

fn required_ref(kind: &str, reference: Option<&str>) -> Result<String, String> {
    reference
        .filter(|value| !value.is_empty())
        .map(cairn_common::uri::canonicalize_uri_identity)
        .ok_or_else(|| format!("wake source kind '{kind}' requires ref"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeScope {
    pub source: WakeSource,
    pub fact_kinds: Option<Vec<String>>,
}

impl WakeScope {
    pub fn new(source: WakeSource, fact_kinds: Option<Vec<String>>) -> Self {
        Self { source, fact_kinds }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeEvent {
    pub source: WakeSource,
    pub fact_kind: String,
    pub detail_uri: Option<String>,
    pub delivery: WakeDelivery,
    pub urgency: DeliveryUrgency,
    /// What a muted subscription does with this event. The producer decides,
    /// because it is a property of what the event is, not of who muted it.
    #[serde(default)]
    pub muted: MutedDelivery,
}

/// How a non-interrupt event reaches a subscription its recipient has muted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutedDelivery {
    /// Not at all. The default for every live caller with no ride-along
    /// channel of its own (terminal, condition, resource, process, broadcast).
    #[default]
    Drop,
    /// Passively: it rides along with the recipient's next run instead of
    /// waking it (CAIRN-1900: mute governs how loudly, never whether).
    RideAlong,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WakeDelivery {
    /// Deliver one known subscriber job if its best matching subscription accepts.
    Targeted {
        subscriber_job_id: String,
        #[serde(default)]
        sender_name: Option<String>,
        message: String,
    },
    /// Deliver every job whose wake subscriptions match the event source/fact.
    Broadcast { message: String },
    /// Message-like content for digest routing. Accepted active delivery remains
    /// with the durable message/side-channel row that already exists.
    MessageDigest {
        subscriber_job_id: String,
        content: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeRouteAction {
    Delivered,
    Suppressed,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeSubscriptionState {
    Active,
    Muted,
    Unsubscribed,
}

impl WakeSubscriptionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Muted => "muted",
            Self::Unsubscribed => "unsubscribed",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "muted" => Self::Muted,
            "unsubscribed" => Self::Unsubscribed,
            _ => Self::Active,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeSubscription {
    pub id: String,
    pub job_id: String,
    pub source_kind: String,
    pub source_ref: Option<String>,
    pub fact_kinds: Option<Vec<String>>,
    pub state: WakeSubscriptionState,
    mute_until_kind: Option<String>,
    mute_until_ref: Option<String>,
    pub created_by: String,
    created_at: i64,
    pub updated_at: i64,
    /// Consumed (row deleted) the first time a matching wake routes to it, so a
    /// one-time fact like a terminal exit can never wake the subscriber twice.
    pub one_shot: bool,
    /// Literal substring an output-phrase terminal subscription watches for in
    /// the terminal's output. `None` for every other subscription kind.
    pub match_phrase: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SuppressedWake {
    pub id: String,
    pub subscription_id: Option<String>,
    pub job_id: String,
    pub source_kind: String,
    pub source_ref: Option<String>,
    pub fact_kind: Option<String>,
    pub occurrences: i64,
    pub latest_detail_uri: Option<String>,
    pub content: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub delivered_at: Option<i64>,
}

impl SuppressedWake {
    pub fn render_digest(notices: &[SuppressedWake]) -> String {
        Self::render_digest_with_context(notices, None)
    }

    pub fn render_digest_with_context(
        notices: &[SuppressedWake],
        woken_by: Option<&WakeSource>,
    ) -> String {
        if notices.is_empty() {
            return String::new();
        }
        let mut facts = Vec::new();
        let mut messages = Vec::new();
        for notice in notices {
            if let Some(content) = &notice.content {
                messages.push(format!("  • {}", content));
            } else {
                let source = match &notice.source_ref {
                    Some(source_ref) => format!("{} {}", notice.source_kind, source_ref),
                    None => notice.source_kind.clone(),
                };
                let kind = notice.fact_kind.as_deref().unwrap_or("event");
                let mut line = format!("  • {source} / {kind} ×{}", notice.occurrences.max(1));
                if let Some(detail_uri) = &notice.latest_detail_uri {
                    line.push_str(&format!(" — latest: {detail_uri}"));
                }
                facts.push(line);
            }
        }
        let mut lifted = notices
            .iter()
            .map(|notice| match &notice.source_ref {
                Some(source_ref) => format!("{} {}", notice.source_kind, source_ref),
                None => notice.source_kind.clone(),
            })
            .collect::<Vec<_>>();
        lifted.sort();
        lifted.dedup();
        let lifted = lifted.join(", ");
        let woken_by = woken_by
            .map(|source| match source.reference() {
                Some(reference) => format!("{} {}", source.kind(), reference),
                None => source.kind().to_string(),
            })
            .unwrap_or_else(|| "live resume".to_string());
        let mut out = format!(
            "[Resuming — lifting wake snooze on {lifted}; woken by: {woken_by}]\nWhile snoozed:"
        );
        if facts.is_empty() {
            out.push_str("\n  • No attention facts.");
        } else {
            out.push('\n');
            out.push_str(&facts.join("\n"));
        }
        if !messages.is_empty() {
            out.push_str(&format!(
                "\nMessages ({}):\n{}",
                messages.len(),
                messages.join("\n")
            ));
        }
        out
    }
}

pub fn fact_kinds_json(fact_kinds: Option<&[String]>) -> Option<String> {
    fact_kinds.map(|values| {
        let mut values = values.to_vec();
        values.sort();
        values.dedup();
        serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string())
    })
}

pub fn subscription_from_row(row: &cairn_db::turso::Row) -> DbResult<WakeSubscription> {
    let fact_kinds_json = row.opt_text(4)?;
    let fact_kinds = fact_kinds_json
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok());
    Ok(WakeSubscription {
        id: row.text(0)?,
        job_id: row.text(1)?,
        source_kind: row.text(2)?,
        source_ref: row.opt_text(3)?,
        fact_kinds,
        state: WakeSubscriptionState::from_str(&row.text(5)?),
        mute_until_kind: row.opt_text(6)?,
        mute_until_ref: row.opt_text(7)?,
        created_by: row.text(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        one_shot: row.i64(11)? != 0,
        match_phrase: row.opt_text(12)?,
    })
}

pub fn suppressed_from_row(row: &cairn_db::turso::Row) -> DbResult<SuppressedWake> {
    Ok(SuppressedWake {
        id: row.text(0)?,
        subscription_id: row.opt_text(1)?,
        job_id: row.text(2)?,
        source_kind: row.text(3)?,
        source_ref: row.opt_text(4)?,
        fact_kind: row.opt_text(5)?,
        occurrences: row.i64(6)?,
        latest_detail_uri: row.opt_text(7)?,
        content: row.opt_text(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
        delivered_at: row.opt_i64(11)?,
    })
}
