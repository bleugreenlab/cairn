//! Camps: durable project-level coordination containers (cairn/6255).
//!
//! A camp owns intent and references. Who is involved is stored once, in the
//! place each kind of involvement already lives: the lead on the camp row,
//! watch/wake as camp-subject wake subscriptions, confirm with the artifact
//! gate. [`CampParticipant`] is the census that projects those sources into one
//! effective role per principal; it is never stored.

use cairn_common::identity::PrincipalRef;
use serde::{Deserialize, Serialize};

/// Every project's one base camp is addressed by this slug, and no ordinary
/// camp may take it.
pub const BASE_CAMP_SLUG: &str = "base";

/// The longest slug a camp may take: an address segment, not a description.
pub const CAMP_SLUG_MAX_LEN: usize = 64;

/// Whether `slug` may address an ordinary camp.
///
/// A slug is a stable URI segment: lowercase ASCII letters, digits and single
/// hyphens, starting with a letter. `base` is reserved for the project's base
/// camp, which is born with the project rather than pitched.
pub fn validate_camp_slug(slug: &str) -> Result<(), String> {
    if slug.is_empty() || slug.len() > CAMP_SLUG_MAX_LEN {
        return Err(format!(
            "camp slug must be 1 to {CAMP_SLUG_MAX_LEN} characters"
        ));
    }
    if slug == BASE_CAMP_SLUG {
        return Err("camp slug `base` is reserved for the project's base camp".into());
    }
    if !slug.starts_with(|c: char| c.is_ascii_lowercase()) {
        return Err("camp slug must start with a lowercase letter".into());
    }
    if !slug
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        || slug.ends_with('-')
        || slug.contains("--")
    {
        return Err(
            "camp slug may contain only lowercase letters, digits and single hyphens, and may not end with a hyphen"
                .into(),
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampKind {
    Base,
    Camp,
}

impl CampKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Camp => "camp",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "base" => Some(Self::Base),
            "camp" => Some(Self::Camp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampStatus {
    Pitched,
    Struck,
}

impl CampStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pitched => "pitched",
            Self::Struck => "struck",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pitched" => Some(Self::Pitched),
            "struck" => Some(Self::Struck),
            _ => None,
        }
    }
}

/// How involved a principal is in a camp.
///
/// The order is the attention order: each role receives at least what every
/// role below it receives, which is why a census can take the maximum of a
/// principal's sources and never needs to combine them otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampRole {
    /// Passive lines only.
    Watch,
    /// The camp's facts wake it as they wake the lead.
    Wake,
    /// A veto: the camp's gated artifacts wait on this principal's signature.
    Confirm,
    /// The camp's one lead thread.
    Lead,
}

impl CampRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Watch => "watch",
            Self::Wake => "wake",
            Self::Confirm => "confirm",
            Self::Lead => "lead",
        }
    }
}

/// The level a camp-subject subscription stores. Confirm and lead are not
/// levels: they live with the artifact gate and on the camp row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampLevel {
    Watch,
    Wake,
}

impl CampLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Watch => "watch",
            Self::Wake => "wake",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "watch" => Some(Self::Watch),
            "wake" => Some(Self::Wake),
            _ => None,
        }
    }

    pub fn role(self) -> CampRole {
        match self {
            Self::Watch => CampRole::Watch,
            Self::Wake => CampRole::Wake,
        }
    }
}

/// The stable identity a camp role is keyed by.
///
/// A thread is its `threads.id`, never its name or a session job, so a rename
/// or a session replacement changes nothing about its involvement. A human is
/// issuer plus subject: the same subject string under two issuers is two
/// people.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CampSubject {
    Thread {
        thread_id: String,
    },
    Human {
        issuer: String,
        subject: String,
        organization: Option<String>,
    },
}

/// Separates issuer from subject in a human's key. A control character, so no
/// validated principal field can contain it (`PrincipalRef` validation refuses
/// control characters), which keeps the key unambiguous.
const HUMAN_KEY_SEPARATOR: char = '\u{1f}';
const HUMAN_KEY_PREFIX: &str = "human:";

impl CampSubject {
    /// The durable key a role row stores. A thread's key is its bare id, which
    /// move-to-team rekeys like any other id column; a human's key is not an
    /// id and is left alone.
    pub fn key(&self) -> String {
        match self {
            Self::Thread { thread_id } => thread_id.clone(),
            Self::Human {
                issuer, subject, ..
            } => format!("{HUMAN_KEY_PREFIX}{issuer}{HUMAN_KEY_SEPARATOR}{subject}"),
        }
    }

    pub fn thread_id(&self) -> Option<&str> {
        match self {
            Self::Thread { thread_id } => Some(thread_id),
            Self::Human { .. } => None,
        }
    }

    /// Whether a stored key names a thread rather than a human.
    pub fn key_is_thread(key: &str) -> bool {
        !key.starts_with(HUMAN_KEY_PREFIX)
    }

    /// The human subject a principal names, if it names one. Thread subjects
    /// are resolved from their URI by the caller, which owns the thread table.
    pub fn human_from_principal(principal: &PrincipalRef) -> Option<Self> {
        match principal {
            PrincipalRef::Human {
                issuer,
                subject,
                organization,
            } => Some(Self::Human {
                issuer: issuer.clone(),
                subject: subject.clone(),
                organization: organization.clone(),
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Camp {
    pub id: String,
    pub project_id: String,
    pub slug: String,
    pub title: String,
    pub kind: CampKind,
    pub lead_thread_id: String,
    /// The thread the lead has been offered to and has not yet accepted.
    pub lead_offer_thread_id: Option<String>,
    pub status: CampStatus,
    pub policy_epoch: i64,
    pub struck_at: Option<i64>,
    pub struck_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Camp {
    pub fn is_base(&self) -> bool {
        self.kind == CampKind::Base
    }

    pub fn is_struck(&self) -> bool {
        self.status == CampStatus::Struck
    }
}

/// One principal's effective involvement in one camp, projected from every
/// source that records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampParticipant {
    pub subject_key: String,
    pub subject: CampSubject,
    /// The principal as recorded when it joined. A thread's current name is
    /// resolved from `subject` on the way to a surface, not from this.
    pub principal: Option<PrincipalRef>,
    /// The maximum of every source below.
    pub role: CampRole,
    pub lead: bool,
    pub level: Option<CampLevel>,
    pub reason: Option<String>,
    /// Who set this participant's level, when someone did: absent for the
    /// system's own seating (birth, backfill) and for the lead edge.
    pub assigned_by: Option<PrincipalRef>,
    pub since: i64,
}

/// What a camp journal row records. Every kind the journal's CHECK admits is
/// named here, so a history line is generated from a known kind; a stored kind
/// this build does not know reads as [`CampEventKind::Other`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CampEventKind {
    Pitched,
    Joined,
    RoleChanged,
    Left,
    LeadOffered,
    LeadChanged,
    Retitled,
    Struck,
    IssueJoined,
    IssueLeft,
    Other(String),
}

impl CampEventKind {
    pub fn parse(kind: &str) -> Self {
        match kind {
            "pitched" => Self::Pitched,
            "joined" => Self::Joined,
            "role_changed" => Self::RoleChanged,
            "left" => Self::Left,
            "lead_offered" => Self::LeadOffered,
            "lead_changed" => Self::LeadChanged,
            "retitled" => Self::Retitled,
            "struck" => Self::Struck,
            "issue_joined" => Self::IssueJoined,
            "issue_left" => Self::IssueLeft,
            other => Self::Other(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_order_by_attention() {
        assert!(CampRole::Watch < CampRole::Wake);
        assert!(CampRole::Wake < CampRole::Confirm);
        assert!(CampRole::Confirm < CampRole::Lead);
        assert_eq!(CampLevel::Wake.role(), CampRole::Wake);
    }

    #[test]
    fn the_same_subject_under_two_issuers_is_two_humans() {
        let alice = |issuer: &str| CampSubject::Human {
            issuer: issuer.into(),
            subject: "alice".into(),
            organization: None,
        };
        assert_ne!(alice("github").key(), alice("google").key());
        assert!(!CampSubject::key_is_thread(&alice("github").key()));
        let thread = CampSubject::Thread {
            thread_id: "6f1c0a52-8f0e-4d55-9c1a-2b8d3a1e7f00".into(),
        };
        assert_eq!(thread.key(), "6f1c0a52-8f0e-4d55-9c1a-2b8d3a1e7f00");
        assert!(CampSubject::key_is_thread(&thread.key()));
    }

    #[test]
    fn slugs_are_address_segments_and_base_is_reserved() {
        for good in ["rebuild", "git-free", "m6082", "a"] {
            assert!(validate_camp_slug(good).is_ok(), "{good}");
        }
        for bad in [
            "",
            "base",
            "Rebuild",
            "9lives",
            "two--hyphens",
            "trailing-",
            "has space",
            "a/b",
        ] {
            assert!(validate_camp_slug(bad).is_err(), "{bad}");
        }
        assert!(validate_camp_slug(&"a".repeat(CAMP_SLUG_MAX_LEN + 1)).is_err());
    }
}
