//! Camps: named conversations of a project (cairn/6255, cut down by cairn/7002).
//!
//! There are two kinds. Base camp is the project's one passive channel: every
//! open thread of the project is in it, implicitly, and what reaches it rides
//! along at each thread's next turn. A camp is a group conversation among the
//! threads and the operator that are its members, stored as camp-owned wake
//! subscription rows, and each post to it wakes every other member. The
//! delivery level is a property of the kind, never of a member.

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
/// camp, which is born with the project rather than created by anyone.
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
    Active,
    Archived,
}

impl CampStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "archived" => Some(Self::Archived),
            _ => None,
        }
    }
}

/// The stored key of the operator's membership. The literal the system already
/// attributes operator authorship to (`headline_read_cursors.reader`), until an
/// operator principal id exists to carry instead.
pub const OPERATOR_MEMBER_KEY: &str = "operator";

/// A camp member's stable identity.
///
/// A thread is its `threads.id`, never its name or a session job, so a rename
/// or a session replacement changes nothing about its membership.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CampSubject {
    Thread { thread_id: String },
    Operator,
}

impl CampSubject {
    /// The durable key a member row stores. A thread's key is its bare id,
    /// which move-to-team rekeys like any other id column; the operator's is
    /// [`OPERATOR_MEMBER_KEY`], which it leaves alone.
    pub fn key(&self) -> String {
        match self {
            Self::Thread { thread_id } => thread_id.clone(),
            Self::Operator => OPERATOR_MEMBER_KEY.to_string(),
        }
    }

    pub fn thread_id(&self) -> Option<&str> {
        match self {
            Self::Thread { thread_id } => Some(thread_id),
            Self::Operator => None,
        }
    }

    /// Whether a stored key names a thread rather than the operator.
    pub fn key_is_thread(key: &str) -> bool {
        key != OPERATOR_MEMBER_KEY
    }

    /// The subject a stored key names.
    pub fn from_key(key: &str) -> Self {
        if Self::key_is_thread(key) {
            Self::Thread {
                thread_id: key.to_string(),
            }
        } else {
            Self::Operator
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
    pub status: CampStatus,
    pub archived_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Camp {
    pub fn is_base(&self) -> bool {
        self.kind == CampKind::Base
    }

    pub fn is_archived(&self) -> bool {
        self.status == CampStatus::Archived
    }
}

/// One member of a camp. Never stored as such: base camp's members are its
/// project's open threads, and a camp's are its member rows minus closed
/// threads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampMember {
    pub subject_key: String,
    pub subject: CampSubject,
    pub since: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_keys_round_trip() {
        assert_eq!(CampSubject::Operator.key(), OPERATOR_MEMBER_KEY);
        assert!(!CampSubject::key_is_thread(OPERATOR_MEMBER_KEY));
        assert_eq!(CampSubject::from_key("operator"), CampSubject::Operator);
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
