use std::str::FromStr;

use cairn_common::uri::{parse_uri, CairnResource};

use super::ConversationAddress;

pub const MESSAGE_CLASS_QUESTION: i64 = 1;
pub const MESSAGE_CLASS_PERMISSION: i64 = 1 << 1;
pub const MESSAGE_CLASS_NOTIFY: i64 = 1 << 2;
pub const MESSAGE_CLASSES_ALL: i64 =
    MESSAGE_CLASS_QUESTION | MESSAGE_CLASS_PERMISSION | MESSAGE_CLASS_NOTIFY;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Follow,
    Structural,
}

impl BindingKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Follow => "follow",
            Self::Structural => "structural",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowTarget {
    Thread { project: String, name: String },
    Issue { project: String, number: i32 },
}

impl FollowTarget {
    pub fn parse(uri: &str) -> Result<Self, String> {
        match parse_uri(uri) {
            Some(CairnResource::Thread {
                project,
                name,
                path,
            }) if path.is_empty() => Ok(Self::Thread { project, name }),
            Some(CairnResource::Issue { project, number }) => Ok(Self::Issue { project, number }),
            _ => Err(format!("not a followable Cairn URI: {uri}")),
        }
    }

    pub fn project(&self) -> &str {
        match self {
            Self::Thread { project, .. } | Self::Issue { project, .. } => project,
        }
    }

    pub fn uri(&self) -> String {
        match self {
            Self::Thread { project, name } => format!("cairn://p/{project}/{name}"),
            Self::Issue { project, number } => format!("cairn://p/{project}/{number}"),
        }
    }

    pub fn selector(&self) -> String {
        match self {
            Self::Thread { name, .. } => name.clone(),
            Self::Issue { number, .. } => number.to_string(),
        }
    }
}

pub fn canonical_conversation(provider: &str, conversation: &str) -> Result<String, String> {
    let parsed = ConversationAddress::from_str(conversation).map_err(|error| error.to_string())?;
    if parsed.provider().id() != provider {
        return Err(format!(
            "conversation provider {} does not match binding provider {provider}",
            parsed.provider()
        ));
    }
    let canonical = parsed.to_string();
    if canonical != conversation {
        return Err(format!(
            "conversation address must be canonical: expected {canonical}"
        ));
    }
    Ok(canonical)
}

pub fn legacy_conversation(provider: &str) -> Result<String, String> {
    match provider {
        "imessage" => Ok("imessage:legacy".into()),
        "telegram" => Ok("telegram:0".into()),
        "discord" => Ok("discord:0/0".into()),
        _ => Err(format!("unsupported conversation provider: {provider}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follow_target_round_trips_canonical_vocabulary() {
        for uri in ["cairn://p/cairn/general", "cairn://p/cairn/4006"] {
            assert_eq!(FollowTarget::parse(uri).unwrap().uri(), uri);
        }
        assert!(FollowTarget::parse("cairn:~/general").is_err());
        assert!(FollowTarget::parse("cairn://p/cairn/general/messages").is_err());
    }

    #[test]
    fn conversation_validation_requires_provider_and_canonical_address() {
        assert_eq!(
            canonical_conversation("discord", "discord:42/7").unwrap(),
            "discord:42/7"
        );
        assert!(canonical_conversation("telegram", "discord:42/7").is_err());
        assert!(canonical_conversation("discord", "42/7").is_err());
    }
}
