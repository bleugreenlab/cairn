use std::str::FromStr;

use cairn_uri::{parse_uri, CairnResource};

use super::ConversationAddress;

pub const MESSAGE_CLASS_QUESTION: i64 = 1;
pub const MESSAGE_CLASS_PERMISSION: i64 = 1 << 1;
pub const MESSAGE_CLASS_NOTIFY: i64 = 1 << 2;
/// Every class that delivers operator ATTENTION: questions, permissions, and
/// notifications raised for the operator.
///
/// Deliberately not every bit. [`MESSAGE_CLASS_AGENT_TEXT`] is a different kind
/// of grant -- it lets an agent speak in a conversation rather than carrying an
/// ask to the operator -- so a gate can never request it, and a caller that
/// wants it names it explicitly.
pub const MESSAGE_CLASSES_ALL: i64 =
    MESSAGE_CLASS_QUESTION | MESSAGE_CLASS_PERMISSION | MESSAGE_CLASS_NOTIFY;
/// The operator's grant for agents to send free text into this conversation
/// by appending to the conversation's resource under `cairn://channels`. Discord structural surfaces (a
/// thread's or issue's own channel) carry it by default.
pub const MESSAGE_CLASS_AGENT_TEXT: i64 = 1 << 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    /// A conversation that is one thread's or issue's own surface.
    Structural,
    /// A conversation bound to a whole project rather than to one thread: its
    /// target is the project root and it carries exactly
    /// [`MESSAGE_CLASS_AGENT_TEXT`]. Unaddressed text there is a
    /// `channel_message` fact for routes to act on, and threads reply into it
    /// with agent text. Never resolved as a [`BindingTarget`].
    Ambient,
}

impl BindingKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Structural => "structural",
            Self::Ambient => "ambient",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingTarget {
    Thread { project: String, name: String },
    Issue { project: String, number: i32 },
}

impl BindingTarget {
    pub fn parse(uri: &str) -> Result<Self, String> {
        match parse_uri(uri) {
            Some(CairnResource::Thread {
                project,
                name,
                path,
            }) if path.is_empty() => Ok(Self::Thread { project, name }),
            Some(CairnResource::Issue { project, number }) => Ok(Self::Issue { project, number }),
            _ => Err(format!("not a thread or issue URI: {uri}")),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_target_round_trips_canonical_vocabulary() {
        for uri in ["cairn://p/cairn/general", "cairn://p/cairn/4006"] {
            assert_eq!(BindingTarget::parse(uri).unwrap().uri(), uri);
        }
        assert!(BindingTarget::parse("cairn:~/general").is_err());
        assert!(BindingTarget::parse("cairn://p/cairn/general/messages").is_err());
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
