//! Labels and resource addresses for channel conversations.
//!
//! A conversation's provider address (`discord:GUILD/CHANNEL`, `telegram:CHAT`,
//! `imessage:HANDLE`) is transport plumbing. Everything an agent or the operator
//! reads names a conversation by the label derived here from its binding, and
//! links it by the `cairn://channels/{provider}/...` URI derived here too. Both
//! are computed when read, never stored, so neither can drift from the binding
//! they describe.
//!
//! Discord URIs use Cairn names: `cairn://channels/discord/{project}` is a
//! project's ambient channel, and `cairn://channels/discord/{project}/{thread}`
//! or `.../{project}/{issue}` is the channel bound to that thread or issue.
//! Telegram and iMessage have no Cairn name, so their URIs carry the provider's
//! own chat id or handle.

use cairn_uri::{encode_path_segment, parse_uri, CairnResource};

use super::{bindings::BindingKind, ledger, ConversationAddress, ConversationProvider};
use crate::storage::LocalDb;

pub const CHANNELS_URI: &str = "cairn://channels";

pub fn provider_uri(provider: ConversationProvider) -> String {
    format!("{CHANNELS_URI}/{}", provider.id())
}

/// The Discord channel name Cairn gives the surface bound to `target_uri`: the
/// target's last segment, lowercased, with anything Discord would refuse turned
/// into `-`. A project's ambient channel is always `ambient`.
pub(crate) fn discord_channel_name(
    target_uri: Option<&str>,
    project_key: &str,
    ambient: bool,
) -> String {
    if ambient {
        return "ambient".into();
    }
    let source = target_uri
        .and_then(|uri| uri.rsplit('/').next())
        .unwrap_or(project_key);
    let mut name = source
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    name.truncate(80);
    if name.is_empty() {
        "cairn".into()
    } else {
        name
    }
}

/// The project a binding target belongs to, and the thread name or issue number
/// it names (`None` for a project root).
fn target_parts(target: &str) -> Option<(String, Option<String>)> {
    match parse_uri(target)? {
        CairnResource::Project { project } => Some((project, None)),
        CairnResource::Thread {
            project,
            name,
            path,
        } if path.is_empty() => Some((project, Some(name))),
        CairnResource::Issue { project, number } => Some((project, Some(number.to_string()))),
        _ => None,
    }
}

/// The human name of a conversation: `#channels · cairn · Discord`,
/// `#ambient · cairn · Discord`, `Ops · Telegram` (the chat's own `title`,
/// fetched by `titles`) or `operator chat · Telegram` without one,
/// `+15551234567 · iMessage`.
pub fn conversation_label(
    provider: ConversationProvider,
    conversation: &str,
    binding_kind: Option<&str>,
    target: Option<&str>,
    title: Option<&str>,
) -> String {
    let display = provider.display_name();
    match provider {
        ConversationProvider::Discord => {
            let ambient = binding_kind == Some(BindingKind::Ambient.as_str());
            match target.and_then(target_parts) {
                Some((project, segment)) => {
                    let name = discord_channel_name(target, &project, ambient || segment.is_none());
                    format!("#{name} · {project} · {display}")
                }
                None => format!("unbound channel · {display}"),
            }
        }
        ConversationProvider::Telegram => match title {
            Some(title) => format!("{title} · {display}"),
            None => format!("operator chat · {display}"),
        },
        ConversationProvider::IMessage => {
            let handle = conversation
                .strip_prefix("imessage:")
                .unwrap_or(conversation);
            format!("{handle} · {display}")
        }
    }
}

/// The `cairn://channels/...` resource a conversation is read and written at,
/// or `None` when it has none: a Discord channel with no structural or ambient
/// binding to name it.
pub fn conversation_uri(
    provider: ConversationProvider,
    conversation: &str,
    binding_kind: Option<&str>,
    target: Option<&str>,
) -> Option<String> {
    let base = provider_uri(provider);
    match provider {
        ConversationProvider::Discord => {
            let kind = binding_kind?;
            if kind != BindingKind::Ambient.as_str() && kind != BindingKind::Structural.as_str() {
                return None;
            }
            match target_parts(target?)? {
                (project, None) => Some(format!("{base}/{}", encode_path_segment(&project))),
                (project, Some(segment)) => Some(format!(
                    "{base}/{}/{}",
                    encode_path_segment(&project),
                    encode_path_segment(&segment)
                )),
            }
        }
        ConversationProvider::Telegram | ConversationProvider::IMessage => {
            let destination = conversation.split_once(':')?.1;
            Some(format!("{base}/{}", encode_path_segment(destination)))
        }
    }
}

/// Who wrote a message, as the conversation view and facts name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sender<'a> {
    /// An allowlisted person on the provider.
    Operator,
    /// A thread or node, by its home URI.
    Agent(&'a str),
    /// A route, by id.
    Route(&'a str),
    /// Cairn itself: asks, reviews, notices.
    Cairn,
    /// A provider sender the allowlist refused, by its provider id.
    Stranger(&'a str),
}

pub fn sender_label(sender: Sender<'_>) -> String {
    match sender {
        Sender::Operator => "operator".into(),
        Sender::Agent(home) => cairn_uri::canonicalize_uri_identity(home),
        Sender::Route(id) => format!("route {id}"),
        Sender::Cairn => "Cairn".into(),
        Sender::Stranger(id) => format!("unknown sender {id}"),
    }
}

/// A conversation a `cairn://channels/{provider}/...` URI resolved to: its
/// canonical provider address and every active binding it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConversation {
    pub provider: ConversationProvider,
    pub conversation: String,
    pub bindings: Vec<ledger::ActiveBinding>,
}

impl ResolvedConversation {
    /// The binding that names the conversation: its first.
    pub fn primary(&self) -> Option<&ledger::ActiveBinding> {
        self.bindings.first()
    }

    /// The conversation's label, with the provider's own `title` for it when
    /// the caller fetched one.
    pub fn label(&self, title: Option<&str>) -> String {
        let binding = self.primary();
        conversation_label(
            self.provider,
            &self.conversation,
            binding.map(|binding| binding.binding_kind.as_str()),
            binding.map(|binding| binding.target_uri.as_str()),
            title,
        )
    }
}

/// Resolve the (already percent-decoded) path segments after
/// `cairn://channels/{provider}/` to the conversation they name. Every refusal
/// names what the URI should look like.
pub async fn resolve_conversation_uri(
    db: &LocalDb,
    provider: ConversationProvider,
    segments: &[String],
) -> Result<ResolvedConversation, String> {
    let base = provider_uri(provider);
    match provider {
        ConversationProvider::Discord => {
            let (target, kind) = match segments {
                [project] => (
                    cairn_uri::build_project_uri(&cairn_uri::canonical_project(project)),
                    BindingKind::Ambient,
                ),
                [project, segment] => {
                    let project = cairn_uri::canonical_project(project);
                    let target = match segment.parse::<i32>() {
                        Ok(number) => format!("{}/{number}", cairn_uri::build_project_uri(&project)),
                        Err(_) => cairn_uri::build_thread_uri(&project, segment),
                    };
                    (target, BindingKind::Structural)
                }
                _ => {
                    return Err(format!(
                        "A Discord conversation is {base}/PROJECT (the project's #ambient) or {base}/PROJECT/THREAD_OR_ISSUE; read {base} for the conversations you can reach"
                    ))
                }
            };
            let target = cairn_uri::canonicalize_uri_identity(&target);
            let binding = ledger::binding_for_target(db, provider.id(), &target, kind)
                .await?
                .ok_or_else(|| {
                    format!(
                        "No Discord channel is bound to {target}; read {base} for the conversations you can reach"
                    )
                })?;
            Ok(ResolvedConversation {
                provider,
                conversation: binding.conversation.clone(),
                bindings: vec![binding],
            })
        }
        ConversationProvider::Telegram | ConversationProvider::IMessage => {
            let [destination] = segments else {
                return Err(format!(
                    "A {} conversation is {base}/{}; read {base} for the conversations you can reach",
                    provider.display_name(),
                    if provider == ConversationProvider::Telegram {
                        "CHAT_ID"
                    } else {
                        "HANDLE"
                    }
                ));
            };
            let address = format!("{}:{destination}", provider.id())
                .parse::<ConversationAddress>()
                .map_err(|error| format!("{base}/{destination} names no conversation: {error}"))?;
            let conversation = address.to_string();
            let bindings =
                ledger::bindings_for_conversation(db, provider.id(), &conversation).await?;
            Ok(ResolvedConversation {
                provider,
                conversation,
                bindings,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_name_a_conversation_without_its_provider_address() {
        let discord = ConversationProvider::Discord;
        assert_eq!(
            conversation_label(
                discord,
                "discord:1/9",
                Some("ambient"),
                Some("cairn://p/cairn"),
                None
            ),
            "#ambient · cairn · Discord"
        );
        assert_eq!(
            conversation_label(
                discord,
                "discord:1/2",
                Some("structural"),
                Some("cairn://p/cairn/channels"),
                None
            ),
            "#channels · cairn · Discord"
        );
        assert_eq!(
            conversation_label(
                discord,
                "discord:1/3",
                Some("structural"),
                Some("cairn://p/cairn/6772"),
                None
            ),
            "#6772 · cairn · Discord"
        );
        assert_eq!(
            conversation_label(discord, "discord:1/4", None, None, None),
            "unbound channel · Discord"
        );
        assert_eq!(
            conversation_label(
                ConversationProvider::Telegram,
                "telegram:42",
                None,
                None,
                None
            ),
            "operator chat · Telegram"
        );
        assert_eq!(
            conversation_label(
                ConversationProvider::Telegram,
                "telegram:42",
                None,
                None,
                Some("Ops")
            ),
            "Ops · Telegram"
        );
        assert_eq!(
            conversation_label(
                ConversationProvider::IMessage,
                "imessage:+15551234567",
                None,
                None,
                None
            ),
            "+15551234567 · iMessage"
        );
    }

    #[test]
    fn uris_use_cairn_names_where_a_name_exists() {
        let discord = ConversationProvider::Discord;
        assert_eq!(
            conversation_uri(
                discord,
                "discord:1/9",
                Some("ambient"),
                Some("cairn://p/cairn")
            ),
            Some("cairn://channels/discord/cairn".into())
        );
        assert_eq!(
            conversation_uri(
                discord,
                "discord:1/2",
                Some("structural"),
                Some("cairn://p/cairn/channels")
            ),
            Some("cairn://channels/discord/cairn/channels".into())
        );
        assert_eq!(
            conversation_uri(
                discord,
                "discord:1/3",
                Some("structural"),
                Some("cairn://p/cairn/6772")
            ),
            Some("cairn://channels/discord/cairn/6772".into())
        );
        assert_eq!(conversation_uri(discord, "discord:1/4", None, None), None);
        assert_eq!(
            conversation_uri(
                ConversationProvider::Telegram,
                "telegram:-100123",
                None,
                None
            ),
            Some("cairn://channels/telegram/-100123".into())
        );
        assert_eq!(
            conversation_uri(
                ConversationProvider::IMessage,
                "imessage:+15551234567",
                None,
                None
            ),
            Some("cairn://channels/imessage/%2B15551234567".into())
        );
    }

    #[tokio::test]
    async fn a_conversation_uri_resolves_through_its_binding() {
        let db = crate::storage::migrated_test_db("channels-label-resolve.db").await;
        for (conversation, target, kind, classes) in [
            ("discord:1/9", "cairn://p/cairn", BindingKind::Ambient, 8),
            (
                "discord:1/2",
                "cairn://p/cairn/channels",
                BindingKind::Structural,
                15,
            ),
            (
                "discord:1/3",
                "cairn://p/cairn/6772",
                BindingKind::Structural,
                15,
            ),
        ] {
            assert!(
                ledger::bind_target(&db, "discord", conversation, target, kind, classes, 1)
                    .await
                    .unwrap()
            );
        }
        let resolve = |segments: &[&str]| {
            let db = &db;
            let segments = segments.iter().map(|s| s.to_string()).collect::<Vec<_>>();
            async move { resolve_conversation_uri(db, ConversationProvider::Discord, &segments).await }
        };
        let ambient = resolve(&["cairn"]).await.unwrap();
        assert_eq!(ambient.conversation, "discord:1/9");
        assert_eq!(ambient.label(None), "#ambient · cairn · Discord");
        assert_eq!(
            resolve(&["cairn", "channels"]).await.unwrap().conversation,
            "discord:1/2"
        );
        assert_eq!(
            resolve(&["cairn", "6772"]).await.unwrap().conversation,
            "discord:1/3"
        );
        let missing = resolve(&["cairn", "nope"]).await.unwrap_err();
        assert!(
            missing.contains("No Discord channel is bound to cairn://p/cairn/nope"),
            "{missing}"
        );
        assert!(resolve(&["a", "b", "c"]).await.is_err());

        let telegram =
            resolve_conversation_uri(&db, ConversationProvider::Telegram, &["42".to_string()])
                .await
                .unwrap();
        assert_eq!(telegram.conversation, "telegram:42");
        assert!(telegram.bindings.is_empty());
    }
}
