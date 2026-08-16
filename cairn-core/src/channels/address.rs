use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConversationProvider {
    IMessage,
    Telegram,
    Discord,
}

impl ConversationProvider {
    pub const ALL: [Self; 3] = [Self::IMessage, Self::Telegram, Self::Discord];

    pub const fn id(self) -> &'static str {
        match self {
            Self::IMessage => "imessage",
            Self::Telegram => "telegram",
            Self::Discord => "discord",
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            Self::IMessage => "iMessage",
            Self::Telegram => "Telegram",
            Self::Discord => "Discord",
        }
    }
}

impl fmt::Display for ConversationProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.id())
    }
}

impl FromStr for ConversationProvider {
    type Err = ConversationAddressError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "imessage" => Ok(Self::IMessage),
            "telegram" => Ok(Self::Telegram),
            "discord" => Ok(Self::Discord),
            provider => Err(ConversationAddressError::UnknownProvider(
                provider.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationDestination {
    IMessage { handle: String },
    Telegram { chat_id: i64 },
    Discord { guild_id: u64, channel_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationAddress {
    destination: ConversationDestination,
}

impl ConversationAddress {
    pub fn imessage(handle: &str) -> Result<Self, ConversationAddressError> {
        let handle = super::imessage::normalize_handle(handle);
        if handle.is_empty() {
            return Err(ConversationAddressError::Malformed {
                provider: ConversationProvider::IMessage,
                message: "iMessage handle must not be empty".into(),
            });
        }
        Ok(Self {
            destination: ConversationDestination::IMessage { handle },
        })
    }

    pub fn telegram(chat_id: &str) -> Result<Self, ConversationAddressError> {
        let chat_id =
            chat_id
                .trim()
                .parse::<i64>()
                .map_err(|_| ConversationAddressError::Malformed {
                    provider: ConversationProvider::Telegram,
                    message: "Telegram chat ID must be a signed integer".into(),
                })?;
        Ok(Self {
            destination: ConversationDestination::Telegram { chat_id },
        })
    }

    pub fn discord(guild_id: &str, channel_id: &str) -> Result<Self, ConversationAddressError> {
        let numeric = |component: &str, name: &str| {
            component
                .trim()
                .parse::<u64>()
                .map_err(|_| ConversationAddressError::Malformed {
                    provider: ConversationProvider::Discord,
                    message: format!("Discord {name} ID must be an unsigned integer"),
                })
        };
        Ok(Self {
            destination: ConversationDestination::Discord {
                guild_id: numeric(guild_id, "guild")?,
                channel_id: numeric(channel_id, "channel")?,
            },
        })
    }

    pub const fn provider(&self) -> ConversationProvider {
        match self.destination {
            ConversationDestination::IMessage { .. } => ConversationProvider::IMessage,
            ConversationDestination::Telegram { .. } => ConversationProvider::Telegram,
            ConversationDestination::Discord { .. } => ConversationProvider::Discord,
        }
    }

    pub const fn destination(&self) -> &ConversationDestination {
        &self.destination
    }
}

impl FromStr for ConversationAddress {
    type Err = ConversationAddressError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (provider, destination) = value.split_once(':').ok_or_else(|| {
            ConversationAddressError::MissingProviderSeparator(value.trim().to_string())
        })?;
        match ConversationProvider::from_str(provider)? {
            ConversationProvider::IMessage => Self::imessage(destination),
            ConversationProvider::Telegram => Self::telegram(destination),
            ConversationProvider::Discord => {
                let (guild_id, channel_id) = destination.split_once('/').ok_or_else(|| {
                    ConversationAddressError::Malformed {
                        provider: ConversationProvider::Discord,
                        message:
                            "Discord address must contain guild and channel IDs separated by '/'"
                                .into(),
                    }
                })?;
                if channel_id.contains('/') {
                    return Err(ConversationAddressError::Malformed {
                        provider: ConversationProvider::Discord,
                        message: "Discord address must contain exactly one guild/channel separator"
                            .into(),
                    });
                }
                Self::discord(guild_id, channel_id)
            }
        }
    }
}

impl fmt::Display for ConversationAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.destination {
            ConversationDestination::IMessage { handle } => {
                write!(formatter, "imessage:{handle}")
            }
            ConversationDestination::Telegram { chat_id } => {
                write!(formatter, "telegram:{chat_id}")
            }
            ConversationDestination::Discord {
                guild_id,
                channel_id,
            } => write!(formatter, "discord:{guild_id}/{channel_id}"),
        }
    }
}

impl Serialize for ConversationAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ConversationAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationAddressError {
    MissingProviderSeparator(String),
    UnknownProvider(String),
    Malformed {
        provider: ConversationProvider,
        message: String,
    },
}

impl fmt::Display for ConversationAddressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingProviderSeparator(address) => write!(
                formatter,
                "conversation address `{address}` must begin with a provider followed by ':'"
            ),
            Self::UnknownProvider(provider) => write!(
                formatter,
                "unknown conversation provider `{provider}`; expected imessage, telegram, or discord"
            ),
            Self::Malformed { message, .. } => formatter.write_str(message),
        }
    }
}

impl Error for ConversationAddressError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ConversationCapabilities {
    pub append_to_conversation: bool,
    pub create_into_container: bool,
}

pub const fn conversation_capabilities(provider: ConversationProvider) -> ConversationCapabilities {
    ConversationCapabilities {
        append_to_conversation: true,
        create_into_container: matches!(provider, ConversationProvider::Discord),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_normalize_and_round_trip() {
        let cases = [
            (" IMessage: USER@Example.COM ", "imessage:user@example.com"),
            ("TELEGRAM: -00123 ", "telegram:-123"),
            ("Discord: 001 / 002 ", "discord:1/2"),
        ];
        for (input, canonical) in cases {
            let parsed: ConversationAddress = input.parse().unwrap();
            assert_eq!(parsed.to_string(), canonical);
            assert_eq!(canonical.parse::<ConversationAddress>().unwrap(), parsed);
            assert_eq!(
                serde_json::to_string(&parsed).unwrap(),
                format!("\"{canonical}\"")
            );
            assert_eq!(
                serde_json::from_str::<ConversationAddress>(&format!("\"{canonical}\"")).unwrap(),
                parsed
            );
        }
    }

    #[test]
    fn malformed_addresses_have_human_messages() {
        assert_eq!(
            "linear:cairn-1"
                .parse::<ConversationAddress>()
                .unwrap_err()
                .to_string(),
            "unknown conversation provider `linear`; expected imessage, telegram, or discord"
        );
        assert_eq!(
            "telegram:operator"
                .parse::<ConversationAddress>()
                .unwrap_err()
                .to_string(),
            "Telegram chat ID must be a signed integer"
        );
        assert_eq!(
            "discord:1"
                .parse::<ConversationAddress>()
                .unwrap_err()
                .to_string(),
            "Discord address must contain guild and channel IDs separated by '/'"
        );
    }

    #[test]
    fn only_discord_can_create_into_a_container() {
        for provider in ConversationProvider::ALL {
            assert_eq!(
                conversation_capabilities(provider),
                ConversationCapabilities {
                    append_to_conversation: true,
                    create_into_container: matches!(provider, ConversationProvider::Discord),
                }
            );
        }
    }
}
