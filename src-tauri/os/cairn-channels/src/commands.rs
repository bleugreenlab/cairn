//! Canonical channel slash-command vocabulary shared by routing and providers.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelCommandSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub takes_argument: bool,
}

pub const CHANNEL_COMMANDS: &[ChannelCommandSpec] = &[
    ChannelCommandSpec {
        name: "threads",
        description: "List and follow threads",
        takes_argument: false,
    },
    ChannelCommandSpec {
        name: "issues",
        description: "List and follow issues",
        takes_argument: false,
    },
    ChannelCommandSpec {
        name: "focus",
        description: "Focus a followed thread or issue",
        takes_argument: true,
    },
    ChannelCommandSpec {
        name: "unfollow",
        description: "Stop following a thread or issue",
        takes_argument: true,
    },
    ChannelCommandSpec {
        name: "help",
        description: "Show channel commands",
        takes_argument: false,
    },
];

pub fn command_spec(name: &str) -> Option<&'static ChannelCommandSpec> {
    CHANNEL_COMMANDS
        .iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}
