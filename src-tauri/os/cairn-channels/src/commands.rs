//! Canonical channel slash-command vocabulary shared by routing and providers.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelCommandSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub takes_argument: bool,
}

/// Every command a channel answers. Providers that publish a command menu
/// (Telegram's `setMyCommands`, on every provider start) publish exactly this.
pub const CHANNEL_COMMANDS: &[ChannelCommandSpec] = &[ChannelCommandSpec {
    name: "help",
    description: "Show where your messages go",
    takes_argument: false,
}];

pub fn command_spec(name: &str) -> Option<&'static ChannelCommandSpec> {
    CHANNEL_COMMANDS
        .iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}
