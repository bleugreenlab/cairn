//! Trust resolution for user-accepted project terminal commands.
//!
//! Project config declares the exact shortcut command. User-owned settings record
//! whether that command may run outside the worktree fence. Neither source is
//! sufficient alone: an accepted string must still exactly match a command in the
//! current project's config after whitespace normalization.

use crate::models::TerminalCommand;

/// Trust resolved for one command from project declarations and user acceptance.
#[derive(Debug, Default, PartialEq)]
pub struct ResolvedCarveouts {
    pub(crate) unconfined: bool,
}

impl ResolvedCarveouts {
    pub fn is_empty(&self) -> bool {
        !self.unconfined
    }
}

/// Resolve whether `command` is an accepted exact project terminal command.
///
/// Matching is layout-insensitive but otherwise exact. Substrings, added
/// arguments, and commands absent from this project's declarations do not inherit
/// trust from an accepted command.
pub(crate) fn resolve_carveouts(
    command: &str,
    project_terminals: &[TerminalCommand],
    accepted: &[String],
) -> ResolvedCarveouts {
    let command = normalize(command);
    let declared = project_terminals
        .iter()
        .map(|terminal| normalize(&terminal.command))
        .any(|candidate| !candidate.is_empty() && candidate == command);
    let accepted = accepted
        .iter()
        .map(|candidate| normalize(candidate))
        .any(|candidate| candidate == command);

    ResolvedCarveouts {
        unconfined: declared && accepted,
    }
}

/// Collapse runs of whitespace so command matching is layout-insensitive.
fn normalize(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terminal(command: &str) -> TerminalCommand {
        TerminalCommand {
            name: "dev".to_string(),
            command: command.to_string(),
        }
    }

    #[test]
    fn unaccepted_declared_command_remains_fenced() {
        let result = resolve_carveouts("bun dev", &[terminal("bun dev")], &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn accepted_exact_command_is_unconfined() {
        let command = "bun dev:instance --seed empty";
        let result = resolve_carveouts(command, &[terminal(command)], &[command.to_string()]);
        assert!(result.unconfined);
    }

    #[test]
    fn whitespace_normalized_matching_is_unconfined() {
        let result = resolve_carveouts(
            " bun   dev:instance   --seed empty ",
            &[terminal("bun dev:instance --seed empty")],
            &["bun	dev:instance --seed   empty".to_string()],
        );
        assert!(result.unconfined);
    }

    #[test]
    fn similar_or_different_commands_are_not_authorized() {
        let declared = [terminal("bun dev")];
        let accepted = ["bun dev".to_string()];
        for command in ["bun dev --seed empty", "bun", "cargo test"] {
            assert!(resolve_carveouts(command, &declared, &accepted).is_empty());
        }
    }

    #[test]
    fn command_from_another_project_is_not_authorized() {
        let result = resolve_carveouts(
            "bun dev",
            &[terminal("cargo test")],
            &["bun dev".to_string()],
        );
        assert!(result.is_empty());
    }
}
