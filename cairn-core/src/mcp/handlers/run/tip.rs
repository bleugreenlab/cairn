//! Advisory tip: nudge shell-wrapped interpreter one-liners toward inline
//! `{code, interpreter}`. Detection is a cheap token scan, never a shell parser —
//! it is fine to miss exotic forms, and it fails open (a miss just omits the
//! tip). The tip is purely advisory: it never alters an item's success/failure or
//! the batch's exit semantics; it rides on the composed output like the cd /
//! write-check advisories.

use super::types::RunItem;

/// One-line nudge for a wrapped python one-liner. Kept to a single line so it
/// stays a low-noise footnote on the item's output.
const PYTHON_TIP: &str = "tip: inline code runs natively — pass {code, interpreter:\"python\"} instead of python3 -c; no shell quoting, and PEP 723 `# /// script` deps resolve through uv.";

/// One-line nudge for a wrapped bun/node one-liner.
const TS_TIP: &str = "tip: inline code runs natively — pass {code, interpreter:\"typescript\"} instead of bun -e; no shell quoting, and the worktree node_modules + @cairn/sdk import zero-config.";

/// The one-line tip for the first shell `command` item in the batch that looks
/// like a wrapped interpreter one-liner, or `None`. `code` and `target` items
/// never match — they are already the recommended form — so the tip only ever
/// fires against a raw shell command. At most one tip per batch (first match
/// wins).
pub(super) fn interpreter_tip(commands: &[RunItem]) -> Option<&'static str> {
    for item in commands {
        // Only shell `command` items. Inline code and skill/MCP targets are
        // already the ideal shape, so they are skipped outright.
        if item.code.is_some() || item.target.is_some() {
            continue;
        }
        let Some(command) = item.command.as_deref() else {
            continue;
        };
        if let Some(tip) = tip_for_command(command) {
            return Some(tip);
        }
    }
    None
}

/// Match a single command string against the wrap-an-interpreter shapes:
/// `python[3] -c` / `bun -e` / `node -e`, or a `python[3] <<EOF` heredoc. Tokens
/// are split on whitespace and compared by basename exactly, so an interpreter
/// name that only appears inside a quoted argument — `echo "python3 -c ..."`
/// splits to `"python3` / `-c ...`, neither an exact `python3` — does not
/// false-positive. Running an actual script file (`python3 script.py`) has no
/// `-c`/`-e`/heredoc token, so it is correctly left alone.
fn tip_for_command(command: &str) -> Option<&'static str> {
    let toks: Vec<&str> = command.split_whitespace().collect();
    for pair in toks.windows(2) {
        // Basename: `/usr/bin/python3` and `env python3` both resolve to the bare
        // interpreter name (the `env` case matches on the `python3` token).
        let prog = pair[0].rsplit('/').next().unwrap_or(pair[0]);
        let next = pair[1];
        match prog {
            "python" | "python3" => {
                // `-c CODE` eval, or a `<<EOF` heredoc feeding the interpreter.
                if next == "-c" || next.starts_with("<<") {
                    return Some(PYTHON_TIP);
                }
            }
            "bun" | "node" => {
                if next == "-e" {
                    return Some(TS_TIP);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(command: &str) -> RunItem {
        RunItem {
            command: Some(command.to_string()),
            description: None,
            timeout: None,
            target: None,
            payload: None,
            code: None,
            background: None,
            interpreter: None,
            repl: None,
            wait_for: None,
        }
    }

    fn code_item(code: &str, interpreter: &str) -> RunItem {
        RunItem {
            command: None,
            description: None,
            timeout: None,
            target: None,
            payload: None,
            code: Some(code.to_string()),
            background: None,
            interpreter: Some(interpreter.to_string()),
            repl: None,
            wait_for: None,
        }
    }

    fn target_item(target: &str) -> RunItem {
        RunItem {
            command: None,
            description: None,
            timeout: None,
            target: Some(target.to_string()),
            payload: None,
            code: None,
            background: None,
            interpreter: None,
            repl: None,
            wait_for: None,
        }
    }

    #[test]
    fn python_dash_c_gets_python_tip() {
        assert_eq!(tip_for_command("python3 -c 'print(1)'"), Some(PYTHON_TIP));
        assert_eq!(tip_for_command("python -c 'print(1)'"), Some(PYTHON_TIP));
    }

    #[test]
    fn bun_and_node_dash_e_get_ts_tip() {
        assert_eq!(tip_for_command("bun -e 'console.log(1)'"), Some(TS_TIP));
        assert_eq!(tip_for_command("node -e 'console.log(1)'"), Some(TS_TIP));
    }

    #[test]
    fn python_heredoc_gets_python_tip() {
        assert_eq!(
            tip_for_command("python3 <<'EOF'\nprint(1)\nEOF"),
            Some(PYTHON_TIP)
        );
        assert_eq!(
            tip_for_command("python << EOF\nprint(1)\nEOF"),
            Some(PYTHON_TIP)
        );
    }

    #[test]
    fn absolute_path_and_env_prefix_still_match() {
        assert_eq!(
            tip_for_command("/usr/bin/python3 -c 'print(1)'"),
            Some(PYTHON_TIP)
        );
        assert_eq!(
            tip_for_command("env python3 -c 'print(1)'"),
            Some(PYTHON_TIP)
        );
    }

    #[test]
    fn quoted_mention_does_not_false_positive() {
        // The interpreter name only appears inside a quoted argument, so the
        // tokens never line up as an exact `python3` followed by `-c`.
        assert_eq!(tip_for_command("echo \"python3 -c foo\""), None);
        assert_eq!(tip_for_command("grep -n 'bun -e' src"), None);
    }

    #[test]
    fn real_script_and_ordinary_commands_get_no_tip() {
        // Running an actual script file is not a wrap.
        assert_eq!(tip_for_command("python3 script.py"), None);
        assert_eq!(tip_for_command("cargo test -p cairn-core"), None);
        assert_eq!(tip_for_command("ls -la"), None);
    }

    #[test]
    fn batch_returns_first_matching_shell_command() {
        let commands = vec![cmd("ls"), cmd("python3 -c 'print(1)'")];
        assert_eq!(interpreter_tip(&commands), Some(PYTHON_TIP));
    }

    #[test]
    fn code_and_target_items_never_trigger_tip() {
        // A code item whose text literally contains `python3 -c`, and a target
        // item — neither is a shell command, so neither fires.
        let commands = vec![
            code_item("# python3 -c not a shell command", "python"),
            target_item("cairn://skills/x/scripts/y"),
        ];
        assert_eq!(interpreter_tip(&commands), None);
    }

    #[test]
    fn nonmatching_batch_returns_none() {
        assert_eq!(interpreter_tip(&[cmd("git status")]), None);
    }
}
