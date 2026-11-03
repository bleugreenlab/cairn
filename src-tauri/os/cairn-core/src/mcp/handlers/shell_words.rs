//! Reading a shell command line as the shell would split it.
//!
//! One splitter serves every place Cairn *reads* an agent's command without
//! running it: the fence's git-write recognizer and the classification view
//! that grants, carveouts and read-equivalent routing consult. Nothing here
//! rewrites what executes; a `run` command item always runs exactly as the
//! agent wrote it.

use std::borrow::Cow;

/// One shell word, with its quoting removed.
#[derive(Debug)]
pub(crate) struct Word {
    pub(crate) text: String,
    /// Written without quoting, escaping, or substitution, so the shell can
    /// read it as a reserved word.
    pub(crate) plain: bool,
    /// Contains a command substitution, whose expansion is unknown.
    pub(crate) substituted: bool,
    /// The shell hands `text` to the program unchanged, as this one argument.
    /// False when anything outside single quotes and escapes could rewrite or
    /// split it first: a parameter, command or arithmetic expansion, a glob,
    /// brace or tilde expansion, or a redirection operator.
    pub(crate) literal: bool,
}

/// One element of a command, in the order the shell meets it.
#[derive(Debug)]
pub(crate) enum Piece {
    /// The words of one simple command.
    Command(Vec<Word>),
    /// `(`: a subshell begins. A `cd` inside it lasts until the matching `)`.
    Open,
    Close,
}

/// A control operator between two simple commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operator {
    /// `|`
    Pipe,
    /// `|&`: stdout and stderr both feed the next command.
    PipeAll,
    /// `&&`
    And,
    /// `||`
    Or,
    /// `;` or a newline.
    Sequence,
    /// `&`
    Background,
}

/// One element of a command line, operators included, in the order the shell
/// meets it.
#[derive(Debug)]
pub(crate) enum Element {
    Piece(Piece),
    Operator(Operator),
}

/// Reserved words that can open a simple command without being its program.
/// `fi`, `done` and `}` close a construct and are read as ordinary programs
/// that do nothing.
const RESERVED_PREFIXES: &[&str] = &[
    "if", "then", "elif", "else", "while", "until", "do", "!", "{", "time",
];

pub(crate) fn is_reserved_prefix(word: &Word) -> bool {
    word.plain && RESERVED_PREFIXES.contains(&word.text.as_str())
}

/// Programs [`launcher_inner`] reads through.
const LAUNCHERS: &[&str] = &["bash", "sh", "zsh", "/bin/bash", "/bin/sh", "/bin/zsh"];

/// The command a whole-line shell launcher runs, when `command` is exactly
/// `<launcher> -c|-lc <one literal word>` and nothing else.
///
/// A chain (`bash -c 'a' && b`), a pipe, a redirection, positional arguments
/// after the script, an assignment prefix, or unbalanced quoting all mean the
/// line is not one launcher invocation, so it has no inner command and is read
/// as the line it is. The script word must be literal, because the outer shell
/// would otherwise rewrite it before the launcher saw it, and a grant or
/// exemption must never match text the launcher does not receive. One level
/// only: a launcher inside the inner command is not read through.
pub(crate) fn launcher_inner(command: &str) -> Option<String> {
    let mut pieces = simple_commands(command)?;
    let [Piece::Command(words)] = pieces.as_mut_slice() else {
        return None;
    };
    let [program, flag, script] = words.as_mut_slice() else {
        return None;
    };
    let is_launcher = program.plain && LAUNCHERS.contains(&program.text.as_str());
    let is_command_flag = flag.plain && matches!(flag.text.as_str(), "-c" | "-lc");
    (is_launcher && is_command_flag && script.literal).then(|| std::mem::take(&mut script.text))
}

/// What classifiers read: a whole-line launcher's inner command, or the
/// command itself.
pub(crate) fn command_for_classification(command: &str) -> Cow<'_, str> {
    launcher_inner(command).map_or(Cow::Borrowed(command), Cow::Owned)
}

/// The word being built, started on its first character.
fn word_at(word: &mut Option<Word>) -> &mut Word {
    word.get_or_insert_with(|| Word {
        text: String::new(),
        plain: true,
        substituted: false,
        literal: true,
    })
}

/// Characters that, outside quotes, let the shell rewrite or split a word.
fn rewrites_unquoted(c: char) -> bool {
    matches!(c, '$' | '*' | '?' | '[' | '{' | '~' | '<' | '>')
}

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

/// Split a command into its simple commands, separated by `;`, `&&`, `||`,
/// `|`, `&` and newlines, with `( ... )` subshells marked. Quotes are
/// honoured, so a `git mv` inside a `sed` expression is a word, not a
/// command, and a command substitution is kept whole as part of its word: its
/// separators and parentheses belong to the subshell it runs in, not to this
/// command. `{ ... }` groups run in the current shell, so their braces are
/// ordinary words.
///
/// Returns `None` for any construct whose effect cannot be read from the text:
/// process substitution, a heredoc, a parenthesis that does not open a
/// command (an array, a function definition, a `case` pattern), or unbalanced
/// quoting, substitution, or parentheses.
pub(crate) fn simple_commands(command: &str) -> Option<Vec<Piece>> {
    Some(
        command_line(command)?
            .into_iter()
            .filter_map(|element| match element {
                Element::Piece(piece) => Some(piece),
                Element::Operator(_) => None,
            })
            .collect(),
    )
}

/// [`simple_commands`] with the control operators between them kept, for a
/// reader that must know how the commands are joined: whether a stage is piped,
/// chained on success, or merely sequenced.
pub(crate) fn command_line(command: &str) -> Option<Vec<Element>> {
    let mut pieces = Vec::new();
    let mut words: Vec<Word> = Vec::new();
    let mut word: Option<Word> = None;
    let mut depth = 0usize;
    let mut chars = command.chars().peekable();

    let end_word = |words: &mut Vec<Word>, word: &mut Option<Word>| words.extend(word.take());
    let end_command = |pieces: &mut Vec<Element>, words: &mut Vec<Word>| {
        if !words.is_empty() {
            pieces.push(Element::Piece(Piece::Command(std::mem::take(words))));
        }
    };

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                let word = word_at(&mut word);
                word.plain = false;
                loop {
                    match chars.next()? {
                        '\'' => break,
                        ch => word.text.push(ch),
                    }
                }
            }
            '"' => {
                let word = word_at(&mut word);
                word.plain = false;
                double_quoted(&mut chars, word)?;
            }
            '\\' => match chars.next()? {
                '\n' => {}
                escaped => {
                    let word = word_at(&mut word);
                    word.plain = false;
                    word.text.push(escaped);
                }
            },
            '`' => backquoted(&mut chars, word_at(&mut word))?,
            '$' if chars.peek() == Some(&'(') => {
                chars.next();
                substitution(&mut chars, word_at(&mut word))?;
            }
            '<' | '>' if chars.peek() == Some(&'(') => return None,
            '<' if chars.peek() == Some(&'<') => return None,
            '#' if word.is_none() => {
                while chars.peek().is_some_and(|&next| next != '\n') {
                    chars.next();
                }
            }
            // `2>&1`, `>&2`, `&>file`: redirections, not separators.
            '&' if word
                .as_ref()
                .is_some_and(|word| word.text.ends_with(['>', '<']))
                || chars.peek() == Some(&'>') =>
            {
                word_at(&mut word).text.push(c);
            }
            '(' => {
                if word.is_some() || !words.iter().all(is_reserved_prefix) {
                    return None;
                }
                end_command(&mut pieces, &mut words);
                pieces.push(Element::Piece(Piece::Open));
                depth += 1;
            }
            ')' => {
                end_word(&mut words, &mut word);
                end_command(&mut pieces, &mut words);
                depth = depth.checked_sub(1)?;
                pieces.push(Element::Piece(Piece::Close));
            }
            ';' | '&' | '|' | '\n' => {
                end_word(&mut words, &mut word);
                end_command(&mut pieces, &mut words);
                let operator = match (c, chars.peek()) {
                    ('&', Some('&')) => {
                        chars.next();
                        Operator::And
                    }
                    ('&', _) => Operator::Background,
                    ('|', Some('|')) => {
                        chars.next();
                        Operator::Or
                    }
                    ('|', Some('&')) => {
                        chars.next();
                        Operator::PipeAll
                    }
                    ('|', _) => Operator::Pipe,
                    _ => Operator::Sequence,
                };
                pieces.push(Element::Operator(operator));
            }
            c if c.is_whitespace() => end_word(&mut words, &mut word),
            c => {
                let word = word_at(&mut word);
                if rewrites_unquoted(c) {
                    word.literal = false;
                }
                word.text.push(c);
            }
        }
    }
    end_word(&mut words, &mut word);
    end_command(&mut pieces, &mut words);
    (depth == 0).then_some(pieces)
}

/// Read the rest of a double-quoted span whose opening `"` was just read.
fn double_quoted(chars: &mut Chars<'_>, word: &mut Word) -> Option<()> {
    loop {
        match chars.next()? {
            '"' => return Some(()),
            '`' => backquoted(chars, word)?,
            '$' if chars.peek() == Some(&'(') => {
                chars.next();
                substitution(chars, word)?;
            }
            '\\' => match chars.next()? {
                '\n' => {}
                escaped @ ('"' | '\\' | '$' | '`') => word.text.push(escaped),
                other => {
                    word.text.push('\\');
                    word.text.push(other);
                }
            },
            ch => {
                if ch == '$' {
                    word.literal = false;
                }
                word.text.push(ch);
            }
        }
    }
}

/// Read the rest of a `$( ... )` or `$(( ... ))` whose opening `$(` was just
/// read, keeping its text in the word. Nested quotes and substitutions are
/// honoured so that a `)` inside them does not end it.
fn substitution(chars: &mut Chars<'_>, word: &mut Word) -> Option<()> {
    word.plain = false;
    word.substituted = true;
    word.literal = false;
    word.text.push_str("$(");
    let mut depth = 1usize;
    while depth > 0 {
        let c = chars.next()?;
        match c {
            '\'' => {
                word.text.push(c);
                loop {
                    let ch = chars.next()?;
                    word.text.push(ch);
                    if ch == '\'' {
                        break;
                    }
                }
                continue;
            }
            '"' => {
                word.text.push(c);
                double_quoted(chars, word)?;
                word.text.push(c);
                continue;
            }
            '`' => {
                backquoted(chars, word)?;
                continue;
            }
            '\\' => {
                word.text.push(c);
                word.text.push(chars.next()?);
                continue;
            }
            '<' if matches!(chars.peek(), Some('<' | '(')) => return None,
            '>' if chars.peek() == Some(&'(') => return None,
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        word.text.push(c);
    }
    Some(())
}

/// Read the rest of a backquoted substitution whose opening backquote was
/// just read, keeping its text in the word.
fn backquoted(chars: &mut Chars<'_>, word: &mut Word) -> Option<()> {
    word.plain = false;
    word.substituted = true;
    word.literal = false;
    word.text.push('`');
    loop {
        match chars.next()? {
            '`' => break,
            '\\' => {
                word.text.push('\\');
                word.text.push(chars.next()?);
            }
            '<' if matches!(chars.peek(), Some('<' | '(')) => return None,
            '>' if chars.peek() == Some(&'(') => return None,
            ch => word.text.push(ch),
        }
    }
    word.text.push('`');
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::handlers::normalize_command;

    #[test]
    fn a_whole_line_launcher_has_its_literal_script_as_inner_command() {
        assert_eq!(
            launcher_inner("bash -lc 'git status --short'").as_deref(),
            Some("git status --short")
        );
        assert_eq!(
            launcher_inner(r#"/bin/zsh -lc "sed -n '120,520p' src/app.tsx""#).as_deref(),
            Some("sed -n '120,520p' src/app.tsx")
        );
        assert_eq!(
            launcher_inner(r#"  sh -c "echo \"quoted\" \$HOME"  "#).as_deref(),
            Some(r#"echo "quoted" $HOME"#)
        );
        assert_eq!(launcher_inner("bash -c ls").as_deref(), Some("ls"));
    }

    #[test]
    fn anything_but_one_launcher_invocation_has_no_inner_command() {
        for command in [
            // A chain or pipe is not one launcher invocation.
            "bash -c 'echo a' && echo 'b   c'",
            "sh -c \"echo x\" | tr x y",
            // Positional arguments after the script.
            "bash -c 'echo \"$1\"' _ a",
            // The outer shell rewrites the script before the launcher sees it.
            "bash -c \"echo $HOME\"",
            "bash -c \"echo $(date)\"",
            "bash -c \"echo `date`\"",
            "bash -c ls*",
            "bash -c ~/x.sh",
            // A redirection, spaced or glued to the script.
            "bash -c 'x' > out",
            "bash -c 'x'>out",
            // An assignment prefix, unbalanced quoting, or no launcher at all.
            "FOO=1 bash -c 'x'",
            "bash -c 'unterminated",
            "git status",
            "bashx -c 'x'",
            "'bash' -c 'x'",
            "bash -e 'x'",
            "bash -c",
        ] {
            assert_eq!(launcher_inner(command), None, "{command}");
            assert_eq!(command_for_classification(command), command, "{command}");
        }
    }

    #[test]
    fn a_command_line_keeps_the_operators_between_its_commands() {
        let shape = |command: &str| -> Vec<String> {
            command_line(command)
                .expect("parses")
                .into_iter()
                .map(|element| match element {
                    Element::Piece(Piece::Command(words)) => words
                        .iter()
                        .map(|word| word.text.as_str())
                        .collect::<Vec<_>>()
                        .join(" "),
                    Element::Piece(Piece::Open) => "(".to_string(),
                    Element::Piece(Piece::Close) => ")".to_string(),
                    Element::Operator(operator) => format!("{operator:?}"),
                })
                .collect()
        };
        assert_eq!(
            shape("cd src && rg -n 'a|b' | head -5"),
            ["cd src", "And", "rg -n a|b", "Pipe", "head -5"]
        );
        assert_eq!(
            shape("a || b; c & d |& e"),
            [
                "a",
                "Or",
                "b",
                "Sequence",
                "c",
                "Background",
                "d",
                "PipeAll",
                "e"
            ]
        );
        // A redirection's ampersand is part of its word, never an operator.
        assert_eq!(shape("rg x 2>&1 | wc -l"), ["rg x 2>&1", "Pipe", "wc -l"]);
        // Operators drop out of the plain view, which is unchanged.
        assert_eq!(simple_commands("a && b | c").expect("parses").len(), 3);
    }

    #[test]
    fn normalize_command_reads_through_only_a_whole_line_launcher() {
        assert_eq!(
            normalize_command("bash -lc 'git status --short'"),
            normalize_command("git status --short")
        );
        let chained = "bash -c 'echo a' && echo 'b   c'";
        assert_eq!(
            normalize_command(chained),
            chained.split_whitespace().collect::<Vec<_>>().join(" ")
        );
    }
}
