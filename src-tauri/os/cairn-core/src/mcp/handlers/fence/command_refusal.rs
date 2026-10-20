//! Instructive refusals for command-scoped sandbox crossings.
//!
//! A command the kernel blocked without naming a path becomes an escape prompt
//! that only an operator can answer, by re-running the command with no sandbox
//! at all. For one family of commands that prompt is the wrong question: git's
//! staging records and refs live outside a build slot's working directory, so a
//! `git mv` or `git checkout -- <path>` is always blocked there, and it is never
//! needed, because a batch's commit is derived from the working tree and the
//! branch belongs to Cairn. Every such command has an ordinary in-sandbox
//! equivalent, so the fence answers with that equivalent instead of spending an
//! operator's attention.
//!
//! The recognizer is deliberately narrow and fails safe. It reads the agent's
//! raw command, splits it into the simple commands the shell would run, notes
//! which of them run inside a `( ... )` subshell, and looks for an explicit git
//! subcommand. What matters is that a recognized git step runs against the
//! working directory's repository, so two shapes are read rather than given up
//! on: a command substitution (`$(...)`, backticks, `$((...))`) is an opaque
//! word, because it runs in a subshell and cannot change the directory or
//! environment of anything after it, and a `cd` inside parentheses only
//! disqualifies the steps before the matching `)`. A git step inside a
//! substitution is not a step of the command and is never recognized.
//!
//! Anything it cannot read with confidence -- process substitution, a heredoc,
//! a substitution or `case` where a program name belongs, a `git` global
//! option, a redirected `GIT_*` variable, a `cd`, `eval` or `source` that may
//! leave the working directory, a launcher such as `sh -c` -- yields no
//! refusal, and the crossing falls through to the ordinary fence policy
//! exactly as before. A missed recognition costs what the prompt always cost;
//! a wrong one would refuse work that could have been approved.
//!
//! A git writer later in a chain (`... && git mv a b`) is still recognized. The
//! step is blocked wherever it appears, so naming it is true regardless of what
//! else the command does.

/// The refusal for a blocked command whose blocked step has an ordinary route
/// inside the sandbox, or `None` to leave the crossing to the fence policy.
pub(crate) fn denied_command_refusal(command: &str) -> Option<String> {
    // For each enclosing subshell, innermost last: whether a step inside it
    // may have left the working directory.
    let mut subshells: Vec<bool> = Vec::new();
    for piece in simple_commands(command)? {
        match piece {
            Piece::Open => subshells.push(false),
            Piece::Close => {
                subshells.pop();
            }
            Piece::Command(words) => match classify(&words) {
                Step::GitWrite(write) if !subshells.contains(&true) => {
                    return Some(write.refusal());
                }
                Step::MayLeaveWorktree => match subshells.last_mut() {
                    Some(left) => *left = true,
                    None => return None,
                },
                Step::Unreadable => return None,
                Step::GitWrite(_) | Step::Other => {}
            },
        }
    }
    None
}

/// What a git subcommand writes outside the working directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Writes {
    /// The staging area (and, for `commit`, history built from it).
    Index,
    /// Branches, or history fetched into the repository.
    Refs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GitWrite {
    subcommand: &'static str,
    writes: Writes,
    /// The in-sandbox way to get what the step was for.
    route: &'static str,
}

const RESTORE_ROUTE: &str = "To put a file back the way a revision has it, write it from that revision: `git show <rev>:<path> > <path>`. To look at another branch without changing anything, `read` `file:<path>?branch=<name>`.";

const BRANCH_ROUTE: &str = "To look at another branch without changing anything, `read` `file:<path>?branch=<name>`. If an advance of your base left conflicts, read both versions and write the merged result. The git-workflow skill (cairn://skills/git-workflow) covers both.";

/// Every recognized writer. Explicit on purpose: a subcommand earns a place
/// here only once it is known to be blocked in a build slot and to have a
/// route that works there.
const GIT_WRITES: &[GitWrite] = &[
    GitWrite {
        subcommand: "add",
        writes: Writes::Index,
        route: "There is nothing to add: every change in your working tree is committed for you.",
    },
    GitWrite {
        subcommand: "mv",
        writes: Writes::Index,
        route: "Move it with plain `mv` instead; git recognizes the rename from the file's content when the change is committed.",
    },
    GitWrite {
        subcommand: "rm",
        writes: Writes::Index,
        route: "Delete it with plain `rm` instead (`rm -r` for a directory); the deletion is committed like any other change.",
    },
    GitWrite {
        subcommand: "commit",
        writes: Writes::Index,
        route: "The batch's `commit_msg` is the commit, so put the message there instead.",
    },
    GitWrite {
        subcommand: "restore",
        writes: Writes::Index,
        route: RESTORE_ROUTE,
    },
    GitWrite {
        subcommand: "checkout",
        writes: Writes::Index,
        route: RESTORE_ROUTE,
    },
    GitWrite {
        subcommand: "reset",
        writes: Writes::Index,
        route: "Nothing staged needs undoing. To put a file back the way a revision has it: `git show <rev>:<path> > <path>`.",
    },
    GitWrite {
        subcommand: "stash",
        writes: Writes::Index,
        route: "To set changes aside, save them as a patch with `git diff > \"$TMPDIR/changes.patch\"` and bring them back with `git apply \"$TMPDIR/changes.patch\"`.",
    },
    GitWrite {
        subcommand: "cherry-pick",
        writes: Writes::Index,
        route: "To bring in a commit's change, apply it to the working tree: `git show <commit> | git apply`.",
    },
    GitWrite {
        subcommand: "revert",
        writes: Writes::Index,
        route: "To undo a commit's change, reverse it in the working tree: `git show <commit> | git apply -R`.",
    },
    GitWrite {
        subcommand: "apply",
        writes: Writes::Index,
        route: "Leave out the index option: plain `git apply` changes the working tree, which is everything the commit needs.",
    },
    GitWrite {
        subcommand: "fetch",
        writes: Writes::Refs,
        route: BRANCH_ROUTE,
    },
    GitWrite {
        subcommand: "pull",
        writes: Writes::Refs,
        route: BRANCH_ROUTE,
    },
    GitWrite {
        subcommand: "merge",
        writes: Writes::Refs,
        route: BRANCH_ROUTE,
    },
    GitWrite {
        subcommand: "rebase",
        writes: Writes::Refs,
        route: BRANCH_ROUTE,
    },
    GitWrite {
        subcommand: "switch",
        writes: Writes::Refs,
        route: BRANCH_ROUTE,
    },
];

impl GitWrite {
    /// The table entry for `git <subcommand> <args>`, when that invocation
    /// writes. Some subcommands have read-only or route-less forms, which are
    /// left to the fence.
    fn recognize(subcommand: &str, args: &[Word]) -> Option<GitWrite> {
        let entry = GIT_WRITES
            .iter()
            .find(|entry| entry.subcommand == subcommand)?;
        let has = |flag: &str| args.iter().any(|arg| arg.text == flag);
        let writes = match subcommand {
            // Plain `git apply` writes only the working tree and is never blocked.
            "apply" => has("--index") || has("--cached") || has("-3") || has("--3way"),
            // Untracking a file while keeping it has no working-tree equivalent.
            "rm" => !has("--cached"),
            // `git checkout` alone only reports.
            "checkout" => !args.is_empty(),
            "stash" => !matches!(
                args.first().map(|arg| arg.text.as_str()),
                Some("list" | "show")
            ),
            _ => true,
        };
        writes.then_some(*entry)
    }

    fn refusal(&self) -> String {
        let sub = self.subcommand;
        let why = match self.writes {
            Writes::Index => format!(
                "This command runs `git {sub}`, which writes git's own staging records. Those live outside your working directory, so the sandbox blocked that step, and approving it would not help: when a batch carries a `commit_msg`, Cairn commits your working tree exactly as it stands and ignores anything staged or committed by hand."
            ),
            Writes::Refs => format!(
                "This command runs `git {sub}`, which moves branches or fetches history into the repository outside your working directory, so the sandbox blocked that step. Cairn owns your branch: it brings an advanced base into your work itself and tells you when it does, so there is nothing to fetch, merge, or rebase by hand."
            ),
        };
        format!(
            "{why} {} Then run the command again without the `git {sub}` step; anything before it in the command may already have run.",
            self.route
        )
    }
}

/// What one top-level simple command means for recognition.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    GitWrite(GitWrite),
    /// Later steps may run against a different repository, so nothing after
    /// this point can be attributed to the working directory's git.
    MayLeaveWorktree,
    /// The program itself cannot be read from the text (a substitution or a
    /// `case` where the program name belongs), so neither can anything after.
    Unreadable,
    Other,
}

/// Reserved words that can open a simple command without being its program.
/// `fi`, `done` and `}` close a construct and are read as ordinary programs
/// that do nothing.
const RESERVED_PREFIXES: &[&str] = &[
    "if", "then", "elif", "else", "while", "until", "do", "!", "{", "time",
];

fn is_reserved_prefix(word: &Word) -> bool {
    word.plain && RESERVED_PREFIXES.contains(&word.text.as_str())
}

/// The words after any leading options (`time -p`, `command -p`).
fn skip_options(words: &[Word]) -> &[Word] {
    let options = words
        .iter()
        .take_while(|word| word.text.starts_with('-'))
        .count();
    &words[options..]
}

fn classify(words: &[Word]) -> Step {
    let mut rest = words;
    loop {
        while let Some((first, tail)) = rest.split_first() {
            if !is_reserved_prefix(first) {
                break;
            }
            rest = if first.text == "time" {
                skip_options(tail)
            } else {
                tail
            };
        }
        while let Some(name) = rest.first().and_then(|word| assignment_name(&word.text)) {
            if name.starts_with("GIT_") {
                return Step::MayLeaveWorktree;
            }
            rest = &rest[1..];
        }
        let Some((program, args)) = rest.split_first() else {
            return Step::Other;
        };
        if program.substituted {
            return Step::Unreadable;
        }
        return match program.text.as_str() {
            // Both run the command that follows in the current shell.
            "builtin" | "command" => {
                rest = skip_options(args);
                continue;
            }
            // A `case` pattern's `)` is not a subshell, so the splitter's
            // scopes are wrong from here on.
            "case" if program.plain => Step::Unreadable,
            "cd" | "pushd" => match args {
                [dir] if !dir.substituted && stays_inside(&dir.text) => Step::Other,
                _ => Step::MayLeaveWorktree,
            },
            "popd" | "eval" | "source" | "." => Step::MayLeaveWorktree,
            "export" if args.iter().any(|arg| arg.text.starts_with("GIT_")) => {
                Step::MayLeaveWorktree
            }
            // A global option (`-C <dir>`, `--git-dir`, `-c`) can point git
            // elsewhere, so only `git <subcommand>` itself is read.
            "git" => match args.split_first() {
                Some((subcommand, args)) => {
                    GitWrite::recognize(&subcommand.text, args).map_or(Step::Other, Step::GitWrite)
                }
                None => Step::Other,
            },
            _ => Step::Other,
        };
    }
}

/// The variable a leading `NAME=value` word assigns, if it is one.
fn assignment_name(word: &str) -> Option<&str> {
    let (name, _) = word.split_once('=')?;
    let mut chars = name.chars();
    let first = chars.next()?;
    ((first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_'))
    .then_some(name)
}

/// Whether a `cd` target is a plain relative path that cannot climb out of
/// the directory the command started in.
fn stays_inside(dir: &str) -> bool {
    !dir.is_empty()
        && !dir.starts_with(['/', '~', '-'])
        && !dir.contains(['$', '{'])
        && !dir.split('/').any(|part| part == "..")
}

/// One shell word, with its quoting removed.
#[derive(Debug)]
struct Word {
    text: String,
    /// Written without quoting, escaping, or substitution, so the shell can
    /// read it as a reserved word.
    plain: bool,
    /// Contains a command substitution, whose expansion is unknown.
    substituted: bool,
}

/// One element of a command, in the order the shell meets it.
#[derive(Debug)]
enum Piece {
    /// The words of one simple command.
    Command(Vec<Word>),
    /// `(`: a subshell begins. A `cd` inside it lasts until the matching `)`.
    Open,
    Close,
}

/// The word being built, started on its first character.
fn word_at(word: &mut Option<Word>) -> &mut Word {
    word.get_or_insert_with(|| Word {
        text: String::new(),
        plain: true,
        substituted: false,
    })
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
fn simple_commands(command: &str) -> Option<Vec<Piece>> {
    let mut pieces = Vec::new();
    let mut words: Vec<Word> = Vec::new();
    let mut word: Option<Word> = None;
    let mut depth = 0usize;
    let mut chars = command.chars().peekable();

    let end_word = |words: &mut Vec<Word>, word: &mut Option<Word>| words.extend(word.take());
    let end_command = |pieces: &mut Vec<Piece>, words: &mut Vec<Word>| {
        if !words.is_empty() {
            pieces.push(Piece::Command(std::mem::take(words)));
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
                pieces.push(Piece::Open);
                depth += 1;
            }
            ')' => {
                end_word(&mut words, &mut word);
                end_command(&mut pieces, &mut words);
                depth = depth.checked_sub(1)?;
                pieces.push(Piece::Close);
            }
            ';' | '&' | '|' | '\n' => {
                end_word(&mut words, &mut word);
                end_command(&mut pieces, &mut words);
            }
            c if c.is_whitespace() => end_word(&mut words, &mut word),
            c => word_at(&mut word).text.push(c),
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
            ch => word.text.push(ch),
        }
    }
}

/// Read the rest of a `$( ... )` or `$(( ... ))` whose opening `$(` was just
/// read, keeping its text in the word. Nested quotes and substitutions are
/// honoured so that a `)` inside them does not end it.
fn substitution(chars: &mut Chars<'_>, word: &mut Word) -> Option<()> {
    word.plain = false;
    word.substituted = true;
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

    fn refused_for(command: &str) -> Option<&'static str> {
        denied_command_refusal(command).map(|refusal| {
            GIT_WRITES
                .iter()
                .find(|entry| refusal.contains(&format!("runs `git {}`", entry.subcommand)))
                .expect("a refusal names its subcommand")
                .subcommand
        })
    }

    /// Commands agents actually ran that were blocked and prompted an
    /// operator, taken from stored `shell_command_escape` permission rows.
    #[test]
    fn recorded_specimens_are_recognized() {
        for (command, subcommand) in [
            ("git mv src-tauri/cairn-executor src-tauri/cairn-exec", "mv"),
            (
                "cd src-tauri/turso_migrations && git mv 0242_fact_log.sql 0244_fact_log.sql && git mv 0243_drop_route_fact_samples.sql 0245_drop_route_fact_samples.sql && ls | tail -4",
                "mv",
            ),
            ("git rm -rq preview && git status --short", "rm"),
            (
                "git cherry-pick --no-commit 0bef5e4fbf 2421bf9c38 && git status --short | wc -l",
                "cherry-pick",
            ),
            ("git apply --index $TMPDIR/pr3967.diff && git status --short", "apply"),
            (
                "git diff --stat HEAD~1 HEAD | tail -3; git checkout HEAD~1 -- tests/fixtures/referee/prevention && git status --short | wc -l",
                "checkout",
            ),
            (
                "set -e\nFILES=$X\ngit checkout 950d5622d94e -- $FILES\ngit status --short",
                "checkout",
            ),
            (
                "set -e; mkdir -p src-tauri/packs/core/routes; git mv src-tauri/routes/a.yaml src-tauri/packs/core/routes/; git status --short",
                "mv",
            ),
            (
                "git fetch -q origin main 2>&1 | tail -2; git log --oneline -1 origin/main",
                "fetch",
            ),
            (
                "set -e; P=$TMPDIR/resolve3.patch; git apply --binary $P; git add -A; git write-tree; git reset -q",
                "add",
            ),
            // The executor relays a native-shell command with a leading space.
            (" git rm -q crates/tm-oracle/tests/face_down.rs && git status --short | head", "rm"),
            ("git commit -qm wip", "commit"),
            ("git stash", "stash"),
            (
                "git diff --binary 0fe4e354 12deb8e7 | git apply --index && git diff --cached --stat | tail -1 && test -z \"$(git diff HEAD 12deb8e7 --stat)\" && echo tree-equals-main",
                "apply",
            ),
            (
                "set -e; D=$(ls -d $TMPDIR/plexmono-dl.* 2>/dev/null | head -1); if [ -z \"$D\" ] || [ ! -d \"$D/x/ibm-plex-mono\" ]; then D=$(mktemp -d \"$TMPDIR/plexmono-dl.XXXXXX\"); (cd $D && curl -sfL -o ibm-plex-mono.zip 'https://github.com/IBM/plex/releases/download/%40ibm%2Fplex-mono%402.5.0/ibm-plex-mono.zip' && mkdir x && cd x && unzip -q ../ibm-plex-mono.zip); fi; echo \"$(sha256sum $D/ibm-plex-mono.zip)\"; [ \"$(sha256sum $D/ibm-plex-mono.zip | cut -d' ' -f1)\" = 6d23f01257663d8cc49a0d64c22ced630b79e0e2a0ac08a0da86e9a38bbc481c ]; B=$D/x/ibm-plex-mono; for s in Regular Bold Italic BoldItalic; do cp $B/fonts/complete/woff2/IBMPlexMono-$s.woff2 packages/tokens/fonts/; done; cp $B/LICENSE.txt packages/tokens/fonts/IBMPlexMono-OFL.txt; git rm -q src/assets/fonts/BerkeleyMono-*.woff2; ls src/assets/fonts 2>/dev/null || echo 'src/assets/fonts gone'; cd packages/tokens/fonts; for f in IBMPlexMono-*; do printf '%s %s magic=%s ' $f $(stat -c%s $f) \"$(head -c4 $f)\"; sha256sum $f | cut -d' ' -f1; done",
                "rm",
            ),
        ] {
            assert_eq!(refused_for(command), Some(subcommand), "{command}");
        }
    }

    #[test]
    fn read_only_git_and_route_less_forms_fall_through() {
        for command in [
            "git status --short",
            "git log --oneline -5 && git diff --stat",
            "git show HEAD:src/lib.rs > src/lib.rs",
            "git show abc123 | git apply",
            "git apply -R $TMPDIR/p.diff",
            "git rm --cached secrets.env",
            "git stash list",
            "git checkout",
            "git merge-base --is-ancestor origin/main HEAD",
            "git",
        ] {
            assert_eq!(refused_for(command), None, "{command}");
        }
    }

    /// The fail-safe half: shapes the recognizer cannot read with confidence
    /// keep today's behavior.
    #[test]
    fn unreadable_shapes_fall_through() {
        for command in [
            // A launcher: the git step is a quoted argument, not a command.
            "sh -c 'git mv a b'",
            "bash -c \"git add .\"",
            // An alias or wrapper is a different program.
            "g mv a b",
            "xargs git add",
            // Substitution and heredocs execute text the splitter cannot see.
            "echo $(git add .)",
            "echo `git add .`",
            "python3 - <<'EOF'\ngit add .\nEOF",
            "diff <(git show HEAD:a) a",
            // Global options and GIT_* variables can point git elsewhere.
            "git -C $TMPDIR/scratch add .",
            "git --git-dir=/tmp/x add .",
            "GIT_INDEX_FILE=$TMPDIR/index git add .",
            "export GIT_DIR=$TMPDIR/repo.git; git add .",
            // A cd that may leave the working directory.
            "cd $TMPDIR/scratch && git init -q && git add . && git commit -qm x",
            "cd /tmp/repo && git mv a b",
            "cd ../.. && git add .",
            "cd ~ && git add .",
            "cd && git add .",
            // Quoted text that merely mentions git.
            "sed -i 's/git mv a b/x/' notes.md",
            "echo 'git add .; git commit'",
            // Unbalanced quoting.
            "git add 'unterminated",
            // A comment.
            "ls # git add .",
            // A substitution where the program name belongs could be anything.
            "$(echo git) add .",
            "`which git` add .",
            // A cd into a substitution's output may go anywhere.
            "cd $(mktemp -d) && git add .",
            "cd `mktemp -d` && git add .",
            "git add $(foo",
            // A cd that may leave, inside a subshell, covers its own steps.
            "(cd /tmp/repo && git add .)",
            "(cd /tmp/repo; (git add .))",
            // Braces group in the current shell, so their cd persists.
            "{ cd /tmp/repo; }; git add .",
            // Reserved words and prefixes do not hide a cd.
            "if true; then cd /tmp/repo; fi; git add .",
            "for d in a; do cd /tmp/$d; done; git add .",
            "! cd /tmp/repo; git add .",
            "time -p cd /tmp/repo; git add .",
            "builtin cd /tmp/repo; git add .",
            "command cd /tmp/repo; git add .",
            // Running other text in this shell may change anything.
            "source env.sh; git add .",
            ". env.sh; git add .",
            "eval \"$X\"; git add .",
            // A case pattern's parenthesis is not a subshell.
            "case $x in a) git add .;; esac",
            "case $x in (a) git add .;; esac",
            // Parentheses that do not open a command.
            "A=(git add .)",
            "f () { git add .; }",
            "git add .)",
            "(git add .",
            // Heredocs and process substitution stay unread, even nested.
            "echo $(cat <<EOF\nx\nEOF\n); git add .",
            "echo $(diff <(ls) a); git add .",
        ] {
            assert_eq!(refused_for(command), None, "{command}");
        }
    }

    /// Substitutions and subshells run apart from the steps after them, so
    /// they do not hide a writer that follows.
    #[test]
    fn substitutions_and_subshells_do_not_hide_a_writer() {
        for (command, subcommand) in [
            ("(cd /tmp/repo && make); git add x", "add"),
            ("X=$(date); git mv a b", "mv"),
            ("git rm -q \"$(ls old)\"", "rm"),
            (
                "if [ -n \"$(git status --short)\" ]; then git stash; fi",
                "stash",
            ),
            ("N=$((1 + (2 * 3))); git add x", "add"),
            ("echo \"$(echo ')' \"(\")\"; git add x", "add"),
            ("X=`date`; git add x", "add"),
            ("echo $(cd /tmp/repo; pwd); git add x", "add"),
            ("{ cd docs; }; git rm old.md", "rm"),
            ("command git add x", "add"),
            ("while false; do :; done; git add x", "add"),
        ] {
            assert_eq!(refused_for(command), Some(subcommand), "{command}");
        }
    }

    #[test]
    fn a_writer_after_a_harmless_relative_cd_is_recognized() {
        assert_eq!(
            refused_for("cd ui/src && git mv table.ts games.ts && sed -i 's#a#b#' store.ts"),
            Some("mv")
        );
        // Redirections are not separators.
        assert_eq!(refused_for("git add x 2>&1 | tail -3"), Some("add"));
        assert_eq!(refused_for("(cd docs && git rm -q old.md)"), Some("rm"));
    }

    #[test]
    fn every_refusal_names_its_route_without_substrate_vocabulary() {
        for entry in GIT_WRITES {
            let refusal = entry.refusal();
            assert!(refusal.contains(entry.route), "{refusal}");
            assert!(
                refusal.contains(&format!("without the `git {}` step", entry.subcommand)),
                "{refusal}"
            );
            crate::system_prompt::assert_no_substrate_vocabulary(
                &format!("git {} refusal", entry.subcommand),
                &refusal,
            );
        }
    }

    #[test]
    fn each_subcommand_is_listed_once() {
        for (index, entry) in GIT_WRITES.iter().enumerate() {
            assert!(
                !GIT_WRITES[..index]
                    .iter()
                    .any(|earlier| earlier.subcommand == entry.subcommand),
                "git {} is listed twice",
                entry.subcommand
            );
        }
    }
}
