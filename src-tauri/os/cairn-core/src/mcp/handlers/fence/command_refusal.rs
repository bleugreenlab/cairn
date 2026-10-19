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
//! raw command, splits it into the simple commands the shell would run at top
//! level, and looks for an explicit git subcommand. Anything it cannot read
//! with confidence -- command or process substitution, a heredoc, a `git`
//! global option, a redirected `GIT_*` variable, a `cd` that may leave the
//! working directory, a launcher such as `sh -c` -- yields no refusal, and the
//! crossing falls through to the ordinary fence policy exactly as before. A
//! missed recognition costs what the prompt always cost; a wrong one would
//! refuse work that could have been approved.
//!
//! A git writer later in a chain (`... && git mv a b`) is still recognized. The
//! step is blocked wherever it appears, so naming it is true regardless of what
//! else the command does.

/// The refusal for a blocked command whose blocked step has an ordinary route
/// inside the sandbox, or `None` to leave the crossing to the fence policy.
pub(crate) fn denied_command_refusal(command: &str) -> Option<String> {
    for words in simple_commands(command)? {
        match classify(&words) {
            Step::GitWrite(write) => return Some(write.refusal()),
            Step::MayLeaveWorktree => return None,
            Step::Other => {}
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
    fn recognize(subcommand: &str, args: &[String]) -> Option<GitWrite> {
        let entry = GIT_WRITES
            .iter()
            .find(|entry| entry.subcommand == subcommand)?;
        let has = |flag: &str| args.iter().any(|arg| arg == flag);
        let writes = match subcommand {
            // Plain `git apply` writes only the working tree and is never blocked.
            "apply" => has("--index") || has("--cached") || has("-3") || has("--3way"),
            // Untracking a file while keeping it has no working-tree equivalent.
            "rm" => !has("--cached"),
            // `git checkout` alone only reports.
            "checkout" => !args.is_empty(),
            "stash" => !matches!(args.first().map(String::as_str), Some("list" | "show")),
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
    Other,
}

fn classify(words: &[String]) -> Step {
    let mut rest = words;
    while let Some(name) = rest.first().and_then(|word| assignment_name(word)) {
        if name.starts_with("GIT_") {
            return Step::MayLeaveWorktree;
        }
        rest = &rest[1..];
    }
    let Some((program, args)) = rest.split_first() else {
        return Step::Other;
    };
    match program.as_str() {
        "cd" | "pushd" => match args {
            [dir] if stays_inside(dir) => Step::Other,
            _ => Step::MayLeaveWorktree,
        },
        "popd" => Step::MayLeaveWorktree,
        "export" if args.iter().any(|arg| arg.starts_with("GIT_")) => Step::MayLeaveWorktree,
        // A global option (`-C <dir>`, `--git-dir`, `-c`) can point git
        // elsewhere, so only `git <subcommand>` itself is read.
        "git" => match args.split_first() {
            Some((subcommand, args)) => {
                GitWrite::recognize(subcommand, args).map_or(Step::Other, Step::GitWrite)
            }
            None => Step::Other,
        },
        _ => Step::Other,
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
        && !dir.contains('$')
        && !dir.split('/').any(|part| part == "..")
}

/// Split a command into the words of each simple command the shell runs at
/// top level, separated by `;`, `&&`, `||`, `|`, `&`, newlines, and
/// parentheses. Quotes are honoured, so a `git mv` inside a `sed` expression
/// is a word, not a command.
///
/// Returns `None` for any construct whose effect cannot be read from the text:
/// command or process substitution, a heredoc, or unbalanced quoting.
fn simple_commands(command: &str) -> Option<Vec<Vec<String>>> {
    let mut segments = Vec::new();
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = command.chars().peekable();

    let end_word = |words: &mut Vec<String>, word: &mut String, in_word: &mut bool| {
        if *in_word {
            words.push(std::mem::take(word));
            *in_word = false;
        }
    };

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '\'' => break,
                        ch => word.push(ch),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '"' => break,
                        '`' => return None,
                        '$' if chars.peek() == Some(&'(') => return None,
                        '\\' => match chars.next()? {
                            '\n' => {}
                            escaped @ ('"' | '\\' | '$' | '`') => word.push(escaped),
                            other => {
                                word.push('\\');
                                word.push(other);
                            }
                        },
                        ch => word.push(ch),
                    }
                }
            }
            '\\' => match chars.next()? {
                '\n' => {}
                escaped => {
                    in_word = true;
                    word.push(escaped);
                }
            },
            '`' => return None,
            '$' if chars.peek() == Some(&'(') => return None,
            '<' | '>' if matches!(chars.peek(), Some('(')) => return None,
            '<' if chars.peek() == Some(&'<') => return None,
            '#' if !in_word => {
                while chars.peek().is_some_and(|&next| next != '\n') {
                    chars.next();
                }
            }
            // `2>&1`, `>&2`, `&>file`: redirections, not separators.
            '&' if word.ends_with(['>', '<']) || chars.peek() == Some(&'>') => {
                in_word = true;
                word.push(c);
            }
            ';' | '&' | '|' | '\n' | '(' | ')' => {
                end_word(&mut words, &mut word, &mut in_word);
                if !words.is_empty() {
                    segments.push(std::mem::take(&mut words));
                }
            }
            c if c.is_whitespace() => end_word(&mut words, &mut word, &mut in_word),
            c => {
                in_word = true;
                word.push(c);
            }
        }
    }
    end_word(&mut words, &mut word, &mut in_word);
    if !words.is_empty() {
        segments.push(words);
    }
    Some(segments)
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
        ] {
            assert_eq!(refused_for(command), None, "{command}");
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
