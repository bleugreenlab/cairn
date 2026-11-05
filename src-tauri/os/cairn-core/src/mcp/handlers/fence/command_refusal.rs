//! Instructive refusals for command-scoped sandbox crossings.
//!
//! A command the kernel blocked without naming a path has nothing bounded to
//! approve, so it gets a refusal, and that refusal is generic. One family of
//! commands can be told something better: git's staging records and refs live
//! outside a build slot's working directory, so a `git mv` or
//! `git checkout -- <path>` is always blocked there, and it is never needed,
//! because a batch's commit is derived from the working tree and the branch
//! belongs to Cairn. Every such command has an ordinary in-sandbox equivalent,
//! so the fence answers with that equivalent.
//!
//! The recognizer is deliberately narrow and fails safe. It reads the agent's
//! command (through a whole-line launcher, see below), splits it into the
//! simple commands the shell would run ([`simple_commands`]), notes
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
//! leave the working directory, a launcher inside a chain or given positional
//! arguments (`sh -c 'git mv "$1" b' _ a`) -- yields no refusal, and the
//! crossing falls through to the ordinary fence policy exactly as before. A
//! missed recognition costs what the prompt always cost; a wrong one would
//! refuse work that could have been approved.
//!
//! A launcher that is the whole line with one literal script
//! (`sh -c 'git mv a b'`) is read through to that script
//! ([`command_for_classification`]), because the script is
//! exactly what the named shell runs.
//!
//! A git writer later in a chain (`... && git mv a b`) is still recognized. The
//! step is blocked wherever it appears, so naming it is true regardless of what
//! else the command does.
//!
//! A git call an inline interpreter (`uv run -`, `bun -e`) or a script makes is
//! not in the command text at all. For those, the denied path itself is read:
//! [`recovered_git_write_refusal`] takes the path the sandbox recorded, and
//! [`reported_git_write_refusal`] the denial line the item's own output carries
//! when no path was recovered. Either answers with one generic refusal when the
//! path is a write to git's staging records or refs ([`is_git_internal_write`]).

use crate::mcp::handlers::shell_words::{
    command_for_classification, is_reserved_prefix, simple_commands, Piece, Word,
};

/// The refusal for a blocked command whose blocked step has an ordinary route
/// inside the sandbox, or `None` to leave the crossing to the fence policy.
pub(crate) fn denied_command_refusal(command: &str) -> Option<String> {
    // For each enclosing subshell, innermost last: whether a step inside it
    // may have left the working directory.
    let mut subshells: Vec<bool> = Vec::new();
    for piece in simple_commands(&command_for_classification(command))? {
        match piece {
            Piece::Open => subshells.push(false),
            Piece::Close => {
                subshells.pop();
            }
            Piece::Command(words) => match classify(&words) {
                Step::GitWrite(write) if !subshells.contains(&true) => {
                    return Some(write.refusal());
                }
                // At top level a step that may leave ends recognition.
                Step::MayLeaveWorktree => *subshells.last_mut()? = true,
                Step::Unreadable => return None,
                Step::GitWrite(_) | Step::Other => {}
            },
        }
    }
    None
}

/// The refusal for a denial whose path was not recovered but whose output
/// reports a write to git's staging records or refs, or `None`.
///
/// This is the route for a git call the command text cannot show: one made
/// from an inline interpreter or a script. `reported` is the item's own denial
/// line, which the agent's code wrote, so it is trusted only in the narrowing
/// direction. It can only choose this refusal over the generic one, and nothing
/// read from it is echoed back or reaches what a grant would be scoped by.
pub(crate) fn reported_git_write_refusal(reported: &str) -> Option<String> {
    reported_paths(reported)
        .any(is_git_internal_write)
        .then(|| git_internal_write_text(REPORTED_OPENING))
}

/// The refusal for a denial the sandbox recorded on `path`, when that path is
/// a write to git's staging records or refs, or `None`.
///
/// The path comes from the sandbox's own violation record, so unlike a
/// reported line it is not the agent's text. It matters here because the
/// crossing it would otherwise raise is path-scoped: an allow would add `path`
/// to the sandbox's writable set and let the item write git's index or refs
/// directly.
pub(crate) fn recovered_git_write_refusal(path: &std::path::Path) -> Option<String> {
    path.to_str()
        .is_some_and(is_git_internal_write)
        .then(|| git_internal_write_text(RECORDED_OPENING))
}

/// Whether `path` is a file git writes to change its staging records or refs:
/// inside a git directory (a component ending in `.git`), a linked worktree's
/// own index, `HEAD` and refs under `worktrees/<name>/`, or the repository's
/// shared refs, reflogs and `packed-refs`.
///
/// Matched by structure, not by substring, so a file merely named
/// `index.lock`, or one under a directory that only mentions `.git`, is not
/// one, and only an absolute path counts, as git always names these files.
/// The repository's top-level `index.lock` is left out on purpose: it is the
/// main checkout's index, which no build slot's git writes.
pub(crate) fn is_git_internal_write(path: &str) -> bool {
    let drive = path.as_bytes();
    let absolute = path.starts_with('/')
        || (drive.len() > 2
            && drive[0].is_ascii_alphabetic()
            && drive[1] == b':'
            && matches!(drive[2], b'/' | b'\\'));
    if !absolute {
        return false;
    }
    let parts: Vec<&str> = path.split(['/', '\\']).collect();
    parts.iter().enumerate().any(|(at, part)| {
        part.ends_with(".git")
            && match &parts[at + 1..] {
                ["worktrees", name, record @ ..] if is_name(name) => {
                    matches!(record, ["index.lock" | "HEAD.lock" | "ORIG_HEAD.lock"])
                        || is_ref_record(record)
                }
                ["packed-refs.lock"] => true,
                shared => is_ref_record(shared),
            }
    })
}

/// A ref or reflog file: `refs/...` or `logs/...`, kept both by the repository
/// and by each worktree.
fn is_ref_record(record: &[&str]) -> bool {
    matches!(record, ["refs" | "logs", rest @ ..] if !rest.is_empty() && rest.iter().all(|part| is_name(part)))
}

fn is_name(part: &str) -> bool {
    !part.is_empty() && part != "." && part != ".."
}

/// The candidate paths a denial line names: each quoted span, and each
/// whitespace-separated word stripped of the punctuation that error formats put
/// around a path (`open /x: permission denied`, `Unable to create '/x'`).
fn reported_paths(line: &str) -> impl Iterator<Item = &str> {
    let quoted = ['\'', '"']
        .into_iter()
        .flat_map(move |quote| line.split(quote).skip(1).step_by(2));
    let bare = line
        .split_whitespace()
        .map(|word| word.trim_matches(['\'', '"', '`', ':', ',', ';']));
    quoted.chain(bare)
}

/// How a refusal read from the item's output says where its path came from.
const REPORTED_OPENING: &str = "This item's own output reports that the sandbox denied a write to git's staging records or refs. That report is unverified, since the item wrote it; the usual cause is a git call made from an inline interpreter or a script, where the command text does not show it.";

/// How a refusal read from the sandbox's own record says where its path came
/// from.
const RECORDED_OPENING: &str = "The sandbox recorded that it denied this item a write to git's staging records or refs. The command text does not show a git call, so the usual cause is one made from an inline interpreter or a script.";

fn git_internal_write_text(opening: &str) -> String {
    format!(
        "{opening} Those records live outside your working directory, so the sandbox blocks every write to them, and {COMMIT_IS_DERIVED} {CAIRN_OWNS_THE_BRANCH} The route is `commit_msg` on the `run` or `write` that makes the change: it stages and commits your working tree for you. {MV_ROUTE} {RM_ROUTE} Then run the item again without its git writes; anything before them may already have run."
    )
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

/// Why approving a blocked write to the staging records would not help.
const COMMIT_IS_DERIVED: &str = "approving it would not help: when a batch carries a `commit_msg`, Cairn commits your working tree exactly as it stands and ignores anything staged or committed by hand.";

/// Why a blocked write to refs needs nothing done by hand.
const CAIRN_OWNS_THE_BRANCH: &str = "Cairn owns your branch: it brings an advanced base into your work itself and tells you when it does, so there is nothing to fetch, merge, or rebase by hand.";

const MV_ROUTE: &str = "Move files with plain `mv` instead; git recognizes a rename from the file's content when the change is committed.";

const RM_ROUTE: &str = "Delete files with plain `rm` instead (`rm -r` for a directory); a deletion is committed like any other change.";

const RESTORE_ROUTE: &str = "To put a file back the way a revision has it, write it from that revision: `git show <rev>:<path> > <path>`. To look at another branch without changing anything, `read` `file:<path>?branch=<name>`.";

const BRANCH_ROUTE: &str = "To look at another branch without changing anything, `read` `file:<path>?branch=<name>`. If an advance of your base left conflicts, open `cairn:~/rebase`, commit the merged files, and then request the replay there, because only the replay moves your branch onto the base. The git-workflow skill (cairn://skills/git-workflow) covers both.";

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
        route: MV_ROUTE,
    },
    GitWrite {
        subcommand: "rm",
        writes: Writes::Index,
        route: RM_ROUTE,
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
                "This command runs `git {sub}`, which writes git's own staging records. Those live outside your working directory, so the sandbox blocked that step, and {COMMIT_IS_DERIVED}"
            ),
            Writes::Refs => format!(
                "This command runs `git {sub}`, which moves branches or fetches history into the repository outside your working directory, so the sandbox blocked that step. {CAIRN_OWNS_THE_BRANCH}"
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
            // A whole-line launcher runs its one literal script.
            ("sh -c 'git mv a b'", "mv"),
            ("bash -c \"git add .\"", "add"),
            (" bash -lc 'cd docs && git rm -q old.md'", "rm"),
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
            // A launcher that is not the whole line, or whose script the
            // outer shell or its arguments change: the git step is a quoted
            // argument, not a command.
            "sh -c 'git mv \"$1\" b' _ a",
            "bash -c 'git add .' && true",
            "bash -c \"git add $X\"",
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
            // host-default-branch: a command string the fence classifies, never run.
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

    /// A blocked `git merge`/`rebase` is the moment an agent is reaching for the
    /// wrong way to take an advanced base, so the route names the right one: the
    /// session and its replay, not just the merge.
    #[test]
    fn the_branch_route_names_the_rebase_session_and_its_replay() {
        assert!(BRANCH_ROUTE.contains("`cairn:~/rebase`"), "{BRANCH_ROUTE}");
        assert!(
            BRANCH_ROUTE.contains("request the replay"),
            "{BRANCH_ROUTE}"
        );
        assert!(BRANCH_ROUTE.contains("?branch="), "{BRANCH_ROUTE}");
    }

    /// A posts/518 specimen: an inline `uv run -` script's git, denied its
    /// linked worktree's index lock on a Linux executor.
    const SPECIMEN: &str = "fatal: Unable to create '/home/dev/.cairn-exec/managed-objects/e2e056b6025ecec5f1bbfb1d6fc1026e8a3b75fa93873322d535a125c6742df7/repository.git/worktrees/slot-6/index.lock': Permission denied";

    #[test]
    fn git_internal_writes_are_recognized_by_structure() {
        for path in [
            "/home/dev/.cairn-exec/managed-objects/k/repository.git/worktrees/slot-6/index.lock",
            "/home/dev/.cairn-exec/managed-objects/k/repository.git/worktrees/slot-6/HEAD.lock",
            "/home/dev/.cairn-exec/managed-objects/k/repository.git/worktrees/slot-6/logs/HEAD",
            "/home/dev/.cairn-exec/managed-objects/k/repository.git/refs/heads/agent/x.lock",
            "/home/dev/.cairn-exec/managed-objects/k/repository.git/packed-refs.lock",
            "/Users/dev/projects/app/.git/worktrees/APP-1-builder-0/index.lock",
            "/Users/dev/projects/app/.git/logs/refs/heads/main",
            "C:\\cairn\\repository.git\\worktrees\\slot-2\\index.lock",
        ] {
            assert!(is_git_internal_write(path), "{path}");
        }
        for path in [
            // A file merely named like a git lock.
            "/home/dev/project/build/index.lock",
            "/home/dev/project/index.lock",
            // A path that only mentions `.git`.
            "/home/dev/project.git.bak/worktrees/x/index.lock",
            "/home/dev/.gitconfig",
            "/home/dev/repository.git/objects/ab/cdef",
            "/home/dev/repository.git/config.lock",
            // The main checkout's own index is not a slot's.
            "/home/dev/project/.git/index.lock",
            // No worktree name, or no ref under `refs/`.
            "/home/dev/repository.git/worktrees/index.lock",
            "/home/dev/repository.git/refs",
            "/home/dev/repository.git/refs/../../etc/passwd",
            "/home/dev/.ssh/known_hosts",
            // Git names these files absolutely.
            "repository.git/worktrees/slot-6/index.lock",
        ] {
            assert!(!is_git_internal_write(path), "{path}");
        }
    }

    #[test]
    fn a_reported_git_internal_write_is_refused_with_the_shared_route() {
        for line in [
            SPECIMEN,
            "error: cannot lock ref 'refs/heads/main': Unable to create '/r/repository.git/refs/heads/main.lock': Permission denied",
            "open /r/repository.git/worktrees/slot-1/index.lock: permission denied",
            "PermissionError: [Errno 13] Permission denied: \"/r/.git/worktrees/w/HEAD.lock\"",
        ] {
            let refusal = reported_git_write_refusal(line).unwrap_or_else(|| panic!("{line}"));
            for shared in [COMMIT_IS_DERIVED, CAIRN_OWNS_THE_BRANCH, MV_ROUTE, RM_ROUTE] {
                assert!(refusal.contains(shared), "{refusal}");
            }
            assert!(refusal.contains("unverified"), "{refusal}");
            assert!(refusal.contains("inline interpreter or a script"), "{refusal}");
            assert!(
                refusal.contains("`commit_msg` on the `run` or `write`"),
                "{refusal}"
            );
            // The output's text is never echoed back.
            assert!(!refusal.contains("/r/"), "{refusal}");
            assert!(!refusal.contains("managed-objects"), "{refusal}");
            crate::system_prompt::assert_no_substrate_vocabulary("reported git write refusal", &refusal);
        }
    }

    #[test]
    fn a_recorded_git_internal_write_is_refused_as_verified() {
        let path = std::path::Path::new(
            "/Users/dev/.cairn-exec/managed-objects/m/repository.git/worktrees/slot-6/index.lock",
        );
        let refusal = recovered_git_write_refusal(path).expect("a git-internal path");
        for shared in [COMMIT_IS_DERIVED, CAIRN_OWNS_THE_BRANCH, MV_ROUTE, RM_ROUTE] {
            assert!(refusal.contains(shared), "{refusal}");
        }
        assert!(refusal.contains("The sandbox recorded"), "{refusal}");
        assert!(!refusal.contains("unverified"), "{refusal}");
        assert!(!refusal.contains("slot-6"), "{refusal}");
        crate::system_prompt::assert_no_substrate_vocabulary(
            "recorded git write refusal",
            &refusal,
        );

        for elsewhere in ["/home/dev/project/build/index.lock", "/outside/x"] {
            assert_eq!(
                recovered_git_write_refusal(std::path::Path::new(elsewhere)),
                None,
                "{elsewhere}"
            );
        }
    }

    #[test]
    fn a_reported_denial_elsewhere_has_no_refusal() {
        for line in [
            "error: cannot open '/Users/x/.ssh/known_hosts': Permission denied",
            "cp: cannot create regular file '/home/dev/project/build/index.lock': Permission denied",
            "mention of repository.git/worktrees/slot-6/index.lock in prose: EPERM",
            "Permission denied (os error 13)",
        ] {
            assert_eq!(reported_git_write_refusal(line), None, "{line}");
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
