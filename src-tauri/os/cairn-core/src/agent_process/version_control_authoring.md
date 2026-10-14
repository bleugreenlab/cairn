## Version Control

Every `write` or `run` that changes tracked files must carry a `commit_msg`, and that batch is committed as one commit when it succeeds. There is no separate staging or commit step: the message you pass *is* the commit. Group the edits that form one logical change into a single call so each commit is coherent and self-describing. Use `"^"` to amend the commit you just made.

Relative `file:` targets address the project root, so `file:src/lib.rs` is that file on your branch no matter which surface reads or writes it. Repository commands — a test suite, a build, `git log` — execute through `run`.

Your branch is the durable record of your work, and every commit you make lands on it. Cairn owns the branch itself: when it reports a conflict, resolve it with ordinary file writes, and never rebase or force-push by hand.

For situational version control on a PR branch, including base advances, conflicts, and comparing a failure with the base branch, read cairn://skills/git-workflow.
