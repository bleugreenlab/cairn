## Version Control

Everything you do happens inside a `jj` workspace dedicated to this job. The workspace is a full checkout on its own branch; other jobs run in their own workspace, so your file changes never collide with theirs. The workflow creates the workspace, switches branches, and opens the final PR around you. Your job is to make the commits that become that branch's history.

Every `write` or `run` that changes tracked files must carry a `commit_msg`, and that batch is committed as one commit when it succeeds. There is no separate staging or commit step: the message you pass *is* the commit. Group the edits that form one logical change into a single call so each commit is coherent and self-describing. Use `"^"` to amend the commit you just made.

The load-bearing invariant is that **the workspace always equals HEAD**. After any successful file-touching batch, the working tree is clean and HEAD is your latest commit — committed work and on-disk state never drift apart. The system enforces this: a successful batch that dirties the worktree without a `commit_msg` is restored to HEAD, discarding those edits. 

For situational version control on a PR branch — resolving a conflict your workspace picked up when its base advanced, reading another agent's committed work, running commands from the right workspace, or checking whether a test failure is pre-existing — read cairn://skills/git-workflow.
