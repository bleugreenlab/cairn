//! Issue resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const ISSUE_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Issue,
        uri_template: "cairn://p/{project}/{number}",
        name: "Issue details",
        description: "Issue overview with comments, PR data, and execution history",
        read_projections: NO_PROJECTIONS,
        related: ISSUE_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    TITLE,
                    DESCRIPTION,
                    KeySpec::with_aliases(
                        "depends_on",
                        &["dependsOn"],
                        KeyType::Array,
                        "full replacement array of issue URIs",
                    ),
                    LABELS,
                    KeySpec::new(
                        "status",
                        KeyType::Str,
                        "record a resolution (merged | closed); to MERGE a PR, patch its create-pr artifact with action:\"merge\" instead — status:merged with an open PR is refused",
                    ),
                    KeySpec::new(
                        "confirm",
                        KeyType::Bool,
                        "resolve even though the issue still has live work: running work is stopped (resumable) and work that never started is cancelled; the first unconfirmed attempt lists what is live",
                    ),
                    KeySpec::new(
                        "parent",
                        KeyType::Str,
                        "canonical issue URI or thread URI to adopt under: an issue parent also confers branch ancestry (future executions branch from / merge to its branch), a thread parent routes attention only and leaves the child on the base branch; null to orphan back to project-level routing and the base branch",
                    ),
                ],
                label: "patch issue",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER\",mode:\"patch\",payload:{status:\"closed\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[],
                label: "append comment",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER\",mode:\"append\",payload:{content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete issue",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_THREADS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectThreads,
    uri_template: "cairn://p/{project}/threads",
    name: "Project threads",
    description: "First-class threads in this project, addressed by stable names",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[KeySpec::new(
            "name",
            KeyType::Str,
            "the thread's one identifier: a stable non-numeric name",
        )],
        optional: &[KeySpec::new("jurisdiction", KeyType::Str, ""), CONTENT],
        label: "create thread",
        example: "write({changes:[{target:\"cairn://p/PROJECT/threads\",mode:\"append\",payload:{name:\"topic\"}}]})",
    }],
};

pub(crate) const THREAD_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Thread,
    uri_template: "cairn://p/{project}/{name}",
    name: "Thread",
    // The descendants are named here rather than as `related` links because a
    // link renders the TARGET contract's uri_template, and every one of these
    // resolves to a node-family kind whose template spells an issue coordinate a
    // thread does not have. Reading any of these addresses returns that family's
    // own affordance block, so this line only has to say the addresses exist.
    description: "A first-class thread overview. Its session owns the same job-scoped resources an execution node does, addressed beneath the thread name: /chat, /arc, /todos, /tasks, /task/{task}, /memories, /messages, /wakes, /questions, /permissions, /terminal/{slug}, /browser, /repl/{slug}, /calls, /progress, /symbols. Branch-shaped resources (diff, changed, checks, rebase) do not apply -- a thread session has no branch.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                KeySpec::new("jurisdiction", KeyType::Str, ""),
                KeySpec::new(
                    "status",
                    KeyType::Str,
                    "active | closed; closing makes the thread dormant (out of thread listings, refuses prompts and wakes) while keeping its transcript, children, and address — patch it back to active to reopen",
                ),
                KeySpec::new("definition", KeyType::Str, "thread definition JSON"),
                KeySpec::new(
                    "name",
                    KeyType::Str,
                    "the thread's one identifier; renaming re-points every link to it",
                ),
                KeySpec::new(
                    "model",
                    KeyType::Str,
                    "model this thread's session runs on; effective next turn",
                ),
            ],
            label: "patch thread",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NAME\",mode:\"patch\",payload:{status:\"closed\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "append thread message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NAME\",mode:\"append\",payload:{content:\"...\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "delete thread",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NAME\",mode:\"delete\"}]})",
        },
    ],
};

pub(crate) const CHANGED_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Changed,
    uri_template: "cairn://p/{project}/{number}/changed",
    name: "Issue changed files",
    description: "All files changed across executions for an issue",
    read_projections: &[
        ProjectionSpec {
            key: "glob",
            values: "PATTERN",
        },
        ProjectionSpec {
            key: "output_mode",
            values: "files_with_matches|content|count",
        },
    ],
    related: &[RelatedSpec {
        label: "node diff",
        kind: ResourceKind::NodeDiff,
        actions: false,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const ISSUE_EXECUTIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::IssueExecutions,
        uri_template: "cairn://p/{project}/{number}/executions",
        name: "Issue executions",
        description: "Executions for an issue. Append {recipe, backend?, branch?, overrides?} to start a new execution programmatically.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[KeySpec::new(
                "recipe",
                KeyType::Str,
                "recipe id to run; discover ids via cairn://recipes",
            )],
            optional: &[
                KeySpec::new(
                    "backend",
                    KeyType::Str,
                    "claude|codex; defaults to the recipe/agent default",
                ),
                KeySpec::new(
                    "branch",
                    KeyType::Str,
                    "new|base — where this execution's work lands; defaults to new (mint a branch and ship a PR). Only a recipe that declares the target accepts it",
                ),
                LAUNCH_OVERRIDES,
            ],
            label: "start execution",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions\",mode:\"append\",payload:{recipe:\"planbuild\",overrides:{without:[\"review\"]}}}]})",
        }],
    };

pub(crate) const ISSUE_EXECUTION_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::IssueExecution,
        uri_template: "cairn://p/{project}/{number}/executions/{exec_seq}",
        name: "Execution snapshot",
        description: "A single execution's frozen snapshot: the recipe (nodes/edges/trigger), every agent snapshot (prompt, tools, model selection, fence, skills), and skills. Read renders it. Patch {agent, snapshot} merges the given snapshot fields over one agent snapshot (send only what changes; a full snapshot replaces every field), mirroring the UI snapshot editor (fence reaches a live session immediately, model on the next turn, prompt on the next session). An agent cannot edit its own snapshot, nor change the fence of any agent in its own execution.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[
                KeySpec::new(
                    "agent",
                    KeyType::Str,
                    "agentConfigId key in the snapshot's agents map",
                ),
                KeySpec::new(
                    "snapshot",
                    KeyType::Object,
                    "agent-snapshot fields to merge over the current snapshot (camelCase; send only what changes, or a full AgentSnapshot to replace every field)",
                ),
            ],
            optional: &[],
            label: "edit agent snapshot",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions/2\",mode:\"patch\",payload:{agent:\"builder\",snapshot:{fence:\"deny\"}}}]})",
        }],
    };

pub(crate) const ISSUE_MESSAGES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::IssueMessages,
        uri_template: "cairn://p/{project}/{number}/messages",
        name: "Issue messages",
        description: "Messages between agents working on an issue",
        read_projections: &[
            ProjectionSpec {
                key: "before",
                values: "CURSOR",
            },
            ProjectionSpec {
                key: "after",
                values: "CURSOR",
            },
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: ISSUE_MESSAGES_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "append message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    };

pub(crate) const ISSUE_COMMENTS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::IssueComments,
        uri_template: "cairn://p/{project}/{number}/comments",
        name: "Issue comments",
        description: "Stored comments on an issue, each with its stable id, source (user or agent), and timestamp. Read-only here: post a new comment by appending to the issue URI (cairn://p/PROJECT/NUMBER); edit or delete an existing one through its cairn://p/PROJECT/NUMBER/comments/{id} member URI.",
        read_projections: NO_PROJECTIONS,
        related: ISSUE_COMMENTS_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const ISSUE_COMMENT_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::IssueComment,
        uri_template: "cairn://p/{project}/{number}/comments/{comment_seq}",
        name: "Issue comment",
        description: "A single issue comment addressed by its stable, 1-based per-issue sequence (the N in /comments/N, shown as [#N] in the issue's comment list); patch edits its content, delete removes it.",
        read_projections: NO_PROJECTIONS,
        related: ISSUE_COMMENT_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[CONTENT],
                optional: &[],
                label: "edit comment",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/comments/N\",mode:\"patch\",payload:{content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete comment",
                example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/comments/N\",mode:\"delete\"}]})",
            },
        ],
    };
