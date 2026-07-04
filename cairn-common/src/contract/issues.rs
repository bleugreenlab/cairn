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
                    KeySpec::new(
                        "labels",
                        KeyType::Array,
                        "full replacement label refs by name or slug",
                    ),
                    KeySpec::new(
                        "status",
                        KeyType::Str,
                        "record a resolution (merged | closed); to MERGE a PR, patch its create-pr artifact with action:\"merge\" instead — status:merged with an open PR is refused",
                    ),
                    KeySpec::new(
                        "parent",
                        KeyType::Str,
                        "canonical issue URI to adopt under (future executions branch from / merge to the parent's branch); null to orphan back to the base branch",
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
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const ISSUE_EXECUTIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::IssueExecutions,
        uri_template: "cairn://p/{project}/{number}/executions",
        name: "Issue executions",
        description: "Executions for an issue. Append {recipe, backend?} to start a new execution programmatically.",
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
            optional: &[KeySpec::new(
                "backend",
                KeyType::Str,
                "claude|codex; defaults to the recipe/agent default",
            )],
            label: "start execution",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions\",mode:\"append\",payload:{recipe:\"build\",backend:\"claude\"}}]})",
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
