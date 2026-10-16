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

pub(crate) const THREAD_DB_SCHEMA_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadDbSchema,
    uri_template: "cairn://p/{project}/{name}/db/schema",
    name: "Thread database schema",
    description: "Generated, read-only SQL source for this thread database. It includes tables, indexes, triggers, views, and other schema objects in deterministic order, with Cairn-owned and agent-owned objects identified explicitly. Schema mutations continue through SQL writes to the thread database; this projection is not a second mutation authority.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec {
        label: "database overview and queries",
        kind: ResourceKind::ThreadDb,
        actions: true,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const THREAD_BRANCHES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadBranches,
    uri_template: "cairn://p/{project}/{thread}/branches",
    name: "Thread branches",
    description: "Numbered branches owned by this thread. An append is the declaration rendered after the write pre-scan has atomically minted its backing ref and job.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[
            TITLE,
            KeySpec::new("pr", KeyType::Object, "PR request with optional body"),
        ],
        optional: &[],
        label: "mint thread branch",
        example: "write({changes:[{target:\"cairn:~/branches\",mode:\"append\",payload:{title:\"...\",pr:{body:\"...\"}}},{target:\"file:path\",mode:\"patch\",payload:{old_string:\"old\",new_string:\"new\"}}],commit_msg:\"...\"})",
    }],
};

pub(crate) const THREAD_BRANCH_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadBranch,
    uri_template: "cairn://p/{project}/{thread}/branches/{number}",
    name: "Thread branch",
    description: "A numbered thread branch with its backing ref, lifecycle stage, and PR state",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[],
        optional: &[
            TITLE,
            KeySpec::new("stage", KeyType::Str, "abandoned only"),
            KeySpec::new("action", KeyType::Str, "merge|close — act on this branch's create-pr artifact"),
            KeySpec::new("method", KeyType::Str, "merge method for action:merge (default squash)"),
        ],
        label: "patch or act on thread branch PR",
        example: "write({changes:[{target:\"cairn:~/branches/1\",mode:\"patch\",payload:{action:\"merge\",method:\"squash\"}}]})",
    }],
};

pub(crate) const THREAD_DB_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadDb,
    uri_template: "cairn://p/{project}/{name}/db",
    name: "Thread database",
    description: "This thread's own durable SQL store: a private database that survives compaction, session teardown, and executor placement. SQL is the whole grammar — CREATE TABLE declares the schema. Read it bare for a compact schema overview (tables, columns with types, row counts), or with ?sql= (SELECT / read-only WITH / EXPLAIN / schema PRAGMA) — from any thread, by canonical URI. There is no per-task database: cairn:~/db from a spawned task resolves to this owning thread's store. Only the owning thread (and tasks it spawned) may write. Contents never enter the thread's compaction seed — the seed carries a one-line census instead. Not synced.",
    read_projections: &[
        ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA (verbatim; omit for the schema overview)" },
        ProjectionSpec { key: "table", values: "NAME — one table's full column and index list, no elision (overview only)" },
        ProjectionSpec { key: "offset", values: "N rows (or tables) to skip (default 0)" },
        ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000); N tables in the overview (default 40, max 200)" },
        ProjectionSpec { key: "format", values: "json for JSONL rows; omit for tab-separated text" },
    ],
    related: &[RelatedSpec {
        label: "generated SQL schema",
        kind: ResourceKind::ThreadDbSchema,
        actions: false,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[KeySpec::new("sql", KeyType::Str, "one or more statements, ; separated (DDL + DML)")],
        optional: &[KeySpec::new("params", KeyType::Array, "bound values: one flat array for a single statement, or one inner array per statement")],
        label: "execute SQL against this thread's store",
        example: r#"write({changes:[{target:"cairn:~/db",mode:"patch",payload:{sql:"CREATE TABLE notes(body TEXT); INSERT INTO notes(body) VALUES(?)",params:[[],["durable context"]]}}]})"#,
    }],
};

pub(crate) const THREAD_PROCEDURES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadProcedures,
    uri_template: "cairn://p/{project}/{name}/procedures",
    name: "Thread procedures",
    description: "Named, replayable run batches stored in this thread's durable database. Procedures execute with the invoking thread's identity; read another thread's procedures by canonical URI, but copy one into your own thread before invoking it.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "automations", kind: ResourceKind::ThreadAutomations, actions: true }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Create,
        required: &[
            KeySpec::new("name", KeyType::Str, "[a-z0-9][a-z0-9-]{0,63}"),
            KeySpec::new("body", KeyType::Object, "stored run batch envelope"),
        ],
        optional: &[KeySpec::new("description", KeyType::Str, "")],
        label: "create procedure",
        example: r#"write({changes:[{target:"cairn:~/procedures",mode:"create",payload:{name:"collect",body:{commands:[{code:"console.log('ok')",interpreter:"javascript"}]}}}]})"#,
    }],
};

pub(crate) const THREAD_PROCEDURE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadProcedure,
    uri_template: "cairn://p/{project}/{name}/procedures/{procedure}",
    name: "Thread procedure",
    description: "One named, replayable run batch. Invoke the owning thread's procedure with run({commands:[{target:\"cairn:~/procedures/{procedure}\",payload:{args:[...]}}]}).",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "automations", kind: ResourceKind::ThreadAutomations, actions: true }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                KeySpec::new("body", KeyType::Object, "replacement run batch envelope"),
                KeySpec::new("description", KeyType::Str, "null clears it"),
            ],
            label: "edit procedure",
            example: r#"write({changes:[{target:"cairn:~/procedures/collect",mode:"patch",payload:{description:"Collect observations"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "delete procedure",
            example: r#"write({changes:[{target:"cairn:~/procedures/collect",mode:"delete"}]})"#,
        },
    ],
};

pub(crate) const THREAD_AUTOMATIONS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadAutomations,
    uri_template: "cairn://p/{project}/{name}/automations",
    name: "Thread automations",
    description: "Cron-like collectors that execute a stored procedure without a model call. Unlike wakes, an automation runs work and normally wakes nobody; the first failure of each failure streak is delivered to the owning thread.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "procedures", kind: ResourceKind::ThreadProcedures, actions: true }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Create,
        required: &[
            KeySpec::new("procedure", KeyType::Str, "procedure name"),
            KeySpec::new("every", KeyType::Str, "5m to 30d"),
        ],
        optional: &[KeySpec::new("args", KeyType::Array, "positional arguments")],
        label: "create automation",
        example: r#"write({changes:[{target:"cairn:~/automations",mode:"create",payload:{procedure:"collect",every:"6h"}}]})"#,
    }],
};

pub(crate) const THREAD_AUTOMATION_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadAutomation,
    uri_template: "cairn://p/{project}/{name}/automations/{id}",
    name: "Thread automation",
    description: "One scheduled procedure invocation, including its cadence, next due occurrence, and last outcome.",
    read_projections: NO_PROJECTIONS,
    related: &[RelatedSpec { label: "procedures", kind: ResourceKind::ThreadProcedures, actions: true }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                KeySpec::new("every", KeyType::Str, "5m to 30d"),
                KeySpec::new("args", KeyType::Array, "positional arguments"),
                KeySpec::new("state", KeyType::Str, "active | muted"),
            ],
            label: "edit automation",
            example: r#"write({changes:[{target:"cairn:~/automations/ID",mode:"patch",payload:{state:"muted"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "delete automation",
            example: r#"write({changes:[{target:"cairn:~/automations/ID",mode:"delete"}]})"#,
        },
    ],
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
    description: "A first-class thread overview. Its session owns the same job-scoped resources an execution node does, addressed beneath the thread name: /chat, /arc, /branches, /todos, /tasks, /task/{task}, /memories, /messages, /wakes, /questions, /permissions, /terminal/{slug}, /browser, /repl/{slug}, /calls, /progress, /symbols, /db, /procedures, /automations. The session has no default branch; numbered branches are explicit resources beneath /branches. Branch-shaped resources (diff, changed, checks, rebase) do not apply to the thread session itself.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                KeySpec::new(
                    "action",
                    KeyType::Str,
                    "stop; interrupts a live turn and parks the session warm, or cancels runless pending work and idles the session job",
                ),
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
            label: "patch or stop thread",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NAME\",mode:\"patch\",payload:{action:\"stop\"}}]})",
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
        description: "Executions for an issue. Append {recipe, backend?, branch?, continuity?, lifecycle?, overrides?, after?} to start a new execution programmatically. Starting an ordinary execution IS the decision to supersede whichever execution owns this issue's attempt now: that one is fenced (its nodes take no further turns and its effects are refused) and its agents are stopped. With `after` the execution is filed HELD and starts only when every gate clause is satisfied.",
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
                KeySpec::new(
                    "continuity",
                    KeyType::Str,
                    "what this execution does with committed work earlier executions of this issue left on their branches. Defaults to auto: carry the latest valid committed head forward when exactly one exists, and refuse (naming the candidates) when several unrelated heads make 'latest' undefined. `clean` starts at the base branch and supersedes prior work; a node URI, branch name, or commit id from a refusal adopts that one. Either explicit form binds later retries too, so a head one execution sets aside is not reinstated by the next",
                ),
                KeySpec::new(
                    "lifecycle",
                    KeyType::Str,
                    "canonical|parallel — defaults to canonical, this issue's next attempt, which supersedes and fences the execution that owns the attempt now. `parallel` declares an alternative that runs beside the canonical one, never claims to be the issue's implementation, and is never fenced by a successor",
                ),
                LAUNCH_OVERRIDES,
                AFTER_GATES,
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
        description: "A single execution's frozen snapshot: the recipe (nodes/edges/trigger), every agent snapshot (prompt, tools, model selection, fence, skills), and skills. Read renders it. Patch {agent, snapshot} merges the given snapshot fields over one agent snapshot (send only what changes; a full snapshot replaces every field), mirroring the UI snapshot editor (fence reaches a live session immediately, model on the next turn, prompt on the next session). An agent cannot edit its own snapshot, nor change the fence of any agent in its own execution. Read also lists every condition evaluation; Patch {reevaluate} re-runs a condition that blocked and routed nowhere, recording a new routed or blocked row beside the old one.",
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
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[KeySpec::new(
                "reevaluate",
                KeyType::Str,
                "id of a blocked condition node in this execution's recipe",
            )],
            optional: &[],
            label: "re-evaluate blocked condition",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions/2\",mode:\"patch\",payload:{reevaluate:\"check-risk\"}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[KeySpec::new(
                "release",
                KeyType::Bool,
                "true starts a HELD execution now, without waiting for its gates; recorded as an override with the acting principal",
            )],
            optional: &[],
            label: "release held execution early",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions/2\",mode:\"patch\",payload:{release:true}}]})",
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[KeySpec::new(
                "after",
                KeyType::Array,
                "full replacement list of a HELD execution's gate clauses; a kept clause keeps its evidence, and [] starts it now. Refused once the execution has been released",
            )],
            optional: &[],
            label: "replace held execution's gates",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/executions/2\",mode:\"patch\",payload:{after:[{fact:\"issue_resolution\",issue:\"cairn://p/PROJECT/12\",resolution:\"merged\"}]}}]})",
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
