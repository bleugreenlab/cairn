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

pub(crate) const THREAD_QUEUE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadQueue,
    uri_template: "cairn://p/{project}/{thread}/queue",
    name: "Thread queue",
    description: "This thread's children's executions that the execution queue's caps hold, in the order they start: each with its issue, its place on each cap holding it (fleet, thread), and how long it has been held. Patch `order` to put children first: the listed children take the earliest places this thread's cap-held set already occupies, in both the fleet order and the thread order, and unlisted ones follow in their current order. No other thread's place moves and no cap is skipped; only the operator's Start now skips the queue. Ordered by the thread itself (its sessions and their tasks) or the operator. To file straight to the head, pass place:\"head\" with the execution.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[KeySpec::new(
            "order",
            KeyType::Array,
            "this thread's children by issue number, first to start first; each takes its cap-held executions with it",
        )],
        optional: &[],
        label: "order this thread's held children",
        example: "write({changes:[{target:\"cairn:~/queue\",mode:\"patch\",payload:{order:[5841,5829]}}]})",
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
            PR_PRECONDITION,
        ],
        label: "patch or act on thread branch PR",
        example: "write({changes:[{target:\"cairn:~/branches/1\",mode:\"patch\",payload:{action:\"merge\",method:\"squash\"}}]})",
    }],
};

pub(crate) const THREAD_DB_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadDb,
    uri_template: "cairn://p/{project}/{name}/db",
    name: "Thread database",
    description: "This thread's own durable SQL store: a private database that survives compaction, session teardown, and executor placement. SQL is the whole grammar — CREATE TABLE declares the schema. Read it bare for a compact schema overview (tables, columns with types, each table's max rowid as a size that costs a seek rather than a count, index counts), or with ?sql= (SELECT / read-only WITH / EXPLAIN / schema PRAGMA) — from any thread, by canonical URI. There is no per-task database: cairn:~/db from a spawned task resolves to this owning thread's store. Only the owning thread (and tasks it spawned) may write, and the operator, through a page the thread owns or the app's database browser, may write rows. Contents never enter the thread's compaction seed — the seed carries a one-line census instead. Not synced.",
    read_projections: &[
        ProjectionSpec { key: "sql", values: "read-only SELECT/WITH, EXPLAIN [QUERY PLAN], or schema PRAGMA (verbatim; omit for the schema overview)" },
        ProjectionSpec { key: "table", values: "NAME — one table's full column list and every index on it (columns or expressions, unique/partial marks, a partial index's WHERE predicate), no elision (overview only)" },
        ProjectionSpec { key: "offset", values: "N rows (or tables) to skip (default 0)" },
        ProjectionSpec { key: "limit", values: "N rows (default 100, max 1000); N tables in the overview (default 40, max 200)" },
        ProjectionSpec { key: "format", values: "json: with sql=, JSONL rows; without it, the overview as one document (with table=, every column's type, nullability, key, and whether an insert must name it); omit for text" },
    ],
    related: &[
        RelatedSpec {
            label: "generated SQL schema",
            kind: ResourceKind::ThreadDbSchema,
            actions: false,
        },
        RelatedSpec {
            label: "one table's rows, each addressed by primary key",
            kind: ResourceKind::ThreadDbTable,
            actions: false,
        },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[KeySpec::new("sql", KeyType::Str, "one or more statements, ; separated (DDL + DML)")],
        optional: &[KeySpec::new("params", KeyType::Array, "bound values: one flat array for a single statement, or one inner array per statement")],
        label: "execute SQL against this thread's store",
        example: r#"write({changes:[{target:"cairn:~/db",mode:"patch",payload:{sql:"CREATE TABLE notes(body TEXT); INSERT INTO notes(body) VALUES(?)",params:[[],["durable context"]]}}]})"#,
    }],
};

/// Row-collection read keys, shared by a thread table and an application table
/// so the two stores keep one read grammar.
pub(crate) const DB_TABLE_PROJECTIONS: &[ProjectionSpec] = &[
    ProjectionSpec {
        key: "where",
        values: "COL=VALUE, COL!=VALUE, COL=null (IS NULL) or COL!=null (IS NOT NULL) — repeatable (AND); COL!=VALUE also matches a null; LIKE and ranges stay in ?sql=",
    },
    ProjectionSpec {
        key: "order",
        values: "COL ascending or -COL descending (default: primary key ascending)",
    },
    ProjectionSpec {
        key: "offset",
        values: "N rows to skip (default 0)",
    },
    ProjectionSpec {
        key: "limit",
        values: "N rows (default 100, max 1000)",
    },
    ProjectionSpec {
        key: "format",
        values: "json for one document with rows as column-name objects; omit for a markdown table",
    },
];

pub(crate) const DB_ROW_PROJECTIONS: &[ProjectionSpec] = &[ProjectionSpec {
    key: "format",
    values: "json for the row as a column-name object; omit for a markdown table",
}];

pub(crate) const THREAD_DB_TABLE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadDbTable,
    uri_template: "cairn://p/{project}/{name}/db/{table}",
    name: "Thread database table",
    description: "The rows of one table in this thread's database, each addressed by its own row URI when the table has a single-column primary key. Read with where= filters, ordering, and paging. Append a column map to insert a row, and the reply carries the new row's URI (a generated key included); append {rows:[...]} to insert up to 1000 in one transaction, the reply naming each row in order; add upsert:true to update a row's given columns, leaving the rest, when its primary key already exists. Patch or delete the collection with where= in the target to change every matching row; the reply carries the count, and without where= the write is refused. The affordance block lists the live columns, types, nullability, and primary key. The segment `schema` is reserved: read a table named `schema` with ?sql= or the database's ?table=schema.",
    read_projections: DB_TABLE_PROJECTIONS,
    related: &[RelatedSpec {
        label: "database overview and queries",
        kind: ResourceKind::ThreadDb,
        actions: true,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Append,
            required: &[],
            optional: &[],
            label: "insert one row (payload is a column map), or many with {rows:[...]}; upsert:true updates the given columns on a primary-key conflict",
            example: r#"write({changes:[{target:"cairn:~/db/TABLE",mode:"append",payload:{rows:[{COLUMN:"a"},{COLUMN:"b"}]}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[],
            label: "update columns of every row the target's where= matches (payload is a column map); the reply carries the count",
            example: r#"write({changes:[{target:"cairn:~/db/TABLE?where=COLUMN=value",mode:"patch",payload:{COLUMN:"new"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "delete every row the target's where= matches; the reply carries the count",
            example: r#"write({changes:[{target:"cairn:~/db/TABLE?where=COLUMN=null",mode:"delete"}]})"#,
        },
    ],
};

pub(crate) const THREAD_DB_ROW_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ThreadDbRow,
    uri_template: "cairn://p/{project}/{name}/db/{table}/{pk}",
    name: "Thread database row",
    description: "One row of a thread database table, addressed by its single-column primary key. Patch a column map to update it; delete to remove it. The primary key is the row's address and is not patchable. A table with a composite or absent primary key has no row URIs; read its rows with ?sql= or the collection's where= filter.",
    read_projections: DB_ROW_PROJECTIONS,
    related: &[RelatedSpec {
        label: "database overview and queries",
        kind: ResourceKind::ThreadDb,
        actions: true,
    }],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[],
            label: "update columns of this row (payload is a column map)",
            example: r#"write({changes:[{target:"cairn:~/db/TABLE/PK",mode:"patch",payload:{COLUMN:"value"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "delete this row",
            example: r#"write({changes:[{target:"cairn:~/db/TABLE/PK",mode:"delete"}]})"#,
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
    description: "A first-class thread overview. Its session owns the same job-scoped resources an execution node does, addressed beneath the thread name: /chat, /arc, /branches, /queue (its children's cap-held executions in start order; patch {order:[N,...]} to reorder), /todos, /tasks, /task/{task}, /memories, /messages, /wakes, /questions, /permissions, /terminal/{slug}, /browser, /repl/{slug}, /calls, /progress, /symbols, /db, /pages, /services. The session has no default branch; numbered branches are explicit resources beneath /branches. Branch-shaped resources (diff, changed, checks, rebase) do not apply to the thread session itself.",
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
        ProjectionSpec {
            key: "limit",
            values: "files listed (default all)",
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
        description: "Executions for an issue. Append {recipe, backend?, branch?, continuity?, lifecycle?, overrides?, after?, place?} to start a new execution programmatically. Starting an ordinary execution IS the decision to supersede whichever execution owns this issue's attempt now: that one is fenced (its nodes take no further turns and its effects are refused) and its agents are stopped. With `after` the execution is filed HELD and starts only when every gate clause is satisfied; a `tenet_verdict` clause holds it on the operator's verdict on a tenet.",
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
                QUEUE_PLACE,
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
