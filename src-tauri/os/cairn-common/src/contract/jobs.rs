//! Node/task coordination resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;
use cairn_uri::ResourceKind;

pub(crate) const JOB_TODOS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::JobTodos,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/todos",
        name: "Node todos",
        description: "Todo list owned by a node job (read, replace/append/patch via change)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[TODOS],
                optional: &[],
                label: "replace todos",
                example: "write({changes:[{target:\"cairn:~/todos\",mode:\"replace\",payload:{todos:[{content:\"...\",status:\"pending\"}]}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[TODOS],
                optional: &[],
                label: "append todos",
                example: "write({changes:[{target:\"cairn:~/todos\",mode:\"append\",payload:{todos:[{content:\"...\",status:\"pending\"}]}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[UPDATES],
                optional: &[],
                label: "patch todos",
                example: "write({changes:[{target:\"cairn:~/todos\",mode:\"patch\",payload:{updates:[{id:\"...\",status:\"completed\"}]}}]})",
            },
        ],
    };

pub(crate) const NODE_CHECKS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeChecks,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/checks",
        name: "Node checks",
        description: "Turn-end project check results for a node job — running: live log tail; done: cached pass/fail verdicts (read-only)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const NODE_BATCHES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeBatches,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/batches",
    name: "Node batches",
    description: "Run batches owned by this node only (not its tasks): those still in flight (suspended with their call or detached from it), then those that kept running after their call answered with a handle, newest first: state, verdict, and what each committed. Cancel one at its own batches/<request_id>",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_BATCH_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeBatch,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/batches/{request_id}",
    name: "Node batch",
    description: "One run batch of this node, in flight (suspended with its call or detached from it) or settled: where it stands now and the item it is running, its verdict and output once settled, and whether it committed. A patch with action:cancel stops this batch only; a call waiting on it resumes with a cancelled result, and a commit it was already writing stands",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[BATCH_ACTION],
        optional: &[],
        label: "cancel this run batch",
        example: "write({changes:[{target:\"cairn:~/batches/REQUEST_ID\",mode:\"patch\",payload:{action:\"cancel\"}}]})",
    }],
};

pub(crate) const TASK_CHECKS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskChecks,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/checks",
    name: "Task checks",
    description: "Turn-end project check results for a sub-agent task job (read-only)",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_WAKES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeWakes,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/wakes",
        name: "Node wakes",
        description: "Wake subscriptions owned by a node job (read; subscribe/mute/unmute/unsubscribe via write)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[],
                optional: &[KeySpec::new("subscribe", KeyType::Object, "source filter; kind:\"terminal\" resumes the node when a terminal exits, kind:\"checks\" when a node's project check lanes settle (ref defaults to your own node), kind:\"posts\" when a post your home may see is published (no ref), kind:\"rebuild\" with contains:<40-hex commit | issue URI> once when the runner boots with a build whose source commit contains it (fires at once if the current boot already does)"), KeySpec::new("schedule", KeyType::Object, "thread-only anchored recurrence with every and reason"), KeySpec::new("condition", KeyType::Object, "thread-only standing SQL predicate over cairn:~/db (sql, reason, optional once); evaluated after every committed write and fires when it starts returning rows"), KeySpec::new("mute", KeyType::Object, "source filter"), KeySpec::new("until", KeyType::Object, "source filter that lifts the mute")],
                label: "subscribe, schedule, arm a condition, or mute wakes",
                example: "write({changes:[{target:\"cairn:~/wakes\",mode:\"append\",payload:{subscribe:{kind:\"terminal\",ref:\"cairn:~/terminal/<slug>\",on:\"exit\"}}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[KeySpec::new("unmute", KeyType::Object, "source filter")],
                optional: &[],
                label: "unmute wakes",
                example: "write({changes:[{target:\"cairn:~/wakes\",mode:\"patch\",payload:{unmute:{kind:\"issue\",ref:\"cairn://p/CAIRN/1\"}}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[KeySpec::new("unsubscribe", KeyType::Object, "source filter")],
                optional: &[],
                label: "unsubscribe wakes",
                example: "write({changes:[{target:\"cairn:~/wakes\",mode:\"delete\",payload:{unsubscribe:{kind:\"issue\",ref:\"cairn://p/CAIRN/1\"}}}]})",
            },
        ],
    };

pub(crate) const NODE_TASKS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeTasks,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/tasks",
        name: "Node tasks",
        description: "Delegated sub-agent tasks owned by a node job (read; spawn via change append)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[SUBAGENT_TYPE, TASK_DESCRIPTION],
            optional: &[
                KeySpec::new("prompt", KeyType::Str, ""),
                KeySpec::new(
                    "models",
                    KeyType::Array,
                    "the allotment the task selects its model within, replacing its agent's: pool ids and provider/model pairs, e.g. [lg] or [claude/opus]; defaults to the agent's",
                ),
                KeySpec::new(
                    "session",
                    KeyType::Str,
                    "new (fresh context) | fork (copy parent's); default new",
                ),
                KeySpec::new(
                    "background",
                    KeyType::Bool,
                    "fire-and-forget; returns task URIs without waiting",
                ),
            ],
            label: "spawn task",
            example: "write({changes:[{target:\"cairn:~/tasks\",mode:\"append\",payload:{subagentType:\"Explore\",description:\"map parser flow\",prompt:\"...\"}}]})",
        }],
    };

pub(crate) const NODE_CALLS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeCalls,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/calls",
    name: "Node calls",
    description:
        "Ephemeral agent calls owned by a node job — one prompt in, schema-validated JSON out (read; spawn via change append)",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Append,
        required: &[KeySpec::new("prompt", KeyType::Str, "the call's instruction")],
        optional: &[
            KeySpec::new(
                "subagentType",
                KeyType::Str,
                "worker agent name; defaults to Explore",
            ),
            KeySpec::new(
                "outputSchema",
                KeyType::Str,
                "preset name or inline JSON Schema; defaults to the return contract",
            ),
            KeySpec::new(
                "branch",
                KeyType::Str,
                "inherit (default; the caller's branch) | none (no repository access)",
            ),
            KeySpec::new(
                "models",
                KeyType::Array,
                "the allotment the call selects its model within, replacing its agent's: pool ids and provider/model pairs, e.g. [sm] or [claude/haiku]; defaults to the agent's",
            ),
            KeySpec::new("label", KeyType::Str, "durable workflow tag"),
            KeySpec::new("phase", KeyType::Str, "durable workflow phase tag"),
            KeySpec::new(
                "background",
                KeyType::Bool,
                "fire-and-forget; returns call URIs without waiting",
            ),
        ],
        label: "spawn call",
        example: "write({changes:[{target:\"cairn:~/calls\",mode:\"append\",payload:{prompt:\"Summarize X as JSON\",outputSchema:\"return\"}}]})",
    }],
};

pub(crate) const NODE_QUESTIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeQuestions,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/questions",
        name: "Node questions",
        description: "User questions asked by a node job (read; ask via change append)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[QUESTIONS],
            optional: &[
                KeySpec::new(
                    "blocking",
                    KeyType::Bool,
                    "default true; false records the question and returns its URI at once, and you keep working: the answer reaches you when it is given, at your next tool call or by resuming you if your turn has ended (it also stays readable at the URI, and ?wait blocks on it)",
                ),
                KeySpec::new(
                    "ordinal",
                    KeyType::Int,
                    "a workflow's dispatch ordinal for a non-blocking ask; a replay at the same ordinal returns the same question",
                ),
            ],
            label: "ask question",
            example: "write({changes:[{target:\"cairn:~/questions\",mode:\"append\",payload:{questions:[{question:\"...\",options:[{label:\"...\",description:\"...\"}],multiSelect:false}]}}]})",
        }],
    };

pub(crate) const NODE_QUESTION_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeQuestion,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/questions/{segment}",
        name: "Node question",
        description: "A single user question and its response",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[ANSWER, ANSWERS],
                label: "answer question",
                example: "write({changes:[{target:\"cairn://p/PROJ/1/1/planner/questions/q-1\",mode:\"patch\",payload:{answers:[{index:0,selection:\"Option A\"},{index:1,text:\"free-form answer\"}]}}]})", // single-question shorthand: payload:{answer:\"Option 1\"}
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[],
                optional: &[ANSWER, ANSWERS],
                label: "answer question (compat)",
                example: "write({changes:[{target:\"cairn://p/PROJ/1/1/planner/questions/q-1\",mode:\"append\",payload:{answer:\"Option 1\"}}]})",
            },
        ],
    };

pub(crate) const NODE_PERMISSIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodePermissions,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/permissions",
        name: "Node permissions",
        description: "Permission requests raised by a node job: pending worktree-fence crossings and tool prompts plus their resolutions (read; answer one via its segment)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const NODE_PERMISSION_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodePermission,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/permissions/{segment}",
        name: "Node permission",
        description: "A single permission request and its resolution; patch {decision, scope} to answer (allow/deny, once/session)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                PERMISSION_DECISION,
                PERMISSION_SCOPE,
                PERMISSION_LIFETIME,
                PERMISSION_EXPIRES_IN_SECONDS,
                PERMISSION_MESSAGE,
            ],
            label: "answer permission",
            example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/permissions/perm-1\",mode:\"patch\",payload:{decision:\"deny\",message:\"write it under $TMPDIR instead\"}}]})",
        }],
    };

pub(crate) const TASK_PERMISSIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::TaskPermissions,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/permissions",
        name: "Task permissions",
        description: "Permission requests raised by a sub-agent task job: pending worktree-fence crossings and tool prompts plus their resolutions (read; answer one via its segment)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const TASK_PERMISSION_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::TaskPermission,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/permissions/{segment}",
        name: "Task permission",
        description: "A single permission request raised by a sub-agent task job and its resolution; patch {decision, scope} to answer (allow/deny, once/session)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                PERMISSION_DECISION,
                PERMISSION_SCOPE,
                PERMISSION_LIFETIME,
                PERMISSION_EXPIRES_IN_SECONDS,
                PERMISSION_MESSAGE,
            ],
            label: "answer permission",
            example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/task/review/permissions/perm-1\",mode:\"patch\",payload:{decision:\"deny\",message:\"write it under $TMPDIR instead\"}}]})",
        }],
    };

pub(crate) const TASK_BATCHES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskBatches,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/batches",
    name: "Task batches",
    description: "Run batches owned by this task only: those in flight, then those that outlived their call, newest first. Cancel one at its own batches/<request_id>",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const TASK_BATCH_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskBatch,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/batches/{request_id}",
    name: "Task batch",
    description: "One run batch of this task, in flight or settled: where it stands and the item it is running, its verdict, output and commit. A patch with action:cancel stops this batch only; a call waiting on it resumes with a cancelled result, and a commit it was already writing stands",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[BATCH_ACTION],
        optional: &[],
        label: "cancel this run batch",
        example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/task/review/batches/REQUEST_ID\",mode:\"patch\",payload:{action:\"cancel\"}}]})",
    }],
};
