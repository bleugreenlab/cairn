//! Node/task coordination resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

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
                optional: &[KeySpec::new("subscribe", KeyType::Object, "source filter; kind:\"terminal\" resumes the node when a terminal exits"), KeySpec::new("mute", KeyType::Object, "source filter"), KeySpec::new("until", KeyType::Object, "source filter that lifts the mute")],
                label: "subscribe or mute wakes",
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
                    "tier",
                    KeyType::Str,
                    "sm|md|lg preset, or a model name; defaults to the parent's",
                ),
                KeySpec::new(
                    "backend",
                    KeyType::Str,
                    "claude|codex; defaults to the parent's",
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
            optional: &[KeySpec::new(
                "background",
                KeyType::Bool,
                "fire-and-forget; returns without waiting for the answer",
            )],
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
            optional: &[PERMISSION_DECISION, PERMISSION_SCOPE],
            label: "answer permission",
            example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/permissions/perm-1\",mode:\"patch\",payload:{decision:\"allow\",scope:\"once\"}}]})",
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
            optional: &[PERMISSION_DECISION, PERMISSION_SCOPE],
            label: "answer permission",
            example: "write({changes:[{target:\"cairn://p/PROJ/1/1/builder/task/review/permissions/perm-1\",mode:\"patch\",payload:{decision:\"allow\",scope:\"once\"}}]})",
        }],
    };
