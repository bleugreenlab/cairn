//! Task resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const TASK_TERMINAL_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::TaskTerminal,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/terminal/{slug}",
        name: "Task terminal",
        description: "Sub-agent task terminal output scoped to the task job",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[COMMAND],
                optional: &[DESCRIPTION, WAKE],
                label: "start terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"create\",payload:{command:\"...\",wake:\"exit\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[SUBMIT],
                label: "send to terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"append\",payload:{content:\"rs\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "stop terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const TASK_BROWSER_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::TaskBrowser,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/browser/{slug}",
        name: "Task browser",
        description: "Sub-agent task shared browser pane (native webview) scoped to the task job. replace = go to a URL (sets the page; url required). create = open/ensure the pane (url optional). patch = drive it: navigate (url/navigate) or history/interaction (action: back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad; click/type/scroll take a selector, visible text, or a ?interactive handle). delete = close it. create/replace/patch are an idempotent ensure — they reuse the open pane (reopening it if closed) and never error on an existing slug. Read returns the live url/title/status plus current page content (paged via ?offset/?limit); ?screenshot for a native PNG, ?console/?network for captured runtime buffers, ?interactive for actionable elements as durable handles (e1..eN). Add ?return_content=true to a write to get the post-action page inline. Self-heals across an app restart.",
        read_projections: BROWSER_READ_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[BROWSER_URL],
                label: "open/ensure browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"create\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[BROWSER_URL],
                optional: &[],
                label: "go to a URL (set the page)",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"replace\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    BROWSER_URL,
                    BROWSER_ACTION,
                    BROWSER_SELECTOR,
                    BROWSER_TEXT,
                    BROWSER_HANDLE,
                    BROWSER_VALUE,
                    BROWSER_SUBMIT,
                    BROWSER_TO,
                    BROWSER_BY,
                    BROWSER_TIMEOUT_MS,
                    BROWSER_KINDS,
                ],
                label: "navigate / drive browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"patch\",payload:{action:\"click\",text:\"Sign in\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "close browser",
                example: "write({changes:[{target:\"cairn:~/browser\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const TASK_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Task,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}",
    name: "Task summary",
    description: "Sub-agent task job summary with status and metadata",
    read_projections: NO_PROJECTIONS,
    related: TASK_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const TASK_MESSAGES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::TaskMessages,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/messages",
        name: "Task messages",
        description: "Direct messages to and from a sub-agent task. The task analogue of node /messages: append delivers a direct message, read returns the task's direct-message stream.",
        read_projections: &[
            ProjectionSpec {
                key: "since",
                values: "EPOCH",
            },
            ProjectionSpec {
                key: "limit",
                values: "N",
            },
        ],
        related: TASK_MESSAGES_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "send direct message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE/task/NAME/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    };

pub(crate) const TASK_CHAT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskChat,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat",
    name: "Task transcript",
    description: "Turn-structured digest of the sub-task conversation",
    read_projections: &[ProjectionSpec {
        key: "latest",
        values: "true|false (newest turn first; events within a turn stay chronological)",
    }],
    related: &[
        RelatedSpec {
            label: "full turn",
            kind: ResourceKind::TaskChatTurn,
            actions: false,
        },
        RelatedSpec {
            label: "raw stream",
            kind: ResourceKind::TaskChatRaw,
            actions: false,
        },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const TASK_CHAT_RAW_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskChatRaw,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat/raw",
    name: "Raw task transcript",
    description:
        "Full unsummarized sub-task transcript stream; the digest's programmatic and grep fallback",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const TASK_CHAT_TURN_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskChatTurn,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat/turn/{turn}",
    name: "Task transcript turn",
    description: "Turn-scoped task transcript slice",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const TASK_CHAT_EVENT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::TaskChatEvent,
    uri_template:
        "cairn://p/{project}/{number}/{exec}/{node}/task/{name}/chat/{run_seq}/{event_seq}",
    name: "Task transcript event",
    description: "Single event from a task transcript",
    read_projections: &[
        ProjectionSpec {
            key: "offset",
            values: "N",
        },
        ProjectionSpec {
            key: "limit",
            values: "N",
        },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const TASK_ARTIFACT_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::TaskArtifact,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/task/{task}/{name}",
        name: "Task artifact",
        description: "Sub-task output artifact. Write it via change to cairn:~/<name>; the payload is validated against the task's declared schema. Patch accepts either artifact-field merge payloads (for example {content}) or text replacement operations ({old_string,new_string,field?}); text replacement helper keys are operations and are not stored as artifact metadata.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[],
                label: "write artifact",
                example: "write({changes:[{target:\"cairn:~/result\",mode:\"create\",payload:{content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[CONTENT, OLD_STRING, NEW_STRING, FIELD, REPLACE_ALL],
                label: "edit artifact",
                example: "field merge: write({changes:[{target:\"cairn:~/result\",mode:\"patch\",payload:{content:\"...\"}}]}) | text replacement: write({changes:[{target:\"cairn:~/result\",mode:\"patch\",payload:{old_string:\"old\",new_string:\"new\",field:\"content\"}}]})",
            },
        ],
    };
