//! Node resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const NODE_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Node,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}",
        name: "Node summary",
        description: "Execution node summary with status and metadata. A patch with action:stop interrupts a running node's active turn and parks its session warm (resumable, not a kill). A `pr` action node also reads back its live GitHub state and accepts merge/close/refresh actions on this bare URI.",
        read_projections: &[ProjectionSpec {
            key: "diff",
            values: "full (inline the live PR patch text on a pr action node)",
        }],
        related: NODE_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[NODE_ACTION],
            optional: &[PR_METHOD],
            label: "stop a running node or act on a pr action node",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE\",mode:\"patch\",payload:{action:\"stop\"}}]})",
        }],
    };

pub(crate) const NODE_MESSAGES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeMessages,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/messages",
        name: "Node messages",
        description: "Direct messages to and from a node agent. Canonical messaging target for a node, symmetric with project/issue /messages: append delivers a direct message (queued and steered if the recipient is mid-turn), read returns the node's direct-message stream.",
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
        related: NODE_MESSAGES_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "send direct message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    };

pub(crate) const NODE_PROGRESS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeProgress,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/progress",
        name: "Workflow progress",
        description: "Durable phase/log progress timeline for a workflow node. Append records a typed entry -- a `phase` boundary (text = phase name) or a `log` line (text = message); read returns the chronological timeline with timestamps. The harness phase()/log() verbs write here and the workflow monitoring panel renders it.",
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
        related: NODE_PROGRESS_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[PROGRESS_KIND, PROGRESS_TEXT],
            optional: &[],
            label: "append a phase or log entry",
            example: "write({changes:[{target:\"cairn://p/PROJECT/NUMBER/EXEC/NODE/progress\",mode:\"append\",payload:{kind:\"phase\",text:\"scope\"}}]})",
        }],
    };

pub(crate) const NODE_CHAT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeChat,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat",
    name: "Node transcript",
    description: "Turn-structured digest of the agent conversation",
    read_projections: &[
        ProjectionSpec {
            key: "latest",
            values: "true|false (newest turn first; events within a turn stay chronological)",
        },
        ProjectionSpec {
            key: "messages",
            values: "full (render user & assistant messages unabridged instead of truncated)",
        },
        ProjectionSpec {
            key: "diffs",
            values: "true (inline each file write's change body beneath its row)",
        },
    ],
    related: &[
        RelatedSpec {
            label: "full turn",
            kind: ResourceKind::NodeChatTurn,
            actions: false,
        },
        RelatedSpec {
            label: "raw stream",
            kind: ResourceKind::NodeChatRaw,
            actions: false,
        },
    ],
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_CHAT_RAW_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeChatRaw,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat/raw",
    name: "Raw transcript",
    description: "Full unsummarized transcript stream; the digest's programmatic and grep fallback",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_CHAT_TURN_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeChatTurn,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat/turn/{turn}",
    name: "Transcript turn",
    description: "Turn-scoped transcript slice",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const NODE_CHAT_EVENT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeChatEvent,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/chat/{run_seq}/{event_seq}",
    name: "Transcript event",
    description: "Single event from a node transcript",
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

pub(crate) const NODE_ARTIFACT_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeArtifact,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/{name}",
        name: "Node artifact",
        description: "Agent output artifact (plan, PR, etc.). Write it via change to cairn:~/<name>; the payload is validated against the node's declared schema. Patch accepts either artifact-field merge payloads (for example {content}) or text replacement operations ({old_string,new_string,field?}); text replacement helper keys are operations and are not stored as artifact metadata. A PR artifact reads back its live GitHub state and accepts merge/close/refresh actions.",
        read_projections: &[ProjectionSpec {
            key: "diff",
            values: "full (inline the live PR patch text on a PR artifact)",
        }],
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[],
                label: "write artifact",
                example: "write({changes:[{target:\"cairn:~/plan\",mode:\"create\",payload:{title:\"...\",content:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    CONTENT,
                    OLD_STRING,
                    NEW_STRING,
                    FIELD,
                    REPLACE_ALL,
                    CONFIRMED,
                    PR_ACTION,
                    PR_METHOD,
                ],
                label: "edit, confirm, or act on a PR artifact",
                example: "field merge: write({changes:[{target:\"cairn:~/plan\",mode:\"patch\",payload:{content:\"...\"}}]}) | text replacement: write({changes:[{target:\"cairn:~/plan\",mode:\"patch\",payload:{old_string:\"old\",new_string:\"new\",field:\"content\"}}]}) | confirm a gated artifact: write({changes:[{target:\"cairn:~/plan\",mode:\"patch\",payload:{confirmed:true}}]}) | merge a PR artifact: write({changes:[{target:\"cairn:~/pr\",mode:\"patch\",payload:{action:\"merge\",method:\"squash\"}}]})",
            },
        ],
    };

pub(crate) const NODE_CHANGED_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeChanged,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/changed",
    name: "Node changed files",
    description: "Files changed by a specific execution node",
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

pub(crate) const NODE_TERMINAL_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeTerminal,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/terminal/{slug}",
        name: "Node terminal",
        description: "Execution node terminal output",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[COMMAND],
                optional: &[DESCRIPTION, WAKE],
                label: "start terminal",
                example: "write({changes:[{target:\"cairn:~/terminal/SLUG\",mode:\"create\",payload:{command:\"...\",wake:\"ready\"}}]})",
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

pub(crate) const NODE_BROWSER_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeBrowser,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/browser/{slug}",
        name: "Node browser",
        description: "Execution node shared browser pane (native webview). cairn:~/browser is the default shared session (slug optional; add /SLUG for additional browsers). replace = go to a URL (sets the page; url required). create = open/ensure the pane (url optional). patch = drive it: navigate (url/navigate) or history/interaction (action: back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad; click/type/scroll take a selector, visible text, or a ?interactive handle). delete = close it. create/replace/patch are an idempotent ensure — they reuse the open pane (reopening it if closed) and never error on an existing slug. Read returns the live url/title/status plus current page content (paged via ?offset/?limit); ?screenshot for a native PNG, ?console/?network for captured runtime buffers, ?interactive for actionable elements as durable handles (e1..eN). Add ?return_content=true to a write to get the post-action page inline. The user sees and can drive the same session, which self-heals across an app restart.",
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
