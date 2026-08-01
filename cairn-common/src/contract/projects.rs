//! Project & workspace resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const PROJECT_IMAGE_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectImage,
    uri_template: "cairn://p/{project}/{issue}/images/{n}",
    name: "Project image",
    description: "Immutable stored image returned as one native image block; also addressable project-wide (cairn://p/{project}/images/{n}) and, for images stored before scoped addresses existed, by content hash",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_IMAGES_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectImages,
    uri_template: "cairn://p/{project}/{issue}/images",
    name: "Issue images",
    description: "Every image minted in this scope, in ordinal order. An image address ends in its ordinal here, so you may construct a sibling's URI directly (images/4) instead of looking it up; ordinals are stable and never reused. Also addressable project-wide (cairn://p/{project}/images)",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Project,
    uri_template: "cairn://p/{project}",
    name: "Project overview",
    description: "Project overview with recent issues and status",
    read_projections: &[
        ProjectionSpec {
            key: "search",
            values: "QUERY (full-text; AND across words, prefix-fuzzy last word, title-boosted, recency-tiebroken)",
        },
        ProjectionSpec {
            key: "content_types",
            values: "issue,comment,artifact,event,message (comma-separated)",
        },
        ProjectionSpec {
            key: "role",
            values: "assistant|user|tool (events) | user|agent (comments)",
        },
        ProjectionSpec {
            key: "in",
            values: "title (match the title field only)",
        },
        ProjectionSpec {
            key: "issue",
            values: "NUMBER (search within one issue's history)",
        },
        ProjectionSpec {
            key: "limit",
            values: "N",
        },
        ProjectionSpec {
            key: "since",
            values: "EPOCH",
        },
    ],
    related: PROJECT_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[MutationSpec {
        mode: ChangeMode::Patch,
        required: &[],
        optional: &[PROJECT_CREATE_NAME, PROJECT_HIDDEN, PROJECT_REMOTE_URL],
        label: "patch project (rename / hide / attach remote)",
        example:
            "write({changes:[{target:\"cairn://p/PROJECT\",mode:\"patch\",payload:{hidden:true}}]})",
    }],
};

pub(crate) const SETTINGS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Settings,
        uri_template: "cairn://settings",
        name: "Workspace settings",
        description: "The workspace-global settings document with every section: app prefs (branchPrefix, maxThinkingTokens, mergeType, pullOnMerge, orphanCleanupDays, repoTargetSweepDays, bugReports, thinkingDisplayMode, memoryReviewEnabled, pendingMemoryThreshold, externalReplies), backends (activeBackend/tiers/backends plus a read-only model catalog and usage), git identities, provider accounts, keybinds, build services, and read-only GitHub status. patch routes each present key to its existing store; GitHub is read-only and OAuth account-add stays UI-only.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                SETTINGS_BRANCH_PREFIX,
                SETTINGS_MERGE_TYPE,
                SETTINGS_MEMORY_REVIEW_ENABLED,
                SETTINGS_ACTIVE_BACKEND,
                SETTINGS_TIERS,
                SETTINGS_BACKENDS,
                SETTINGS_GIT_IDENTITIES,
                SETTINGS_ACCOUNTS,
                SETTINGS_KEYBINDS,
                SETTINGS_BUILD_SERVICES,
            ],
            label: "patch workspace settings",
            example: "write({changes:[{target:\"cairn://settings\",mode:\"patch\",payload:{branchPrefix:\"agent\",keybinds:{set:[{action:\"issue.create\",key:\"n\",modifiers:[\"meta\"]}]}}}]})",
        }],
    };

pub(crate) const PROJECTS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Projects,
        uri_template: "cairn://projects",
        name: "Projects",
        description: "All projects with canonical project URIs; create registers a new project from a local git repo path",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[PROJECT_KEY, PROJECT_CREATE_NAME, PROJECT_REPO_PATH],
            optional: &[PROJECT_DEFAULT_BRANCH, PROJECT_TEAM_ID],
            label: "create project",
            example: "write({changes:[{target:\"cairn://projects\",mode:\"create\",payload:{key:\"DEMO\",name:\"Demo\",repoPath:\"/abs/path/to/repo\"}}]})",
        }],
    };

pub(crate) const PROJECT_SETTINGS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectSettings,
        uri_template: "cairn://p/{project}/settings",
        name: "Project settings",
        description: "Project-scoped configuration: setup/terminal commands, worktree populate rules, default branch, per-project identity overrides, external references, and background-testing checks. patch routes each present key to its store (project-settings.yaml, the projects DB row, or the global references store).",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[
                PS_SETUP_COMMANDS,
                PS_TERMINAL_COMMANDS,
                PS_MATERIALIZATION_POPULATE,
                PROJECT_DEFAULT_BRANCH,
                PS_ACCOUNT_OVERRIDES,
                PS_REFERENCES,
                PS_CHECKS,
            ],
            label: "patch project settings",
            example: "write({changes:[{target:\"cairn://p/PROJECT/settings\",mode:\"patch\",payload:{setupCommands:[\"bun install\"]}}]})",
        }],
    };

pub(crate) const PROJECT_CHECK_RESULTS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectCheckResults,
    uri_template: "cairn://p/{project}/check-results/{commit}",
    name: "Project check results",
    description: "Immutable check observations for an exact commit, suite, and environment fingerprint (read-only)",
    read_projections: &[
        ProjectionSpec {
            key: "suite",
            values: "required check suite name",
        },
        ProjectionSpec {
            key: "environment",
            values: "required exact fingerprint, or current for the local runner",
        },
        ProjectionSpec {
            key: "status",
            values: "passed|failed|skipped — optional exact test-status filter",
        },
        ProjectionSpec {
            key: "name",
            values: "optional case-sensitive substring of the test name",
        },
        ProjectionSpec {
            key: "offset",
            values: "non-negative test-row offset (default 0)",
        },
        ProjectionSpec {
            key: "limit",
            values: "test rows per page (default 100, maximum 1000)",
        },
    ],
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_ISSUES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectIssues,
        uri_template: "cairn://p/{project}/issues",
        name: "Project issues",
        description: "Project issue collection with canonical issue URIs",
        read_projections: &[
            ProjectionSpec {
                key: "status",
                values: "backlog,active",
            },
            ProjectionSpec {
                key: "limit",
                values: "N issues (default 20)",
            },
            ProjectionSpec {
                key: "offset",
                values: "N issues to skip (paging)",
            },
            ProjectionSpec {
                key: "sort",
                values: "updated_desc|created_asc|created_desc|updated_asc",
            },
            ProjectionSpec {
                key: "ready",
                values: "true|false",
            },
            ProjectionSpec {
                key: "label",
                values: "<name|slug>",
            },
            ProjectionSpec {
                key: "labels",
                values: "a,b (AND)",
            },
        ],
        related: PROJECT_CHILD_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[TITLE],
            optional: &[DESCRIPTION, EXECUTION, PARENT, LABELS],
            label: "create issue",
            example: "write({changes:[{target:\"cairn://p/PROJECT/issues\",mode:\"append\",payload:{title:\"...\"}}]})",
        }],
    };

pub(crate) const PROJECT_MESSAGES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectMessages,
        uri_template: "cairn://p/{project}/messages",
        name: "Project messages",
        description: "Project-wide messages between agents",
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
        related: PROJECT_CHILD_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[],
            label: "append message",
            example: "write({changes:[{target:\"cairn://p/PROJECT/messages\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    };

pub(crate) const PROJECT_TERMINAL_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectTerminal,
        uri_template: "cairn://p/{project}/terminal/{slug}",
        name: "Project terminal",
        description: "Project-scoped terminal output",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[COMMAND],
                optional: &[DESCRIPTION, WAKE],
                label: "start terminal",
                example: "write({changes:[{target:\"cairn://p/PROJECT/terminal/SLUG\",mode:\"create\",payload:{command:\"...\",wake:\"exit\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Append,
                required: &[CONTENT],
                optional: &[SUBMIT],
                label: "send to terminal",
                example: "write({changes:[{target:\"cairn://p/PROJECT/terminal/SLUG\",mode:\"append\",payload:{content:\"rs\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "stop terminal",
                example: "write({changes:[{target:\"cairn://p/PROJECT/terminal/SLUG\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_BROWSER_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectBrowser,
        uri_template: "cairn://p/{project}/browser/{slug}",
        name: "Project browser",
        description: "Project-scoped shared browser pane (native webview). replace = go to a URL (sets the page; url required). create = open/ensure the pane (url optional). patch = drive it: navigate (url/navigate) or history/interaction (action: back|forward|reload|click|type|select|drag|scroll|waitFor|waitForNavigation|waitForLoad|clearData; click/type/select/scroll take a selector, visible text, or a ?interactive handle, and drag adds a toSelector/toText/toHandle destination). delete = close it. create/replace/patch are an idempotent ensure — they reuse the open pane (reopening it if closed) and never error on an existing slug. Read returns the live url/title/status plus current page content (paged via ?offset/?limit); ?screenshot for a native PNG, ?console/?network for captured runtime buffers, ?interactive for actionable elements as durable handles (e1..eN). Add ?return_content=true to a write to get the post-action page inline. The pane self-heals across an app restart.",
        read_projections: BROWSER_READ_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Create,
                required: &[],
                optional: &[BROWSER_URL],
                label: "open/ensure browser",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"create\",payload:{url:\"https://example.com\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Replace,
                required: &[BROWSER_URL],
                optional: &[],
                label: "go to a URL (set the page)",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"replace\",payload:{url:\"https://example.com\"}}]})",
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
                    BROWSER_TO_SELECTOR,
                    BROWSER_TO_TEXT,
                    BROWSER_TO_HANDLE,
                    BROWSER_MODE,
                    BROWSER_STEPS,
                    BROWSER_DELAY_MS,
                    BROWSER_TO,
                    BROWSER_BY,
                    BROWSER_TIMEOUT_MS,
                    BROWSER_KINDS,
                ],
                label: "navigate / drive browser",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"patch\",payload:{action:\"click\",text:\"Sign in\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "close browser",
                example: "write({changes:[{target:\"cairn://p/PROJECT/browser\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_BROWSER_NETWORK_REQUEST_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectBrowserNetworkRequest,
    uri_template: "cairn://p/{project}/browser/{slug}/network/{request_id}",
    name: "Project browser network request",
    description: "Sanitized detail for one stable browser network capture handle. Request and response bodies are bounded, binary and cross-origin bodies are explicitly omitted, and portable redirect data is aggregate-only. Handles live in the runner's bounded in-memory archive until browser close, runner restart, or eviction; an expired handle is reported explicitly.",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[],
};
