//! Workspace registry & misc resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const BUG_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Bug,
        uri_template: "cairn://bug",
        name: "Bug report",
        description: "Global bug report sink — append a report with payload.category, payload.title, payload.description, and optional payload.toolName",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[BUG_CATEGORY, TITLE, DESCRIPTION],
            optional: &[KeySpec::new("toolName", KeyType::Str, "related tool")],
            label: "submit bug report",
            example: "write({changes:[{target:\"cairn://bug\",mode:\"append\",payload:{category:\"...\",title:\"...\",description:\"...\"}}]})",
        }],
    };

pub(crate) const SKILLS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Skills,
        uri_template: "cairn://skills",
        name: "Skills",
        description: "Workspace and current-project skills with canonical URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
            optional: &[],
            label: "create skill",
            example: "write({changes:[{target:\"cairn://skills\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\"}}]})",
        }],
    };

pub(crate) const SKILL_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Skill,
        uri_template: "cairn://skills/{skill_id}",
        name: "Skill",
        description: "Skill SKILL.md content, plus references/scripts/assets package links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
                label: "patch skill",
                example: "write({changes:[{target:\"cairn://skills/ID\",mode:\"patch\",payload:{description:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[KeySpec::new("reason", KeyType::Str, "why it was removed")],
                label: "delete skill",
                example: "write({changes:[{target:\"cairn://skills/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_SKILLS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectSkills,
        uri_template: "cairn://p/{project}/skills",
        name: "Project skills",
        description: "Skills available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
            optional: &[],
            label: "create skill",
            example: "write({changes:[{target:\"cairn://p/PROJECT/skills\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\"}}]})",
        }],
    };

pub(crate) const PROJECT_SKILL_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectSkill,
        uri_template: "cairn://p/{project}/skills/{skill_id}",
        name: "Project skill",
        description: "Project-scoped skill SKILL.md content and package files",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[SKILL_NAME, DESCRIPTION, SKILL_PROMPT],
                label: "patch skill",
                example: "write({changes:[{target:\"cairn://p/PROJECT/skills/ID\",mode:\"patch\",payload:{description:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[KeySpec::new("reason", KeyType::Str, "why it was removed")],
                label: "delete skill",
                example: "write({changes:[{target:\"cairn://p/PROJECT/skills/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_REFERENCES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectReferences,
        uri_template: "cairn://p/{project}/references",
        name: "Project references",
        description: "Project-scoped external git repos and local directories configured in .cairn/config.yaml, with resolved status and member URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[REFERENCE_NAME],
            optional: &[REFERENCE_GIT, REFERENCE_PATH, DESCRIPTION, REFERENCE_BRANCH],
            label: "create project reference",
            example: "write({changes:[{target:\"cairn://p/PROJECT/references\",mode:\"create\",payload:{name:\"openpnp\",git:\"https://github.com/openpnp/openpnp.git\",description:\"OpenPnP source\"}}]})",
        }],
    };

pub(crate) const PROJECT_REFERENCE_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectReference,
        uri_template: "cairn://p/{project}/references/{name}",
        name: "Project reference",
        description: "A single project reference; patch editable source fields or delete it from project config without deleting the global clone. When switching between path and git sources, send null for the old source field so exactly one of git/path remains set.",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[REFERENCE_GIT, REFERENCE_PATH, DESCRIPTION, REFERENCE_BRANCH, REFERENCE_REFRESH],
                label: "patch project reference",
                example: "write({changes:[{target:\"cairn://p/PROJECT/references/openpnp\",mode:\"patch\",payload:{description:\"Updated description\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete project reference",
                example: "write({changes:[{target:\"cairn://p/PROJECT/references/openpnp\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const LABELS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Labels,
        uri_template: "cairn://labels",
        name: "Labels",
        description: "Workspace label vocabulary with canonical URI links and issue usage counts",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[LABEL_NAME],
            optional: &[LABEL_COLOR],
            label: "create label",
            example: "write({changes:[{target:\"cairn://labels\",mode:\"create\",payload:{name:\"Needs Review\",color:\"#6B8F71\"}}]})",
        }],
    };

pub(crate) const LABEL_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Label,
        uri_template: "cairn://labels/{label_id}",
        name: "Label",
        description: "A single workspace label; deleting it detaches it from all issues",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[LABEL_NAME, LABEL_COLOR],
                label: "patch label",
                example: "write({changes:[{target:\"cairn://labels/needs-review\",mode:\"patch\",payload:{name:\"Reviewed\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[],
                label: "delete label",
                example: "write({changes:[{target:\"cairn://labels/needs-review\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const NODE_SYMBOLS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeSymbols,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/symbols",
        name: "Node Symbols",
        description: "Structural code navigation over this node's worktree via the in-process ast-grep engine. Append a symbol (`/build_widget`) and pick an op with `?op=` (definition|references|callers|implementations); an absent op returns an overview (definition site, signature, and reference count). Scope with `?in=<glob>`. No language server, no index — files are parsed on demand.",
        read_projections: SYMBOLS_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const PROJECT_SYMBOLS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectSymbols,
        uri_template: "cairn://p/{project}/symbols",
        name: "Project Symbols",
        description: "Structural code navigation over the project's main checkout — the node-less fallback to the node-scoped symbols resource. Same ops and projections; append a symbol and pick an op with `?op=`.",
        read_projections: SYMBOLS_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: NO_MUTATIONS,
    };

pub(crate) const NODE_MEMORIES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::NodeMemories,
        uri_template: "cairn://p/{project}/{number}/{exec}/{node}/memories",
        name: "Node memories",
        description: "Draft intake ledger entries captured by a node; append creates a draft",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Append,
            required: &[CONTENT],
            optional: &[MEMORY_NAME, MEMORY_SCOPE],
            label: "append node memory",
            example: "write({changes:[{target:\"cairn:~/memories\",mode:\"append\",payload:{content:\"...\"}}]})",
        }],
    };

pub(crate) const NODE_MEMORY_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::NodeMemory,
    uri_template: "cairn://p/{project}/{number}/{exec}/{node}/memories/{memory_seq}",
    name: "Node memory",
    description: "A single node-captured draft/pending intake ledger entry",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: &[
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[],
            optional: &[CONTENT, MEMORY_STATUS],
            label: "patch node memory",
            example: r#"write({changes:[{target:"cairn:~/memories/1",mode:"patch",payload:{content:"...",status:"draft"}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Patch,
            required: &[MEMORY_ACTION, MEMORY_REASON],
            optional: &[MEMORY_NEW_SCOPE],
            label: "triage node memory",
            example: r#"write({changes:[{target:"cairn:~/memories/1",mode:"patch",payload:{action:"promote",reason:"..."}}]})"#,
        },
        MutationSpec {
            mode: ChangeMode::Delete,
            required: &[],
            optional: &[],
            label: "delete node memory",
            example: r#"write({changes:[{target:"cairn:~/memories/1",mode:"delete"}]})"#,
        },
    ],
};
