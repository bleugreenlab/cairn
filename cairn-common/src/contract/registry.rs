//! Recipe / agent / action registry resource contracts.
//!
//! Verbatim `ResourceContract` table entries, assembled into
//! `RESOURCE_CONTRACTS` by the module facade in table order.

use super::specs::*;
use super::types::*;

pub(crate) const RECIPES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Recipes,
        uri_template: "cairn://recipes",
        name: "Recipes",
        description: "Workspace and current-project recipes with valid ids for starting executions",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[RECIPE_CONTENT],
            optional: &[RECIPE_ID],
            label: "create recipe",
            example: "write({changes:[{target:\"cairn://recipes\",mode:\"create\",payload:{content:\"cairnVersion: 1\\nname: ...\\ntrigger: manual\\nnodes: []\\nedges: []\\n\"}}]})",
        }],
    };

pub(crate) const RECIPE_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Recipe,
        uri_template: "cairn://recipes/{recipe_id}",
        name: "Recipe",
        description: "A single recipe rendered as its full editable YAML source (cairnVersion, name, trigger, nodes, edges); patch it with a full content replace or a targeted old_string/new_string edit",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: RECIPE_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    RECIPE_CONTENT,
                    RECIPE_OLD_STRING,
                    RECIPE_NEW_STRING,
                    RECIPE_REPLACE_ALL,
                ],
                label: "edit recipe (full content replace or targeted text replacement)",
                example: "write({changes:[{target:\"cairn://recipes/ID\",mode:\"patch\",payload:{old_string:\"name: Old Name\",new_string:\"name: New Name\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete recipe",
                example: "write({changes:[{target:\"cairn://recipes/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_RECIPES_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectRecipes,
        uri_template: "cairn://p/{project}/recipes",
        name: "Project recipes",
        description: "Recipes available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[RECIPE_CONTENT],
            optional: &[RECIPE_ID],
            label: "create recipe",
            example: "write({changes:[{target:\"cairn://p/PROJECT/recipes\",mode:\"create\",payload:{content:\"cairnVersion: 1\\nname: ...\\n\"}}]})",
        }],
    };

pub(crate) const PROJECT_RECIPE_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectRecipe,
        uri_template: "cairn://p/{project}/recipes/{recipe_id}",
        name: "Project recipe",
        description: "A single project-scoped recipe rendered as its full editable YAML source; patch it with a full content replace or a targeted old_string/new_string edit",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: RECIPE_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    RECIPE_CONTENT,
                    RECIPE_OLD_STRING,
                    RECIPE_NEW_STRING,
                    RECIPE_REPLACE_ALL,
                ],
                label: "edit recipe (full content replace or targeted text replacement)",
                example: "write({changes:[{target:\"cairn://p/PROJECT/recipes/ID\",mode:\"patch\",payload:{old_string:\"name: Old Name\",new_string:\"name: New Name\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete recipe",
                example: "write({changes:[{target:\"cairn://p/PROJECT/recipes/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const WORKFLOWS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Workflows,
    uri_template: "cairn://workflows",
    name: "Workflows",
    description: "Workspace and current-project workflows: reusable Bun scripts that orchestrate ephemeral agent calls and deterministic code",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const WORKFLOW_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::Workflow,
    uri_template: "cairn://workflows/{workflow_id}",
    name: "Workflow",
    description: "A single workflow package's metadata: name, description, args JSON Schema, output schema, and script entry",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_WORKFLOWS_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectWorkflows,
    uri_template: "cairn://p/{project}/workflows",
    name: "Project workflows",
    description: "Workflows available in a project's context (workspace + project)",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const PROJECT_WORKFLOW_CONTRACT: ResourceContract = ResourceContract {
    kind: ResourceKind::ProjectWorkflow,
    uri_template: "cairn://p/{project}/workflows/{workflow_id}",
    name: "Project workflow",
    description: "A single project-scoped workflow package's metadata",
    read_projections: NO_PROJECTIONS,
    related: NO_RELATED,
    cross_actions: NO_CROSS_ACTIONS,
    mutations: NO_MUTATIONS,
};

pub(crate) const AGENTS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Agents,
        uri_template: "cairn://agents",
        name: "Agents",
        description: "Workspace and current-project agents with canonical URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[AGENT_NAME, DESCRIPTION, AGENT_PROMPT, AGENT_TOOLS],
            optional: &[
                AGENT_TIER,
                AGENT_BACKEND,
                AGENT_FENCE,
                AGENT_DISALLOWED,
                AGENT_SKILLS,
            ],
            label: "create agent",
            example: "write({changes:[{target:\"cairn://agents\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\",tools:[\"Read\"]}}]})",
        }],
    };

pub(crate) const AGENT_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Agent,
        uri_template: "cairn://agents/{agent_id}",
        name: "Agent",
        description: "A single agent: prompt, tools, tier, and behavior settings",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    AGENT_NAME,
                    DESCRIPTION,
                    AGENT_PROMPT,
                    AGENT_TOOLS,
                    AGENT_TIER,
                    AGENT_BACKEND,
                    AGENT_FENCE,
                    AGENT_DISALLOWED,
                    AGENT_SKILLS,
                ],
                label: "patch agent",
                example: "write({changes:[{target:\"cairn://agents/ID\",mode:\"patch\",payload:{prompt:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete agent",
                example: "write({changes:[{target:\"cairn://agents/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_AGENTS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectAgents,
        uri_template: "cairn://p/{project}/agents",
        name: "Project agents",
        description: "Agents available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[AGENT_NAME, DESCRIPTION, AGENT_PROMPT, AGENT_TOOLS],
            optional: &[
                AGENT_TIER,
                AGENT_BACKEND,
                AGENT_FENCE,
                AGENT_DISALLOWED,
                AGENT_SKILLS,
            ],
            label: "create agent",
            example: "write({changes:[{target:\"cairn://p/PROJECT/agents\",mode:\"create\",payload:{name:\"...\",description:\"...\",prompt:\"...\",tools:[\"Read\"]}}]})",
        }],
    };

pub(crate) const PROJECT_AGENT_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectAgent,
        uri_template: "cairn://p/{project}/agents/{agent_id}",
        name: "Project agent",
        description: "A single project-scoped agent: prompt, tools, tier, and behavior settings",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    AGENT_NAME,
                    DESCRIPTION,
                    AGENT_PROMPT,
                    AGENT_TOOLS,
                    AGENT_TIER,
                    AGENT_BACKEND,
                    AGENT_FENCE,
                    AGENT_DISALLOWED,
                    AGENT_SKILLS,
                ],
                label: "patch agent",
                example: "write({changes:[{target:\"cairn://p/PROJECT/agents/ID\",mode:\"patch\",payload:{prompt:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete agent",
                example: "write({changes:[{target:\"cairn://p/PROJECT/agents/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const ACTIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Actions,
        uri_template: "cairn://actions",
        name: "Actions",
        description: "Workspace and current-project action definitions with canonical URI links",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[ACTION_NAME, ACTION_COMMAND],
            optional: &[DESCRIPTION, ACTION_INPUT_SCHEMA, ACTION_OUTPUT_SCHEMA],
            label: "create action",
            example: "write({changes:[{target:\"cairn://actions\",mode:\"create\",payload:{name:\"...\",commandTemplate:\"gh pr create --title {{title:string}}\"}}]})",
        }],
    };

pub(crate) const ACTION_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::Action,
        uri_template: "cairn://actions/{action_id}",
        name: "Action",
        description: "A single action definition: command template and input/output schema (built-ins are read-only)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    ACTION_NAME,
                    DESCRIPTION,
                    ACTION_COMMAND,
                    ACTION_INPUT_SCHEMA,
                    ACTION_OUTPUT_SCHEMA,
                ],
                label: "patch action",
                example: "write({changes:[{target:\"cairn://actions/ID\",mode:\"patch\",payload:{commandTemplate:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete action",
                example: "write({changes:[{target:\"cairn://actions/ID\",mode:\"delete\"}]})",
            },
        ],
    };

pub(crate) const PROJECT_ACTIONS_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectActions,
        uri_template: "cairn://p/{project}/actions",
        name: "Project actions",
        description: "Action definitions available in a project's context (workspace + project)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[MutationSpec {
            mode: ChangeMode::Create,
            required: &[ACTION_NAME, ACTION_COMMAND],
            optional: &[DESCRIPTION, ACTION_INPUT_SCHEMA, ACTION_OUTPUT_SCHEMA],
            label: "create action",
            example: "write({changes:[{target:\"cairn://p/PROJECT/actions\",mode:\"create\",payload:{name:\"...\",commandTemplate:\"...\"}}]})",
        }],
    };

pub(crate) const PROJECT_ACTION_CONTRACT: ResourceContract =
    ResourceContract {
        kind: ResourceKind::ProjectAction,
        uri_template: "cairn://p/{project}/actions/{action_id}",
        name: "Project action",
        description: "A single project-scoped action definition (built-ins are read-only)",
        read_projections: NO_PROJECTIONS,
        related: NO_RELATED,
        cross_actions: NO_CROSS_ACTIONS,
        mutations: &[
            MutationSpec {
                mode: ChangeMode::Patch,
                required: &[],
                optional: &[
                    ACTION_NAME,
                    DESCRIPTION,
                    ACTION_COMMAND,
                    ACTION_INPUT_SCHEMA,
                    ACTION_OUTPUT_SCHEMA,
                ],
                label: "patch action",
                example: "write({changes:[{target:\"cairn://p/PROJECT/actions/ID\",mode:\"patch\",payload:{commandTemplate:\"...\"}}]})",
            },
            MutationSpec {
                mode: ChangeMode::Delete,
                required: &[],
                optional: &[DELETE_REASON],
                label: "delete action",
                example: "write({changes:[{target:\"cairn://p/PROJECT/actions/ID\",mode:\"delete\"}]})",
            },
        ],
    };
