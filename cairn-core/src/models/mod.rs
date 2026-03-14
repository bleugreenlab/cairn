//! Domain models for the Cairn application.
//!
//! This module contains all the domain types used across the application.
//! Types are organized by domain area.

mod action;
mod agent;
mod artifact;
mod chat;
mod common;
mod docs;
mod execution;
mod files;
mod github;
mod issue;
mod job;
mod memory;
mod message;
mod pr;
mod project;
mod recipe;
mod recipe_file;
mod run;
mod search;
mod skill;
mod snapshot;
mod toolkit;
pub mod webhook;
mod workspace;

// Common enums
pub use common::{MergeType, Model};

// Workspace and settings
pub use workspace::{Settings, UpdateSettings};

// Project types
pub use project::{CreateProject, Project, TerminalCommand, UpdateProject};

// Issue types
pub use issue::{
    Comment, CommentSource, CreateComment, CreateIssue, Issue, IssueStatus, UpdateIssue, WaitState,
};

// Job types (replaces timeline_nodes)
pub use job::{Job, JobStatus};

// Chat types (project-level conversations)
pub use chat::Chat;

// Execution types (recipe instances)
pub use execution::{
    ConditionEvaluation, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionStatus, TriggerType,
};

// Run types
pub use run::{Event, PermissionRequest, Prompt, Run, RunStatus, RunTodos, TodoItem};

// Recipe types
pub use recipe::{
    ActionNodeConfig, AgentGitConfig, AgentNodeConfig, ArtifactNodeConfig, CheckpointNodeConfig,
    CheckpointType, ConditionErrorBehavior, ConditionNodeConfig, ConditionType, ContextNodeConfig,
    CreateRecipe, NodeConfig, NodePosition, Recipe, RecipeContext, RecipeEdge, RecipeEdgeType,
    RecipeNode, RecipeNodeType, RecipeTrigger, RecipeVersionInfo, ScheduleAt, ScheduleConfig,
    ScheduleEvery, ScheduleInterval, SchedulePeriod, SchemaConfig, TriggerConfig, UpdateRecipe,
    WebhookFilters, WorktreeMode,
};

// Recipe file types (export/import)
pub use recipe_file::{RecipeFile, RecipeFileValidation};

// Artifact types
pub use artifact::{AnnotationInput, Artifact};

// PR types
pub use pr::{
    Check, CheckFailureDetails, CheckState, ChecksStatus, MergeableState, PrCache, PrDataSummary,
    PrState, ReviewDecision,
};

// Agent types
pub use agent::{
    AgentConfig, CreateAgentConfig, OutputSchema, OutputSchemaInfo, UpdateAgentConfig,
};

// Toolkit and MCP types
pub use toolkit::ALWAYS_DISALLOWED_TOOLS;

// Documentation types
pub use docs::{DocContent, DocFile, DocReference};

// Action config types
pub use action::{
    generate_input_schema, interpolate_template, parse_template, ActionConfig, ActionRun,
    CreateActionConfig, TemplateVariable, UpdateActionConfig,
};

// Skill config types
pub use skill::{CreateSkillConfig, SkillConfig, UpdateSkillConfig};

// Execution snapshot types
pub use snapshot::{
    AgentSnapshot, ExecutionSnapshot, RecipeSnapshot, SkillSnapshot, SnapshotOverrides,
    ToolSnapshot, TriggerContext,
};

// File browsing types
pub use files::{detect_language, BranchInfo, FileContent, RepoFile};

// Message types
pub use message::{ChannelType, Message};

// Memory types
pub use memory::{
    CreateMemory, CreateMemoryTrigger, Memory, MemoryConfidence, MemoryTrigger, UpdateMemory,
};

// Webhook types
pub use webhook::WebhookEvent;

// Search types
pub use search::{SearchContentType, SearchFilters, SearchResult};
