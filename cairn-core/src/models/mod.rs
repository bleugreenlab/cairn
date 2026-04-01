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
mod embedding;
mod execution;
mod files;
mod github;
mod issue;
mod job;
mod manager;
mod memory;
mod message;
mod permissions;
mod pr;
mod project;
mod provider_usage;
mod recipe;
mod recipe_file;
mod run;
mod search;
mod session;
mod skill;
mod snapshot;
mod toolkit;
mod trigger_event;
mod turn;
pub mod webhook;
mod workspace;

// Common enums
pub use common::{MergeType, Model, Preset, RuntimeExtras, ThinkingDisplayMode, ToolDetailLevel};

// Workspace and settings
pub use workspace::{Settings, UpdateSettings};

// Project types
pub use project::{CreateProject, Project, TerminalCommand, UpdateProject};

// Provider usage snapshot types
pub use provider_usage::{
    ProviderCreditsSnapshot, ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow,
};

// Issue types
pub use issue::{
    Comment, CommentSource, CreateComment, CreateIssue, Issue, IssueAttention, IssueProgress,
    IssueStatus, UpdateIssue,
};

// Job types (replaces timeline_nodes)
pub use job::{Job, JobStatus};

// Manager types
pub use manager::{CreateManager, Manager, ManagerScopeKind, ManagerStatus, UpdateManager};

// Chat types (project-level conversations)
pub use chat::Chat;

// Execution types (recipe instances)
pub use execution::{
    ConditionEvaluation, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionStatus, TriggerType, TriggeredExecution,
};

// Run types
pub use run::{Event, PermissionRequest, Prompt, Run, RunStartMode, RunStatus, RunTodos, TodoItem};

// Session types (durable conversation identity)
pub use session::{Session, SessionStatus};

// Turn types
pub use turn::{Turn, TurnStartReason, TurnState, TurnYieldReason};

// Recipe types
pub use recipe::{
    AccumulationScope, ActionNodeConfig, AgentFilter, AgentFilterMode, AgentGitConfig,
    AgentNodeConfig, ArtifactNodeConfig, CheckpointNodeConfig, CheckpointType,
    ConditionErrorBehavior, ConditionNodeConfig, ConditionType, ContextNodeConfig, CreateRecipe,
    EventFilter, NodeConfig, NodePosition, Recipe, RecipeEdge, RecipeEdgeType, RecipeNode,
    RecipeNodeType, RecipeTrigger, RecipeVersionInfo, ScheduleAt, ScheduleConfig, ScheduleEvery,
    ScheduleInterval, SchedulePeriod, SchemaConfig, TriggerConfig, TriggerScope, UpdateRecipe,
    WorktreeMode,
};

// Recipe file types (export/import)
pub use recipe_file::{RecipeFile, RecipeFileValidation};

// Artifact types
pub use artifact::{AnnotationInput, Artifact};

// PR types
pub use pr::{
    Check, CheckFailureDetails, CheckState, ChecksStatus, MergeableState, PrCache, PrDataSummary,
    PrState, ProjectPrEntry, ReviewDecision,
};

// Agent types
pub use agent::{
    AgentConfig, CreateAgentConfig, OutputSchema, OutputSchemaInfo, UpdateAgentConfig,
};

// Permission types
pub use permissions::{
    split_legacy_permission_mode, tool_policies_from_legacy_lists, tool_policies_to_legacy_lists,
    ApprovalPolicy, FilesystemScope, ToolPolicy,
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
    AgentSnapshot, DelegatedOutputContract, DelegatedOwnershipScope, DelegatedSessionMode,
    DelegatedSessionStrategy, DelegatedStatus, DelegatedWorkPacket, DelegationOrigin,
    ExecutionSnapshot, RecipeSnapshot, SkillSnapshot, SnapshotOverrides, SnapshotPresets,
    ToolSnapshot, TriggerContext,
};

// File browsing types
pub use files::{detect_language, BranchInfo, FileContent, RepoFile};

// Message types
pub use message::{ChannelType, Message};

// Embedding types
pub use embedding::EventEmbedding;

// Memory types
pub use memory::{
    CreateMemory, CreateMemoryTrigger, Memory, MemoryConfidence, MemoryTrigger, UpdateMemory,
};

// Trigger event types
pub use trigger_event::{JobEndedEvent, SkillCalledEvent, TriggerEvent};

// Webhook types
pub use webhook::WebhookEvent;

// Search types
pub use search::{SearchContentType, SearchFilters, SearchResult};

// GitHub status types
pub use github::{GitHubStatus, RelayStatus};
