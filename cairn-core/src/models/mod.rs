//! Domain models for the Cairn application.
//!
//! This module contains all the domain types used across the application.
//! Types are organized by domain area.

mod action;
mod agent;
mod artifact;
mod common;
mod context_tokens;
mod docs;
mod embedding;
mod execution;
mod files;
mod github;
mod issue;
mod job;
mod label;
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
pub use common::{
    MergeType, Model, ModelSelection, Preset, PresetOptionValue, RuntimeExtras,
    ThinkingDisplayMode, ToolDetailLevel,
};

// Workspace and settings
pub use workspace::{ExternalReplyMode, Settings, UpdateSettings};

// Project types
pub use project::{CreateProject, Project, ProjectRemoteStatus, TerminalCommand, UpdateProject};

// Context token snapshot types
pub(crate) use context_tokens::get_latest_context_token_event;
pub use context_tokens::ContextTokenState;

// Provider usage snapshot types
pub use provider_usage::{
    ProviderCreditsSnapshot, ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow,
};

// Issue types
pub use issue::{
    Comment, CommentSource, CreateComment, CreateIssue, Issue, IssueAttention, IssueProgress,
    IssueStatus, UpdateIssue,
};
pub use label::{CreateLabel, Label, UpdateLabel};

// Job types (replaces timeline_nodes)
pub use job::{Job, JobStatus, NodeAttempt};

// Execution types (recipe instances)
pub use execution::{
    ConditionEvaluation, Execution, ExecutionDetail, ExecutionFilters, ExecutionListItem,
    ExecutionListResult, ExecutionStatus, TriggerType, TriggeredExecution,
};

// Run types
pub use run::{
    Event, PermissionRequest, PermissionStatus, Prompt, Run, RunStartMode, RunStatus, RunTodos,
    TodoItem,
};

// Session types (durable conversation identity)
pub use session::{Session, SessionStatus};

// Turn types
pub use turn::{Turn, TurnStartReason, TurnState, TurnYieldReason};

// Recipe types
pub use recipe::{
    AccumulationScope, ActionNodeConfig, AgentFilter, AgentFilterMode, AgentGitConfig,
    AgentNodeConfig, ArtifactNodeConfig, CheckpointNodeConfig, ConditionErrorBehavior,
    ConditionNodeConfig, ConditionType, ConfirmPolicy, ContextNodeConfig, CreateRecipe,
    EventFilter, NodeConfig, NodePosition, Recipe, RecipeEdge, RecipeEdgeType, RecipeNode,
    RecipeNodeType, RecipeTrigger, RecipeVersionInfo, ScheduleAt, ScheduleConfig, ScheduleEvery,
    ScheduleInterval, SchedulePeriod, SchemaConfig, TriggerConfig, TriggerScope, UpdateRecipe,
    WorktreeMode,
};

// Recipe file types (export/import)
pub use recipe_file::{RecipeFile, RecipeFileValidation};

// Artifact types
pub use artifact::Artifact;

// PR types
pub use pr::{
    Check, CheckFailureDetails, CheckState, ChecksStatus, MergeableState, PrCache, PrDataSummary,
    PrState, ReviewDecision,
};

// Agent types
pub use agent::{
    AgentConfig, CreateAgentConfig, OutputSchema, OutputSchemaInfo, UpdateAgentConfig,
};

// Permission types
pub use permissions::{Fence, LegacyOnEscape, LegacySandbox};

// Toolkit and MCP types
pub use toolkit::{ALL_NATIVE_TOOLS, ALWAYS_DISALLOWED_TOOLS};

// Documentation types
pub use docs::{DocContent, DocFile, DocReference};

// Action config types
pub use action::{
    generate_input_schema, interpolate_template, parse_template, ActionConfig, ActionRun,
    ActionRunStatus, CreateActionConfig, TemplateVariable, UpdateActionConfig,
};

// Skill config types
pub use skill::{CreateSkillConfig, SkillConfig, UpdateSkillConfig};

// Execution snapshot types
pub use snapshot::{
    AgentSnapshot, DelegatedOutputContract, DelegatedOwnershipScope, DelegatedSessionMode,
    DelegatedSessionStrategy, DelegatedStatus, DelegatedWorkPacket, DelegationOrigin,
    ExecutionSnapshot, RecipeSnapshot, SkillSnapshot, SnapshotOverrides, SnapshotPresets,
    TriggerContext,
};

// File browsing types
pub use files::{detect_language, BranchInfo, FileContent};

// Message types
pub use message::{ChannelType, Message};

// Embedding types
pub use embedding::EventEmbedding;

// Memory types
pub use memory::{
    CreateMemory, Memory, MemoryScope, MemoryStatus, MemoryTriageDecision, UpdateMemory,
};

// Trigger event types
pub use trigger_event::{JobEndedEvent, SkillCalledEvent, TriggerEvent};

// Webhook types
pub use webhook::WebhookEvent;

// Search types
pub use search::{SearchContentType, SearchFilters, SearchResult};

// GitHub status types
pub use github::{GitHubStatus, RelayStatus};
