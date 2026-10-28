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
mod execution;
mod files;
mod github;
mod identity;
mod issue;
pub use identity::GitIdentity;
mod job;
mod label;
mod launch_deltas;
mod memory;
mod message;
mod permissions;
mod post;
mod pr;
mod project;
mod recipe;
mod recipe_file;
mod run;
mod search;
mod session;
mod skill;
mod snapshot;
mod thread;
mod trigger_event;
mod turn;
mod workflow;
mod workspace;

// Common enums
pub use common::{
    MergeType, Model, ModelSelection, Preset, PresetOptionValue, RuntimeExtras,
    ThinkingDisplayMode, ToolDetailLevel, TranscriptDensity, TranscriptTextSize,
};

// Workspace and settings
pub use workspace::{
    ChannelInboundCapabilities, ChannelsConfig, DiscordChannelConfig, ExternalReplyMode,
    IMessageChannelConfig, MessageClassPolicy, OpenRouterRouting, OpenRouterSort, Settings,
    TelegramChannelConfig, UpdateSettings,
};

// Project types
pub use project::{
    read_declared_check, CheckCommand, CheckPolicy, CheckResourceClass, CheckScopeSelector,
    CheckTree, CheckVocabulary, CheckWhen, CreateProject, DeclaredCheck, HumanBaseMergePolicy,
    HumanBaseMergePolicyError, HumanBaseMergePolicyReadState, HumanBaseMergePolicyState,
    MainCadence, MoveProject, PopulateConfig, Project, ProjectRemoteStatus, TerminalCommand,
    UnsupportedCheck, UpdateProject, VERDICT_PLATFORMS,
};

// Context token snapshot types
pub use context_tokens::{ContextTokenEventSnapshot, ContextTokenState};

// Issue types
pub use issue::{
    Comment, CommentSource, CreateComment, CreateIssue, Issue, IssueAttention, IssueProgress,
    IssueStatus, SidebarActiveIssue, UpdateIssue,
};
pub use label::{CreateLabel, Label, UpdateLabel};
pub use post::{CreatePost, CreatePostComment, Post, PostComment};

// Thread types
pub use thread::{CreateThread, Thread, ThreadStatus, UpdateThread};

// Job types (replaces timeline_nodes)
pub use job::{Job, JobStatus};

// Execution types (recipe instances)
pub use execution::{
    ConditionEvaluation, ConditionOutcome, Execution, ExecutionDetail, ExecutionFilters,
    ExecutionGate, ExecutionGateCensus, ExecutionLifecycle, ExecutionListItem, ExecutionListResult,
    ExecutionStatus, GateCensusEntry, GateEvidenceState, HeldExecution, Initiator, ReleaseOverride,
    TriggerType, TriggeredExecution,
};

// Run types
pub use run::{
    Event, PermissionRequest, PermissionStatus, Prompt, ReadSegmentTokens, ResolutionReceipt, Run,
    RunStartMode, RunStatus, RunTodos, TodoItem,
};

// Session types (durable conversation identity)
pub use session::{Session, SessionStatus};

// Turn types
pub use turn::{Turn, TurnEndReason, TurnStartReason, TurnState, TurnYieldReason};

// Recipe types
pub use recipe::{
    default_branch_targets, is_legacy_ai_condition, legacy_port_handle,
    normalize_legacy_port_handles, AccumulationScope, ActionNodeConfig, AgentFilter,
    AgentFilterMode, AgentGitConfig, AgentNodeConfig, ArtifactNodeConfig, BranchMode, BranchTarget,
    CheckpointNodeConfig, ConditionErrorBehavior, ConditionNodeConfig, ConfirmPolicy,
    ContextNodeConfig, Continuity, CreateRecipe, EventFilter, NodeConfig, NodePosition, Recipe,
    RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType, RecipeTrigger, RecipeVersionInfo,
    ScheduleAt, ScheduleConfig, ScheduleEvery, ScheduleInterval, SchedulePeriod, SchemaConfig,
    TriggerConfig, TriggerScope, UpdateRecipe,
};
pub use recipe::{CONTEXT_IN_HANDLE, CONTEXT_OUT_HANDLE, CONTEXT_REF_HANDLE, CONTEXT_SELF_HANDLE};

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

// Documentation types
pub use docs::{DocContent, DocFile, DocReference};

// Action config types
pub use action::{
    generate_input_schema, interpolate_template, parse_template, ActionConfig, ActionRun,
    ActionRunStatus, CreateActionConfig, TemplateVariable, UpdateActionConfig,
};

// Skill config types
pub use skill::{CreateSkillConfig, SkillConfig, UpdateSkillConfig};

// Workflow config types (settings surface)
pub use workflow::{SaveWorkflowConfig, WorkflowConfig, WorkflowConfigDetail};

// Execution snapshot types
pub use snapshot::{
    merge_agent_patch, AgentSnapshot, DelegatedOutputContract, DelegatedOwnershipScope,
    DelegatedSessionMode, DelegatedSessionStrategy, DelegatedStatus, DelegatedWorkPacket,
    DelegationOrigin, ExecutionSnapshot, ModelRouting, ModelRoutingDecision, ModelRoutingSource,
    RecipeSnapshot, SkillSnapshot, SnapshotOverrides, SnapshotPresets, TriggerContext,
};

// Launch-time snapshot deltas (the agent-side grammar for SnapshotOverrides)
pub use launch_deltas::{LaunchCustomization, LaunchDeltas};

// File browsing types
pub use files::{detect_language, BranchInfo, FileContent};

// Message types
pub use message::{ChannelType, DeliveryUrgency, Message};

// Embedding types

// Memory types
pub use memory::{
    CreateMemory, Memory, MemoryScope, MemoryStatus, MemoryTriageDecision, UpdateMemory,
};

// Trigger event types
pub use trigger_event::{JobEndedEvent, SkillCalledEvent, TriggerEvent};

// Webhook types

// Search types
pub use search::{SearchContentType, SearchFilters, SearchResult};

// GitHub status types
pub use github::{EventDeliveryStatus, GitHubAppAuthorizationStatus, GitHubStatus};

pub mod decision;
pub mod question;
