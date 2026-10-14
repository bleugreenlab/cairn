//! One canonical sample of every [`CairnResource`] variant, and what parsing
//! its own canonical URI owes it.
//!
//! `to_uri` and `parse_uri` are two halves of one grammar, and nothing but a
//! test holds them together. A hand-written list of families cannot fail when a
//! case is MISSING, so for fifteen days `build.rs` rendered
//! `procedures/<name>` that `parse.rs` read back as an anonymous thread path,
//! and every procedure invocation and scheduled automation silently died
//! (CAIRN-4725). The list had no arm for it because nobody wrote one.
//!
//! This table is exhaustive BY CONSTRUCTION: the macro that builds it also
//! emits a `match` over every variant, so adding a `CairnResource` without a
//! sample here fails to compile, naming the variant it is missing. What each
//! sample asserts is [`RoundTrip`].

use super::types::{AttachmentRef, AttachmentScopeRef, CairnResource};

/// Who reads a variant's canonical URI back into the variant.
///
/// Both halves are round-trip guarantees; they differ only in how many readers
/// the trip goes through, and that difference is the deliberate ownership
/// boundary between the two crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundTrip {
    /// The common parser is the whole reader: `parse_uri(x.to_uri()) == x`.
    Parser,
    /// A thread sub-resource. The common parser deliberately stops every thread
    /// URI at `CairnResource::Thread { path }` and hands the path on intact;
    /// `cairn-core`'s `delegate_thread_descendant` is the single reader that
    /// names the variant. So the guarantee is
    /// `delegate(parse_uri(x.to_uri())) == x`, and cairn-core's
    /// `every_resource_variant_survives_its_own_uri` is where it is pinned.
    ///
    /// One resolver for thread sub-paths: a `procedures` or `db` arm added to
    /// the common parser would be a second implementation of what a thread
    /// sub-path means, free to drift from the one read and write dispatch use.
    ThreadResolver,
}

/// A variant, an instance of it, and the round-trip contract it answers to.
#[derive(Debug, Clone)]
pub struct ResourceSample {
    pub resource: CairnResource,
    pub round_trip: RoundTrip,
}

macro_rules! resource_samples {
    ($($variant:ident { $($field:ident : $value:expr),* $(,)? } => $round_trip:ident),+ $(,)?) => {
        /// Every `CairnResource` variant, once, with a canonical instance.
        ///
        /// Total over the enum: see the compile-time guard below.
        pub fn every_resource_variant() -> Vec<ResourceSample> {
            vec![$(
                ResourceSample {
                    resource: CairnResource::$variant { $($field: $value),* },
                    round_trip: RoundTrip::$round_trip,
                }
            ),+]
        }

        /// The exhaustiveness guard. It is never called; its only job is to be
        /// a `match` over every variant the table names, so a variant the table
        /// omits is a non-exhaustive-patterns error pointing straight at the
        /// list above.
        #[allow(dead_code)]
        fn every_variant_is_sampled(resource: &CairnResource) {
            match resource {
                $(CairnResource::$variant { .. } => ()),+
            }
        }
    };
}

fn project() -> String {
    "cairn".to_string()
}

fn thread() -> String {
    "design-review".to_string()
}

fn node() -> String {
    "builder".to_string()
}

fn task() -> String {
    "review-rust".to_string()
}

/// A content-store key, in the 64-hex shape the permalink form validates.
fn content_hash() -> String {
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string()
}

resource_samples! {
    // ---- Project scope ----
    Project { project: project() } => Parser,
    Projects {} => Parser,
    ProjectSettings { project: project() } => Parser,
    ProjectIssues { project: project() } => Parser,
    ProjectThreads { project: project() } => Parser,
    ProjectMessages { project: project() } => Parser,
    ProjectPosts { project: project() } => Parser,
    ProjectCodemap { project: project() } => Parser,
    ProjectMap { project: project() } => Parser,
    ProjectHome { project: project() } => Parser,
    ProjectCheckResults { project: project(), revision: "main".to_string() } => Parser,
    ProjectCheckObservation { project: project(), handle: "rust-tests@main".to_string() } => Parser,
    ProjectTerminal { project: project(), slug: "build".to_string() } => Parser,
    ProjectBrowser { project: project(), slug: "default".to_string() } => Parser,
    ProjectBrowserNetworkRequest {
        project: project(),
        slug: "default".to_string(),
        request_id: "req-1".to_string(),
    } => Parser,
    ProjectSymbols { project: project(), symbol: Some("build_widget".to_string()) } => Parser,
    ProjectReferences { project: project() } => Parser,
    ProjectReference { project: project(), name: "ast-grep".to_string() } => Parser,

    // ---- Attachments: both address forms, both ordinal scopes ----
    ProjectImages { project: project(), scope: AttachmentScopeRef::Issue(42) } => Parser,
    ProjectImage { project: project(), reference: AttachmentRef::Hash(content_hash()) } => Parser,
    ProjectAttachments { project: project(), scope: AttachmentScopeRef::Thread(thread()) } => Parser,
    ProjectAttachment {
        project: project(),
        reference: AttachmentRef::Scoped { scope: AttachmentScopeRef::Project, ordinal: 3 },
    } => Parser,

    // ---- Issues ----
    Issue { project: project(), number: 42 } => Parser,
    IssueMessages { project: project(), number: 42 } => Parser,
    IssueComments { project: project(), number: 42 } => Parser,
    IssueComment { project: project(), number: 42, comment_seq: 3 } => Parser,
    IssueExecutions { project: project(), number: 42 } => Parser,
    IssueExecution { project: project(), number: 42, exec_seq: 2 } => Parser,
    Changed { project: project(), number: 42 } => Parser,

    // ---- Threads ----
    // The thread itself is the common parser's; everything beneath it is the
    // core resolver's, and `Thread { path }` is the handoff both agree on.
    Thread { project: project(), name: thread(), path: Vec::new() } => Parser,
    ThreadBranches { project: project(), thread: thread() } => ThreadResolver,
    ThreadBranch { project: project(), thread: thread(), number: 3 } => ThreadResolver,
    ThreadDb { project: project(), name: thread() } => ThreadResolver,
    ThreadDbSchema { project: project(), name: thread() } => ThreadResolver,
    ThreadProcedures { project: project(), name: thread() } => ThreadResolver,
    ThreadProcedure {
        project: project(),
        name: thread(),
        procedure: "collect-fast-pass".to_string(),
    } => ThreadResolver,
    ThreadAutomations { project: project(), name: thread() } => ThreadResolver,
    ThreadAutomation { project: project(), name: thread(), id: "auto-1".to_string() } => ThreadResolver,

    // ---- Node jobs ----
    Node { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeChat { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeChatRaw { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeChatTurn {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        turn_seq: 3,
    } => Parser,
    NodeChatEvent {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        run_seq: 1,
        event_seq: 7,
    } => Parser,
    NodeArtifact {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        name: Some("plan".to_string()),
    } => Parser,
    NodeDiff { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeRebase { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeMessages { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeProgress { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeTasks { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeCalls { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeWakes { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeChecks { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeQuestions { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeQuestion {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        segment: "q-1".to_string(),
    } => Parser,
    NodePermissions { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodePermission {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        segment: "perm-1".to_string(),
    } => Parser,
    NodeMemories { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeMemory {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        memory_seq: 4,
    } => Parser,
    NodeEras { project: project(), number: 42, exec_seq: 2, node_id: node() } => Parser,
    NodeEra {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        era_seq: 7,
    } => Parser,
    NodeSymbols {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        symbol: Some("build_widget".to_string()),
    } => Parser,
    NodeTerminal {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        slug: "smoke".to_string(),
    } => Parser,
    NodeRepl {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        slug: "analysis".to_string(),
    } => Parser,
    NodeBrowser {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        slug: "default".to_string(),
    } => Parser,
    NodeBrowserNetworkRequest {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        slug: "main".to_string(),
        request_id: "req-1".to_string(),
    } => Parser,

    // ---- Sub-agent task jobs ----
    Task { project: project(), number: 42, exec_seq: 2, node_id: node(), task_name: task() } => Parser,
    TaskChat { project: project(), number: 42, exec_seq: 2, node_id: node(), task_name: task() } => Parser,
    TaskChatRaw { project: project(), number: 42, exec_seq: 2, node_id: node(), task_name: task() } => Parser,
    TaskChatTurn {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        turn_seq: 2,
    } => Parser,
    TaskChatEvent {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        run_seq: 1,
        event_seq: 5,
    } => Parser,
    TaskArtifact {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        name: Some("return".to_string()),
    } => Parser,
    TaskMessages { project: project(), number: 42, exec_seq: 2, node_id: node(), task_name: task() } => Parser,
    TaskChecks { project: project(), number: 42, exec_seq: 2, node_id: node(), task_name: task() } => Parser,
    TaskPermissions { project: project(), number: 42, exec_seq: 2, node_id: node(), task_name: task() } => Parser,
    TaskPermission {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        segment: "perm-1".to_string(),
    } => Parser,
    TaskTerminal {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        slug: "build".to_string(),
    } => Parser,
    TaskBrowser {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        slug: "default".to_string(),
    } => Parser,
    TaskBrowserNetworkRequest {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: task(),
        slug: "main".to_string(),
        request_id: "req-1".to_string(),
    } => Parser,

    // ---- Job-shaped, addressed from either a node or a task ----
    JobTodos {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: Some(task()),
    } => Parser,
    HomeFeed {
        project: project(),
        number: 42,
        exec_seq: 2,
        node_id: node(),
        task_name: None,
    } => Parser,

    // ---- Contextual registries ----
    Packs {} => Parser,
    Pack { pack_id: "starter".to_string() } => Parser,
    Skills {} => Parser,
    Skill { skill_id: "testing".to_string(), path: vec!["SKILL.md".to_string()] } => Parser,
    ProjectSkills { project: project() } => Parser,
    ProjectSkill {
        project: project(),
        skill_id: "testing".to_string(),
        path: vec!["SKILL.md".to_string()],
    } => Parser,
    Recipes {} => Parser,
    Recipe { recipe_id: "build".to_string() } => Parser,
    ProjectRecipes { project: project() } => Parser,
    ProjectRecipe { project: project(), recipe_id: "build".to_string() } => Parser,
    Workflows {} => Parser,
    Workflow { workflow_id: "triage".to_string() } => Parser,
    ProjectWorkflows { project: project() } => Parser,
    ProjectWorkflow { project: project(), workflow_id: "triage".to_string() } => Parser,
    Routes {} => Parser,
    Route { route_id: "inbox".to_string() } => Parser,
    RouteHistory { route_id: "inbox".to_string() } => Parser,
    RouteHistoryEntry { route_id: "inbox".to_string(), seq: 5 } => Parser,
    ProjectRoutes { project: project() } => Parser,
    ProjectRoute { project: project(), route_id: "inbox".to_string() } => Parser,
    ProjectRouteHistory { project: project(), route_id: "inbox".to_string() } => Parser,
    ProjectRouteHistoryEntry {
        project: project(),
        route_id: "inbox".to_string(),
        seq: 5,
    } => Parser,
    ProjectEvents { project: project() } => Parser,
    ProjectEvent { project: project(), id: 4812 } => Parser,
    WorkspaceEvents {} => Parser,
    WorkspaceEvent { id: 4812 } => Parser,
    Responses {} => Parser,
    Response { response_id: "summarize".to_string() } => Parser,
    ResponseHistory { response_id: "summarize".to_string() } => Parser,
    ResponseHistoryEntry { response_id: "summarize".to_string(), seq: 5 } => Parser,
    ProjectResponses { project: project() } => Parser,
    ProjectResponse { project: project(), response_id: "summarize".to_string() } => Parser,
    ProjectResponseHistory { project: project(), response_id: "summarize".to_string() } => Parser,
    ProjectResponseHistoryEntry {
        project: project(),
        response_id: "summarize".to_string(),
        seq: 5,
    } => Parser,
    Agents {} => Parser,
    Agent { agent_id: "build".to_string() } => Parser,
    ProjectAgents { project: project() } => Parser,
    ProjectAgent { project: project(), agent_id: "build".to_string() } => Parser,
    Actions {} => Parser,
    Action { action_id: "deploy".to_string() } => Parser,
    ProjectActions { project: project() } => Parser,
    ProjectAction { project: project(), action_id: "deploy".to_string() } => Parser,
    Labels {} => Parser,
    Label { label_id: "bug".to_string() } => Parser,

    // ---- Workspace singletons and the fleet ----
    Posts {} => Parser,
    Post { id: 12 } => Parser,
    PostComment { post_id: 12, comment_id: 3 } => Parser,
    Settings {} => Parser,
    Db {} => Parser,
    Dev {} => Parser,
    DevDb {} => Parser,
    DevPid {} => Parser,
    Logs {} => Parser,
    ChannelsConversations {} => Parser,
    Executors {} => Parser,
    Executor { name: "mac-mini".to_string() } => Parser,
    ExecutorAction { name: "mac-mini".to_string(), action: "restart".to_string() } => Parser,
    Grants {} => Parser,
    Grant { id: "grant-1".to_string() } => Parser,
    Llm {} => Parser,
    LlmCatalog {} => Parser,
    LlmAccounts {} => Parser,
    LlmAccount { id: "acc_7b58857c-d1db".to_string() } => Parser,
    LlmUsage {} => Parser,
    LlmSessions {} => Parser,
    LlmSession { id: "954588b6-c77d-47e6-b466-b48841cc52cb".to_string() } => Parser,
    Bug {} => Parser,
    Help {} => Parser,
    WebSearch {} => Parser,
    Mcp { server: Some("turso".to_string()), resource: Some("execute_query".to_string()) } => Parser,
}
