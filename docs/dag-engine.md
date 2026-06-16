# Execution Engine

The execution engine turns recipe definitions into running multi-agent workflows. A recipe is a DAG of typed nodes connected by edges. When triggered, the recipe is snapshotted, jobs are created for executable nodes, and an advancement loop drives the workflow forward by checking dependencies and dispatching work.

## Concepts

**Recipe** — a static workflow template. Defines nodes (what to do) and edges (in what order, with what data). Stored as YAML configuration.

**Execution** — a runtime instance of a recipe, tied to a specific issue. Contains a frozen snapshot of the recipe and all referenced configuration (agents, skills, tools, trigger context). The snapshot is the execution's own copy — it can be extended during execution when delegated task packets materialize into ordinary nodes without affecting the recipe definition.

**Job** — a unit of work within an execution. Created for agent nodes. Tracks status, worktree assignment, and links to runs.

**Run** — a single Claude process invocation within a job. A job may have multiple runs (resume, follow-up messages). Each run produces a sequence of events stored in the database.

## Recipe Model

### Node Types

| Type | Purpose | Creates Job? | Execution |
|------|---------|:---:|-----------|
| **Trigger** | Entry point, provides execution context | No | Implicit — marks where the DAG starts |
| **Agent** | Runs a Claude session | Yes | Host spawns Claude subprocess |
| **Action** | Inline operation (create_pr, merge, etc.) | No | Executed inline by the effect loop |
| **Checkpoint** | Approval gate or programmatic validation | No | Blocks until approved or command exits 0 |
| **Condition** | Branching logic (expression or AI evaluation) | No | Evaluates inline, stores selected port |
| **Context** | Markdown content passed to downstream agents | No | Metadata only — resolved during input gathering |
| **Artifact** | Schema definition for expected output | No | Metadata only — used for schema resolution |
| **Executor** | Reads a TaskList artifact and expands into sub-DAG | Yes | Expansion creates new nodes and jobs |

### Edge Types

**Control edges** establish ordering: a node can't run until all its incoming control-edge sources are complete. These form the DAG structure that the advancement loop traverses.

**Context edges** establish data flow: when a node runs, the engine resolves its upstream context edges to gather input. The output of the source node (artifact, assistant message, or markdown content) is formatted and injected into the downstream agent's prompt.

### Node Configuration

Each node type has its own config struct. Key fields:

**AgentNodeConfig** — `agent_config_id` (which agent to use), `checkpoint` (optional approval gate after completion), `output_schema` (expected structured output), `git_config` (worktree mode: own branch, inherit parent's, or none).

**ActionNodeConfig** — `action` (builtin name like `create_pr`), `action_params` (runtime parameters), `input_schema`/`output_schema` for structured I/O, optional checkpoint.

**ConditionNodeConfig** — `condition_type` (programmatic expression or AI evaluation), `expression` or `question`, `ports` (output branch names like `["yes", "no"]`), `default_port` for error fallback.

**CheckpointNodeConfig** — `checkpoint_type` (approval or programmatic), `command` (shell command for programmatic — exit 0 means approved), `prompt` (message shown in UI).

## Snapshots

When an execution starts, `ExecutionSnapshot` captures everything needed to run the workflow:

```
ExecutionSnapshot
├── recipe: RecipeSnapshot (nodes + edges)
├── agents: HashMap<String, AgentSnapshot>
├── skills: HashMap<String, SkillSnapshot>
├── tools: HashMap<String, ToolSnapshot>
├── trigger_context: TriggerContext (issue_id, project_id, trigger_type, issue_skills)
└── created_at: timestamp
```

The snapshot is serialized as JSON and stored in the execution record. All subsequent operations — job creation, dependency checking, input resolution, and delegated task materialization — read from the snapshot, not from config files. This makes executions reproducible and immune to config changes mid-flight.

`SnapshotOverrides` allows editing the recipe or agents before execution starts, supporting per-execution customization.

## Job Creation

When an execution starts, `create_jobs_from_nodes()` converts recipe nodes into job records:

1. **Reachability analysis** — BFS from trigger nodes via control edges determines which nodes are reachable. Unreachable nodes (disconnected subgraphs) get no jobs.

2. **Node-to-job mapping** — only Agent and Executor nodes create jobs. Trigger, Context, and Artifact nodes are metadata. Action, Condition, and Checkpoint nodes execute inline during advancement (no persistent job record).

3. **Worktree assignment** — each agent job gets a worktree mode:
   - **Own** (default): new branch and worktree created at job preparation time
   - **Inherit**: uses parent agent's worktree path and branch
   - **None**: reads from main branch, no dedicated worktree

All jobs start in `pending` status.

## Advancement Loop

The core of the engine is `advance_execution_with_actions()`, which finds ready work and dispatches it. It runs whenever a job completes, an action finishes, or a condition evaluates.

### Finding Ready Jobs

`advance_execution_impl()` iterates pending jobs and checks readiness:

- **Control edges**: all incoming control-edge sources must be complete (or be trigger nodes, which are implicitly complete)
- **Condition edges**: the condition node must have evaluated, and its selected port must match the edge's source handle

Jobs that pass all checks transition from `pending` to `ready`.

### Handling Ready Jobs by Type

`advance_execution_with_actions()` categorizes ready jobs and handles each node type:

| Node Type | What Happens |
|-----------|-------------|
| **Agent** | Returned to the host for Claude session spawning |
| **Checkpoint (Approval)** | Job blocked, waits for user approval |
| **Checkpoint (Programmatic)** | Shell command executed; exit 0 → approved, else rejected. Results cached by commit hash |
| **Action** | Action run record created, executed inline by the effect loop, advancement re-triggered |
| **Condition** | Upstream artifacts loaded, expression evaluated (or AI called), selected port stored, advancement re-triggered |

Agent jobs are the only type returned to the caller — everything else resolves inline and re-enters the advancement loop.

### Completion Detection

After processing, the engine checks whether all jobs, actions, and conditions are complete. If nothing incomplete remains, the execution is marked complete and the issue status may transition.

## Input Resolution

Before an agent runs, `resolve_job_inputs()` gathers data from upstream context edges:

- **Trigger nodes**: execution context (project info, issue details)
- **Context nodes**: markdown content from the node's config
- **Agent nodes**: the latest artifact from the upstream job, keyed by source handle
- **Fallback**: if an upstream agent produced no artifact, the last assistant message is used

Resolved inputs are formatted as markdown sections and injected into the agent's prompt, giving it the context produced by earlier stages of the workflow.

## TaskList Delegation

TaskList workflows use a normal Agent node configured with the `executor` agent. An upstream agent produces a `TaskList` artifact — a structured list of tasks with dependencies. The executor agent:

1. **Loads the TaskList** from upstream context
2. **Validates and groups dependencies** — topological waves detect cycles and invalid dependency references
3. **Delegates a wave** with one blocking `cairn:~/tasks` batch append containing one task packet per task
4. **Materializes packets** through DAG advancement into ordinary trigger/context/agent nodes
5. **Runs child jobs** under the executor job; delegated children inherit the executor worktree path
6. **Integrates between waves** before delegating the next dependency tier
7. **Writes the PR artifact** after all waves are complete and verification passes

The TaskList structure:
```
TaskList
├── objective: String
├── requirements: Vec<String>
└── tasks: Vec<TaskListTask>
    ├── id, title, agent, prompt
    ├── dependencies: Vec<String>  (task IDs that must complete first)
    └── model: Option<String>
```

## Schema Resolution

Agents may need to produce structured output. The schema is resolved with a priority chain:

1. The node's own `output_schema` (from AgentNodeConfig)
2. A downstream action node's `input_schema` (via context edge)
3. A downstream artifact node's schema (via context edge)

Preset schemas (`plan`, `implementation`, `document`, `summary`) are bundled. Custom schemas use JSON Schema with field definitions.

## Job Lifecycle

```
pending → ready → running → complete
                          → failed
              → blocked   (checkpoint awaiting approval)
```

**Job preparation** (`prepare_job()`) handles the transition from ready to running: sets up the worktree (if needed), loads the agent config, resolves the output schema, creates a run record, and returns a `PreparedJob` with everything the host needs to spawn Claude.

**Job completion** (`on_job_complete_impl()`) triggers DAG advancement, which may make downstream jobs ready, evaluate conditions, or materialize delegated task packets — continuing the workflow.
