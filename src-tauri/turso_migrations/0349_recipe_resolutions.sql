-- lineage: shared
-- cairn/5814: how each execution create resolved its recipe, and every live
-- edit to a step's allotment afterwards. These are the labels the judgment
-- thread measures a recipe chooser on, so they are durable and joinable:
-- `recipe_resolutions.execution_id` to `jobs.execution_id` (and so to each
-- step's `jobs.selection`), and a re-run to the execution it superseded.
--
-- No CHECK on the enum-like columns (`source`, `created_by_kind`): widening a
-- CHECK on a synced table is a create-copy-drop rebuild, which breaks Turso
-- sync triggers. Rust writes them from closed enums.
--
-- Shared: all three describe ProjectScoped executions and are read wherever
-- the execution is.

-- The recipes a creator could have started, as the chooser saw them, written
-- once per distinct set. Content-addressed per project (a project's own
-- recipes are part of its set), so a resolution row stays small and still
-- names an exact, replayable question.
CREATE TABLE IF NOT EXISTS recipe_option_sets (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    -- sha256 of `candidates` as stored.
    candidates_hash TEXT NOT NULL,
    -- JSON [{id, name, description}], sorted by id.
    candidates TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, candidates_hash)
);

CREATE TABLE IF NOT EXISTS recipe_resolutions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    execution_id TEXT NOT NULL UNIQUE REFERENCES executions(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    -- Who filed it: the calling node or thread URI, NULL for the CLI and for
    -- machinery. `created_by_kind` is cli | thread | agent |
    -- route_dispatcher | system: a person naming a recipe is a stronger label
    -- than a coordinator passing through its default.
    created_by TEXT,
    created_by_kind TEXT NOT NULL,
    -- What ran: named | chosen | workspace_default | default_after_abstain.
    source TEXT NOT NULL,
    resolved_recipe TEXT NOT NULL,
    -- What decided it: 'named by creator', 'defaultRecipe setting', or the
    -- chooser's own answerer (`decide:<model>@<question-version>`).
    measured_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    confidence REAL,
    abstain_reason TEXT,
    -- What the installed chooser said, on every row a chooser was installed
    -- for, named ones included, whether or not it decided. NULL while none is
    -- installed. An override is a row where `chooser_recipe` differs from
    -- `resolved_recipe`; an abstain leaves `chooser_recipe` NULL with its
    -- reason.
    chooser_recipe TEXT,
    chooser_confidence REAL,
    chooser_reason TEXT,
    chooser_measured_by TEXT,
    -- recipe_option_sets(project_id, candidates_hash).
    candidates_hash TEXT NOT NULL,
    -- The `overrides` the creator filed, as filed (JSON), or NULL.
    overrides TEXT,
    -- JSON [{node, allotment, pinned, model, provenance, dependsOn, stored,
    -- error}]: what each step would pick at filing. A prediction, not a lock:
    -- each step selects at its own launch and stores that on its job.
    preview TEXT NOT NULL,
    -- The canonical execution this create superseded: a re-run. Comparing the
    -- two rows' recipe and overrides says whether it re-ran on another recipe
    -- or on bigger models.
    supersedes_execution_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_recipe_resolutions_issue ON recipe_resolutions(issue_id);
CREATE INDEX IF NOT EXISTS idx_recipe_resolutions_supersedes
    ON recipe_resolutions(supersedes_execution_id);

-- A live edit to one agent's snapshot in a running or held execution, through
-- the one door both the UI and the execution resource use. Only edits that
-- changed something are recorded.
CREATE TABLE IF NOT EXISTS execution_agent_edits (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    -- The editing node or thread URI, 'operator' for the app, NULL for the CLI.
    actor TEXT,
    -- JSON array of the snapshot keys that changed.
    changed TEXT NOT NULL,
    -- JSON {models, selection} before and after.
    before TEXT NOT NULL,
    after TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_execution_agent_edits_execution
    ON execution_agent_edits(execution_id);
