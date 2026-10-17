-- Tenets: the human checkpoint for threads.
--
-- A thread proposes a question with one to five options, option 1 being its
-- own recommendation; the operator answers with an option, `none`, or
-- `your_call` (which adopts option 1). The tenet's TEXT is derived as
-- options[selected_option] and its rejected alternatives as the other
-- options, so neither is stored twice.
--
-- `cairn-core/src/tenets` is the only writer of this table.

-- Numbers are allocated per project inside the same write as the INSERT, the
-- way `next_issue_number` is.
ALTER TABLE projects ADD COLUMN next_tenet_number INTEGER NOT NULL DEFAULT 1;
-- What a tenet gate releases on. Only `answered` is reachable today: no
-- verified authenticator path exists for `ratified`, so the policy setter
-- refuses it. Operator-only, and deliberately not in `.cairn/config.yaml`,
-- where a branch could ungate itself.
ALTER TABLE projects ADD COLUMN tenet_release TEXT NOT NULL DEFAULT 'answered'
  CHECK (tenet_release IN ('answered','ratified'));
-- The recipes whose filing by a thread requires a tenet. Operator-only, for
-- the same reason.
ALTER TABLE projects ADD COLUMN tenet_gated_recipes TEXT NOT NULL
  DEFAULT '["planbuild","coordinator","PlanCoord"]';

CREATE TABLE tenets (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  number INTEGER NOT NULL,
  proposing_thread_id TEXT NOT NULL REFERENCES threads(id),
  -- Set only on tenets migrated from a thread's rulings, so their old
  -- addresses keep resolving as an alias of the number.
  slug TEXT,
  question TEXT NOT NULL CHECK (length(trim(question)) > 0),
  -- JSON array of 1..5 strings; options[0] is the thread's recommendation.
  options_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('proposed','approved','declined','withdrawn','superseded')),
  verdict TEXT CHECK (verdict IN ('option','none','your_call')),
  -- 0-based; set for `option` and `your_call` (which adopts 0).
  selected_option INTEGER,
  comment TEXT,
  -- PrincipalRef JSON (Human or Machine); NULL when migrated.
  answered_by_json TEXT,
  answer_source TEXT CHECK (answer_source IN ('principal','device','migrated')),
  answered_at INTEGER,
  -- A tenet number in the same project, honored only when this one is
  -- approved.
  supersedes INTEGER,
  origin TEXT NOT NULL CHECK (origin IN ('filing-gate','standing','migrated-ruling')),
  -- A migrated ruling's rationale and provenance, verbatim.
  legacy_json TEXT,
  -- Edge state for the contradiction wake.
  contradiction_alerted_at INTEGER,
  proposer_principal_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (project_id, number),
  UNIQUE (project_id, slug),
  -- A verdict exists exactly when the tenet was answered. A superseded tenet
  -- was approved first, so it keeps its verdict.
  CHECK ((verdict IS NULL) = (status IN ('proposed','withdrawn'))),
  CHECK ((selected_option IS NULL) = (verdict IS NULL OR verdict = 'none'))
);
CREATE INDEX tenets_thread ON tenets(proposing_thread_id, status);
CREATE INDEX tenets_status ON tenets(project_id, status, answered_at);
