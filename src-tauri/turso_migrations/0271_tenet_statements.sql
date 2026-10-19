-- Tenets are yes/no statements (cairn/5113).
--
-- A thread proposes one statement of how the project does things, and the
-- operator answers yes, no, or `your_call` (approval that records the thread
-- was simply trusted). The question-with-options shape of 0261 retires, and
-- each row's statement is the text the operator was meant to read:
--
-- - A row with no verdict (proposed or withdrawn) keeps its question. Threads
--   wrote the full proposal there (what, why, source); option 1 was only a
--   label for the recommended choice.
-- - A row with a verdict keeps the option it adopted, falling back to
--   option 1; its other options are dropped.
--
-- Imported rulings carry one option equal to their question, so either rule
-- gives them the same text.
--
-- The verdict CHECK cannot be widened in place, so the table is rebuilt with
-- foreign keys off (`tenet_evidence` references it); every row is carried.

CREATE TABLE tenets_new (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  number INTEGER NOT NULL,
  proposing_thread_id TEXT NOT NULL REFERENCES threads(id),
  -- Set only on tenets migrated from a thread's rulings, so their old
  -- addresses keep resolving as an alias of the number.
  slug TEXT,
  -- "This is how we do things", written for the operator to answer yes or no.
  statement TEXT NOT NULL CHECK (length(trim(statement)) > 0),
  status TEXT NOT NULL CHECK (status IN ('proposed','approved','declined','withdrawn','superseded')),
  -- `yes` and `your_call` approve; `no` declines.
  verdict TEXT CHECK (verdict IN ('yes','no','your_call')),
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
  CHECK ((verdict IS NULL) = (status IN ('proposed','withdrawn')))
);

INSERT INTO tenets_new (
  id, project_id, number, proposing_thread_id, slug, statement, status, verdict,
  comment, answered_by_json, answer_source, answered_at, supersedes, origin,
  legacy_json, contradiction_alerted_at, proposer_principal_json, created_at,
  updated_at
)
SELECT
  id, project_id, number, proposing_thread_id, slug,
  CASE
    WHEN verdict IS NULL THEN question
    ELSE COALESCE(
      NULLIF(trim(json_extract(options_json, '$[' || COALESCE(selected_option, 0) || ']')), ''),
      question
    )
  END,
  status,
  CASE verdict WHEN 'option' THEN 'yes' WHEN 'none' THEN 'no' ELSE verdict END,
  comment, answered_by_json, answer_source, answered_at, supersedes, origin,
  legacy_json, contradiction_alerted_at, proposer_principal_json, created_at,
  updated_at
FROM tenets;

DROP TABLE tenets;
ALTER TABLE tenets_new RENAME TO tenets;
CREATE INDEX tenets_thread ON tenets(proposing_thread_id, status);
CREATE INDEX tenets_status ON tenets(project_id, status, answered_at);
