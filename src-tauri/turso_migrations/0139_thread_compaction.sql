-- Rolling compaction state for thread sessions (CAIRN-3388).
--
-- A thread is a durable, non-terminal session: it orchestrates children and
-- converses indefinitely, so the one-session memory horizon of the ordinary
-- cold-resume digest is backwards for it. Compaction replaces concluded history
-- with a table of contents whose every line is re-readable at a URI, and keeps
-- the authored arc plus a recency window verbatim.
--
-- Three tables, all MECHANICAL. Authored intent (the arc) is an artifact the
-- thread owns and is never copied here: the whole design rests on generation and
-- authorship staying separable, and a copy of the arc in this schema would be the
-- second representation that drifts.
--
-- Ranges are recorded as TURN BLOCK INDICES, not turn ids, because that is the
-- coordinate both the transcript renderer and the re-read address
-- (`.../chat?offset=I&limit=N`) actually use. `turns.sequence` restarts at 1 in
-- every rotated session (`MAX(sequence) WHERE session_id`), so a turn number is
-- ambiguous across the many sessions a thread accumulates, while block index is
-- the job's single append-only ordering. Turn ids ride along nullable for
-- forensics.

-- Eligibility, not action. A child reaching a terminal status makes the parent
-- turns that discussed it compactable; a TRIGGER decides when that is applied.
-- Keyed by (job, child) so re-delivery of the same terminal fact is a no-op, and
-- so a consumed mark can never be resurrected by a later duplicate.
CREATE TABLE thread_compaction_marks (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    child_issue_id TEXT NOT NULL,
    child_issue_uri TEXT NOT NULL,
    child_title TEXT NOT NULL,
    final_status TEXT NOT NULL,
    marked_at INTEGER NOT NULL,
    consumed_generation INTEGER,
    PRIMARY KEY (job_id, child_issue_id)
);

-- One row per composed compaction. `source_session_id` is the session that was
-- live when the seed was composed, and it is what says whether the compaction
-- ever took effect: a generation is APPLIED once the job's current session is no
-- longer that one, because rotation is the only thing that moves the pointer.
-- A generation still naming the current session is PENDING -- composed, but its
-- rotation never landed -- and is ignored, which keeps a failed rotation exactly
-- as retryable as if it had never been attempted. Deriving "did it apply" from
-- the pointer that defines the answer is what keeps a second bookkeeping write
-- from disagreeing with reality.
--
-- `source_bytes` / `candidate_bytes` are the P and p the trigger compared,
-- persisted so the 3x threshold can be calibrated against what actually happened
-- rather than re-argued from first principles.
CREATE TABLE thread_compactions (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    source_session_id TEXT NOT NULL,
    compacted_through_block INTEGER,
    recency_start_block INTEGER NOT NULL,
    source_bytes INTEGER NOT NULL,
    candidate_bytes INTEGER NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN ('expiry', 'ratio', 'manual')),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, generation)
);

-- The generated table of contents. Entries are carried forward generation to
-- generation so a chapter keeps one stable overview and address for the life of
-- the thread, and so the table stays directly queryable -- a later fork detector
-- measures TOC growth by counting rows here, with no transcript replay.
CREATE TABLE thread_compaction_entries (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    position INTEGER NOT NULL,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('child', 'interstitial')),
    overview TEXT NOT NULL,
    content_uri TEXT NOT NULL,
    child_issue_id TEXT,
    start_block INTEGER NOT NULL,
    end_block INTEGER NOT NULL,
    start_turn_id TEXT,
    end_turn_id TEXT,
    PRIMARY KEY (job_id, generation, position)
);
