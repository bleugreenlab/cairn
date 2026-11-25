-- lineage: private
-- Re-verifying a profile onto its template's current engine pin (cairn/7591).
--
-- A re-verify writes a candidate profile under its own id: the profile with
-- only its engine replaced by the pin. Only the re-verify's own verification
-- ever runs the candidate; the promotion gate reads this table to keep it out
-- of placement, loads and offers, whatever state its row is in. A pass moves
-- the profile onto the candidate's engine and removes the candidate in one
-- transaction; anything else removes the candidate and leaves the profile as
-- it was.
--
-- `profile_digest` is the profile as it was when the re-verify began: the move
-- lands only while it is still exactly that. `candidate_digest` and
-- `surface_digest` are the candidate as written. A governed (generated)
-- profile's row is also the operator's approval of exactly that runnable
-- surface (`approved_by`, `approved_at`), the engine change shown as
-- `from_engine` -> `to_engine`; it binds the one verification it admits
-- (`verification_id`), and its pass re-promotes the moved profile in the same
-- transaction as the move. Nothing here reopens or speaks for the generation
-- that first wrote the profile.
CREATE TABLE inference_pin_moves (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL UNIQUE,
    pool_id TEXT NOT NULL,
    template_id TEXT NOT NULL,
    from_engine TEXT NOT NULL,
    to_engine TEXT NOT NULL,
    profile_digest TEXT NOT NULL,
    candidate_digest TEXT NOT NULL,
    surface_digest TEXT NOT NULL,
    governed INTEGER NOT NULL CHECK (governed IN (0, 1)),
    approved_by TEXT,
    approved_at INTEGER,
    verification_id TEXT,
    state TEXT NOT NULL,
    reason TEXT,
    started_at INTEGER NOT NULL,
    finished_at INTEGER
);

-- At most one re-verify of a profile is under way: two starts that each read
-- the profile idle conflict here instead of both committing.
CREATE UNIQUE INDEX idx_inference_pin_moves_verifying
    ON inference_pin_moves (profile_id) WHERE state = 'verifying';
