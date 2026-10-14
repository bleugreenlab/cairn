-- Durable recovery floors for contended identifiers whose branch contribution
-- can be silently absorbed by a base advance. This is runner-local jj state:
-- store_path identifies a store owned by this installation and must never sync.
CREATE TABLE contended_identifier_obligations (
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    store_path TEXT NOT NULL,
    bookmark TEXT NOT NULL,
    identifier TEXT NOT NULL,
    delta INTEGER NOT NULL CHECK (delta >= 0),
    status TEXT NOT NULL DEFAULT 'outstanding'
        CHECK (status IN ('outstanding', 'discharged', 'orphaned')),
    basis_fork_value INTEGER NOT NULL CHECK (basis_fork_value >= 0),
    basis_branch_value INTEGER NOT NULL CHECK (basis_branch_value >= 0),
    basis_base_value INTEGER NOT NULL CHECK (basis_base_value >= 0),
    last_diagnostic TEXT,
    discharged_actor_kind TEXT,
    discharged_actor_identity TEXT,
    discharged_surface TEXT,
    discharged_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (project_id, store_path, bookmark, identifier),
    CHECK (
        (status = 'discharged' AND discharged_actor_kind IS NOT NULL
                               AND discharged_surface IS NOT NULL
                               AND discharged_at IS NOT NULL)
        OR
        (status != 'discharged' AND discharged_actor_kind IS NULL
                                AND discharged_actor_identity IS NULL
                                AND discharged_surface IS NULL
                                AND discharged_at IS NULL)
    )
);

CREATE INDEX idx_contended_identifier_obligations_merge_gate
    ON contended_identifier_obligations(project_id, bookmark, status);