-- lineage: private
-- cairn/6323: the operator's trust in a project's library definitions (the
-- agents, recipes, skills, responses, routes and workflows under the checkout's
-- `.cairn/`). An untrusted project's definitions are skipped; a trusted
-- project's definition takes effect only once the operator has confirmed its
-- authority projection (tool grants, hooks, scripts, model selection), and a
-- confirmation binds the exact projection by digest, so any change to an
-- authority field asks again.
--
-- Private: trust is one operator's decision on this install about one checkout
-- on this machine. It never syncs to a teammate and never travels with an
-- export. `project_path` is the checkout that was trusted; the runner clears the
-- trust and every confirmation when the project's checkout moves.
--
-- No foreign key to `projects`: a team project's row lives in its team replica,
-- not this database. The runner removes rows whose project is gone.
CREATE TABLE IF NOT EXISTS project_library_trust (
    project_id TEXT PRIMARY KEY,
    project_key TEXT NOT NULL,
    project_path TEXT NOT NULL,
    trusted_by_json TEXT NOT NULL,
    trusted_at_ms INTEGER NOT NULL
);

-- One confirmed authority projection per definition. `authority_json` is the
-- canonical JSON of the projection the operator was shown, and
-- `authority_digest` its SHA-256.
CREATE TABLE IF NOT EXISTS project_library_confirmations (
    project_id TEXT NOT NULL
        REFERENCES project_library_trust(project_id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    authority_json TEXT NOT NULL,
    authority_digest TEXT NOT NULL,
    confirmed_by_json TEXT NOT NULL,
    confirmed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (project_id, kind, name)
);
