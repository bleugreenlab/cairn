-- lineage: private
-- cairn/5873: machine capabilities a project's work turned out to need, learned
-- from a run that failed for want of one. A check whose git dependency fetch
-- failed on a credential (classified `infrastructure`) records that its
-- project needs `github-credential`, and placement then refuses, by name, any
-- machine that has measured itself without one, whether or not the project
-- declares `requires:`.
--
-- Sticky by design: the row's only effect is to keep the project off machines
-- that cannot fetch its dependencies, and it is shown with its evidence on
-- cairn://executors. Private because the evidence names this installation's
-- machines, and the requirement gates only this runner's placement.
--
-- evidence_repository is the repository the failing fetch named, when it
-- named one; the executor's probe checks the credential against it.
CREATE TABLE IF NOT EXISTS project_learned_requirements (
    project_id TEXT NOT NULL,
    requirement TEXT NOT NULL,
    evidence_repository TEXT,
    executor_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    first_learned_at_ms INTEGER NOT NULL,
    last_seen_at_ms INTEGER NOT NULL,
    PRIMARY KEY (project_id, requirement)
);
