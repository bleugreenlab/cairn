-- cairn/5175: check observations whose platform label was the requester's.
--
-- When a lane was claimed by a remote machine and that machine's account of
-- the run was lost (a dropped link, a replaced connection), the row named the
-- claiming executor but took its platform, architecture, toolchain, and verdict
-- environment from the coordinator recording it. A fedora lane requested from
-- a Mac was stored as macos/aarch64, polluting the macOS baseline and hiding
-- from the Linux one. The recorder now labels such a row from the platform the
-- machine advertised when it claimed the lane; this corrects the rows written
-- before that.
--
-- Those rows are recognisable without guessing: they name an executor but
-- carry no executor device id, because only the executor's own report supplies
-- one. Each machine's real platform is taken from the rows it DID attest,
-- and only where every such row agrees on one platform and architecture.
-- The colocated executor is excluded: its claim-only rows were labelled by the
-- process that was that machine, so their label is already right.
--
-- Every corrected row is recorded in `check_observation_relabels` with the
-- label it carried, so the correction is countable and reversible.
CREATE TABLE check_observation_relabels (
    observation_id TEXT PRIMARY KEY
        REFERENCES check_result_observations(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    recorded_platform TEXT,
    recorded_arch TEXT,
    corrected_platform TEXT NOT NULL,
    corrected_arch TEXT NOT NULL,
    reason TEXT NOT NULL,
    corrected_at INTEGER NOT NULL
);

INSERT INTO check_observation_relabels (
    observation_id, project_id, executor_id, recorded_platform, recorded_arch,
    corrected_platform, corrected_arch, reason, corrected_at
)
SELECT o.id, o.project_id, o.executor_id, o.verdict_platform, o.verdict_arch,
       attested.os, attested.arch,
       'claim-only row labelled with the recording coordinator''s platform (cairn/5175)',
       unixepoch() * 1000
  FROM check_result_observations o
  JOIN (
        SELECT executor_id,
               MIN(verdict_platform) AS os,
               MIN(verdict_arch) AS arch
          FROM check_result_observations
         WHERE executor_device_id IS NOT NULL
           AND executor_id IS NOT NULL
           AND executor_id <> 'colocated'
           AND verdict_platform IS NOT NULL
           AND verdict_arch IS NOT NULL
         GROUP BY executor_id
        HAVING MIN(verdict_platform) = MAX(verdict_platform)
           AND MIN(verdict_arch) = MAX(verdict_arch)
       ) attested ON attested.executor_id = o.executor_id
 WHERE o.executor_device_id IS NULL
   AND (o.verdict_platform IS NOT attested.os OR o.verdict_arch IS NOT attested.arch);

-- Observations are immutable, and the trigger that says so stays the rule for
-- every writer. It is lifted here only for this audited correction of labels
-- the recorder got wrong, inside this migration's own transaction, and
-- restored unchanged below.
DROP TRIGGER check_result_observations_immutable;

-- The toolchain and verdict environment on these rows were the coordinator's
-- too, and say nothing about the machine that held the lane.
UPDATE check_result_observations
   SET verdict_platform = (SELECT r.corrected_platform FROM check_observation_relabels r
                            WHERE r.observation_id = check_result_observations.id),
       verdict_arch = (SELECT r.corrected_arch FROM check_observation_relabels r
                        WHERE r.observation_id = check_result_observations.id),
       verdict_environment_hash = NULL,
       toolchain_fingerprint = NULL
 WHERE id IN (SELECT observation_id FROM check_observation_relabels);

CREATE TRIGGER check_result_observations_immutable
BEFORE UPDATE ON check_result_observations
BEGIN SELECT RAISE(ABORT, 'check result observations are immutable'); END;

-- The hot projection copies the label from its source observation.
UPDATE check_result_cache
   SET verdict_platform = (SELECT r.corrected_platform FROM check_observation_relabels r
                            WHERE r.observation_id = check_result_cache.source_observation_id),
       verdict_arch = (SELECT r.corrected_arch FROM check_observation_relabels r
                        WHERE r.observation_id = check_result_cache.source_observation_id),
       verdict_environment_hash = NULL,
       toolchain_fingerprint = NULL
 WHERE source_observation_id IN (SELECT observation_id FROM check_observation_relabels);
