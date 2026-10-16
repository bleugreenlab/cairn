-- Retire `depends_on` into `after:` gates (cairn/4735).
--
-- `issue_dependencies` was an issue-level edge enforced at job readiness: a job
-- of the dependent issue was not ready while any target was unresolved, and a
-- target's resolution re-advanced the dependents' running executions. Both
-- enforcement paths are deleted with this migration. An execution now waits
-- through its own `after:` gates (0255), which hold it before it has any jobs
-- rather than holding jobs that already exist.
--
-- No edge is carried across as a live hold. Each edge on an issue that has an
-- execution becomes a gate on that issue's NEWEST execution, latched satisfied
-- at migration time: the history stays visible on the execution it shaped, and
-- it holds nothing. Each edge on an issue with no execution is dropped -- with
-- no jobs, it never held anything either.
--
-- Counted against the operator's database when this was written (10-13): 187
-- edges on 126 issues; 164 converted into satisfied gates, 23 dropped because
-- their issue had no execution.
--
-- Shared: `issue_dependencies` exists in both lineages, so the conversion and
-- the drop compose into both. The drop is one statement that names no table
-- created earlier in this batch, which is the shape a synced replica replays
-- safely.

-- 1. An execution receiving a migrated gate must not become held. "Has gates
--    AND released_at IS NULL" is the whole held predicate, and the sweep
--    releases a held execution by minting its jobs -- so an execution that
--    already ran, given a gate and left unreleased, would project as `held` and
--    then run a second time. Mark it released at its own start, BEFORE its
--    first gate arrives. An execution that is already held carries gates of its
--    own and stays unreleased: those gates still decide when it starts, and a
--    satisfied gate adds no hold.
UPDATE executions
   SET released_at = started_at
 WHERE released_at IS NULL
   AND NOT EXISTS (
       SELECT 1 FROM execution_gates g WHERE g.execution_id = executions.id
   )
   AND id IN (
       SELECT (
           SELECT e.id
             FROM executions e
            WHERE e.issue_id = d.issue_id
            ORDER BY e.seq DESC, e.started_at DESC, e.id DESC
            LIMIT 1
       )
         FROM issue_dependencies d
   );

-- 2. One satisfied `issue_resolution` gate per distinct target, in the stored
--    predicate's canonical form: keys sorted, project key lowercased -- the
--    exact string `canonical_predicates` writes, so cycle detection and gate
--    patching compare a migrated gate like any other. The target URI is parsed
--    rather than resolved: an edge whose target is missing or unresolved is
--    history all the same, and its satisfied latch is what makes it hold
--    nothing. Ordered after any gates the execution already has.
--
--    The id inherits the execution's team prefix, as `mint_child` does, so a
--    team replica's gate routes with its execution; the suffix is a canonical
--    v4 UUID because a prefixed id must carry one.
INSERT INTO execution_gates (
    id, execution_id, seq, predicate_json, satisfied_at, last_outcome, created_at
)
SELECT
    CASE WHEN instr(t.execution_id, '~') > 0
         THEN substr(t.execution_id, 1, instr(t.execution_id, '~'))
         ELSE ''
    END
      || lower(hex(randomblob(4))) || '-'
      || lower(hex(randomblob(2))) || '-4'
      || substr(lower(hex(randomblob(2))), 2) || '-'
      || substr('89ab', 1 + (abs(random()) % 4), 1)
      || substr(lower(hex(randomblob(2))), 2) || '-'
      || lower(hex(randomblob(6))),
    t.execution_id,
    t.base_seq + t.rank,
    '{"fact":"issue_resolution","issue":"cairn://p/' || t.project_key || '/' || t.number || '"}',
    unixepoch(),
    'satisfied',
    unixepoch()
FROM (
    SELECT DISTINCT
        edge.execution_id,
        edge.project_key,
        edge.number,
        COALESCE(
            (SELECT MAX(g.seq) + 1 FROM execution_gates g
              WHERE g.execution_id = edge.execution_id),
            0
        ) AS base_seq,
        (SELECT COUNT(DISTINCT lower(d2.depends_on_uri))
           FROM issue_dependencies d2
          WHERE d2.issue_id = edge.issue_id
            AND lower(d2.depends_on_uri) < edge.canonical_uri) AS rank
    FROM (
        SELECT
            d.issue_id,
            lower(d.depends_on_uri) AS canonical_uri,
            lower(substr(d.depends_on_uri, 11,
                         instr(substr(d.depends_on_uri, 11), '/') - 1)) AS project_key,
            CAST(substr(d.depends_on_uri,
                        11 + instr(substr(d.depends_on_uri, 11), '/')) AS INTEGER) AS number,
            (SELECT e.id
               FROM executions e
              WHERE e.issue_id = d.issue_id
              ORDER BY e.seq DESC, e.started_at DESC, e.id DESC
              LIMIT 1) AS execution_id
        FROM issue_dependencies d
        WHERE d.depends_on_uri LIKE 'cairn://p/%/%'
    ) edge
    WHERE edge.execution_id IS NOT NULL
) t;

-- 3. Nothing reads the table any more.
DROP TABLE issue_dependencies;
