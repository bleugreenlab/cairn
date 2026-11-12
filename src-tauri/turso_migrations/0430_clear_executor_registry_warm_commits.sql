-- lineage: team
-- cairn/6950: warm roots left the advertisement, so the registry no longer
-- writes them. Rows written before that hold every root a machine ever fetched,
-- over a megabyte each; empty them so a team replica stops carrying it.
UPDATE executor_registry SET warm_commits = '[]' WHERE warm_commits <> '[]';
