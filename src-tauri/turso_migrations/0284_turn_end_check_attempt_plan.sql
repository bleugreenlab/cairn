-- cairn/5282: the plan a review wave published, kept on its attempt row.
--
-- A wave plans against one head: its sealed commit, tree, the checks contract
-- that commit declares, each check's input hash, and which lanes apply. The
-- `/checks` projection reads lanes by exactly those keys. While the node's
-- branch lives they can be recomputed; once a merge or close retires it they
-- cannot, so the wave records them as it publishes them.
ALTER TABLE turn_end_check_attempts ADD COLUMN sealed_commit TEXT;
ALTER TABLE turn_end_check_attempts ADD COLUMN plan_json TEXT;
