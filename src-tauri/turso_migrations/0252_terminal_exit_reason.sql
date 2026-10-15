-- cairn/4977: why a terminal ended when its exit status alone cannot say. NULL
-- for an ordinary exit, where the exit code is the whole story. Set when Cairn
-- settles a terminal whose process it can prove is gone without ever having
-- received its exit ("process lost: ..."), so the exit wake, a waitFor on the
-- exit, and a terminal read all carry the explanation.
ALTER TABLE job_terminals ADD COLUMN exit_reason TEXT;