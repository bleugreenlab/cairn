-- Machine-local clone path for team project repositories (CAIRN-2223).
--
-- project_routes is private-only, so this path never syncs through the team
-- replica. NULL means this machine has not cloned the team project repository.
ALTER TABLE project_routes ADD COLUMN local_repo_path TEXT;
