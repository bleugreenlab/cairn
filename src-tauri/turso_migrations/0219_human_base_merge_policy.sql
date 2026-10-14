ALTER TABLE projects
ADD COLUMN human_base_merge_policy TEXT NOT NULL
DEFAULT '{"version":1,"state":"disabled"}';