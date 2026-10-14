-- Migration: drop workspaces timezone column

ALTER TABLE workspaces DROP COLUMN timezone;
