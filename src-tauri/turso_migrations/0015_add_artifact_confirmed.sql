-- Migration: add artifact confirmed
--
-- `artifacts.confirmed` is the durable resolution fact for the job-status
-- projection (CAIRN-1156). A job that blocks at an approval checkpoint derives
-- `Blocked` until its artifact is confirmed, at which point it derives `Complete`.
-- Approve flips this to 1; it stays 0 while pending or rejected.

ALTER TABLE artifacts ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0;
