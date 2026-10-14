-- Migration: add tool_use_id to prompts
--
-- Records the originating `change` tool_use id when a question is asked. The
-- fast path (<45s) resolves the question inline as that change call's own
-- tool_result. The slow path (>45s durable suspend/resume) needs the same id so
-- it can attach a synthetic tool_result to the very same Question call, instead
-- of surfacing the answer as a separate user block. Nullable; prompts asked
-- before this column existed simply have no id and fall back to the old render.

ALTER TABLE prompts ADD COLUMN tool_use_id TEXT;
