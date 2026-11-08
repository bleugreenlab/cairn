-- lineage: shared
CREATE INDEX idx_execution_history_inline_pack
    ON execution_history(execution_id)
    WHERE pack IS NOT NULL;
