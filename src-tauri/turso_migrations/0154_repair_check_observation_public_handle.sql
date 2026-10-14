-- Repair 0146, which was registered and applied with only a header comment.
-- Nullable preserves historical observations, which predate public handles.
ALTER TABLE check_result_observations ADD COLUMN public_handle TEXT;

CREATE UNIQUE INDEX idx_check_result_observations_public_handle
    ON check_result_observations(project_id, public_handle)
    WHERE public_handle IS NOT NULL;