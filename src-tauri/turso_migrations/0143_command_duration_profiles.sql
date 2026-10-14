-- Machine-local learned execution durations for canonical check command
-- identities, stratified by how much of the work was already on the machine.
--
-- Separate from `command_resource_profiles` because the two answer questions
-- with different keys. A reservation asks "how much memory and disk must be held
-- for this", and its answer is a conservative high-water mark that must not be
-- fragmented across warmth or it would stop covering the heaviest case. A
-- duration asks "how long will this take here, in this state", and a warm
-- incremental compile and a cold full one are simply different work: averaging
-- them produces a number that describes neither. One module owns both, and one
-- observation path writes both.
--
-- Deliberately contains no commands, environments, credentials, or filesystem
-- paths.
CREATE TABLE command_duration_profiles (
    identity_version INTEGER NOT NULL,
    command_identity TEXT NOT NULL,
    executor_class TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    toolchain_fingerprint TEXT NOT NULL,
    -- `ExecutionWarmth`, as the executor observed it for the run being recorded.
    warmth TEXT NOT NULL,
    -- Total observations ever recorded for this key. Grows past the retained
    -- window, so a long-lived profile can say how much history stands behind it
    -- even though it predicts from the recent part.
    sample_count INTEGER NOT NULL,
    -- When the newest retained observation finished. A profile whose newest
    -- sample has aged past the profile-age limit stops speaking for the machine.
    updated_at_unix_ms INTEGER NOT NULL,
    -- The bounded recent window, oldest first, as a JSON array of milliseconds.
    -- A median over this window is what the profile predicts, so one anomalous
    -- run cannot become the machine's answer and old runs age out by count.
    recent_duration_ms TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (
        identity_version, command_identity, executor_class,
        os, arch, toolchain_fingerprint, warmth
    )
);
