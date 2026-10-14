-- Preserve the contaminated peak for diagnosis while removing it from admission.
ALTER TABLE command_resource_profiles ADD COLUMN quarantined_peak_rss_bytes INTEGER;
ALTER TABLE command_resource_profiles ADD COLUMN memory_quarantine_reason TEXT;

UPDATE command_resource_profiles
SET quarantined_peak_rss_bytes = upper_peak_rss_bytes,
    upper_peak_rss_bytes = NULL,
    memory_quarantine_reason = 'saturationWindow2026-08-04'
WHERE updated_at_unix_ms BETWEEN 1785871800000 AND 1785886200000
  AND upper_peak_rss_bytes IS NOT NULL;
