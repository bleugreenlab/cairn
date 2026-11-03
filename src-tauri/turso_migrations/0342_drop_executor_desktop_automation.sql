-- lineage: private
-- Machine services (cairn/5932) replace desktop automation: an instance's
-- probe and catalog live in its supervisor cell, derived live and never
-- stored, so the cache desktop automation kept here has no reader.
DROP TABLE IF EXISTS executor_desktop_automation;
