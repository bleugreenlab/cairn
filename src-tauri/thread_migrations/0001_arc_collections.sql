CREATE TABLE _cairn_rulings (
  id         INTEGER PRIMARY KEY,
  slug       TEXT NOT NULL UNIQUE
             CHECK (slug GLOB '[a-z0-9]*' AND slug GLOB '*[a-z0-9]'
                    AND NOT slug GLOB '*[^a-z0-9-]*' AND slug GLOB '*[a-z]*'),
  text       TEXT NOT NULL CHECK (length(trim(text)) > 0),
  status     TEXT NOT NULL CHECK (status IN ('accepted','rejected','superseded')),
  rationale  TEXT NOT NULL CHECK (length(trim(rationale)) > 0),
  provenance TEXT NOT NULL
             CHECK (json_valid(provenance) AND json_type(provenance) = 'array'
                    AND json_array_length(provenance) >= 1),
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE _cairn_arc_imports (
  id          INTEGER PRIMARY KEY CHECK (id = 1),
  imported_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE _cairn_open_questions (
  id         INTEGER PRIMARY KEY,
  question   TEXT NOT NULL CHECK (length(trim(question)) > 0),
  provenance TEXT
             CHECK (provenance IS NULL OR (json_valid(provenance)
                    AND json_type(provenance) = 'array')),
  created_at INTEGER NOT NULL DEFAULT (unixepoch())
);