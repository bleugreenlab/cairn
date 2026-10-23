-- Who asked for a prepared ordinary launch: 'operator' (content an operator
-- authored, or an operator's own start) or 'background' (resumes, wakes,
-- recoveries, delegated children). The admission row's urgent priority is
-- derived from it, so this column is what says why a row is urgent. Rows
-- prepared before it existed were all admitted as ordinary work.
ALTER TABLE prepared_ordinary_launches ADD COLUMN origin TEXT NOT NULL DEFAULT 'background';
