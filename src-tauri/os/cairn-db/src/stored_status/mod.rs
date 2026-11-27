//! Stored statuses: values a reader would otherwise derive from other tables,
//! kept as explicit columns and written by the site that causes them.
//!
//! Each stored fact has one setter. A caller passes the value (or, for a
//! count, the delta) its own write caused; the setter writes only its column,
//! only on change, and reads no other table. Readers combine facts with fixed
//! `CASE`s over columns of the same row, defined once beside the setters.
//! Source scans in cairn-core's `source_structure` tests hold every writer of
//! a fact's inputs to calling its setter, and keep the setters the only
//! writers of these columns.

pub mod jobs;

/// What a setter did. Dropping it unread is a warning: a caller either
/// returns it or decides it does not care.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatusWritten {
    pub changed: bool,
}

impl StatusWritten {
    pub fn changed(self) -> bool {
        self.changed
    }

    /// Fold another setter's result into this one.
    pub fn merge(self, other: Self) -> Self {
        Self {
            changed: self.changed || other.changed,
        }
    }

    pub(crate) fn from_rows(rows: u64) -> Self {
        Self { changed: rows > 0 }
    }
}
