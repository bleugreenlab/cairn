//! Who owns a record.

use serde::{Deserialize, Serialize};

/// Who owns a record. A record that names no source is Cairn's.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum RecordSource {
    /// Mirrored from bginf before it retired and not adopted yet: read-only
    /// until `Inventory::adopt` makes it Cairn's own, keeping its id.
    Bginf,
    /// Cairn's own record, which the operator writes.
    #[default]
    Cairn,
}

impl RecordSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bginf => "bginf",
            Self::Cairn => "cairn",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bginf" => Some(Self::Bginf),
            "cairn" => Some(Self::Cairn),
            _ => None,
        }
    }
}
