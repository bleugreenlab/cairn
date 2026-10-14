//! Values used to attribute authored changes.

use serde::{Deserialize, Serialize};

/// Separate git commit identity (name + email).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitIdentity {
    pub id: String,
    pub label: String,
    pub name: String,
    pub email: String,
    /// First = default
    pub sort_order: i32,
}
