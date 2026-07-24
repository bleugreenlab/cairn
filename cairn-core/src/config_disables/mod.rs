//! Legacy database-backed config disables.
//!
//! Skills, recipes, and agents now use the repository-owned
//! `contextualPackages.disabled` project policy. This module remains the action
//! override authority and owns the one-way migration of historical contextual
//! package rows.

pub mod queries;

pub use queries::{
    disable_config, enable_config, list_disabled_configs, list_disabled_keys,
    migrate_contextual_disables, DisabledConfig,
};
