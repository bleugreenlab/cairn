//! File-based configuration and deterministic selection for Cairn.
//! Runtime consumers provide paths and authored values; credentials, database
//! reconciliation, and running services stay outside this crate.
pub use cairn_models as models;
pub use cairn_packs as pack;
pub use cairn_platform::env;
pub mod files;
pub use files::*;
pub mod agent_markdown;
pub mod agents;
pub mod build_services;
pub mod contextual_packages;
pub mod fleet;
pub mod markdown_frontmatter;
pub mod presets;
pub mod project_reference;
pub mod project_settings;
pub mod providers;
pub mod settings;
pub mod voice;
pub mod yaml_edit;

mod slugs;
pub use slugs::{derive_unique_task_slug, slugify_resource_segment};
