//! File-based configuration and deterministic selection for Cairn.
//! Runtime consumers provide paths and authored values; credentials, database
//! reconciliation, and running services stay outside this crate.
pub use cairn_domain as models;
pub use cairn_packs as pack;
pub use cairn_platform::env;
pub mod files;
pub use files::*;
pub mod agent_markdown;
pub mod agent_pins;
pub mod agents;
pub use cairn_domain::allotment;
pub mod build_services;
pub mod contextual_packages;
pub mod fleet;
pub mod inference;
pub mod legacy_presets;
pub mod markdown_frontmatter;
pub mod project_reference;
pub mod project_settings;
pub mod providers;
pub mod served_api;
pub mod settings;
pub mod voice;
pub mod yaml_edit;

mod slugs;
pub use slugs::{derive_unique_task_slug, slugify_resource_segment};
