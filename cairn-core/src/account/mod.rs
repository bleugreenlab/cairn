//! Account management — unified user account connection to cairn.computer.
//!
//! Replaces the multi-team TeamManager approach with a single account connection.
//! The account subsumes team connections: org features work through the account's
//! org memberships. JWT refresh is handled for the account token.

pub mod anon_device;
pub mod connection;
pub mod content_store;
pub mod executor_enrollment;
pub mod jwt;
pub mod manager;
pub mod org_tokens;
pub mod queries;
pub mod team_sync;
pub mod team_token_minter;

pub use anon_device::AnonDeviceManager;
pub use connection::{AccountConnection, DbAccount, OrgMembership};
pub use content_store::{BrokeredContentStore, BrokeredContentStoreFactory};
pub use manager::AccountManager;
pub use org_tokens::OrgTokenCache;
pub use team_sync::{
    fetch_team_sync_config, mint_team_sync_token, probe_team_sync_status, read_device_jwt,
    ConnectAccountTeamsSummary, SyncConfig, SyncConfigStatus, TeamConnectStatus, TeamSyncReadiness,
    TeamSyncStatus,
};
pub use team_token_minter::{DefaultTeamTokenMinter, TeamTokenMinter};
