//! Account management — unified user account connection to cairn.computer.
//!
//! Replaces the multi-team TeamManager approach with a single account connection.
//! The account subsumes team connections: org features work through the account's
//! org memberships. JWT refresh is handled for the account token.

pub mod connection;
pub mod jwt;
pub mod manager;
pub mod org_tokens;
pub mod queries;

pub use connection::{AccountConnection, DbAccount, OrgMembership};
pub use manager::AccountManager;
pub use org_tokens::OrgTokenCache;
