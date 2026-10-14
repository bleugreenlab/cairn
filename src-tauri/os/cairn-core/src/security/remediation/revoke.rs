//! Taking back the authority a disclosed credential carries.
//!
//! # Why this runs first
//!
//! Of the five things a response does, this is the only one that reduces harm.
//! Quarantine stops *this* system serving a record; it does nothing about a copy
//! someone already took, and by the time a disclosure is noticed a copy may well
//! exist. Revocation acts on the thing still under local control: whether the
//! credential can be handed out again.
//!
//! So it runs before the inventory, not after. An inventory of a large database
//! and a gigabyte of logs takes time, and every second of it is a second the
//! credential is still being issued to whoever asks.
//!
//! # What it reaches, stated narrowly
//!
//! Revocation here acts on *this process's willingness to hand the value out
//! again*. It does not reach the provider. A bearer already copied out of a
//! lease keeps authenticating at GitHub, or Linear, or a model backend, until
//! that provider's own expiry. That is exactly why [`super::rotation`] exists
//! and why an incident is not closed by revoking: for a credential a third party
//! validates, only rotation at that third party ends the disclosure.
//!
//! The one place the two coincide is a provider-minted token leased for exactly
//! the deadline the provider gave it, where the lease expiring and the
//! credential dying are one moment. Everywhere else the gap is real and the
//! incident says so.

use cairn_common::security::SecretId;
use cairn_db::storage::LocalDb;

use crate::authorization;
use crate::security::leases;

/// What revocation actually took.
///
/// Counts rather than a boolean, because "revoked nothing" is a meaningful and
/// common outcome — a credential injected into a child process at startup has no
/// live lease — and reporting it as success would tell an operator the
/// disclosure was contained when nothing was contained at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Revoked {
    /// Live leases taken back.
    pub leases: usize,
    /// Authority grants marked revoked.
    pub grants: usize,
    /// The scopes the grants were revoked under, for the audit record.
    pub scopes: Vec<String>,
}

impl Revoked {
    /// Whether anything was actually taken back.
    pub fn is_empty(&self) -> bool {
        self.leases == 0 && self.grants == 0
    }
}

/// Revoke every lease and authority grant tied to the disclosed credential.
///
/// Grants are found through the lease book's record of which scope each lease
/// exercises. That correspondence is process-local, so a credential this runner
/// has not leased since startup contributes no scopes and therefore no grant
/// revocations — [`Revoked`] reports the zero rather than implying coverage.
pub async fn revoke_authority(db: &LocalDb, secret_id: &SecretId) -> Revoked {
    // Order matters within this function too: take the scopes before revoking,
    // because revocation does not remove a lease from the book but a future
    // change that pruned revoked leases would silently empty this list.
    let scopes = leases().scopes_for_secret(secret_id);
    let revoked_leases = leases().revoke_secret(secret_id);

    let mut grants = 0;
    if !scopes.is_empty() {
        match authorization::list_grants(db, GRANT_SCAN_LIMIT).await {
            Ok(all) => {
                for grant in all {
                    if grant.revoked_at.is_some() {
                        continue;
                    }
                    if !scopes.contains(&grant.scope.shorthand()) {
                        continue;
                    }
                    match authorization::revoke_grant(db, &grant.id, Some(REVOKED_BY)).await {
                        Ok(true) => grants += 1,
                        Ok(false) => {}
                        Err(error) => log::warn!(
                            "disclosure response could not revoke grant {}: {error}",
                            grant.id
                        ),
                    }
                }
            }
            Err(error) => {
                log::warn!("disclosure response could not list authority grants: {error}")
            }
        }
    }

    Revoked {
        leases: revoked_leases,
        grants,
        scopes,
    }
}

/// How many grants a response inspects. Grants are few and a disclosure is
/// rare, so this is a guard against a pathological table rather than a page
/// size; a truncated scan would silently leave authority live.
const GRANT_SCAN_LIMIT: i64 = 10_000;

/// Recorded as the revoker, so the grant history distinguishes an operator
/// withdrawing an approval from an automatic disclosure response.
const REVOKED_BY: &str = "disclosure-response";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::broker::CredentialSource;
    use crate::security::lease::{LeaseAudience, LeaseBook, LeaseTerms};

    fn source() -> CredentialSource {
        CredentialSource::GitHubInstallation {
            installation_id: 77,
        }
    }

    /// A deadline comfortably in the future. The book sweeps leases that can no
    /// longer be presented every time it issues one, so a test that dates its
    /// expiries from zero loses its earlier leases to that sweep.
    fn soon() -> i64 {
        chrono::Utc::now().timestamp() + 3_600
    }

    #[test]
    fn revoking_by_secret_takes_every_lease_for_that_credential() {
        let book = LeaseBook::new();
        let source = source();
        // Two destinations, one credential: the shape a disclosure has to cover.
        let a = book.issue(
            LeaseTerms {
                source: &source,
                audience: LeaseAudience::https("api.github.com"),
                expires_at: soon(),
                purpose: "test",
            },
            "ghs_Qa9Zm2Xp7Lr4Kt8Wd3Nv".to_string(),
        );
        let b = book.issue(
            LeaseTerms {
                source: &source,
                audience: LeaseAudience::https("uploads.github.com"),
                expires_at: soon(),
                purpose: "test",
            },
            "ghs_Qa9Zm2Xp7Lr4Kt8Wd3Nv".to_string(),
        );

        let taken = book.revoke_secret(&source.secret_id());
        assert_eq!(taken, 2);
        assert!(a.present(&LeaseAudience::https("api.github.com")).is_err());
        assert!(b
            .present(&LeaseAudience::https("uploads.github.com"))
            .is_err());
    }

    #[test]
    fn a_credential_with_no_live_lease_revokes_nothing_and_says_so() {
        // The injected-credential case. Reporting zero is the point: an operator
        // reading "revoked 0 leases" knows containment did not happen here and
        // that rotation is the only step that will.
        let book = LeaseBook::new();
        assert_eq!(book.revoke_secret(&SecretId::new("never-leased")), 0);
        assert!(book
            .scopes_for_secret(&SecretId::new("never-leased"))
            .is_empty());
    }

    #[test]
    fn scopes_for_a_secret_come_back_deduplicated() {
        let book = LeaseBook::new();
        let source = source();
        for host in ["api.github.com", "uploads.github.com"] {
            book.issue(
                LeaseTerms {
                    source: &source,
                    audience: LeaseAudience::https(host),
                    expires_at: soon(),
                    purpose: "test",
                },
                "ghs_Qa9Zm2Xp7Lr4Kt8Wd3Nv".to_string(),
            );
        }
        // Two leases, one authority: a grant must not be revoked twice.
        let scopes = book.scopes_for_secret(&source.secret_id());
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0], source.scope().shorthand());
    }
}
