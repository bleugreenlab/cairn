//! Short-lived, audience-bound, revocable credential leases.
//!
//! # What a lease is for
//!
//! The broker's first preference is that a credential never leaves it at all:
//! it performs or signs the operation and hands back a typed non-secret result.
//! Some credentials cannot be used that way. A bearer token has to reach the
//! provider that will check it, and a model backend reads its key out of the
//! subprocess environment. For those, the question stops being *whether*
//! plaintext exists and becomes *how much it is worth*.
//!
//! A lease is the answer to that second question. It is the same bytes with
//! three properties the raw credential does not have:
//!
//! 1. **A deadline.** Every lease carries an `expires_at`, and
//!    [`CredentialLease::present`] refuses past it, so Cairn stops handing the
//!    value out on its own schedule rather than when someone remembers to stop.
//! 2. **An audience.** A lease names the one destination it may be presented to,
//!    and presenting it anywhere else is refused by the call itself rather than
//!    by review. This is only a real check when the audience a caller presents
//!    is derived from where the credential is actually going — a caller that
//!    passes the same constant the lease was minted with has checked nothing.
//!    `broker::github` derives it by parsing the URL of the request it is about
//!    to send, which is why the authorities there expose request methods rather
//!    than a `headers()` accessor: a header map handed back to a caller is a
//!    bearer that has already passed its check and can then be attached to any
//!    URL at all.
//! 3. **A revocation.** Every handle to a lease shares one state, so revoking it
//!    reaches holders that already have it. That is why [`CredentialLease`] may
//!    be cloned where [`super::broker::BrokeredSecret`] may not:
//!    `BrokeredSecret` forbids `Clone` because a copy is an un-zeroized
//!    duplicate nobody can reach, while a lease handle points at one revocable
//!    cell, so a copy is exactly as revocable as the original.
//!
//! # What a lease is not
//!
//! A lease does not make plaintext safe. Once a value is presented — written
//! into a header, an environment variable, or a request body — the consuming
//! process holds it, and nothing here reaches into that process to take it
//! back.
//!
//! Both the deadline and the revocation act on *this* process's willingness to
//! hand the value out again. Neither reaches the provider that will accept it.
//! A bearer already copied out of a lease keeps working at its provider until
//! that provider's own expiry, whatever the lease says — so revoking a GitHub
//! installation token stops Cairn presenting it and stops the next request
//! reusing it, but a copy someone else took would still authenticate at GitHub
//! for the remainder of the hour GitHub minted it for.
//!
//! Where the two coincide the guarantee is stronger, and that is not an
//! accident: a provider-minted token is leased for exactly the deadline the
//! provider gave it, so the lease expiring and the credential dying are the
//! same moment. Where they do not — a key injected into an agent process — the
//! deadline governs re-issuance only.
//!
//! What a lease bounds, then, is how long Cairn will keep handing a value out
//! and how far a value can travel from the one call site allowed to send it.
//! The blast radius keeps its shape but stops being open-ended.
//!
//! # Why the book is process-local
//!
//! Leases live in one process-local [`LeaseBook`], for the same reason the
//! secret registry is process-local: shipping live credential material anywhere
//! is the exact disclosure this module exists to prevent. The book is a cache
//! and a revocation point, not a store. Nothing here is persisted, and a
//! restart correctly leaves every lease behind.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard};

use serde::Serialize;
use zeroize::Zeroizing;

use super::broker::CredentialSource;
use super::registry::registry;
use super::secret::SecretId;

/// Non-secret identity for one lease.
///
/// Names the lease, never its material, so it is safe in logs, refusal
/// messages, and the operator-facing inventory.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct LeaseId(String);

impl LeaseId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The one destination a leased credential may be presented to.
///
/// Non-secret by construction: every variant names a place, never a value. The
/// audience is what makes a lease narrower than the credential behind it — a
/// token good for `api.github.com` is refused at every other destination, so a
/// call site that reaches for the wrong lease fails loudly instead of
/// authenticating somewhere it should not.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum LeaseAudience {
    /// An HTTPS host this credential may be sent to. Compared on the host
    /// alone: a bearer is scoped by who can read it, and every path on one host
    /// can.
    HttpsOrigin { host: String },
    /// A named consumer role Cairn hands the credential to — a child process it
    /// launches, or a client inside this process that will present it onward.
    /// Named by role rather than by pid, so the audience survives a restart of
    /// that role.
    Process { role: String },
    /// One enrolled executor, by the name that addresses it.
    Executor { executor_id: String },
}

impl LeaseAudience {
    /// An HTTPS destination. The host is lower-cased so `API.GitHub.com` and
    /// `api.github.com` are one audience rather than two.
    pub fn https(host: impl AsRef<str>) -> Self {
        Self::HttpsOrigin {
            host: host.as_ref().trim().to_ascii_lowercase(),
        }
    }

    pub fn process(role: impl Into<String>) -> Self {
        Self::Process { role: role.into() }
    }

    pub fn executor(executor_id: impl Into<String>) -> Self {
        Self::Executor {
            executor_id: executor_id.into(),
        }
    }

    /// Human-readable form for refusal messages and the inventory.
    pub fn label(&self) -> String {
        match self {
            Self::HttpsOrigin { host } => format!("https://{host}"),
            Self::Process { role } => format!("process/{role}"),
            Self::Executor { executor_id } => format!("executor/{executor_id}"),
        }
    }
}

/// Why a presentation was refused.
///
/// Every variant names places and times only. These messages reach logs and can
/// reach an operator, so they must stay as non-secret as the audience itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseDenied {
    #[error("lease {lease} is bound to {bound} and was presented to {presented}")]
    Audience {
        lease: LeaseId,
        bound: String,
        presented: String,
    },
    #[error("lease {lease} expired at {expired_at}")]
    Expired { lease: LeaseId, expired_at: i64 },
    #[error("lease {lease} has been revoked")]
    Revoked { lease: LeaseId },
}

/// One lease's shared state.
///
/// Held behind an `Arc` so every handle observes one revocation, and the
/// material sits behind an `RwLock<Option<..>>` so revoking it *takes the bytes
/// away* rather than setting a flag a later reader could ignore.
struct LeaseState {
    id: LeaseId,
    secret_id: SecretId,
    /// The CAIRN-3803 scope shorthand of the credential behind this lease.
    /// Non-secret; carried so the inventory can say which authority a live
    /// lease exercises.
    scope: String,
    audience: LeaseAudience,
    issued_at: i64,
    expires_at: i64,
    /// `None` once revoked. Dropping the `Zeroizing` wipes the bytes.
    material: RwLock<Option<Zeroizing<String>>>,
}

/// A handle to a leased credential.
///
/// Deliberately missing: `Debug`, `Display`, `Serialize`, `Deserialize`. There
/// is exactly one way to reach the bytes — [`Self::present`] — and it takes the
/// audience, so every place plaintext leaves a lease is a call that had to name
/// where it was sending it.
///
/// `Clone` is deliberately present, and it is the one impl this type has that
/// [`super::broker::BrokeredSecret`] refuses. See the module docs: a clone here
/// duplicates a pointer to one revocable cell, not the material.
#[derive(Clone)]
pub struct CredentialLease(Arc<LeaseState>);

impl CredentialLease {
    pub fn id(&self) -> &LeaseId {
        &self.0.id
    }

    /// The registered identity of the material, for detection reports.
    pub fn secret_id(&self) -> &SecretId {
        &self.0.secret_id
    }

    pub fn audience(&self) -> &LeaseAudience {
        &self.0.audience
    }

    pub fn expires_at(&self) -> i64 {
        self.0.expires_at
    }

    /// Whether this lease would still be presentable at `now`.
    pub fn is_live(&self, now: i64) -> bool {
        self.0.expires_at > now
            && self
                .0
                .material
                .read()
                .expect("lease material lock poisoned")
                .is_some()
    }

    /// The credential's plaintext, for presenting to `audience`.
    ///
    /// The deliberate exposure, and the only one. The audience is checked, the
    /// deadline is checked, and revocation is checked under the same read lock
    /// that yields the bytes, so a lease cannot be revoked between the check and
    /// the use.
    ///
    /// The returned value belongs in an HTTP header, a child process
    /// environment, or a request body addressed to `audience` — never in a log
    /// line, a serializer, a database row, or anything a model observes.
    pub fn present(&self, audience: &LeaseAudience) -> Result<Presented<'_>, LeaseDenied> {
        if &self.0.audience != audience {
            return Err(LeaseDenied::Audience {
                lease: self.0.id.clone(),
                bound: self.0.audience.label(),
                presented: audience.label(),
            });
        }
        let guard = self
            .0
            .material
            .read()
            .expect("lease material lock poisoned");
        if guard.is_none() {
            return Err(LeaseDenied::Revoked {
                lease: self.0.id.clone(),
            });
        }
        let now = now();
        if self.0.expires_at <= now {
            return Err(LeaseDenied::Expired {
                lease: self.0.id.clone(),
                expired_at: self.0.expires_at,
            });
        }
        Ok(Presented { guard })
    }

    /// Take the material away from every holder of this lease.
    ///
    /// Returns whether this call was the one that revoked it. Idempotent: a
    /// second revocation is a no-op, so a teardown path may revoke
    /// unconditionally without knowing whether the lease already expired.
    ///
    /// Revocation deliberately does *not* unregister the value for scrubbing.
    /// Output produced while the lease was live can still be crossing a
    /// boundary after it is revoked, and unregistering would un-protect exactly
    /// that in-flight output.
    pub fn revoke(&self) -> bool {
        self.0
            .material
            .write()
            .expect("lease material lock poisoned")
            .take()
            .is_some()
    }
}

/// A lease's plaintext, borrowed for the length of one presentation.
///
/// Holds the read lock, so the material cannot be revoked out from under a
/// caller mid-use. Like every carrier in this subsystem it has no `Debug`, no
/// `Display`, and no `serde`.
pub struct Presented<'a> {
    guard: RwLockReadGuard<'a, Option<Zeroizing<String>>>,
}

impl Presented<'_> {
    pub fn expose(&self) -> &str {
        self.guard
            .as_deref()
            .expect("a presented lease holds material for as long as the read guard is held")
    }
}

/// Terms a lease is issued under. Every field is non-secret.
pub struct LeaseTerms<'a> {
    /// The credential the material came from. Supplies the scope and the
    /// registered identity, so a lease cannot claim an authority its source
    /// does not have.
    pub source: &'a CredentialSource,
    /// The one destination the material may be presented to.
    pub audience: LeaseAudience,
    /// Unix seconds after which [`CredentialLease::present`] refuses.
    pub expires_at: i64,
    /// Audit metadata, recorded with the registration and never consulted to
    /// decide anything.
    pub purpose: &'a str,
}

/// A non-secret row of the live lease inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseRecord {
    pub id: LeaseId,
    pub secret_id: SecretId,
    /// The CAIRN-3803 scope shorthand of the authority behind this lease.
    pub scope: String,
    pub audience: LeaseAudience,
    pub issued_at: i64,
    pub expires_at: i64,
    /// `active`, `expired`, or `revoked`.
    pub status: &'static str,
}

/// The process-local set of live leases.
///
/// Two jobs, and they are the same job seen from two sides: it is the cache
/// that keeps Cairn from minting a fresh provider token per request, and it is
/// the handle a teardown path revokes through. Those were separate concerns
/// before this module — a global token cache with no revocation — and keeping
/// them together is what makes "disconnect this account" reach the tokens
/// already minted under it.
pub struct LeaseBook {
    leases: Mutex<HashMap<LeaseId, Arc<LeaseState>>>,
    sequence: AtomicU64,
}

/// The process's lease book.
pub fn leases() -> &'static LeaseBook {
    static BOOK: OnceLock<LeaseBook> = OnceLock::new();
    BOOK.get_or_init(LeaseBook::new)
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

impl Default for LeaseBook {
    fn default() -> Self {
        Self::new()
    }
}

impl LeaseBook {
    pub fn new() -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            sequence: AtomicU64::new(0),
        }
    }

    /// Register `material` for scrubbing and hand back a lease over it.
    ///
    /// Registration happens before the lease exists, so there is no window in
    /// which leased plaintext is live and unprotected. A registration the
    /// registry refuses (a value too short or too repetitive to scrub for
    /// safely) still yields a lease — refusing to issue would break the
    /// operation to protect output that was never at risk — but it is logged,
    /// because an operator needs to know that observed output is not covered.
    pub fn issue(&self, terms: LeaseTerms<'_>, material: String) -> CredentialLease {
        let secret_id = super::broker::register(
            terms.source,
            super::broker::Declared::Secret,
            terms.purpose,
            material.clone(),
        );
        // Advisory on the registry, load-bearing on the lease: the registry
        // keeps scrubbing a stale value because output can outlive it, while
        // presentation stops at the deadline.
        registry().set_expiry(&secret_id, Some(terms.expires_at));

        let id = LeaseId(format!(
            "lease-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed)
        ));
        let state = Arc::new(LeaseState {
            id: id.clone(),
            secret_id,
            scope: terms.source.scope().shorthand(),
            audience: terms.audience,
            issued_at: now(),
            expires_at: terms.expires_at,
            material: RwLock::new(Some(Zeroizing::new(material))),
        });
        let mut leases = self.leases.lock().expect("lease book poisoned");
        // Bound the book by the one thing that makes an entry worthless: a lease
        // nobody can present. Sweeping on issue means a long-lived process never
        // accumulates dead state without a background task to do it.
        leases.retain(|_, state| is_live(state, now()));
        leases.insert(id, state.clone());
        CredentialLease(state)
    }

    /// A live lease for this credential and audience, if one is already issued
    /// and still valid at `not_before`.
    ///
    /// Callers pass `now + margin` rather than `now`, so a lease about to expire
    /// is re-minted before it is used rather than after it fails.
    pub fn live(
        &self,
        source: &CredentialSource,
        audience: &LeaseAudience,
        not_before: i64,
    ) -> Option<CredentialLease> {
        let wanted = source.secret_id();
        let leases = self.leases.lock().expect("lease book poisoned");
        leases
            .values()
            .find(|state| {
                state.secret_id == wanted
                    && &state.audience == audience
                    && state.expires_at > not_before
                    && state
                        .material
                        .read()
                        .expect("lease material lock poisoned")
                        .is_some()
            })
            .map(|state| CredentialLease(state.clone()))
    }

    /// Revoke one lease by id. Returns whether it was live.
    pub fn revoke(&self, id: &LeaseId) -> bool {
        let leases = self.leases.lock().expect("lease book poisoned");
        leases
            .get(id)
            .is_some_and(|state| CredentialLease(state.clone()).revoke())
    }

    /// Revoke every live lease presentable to `audience`. Returns how many.
    ///
    /// The shape a teardown wants: an executor leaves the fleet, and every lease
    /// that named it stops working without the caller tracking their ids.
    pub fn revoke_audience(&self, audience: &LeaseAudience) -> usize {
        self.revoke_matching(|state| &state.audience == audience)
    }

    /// Revoke every live lease minted from `source`. Returns how many.
    ///
    /// The shape a disconnect wants: an account is signed out, and every token
    /// already derived from it stops working regardless of who holds a handle.
    pub fn revoke_source(&self, source: &CredentialSource) -> usize {
        self.revoke_secret(&source.secret_id())
    }

    /// Revoke every live lease minted from the credential registered under
    /// `secret_id`. Returns how many.
    ///
    /// The shape a *disclosure* wants, which is why it exists beside
    /// [`Self::revoke_source`]. A detection reports a `SecretId` and nothing
    /// else — the crossing that caught the value knows what was registered, not
    /// which store it came out of — so a response that had to name a
    /// `CredentialSource` first could not act on its own evidence.
    ///
    /// Revoking does not unregister, and that is load-bearing here rather than
    /// incidental: an incident's scan matches by registered value, so a response
    /// that unregistered as it revoked would blind itself before it finished
    /// looking. See `security::remediation::inventory`.
    pub fn revoke_secret(&self, secret_id: &SecretId) -> usize {
        let wanted = secret_id.clone();
        self.revoke_matching(move |state| state.secret_id == wanted)
    }

    /// The distinct authority scopes the leases for `secret_id` exercise.
    ///
    /// A disclosure names a credential; authority grants are keyed by scope. The
    /// lease book is what already holds that correspondence — every lease
    /// records both — so this reads it back rather than making the caller
    /// reconstruct a `CredentialSource` it was never given.
    ///
    /// Process-local, so this answers for leases *this* runner minted. A
    /// credential that has not been leased since startup yields nothing, which
    /// is why an incident reports what it revoked rather than claiming to have
    /// revoked everything.
    pub fn scopes_for_secret(&self, secret_id: &SecretId) -> Vec<String> {
        let leases = self.leases.lock().expect("lease book poisoned");
        let mut scopes: Vec<String> = Vec::new();
        for state in leases.values() {
            if &state.secret_id == secret_id && !scopes.contains(&state.scope) {
                scopes.push(state.scope.clone());
            }
        }
        scopes.sort();
        scopes
    }

    fn revoke_matching(&self, predicate: impl Fn(&LeaseState) -> bool) -> usize {
        let leases = self.leases.lock().expect("lease book poisoned");
        leases
            .values()
            .filter(|state| predicate(state))
            .filter(|state| CredentialLease((*state).clone()).revoke())
            .count()
    }

    /// Every lease the book still holds, newest first. Non-secret throughout.
    pub fn inventory(&self) -> Vec<LeaseRecord> {
        let at = now();
        let leases = self.leases.lock().expect("lease book poisoned");
        let mut out: Vec<LeaseRecord> = leases
            .values()
            .map(|state| LeaseRecord {
                id: state.id.clone(),
                secret_id: state.secret_id.clone(),
                scope: state.scope.clone(),
                audience: state.audience.clone(),
                issued_at: state.issued_at,
                expires_at: state.expires_at,
                status: status(state, at),
            })
            .collect();
        out.sort_by(|a, b| b.issued_at.cmp(&a.issued_at).then(a.id.cmp(&b.id)));
        out
    }
}

fn is_live(state: &LeaseState, at: i64) -> bool {
    state.expires_at > at
        && state
            .material
            .read()
            .expect("lease material lock poisoned")
            .is_some()
}

fn status(state: &LeaseState, at: i64) -> &'static str {
    if state
        .material
        .read()
        .expect("lease material lock poisoned")
        .is_none()
    {
        "revoked"
    } else if state.expires_at <= at {
        "expired"
    } else {
        "active"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::Sanitizer;

    fn source() -> CredentialSource {
        CredentialSource::GitHubInstallation {
            installation_id: 4_242,
        }
    }

    fn issue(
        book: &LeaseBook,
        audience: LeaseAudience,
        expires_at: i64,
        material: &str,
    ) -> CredentialLease {
        book.issue(
            LeaseTerms {
                source: &source(),
                audience,
                expires_at,
                purpose: "lease test",
            },
            material.to_string(),
        )
    }

    /// The audience is the point: the same bytes are refused at any destination
    /// but the one they were leased for.
    #[test]
    fn a_lease_is_refused_at_the_wrong_audience() {
        let book = LeaseBook::new();
        let bound = LeaseAudience::https("api.github.com");
        let lease = issue(&book, bound.clone(), now() + 600, "ghs-Kd82Lq04Zx91Wm");

        assert!(lease.present(&bound).is_ok());
        // `unwrap_err` is unavailable here on purpose: it needs `T: Debug`, and
        // a presentation deliberately has none. Destructuring is the shape a
        // caller of this API has to use, so the test uses it too.
        let Err(denied) = lease.present(&LeaseAudience::https("evil.example.com")) else {
            panic!("a lease must be refused at an audience it was not issued for");
        };
        assert!(matches!(denied, LeaseDenied::Audience { .. }), "{denied:?}");
        // The refusal names places, never the value.
        assert!(!denied.to_string().contains("ghs-"));
    }

    /// Host comparison is case-insensitive, so one destination is one audience.
    #[test]
    fn an_https_audience_is_one_audience_regardless_of_case() {
        let book = LeaseBook::new();
        let lease = issue(
            &book,
            LeaseAudience::https("API.GitHub.com"),
            now() + 600,
            "ghs-Bn73Vc16Rt50Ka",
        );
        assert!(lease
            .present(&LeaseAudience::https("api.github.com"))
            .is_ok());
    }

    /// A lease stops working on its own. This is what bounds an exposure whose
    /// holder never comes back to give it up.
    #[test]
    fn a_lease_is_refused_after_its_deadline() {
        let book = LeaseBook::new();
        let audience = LeaseAudience::https("api.github.com");
        let lease = issue(&book, audience.clone(), now() - 1, "ghs-Qw38Nm72Yb04Lx");

        let Err(denied) = lease.present(&audience) else {
            panic!("an expired lease must be refused");
        };
        assert!(matches!(denied, LeaseDenied::Expired { .. }), "{denied:?}");
        assert!(!lease.is_live(now()));
    }

    /// The property that justifies letting a lease handle be cloned at all:
    /// revocation reaches a holder that took its handle before the revocation
    /// happened. A copied `String` could never be recalled this way.
    #[test]
    fn revocation_reaches_a_handle_taken_before_it() {
        let book = LeaseBook::new();
        let audience = LeaseAudience::process("claude-agent");
        let lease = issue(&book, audience.clone(), now() + 600, "sk-Ht91Kd28Zq47Bv");
        let already_held = lease.clone();
        assert!(already_held.present(&audience).is_ok());

        assert!(book.revoke(lease.id()));

        let Err(denied) = already_held.present(&audience) else {
            panic!("a revoked lease must be refused through a handle taken earlier");
        };
        assert!(matches!(denied, LeaseDenied::Revoked { .. }), "{denied:?}");
        // Idempotent: a teardown may revoke without knowing the current state.
        assert!(!book.revoke(lease.id()));
    }

    /// Bulk revocation by destination — the shape a teardown wants when an
    /// account is disconnected or an executor leaves the fleet.
    #[test]
    fn revoking_an_audience_takes_every_lease_for_that_destination() {
        let book = LeaseBook::new();
        let github = LeaseAudience::https("api.github.com");
        let elsewhere = LeaseAudience::process("claude-agent");
        let a = issue(&book, github.clone(), now() + 600, "ghs-Ab19Cd37Ef55Gh");
        let b = issue(&book, github.clone(), now() + 600, "ghs-Ij73Kl91Mn28Op");
        let untouched = issue(&book, elsewhere.clone(), now() + 600, "sk-Qr46St82Uv13Wx");

        assert_eq!(book.revoke_audience(&github), 2);
        assert!(a.present(&github).is_err());
        assert!(b.present(&github).is_err());
        assert!(
            untouched.present(&elsewhere).is_ok(),
            "a different destination must be untouched"
        );
    }

    /// Reuse is the other half of the book's job: a live lease is handed back
    /// rather than re-minted, and one inside the caller's refresh margin is not.
    #[test]
    fn the_book_reuses_a_live_lease_and_passes_over_a_stale_one() {
        let book = LeaseBook::new();
        let audience = LeaseAudience::https("api.github.com");
        let lease = issue(&book, audience.clone(), now() + 600, "ghs-Yz57Ab13Cd79Ef");

        let found = book
            .live(&source(), &audience, now())
            .expect("a live lease is reusable");
        assert_eq!(found.id(), lease.id());

        assert!(
            book.live(&source(), &audience, now() + 900).is_none(),
            "a lease inside the refresh margin must be re-minted, not reused"
        );
        assert!(
            book.live(&source(), &LeaseAudience::process("other"), now())
                .is_none(),
            "reuse must not cross audiences"
        );
    }

    /// Issuing registers, so a leased value is a scrub target from the moment it
    /// exists rather than from the moment someone remembers to register it.
    #[test]
    fn a_leased_value_is_scrubbed_from_observed_output() {
        // Its own credential, deliberately. Registering an id that is already
        // registered recomputes that id's forms from the newest material — the
        // behaviour a rotated value needs — so tests sharing one `SecretId`
        // overwrite each other's scrub target. Every other test here shares
        // `source()` harmlessly because none of them looks at the registry;
        // this is the one that does, and a sibling issuing beside it would
        // otherwise replace the value it is about to assert on.
        let source = CredentialSource::GitHubInstallation {
            installation_id: 90_001,
        };
        let value = "ghs-Lm40Pq82Xt17Rd93";
        let _lease = leases().issue(
            LeaseTerms {
                source: &source,
                audience: LeaseAudience::https("api.github.com"),
                expires_at: now() + 600,
                purpose: "lease test",
            },
            value.to_string(),
        );

        let mut sanitizer = Sanitizer::exact();
        assert!(!sanitizer
            .text(&format!("the provider echoed {value} back"))
            .contains(value));
    }

    /// The inventory is what an operator reads, so it must carry no material —
    /// asserted against the serialized form rather than field by field, because
    /// a future field would slip past a field-by-field check.
    #[test]
    fn the_inventory_carries_no_material() {
        let book = LeaseBook::new();
        let value = "ghs-Wv62Jn08Hs34Ty";
        issue(
            &book,
            LeaseAudience::https("api.github.com"),
            now() + 600,
            value,
        );

        let inventory = book.inventory();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].status, "active");
        assert_eq!(
            inventory[0].audience,
            LeaseAudience::https("api.github.com")
        );
        let rendered = serde_json::to_string(&inventory).unwrap();
        assert!(
            !rendered.contains(value),
            "the lease inventory must never carry material: {rendered}"
        );
    }

    /// Status is reported honestly for each of the three ways a lease stops
    /// working.
    #[test]
    fn the_inventory_reports_expired_and_revoked_distinctly() {
        let book = LeaseBook::new();
        let audience = LeaseAudience::process("claude-agent");
        let live = issue(&book, audience.clone(), now() + 600, "sk-Ax72Bd19Ce46Df");
        let revoked = issue(&book, audience.clone(), now() + 600, "sk-Gh83Ij25Kl61Mn");
        book.revoke(revoked.id());

        let by_id = |id: &LeaseId| {
            book.inventory()
                .into_iter()
                .find(|record| &record.id == id)
                .expect("lease is in the inventory")
        };
        assert_eq!(by_id(live.id()).status, "active");
        assert_eq!(by_id(revoked.id()).status, "revoked");
    }

    /// Compile-time proof that a lease and its presentation cannot be formatted
    /// or serialized. Mirrors the probes on `BrokeredSecret`; see
    /// `security::broker` for how they work.
    mod no_leaking_impls {
        macro_rules! absence_probe {
            ($module:ident, $target:ty, $bound:path, $test:ident, $message:literal) => {
                mod $module {
                    use std::marker::PhantomData;

                    pub struct Probe<T>(PhantomData<T>);

                    pub trait Absent {
                        fn implements() -> bool {
                            false
                        }
                    }
                    impl<T> Absent for Probe<T> {}

                    impl<T: $bound> Probe<T> {
                        fn implements() -> bool {
                            true
                        }
                    }

                    #[test]
                    fn $test() {
                        assert!(!Probe::<$target>::implements(), $message);
                        assert!(Probe::<String>::implements(), "probe is inert");
                    }
                }
            };
        }

        absence_probe!(
            lease_debug,
            crate::security::lease::CredentialLease,
            std::fmt::Debug,
            a_lease_has_no_debug,
            "CredentialLease must not be formattable"
        );
        absence_probe!(
            lease_serialize,
            crate::security::lease::CredentialLease,
            serde::Serialize,
            a_lease_has_no_serde,
            "CredentialLease must not be serializable"
        );
        absence_probe!(
            presented_debug,
            crate::security::lease::Presented<'static>,
            std::fmt::Debug,
            a_presentation_has_no_debug,
            "a presented lease must not be formattable"
        );
        absence_probe!(
            presented_serialize,
            crate::security::lease::Presented<'static>,
            serde::Serialize,
            a_presentation_has_no_serde,
            "a presented lease must not be serializable"
        );
    }
}
