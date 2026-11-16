//! Profile-store reads, memoized across placement passes.
//!
//! A request waiting for a machine is re-priced against every candidate on
//! each placement pass, at least four times a second, and every pricing reads
//! the same few rows: the command's resource and duration profiles in that
//! machine's context, its suite's memory evidence, the context's contention
//! curve, and the machine's recent outcomes of it. On 11-16 those point reads,
//! repeated for every parked request against every machine, held twelve of
//! the runner host's sixteen cores (cairn/7301). The rows change only when a
//! run on some machine teaches the store something, so a pass that finds
//! nothing new should read nothing.
//!
//! Each loader names its lookup by its own arguments and asks [`Lookup`]
//! first. A value is reused while the database instance's profile revision
//! is the one it was read at and it is younger than [`PROFILE_READ_TTL_MS`].
//! Every writer of these tables takes a [`Writing`] guard, which advances the
//! revision when the write returns, so the next pass after a lesson reads the
//! lesson. The age bound is for rows nothing here wrote: retention's pruning
//! and rows a synced store receives.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use crate::storage::LocalDb;
use cairn_common::executor_protocol::CommandResourceIdentity;

/// How long a memoized read stands without a write through this module.
pub(super) const PROFILE_READ_TTL_MS: u64 = 30_000;

/// The most lookups held at once. Past it, stale ones are dropped first and
/// then everything, which costs one pass of reads.
const MAX_MEMOIZED: usize = 16_384;

struct Memo {
    revision: u64,
    read_at_unix_ms: u64,
    value: Arc<dyn Any + Send + Sync>,
}

#[derive(Default)]
struct Reads {
    /// Each database instance's profile revision: every write advances it.
    revisions: HashMap<u64, u64>,
    /// Advanced by the writes that can change any request's needs without
    /// naming whom (an operator forgetting a machine's kills).
    needs_epochs: HashMap<u64, u64>,
    /// Advanced by the writes that name what they taught: an identity, a
    /// suite, a project.
    scopes: HashMap<(u64, String), u64>,
    memos: HashMap<(u64, String), Memo>,
}

impl Reads {
    fn revision(&self, instance: u64) -> u64 {
        self.revisions.get(&instance).copied().unwrap_or(0)
    }
}

static READS: LazyLock<Mutex<Reads>> = LazyLock::new(Default::default);

fn reads() -> MutexGuard<'static, Reads> {
    READS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One loader's lookup: answered from memory when nothing has been written
/// since it was last read, and kept after a read otherwise.
pub(super) struct Lookup {
    instance: u64,
    key: String,
    revision: u64,
}

impl Lookup {
    /// `key` must name the lookup and every argument its answer depends on.
    pub(super) fn new(db: &LocalDb, key: String) -> Self {
        let instance = db.instance();
        let revision = reads().revision(instance);
        Self {
            instance,
            key,
            revision,
        }
    }

    /// The value this lookup last read, when it still stands.
    pub(super) fn hit<T: Clone + 'static>(&self) -> Option<T> {
        let now = super::unix_time_ms();
        let reads = reads();
        let memo = reads.memos.get(&(self.instance, self.key.clone()))?;
        (memo.revision == self.revision
            && now.saturating_sub(memo.read_at_unix_ms) < PROFILE_READ_TTL_MS)
            .then(|| memo.value.downcast_ref::<T>().cloned())
            .flatten()
    }

    /// Keep what the read returned. A failed read is not kept, and neither
    /// is one a write overtook: it may describe the rows before that write.
    pub(super) fn keep<T: Clone + Send + Sync + 'static>(self, read: &Result<T, String>) {
        let Ok(value) = read else {
            return;
        };
        let now = super::unix_time_ms();
        let mut reads = reads();
        let revision = reads.revision(self.instance);
        if revision != self.revision {
            return;
        }
        if reads.memos.len() >= MAX_MEMOIZED {
            let revisions = reads.revisions.clone();
            reads.memos.retain(|(instance, _), memo| {
                memo.revision == revisions.get(instance).copied().unwrap_or(0)
                    && now.saturating_sub(memo.read_at_unix_ms) < PROFILE_READ_TTL_MS
            });
            if reads.memos.len() >= MAX_MEMOIZED {
                reads.memos.clear();
            }
        }
        reads.memos.insert(
            (self.instance, self.key),
            Memo {
                revision,
                read_at_unix_ms: now,
                value: Arc::new(value.clone()),
            },
        );
    }
}

/// What a request's needs were worked out from, as one comparable value.
/// Equal stamps mean nothing the needs were derived from has been written
/// since; see [`needs_stamp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NeedsStamp {
    epoch: u64,
    scoped: u64,
}

/// The scope a command identity's rows are written under.
pub(super) fn identity_scope(identity: &CommandResourceIdentity) -> String {
    format!("identity\u{1f}{}:{}", identity.version, identity.key)
}

/// The scopes a suite's evidence is written under: the suite, and its
/// project, whose class prior reads every suite the project has.
pub(super) fn suite_scopes(suite_key: &str) -> Vec<String> {
    let mut scopes = vec![format!("suite\u{1f}{suite_key}")];
    if let Some((project_id, _)) = suite_key.split_once('/') {
        scopes.push(format!("project\u{1f}{project_id}"));
    }
    scopes
}

/// The stamp of what a request's needs depend on, given every scope its
/// identities and suites name.
///
/// A write that taught the store about one identity advances only that
/// identity's scope, so a finish of one command leaves every other queued
/// request's needs standing. A write that names nothing advances the epoch,
/// which every stamp includes.
pub(super) fn needs_stamp(db: &LocalDb, scopes: &[String]) -> NeedsStamp {
    let instance = db.instance();
    let reads = reads();
    NeedsStamp {
        epoch: reads.needs_epochs.get(&instance).copied().unwrap_or(0),
        scoped: scopes
            .iter()
            .map(|scope| {
                reads
                    .scopes
                    .get(&(instance, scope.clone()))
                    .copied()
                    .unwrap_or(0)
            })
            .sum(),
    }
}

enum Taught {
    /// Anything may have changed: the default, which errs toward re-reading.
    Anything,
    /// Only these scopes' needs may have changed.
    Scopes(Vec<String>),
    /// Nothing a request's needs read: contention, duration and reliability
    /// rows, which ranking reads and needs do not.
    NoNeeds,
}

/// Held by a writer of the profile tables for the length of its write.
/// Dropping it, on every path out of the writer, advances the instance's
/// revision, so no read from before the write is answered after it, and the
/// needs stamp of whatever the write taught.
pub(super) struct Writing {
    instance: u64,
    taught: Taught,
}

impl Writing {
    /// A write that may change any request's needs.
    pub(super) fn of(db: &LocalDb) -> Self {
        Self {
            instance: db.instance(),
            taught: Taught::Anything,
        }
    }

    /// A write that taught the store about `scopes` only.
    pub(super) fn teaching(db: &LocalDb, scopes: Vec<String>) -> Self {
        Self {
            instance: db.instance(),
            taught: Taught::Scopes(scopes),
        }
    }

    /// A write to rows no request's needs read.
    pub(super) fn apart_from_needs(db: &LocalDb) -> Self {
        Self {
            instance: db.instance(),
            taught: Taught::NoNeeds,
        }
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        let mut reads = reads();
        *reads.revisions.entry(self.instance).or_default() += 1;
        match &self.taught {
            Taught::Anything => *reads.needs_epochs.entry(self.instance).or_default() += 1,
            Taught::Scopes(scopes) => {
                for scope in scopes {
                    *reads
                        .scopes
                        .entry((self.instance, scope.clone()))
                        .or_default() += 1;
                }
            }
            Taught::NoNeeds => {}
        }
    }
}
