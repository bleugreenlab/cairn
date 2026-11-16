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
    /// Each database instance's profile revision.
    revisions: HashMap<u64, u64>,
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

/// Held by a writer of the profile tables for the length of its write.
/// Dropping it, on every path out of the writer, advances the instance's
/// revision, so no read from before the write is answered after it.
pub(super) struct Writing {
    instance: u64,
}

impl Writing {
    pub(super) fn of(db: &LocalDb) -> Self {
        Self {
            instance: db.instance(),
        }
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        *reads().revisions.entry(self.instance).or_default() += 1;
    }
}
