//! The durable stores a disclosed credential can be sitting in, and what may be
//! done to each without an operator saying so.
//!
//! # One question decides everything
//!
//! For each store, ask: **is there a cleaner copy of this content upstream?**
//!
//! The answer, and nothing else, decides the store's [`RecordClass`], and the
//! class decides the response. It is deliberately not a question about who wrote
//! the content — an agent's terminal output and a user's issue body are both
//! "authored" in the loose sense, and neither can be regenerated, so both get
//! the same treatment. What matters is whether destroying this copy destroys
//! information.
//!
//! - [`RecordClass::Source`] — nothing upstream. This record *is* the account of
//!   what happened, so it is never edited or deleted: at most it is withheld
//!   from serving and kept on disk. Repairing it is an explicit operator
//!   action, never automatic. This is the "never silently rewrite authored
//!   state" rule, expressed as a type.
//! - [`RecordClass::Derived`] — regenerable from a source. It is rebuilt from
//!   sources that have already been quarantined, so the rebuild is sanitized by
//!   construction rather than by a second scrubbing pass.
//! - [`RecordClass::Ephemeral`] — a cache with no independent value. Purged.
//!
//! # Why derived stores are not scanned
//!
//! A derived store's disposition does not depend on what is in it. It is rebuilt
//! from sources whose disclosure has already been handled, so a scan could not
//! change the decision — and the result is *better* than a scan would give,
//! because rebuilding also removes content a scan would have missed.
//!
//! That is what makes the embedding tables and the Tantivy index tractable at
//! all. Neither can be searched for a credential's bytes: an embedding is a
//! vector, and an inverted index holds tokens rather than text. Neither needs to
//! be.
//!
//! Ephemeral stores *are* scanned, even though their handling is unconditional,
//! because the scan changes its *scope*: knowing which cache rows carry the
//! credential is the difference between dropping three of them and dropping
//! every check result on the machine. Source and ephemeral stores are therefore
//! scanned and derived ones are not — see [`SinkKind::is_scanned`].
//!
//! # Class says what is permitted; a gate says what is possible
//!
//! Being a source store earns a record the *right* to be withheld rather than
//! rewritten. It does not by itself make withholding happen. Withholding costs
//! a durable read gate at a chokepoint every reader passes through, and only
//! two stores have one: transcript events, served through event
//! reconstruction, and archival segment blobs, served through a single
//! hash-addressed load.
//!
//! The others — messages, artifacts, issue bodies, REPL exchanges, terminal
//! tails, and the on-disk logs — are read from many call sites scattered across
//! the tree. A gate on some of them would be worse than none, because it would
//! license the report to say "contained" while the ungated paths kept serving
//! the credential. So [`SinkKind::gate`] states the difference, those records
//! are recorded as [`Disposition::Reported`] rather than
//! [`Disposition::Quarantined`], and the incident tells the operator plainly
//! that they are still being served.
//!
//! They are still *scanned*, because naming the exact records someone has to go
//! deal with is most of what an inventory is worth.
//!
//! # Sinks this build cannot reach
//!
//! A few stores hold a credential where this process cannot act on it. Those are
//! [`Reach::Manual`], and they are *listed in the incident report* rather than
//! omitted from the taxonomy. An operator being told "check this yourself, for
//! this reason" is a completed inventory; an operator not being told is the
//! silent gap the whole exercise exists to close.

use std::fmt;

/// Whether a store holds the only copy of its content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordClass {
    /// Nothing upstream holds this content. Quarantine; never rewrite
    /// automatically.
    Source,
    /// Regenerable from a source store. Rebuild.
    Derived,
    /// A cache. Purge.
    Ephemeral,
}

impl RecordClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Derived => "derived",
            Self::Ephemeral => "ephemeral",
        }
    }

    /// The disposition this class earns. Total by construction: there is no
    /// store whose class is known and whose handling is undecided.
    pub fn disposition(self) -> Disposition {
        match self {
            Self::Source => Disposition::Quarantined,
            Self::Derived => Disposition::Rebuilt,
            Self::Ephemeral => Disposition::Purged,
        }
    }
}

/// What was done to an affected record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Withheld from serving, left intact on disk.
    Quarantined,
    /// Found and named, but still being served: this store has no read gate, so
    /// containment is an operator action. Distinct from `Quarantined` on
    /// purpose — collapsing the two would let an incident report a record as
    /// contained while every read of it still returns the credential, which is
    /// the one lie this subsystem must never tell.
    Reported,
    /// Regenerated from quarantined sources.
    Rebuilt,
    /// Deleted; it will regenerate on demand.
    Purged,
    /// Redacted in place. Only ever reached through an explicit operator
    /// request — no automatic path produces it.
    Repaired,
}

impl Disposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quarantined => "quarantined",
            Self::Reported => "reported",
            Self::Rebuilt => "rebuilt",
            Self::Purged => "purged",
            Self::Repaired => "repaired",
        }
    }
}

/// Whether this build can act on a store itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// This process inventories and remediates the store directly.
    Automatic,
    /// This process cannot act on the store. The reason is reported to the
    /// operator as part of the incident, so the gap is delegated rather than
    /// hidden.
    Manual(&'static str),
}

/// Whether a source store's records can actually be withheld from serving.
///
/// Separate from [`Reach`], and the distinction is the one this taxonomy most
/// needs to keep straight. Reach answers *can this process find and touch the
/// records*; a gate answers *will a reader be stopped from seeing one*. A store
/// can be fully reachable — scanned, counted, inventoried — and still have no
/// gate, and conflating the two is how an incident comes to report a record as
/// contained while every read of it keeps returning the credential.
///
/// Withholding is not something a store can opt into by declaration. It costs a
/// durable read gate at a real chokepoint, and a store whose reads are spread
/// across many call sites does not have one until someone builds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// A durable read gate consults the quarantine before serving this store's
    /// records, so quarantining one actually withholds it.
    Withholds,
    /// No read gate exists. The records are found, counted, and reported, and
    /// containment is the operator's move. The reason travels into the incident
    /// so the gap is stated rather than implied.
    Reports(&'static str),
}

/// Every durable store that can hold a disclosed credential.
///
/// Exhaustive on purpose. Adding a store to Cairn that holds observed output
/// means adding a variant here, and the compiler then demands a class and a
/// reach for it. A store that is missing from this enum is a store an incident
/// response will not mention, which is the failure mode this taxonomy exists to
/// make impossible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkKind {
    /// `events.data` — the transcript. The account of what the agent did.
    TranscriptEvent,
    /// `artifacts.data` — plans, PR bodies, and other node outputs.
    Artifact,
    /// `messages.content` — issue, node, and thread messages.
    Message,
    /// `issues.title` and `issues.description`.
    IssueBody,
    /// `repl_exchanges` — code, values, stdout, stderr, and errors of a REPL.
    ReplExchange,
    /// `job_terminals.output_tail` — the bounded tail kept in the database.
    TerminalTail,
    /// The rotated JSONL logs and the runner's stderr file.
    ProcessLog,
    /// A terminal's persisted scrollback under the job scratch directory.
    TerminalLog,
    /// `message_stream_chunks` — the accumulator a streamed message is built
    /// from. Redundant once its event exists.
    StreamChunk,
    /// `archival_blobs` and the content-addressed segment store — compressed
    /// segments of events that were rewritten at teardown.
    ArchivalBlob,
    /// The Tantivy full-text index.
    SearchIndex,
    /// The event, turn, resource, and issue embedding tables.
    Embedding,
    /// `check_result_cache` and the test-result rows hanging off it.
    CheckResultCache,
    /// Rows a team replica holds for the same content. Reachable only from the
    /// replica's own host.
    TeamReplica,
    /// Copies that left the machine: a pushed commit, a PR body, an uploaded
    /// log, a backup.
    ExternalCopy,
}

/// Every variant, for exhaustive iteration in reports and tests.
pub const ALL_SINKS: &[SinkKind] = &[
    SinkKind::TranscriptEvent,
    SinkKind::Artifact,
    SinkKind::Message,
    SinkKind::IssueBody,
    SinkKind::ReplExchange,
    SinkKind::TerminalTail,
    SinkKind::ProcessLog,
    SinkKind::TerminalLog,
    SinkKind::StreamChunk,
    SinkKind::ArchivalBlob,
    SinkKind::SearchIndex,
    SinkKind::Embedding,
    SinkKind::CheckResultCache,
    SinkKind::TeamReplica,
    SinkKind::ExternalCopy,
];

impl SinkKind {
    /// The stable name used in database rows, quarantine keys, and reports.
    pub fn as_str(self) -> &'static str {
        match self {
            // Shares its spelling with `cairn_db::storage::quarantine`, which
            // matches on it in the event read path. A test below pins them.
            Self::TranscriptEvent => cairn_db::storage::quarantine::TRANSCRIPT_EVENT_SINK,
            Self::Artifact => "artifact",
            Self::Message => "message",
            Self::IssueBody => "issue_body",
            Self::ReplExchange => "repl_exchange",
            Self::TerminalTail => "terminal_tail",
            Self::ProcessLog => "process_log",
            Self::TerminalLog => "terminal_log",
            Self::StreamChunk => "stream_chunk",
            Self::ArchivalBlob => cairn_db::storage::quarantine::ARCHIVAL_BLOB_SINK,
            Self::SearchIndex => "search_index",
            Self::Embedding => "embedding",
            Self::CheckResultCache => "check_result_cache",
            Self::TeamReplica => "team_replica",
            Self::ExternalCopy => "external_copy",
        }
    }

    /// A human label for the incident report.
    pub fn label(self) -> &'static str {
        match self {
            Self::TranscriptEvent => "transcript events",
            Self::Artifact => "node artifacts",
            Self::Message => "messages",
            Self::IssueBody => "issue titles and descriptions",
            Self::ReplExchange => "REPL exchanges",
            Self::TerminalTail => "terminal tails",
            Self::ProcessLog => "process logs",
            Self::TerminalLog => "terminal scrollback files",
            Self::StreamChunk => "streamed message chunks",
            Self::ArchivalBlob => "archival blobs",
            Self::SearchIndex => "full-text search index",
            Self::Embedding => "embeddings",
            Self::CheckResultCache => "check result cache",
            Self::TeamReplica => "team replica rows",
            Self::ExternalCopy => "copies off this machine",
        }
    }

    /// Whether this store holds the only copy of its content.
    pub fn record_class(self) -> RecordClass {
        match self {
            // Source: destroying the copy destroys the account of what happened.
            // An archival blob belongs here rather than with the derived stores,
            // and the distinction is easy to get wrong: archival does not copy a
            // segment out of an event, it *moves* it. After teardown the blob is
            // the only place that text exists, so rebuilding it is not possible
            // and purging it would delete history.
            Self::TranscriptEvent
            | Self::Artifact
            | Self::Message
            | Self::IssueBody
            | Self::ReplExchange
            | Self::TerminalTail
            | Self::ProcessLog
            | Self::TerminalLog
            | Self::ArchivalBlob
            | Self::TeamReplica
            | Self::ExternalCopy => RecordClass::Source,

            // Derived: regenerable from the transcript, and regenerated clean
            // because the transcript it regenerates from is already withheld.
            Self::SearchIndex | Self::Embedding => RecordClass::Derived,

            // Ephemeral: a cache. A purged check result re-runs; a purged stream
            // chunk was already folded into its event.
            Self::CheckResultCache | Self::StreamChunk => RecordClass::Ephemeral,
        }
    }

    /// Whether this process can inventory and remediate the store itself.
    pub fn reach(self) -> Reach {
        match self {
            Self::TranscriptEvent
            | Self::Artifact
            | Self::Message
            | Self::IssueBody
            | Self::ReplExchange
            | Self::TerminalTail
            | Self::ProcessLog
            | Self::TerminalLog
            | Self::StreamChunk
            | Self::ArchivalBlob
            | Self::SearchIndex
            | Self::Embedding
            | Self::CheckResultCache => Reach::Automatic,

            Self::TeamReplica => Reach::Manual(
                "A team replica is another machine's database. The quarantine table is \
                 private by design \u{2014} shipping a map of which records carry a credential \
                 would send the disclosure further than it already went \u{2014} so each \
                 replica's host must run its own response.",
            ),
            Self::ExternalCopy => Reach::Manual(
                "A pushed commit, a PR body, an uploaded artifact, or a backup has left \
                 this machine. Nothing here can reach it; rotating the credential is the \
                 only containment that applies.",
            ),
        }
    }

    /// Whether the inventory scans this store for occurrences.
    ///
    /// Every reachable store except a derived one. A derived store is rebuilt
    /// wholesale from sources, so a scan could not change what happens to it; a
    /// source or ephemeral store's scan decides *which* records are acted on.
    /// See the module docs.
    ///
    /// Note that this is deliberately independent of [`Self::gate`]: a store
    /// with no read gate is still scanned, because telling an operator exactly
    /// which records they must go deal with is most of the value an inventory
    /// has.
    pub fn is_scanned(self) -> bool {
        self.record_class() != RecordClass::Derived && matches!(self.reach(), Reach::Automatic)
    }

    /// Whether quarantining a record of this store actually withholds it.
    ///
    /// Only two stores have a real read gate today, and both earn it by having
    /// a single chokepoint every reader passes through: transcript events are
    /// served through event reconstruction, and archival segment blobs through
    /// one hash-addressed load. Everything else is read from many call sites
    /// scattered across the tree, and a gate on some of them is worse than
    /// none — it reports containment while the ungated paths keep serving.
    pub fn gate(self) -> Gate {
        match self {
            Self::TranscriptEvent | Self::ArchivalBlob => Gate::Withholds,

            Self::Artifact | Self::Message | Self::IssueBody | Self::ReplExchange => Gate::Reports(
                "This store is read from many call sites rather than one funnel, so there \
                 is no single seam a gate could sit on. The affected records are listed \
                 above; edit or delete them, and rotate the credential.",
            ),
            Self::TerminalTail => Gate::Reports(
                "A terminal's stored tail is served straight from its row. Clear the \
                 terminal or delete the row, and rotate the credential.",
            ),
            Self::ProcessLog | Self::TerminalLog => Gate::Reports(
                "A file on disk is read by log tooling and by people, outside any Cairn \
                 read path, so nothing here can stand between it and a reader. Delete or \
                 truncate the named files, and rotate the credential.",
            ),

            // Never quarantined: rebuilt from withheld sources, or purged
            // outright. Their disposition answers for them.
            Self::SearchIndex | Self::Embedding => {
                Gate::Reports("Rebuilt from quarantined sources rather than withheld.")
            }
            Self::StreamChunk | Self::CheckResultCache => {
                Gate::Reports("Purged outright rather than withheld.")
            }

            Self::TeamReplica | Self::ExternalCopy => {
                Gate::Reports("Out of this machine's reach entirely; see the reach note.")
            }
        }
    }
}

impl fmt::Display for SinkKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn all_sinks_lists_every_variant_exactly_once() {
        // `ALL_SINKS` drives the report and the tests below, so a variant
        // missing from it is a store an incident silently never mentions.
        let names: HashSet<&str> = ALL_SINKS.iter().map(|sink| sink.as_str()).collect();
        assert_eq!(
            names.len(),
            ALL_SINKS.len(),
            "two sinks share a name, so their quarantine keys would collide"
        );
        // The match arms in `as_str` are exhaustive, so counting them is how we
        // notice a variant that was added to the enum but not to `ALL_SINKS`.
        // Kept as an explicit number rather than derived, so adding a sink is a
        // deliberate edit here too.
        assert_eq!(ALL_SINKS.len(), 15);
    }

    #[test]
    fn only_a_sink_with_a_real_read_gate_claims_to_withhold() {
        // The invariant the whole taxonomy rests on. `Gate::Withholds` is a
        // claim that a reader is actually stopped, so it may only be made by a
        // sink whose name a read gate matches on. Pinned against the gate
        // constants themselves rather than a hand-listed set, so adding a sink
        // and declaring it gated without building the gate fails here.
        let gated: Vec<&str> = ALL_SINKS
            .iter()
            .filter(|sink| sink.gate() == Gate::Withholds)
            .map(|sink| sink.as_str())
            .collect();
        assert_eq!(
            gated,
            vec![
                cairn_db::storage::quarantine::TRANSCRIPT_EVENT_SINK,
                cairn_db::storage::quarantine::ARCHIVAL_BLOB_SINK,
            ],
            "a sink claims to withhold records without a read gate that honours it"
        );
    }

    #[test]
    fn an_ungated_source_store_is_never_reported_as_contained() {
        // These are all source stores, so their class alone would say
        // `Quarantined`. None has a read gate, so an incident must report them
        // as still served rather than as contained.
        for sink in [
            SinkKind::Message,
            SinkKind::Artifact,
            SinkKind::IssueBody,
            SinkKind::ReplExchange,
            SinkKind::TerminalTail,
            SinkKind::ProcessLog,
            SinkKind::TerminalLog,
        ] {
            assert_eq!(sink.record_class(), RecordClass::Source, "{sink}");
            assert!(
                matches!(sink.gate(), Gate::Reports(_)),
                "{sink} claims a gate it does not have"
            );
            // Still scanned: naming the records an operator must go handle is
            // most of what an inventory is for.
            assert!(sink.is_scanned(), "{sink} must still be inventoried");
        }
    }

    #[test]
    fn the_transcript_sink_name_matches_the_read_gate() {
        // cairn-db's read path matches this string. Two spellings would mean
        // quarantining an event the gate never checks for.
        assert_eq!(
            SinkKind::TranscriptEvent.as_str(),
            cairn_db::storage::quarantine::TRANSCRIPT_EVENT_SINK
        );
    }

    #[test]
    fn every_sink_declares_a_class_and_a_reach() {
        for sink in ALL_SINKS {
            // Both are total functions, so this asserts they terminate with a
            // real answer for every variant rather than that a value exists.
            let _ = sink.record_class();
            let _ = sink.reach();
            let _ = sink.label();
        }
    }

    #[test]
    fn a_source_record_is_never_automatically_rewritten() {
        // The core prohibition, as a property of the taxonomy: no source store's
        // automatic disposition mutates it. `Repaired` exists but is reachable
        // only through an explicit operator request.
        for sink in ALL_SINKS {
            if sink.record_class() == RecordClass::Source {
                assert_eq!(
                    sink.record_class().disposition(),
                    Disposition::Quarantined,
                    "{sink} is a source store, so its automatic handling must withhold it, \
                     not change it"
                );
            }
        }
    }

    #[test]
    fn every_reachable_store_except_a_derived_one_is_scanned() {
        for sink in ALL_SINKS {
            let expected =
                sink.record_class() != RecordClass::Derived && sink.reach() == Reach::Automatic;
            assert_eq!(sink.is_scanned(), expected, "{sink}");
        }
        // The index and the embeddings are not scanned: neither can be searched
        // for a credential's bytes, and neither needs to be, because both are
        // rebuilt from sources that have already been withheld.
        assert!(!SinkKind::SearchIndex.is_scanned());
        assert!(!SinkKind::Embedding.is_scanned());
        // A cache is scanned so its purge is targeted rather than total.
        assert!(SinkKind::CheckResultCache.is_scanned());
        assert!(SinkKind::TranscriptEvent.is_scanned());
        // An unreachable store is never scanned, whatever its class.
        assert!(!SinkKind::TeamReplica.is_scanned());
        assert!(!SinkKind::ExternalCopy.is_scanned());
    }

    #[test]
    fn every_unreachable_sink_explains_itself() {
        for sink in ALL_SINKS {
            if let Reach::Manual(reason) = sink.reach() {
                assert!(
                    reason.len() > 40,
                    "{sink} delegates to an operator without telling them why"
                );
            }
        }
    }
}
