//! Contended sequential identifiers, and the rules that resolve them.
//!
//! Several identifiers in this repository are sequential counters allocated on
//! the base branch. A branch must choose its value at authoring time, when it
//! cannot know whether another branch has already taken it. When two branches
//! choose the same value both are correct in isolation, and neither one owns the
//! collision.
//!
//! Most of these collisions never surface as a conflict at all, which is why
//! they need a semantic pass rather than a better merge driver:
//!
//! - Two migrations at one number are two *different filenames*. A rebase sees
//!   two adds of two distinct paths and merges them cleanly, leaving a duplicate
//!   version in a lineage whose version is a primary key.
//! - Two identical constant bumps are the *same edit on both sides*, which
//!   three-way merge resolves without a conflict. The second branch's increment
//!   is absorbed and vanishes.
//!
//! The second failure is the dangerous one, and it is dangerous because it
//! destroys its own evidence: once the increment is absorbed, the fork point
//! carries the absorbed value, so every later recomputation agrees the tree is
//! fine. That asymmetry — a swallowed increment is unrecoverable from the tree,
//! a duplicate migration number stays visible in it — is why only the constants
//! carry a durable obligation with a recovery floor, and the lineage rule does
//! not.
//!
//! The executor protocol version is the exception to delta arithmetic. Its
//! increments are not interchangeable counts but entries in a ledger, each one
//! described by a `/// Version N …` paragraph, so a branch's contribution is the
//! number of paragraphs it wrote rather than the distance its number moved. See
//! [`DeltaEvidence`] for why arithmetic cannot carry that ledger.
//!
//! Everything in this module apart from the obligation journal and the merge
//! gate's three reads ([`protocol_ledger_refusal`]) is a pure function over
//! content. The store reads that supply the base-advance pass, and the commit
//! that applies its result, live in `base_advance`.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{LocalDb, RowExt};
use cairn_db::turso::params;

/// The one registry file both migration lineages register through.
const MIGRATION_REGISTRY: &str = "src-tauri/os/cairn-db/src/storage/migrations.rs";

/// The TypeScript guard that states the same lineage facts for the validating
/// side. Asserted against this registry by a unit test rather than generated,
/// because three constants do not justify a codegen pipeline but silent drift
/// between the two statements of them would be expensive.
///
/// The drift check is the whole of its purpose, so it is compiled only for the
/// tests: a shipped build has no use for a copy of a TypeScript file.
#[cfg(test)]
const MIGRATION_CHECK_SCRIPT: &str = include_str!("../../../../../scripts/check-migrations.ts");

/// Digits in a lineage version prefix. Shared with `version_from_stem` in
/// `cairn-db`, which fails the build on any other shape.
const VERSION_DIGITS: usize = 4;

/// A directory of `NNNN_name.ext` files whose numbers form one contiguous
/// lineage, each file registered by its stem in a Rust registry.
///
/// The number is an identity and hygiene token, not an ordering token: apply
/// order comes from the registry vec's order, never from the number. That is
/// what makes reassigning one a mechanical operation.
pub(crate) struct SequentialFileLineage {
    /// How this lineage names itself in diagnostics.
    pub label: &'static str,
    /// Repo-relative directory holding the lineage's files.
    pub dir: &'static str,
    /// Repo-relative registry file carrying one registration per stem.
    pub registry_file: &'static str,
    /// The registration macro. Matched with a preceding-character guard so
    /// `migration` does not also match the tail of `team_migration`.
    pub macro_name: &'static str,
}

pub(crate) async fn explicitly_discharge_obligation(
    db: &LocalDb,
    project_id: &str,
    bookmark: &str,
    identifier: &str,
    reason: &str,
    attribution: DischargeAttribution<'_>,
) -> Result<bool, String> {
    let changed = db
        .execute(
            "UPDATE contended_identifier_obligations
             SET status = 'discharged', last_diagnostic = ?4,
                 discharged_actor_kind = ?5, discharged_actor_identity = ?6,
                 discharged_surface = ?7, discharged_at = ?8, updated_at = ?8
             WHERE project_id = ?1 AND bookmark = ?2 AND identifier = ?3
               AND status = 'outstanding'",
            params![
                project_id,
                bookmark,
                identifier,
                reason,
                attribution.actor_kind,
                attribution.actor_identity,
                attribution.surface,
                chrono::Utc::now().timestamp()
            ],
        )
        .await
        .map_err(|error| format!("explicitly discharge identifier obligation: {error}"))?;
    Ok(changed > 0)
}

/// Outstanding recovery floors that make a branch unsafe to merge. This query
/// intentionally ignores discharged history and orphaned post-merge evidence.
pub(crate) async fn outstanding_obligation_labels(
    db: &LocalDb,
    project_id: &str,
    bookmark: &str,
) -> Result<Vec<String>, String> {
    let project_id = project_id.to_string();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT identifier FROM contended_identifier_obligations
                     WHERE project_id = ?1 AND bookmark = ?2 AND status = 'outstanding'
                     ORDER BY identifier",
                    params![project_id, bookmark],
                )
                .await?;
            let mut labels = Vec::new();
            while let Some(row) = rows.next().await? {
                labels.push(row.text(0)?);
            }
            Ok(labels)
        })
    })
    .await
    .map_err(|error| format!("load merge-gating identifier obligations: {error}"))
}

/// Another file carrying the same literal value as a monotonic constant, which
/// must move in the same edit set or the tree ships two answers to one question.
pub(crate) struct CompanionLiteral {
    /// Repo-relative file.
    pub file: &'static str,
    /// The exact text immediately preceding the value.
    pub prefix: &'static str,
}

/// A `pub const NAME: u32 = N;` counter that only ever moves forward, where the
/// branch's contribution is its **delta** rather than the absolute value it
/// happened to write.
pub(crate) struct MonotonicConstant {
    /// Stable key. Persisted on the obligation row, so it must not change
    /// without a migration.
    pub label: &'static str,
    /// Repo-relative file declaring the constant.
    pub file: &'static str,
    /// The constant's identifier.
    pub constant: &'static str,
    /// What counts as proof that the branch moved the constant.
    pub evidence: DeltaEvidence,
    /// Files mirroring the same literal.
    pub companions: &'static [CompanionLiteral],
}

/// How the resolver learns how many increments a branch contributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeltaEvidence {
    /// A version is minted only by a doc heading the branch itself added —
    /// `/// Version 135 adds …`, present on the branch tip and not headed at its
    /// fork. The contribution is the number of such headings; the number the
    /// branch happens to carry is not evidence of anything, and a branch that
    /// adds no heading lands on the base's value.
    ///
    /// Arithmetic cannot carry this ledger. `tip − fork` is only the branch's
    /// own work when both ends belong to the branch: a fork read from a stale
    /// record, or a tip an earlier advance already inflated, makes it count the
    /// base's increments as the branch's, and a recovery floor then makes the
    /// miscount permanent. That is how branches that never touched the protocol
    /// carried 109 and then 134 onto main, each superseding a published release.
    /// A heading is written only by an author, so counting headings cannot
    /// inherit somebody else's bump.
    AuthoredHeadings { prefix: &'static str },
    /// The contribution is `tip − fork`, with the stored obligation as a
    /// recovery floor. Right for a counter that has no per-version prose to
    /// count, where an identical bump on two branches really is two bumps.
    Arithmetic,
}

impl DeltaEvidence {
    /// Whether a stored obligation is a floor the live count cannot go below.
    /// Heading evidence lives in the text and survives a rebase with it, so its
    /// stored delta is simply the latest count and a smaller one replaces it.
    pub(crate) fn keeps_floor(self) -> bool {
        matches!(self, Self::Arithmetic)
    }

    pub(crate) fn heading_prefix(self) -> Option<&'static str> {
        match self {
            Self::AuthoredHeadings { prefix } => Some(prefix),
            Self::Arithmetic => None,
        }
    }
}

pub(crate) const SEQUENTIAL_FILE_LINEAGES: &[SequentialFileLineage] = &[
    SequentialFileLineage {
        label: "private",
        dir: "src-tauri/turso_migrations",
        registry_file: MIGRATION_REGISTRY,
        macro_name: "migration",
    },
    SequentialFileLineage {
        label: "team",
        dir: "src-tauri/turso_migrations_team",
        registry_file: MIGRATION_REGISTRY,
        macro_name: "team_migration",
    },
];

/// The contended constants.
///
/// Deliberately not registered: `COMPANION_PROTOCOL_VERSION`,
/// `CHECK_RESULT_SCHEMA_VERSION`, and the voice protocol versions. They have the
/// same declaration shape but are not contended in practice, and registering a
/// frozen constant would mean an advance could move a value nobody asked to move.
pub(crate) const MONOTONIC_CONSTANTS: &[MonotonicConstant] = &[
    MonotonicConstant {
        label: "executor_protocol_version",
        file: "src-tauri/os/cairn-common/src/executor_protocol.rs",
        constant: "EXECUTOR_PROTOCOL_VERSION",
        evidence: DeltaEvidence::AuthoredHeadings { prefix: "Version " },
        companions: &[],
    },
    MonotonicConstant {
        label: "substrate_health_schema_version",
        file: "src-tauri/os/cairn-common/src/executor_protocol.rs",
        constant: "SUBSTRATE_HEALTH_SCHEMA_VERSION",
        evidence: DeltaEvidence::Arithmetic,
        // The desktop fixture is the wire serializer's own output, asserted
        // byte-for-byte by `running_panel_fixture_is_the_wire_serializers_own_output`.
        // Moving the constant without it turns a resolved advance into a red suite.
        companions: &[CompanionLiteral {
            file: "fixtures/substrate-health-snapshot.json",
            prefix: "\"schemaVersion\": ",
        }],
    },
];

/// Every repo-relative path any registered identifier can touch. The gate that
/// decides whether an advance needs this pass at all.
pub(crate) fn registered_paths() -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for lineage in SEQUENTIAL_FILE_LINEAGES {
        paths.insert(lineage.registry_file.to_string());
    }
    for constant in MONOTONIC_CONSTANTS {
        paths.insert(constant.file.to_string());
        for companion in constant.companions {
            paths.insert(companion.file.to_string());
        }
    }
    paths
}

/// Whether a changed path is one this pass could have to resolve. A lineage's
/// `.sql` files are matched by directory because their names are exactly what a
/// collision changes.
pub(crate) fn path_is_registered(path: &str) -> bool {
    if SEQUENTIAL_FILE_LINEAGES
        .iter()
        .any(|lineage| path.starts_with(lineage.dir))
    {
        return true;
    }
    registered_paths().contains(path)
}

/// Files whose conflicting content this pass knows how to union rather than hand
/// to an agent: the registration registries, whose tails two branches append to
/// independently.
pub(crate) fn union_resolvable_paths() -> BTreeSet<String> {
    SEQUENTIAL_FILE_LINEAGES
        .iter()
        .map(|lineage| lineage.registry_file.to_string())
        .collect()
}

/// One file mutation the resolver will apply to a materialized tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FileEdit {
    /// Move a lineage file to its new number, carrying its content untouched.
    Rename { from: String, to: String },
    /// Replace a file's whole content.
    Rewrite { path: String, content: String },
}

impl FileEdit {
    /// The paths this edit touches, for scoping the commit that applies it.
    pub(crate) fn paths(&self) -> Vec<String> {
        match self {
            Self::Rename { from, to } => vec![from.clone(), to.clone()],
            Self::Rewrite { path, .. } => vec![path.clone()],
        }
    }
}

/// What one resolution did, in the vocabulary the agent is told it in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Movement {
    /// A lineage file moved from one number to another.
    Renumbered {
        lineage: String,
        from: String,
        to: String,
        /// `path:line` sites outside the lineage and its registry that still
        /// spell `from`'s version as a bare quoted literal. They are reported,
        /// never rewritten: a number reference does not follow a renumber, and
        /// only the author knows which migration it meant.
        bare_literals: Vec<String>,
    },
    /// A monotonic constant carried its branch's delta onto the new base value.
    Advanced {
        identifier: String,
        from: u64,
        to: u64,
        /// The new base's value, which `delta` was carried onto.
        base: u64,
        delta: u64,
        /// The stored recovery floor supplied the delta because the live tree no
        /// longer could. Worth saying out loud: it means an earlier advance
        /// swallowed this increment.
        from_floor: bool,
        /// Which version paragraphs `delta` counted, for a heading-evidenced
        /// constant; `None` for an arithmetic one.
        headings: Option<HeadingTally>,
    },
    /// A heading-evidenced constant was put back on the base's value because
    /// the branch moved it without writing a version paragraph.
    Reset {
        identifier: String,
        from: u64,
        to: u64,
        /// Versions headed on the tip that the base supplied verbatim.
        absorbed: Vec<u64>,
    },
}

/// The version paragraphs a heading-evidenced resolution counted and those it
/// did not, so the note a builder receives can be checked in one read rather
/// than re-derived from three revisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeadingTally {
    /// Versions the branch headed itself, ascending; these are the delta.
    pub authored: Vec<u64>,
    /// Versions headed on the tip and not at the fork whose every heading line
    /// is the base's own, verbatim: content the branch merged in, not wrote.
    pub absorbed: Vec<u64>,
}

fn version_list(versions: &[u64]) -> String {
    if versions.is_empty() {
        return "none".to_string();
    }
    versions
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// A complete, verified edit set. Computed whole and applied whole: every target
/// is proven present before a byte is written, so a resolution never leaves a
/// renamed file with a registration pointing at a stem that no longer exists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ResolutionPlan {
    pub edits: Vec<FileEdit>,
    pub movements: Vec<Movement>,
    /// Identifier labels whose obligation this plan discharges, whether by
    /// applying it or by finding it already satisfied.
    pub discharged: Vec<String>,
}

impl ResolutionPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.edits.is_empty()
    }

    /// Every path the plan writes, deduplicated, for a path-scoped commit.
    pub(crate) fn touched_paths(&self) -> Vec<String> {
        let mut paths: BTreeSet<String> = BTreeSet::new();
        for edit in &self.edits {
            paths.extend(edit.paths());
        }
        paths.into_iter().collect()
    }

    /// The old→new mapping, rendered for the note the agent receives.
    pub(crate) fn describe(&self) -> String {
        self.movements
            .iter()
            .map(|movement| match movement {
                Movement::Renumbered {
                    lineage,
                    from,
                    to,
                    bare_literals,
                } => {
                    let moved = format!("- [{lineage}] `{from}` → `{to}`");
                    match (bare_literals.is_empty(), from.get(..VERSION_DIGITS)) {
                        (false, Some(version)) => format!(
                            "{moved}. `{version}` still appears as a bare literal at {}; number \
                             references do not follow a renumber — reference the migration by \
                             name.",
                            bare_literals.join(", ")
                        ),
                        _ => moved,
                    }
                }
                Movement::Advanced {
                    identifier,
                    from,
                    to,
                    base,
                    delta,
                    from_floor,
                    headings,
                } => {
                    let floor = if *from_floor {
                        " (recovered from a stored obligation: an earlier advance absorbed this \
                         increment)"
                    } else {
                        ""
                    };
                    let tally = match headings {
                        Some(HeadingTally { authored, absorbed }) => {
                            // The same order-preserving map `rewrite_constant`
                            // applies: authored headings land on base + 1 ..= to.
                            let renumbered = authored
                                .iter()
                                .zip(base + 1..)
                                .map(|(version, landed)| {
                                    if *version == landed {
                                        version.to_string()
                                    } else {
                                        format!("{version} → {landed}")
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!(
                                ". Authored by this branch: {renumbered}. Absorbed from the base \
                                 and not counted: {}.",
                                version_list(absorbed)
                            )
                        }
                        None => String::new(),
                    };
                    format!(
                        "- `{identifier}` {from} → {to}: base {base} + delta {delta}{floor}{tally}"
                    )
                }
                Movement::Reset {
                    identifier,
                    from,
                    to,
                    absorbed,
                } => {
                    let next = to + 1;
                    format!(
                        "- `{identifier}` {from} → {to}: base {to} + delta 0, put back on the \
                         base's value. Authored by this branch: none. Absorbed from the base and \
                         not counted: {}. This branch adds no `/// Version N` paragraph of its \
                         own, so the number it carried denotes no protocol change. If the branch \
                         does change the protocol, it takes {next} and needs a `/// Version \
                         {next} …` paragraph above the constant.",
                        version_list(absorbed)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// A refusal writes nothing at all. Each one names a condition under which
/// writing part of the edit set would be worse than writing none of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    pub reason: String,
}

impl Refusal {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

pub(crate) type PlanResult<T> = Result<T, Refusal>;

/// Durable record of one branch contribution. For an arithmetic constant it is
/// a recovery floor rather than a command: every advance recomputes from the
/// live fork and uses the larger delta, unless the branch is visibly below the
/// base (a retraction). For a heading-evidenced constant it is the latest count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Obligation {
    pub identifier: String,
    pub delta: u64,
    pub status: String,
    pub basis_fork_value: u64,
    pub basis_branch_value: u64,
    pub basis_base_value: u64,
    pub last_diagnostic: Option<String>,
}

/// Attribution for the exceptional, explicit release of an obligation that was
/// computed incorrectly. Automatic discharge uses an internal attribution too,
/// so every released gate retains who or what released it.
pub(crate) struct DischargeAttribution<'a> {
    pub actor_kind: &'a str,
    pub actor_identity: Option<&'a str>,
    pub surface: &'a str,
}

fn sql_integer(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("{field} {value} exceeds SQLite's integer range"))
}

pub(crate) async fn load_obligations(
    db: &LocalDb,
    project_id: &str,
    store_path: &std::path::Path,
    bookmark: &str,
) -> Result<Vec<Obligation>, String> {
    let project_id = project_id.to_string();
    let store_path = store_path.to_string_lossy().into_owned();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT identifier, delta, status, basis_fork_value,
                            basis_branch_value, basis_base_value, last_diagnostic
                     FROM contended_identifier_obligations
                     WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3
                       AND status IN ('outstanding', 'orphaned')
                     ORDER BY identifier",
                    params![project_id, store_path, bookmark],
                )
                .await?;
            let mut obligations = Vec::new();
            while let Some(row) = rows.next().await? {
                obligations.push(Obligation {
                    identifier: row.text(0)?,
                    delta: u64::try_from(row.i64(1)?).map_err(|_| {
                        crate::storage::DbError::internal("negative obligation delta")
                    })?,
                    status: row.text(2)?,
                    basis_fork_value: u64::try_from(row.i64(3)?).map_err(|_| {
                        crate::storage::DbError::internal("negative obligation fork")
                    })?,
                    basis_branch_value: u64::try_from(row.i64(4)?).map_err(|_| {
                        crate::storage::DbError::internal("negative obligation branch")
                    })?,
                    basis_base_value: u64::try_from(row.i64(5)?).map_err(|_| {
                        crate::storage::DbError::internal("negative obligation base")
                    })?,
                    last_diagnostic: row.opt_text(6)?,
                });
            }
            Ok(obligations)
        })
    })
    .await
    .map_err(|error| format!("load contended identifier obligations: {error}"))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_obligation(
    db: &LocalDb,
    project_id: &str,
    store_path: &std::path::Path,
    bookmark: &str,
    identifier: &str,
    delta: u64,
    fork: u64,
    branch: u64,
    base: u64,
    diagnostic: Option<&str>,
    keeps_floor: bool,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO contended_identifier_obligations
         (project_id, store_path, bookmark, identifier, delta, status,
          basis_fork_value, basis_branch_value, basis_base_value, last_diagnostic,
          created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'outstanding', ?6, ?7, ?8, ?9, ?10, ?10)
         ON CONFLICT(project_id, store_path, bookmark, identifier) DO UPDATE SET
           delta = CASE WHEN ?11 THEN max(contended_identifier_obligations.delta, excluded.delta)
                        ELSE excluded.delta END,
           status = 'outstanding',
           basis_fork_value = excluded.basis_fork_value,
           basis_branch_value = excluded.basis_branch_value,
           basis_base_value = excluded.basis_base_value,
           last_diagnostic = excluded.last_diagnostic,
           discharged_actor_kind = NULL,
           discharged_actor_identity = NULL,
           discharged_surface = NULL,
           discharged_at = NULL,
           updated_at = excluded.updated_at",
        params![
            project_id,
            store_path.to_string_lossy().as_ref(),
            bookmark,
            identifier,
            sql_integer(delta, "delta")?,
            sql_integer(fork, "fork value")?,
            sql_integer(branch, "branch value")?,
            sql_integer(base, "base value")?,
            diagnostic,
            now,
            i64::from(keeps_floor)
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist contended identifier obligation: {error}"))
}

pub(crate) async fn discharge_obligation(
    db: &LocalDb,
    project_id: &str,
    store_path: &std::path::Path,
    bookmark: &str,
    identifier: &str,
    attribution: DischargeAttribution<'_>,
) -> Result<bool, String> {
    let changed = db
        .execute(
            "UPDATE contended_identifier_obligations
             SET status = 'discharged', discharged_actor_kind = ?5,
                 discharged_actor_identity = ?6, discharged_surface = ?7,
                 discharged_at = ?8, updated_at = ?8
             WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3
               AND identifier = ?4 AND status = 'outstanding'",
            params![
                project_id,
                store_path.to_string_lossy().as_ref(),
                bookmark,
                identifier,
                attribution.actor_kind,
                attribution.actor_identity,
                attribution.surface,
                chrono::Utc::now().timestamp()
            ],
        )
        .await
        .map_err(|error| format!("discharge contended identifier obligation: {error}"))?;
    Ok(changed > 0)
}

pub(crate) async fn orphan_obligations(
    db: &LocalDb,
    project_id: &str,
    bookmark: &str,
    diagnostic: &str,
) -> Result<u64, String> {
    db.execute(
        "UPDATE contended_identifier_obligations
         SET status = 'orphaned', last_diagnostic = ?3, updated_at = ?4
         WHERE project_id = ?1 AND bookmark = ?2 AND status = 'outstanding'",
        params![
            project_id,
            bookmark,
            diagnostic,
            chrono::Utc::now().timestamp()
        ],
    )
    .await
    .map_err(|error| format!("orphan contended identifier obligations: {error}"))
}

/// Whether text carries literal three-way conflict markers.
///
/// A registry still holding markers cannot have a stem literal rewritten in it,
/// and renaming the `.sql` file anyway would turn a conflict a human can read
/// into a tree that does not compile.
pub(crate) fn has_conflict_markers(text: &str) -> bool {
    text.lines().any(|line| {
        line.starts_with("<<<<<<<") || line.starts_with("=======") || line.starts_with(">>>>>>>")
    })
}

/// The `NNNN` version at the head of a lineage stem.
pub(crate) fn version_of(stem: &str) -> Option<u32> {
    let bytes = stem.as_bytes();
    if bytes.len() <= VERSION_DIGITS || bytes[VERSION_DIGITS] != b'_' {
        return None;
    }
    stem.get(..VERSION_DIGITS)?.parse::<u32>().ok()
}

fn restem(stem: &str, version: u32) -> String {
    format!("{version:0VERSION_DIGITS$}{}", &stem[VERSION_DIGITS..])
}

/// The lineage stems registered in `text`, in the order they are registered.
///
/// A registration is the macro plus its stem literal and nothing else, so this
/// is the whole of what "registered" means — the version, the name, and the
/// embedded SQL are all derived from that one string.
///
/// The preceding-character guard is load-bearing: without it the private matcher
/// also matches the tail of `team_migration!`, and the two lineages would
/// renumber into each other.
fn registrations(text: &str, macro_name: &str) -> Vec<(usize, String)> {
    let needle = format!("{macro_name}!(");
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut cursor = 0usize;
    while let Some(offset) = text[cursor..].find(&needle) {
        let at = cursor + offset;
        cursor = at + needle.len();
        if at > 0 {
            let previous = bytes[at - 1];
            if previous.is_ascii_alphanumeric() || previous == b'_' {
                continue;
            }
        }
        let mut scan = cursor;
        while scan < bytes.len() && (bytes[scan] as char).is_whitespace() {
            scan += 1;
        }
        if scan >= bytes.len() || bytes[scan] != b'"' {
            continue;
        }
        let start = scan + 1;
        let Some(length) = text[start..].find('"') else {
            continue;
        };
        found.push((start, text[start..start + length].to_string()));
    }
    found
}

/// Reassign the numbers of branch-added lineage files that collide, rewriting
/// each stem literal **in place** in the registry.
///
/// `mine` is derived rather than passed: a stem present in the branch tree and
/// absent from the new base is, by construction, one this branch added — which
/// is also the renumbering precondition. `cairn_schema_migrations` keys on
/// `version TEXT PRIMARY KEY` and the runner skips any recorded version, so
/// renumbering an *applied* migration would make it run a second time. A durable
/// database only ever upgrades from base-branch builds, so it can only have
/// recorded versions that exist on the base; a stem absent from the base has
/// never been recorded by one. The precondition is therefore checked here, not
/// assumed, and it is the same rule that protects every hardcoded version
/// literal elsewhere in the tree.
///
/// Registry order is semantically meaningful and is not derived from the
/// numbers, so nothing is ever moved: only the four digits inside an existing
/// literal change.
///
/// The pass acts only when a collision exists — an addition whose number the
/// base holds, or that an earlier-registered addition of this branch already
/// claimed. When it acts, the branch's additions are numbered consecutively
/// above the base in registry order, and only those whose number changes move.
/// For a branch whose additions continued its fork in order, that reads as: the
/// additions before the first collision keep their numbers, and every addition
/// from the first collision onward moves, not just the colliding ones. Moving
/// only the colliding stem to the top would leave a dependent pair reading
/// `0255_a`, `0254_b`: gapless as a set, so no lane objects and apply order
/// (the registry) is unaffected, but a human reads the inversion as a defect
/// and renumbers by hand — exactly the error-prone multi-site edit this pass
/// exists to remove. The postcondition is therefore that the branch's
/// additions strictly increase in registry order, and that the lineage is
/// gapless whenever the base is.
///
/// It stays minimal where it matters: when two of this branch's own additions
/// claim a number that is otherwise free, the first registered keeps it.
/// Moving both would mint a hole, and `scripts/check-migrations.ts` requires
/// the sorted versions to run `0001..N`, so that would red the migrations lane
/// on a branch this pass had just resolved.
pub(crate) fn plan_lineage_renumber(
    lineage: &SequentialFileLineage,
    base_stems: &BTreeSet<String>,
    branch_stems: &BTreeSet<String>,
    registry_text: &str,
) -> PlanResult<Vec<(String, String)>> {
    let mut mine: Vec<&String> = branch_stems.difference(base_stems).collect();
    if mine.is_empty() {
        return Ok(Vec::new());
    }

    // Every part of the decision is made in registry order: which stem keeps a
    // contended number and which numbers the rest receive. The alphabetical
    // order a `BTreeSet` hands back is just as deterministic, so instability is
    // not the objection — it is that a filename is not evidence of anything.
    // Declaration order is the branch's own record of what it added first, and
    // renaming a lineage file does not move it, so no unrelated rename can
    // redirect the rewrite at a different migration. A stem with no
    // registration sorts last; if it turns out to collide, the check below
    // refuses before anything is planned.
    let positions: BTreeMap<String, usize> = registrations(registry_text, lineage.macro_name)
        .into_iter()
        .map(|(offset, stem)| (stem, offset))
        .collect();
    mine.sort_by_key(|stem| positions.get(stem.as_str()).copied().unwrap_or(usize::MAX));

    let taken: BTreeSet<u32> = base_stems
        .iter()
        .filter_map(|stem| version_of(stem))
        .collect();

    let mut mine_versions: Vec<u32> = Vec::with_capacity(mine.len());
    for stem in &mine {
        let Some(version) = version_of(stem) else {
            return Err(Refusal::new(format!(
                "[{}] `{stem}` is not `NNNN_name`, so its number cannot be reassigned",
                lineage.label
            )));
        };
        mine_versions.push(version);
    }

    // A stem collides when its number is already claimed on the base, or when
    // an earlier-registered addition of this branch's own already claimed it.
    // The second happens when two sibling branches merged into this one at
    // different times.
    let mut seen_within_mine: BTreeSet<u32> = BTreeSet::new();
    let collides = mine_versions
        .iter()
        .any(|version| taken.contains(version) || !seen_within_mine.insert(*version));
    if !collides {
        return Ok(Vec::new());
    }

    if has_conflict_markers(registry_text) {
        return Err(Refusal::new(format!(
            "`{}` still carries literal conflict markers, so a stem literal cannot be rewritten in \
             it. Renaming the lineage file anyway would leave a registration pointing at a stem \
             that no longer exists.",
            lineage.registry_file
        )));
    }

    // `mine` already follows the registry. An addition that sits on the next
    // number keeps it, which is every addition before the first collision when
    // the branch continued its fork in order; from the first that does not, the
    // rest follow consecutively.
    let mut next = taken.last().copied().unwrap_or(0);
    let mut assignments = Vec::new();
    for (stem, version) in mine.iter().zip(mine_versions) {
        next += 1;
        if version == next {
            continue;
        }
        if !positions.contains_key(stem.as_str()) {
            return Err(Refusal::new(format!(
                "[{}] `{stem}` has no `{}!(\"{stem}\")` registration in `{}`, so renumbering it \
                 would orphan the file",
                lineage.label, lineage.macro_name, lineage.registry_file
            )));
        }
        let renumbered = restem(stem, next);
        if renumbered.len() != stem.len() {
            return Err(Refusal::new(format!(
                "[{}] `{stem}` would renumber to `{renumbered}`, which is not {VERSION_DIGITS} \
                 digits wide",
                lineage.label
            )));
        }
        assignments.push((stem.to_string(), renumbered));
    }
    Ok(assignments)
}

/// The quoted spellings of each renumbered stem's old version, as fixed-string
/// needles for one search over the tree.
pub(crate) fn bare_version_needles(assignments: &[(String, String)]) -> Vec<String> {
    assignments
        .iter()
        .filter_map(|(from, _)| from.get(..VERSION_DIGITS))
        .flat_map(|version| [format!("\"{version}\""), format!("'{version}'")])
        .collect()
}

/// Where a test or script could name a migration by its bare number: Rust and
/// TypeScript under `src-tauri/` and `scripts/`, minus the lineage directory
/// and the registry, whose stems the pass rewrites itself.
pub(crate) fn bare_version_pathspecs(lineage: &SequentialFileLineage) -> Vec<String> {
    let mut pathspecs: Vec<String> = ["src-tauri", "scripts"]
        .iter()
        .flat_map(|root| ["rs", "ts"].map(|extension| format!(":(glob){root}/**/*.{extension}")))
        .collect();
    pathspecs.push(format!(":(exclude){}", lineage.dir));
    pathspecs.push(format!(":(exclude){}", lineage.registry_file));
    pathspecs
}

/// The `path:line` sites among `hits` that spell `stem`'s version as a bare
/// quoted literal.
pub(crate) fn bare_literal_sites(stem: &str, hits: &[crate::jj::GrepHit]) -> Vec<String> {
    let Some(version) = stem.get(..VERSION_DIGITS) else {
        return Vec::new();
    };
    let quoted = [format!("\"{version}\""), format!("'{version}'")];
    hits.iter()
        .filter(|hit| {
            quoted
                .iter()
                .any(|needle| hit.text.contains(needle.as_str()))
        })
        .map(|hit| format!("{}:{}", hit.path, hit.line))
        .collect()
}

/// Rewrite each reassigned stem literal in place, leaving every other byte —
/// and the registration order — untouched.
pub(crate) fn rewrite_registrations(
    registry_text: &str,
    macro_name: &str,
    assignments: &[(String, String)],
) -> PlanResult<String> {
    let mut text = registry_text.to_string();
    for (old, new) in assignments {
        let sites: Vec<usize> = registrations(&text, macro_name)
            .into_iter()
            .filter(|(_, stem)| stem == old)
            .map(|(offset, _)| offset)
            .collect();
        match sites.len() {
            1 => {
                let at = sites[0];
                text.replace_range(at..at + old.len(), new);
            }
            0 => {
                return Err(Refusal::new(format!(
                "expected exactly one `{macro_name}!(\"{old}\")` registration to rewrite, found \
                     none"
            )))
            }
            found => {
                return Err(Refusal::new(format!(
                "expected exactly one `{macro_name}!(\"{old}\")` registration to rewrite, found \
                     {found}"
            )))
            }
        }
    }
    Ok(text)
}

/// What the resolver concluded for one monotonic constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConstantResolution {
    /// The branch's own increment, after the recovery floor is considered.
    pub delta: u64,
    /// The value the branch should carry on the new base. For an arithmetic
    /// constant it is meaningless when `delta` is zero, because a branch that
    /// contributes nothing is left alone; a heading-evidenced constant is
    /// always converged onto it.
    pub target: u64,
    /// The stored floor supplied the delta because the live tree no longer
    /// could.
    pub from_floor: bool,
    /// The branch sits BELOW the base, which no absorption can produce and only
    /// a deliberate edit can. Its obligation is wrong and is discharged.
    pub retracted: bool,
}

/// Carry the branch's delta onto the base's current value, never its absolute
/// value. This is the [`DeltaEvidence::Arithmetic`] rule; the executor protocol
/// version is resolved by [`resolve_by_evidence`] instead, because a version
/// that no paragraph describes is not an increment worth preserving and the
/// floor below would preserve it forever.
///
/// `fork` is the value at the fork point the branch left, `pre_tip` the value at
/// the branch tip before this rebase, `base` the value on the new base. Correct
/// across every case: the identical-edit collapse (`fork` 87, `pre_tip` and
/// `base` both 88 → delta 1, target 89, recovering an increment a plain merge
/// would have swallowed); a branch that bumped while the base did not (`base` ==
/// `fork`, target == `pre_tip`, so nothing changes); and a base that bumped while
/// the branch did not (delta 0, no-op).
///
/// `stored_delta` is a **recovery floor, not a replayed command**. Replaying it
/// verbatim would overwrite a branch that touched the constant again after the
/// obligation was written, so the live computation is redone every time and
/// `max(live, stored)` is taken. The floor is what survives the case the live
/// computation cannot see: once an advance absorbs the increment, the fork point
/// carries the absorbed value and the live delta is zero *because* the evidence
/// was destroyed.
///
/// The one input where the stored value and the live value disagree and the
/// live one is right is a **deliberate retraction**: the agent decided there was
/// no wire change after all and put the constant back. That reads identically to
/// a swallowed increment in the delta arithmetic, but not in the values — a
/// swallowed increment leaves the branch EQUAL to the base, while a retraction
/// leaves it BELOW. So the floor fires only when the branch has not visibly
/// retracted.
pub(crate) fn resolve_monotonic(
    fork: u64,
    pre_tip: u64,
    base: u64,
    stored_delta: Option<u64>,
) -> ConstantResolution {
    let live = pre_tip.saturating_sub(fork);
    let retracted = pre_tip < base;
    let floor = if retracted {
        0
    } else {
        stored_delta.unwrap_or(0)
    };
    let delta = live.max(floor);
    ConstantResolution {
        delta,
        target: base.saturating_add(delta),
        from_floor: !retracted && floor > live,
        retracted,
    }
}

/// The [`DeltaEvidence::AuthoredHeadings`] rule: the branch lands one version
/// above the base per heading it authored, and on the base's own value when it
/// authored none. There is no floor and no retraction case, because the input
/// is the text the branch carries rather than a number it might have lost.
pub(crate) fn resolve_by_evidence(authored: &[u64], base: u64) -> ConstantResolution {
    let delta = authored.len() as u64;
    ConstantResolution {
        delta,
        target: base.saturating_add(delta),
        from_floor: false,
        retracted: false,
    }
}

/// The version a doc heading line declares, and the byte range of its digits.
///
/// A heading is a top-level doc line whose text begins `{prefix}N` — `/// Version
/// 135 adds …`. Only the start of the doc text counts, so prose mentioning a
/// version mid-sentence is not a heading, and neither is `/// Versions 101–111
/// …`, which is how the ledger records numbers that denote no change. The digit
/// run is read whole, so `Version 13` never matches inside `Version 135`.
///
/// `scripts/check-executor-protocol.ts` states the same shape for the authoring
/// side; `fixtures/executor-protocol-ledger-cases.json` holds both to it.
fn heading_digits(line: &str, prefix: &str) -> Option<(std::ops::Range<usize>, u64)> {
    let doc = line.strip_prefix("///")?;
    let rest = doc.trim_start_matches([' ', '\t']).strip_prefix(prefix)?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let start = line.len() - rest.len();
    let value = rest[..digits].parse().ok()?;
    Some((start..start + digits, value))
}

/// Every heading line in `text`, grouped by the version it declares.
fn heading_lines<'a>(text: &'a str, prefix: &str) -> BTreeMap<u64, BTreeSet<&'a str>> {
    let mut lines: BTreeMap<u64, BTreeSet<&str>> = BTreeMap::new();
    for line in text.lines() {
        if let Some((_, version)) = heading_digits(line, prefix) {
            lines.entry(version).or_default().insert(line);
        }
    }
    lines
}

/// Every version a heading in `text` declares.
pub(crate) fn heading_versions(text: &str, prefix: &str) -> BTreeSet<u64> {
    heading_lines(text, prefix).into_keys().collect()
}

/// The versions the branch itself headed, ascending: declared by a heading on
/// the tip, by no heading at the fork, and not merely copied from the base.
///
/// The fork comparison is by number, so rewording an inherited paragraph is not
/// mistaken for a new version. The base comparison is by line. A tip that took
/// in the base's paragraph — by a hand merge, a union rebase, or a resolved
/// conflict — carries it byte for byte, and counting it would add the base's
/// own bump to the branch's: the inflation that put spurious numbers on branch
/// after branch. A branch that wrote its own paragraph under the number the base
/// also took never matches the base's line, so it still counts, and is
/// renumbered above the base. A version is excluded only when every one of its
/// tip headings is on the base; a tip carrying the base's paragraph and its own
/// under one number has authored that number.
pub(crate) fn authored_versions(
    tip_text: &str,
    fork_text: &str,
    base_text: &str,
    prefix: &str,
) -> Vec<u64> {
    let inherited = heading_versions(fork_text, prefix);
    let base = heading_lines(base_text, prefix);
    heading_lines(tip_text, prefix)
        .into_iter()
        .filter(|(version, lines)| {
            !inherited.contains(version)
                && !base
                    .get(version)
                    .is_some_and(|base_lines| lines.is_subset(base_lines))
        })
        .map(|(version, _)| version)
        .collect()
}

/// Whether landing a branch would put an undocumented or misnumbered version
/// on its destination, as the reason to refuse; `None` when it would not.
///
/// `branch_text`, `fork_text` and `dest_text` are the constant's file at the
/// branch tip, at its live merge-base with the destination, and at the
/// destination tip. A heading is authored by the rule of [`authored_versions`],
/// with the destination as the base, so a branch that merged in the
/// destination's own paragraph is not charged with writing it. The rule, for
/// heading-evidenced constants only:
///
/// - No authored heading: the branch must not move the constant. It may carry
///   the fork's value or the destination's; either merges to the destination's.
/// - Exactly one authored heading `N`: the constant and the heading must both
///   read the destination's value plus one.
/// - More than one: refused. One change takes one version.
///
/// The merge gate, `scripts/check-executor-protocol.ts` and the shared fixture
/// table all apply this one rule. A file that cannot be read fails closed.
pub(crate) fn ledger_violation(
    branch_text: &str,
    fork_text: &str,
    dest_text: &str,
    constant: &MonotonicConstant,
) -> Option<String> {
    let prefix = constant.evidence.heading_prefix()?;
    let name = constant.constant;
    let values = [
        (branch_text, "branch tip"),
        (fork_text, "fork point"),
        (dest_text, "destination"),
    ]
    .map(|(text, side)| read_constant(text, name).ok_or(side));
    let [branch, fork, dest] = match values {
        [Ok(branch), Ok(fork), Ok(dest)] => [branch, fork, dest],
        _ => {
            let side = values.iter().find_map(|value| value.err()).unwrap_or("");
            return Some(format!(
                "`{name}` could not be read at the {side} in the shape `pub const {name}: u32 = N;`"
            ));
        }
    };
    let next = dest + 1;
    let heading = prefix.trim_end();
    let authored = authored_versions(branch_text, fork_text, dest_text, prefix);
    match authored.as_slice() {
        [] if branch == fork || branch == dest => None,
        [] => Some(format!(
            "`{name}` reads {branch} on this branch but {fork} where it left the destination, and \
             the branch adds no `/// {heading} N` paragraph, so the move denotes no protocol \
             change; landing it would publish {branch} as a version nothing describes. If this \
             branch does not change the executor protocol, put `{name}` back to {dest}. If it \
             does, set it to {next} and add a `/// {prefix}{next} …` paragraph above it saying \
             what an older peer gets wrong."
        )),
        [version] if *version != next => Some(format!(
            "this branch's `/// {prefix}{version}` paragraph takes {version}, but the destination \
             is at {dest}, so the next version is {next}. Renumber the paragraph and `{name}` to \
             {next}."
        )),
        [version] if branch != *version => Some(format!(
            "this branch's paragraph is headed `{prefix}{version}` but `{name}` reads {branch}; \
             set it to {version}."
        )),
        [_] => None,
        many => Some(format!(
            "this branch adds `/// {heading} N` paragraphs for {}; one change takes one version. \
             Fold them into a single `/// {prefix}{next}` paragraph and set `{name}` to {next}.",
            many.iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// The merge gate's reading of [`ledger_violation`]: the constant's file at the
/// source bookmark, at its live merge-base with the target, and at the target.
///
/// This is the hard invariant. It does not depend on a check verdict being
/// fresh or on base-advance having run, so a branch that never met either still
/// cannot land a version nothing describes. A repository that does not declare
/// the constant is not subject to it.
pub(crate) fn protocol_ledger_refusal(
    jj: &crate::jj::JjEnv,
    store: &std::path::Path,
    source_branch: &str,
    target_branch: &str,
) -> Result<Option<String>, String> {
    let dest = crate::jj::bookmark_commit(jj, store, target_branch)
        .ok_or_else(|| format!("bookmark `{target_branch}` did not resolve to one commit"))?;
    let mut declared = Vec::new();
    for constant in MONOTONIC_CONSTANTS {
        if constant.evidence.heading_prefix().is_none() {
            continue;
        }
        if let Some(dest_text) = crate::jj::text_at_revision(jj, store, &dest, constant.file)? {
            declared.push((constant, dest_text));
        }
    }
    if declared.is_empty() {
        return Ok(None);
    }
    let source = crate::jj::bookmark_commit(jj, store, source_branch)
        .ok_or_else(|| format!("bookmark `{source_branch}` did not resolve to one commit"))?;
    let fork = crate::jj::revset_commit(jj, store, &format!("heads(::{source} & ::{dest})"))
        .ok_or_else(|| {
            format!("`{source_branch}` and `{target_branch}` have no single merge-base")
        })?;
    for (constant, dest_text) in declared {
        let read = |revision: &str| {
            crate::jj::text_at_revision(jj, store, revision, constant.file)?
                .ok_or_else(|| format!("`{}` is absent at `{revision}`", constant.file))
        };
        let branch_text = read(&source)?;
        let fork_text = read(&fork)?;
        if let Some(violation) = ledger_violation(&branch_text, &fork_text, &dest_text, constant) {
            return Ok(Some(violation));
        }
    }
    Ok(None)
}

/// Read a `pub const NAME: u32 = N;` declaration's value.
pub(crate) fn read_constant(text: &str, name: &str) -> Option<u64> {
    let (_, value) = constant_site(text, name)?;
    Some(value)
}

/// The byte range of a constant's literal, and its value.
fn constant_site(text: &str, name: &str) -> Option<(std::ops::Range<usize>, u64)> {
    for (offset, line) in line_offsets(text) {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("pub const ") {
            continue;
        }
        let Some(rest) = trimmed.strip_prefix("pub const ") else {
            continue;
        };
        let Some(rest) = rest.strip_prefix(name) else {
            continue;
        };
        if !rest.starts_with(':') {
            continue;
        }
        let equals = rest.find('=')?;
        let value_start = rest[equals + 1..].len() - rest[equals + 1..].trim_start().len();
        let literal_start = offset
            + (line.len() - trimmed.len())
            + "pub const ".len()
            + name.len()
            + equals
            + 1
            + value_start;
        let literal = text[literal_start..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>();
        if literal.is_empty() {
            continue;
        }
        let value = literal.parse::<u64>().ok()?;
        return Some((literal_start..literal_start + literal.len(), value));
    }
    None
}

fn line_offsets(text: &str) -> Vec<(usize, &str)> {
    let mut offsets = Vec::new();
    let mut cursor = 0usize;
    for line in text.split_inclusive('\n') {
        let stripped = line.strip_suffix('\n').unwrap_or(line);
        offsets.push((cursor, stripped));
        cursor += line.len();
    }
    offsets
}

/// Rewrite a monotonic constant's declaration to `target`, and renumber the
/// doc headings the BRANCH itself authored to match.
///
/// A version heading is a versioned history: the branch wrote a paragraph headed
/// "Version 88 adds …" alongside its bump. Rewriting the constant without the
/// heading leaves the file lying about itself. `authored` is ascending, and its
/// headings are mapped in order onto the versions just below and up to `target`
/// — for heading evidence `target` is `base + authored.len()`, so they land on
/// `base + 1 ..= target`. Only lines absent from every `inherited` text (the
/// fork and the new base) are touched: a heading the base already carries is the
/// base's own history, even when its number equals one the branch authored.
/// An arithmetic constant has no headings and passes `authored` empty.
pub(crate) fn rewrite_constant(
    text: &str,
    inherited: &[&str],
    constant: &MonotonicConstant,
    authored: &[u64],
    target: u64,
) -> PlanResult<String> {
    let Some((range, _current)) = constant_site(text, constant.constant) else {
        return Err(Refusal::new(format!(
            "`{}` was not found in `{}` in the shape `pub const {}: u32 = N;`",
            constant.constant, constant.file, constant.constant
        )));
    };
    let mut rewritten = text.to_string();
    rewritten.replace_range(range, &target.to_string());

    let Some(prefix) = constant.evidence.heading_prefix() else {
        return Ok(rewritten);
    };
    if authored.is_empty() {
        return Ok(rewritten);
    }
    let first = target + 1 - authored.len() as u64;
    let renumbered: BTreeMap<u64, u64> = authored
        .iter()
        .enumerate()
        .map(|(index, version)| (*version, first + index as u64))
        .collect();
    let inherited: BTreeSet<&str> = inherited.iter().flat_map(|text| text.lines()).collect();
    let moved: Vec<String> = rewritten
        .lines()
        .map(|line| {
            if inherited.contains(line) {
                return line.to_string();
            }
            match heading_digits(line, prefix) {
                Some((digits, version)) => match renumbered.get(&version) {
                    Some(to) => {
                        let mut moved = line.to_string();
                        moved.replace_range(digits, &to.to_string());
                        moved
                    }
                    None => line.to_string(),
                },
                None => line.to_string(),
            }
        })
        .collect();
    let trailing = if rewritten.ends_with('\n') { "\n" } else { "" };
    Ok(format!("{}{trailing}", moved.join("\n")))
}

/// Move a companion file's mirrored literal in the same edit set.
pub(crate) fn rewrite_companion(
    text: &str,
    companion: &CompanionLiteral,
    current: u64,
    target: u64,
) -> PlanResult<String> {
    let needle = format!("{}{current}", companion.prefix);
    let sites: Vec<usize> = text.match_indices(&needle).map(|(at, _)| at).collect();
    let sites: Vec<usize> = sites
        .into_iter()
        .filter(|at| {
            text[at + needle.len()..]
                .chars()
                .next()
                .is_none_or(|c| !c.is_ascii_digit())
        })
        .collect();
    match sites.len() {
        1 => {
            let mut rewritten = text.to_string();
            let at = sites[0];
            rewritten.replace_range(
                at..at + needle.len(),
                &format!("{}{target}", companion.prefix),
            );
            Ok(rewritten)
        }
        found => Err(Refusal::new(format!(
            "expected exactly one `{needle}` in `{}` to move alongside the constant, found {found}",
            companion.file
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stems(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// Assert the property `scripts/check-migrations.ts` enforces downstream:
    /// once the plan is applied, the lineage's versions are distinct and
    /// contiguous from its lowest. The guard requires each version to equal the
    /// file's one-based position, so a renumbering pass that left a duplicate
    /// or minted a hole would red the migrations lane on the very branch it had
    /// just resolved.
    fn assert_resolved_lineage_is_gapless(branch: &[&str], assignments: &[(String, String)]) {
        let renamed: BTreeMap<&str, &str> = assignments
            .iter()
            .map(|(from, to)| (from.as_str(), to.as_str()))
            .collect();
        let mut versions: Vec<u32> = branch
            .iter()
            .map(|stem| renamed.get(stem).copied().unwrap_or(stem))
            .map(|stem| version_of(stem).expect("stem carries a version"))
            .collect();
        versions.sort_unstable();
        for pair in versions.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "resolved lineage is not gapless: {versions:?} from {branch:?} via {assignments:?}"
            );
        }
    }

    /// The lineage facts are stated twice — here for the resolving side, and in
    /// `scripts/check-migrations.ts` for the validating side. A codegen pipeline
    /// for three constants would cost more than it saves, so drift is caught
    /// loudly instead.
    #[test]
    fn the_lineage_registry_agrees_with_the_typescript_guard() {
        assert!(
            MIGRATION_CHECK_SCRIPT.contains(&format!("\"{MIGRATION_REGISTRY}\"")),
            "check-migrations.ts no longer names {MIGRATION_REGISTRY} as the registry"
        );
        for lineage in SEQUENTIAL_FILE_LINEAGES {
            assert!(
                MIGRATION_CHECK_SCRIPT.contains(&format!("\"{}\"", lineage.dir)),
                "check-migrations.ts no longer names {} as a lineage directory",
                lineage.dir
            );
            assert!(
                MIGRATION_CHECK_SCRIPT.contains(&format!("{}!", lineage.macro_name)),
                "check-migrations.ts no longer matches the {} macro",
                lineage.macro_name
            );
        }
    }

    #[test]
    fn a_branch_addition_that_collides_takes_the_next_free_number() {
        let registry = "migration!(\"0227_attachments\"),\nmigration!(\"0228_mine\"),\n";
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0227_attachments", "0228_theirs"]),
            &stems(&["0227_attachments", "0228_theirs", "0228_mine"]),
            registry,
        )
        .expect("planned");
        assert_eq!(
            assignments,
            vec![("0228_mine".to_string(), "0229_mine".to_string())]
        );
        assert_resolved_lineage_is_gapless(
            &["0227_attachments", "0228_theirs", "0228_mine"],
            &assignments,
        );
    }

    #[test]
    fn a_branch_addition_that_does_not_collide_is_left_alone() {
        let registry = "migration!(\"0228_mine\"),\n";
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0227_attachments"]),
            &stems(&["0227_attachments", "0228_mine"]),
            registry,
        )
        .expect("planned");
        assert!(assignments.is_empty(), "{assignments:?}");
    }

    #[test]
    fn assignment_order_follows_the_registry_not_the_filename() {
        // `0228` is free on this base, so exactly one of the two additions has
        // to move: renumbering both would leave 0227, 0229, 0230 and a hole
        // where 0228 used to be. `0228_zeta` registers first, so it is the one
        // that keeps the number, even though `0228_alpha` sorts before it.
        let registry = "migration!(\"0228_zeta\"),\nmigration!(\"0228_alpha\"),\n";
        let branch = ["0227_base", "0228_alpha", "0228_zeta"];
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0227_base"]),
            &stems(&branch),
            registry,
        )
        .expect("planned");
        assert_eq!(
            assignments,
            vec![("0228_alpha".to_string(), "0229_alpha".to_string())]
        );
        assert_resolved_lineage_is_gapless(&branch, &assignments);
    }

    #[test]
    fn renaming_a_lineage_file_does_not_change_which_registration_moves() {
        // Two plans with identical registry structure — first registration,
        // then second, both claiming a `0228` the base does not hold — whose
        // files have been renamed by swapping their names between the two
        // positions. That inverts the alphabetical relation between them while
        // leaving every registration position where it was, so if a filename
        // reached the decision at all the two plans would disagree. The second
        // registration is the one that moves either way: renaming a lineage
        // file cannot redirect the rewrite at a different migration.
        for (first, second) in [
            ("0228_widget", "0228_ledger"),
            ("0228_ledger", "0228_widget"),
        ] {
            let registry = format!("migration!(\"{first}\"),\nmigration!(\"{second}\"),\n");
            let branch = ["0227_base", first, second];
            let assignments = plan_lineage_renumber(
                &SEQUENTIAL_FILE_LINEAGES[0],
                &stems(&["0227_base"]),
                &stems(&branch),
                &registry,
            )
            .expect("planned");
            assert_eq!(
                assignments,
                vec![(second.to_string(), restem(second, 229))],
                "the second registration moves, whatever the two files are called"
            );
            assert_resolved_lineage_is_gapless(&branch, &assignments);
        }
    }

    #[test]
    fn two_additions_on_a_number_the_base_also_holds_both_move() {
        // Nothing may keep `0228` here: the base holds it, so a durable
        // database may already have recorded it. Both additions move, and the
        // pair of numbers they take leaves the lineage gapless.
        let registry = "migration!(\"0228_zeta\"),\nmigration!(\"0228_alpha\"),\n";
        let branch = ["0227_base", "0228_theirs", "0228_alpha", "0228_zeta"];
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0227_base", "0228_theirs"]),
            &stems(&branch),
            registry,
        )
        .expect("planned");
        assert_eq!(
            assignments,
            vec![
                ("0228_zeta".to_string(), "0229_zeta".to_string()),
                ("0228_alpha".to_string(), "0230_alpha".to_string()),
            ]
        );
        assert_resolved_lineage_is_gapless(&branch, &assignments);
    }

    #[test]
    fn a_base_collision_moves_every_later_addition_so_a_dependent_pair_stays_in_order() {
        // `b` depends on `a` and registers after it. Moving only the colliding
        // `a` to the top would read 0253_x, 0255_a, 0254_b: gapless as a set,
        // and an inversion a human renumbers by hand.
        let registry = "migration!(\"0253_x\"),\nmigration!(\"0253_a\"),\nmigration!(\"0254_b\"),\n";
        let branch = ["0252_base", "0253_x", "0253_a", "0254_b"];
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0252_base", "0253_x"]),
            &stems(&branch),
            registry,
        )
        .expect("planned");
        assert_eq!(
            assignments,
            vec![
                ("0253_a".to_string(), "0254_a".to_string()),
                ("0254_b".to_string(), "0255_b".to_string()),
            ]
        );
        assert_resolved_lineage_is_gapless(&branch, &assignments);
    }

    #[test]
    fn a_collision_in_the_middle_keeps_what_precedes_it_and_moves_what_follows() {
        // A sibling merged in `0251_b` beside this branch's `0251_a`, and `c`
        // was added after both. `a` keeps 0251; `b` and `c` take 0252 and
        // 0253 in registry order rather than `b` jumping over `c`.
        let registry =
            "migration!(\"0251_a\"),\nmigration!(\"0251_b\"),\nmigration!(\"0252_c\"),\n";
        let branch = ["0250_base", "0251_a", "0251_b", "0252_c"];
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0250_base"]),
            &stems(&branch),
            registry,
        )
        .expect("planned");
        assert_eq!(
            assignments,
            vec![
                ("0251_b".to_string(), "0252_b".to_string()),
                ("0252_c".to_string(), "0253_c".to_string()),
            ]
        );
        assert_resolved_lineage_is_gapless(&branch, &assignments);
    }

    /// Every branch over a small gapless base: when the pass acts, the base is
    /// untouched, the branch's additions strictly increase in registry order,
    /// and the lineage is gapless. Where the additions before the first
    /// collision continue the base in order — any branch that was gapless and
    /// ordered before the base advanced — the plan is exactly "keep everything
    /// before the first collision, number the rest consecutively from the top".
    #[test]
    fn renumbering_postconditions_hold_for_every_small_lineage() {
        fn sequences(length: usize, values: u32) -> Vec<Vec<u32>> {
            if length == 0 {
                return vec![Vec::new()];
            }
            sequences(length - 1, values)
                .into_iter()
                .flat_map(|prefix| {
                    (1..=values).map(move |value| {
                        let mut next = prefix.clone();
                        next.push(value);
                        next
                    })
                })
                .collect()
        }

        let mut acted = 0usize;
        for base_len in 1..=3u32 {
            let base: Vec<String> = (1..=base_len).map(|v| format!("{v:04}_base")).collect();
            for length in 1..=4 {
                for versions in sequences(length, base_len + 4) {
                    let mine: Vec<String> = versions
                        .iter()
                        .enumerate()
                        .map(|(index, v)| format!("{v:04}_m{index}"))
                        .collect();
                    let registry: String = base
                        .iter()
                        .chain(&mine)
                        .map(|stem| format!("migration!(\"{stem}\"),\n"))
                        .collect();
                    let base_set: BTreeSet<String> = base.iter().cloned().collect();
                    let branch_set: BTreeSet<String> = base.iter().chain(&mine).cloned().collect();
                    let assignments = plan_lineage_renumber(
                        &SEQUENTIAL_FILE_LINEAGES[0],
                        &base_set,
                        &branch_set,
                        &registry,
                    )
                    .expect("planned");

                    let mut seen = BTreeSet::new();
                    let first_collision = versions
                        .iter()
                        .position(|v| *v <= base_len || !seen.insert(*v));
                    let Some(first_collision) = first_collision else {
                        assert!(assignments.is_empty(), "{versions:?} {assignments:?}");
                        continue;
                    };
                    acted += 1;

                    let renamed: BTreeMap<&str, &str> = assignments
                        .iter()
                        .map(|(from, to)| (from.as_str(), to.as_str()))
                        .collect();
                    assert!(
                        renamed.keys().all(|from| !base_set.contains(*from)),
                        "a base stem moved: {assignments:?}"
                    );
                    let landed: Vec<u32> = mine
                        .iter()
                        .map(|stem| renamed.get(stem.as_str()).copied().unwrap_or(stem))
                        .map(|stem| version_of(stem).unwrap())
                        .collect();
                    assert!(
                        landed.windows(2).all(|pair| pair[0] < pair[1]),
                        "additions not increasing: {versions:?} -> {landed:?}"
                    );
                    let all: Vec<&str> = base.iter().chain(&mine).map(String::as_str).collect();
                    assert_resolved_lineage_is_gapless(&all, &assignments);

                    let kept = &versions[..first_collision];
                    if kept.iter().copied().eq(base_len + 1..=base_len + kept.len() as u32) {
                        let start = base_len + kept.len() as u32 + 1;
                        let expected: Vec<(String, String)> = mine[first_collision..]
                            .iter()
                            .zip(start..)
                            .filter(|(stem, to)| version_of(stem) != Some(*to))
                            .map(|(stem, to)| (stem.clone(), restem(stem, to)))
                            .collect();
                        assert_eq!(assignments, expected, "{versions:?}");
                    }
                }
            }
        }
        assert!(acted > 1000, "the enumeration exercised only {acted} plans");
    }

    #[test]
    fn a_bare_literal_of_the_old_version_is_named_in_the_note() {
        let hits = [
            crate::jj::GrepHit {
                path: "src-tauri/a_test.rs".into(),
                line: 7,
                text: "assert_eq!(v, \"0256\");".into(),
            },
            crate::jj::GrepHit {
                path: "scripts/b.test.ts".into(),
                line: 2,
                text: "expect(x).toBe('0256')".into(),
            },
            crate::jj::GrepHit {
                path: "src-tauri/c.rs".into(),
                line: 1,
                text: "let other = \"0257\";".into(),
            },
        ];
        let assignments = [("0256_a".to_string(), "0258_a".to_string())];
        assert_eq!(
            bare_version_needles(&assignments),
            vec!["\"0256\"".to_string(), "'0256'".to_string()]
        );
        let sites = bare_literal_sites("0256_a", &hits);
        assert_eq!(sites, vec!["src-tauri/a_test.rs:7", "scripts/b.test.ts:2"]);
        let plan = ResolutionPlan {
            movements: vec![Movement::Renumbered {
                lineage: "private".into(),
                from: "0256_a".into(),
                to: "0258_a".into(),
                bare_literals: sites,
            }],
            ..ResolutionPlan::default()
        };
        assert_eq!(
            plan.describe(),
            "- [private] `0256_a` → `0258_a`. `0256` still appears as a bare literal at \
             src-tauri/a_test.rs:7, scripts/b.test.ts:2; number references do not follow a \
             renumber — reference the migration by name."
        );
    }

    #[test]
    fn a_stem_on_the_base_is_never_renumbered() {
        // The applied-migration hazard: a version a durable database could
        // already have recorded must not move. Everything on the base is such a
        // version, and `mine` excludes it by construction.
        let registry = "migration!(\"0227_shared\"),\n";
        let assignments = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0227_shared"]),
            &stems(&["0227_shared"]),
            registry,
        )
        .expect("planned");
        assert!(assignments.is_empty());
    }

    #[test]
    fn the_team_matcher_does_not_claim_private_registrations() {
        let registry = "migration!(\"0228_private\"),\nteam_migration!(\"0008_team\"),\n";
        let private = registrations(registry, "migration");
        assert_eq!(
            private.iter().map(|(_, s)| s.as_str()).collect::<Vec<_>>(),
            vec!["0228_private"]
        );
        let team = registrations(registry, "team_migration");
        assert_eq!(
            team.iter().map(|(_, s)| s.as_str()).collect::<Vec<_>>(),
            vec!["0008_team"]
        );
    }

    #[test]
    fn a_multi_line_registration_form_is_still_found() {
        let registry = "migration!(\n    \"0228_mine\",\n    rebuild_fk_off: &[]\n),\n";
        let found = registrations(registry, "migration");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1, "0228_mine");
    }

    #[test]
    fn a_conflicted_registry_refuses_rather_than_orphaning_a_rename() {
        let registry = "<<<<<<< base\nmigration!(\"0228_mine\"),\n=======\n>>>>>>> theirs\n";
        let refusal = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0228_theirs"]),
            &stems(&["0228_theirs", "0228_mine"]),
            registry,
        )
        .expect_err("refused");
        assert!(refusal.reason.contains("conflict markers"), "{refusal:?}");
    }

    #[test]
    fn an_unregistered_addition_refuses_rather_than_orphaning_the_file() {
        let refusal = plan_lineage_renumber(
            &SEQUENTIAL_FILE_LINEAGES[0],
            &stems(&["0228_theirs"]),
            &stems(&["0228_theirs", "0228_mine"]),
            "// nothing registered here\n",
        )
        .expect_err("refused");
        assert!(refusal.reason.contains("no `migration!"), "{refusal:?}");
    }

    #[test]
    fn rewriting_a_stem_moves_only_its_digits_and_keeps_registry_order() {
        let registry =
            "migration!(\"0227_a\"),\nmigration!(\"0228_mine\"),\nmigration!(\"0226_b\"),\n";
        let rewritten = rewrite_registrations(
            registry,
            "migration",
            &[("0228_mine".to_string(), "0229_mine".to_string())],
        )
        .expect("rewritten");
        assert_eq!(
            rewritten,
            "migration!(\"0227_a\"),\nmigration!(\"0229_mine\"),\nmigration!(\"0226_b\"),\n"
        );
    }

    #[test]
    fn the_identical_edit_collapse_recovers_the_lost_increment() {
        // Both branches bumped 87 → 88, so three-way merge absorbed one of them
        // without ever reporting a conflict.
        let resolved = resolve_monotonic(87, 88, 88, None);
        assert_eq!(resolved.delta, 1);
        assert_eq!(resolved.target, 89);
        assert!(!resolved.from_floor);
    }

    #[test]
    fn a_branch_only_bump_lands_on_its_own_value() {
        let resolved = resolve_monotonic(87, 88, 87, None);
        assert_eq!(resolved.delta, 1);
        assert_eq!(resolved.target, 88);
    }

    #[test]
    fn a_base_only_bump_is_a_no_op() {
        let resolved = resolve_monotonic(87, 87, 88, None);
        assert_eq!(resolved.delta, 0);
    }

    #[test]
    fn two_increments_from_one_branch_stay_two() {
        let resolved = resolve_monotonic(87, 89, 89, None);
        assert_eq!(resolved.delta, 2);
        assert_eq!(resolved.target, 91);
    }

    #[test]
    fn a_live_recomputation_supersedes_a_stale_stored_delta() {
        // The branch bumped again after the obligation was written, so the
        // stored 1 is stale and the live 2 wins.
        let resolved = resolve_monotonic(87, 89, 89, Some(1));
        assert_eq!(resolved.delta, 2);
        assert!(!resolved.from_floor);
    }

    #[test]
    fn a_swallowed_increment_is_recovered_from_the_stored_floor() {
        // A missed advance published the branch at 88 and moved the fork point
        // to 88, so the live delta is zero precisely because the evidence was
        // destroyed. The floor is the only thing that still knows.
        let resolved = resolve_monotonic(88, 88, 88, Some(1));
        assert_eq!(resolved.delta, 1);
        assert_eq!(resolved.target, 89);
        assert!(resolved.from_floor);
    }

    #[test]
    fn a_deliberate_retraction_is_not_overridden_by_the_floor() {
        // The agent decided there was no wire change after all and put the
        // constant back. A branch BELOW the base is something no absorption can
        // produce, so it is a retraction and its obligation is wrong.
        let resolved = resolve_monotonic(88, 87, 88, Some(1));
        assert_eq!(resolved.delta, 0);
        assert!(resolved.retracted);
        assert!(!resolved.from_floor);
    }

    #[test]
    fn a_constant_declaration_is_read_and_moved_in_place() {
        let text = "/// docs\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 89;\n";
        assert_eq!(read_constant(text, "EXECUTOR_PROTOCOL_VERSION"), Some(89));
        let constant = &MONOTONIC_CONSTANTS[0];
        let moved = rewrite_constant(text, &[text], constant, &[], 90).expect("rewritten");
        assert_eq!(
            moved,
            "/// docs\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 90;\n"
        );
    }

    #[test]
    fn only_the_heading_the_branch_added_moves_with_the_constant() {
        let fork = "pub const EXECUTOR_PROTOCOL_VERSION: u32 = 87;\n";
        let base = "/// Version 88 reports the allocator split.\n\
                    pub const EXECUTOR_PROTOCOL_VERSION: u32 = 88;\n";
        let ours = "/// Version 88 reports the allocator split.\n\
                    /// Version 88 adds the thing my branch added.\n\
                    pub const EXECUTOR_PROTOCOL_VERSION: u32 = 88;\n";
        let moved = rewrite_constant(ours, &[fork, base], &MONOTONIC_CONSTANTS[0], &[88], 89)
            .expect("rewritten");
        assert_eq!(
            moved,
            "/// Version 88 reports the allocator split.\n\
             /// Version 89 adds the thing my branch added.\n\
             pub const EXECUTOR_PROTOCOL_VERSION: u32 = 89;\n"
        );
    }

    #[test]
    fn a_heading_is_a_top_level_doc_line_that_begins_with_its_version() {
        assert_eq!(
            heading_digits("/// Version 135 adds", "Version ").map(|(_, v)| v),
            Some(135)
        );
        assert_eq!(
            heading_digits("///Version 7", "Version ").map(|(_, v)| v),
            Some(7)
        );
        // The ledger's record of meaningless numbers is not itself a version.
        assert_eq!(
            heading_digits("/// Versions 101–111 denote", "Version "),
            None
        );
        assert_eq!(
            heading_digits("/// see Version 135 for why", "Version "),
            None
        );
        assert_eq!(
            heading_digits("    /// Version 135 of a field", "Version "),
            None
        );
        assert_eq!(heading_digits("// Version 135", "Version "), None);
        assert_eq!(heading_digits("/// Version adds", "Version "), None);
    }

    #[test]
    fn a_heading_version_is_read_whole_so_a_prefix_never_matches_a_longer_number() {
        let text = "/// Version 13 adds y.\n/// Version 135 adds x.\n";
        assert_eq!(
            heading_versions(text, "Version "),
            BTreeSet::from([13, 135])
        );
        let (digits, version) = heading_digits("/// Version 135 adds", "Version ").unwrap();
        assert_eq!((&"/// Version 135 adds"[digits], version), ("135", 135));
    }

    #[test]
    fn authored_versions_ignore_every_heading_the_fork_already_carries() {
        let fork = "/// Version 116 is main's.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 116;\n";
        let tip = "/// Version 116 is main's, reworded.\n/// Version 117 adds x.\n\
                   pub const EXECUTOR_PROTOCOL_VERSION: u32 = 117;\n";
        assert_eq!(authored_versions(tip, fork, fork, "Version "), vec![117]);
        assert!(authored_versions(fork, fork, fork, "Version ").is_empty());
    }

    const MAINS_140: &str = "/// Version 140 is main's.\n";
    const MAINS_141: &str = "/// Version 141 adds main's change.\n";

    fn protocol_file(headings: &[&str], version: u64) -> String {
        format!(
            "{}pub const EXECUTOR_PROTOCOL_VERSION: u32 = {version};\n",
            headings.concat()
        )
    }

    /// Specimens 4906, 4925, 4931, 4907, 4926 and 4982. A branch that changes no
    /// protocol took main's new paragraph in a hand merge, a union rebase or a
    /// resolved conflict, while the fork it is measured from predates that
    /// paragraph. The paragraph is main's, byte for byte, so the branch owes no
    /// version and stays on main's number.
    #[test]
    fn a_paragraph_absorbed_verbatim_from_the_base_is_not_authored() {
        let fork = protocol_file(&[MAINS_140], 140);
        let base = protocol_file(&[MAINS_141, MAINS_140], 141);
        let tip = protocol_file(
            &[
                MAINS_141,
                MAINS_140,
                "/// This branch owes no bump, argued here.\n",
            ],
            141,
        );
        let authored = authored_versions(&tip, &fork, &base, "Version ");
        assert!(authored.is_empty(), "{authored:?}");
        let resolved = resolve_by_evidence(&authored, 141);
        assert_eq!((resolved.delta, resolved.target), (0, 141));
        let rewritten = rewrite_constant(
            &tip,
            &[&fork, &base],
            &MONOTONIC_CONSTANTS[0],
            &authored,
            141,
        )
        .expect("rewritten");
        assert_eq!(rewritten, tip, "nothing to move");
        assert_eq!(
            ledger_violation(&tip, &fork, &base, &MONOTONIC_CONSTANTS[0]),
            None,
            "the merge gate accepts the branch for being correct"
        );
    }

    /// Specimen 4798. A real +1 branch that also took main's paragraph for the
    /// number it had claimed: main's line is excluded, the branch's own is
    /// counted once and moved above main's.
    #[test]
    fn an_authored_paragraph_beside_an_absorbed_one_counts_once() {
        let fork = protocol_file(&[MAINS_140], 140);
        let base = protocol_file(&[MAINS_141, MAINS_140], 141);
        let tip = protocol_file(
            &[
                MAINS_141,
                MAINS_140,
                "/// Version 141 adds the branch's change.\n",
            ],
            141,
        );
        let authored = authored_versions(&tip, &fork, &base, "Version ");
        assert_eq!(authored, vec![141]);
        let resolved = resolve_by_evidence(&authored, 141);
        assert_eq!((resolved.delta, resolved.target), (1, 142));
        let moved = rewrite_constant(
            &tip,
            &[&fork, &base],
            &MONOTONIC_CONSTANTS[0],
            &authored,
            resolved.target,
        )
        .expect("rewritten");
        assert_eq!(
            moved,
            protocol_file(
                &[
                    MAINS_141,
                    MAINS_140,
                    "/// Version 142 adds the branch's change.\n"
                ],
                142
            )
        );

        // The same branch after its builder already renumbered by hand.
        let renumbered = protocol_file(
            &[
                MAINS_141,
                MAINS_140,
                "/// Version 142 adds the branch's change.\n",
            ],
            142,
        );
        let authored = authored_versions(&renumbered, &fork, &base, "Version ");
        assert_eq!(authored, vec![142]);
        assert_eq!(resolve_by_evidence(&authored, 141).target, 142);
        assert_eq!(
            ledger_violation(&renumbered, &fork, &base, &MONOTONIC_CONSTANTS[0]),
            None
        );
    }

    /// A branch's own paragraph under the number main also took is never
    /// main's line, so excluding by line rather than by number keeps it.
    #[test]
    fn a_same_numbered_paragraph_that_differs_from_the_base_is_authored() {
        let fork = protocol_file(&[MAINS_140], 140);
        let base = protocol_file(&[MAINS_141, MAINS_140], 141);
        let tip = protocol_file(&["/// Version 141 adds mine.\n", MAINS_140], 141);
        let authored = authored_versions(&tip, &fork, &base, "Version ");
        assert_eq!(authored, vec![141]);
        assert_eq!(
            (
                resolve_by_evidence(&authored, 141).delta,
                resolve_by_evidence(&authored, 141).target
            ),
            (1, 142)
        );
    }

    /// After a rebase the fork is the base, which already carries every
    /// paragraph main wrote; only the branch's own remain.
    #[test]
    fn when_the_fork_is_the_base_only_the_branchs_own_paragraphs_count() {
        let base = protocol_file(&[MAINS_141, MAINS_140], 141);
        assert!(authored_versions(&base, &base, &base, "Version ").is_empty());
        let tip = protocol_file(&["/// Version 142 adds mine.\n", MAINS_141, MAINS_140], 142);
        assert_eq!(authored_versions(&tip, &base, &base, "Version "), vec![142]);
    }

    #[test]
    fn the_replay_note_states_base_delta_and_which_paragraphs_counted() {
        let plan = ResolutionPlan {
            edits: vec![],
            movements: vec![
                Movement::Advanced {
                    identifier: "EXECUTOR_PROTOCOL_VERSION".into(),
                    from: 141,
                    to: 142,
                    base: 141,
                    delta: 1,
                    from_floor: false,
                    headings: Some(HeadingTally {
                        authored: vec![141],
                        absorbed: vec![],
                    }),
                },
                Movement::Reset {
                    identifier: "EXECUTOR_PROTOCOL_VERSION".into(),
                    from: 142,
                    to: 141,
                    absorbed: vec![141],
                },
                Movement::Advanced {
                    identifier: "SUBSTRATE_HEALTH_SCHEMA_VERSION".into(),
                    from: 8,
                    to: 10,
                    base: 9,
                    delta: 1,
                    from_floor: false,
                    headings: None,
                },
            ],
            discharged: vec![],
        };
        let note = plan.describe();
        assert!(
            note.contains(
                "- `EXECUTOR_PROTOCOL_VERSION` 141 → 142: base 141 + delta 1. Authored by this \
                 branch: 141 → 142. Absorbed from the base and not counted: none."
            ),
            "{note}"
        );
        assert!(
            note.contains(
                "- `EXECUTOR_PROTOCOL_VERSION` 142 → 141: base 141 + delta 0, put back on the \
                 base's value. Authored by this branch: none. Absorbed from the base and not \
                 counted: 141."
            ),
            "{note}"
        );
        assert!(
            note.ends_with("\n- `SUBSTRATE_HEALTH_SCHEMA_VERSION` 8 → 10: base 9 + delta 1"),
            "{note}"
        );
    }

    #[test]
    fn an_inflated_tip_with_no_heading_resolves_to_the_base_value() {
        // The issue's specimen: a branch an earlier advance pushed to 222 while
        // main sat at 134, with no paragraph for any of the numbers between.
        let fork = "/// Version 116 is main's.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 116;\n";
        let tip = "/// Version 116 is main's.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 222;\n";
        let authored = authored_versions(tip, fork, fork, "Version ");
        let resolved = resolve_by_evidence(&authored, 134);
        assert_eq!((resolved.delta, resolved.target), (0, 134));
        let reset = rewrite_constant(tip, &[fork], &MONOTONIC_CONSTANTS[0], &authored, 134)
            .expect("rewritten");
        assert!(
            reset.contains("EXECUTOR_PROTOCOL_VERSION: u32 = 134;"),
            "{reset}"
        );
    }

    #[test]
    fn a_number_only_bump_resolves_to_the_base_value() {
        let fork = "pub const EXECUTOR_PROTOCOL_VERSION: u32 = 134;\n";
        let tip = "pub const EXECUTOR_PROTOCOL_VERSION: u32 = 135;\n";
        let resolved = resolve_by_evidence(&authored_versions(tip, fork, fork, "Version "), 134);
        assert_eq!((resolved.delta, resolved.target), (0, 134));
        assert!(!resolved.from_floor && !resolved.retracted);
    }

    #[test]
    fn one_authored_heading_lands_one_above_the_base_and_is_renumbered() {
        let fork = "pub const EXECUTOR_PROTOCOL_VERSION: u32 = 116;\n";
        let tip = "/// Version 117 adds x.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 117;\n";
        let base = "/// Version 134 is main's.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 134;\n";
        let rebased = "/// Version 134 is main's.\n/// Version 117 adds x.\n\
                       pub const EXECUTOR_PROTOCOL_VERSION: u32 = 117;\n";
        let authored = authored_versions(tip, fork, base, "Version ");
        let resolved = resolve_by_evidence(&authored, 134);
        assert_eq!((resolved.delta, resolved.target), (1, 135));
        let moved = rewrite_constant(
            rebased,
            &[fork, base],
            &MONOTONIC_CONSTANTS[0],
            &authored,
            resolved.target,
        )
        .expect("rewritten");
        assert_eq!(
            moved,
            "/// Version 134 is main's.\n/// Version 135 adds x.\n\
             pub const EXECUTOR_PROTOCOL_VERSION: u32 = 135;\n"
        );
    }

    #[test]
    fn a_base_heading_with_the_number_the_branch_authored_is_not_moved() {
        // Both sides wrote a `Version 135` paragraph; the base's is history.
        let fork = "pub const EXECUTOR_PROTOCOL_VERSION: u32 = 134;\n";
        let base = "/// Version 135 adds main's change.\npub const EXECUTOR_PROTOCOL_VERSION: u32 = 135;\n";
        let rebased = "/// Version 135 adds main's change.\n/// Version 135 adds mine.\n\
                       pub const EXECUTOR_PROTOCOL_VERSION: u32 = 135;\n";
        let moved = rewrite_constant(rebased, &[fork, base], &MONOTONIC_CONSTANTS[0], &[135], 136)
            .expect("rewritten");
        assert_eq!(
            moved,
            "/// Version 135 adds main's change.\n/// Version 136 adds mine.\n\
             pub const EXECUTOR_PROTOCOL_VERSION: u32 = 136;\n"
        );
    }

    #[test]
    fn an_arithmetic_constant_has_no_ledger_to_violate() {
        let text = "pub const SUBSTRATE_HEALTH_SCHEMA_VERSION: u32 = 9;\n";
        assert_eq!(
            ledger_violation(text, "", "", &MONOTONIC_CONSTANTS[1]),
            None
        );
    }

    /// The merge gate and `scripts/check-executor-protocol.ts` state the ledger
    /// rule twice, once per language. Both read this one table.
    #[test]
    fn the_ledger_rule_agrees_with_the_typescript_check_on_every_shared_case() {
        let table: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../fixtures/executor-protocol-ledger-cases.json"
        ))
        .expect("ledger fixture parses");
        let cases = table["cases"].as_array().expect("cases array");
        assert!(!cases.is_empty());
        for case in cases {
            let text = |key: &str| case[key].as_str().expect(key).to_string();
            let violation = ledger_violation(
                &text("branch"),
                &text("fork"),
                &text("dest"),
                &MONOTONIC_CONSTANTS[0],
            );
            assert_eq!(
                violation.is_none(),
                case["ok"].as_bool().expect("ok"),
                "{}: {violation:?}",
                case["name"]
            );
        }
    }

    #[test]
    fn ledger_refusals_name_the_remedy() {
        let at = |version: u64| format!("pub const EXECUTOR_PROTOCOL_VERSION: u32 = {version};\n");
        let inflated = ledger_violation(&at(152), &at(134), &at(134), &MONOTONIC_CONSTANTS[0])
            .expect("refused");
        assert!(
            inflated.contains("put `EXECUTOR_PROTOCOL_VERSION` back to 134"),
            "{inflated}"
        );
        assert!(inflated.contains("`/// Version 135 …`"), "{inflated}");
        let stale = format!("/// Version 117 adds x.\n{}", at(117));
        let misnumbered =
            ledger_violation(&stale, &at(116), &at(134), &MONOTONIC_CONSTANTS[0]).expect("refused");
        assert!(
            misnumbered.contains("the next version is 135"),
            "{misnumbered}"
        );
    }

    #[test]
    fn a_companion_literal_moves_in_the_same_edit_set() {
        let companion = &MONOTONIC_CONSTANTS[1].companions[0];
        let fixture = "{\n  \"schemaVersion\": 8,\n  \"capturedAtUnixMs\": 42\n}\n";
        let moved = rewrite_companion(fixture, companion, 8, 9).expect("rewritten");
        assert!(moved.contains("\"schemaVersion\": 9,"), "{moved}");
    }

    #[test]
    fn a_missing_companion_literal_refuses() {
        let companion = &MONOTONIC_CONSTANTS[1].companions[0];
        let refusal = rewrite_companion("{}", companion, 8, 9).expect_err("refused");
        assert!(refusal.reason.contains("found 0"), "{refusal:?}");
    }

    #[test]
    fn every_registered_identifier_names_a_path_the_gate_recognizes() {
        for constant in MONOTONIC_CONSTANTS {
            assert!(path_is_registered(constant.file), "{}", constant.file);
            for companion in constant.companions {
                assert!(path_is_registered(companion.file), "{}", companion.file);
            }
        }
        for lineage in SEQUENTIAL_FILE_LINEAGES {
            assert!(path_is_registered(lineage.registry_file));
            assert!(path_is_registered(&format!("{}/0228_x.sql", lineage.dir)));
        }
        assert!(!path_is_registered("src/main.rs"));
    }

    #[test]
    fn registered_constants_exist_in_the_files_that_declare_them() {
        // The registry names files and identifiers by string. A rename in the
        // source would otherwise turn every resolution into a silent no-op.
        let protocol = include_str!("../../../cairn-common/src/executor_protocol.rs");
        for constant in MONOTONIC_CONSTANTS {
            assert_eq!(constant.file, MONOTONIC_CONSTANTS[0].file);
            assert!(
                read_constant(protocol, constant.constant).is_some(),
                "{} is no longer declared as `pub const {}: u32 = N;`",
                constant.constant,
                constant.constant
            );
        }
    }

    #[tokio::test]
    async fn obligation_floor_is_durable_and_discharge_is_attributed() {
        let db = crate::storage::migrated_test_db("contended-obligation-test.db").await;
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'ws', 'Project', 'P', '/repo', 1, 1);",
        )
        .await
        .unwrap();
        let store = std::path::Path::new("/store");

        upsert_obligation(
            &db,
            "p",
            store,
            "agent/test",
            "executor_protocol_version",
            2,
            87,
            89,
            89,
            None,
            true,
        )
        .await
        .unwrap();
        // A later computation with less evidence cannot lower the recovery floor.
        upsert_obligation(
            &db,
            "p",
            store,
            "agent/test",
            "executor_protocol_version",
            1,
            89,
            89,
            89,
            Some("increment was absorbed"),
            true,
        )
        .await
        .unwrap();
        let loaded = load_obligations(&db, "p", store, "agent/test")
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].delta, 2);
        assert_eq!(loaded[0].basis_fork_value, 89);

        assert!(discharge_obligation(
            &db,
            "p",
            store,
            "agent/test",
            "executor_protocol_version",
            DischargeAttribution {
                actor_kind: "agent",
                actor_identity: Some("cairn://p/cairn/1/1/builder"),
                surface: "agent-resource-write",
            },
        )
        .await
        .unwrap());
        assert!(load_obligations(&db, "p", store, "agent/test")
            .await
            .unwrap()
            .is_empty());
        let attribution = db
            .query_opt(
                "SELECT discharged_actor_kind, discharged_actor_identity, discharged_surface
                 FROM contended_identifier_obligations WHERE project_id = 'p'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?, row.text(2)?)),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            attribution,
            (
                "agent".to_string(),
                Some("cairn://p/cairn/1/1/builder".to_string()),
                "agent-resource-write".to_string()
            )
        );
    }

    #[tokio::test]
    async fn a_heading_evidenced_obligation_records_the_latest_count_not_a_floor() {
        let db = crate::storage::migrated_test_db("contended-evidence-test.db").await;
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'ws', 'Project', 'P', '/repo', 1, 1);",
        )
        .await
        .unwrap();
        let store = std::path::Path::new("/store");
        // A poisoned arithmetic delta from before heading evidence, then the
        // first count that reads the branch's paragraphs.
        for delta in [18, 1] {
            upsert_obligation(
                &db,
                "p",
                store,
                "agent/test",
                "executor_protocol_version",
                delta,
                134,
                135,
                134,
                None,
                MONOTONIC_CONSTANTS[0].evidence.keeps_floor(),
            )
            .await
            .unwrap();
        }
        let loaded = load_obligations(&db, "p", store, "agent/test")
            .await
            .unwrap();
        assert_eq!(loaded[0].delta, 1);
    }

    #[tokio::test]
    async fn external_merge_retains_an_orphaned_obligation() {
        let db = crate::storage::migrated_test_db("contended-orphan-test.db").await;
        db.execute_script(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('ws', 'Workspace', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p', 'ws', 'Project', 'P', '/repo', 1, 1);",
        )
        .await
        .unwrap();
        let store = std::path::Path::new("/store");
        upsert_obligation(
            &db,
            "p",
            store,
            "agent/test",
            "executor_protocol_version",
            1,
            87,
            88,
            88,
            None,
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            orphan_obligations(&db, "p", "agent/test", "merged outside the local gate")
                .await
                .unwrap(),
            1
        );
        let loaded = load_obligations(&db, "p", store, "agent/test")
            .await
            .unwrap();
        assert_eq!(loaded[0].status, "orphaned");
        assert_eq!(
            loaded[0].last_diagnostic.as_deref(),
            Some("merged outside the local gate")
        );
    }
}
