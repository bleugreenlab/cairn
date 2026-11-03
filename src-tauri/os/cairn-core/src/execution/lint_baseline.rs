//! The base finding set a branch's `lint` lane subtracts (cairn/5851).
//!
//! A whole-tree lint is the right verdict for main and the wrong one for a
//! branch: while main carries an error, every branch cut from it is red before
//! its builder writes a line, so an error the branch adds never changes the
//! glyph. So on a branch the lane's script (`scripts/check-lint.ts`) judges
//! only what the branch adds over its merge base, and this module hands it the
//! merge base's findings.
//!
//! They come from the store, not from a second lint in the cell. Every eslint
//! result records its tree's whole finding set (`lintFindings.tree` in
//! `target_results_json`), so the main cadence's lint row over a main tip, or
//! any other lane's row over a tree with the same lint inputs, is that tree's
//! baseline. The base is keyed by its lint input key under the base's OWN
//! contract, exactly as the base's row was recorded, so a docs-only merge that
//! the cadence never re-ran still finds the row its inputs share, and a branch
//! that edits the lint configuration still subtracts what the base recorded.
//!
//! The transport is a file, `CAIRN_CHECK_LINT_BASELINE`, written from the
//! `*_CONTENT` variable like the other check transports. It is shipped only
//! when the base is not the head: a run of main against main (the cadence, a
//! `--branch main` baseline) carries none and gets the whole-tree verdict. A
//! base whose finding set is not recorded still ships, naming what is missing,
//! so the script reports no verdict instead of the whole tree's red.

use std::path::Path;
use std::sync::Arc;

use crate::execution::checks::{check_input_hash, checks_contract_at_commit};
use crate::execution::inputs::{ResolvedInputs, TreeBlobs, TreeSnapshot};
use crate::jj::{logical_tree_hash, tree_entries, JjEnv};
use crate::storage::LocalDb;

/// The transport variable the runner sets; the executor writes it to the file
/// named by [`LINT_BASELINE_ENV`].
pub(crate) const LINT_BASELINE_CONTENT_ENV: &str = "CAIRN_CHECK_LINT_BASELINE_CONTENT";
/// Mirrors `LINT_BASELINE_ENV` in scripts/lib/eslint-lint-result.ts.
pub(crate) const LINT_BASELINE_ENV: &str = "CAIRN_CHECK_LINT_BASELINE";

/// The checks whose branch verdict is the findings they add over their base.
const BASE_SUBTRACTING_CHECKS: &[&str] = &["lint"];

/// Whether `check_name`'s lane is judged against its base's recorded findings.
pub(crate) fn subtracts_base_findings(check_name: &str) -> bool {
    BASE_SUBTRACTING_CHECKS.contains(&check_name)
}

/// The JSON the script reads (`LintBaselineInput` in eslint-lint-result.ts).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LintBaselineTransport {
    pub(crate) base: String,
    pub(crate) commit: String,
    pub(crate) tree: String,
    pub(crate) input_hash: String,
    pub(crate) fingerprints: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) missing: Option<String>,
}

/// Where the base lives and how a reader names it.
pub(crate) struct LintBaselineSource<'a> {
    pub(crate) jj: &'a JjEnv,
    /// The object database the base's contract is read from.
    pub(crate) object_repository: &'a Path,
    /// The logical repository its tree is read from.
    pub(crate) repository: &'a Path,
    pub(crate) base: &'a str,
    pub(crate) head: &'a str,
    /// The integration branch, when the caller knows it; otherwise the base is
    /// named by its short commit.
    pub(crate) base_label: Option<&'a str>,
}

/// The `(variable, content)` pair to add to a check batch's transport, or
/// `None` when no planned check subtracts its base or the base is the head.
///
/// Synchronous: it reads the object database, the tree, and the store, so
/// async callers run it on the blocking pool.
pub(crate) fn lint_baseline_env<'n>(
    source: &LintBaselineSource<'_>,
    db: Arc<LocalDb>,
    project_id: &str,
    planned: impl IntoIterator<Item = &'n str>,
) -> Option<(String, String)> {
    let check_name = planned
        .into_iter()
        .find(|name| subtracts_base_findings(name))?;
    if source.base == source.head {
        return None;
    }
    let transport = lint_baseline(source, db, project_id, check_name);
    let content = serde_json::to_string(&transport).expect("the baseline transport serializes");
    Some((LINT_BASELINE_CONTENT_ENV.to_string(), content))
}

fn short(commit: &str) -> &str {
    &commit[..commit.len().min(12)]
}

/// The base's recorded finding set for `check_name`, or the reason there is
/// none. Every failure is folded into `missing`: a baseline that cannot be read
/// is a named absence for the script to report, not a reason to fail the lane.
pub(crate) fn lint_baseline(
    source: &LintBaselineSource<'_>,
    db: Arc<LocalDb>,
    project_id: &str,
    check_name: &str,
) -> LintBaselineTransport {
    let mut transport = LintBaselineTransport {
        base: source
            .base_label
            .map(str::to_string)
            .unwrap_or_else(|| short(source.base).to_string()),
        commit: source.base.to_string(),
        tree: "unresolved".to_string(),
        input_hash: "unresolved".to_string(),
        fingerprints: None,
        missing: None,
    };
    let missing = |mut transport: LintBaselineTransport, why: String| {
        transport.missing = Some(why);
        transport
    };
    match logical_tree_hash(source.jj, source.repository, source.base) {
        Ok(tree) => transport.tree = tree,
        Err(error) => return missing(transport, format!("the base tree is unreadable: {error}")),
    }
    let Some(contract) = checks_contract_at_commit(source.object_repository, source.base) else {
        return missing(
            transport,
            "the base declares no readable checks contract".to_string(),
        );
    };
    let checks = contract.contract.checks;
    let Some(check) = checks.get(check_name) else {
        return missing(
            transport,
            format!("the base declares no `{check_name}` check"),
        );
    };
    let entries = tree_entries(source.jj, source.repository, source.base).ok();
    let blobs = TreeBlobs {
        jj: source.jj,
        repository: source.repository,
    };
    let snapshot = TreeSnapshot::new(entries.as_deref(), &blobs);
    let inputs = ResolvedInputs::resolve(&checks, &contract.contract.extra_inputs, &snapshot);
    // The base's row was recorded by whichever cadence ran it, against a change
    // this lookup does not know. A base-subtracting check is not base-relative
    // (its base enters through this transport, not its key), so its key never
    // reads the change and the lookup can name it.
    transport.input_hash = check_input_hash(
        check,
        inputs.for_check(check_name),
        entries.as_deref(),
        &transport.tree,
        crate::execution::checks::CheckChange::unknown(check_name),
    );
    match crate::execution::cache::latest_lint_tree_findings(
        db,
        project_id,
        check_name,
        &transport.input_hash,
    ) {
        Ok(Some(fingerprints)) => {
            transport.fingerprints = Some(fingerprints);
            transport
        }
        Ok(None) => missing(
            transport,
            "no recorded lint result carries a finding set for its inputs".to_string(),
        ),
        Err(error) => missing(transport, format!("the store could not be read: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_lint_lane_subtracts_its_base() {
        assert!(subtracts_base_findings("lint"));
        assert!(!subtracts_base_findings("rust-lint"));
        assert!(!subtracts_base_findings("typecheck"));
    }

    #[test]
    fn the_transport_is_the_shape_the_script_reads() {
        let transport = LintBaselineTransport {
            base: "main".to_string(),
            commit: "c".to_string(),
            tree: "t".to_string(),
            input_hash: "i".to_string(),
            fingerprints: None,
            missing: Some("why".to_string()),
        };
        assert_eq!(
            serde_json::to_value(&transport).unwrap(),
            serde_json::json!({
                "base": "main", "commit": "c", "tree": "t", "inputHash": "i",
                "fingerprints": null, "missing": "why"
            })
        );
    }
}
