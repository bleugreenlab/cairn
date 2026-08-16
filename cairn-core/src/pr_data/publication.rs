//! What a PR artifact is allowed to claim, and how each claim is established.
//!
//! Every state in this module is the result of a probe against something
//! outside Cairn's own database: origin's refs, GitHub's pull-request list, the
//! branch store's bookmark. The `merge_requests` row records what Cairn
//! *intended* to publish; only these probes establish what the world actually
//! holds.
//!
//! Rendering the row's intention as verified fact is how an artifact came to
//! describe a pull request that had never been opened — "OPEN, MERGEABLE,
//! +2,126,840 −30" over a branch that never reached origin — and how a pull
//! request four commits behind its branch came to read as mergeable. Both
//! numbers were locally computed against a ref nobody else could see.
//!
//! The vocabulary here is deliberately the vocabulary of the work rather than of
//! the machinery: an artifact says "the branch has not reached GitHub yet", never
//! "the bookmark failed to export".

use crate::models::PrCache;
use crate::services::GitClient;
use std::path::Path;

/// A pull request found on GitHub by its head branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredPr {
    pub(crate) number: i32,
    pub(crate) url: String,
    /// GitHub's own state string, upper-cased (`OPEN` | `CLOSED` | `MERGED`).
    pub(crate) state: String,
}

/// The verified publication state of a change whose row carries no pull-request
/// number.
///
/// Note that "could not tell" ([`Publication::Unknown`]) is a distinct state
/// from every "not published" state. A probe that failed establishes nothing,
/// and collapsing it into an answer — in either direction — is the same error as
/// trusting the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Publication {
    /// The project has no GitHub remote at all, so the branch itself is the
    /// change under review and there is no pull request to disagree with.
    LocalOnly,
    /// The branch has not reached the remote.
    BranchAbsent,
    /// The branch is on the remote, but no pull request references it.
    NoPullRequest,
    /// A pull request exists for this branch and can be bound to the row.
    Bound(DiscoveredPr),
    /// The probe itself could not run or could not reach the remote.
    Unknown { reason: String },
}

/// Origin's tip for `branch`, or `None` when origin has no such branch.
///
/// The single canonical answer to "is this branch on the remote, and at what
/// commit": existence is derived from it rather than asked separately, so the
/// two questions cannot drift apart.
pub(crate) fn origin_branch_tip(
    git: &dyn GitClient,
    repo: &Path,
    branch: &str,
) -> Result<Option<String>, String> {
    let output = git.run(
        repo,
        vec![
            "ls-remote".to_string(),
            "--heads".to_string(),
            "origin".to_string(),
            format!("refs/heads/{branch}"),
        ],
    )?;
    if !output.success {
        // `ls-remote` without `--exit-code` reports absence as success with empty
        // output, so a failure here is a real failure (no remote configured, no
        // network, no credentials) rather than "the branch is not there".
        return Err(format!("git ls-remote failed: {}", output.stderr.trim()));
    }
    Ok(output
        .stdout
        .lines()
        .find_map(|line| line.split_whitespace().next())
        .map(str::to_string))
}

/// Whether origin carries `branch`. Thin derivation of [`origin_branch_tip`].
pub(crate) fn live_origin_branch_exists(
    git: &dyn GitClient,
    repo: &Path,
    branch: &str,
) -> Result<bool, String> {
    origin_branch_tip(git, repo, branch).map(|tip| tip.is_some())
}

/// Ask GitHub whether a pull request exists for this head branch.
///
/// The capability that turns a stranded artifact back into a usable one: a row
/// whose pull-request number was never recorded — because the open failed, or
/// because a person opened the PR outside Cairn — can be re-bound to the real
/// pull request from the branch alone.
pub(crate) fn discover_pull_request_for_head(
    repo_path: &Path,
    branch: &str,
) -> Result<Option<DiscoveredPr>, String> {
    let output = crate::env::gh()
        .args(gh_pr_list_args(branch))
        .current_dir(repo_path)
        .output()
        .map_err(|error| format!("failed to run gh pr list: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "gh pr list --head {branch} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    select_discovered_pr(&String::from_utf8_lossy(&output.stdout))
}

/// The query that finds a pull request from its head branch alone.
///
/// `--state all` on purpose: a merged or closed pull request is still the right
/// answer for "what happened to this change", and binding it is what lets the
/// issue's resolution follow reality instead of a stranded row.
fn gh_pr_list_args(branch: &str) -> [&str; 10] {
    [
        "pr",
        "list",
        "--head",
        branch,
        "--state",
        "all",
        "--json",
        "number,url,state",
        "--limit",
        "10",
    ]
}

/// Pick the pull request to bind from a `gh pr list --json number,url,state`
/// payload.
///
/// An open PR wins over a merged one and a merged one over a closed one: a
/// branch can accumulate several pull requests over its life, and the one the
/// artifact should describe is the live one.
pub(crate) fn select_discovered_pr(stdout: &str) -> Result<Option<DiscoveredPr>, String> {
    let stdout = stdout.trim();
    if stdout.is_empty() {
        return Ok(None);
    }
    let entries: Vec<serde_json::Value> = serde_json::from_str(stdout)
        .map_err(|error| format!("could not parse the pull-request list: {error}"))?;
    let mut discovered: Vec<DiscoveredPr> = entries
        .iter()
        .filter_map(|entry| {
            let number = i32::try_from(entry.get("number")?.as_i64()?).ok()?;
            // A zero or negative number is not a pull request identity; refusing
            // it here is what keeps the `#0` phantom from being re-bound.
            if number <= 0 {
                return None;
            }
            Some(DiscoveredPr {
                number,
                url: entry.get("url")?.as_str()?.to_string(),
                state: entry
                    .get("state")
                    .and_then(|state| state.as_str())
                    .unwrap_or("OPEN")
                    .to_uppercase(),
            })
        })
        .collect();
    discovered.sort_by_key(|pr| match pr.state.as_str() {
        "OPEN" => 0,
        "MERGED" => 1,
        _ => 2,
    });
    Ok(discovered.into_iter().next())
}

/// What one probe of the world established about an unbound change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnboundProbe {
    pub(crate) publication: Publication,
    /// The commit origin holds for the branch, when origin has it. Carried
    /// alongside the state because "the branch is published" and "the published
    /// branch is the one measured here" are different questions, and a diffstat
    /// is honest only when both answer yes.
    pub(crate) origin_tip: Option<String>,
}

/// Establish what the world holds for a change whose row carries no
/// pull-request number.
///
/// `has_remote` is the creation-time fact recorded on the row (`is_local`
/// inverted), not an inference from the missing number: a remote PR also lacks
/// its number during the window between opening it and GitHub answering.
pub(crate) fn probe_unbound_publication(
    git: &dyn GitClient,
    repo_path: &Path,
    source_branch: &str,
    has_remote: bool,
) -> UnboundProbe {
    probe_unbound_publication_with(
        git,
        repo_path,
        source_branch,
        has_remote,
        discover_pull_request_for_head,
    )
}

/// [`probe_unbound_publication`] with the GitHub lookup supplied, so the state
/// machine can be exercised without a `gh` subprocess.
fn probe_unbound_publication_with(
    git: &dyn GitClient,
    repo_path: &Path,
    source_branch: &str,
    has_remote: bool,
    discover: impl Fn(&Path, &str) -> Result<Option<DiscoveredPr>, String>,
) -> UnboundProbe {
    if !has_remote {
        return UnboundProbe {
            publication: Publication::LocalOnly,
            origin_tip: None,
        };
    }
    match origin_branch_tip(git, repo_path, source_branch) {
        Err(reason) => UnboundProbe {
            publication: Publication::Unknown { reason },
            origin_tip: None,
        },
        Ok(None) => UnboundProbe {
            publication: Publication::BranchAbsent,
            origin_tip: None,
        },
        Ok(Some(tip)) => {
            let publication = match discover(repo_path, source_branch) {
                Ok(Some(pr)) => Publication::Bound(pr),
                Ok(None) => Publication::NoPullRequest,
                Err(reason) => Publication::Unknown { reason },
            };
            UnboundProbe {
                publication,
                origin_tip: Some(tip),
            }
        }
    }
}

/// The sentence an agent or a person reads for a change that is not a live pull
/// request.
pub(crate) fn publication_summary(publication: &Publication, source_branch: &str) -> String {
    match publication {
        Publication::LocalOnly => format!(
            "This project has no GitHub remote, so `{source_branch}` is reviewed as a local branch rather than as a pull request."
        ),
        Publication::BranchAbsent => format!(
            "Not published: the branch `{source_branch}` has not reached GitHub yet, so there is no pull request to review."
        ),
        Publication::NoPullRequest => format!(
            "Not published: `{source_branch}` is on GitHub, but no pull request has been opened for it."
        ),
        Publication::Unknown { reason } => format!(
            "Publication unconfirmed: Cairn could not check whether `{source_branch}` has reached GitHub ({reason})."
        ),
        Publication::Bound(pr) => format!("Pull request #{} — {}", pr.number, pr.url),
    }
}

/// Why an unpublished change shows no mergeability verdict and no change counts.
///
/// Both would be computed against a version of the change that nobody else can
/// see, which is exactly how conflict wreckage in a local checkout once rendered
/// as a pull request's diffstat.
pub(crate) const UNVERIFIED_VERDICT_NOTE: &str =
    "No mergeability verdict and no change counts are shown: both would describe a version of this change that only this machine can see.";

/// One line describing what a refresh actually found, for the reply an agent or
/// a person reads after asking for one.
///
/// Both PR-action dispatch surfaces produce this sentence, so it lives once: a
/// second copy is a second place for an unbound change to be described with a
/// pull-request number it does not have.
pub fn refreshed_summary(cache: &PrCache) -> String {
    let subject = match cache.pr_number {
        Some(number) => format!("PR #{number}"),
        None => "this change, which has no pull request,".to_string(),
    };
    let changes = match (cache.additions, cache.deletions) {
        (Some(additions), Some(deletions)) => format!("+{additions} -{deletions}"),
        _ => "change counts unknown".to_string(),
    };
    format!("{subject} (state {}, {changes})", cache.state)
}

/// The pull request's head against the branch as the store now holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeadDivergence {
    /// The commit the pull request describes.
    pub(crate) pr_head: String,
    /// The commit the branch actually holds.
    pub(crate) branch_head: String,
    /// Whether the pull request's head is an ancestor of the branch head — the
    /// ordinary "the push has not caught up" case, as opposed to two histories
    /// that have genuinely parted.
    pub(crate) pr_is_behind: bool,
}

impl HeadDivergence {
    /// The warning line for a pull request whose head is not the branch head.
    ///
    /// Everything GitHub reports for such a PR — mergeability, checks, the
    /// diffstat — describes the head it holds, not the change under review.
    pub(crate) fn note(&self, source_branch: &str) -> String {
        let held = if self.pr_is_behind {
            "an older version of"
        } else {
            "a different version of"
        };
        format!(
            "⚠️ This pull request shows {held} `{source_branch}`: it holds {pr_head}, while the branch now holds {branch_head}. Every signal below — mergeability, checks, change counts — describes the version the pull request holds, not the current one. Nothing here has validated the current version.",
            pr_head = short_commit(&self.pr_head),
            branch_head = short_commit(&self.branch_head),
        )
    }
}

fn short_commit(commit: &str) -> &str {
    let end = commit
        .char_indices()
        .nth(12)
        .map_or(commit.len(), |(i, _)| i);
    &commit[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::MockGitClient;
    use crate::services::GitOutput;

    fn git_output(success: bool, stdout: &str, stderr: &str) -> GitOutput {
        GitOutput {
            success,
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
        }
    }

    #[test]
    fn origin_branch_tip_reads_the_sha_when_the_branch_is_there() {
        let mut git = MockGitClient::new();
        git.expect_run().returning(|_, _| {
            Ok(git_output(
                true,
                "deadbeefcafe1234\trefs/heads/agent/proj-1-builder\n",
                "",
            ))
        });
        let tip = origin_branch_tip(&git, Path::new("/repo"), "agent/proj-1-builder").unwrap();
        assert_eq!(tip.as_deref(), Some("deadbeefcafe1234"));
    }

    /// Absence is empty output with a zero exit, and must not be mistaken for a
    /// failed probe: this is the branch-never-reached-origin case, and it needs
    /// to reach the artifact as a fact rather than as an error.
    #[test]
    fn origin_branch_tip_reports_absence_as_none() {
        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|_, _| Ok(git_output(true, "", "")));
        let tip = origin_branch_tip(&git, Path::new("/repo"), "agent/proj-1-builder").unwrap();
        assert_eq!(tip, None);
        assert!(
            !live_origin_branch_exists(&git, Path::new("/repo"), "agent/proj-1-builder").unwrap()
        );
    }

    /// A probe that could not run is an error, never a "no". Answering "the
    /// branch is not published" from an unreachable remote is a claim the probe
    /// did not establish.
    #[test]
    fn origin_branch_tip_surfaces_probe_failure() {
        let mut git = MockGitClient::new();
        git.expect_run().returning(|_, _| {
            Ok(git_output(
                false,
                "",
                "fatal: 'origin' does not appear to be a git repository",
            ))
        });
        let error = origin_branch_tip(&git, Path::new("/repo"), "feature").unwrap_err();
        assert!(
            error.contains("does not appear to be a git repository"),
            "{error}"
        );
    }

    #[test]
    fn probe_reports_local_only_without_a_remote() {
        let mut git = MockGitClient::new();
        git.expect_run().never();
        assert_eq!(
            probe_unbound_publication(&git, Path::new("/repo"), "feature", false).publication,
            Publication::LocalOnly
        );
    }

    #[test]
    fn probe_reports_branch_absent_when_origin_lacks_the_branch() {
        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|_, _| Ok(git_output(true, "", "")));
        let probe = probe_unbound_publication(&git, Path::new("/repo"), "feature", true);
        assert_eq!(probe.publication, Publication::BranchAbsent);
        assert_eq!(probe.origin_tip, None);
    }

    #[test]
    fn probe_reports_unknown_when_the_remote_cannot_be_reached() {
        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|_, _| Ok(git_output(false, "", "could not read Username")));
        assert!(matches!(
            probe_unbound_publication(&git, Path::new("/repo"), "feature", true).publication,
            Publication::Unknown { .. }
        ));
    }

    /// The branch is on the remote and GitHub has no pull request for it. This
    /// is the "pushed but never opened" state, and it must be its own answer:
    /// the change is public, but there is nothing to review.
    #[test]
    fn probe_reports_no_pull_request_for_a_published_branch_without_one() {
        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|_, _| Ok(git_output(true, "abc123\trefs/heads/feature\n", "")));
        let probe =
            probe_unbound_publication_with(&git, Path::new("/repo"), "feature", true, |_, _| {
                Ok(None)
            });
        assert_eq!(probe.publication, Publication::NoPullRequest);
        assert_eq!(probe.origin_tip.as_deref(), Some("abc123"));
    }

    /// The repair: a pull request that exists for this head branch is found and
    /// offered for binding, even though the row never recorded it.
    #[test]
    fn probe_binds_a_pull_request_found_by_head_branch() {
        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|_, _| Ok(git_output(true, "abc123\trefs/heads/feature\n", "")));
        let probe = probe_unbound_publication_with(
            &git,
            Path::new("/repo"),
            "feature",
            true,
            |_, branch| {
                assert_eq!(branch, "feature");
                Ok(Some(DiscoveredPr {
                    number: 2797,
                    url: "https://github.com/o/r/pull/2797".to_string(),
                    state: "OPEN".to_string(),
                }))
            },
        );
        let Publication::Bound(found) = probe.publication else {
            panic!("a discoverable pull request must be offered for binding");
        };
        assert_eq!(found.number, 2797);
    }

    /// A branch that never reached the remote is never asked about on GitHub:
    /// there is nothing a pull-request lookup could bind to.
    #[test]
    fn probe_does_not_ask_github_about_an_unpublished_branch() {
        let mut git = MockGitClient::new();
        git.expect_run()
            .returning(|_, _| Ok(git_output(true, "", "")));
        let probe =
            probe_unbound_publication_with(&git, Path::new("/repo"), "feature", true, |_, _| {
                panic!("GitHub must not be queried for a branch that is not there")
            });
        assert_eq!(probe.publication, Publication::BranchAbsent);
    }

    #[test]
    fn head_branch_query_asks_for_every_state() {
        assert_eq!(
            gh_pr_list_args("agent/proj-1-builder"),
            [
                "pr",
                "list",
                "--head",
                "agent/proj-1-builder",
                "--state",
                "all",
                "--json",
                "number,url,state",
                "--limit",
                "10"
            ]
        );
    }

    #[test]
    fn refreshed_summary_never_invents_a_number() {
        let unbound = PrCache {
            id: "mr".to_string(),
            job_id: None,
            pr_number: None,
            pr_url: String::new(),
            title: None,
            body: None,
            state: crate::models::PrState::Unpublished,
            is_draft: false,
            review_decision: None,
            mergeable: crate::models::MergeableState::Unknown,
            additions: None,
            deletions: None,
            checks_status: None,
            checks: Vec::new(),
            fetched_at: 0,
            updated_at: 0,
            is_local: false,
            source_branch: None,
            target_branch: None,
        };
        let summary = refreshed_summary(&unbound);
        assert!(!summary.contains('#'), "no pull-request number: {summary}");
        assert!(summary.contains("UNPUBLISHED"), "{summary}");
        assert!(summary.contains("change counts unknown"), "{summary}");

        let bound = PrCache {
            pr_number: Some(2797),
            state: crate::models::PrState::Open,
            additions: Some(12),
            deletions: Some(3),
            ..unbound
        };
        assert!(refreshed_summary(&bound).contains("PR #2797"));
    }

    #[test]
    fn discovery_prefers_the_open_pull_request() {
        let payload = r#"[
            {"number": 2765, "url": "https://github.com/o/r/pull/2765", "state": "CLOSED"},
            {"number": 2797, "url": "https://github.com/o/r/pull/2797", "state": "OPEN"},
            {"number": 2700, "url": "https://github.com/o/r/pull/2700", "state": "MERGED"}
        ]"#;
        let found = select_discovered_pr(payload).unwrap().unwrap();
        assert_eq!(found.number, 2797);
        assert_eq!(found.state, "OPEN");
    }

    #[test]
    fn discovery_falls_back_to_a_merged_pull_request() {
        let payload = r#"[
            {"number": 10, "url": "https://github.com/o/r/pull/10", "state": "CLOSED"},
            {"number": 11, "url": "https://github.com/o/r/pull/11", "state": "MERGED"}
        ]"#;
        assert_eq!(select_discovered_pr(payload).unwrap().unwrap().number, 11);
    }

    #[test]
    fn discovery_finds_nothing_in_an_empty_list() {
        assert_eq!(select_discovered_pr("[]").unwrap(), None);
        assert_eq!(select_discovered_pr("").unwrap(), None);
    }

    /// The repair must never re-create the phantom it exists to clear: a `#0`
    /// entry is not a pull request identity and is refused as a binding target.
    #[test]
    fn discovery_refuses_a_zero_pull_request_number() {
        let payload = r#"[{"number": 0, "url": "https://github.com/o/r/pull/0", "state": "OPEN"}]"#;
        assert_eq!(select_discovered_pr(payload).unwrap(), None);
    }

    #[test]
    fn summaries_speak_about_the_work_not_the_machinery() {
        let absent = publication_summary(&Publication::BranchAbsent, "agent/proj-1-builder");
        assert!(absent.contains("has not reached GitHub yet"), "{absent}");
        let no_pr = publication_summary(&Publication::NoPullRequest, "agent/proj-1-builder");
        assert!(no_pr.contains("no pull request has been opened"), "{no_pr}");
        for text in [&absent, &no_pr] {
            for jargon in ["bookmark", "export", "coordinate", "revset", "jj "] {
                assert!(
                    !text.to_lowercase().contains(jargon),
                    "substrate vocabulary `{jargon}` leaked into agent-facing text: {text}"
                );
            }
        }
    }

    #[test]
    fn divergence_note_names_both_commits_and_the_direction() {
        let behind = HeadDivergence {
            pr_head: "aaaaaaaaaaaaaaaaaaaa".to_string(),
            branch_head: "bbbbbbbbbbbbbbbbbbbb".to_string(),
            pr_is_behind: true,
        };
        let note = behind.note("agent/proj-1-builder");
        assert!(note.contains("older version"), "{note}");
        assert!(note.contains("aaaaaaaaaaaa"), "{note}");
        assert!(note.contains("bbbbbbbbbbbb"), "{note}");

        let parted = HeadDivergence {
            pr_is_behind: false,
            ..behind
        };
        assert!(parted
            .note("agent/proj-1-builder")
            .contains("different version"));
    }
}
