//! What the running build would make of the `.cairn/config.yaml` a landing
//! puts on the default branch.
//!
//! The app reads the default branch's copy of that file from the project
//! checkout, and the app can be older than the file: a merge can land a word
//! the build was compiled without. A check that uses such a word is set aside
//! as unsupported, so the cost is bounded, but until the app is rebuilt that
//! check silently does not run. A file the build cannot load at all costs much
//! more: every execution in the project, or every check and the Settings page.
//!
//! So a default-branch landing that changes the file is judged by the build
//! that will read it, before anything moves. A file that would stop loading is
//! refused. A file that loads but sets checks aside lands, and the reply names
//! each one and says the app needs a rebuild: refusing it would forbid landing
//! a new cadence together with the code that reads it, which is the ordinary
//! way a cadence is added. The judgement itself is cairn-config's
//! [`judge_landing_config`]; this module reads the three sides and words it.

use std::path::{Path, PathBuf};

use crate::config::project_settings::{
    judge_landing_config, LandingConfigJudgement, UnreadableConfigCost, UnreadableProjectConfig,
    UnsupportedCheck,
};
use crate::orchestrator::Orchestrator;

use super::context::MergeMrContext;

const CONFIG_PATH: &str = ".cairn/config.yaml";

/// Judge the config a landing on the default branch would put there. `Ok(None)`
/// has nothing to say; `Ok(Some(notice))` lands, and the notice is for whoever
/// merged; `Err` is the refusal, worded for the caller.
///
/// Advisory on its own failures, like the parse gate: a side that cannot be
/// read decides nothing, and the landing proceeds as it would have.
pub(super) async fn preflight_landing_config(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
) -> Result<Option<String>, String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let repository = PathBuf::from(&merge_context.mr.repo_path);
    let store = crate::jj::project_store_dir(&orch.config_dir, &repository);
    let (source, target) = (
        merge_context.source_branch.clone(),
        merge_context.target_branch.clone(),
    );
    let judgement = {
        let (source, target) = (source.clone(), target.clone());
        tokio::task::spawn_blocking(move || {
            let sides = ConfigSides::read(&jj, &store, &repository, &target, &source)?;
            Some(judge_landing_config(
                sides.fork.as_deref(),
                sides.target.as_deref(),
                sides.head.as_deref(),
            ))
        })
        .await
        .ok()
        .flatten()
    };
    match judgement {
        None | Some(LandingConfigJudgement::Unaffected) => Ok(None),
        Some(LandingConfigJudgement::Unreadable(unreadable)) => {
            Err(unreadable_config_refusal(&source, &target, &unreadable))
        }
        Some(LandingConfigJudgement::NeedsNewerBuild(unsupported)) => {
            let notice = needs_newer_build_notice(&unsupported);
            log::warn!("merging `{source}` into `{target}`: {notice}");
            Ok(Some(notice))
        }
    }
}

/// The file's content at the branch's fork point, on the target, and at the
/// branch head; `None` where the file is absent.
struct ConfigSides {
    fork: Option<Vec<u8>>,
    target: Option<Vec<u8>>,
    head: Option<Vec<u8>>,
}

impl ConfigSides {
    fn read(
        jj: &crate::jj::JjEnv,
        store: &Path,
        repository: &Path,
        target: &str,
        source: &str,
    ) -> Option<Self> {
        let head = crate::jj::bookmark_commit(jj, store, source)?;
        let target_tip = crate::jj::bookmark_commit(jj, store, target)?;
        let fork = crate::jj::revset_commit(
            jj,
            store,
            &format!("heads(::bookmarks(exact:{target:?}) & ::bookmarks(exact:{source:?}))"),
        )?;
        // The managed store shares the project repository's objects, so its
        // commits are read there, as the landing gate reads them.
        let read = |commit: &str| {
            crate::mcp::handlers::read::file_at_commit(
                repository.to_path_buf(),
                commit.to_string(),
                CONFIG_PATH,
            )
            .map_err(|error| {
                log::warn!(
                    "config preflight: could not read {CONFIG_PATH} at {commit}, so the landing \
                     is not judged: {error}"
                );
            })
            .ok()
        };
        Some(Self {
            fork: read(&fork)?,
            target: read(&target_tip)?,
            head: read(&head)?,
        })
    }
}

pub(super) fn unreadable_config_refusal(
    source: &str,
    target: &str,
    unreadable: &UnreadableProjectConfig,
) -> String {
    let cost = match unreadable.cost {
        UnreadableConfigCost::EveryExecution => {
            "once it lands, every batch, check lane, and terminal in this project is refused \
             until the file is fixed or the app is rebuilt from a revision that reads it"
        }
        UnreadableConfigCost::ChecksAndSettings => {
            "once it lands, no check is planned for any commit carrying it, and the project's \
             terminals and Settings do not load, until the file is fixed or the app is rebuilt \
             from a revision that reads it"
        }
    };
    format!(
        "Refusing to merge `{source}` into `{target}`: the {CONFIG_PATH} it would land cannot be \
         read by the Cairn build that is running, and `{target}`'s copy can. The app reads \
         `{target}`'s copy, so {cost}. `{target}` was not advanced.\n\
         This build says: {reason}\n\
         Fix the file on `{source}` with ordinary edits, commit, then merge again. If the file is \
         right and this build is too old for it, rebuild the app from a revision that reads it \
         first.",
        reason = unreadable.reason.trim(),
    )
}

/// The sentence a landing carries when the config it lands sets checks aside.
pub(super) fn needs_newer_build_notice(
    unsupported: &std::collections::BTreeMap<String, UnsupportedCheck>,
) -> String {
    let named: Vec<String> = unsupported
        .iter()
        .map(|(name, check)| format!("`{name}` (`{}: {}`)", check.field, check.value))
        .collect();
    let checks = match named.as_slice() {
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
        [] => String::new(),
    };
    format!(
        "This config needs a newer build than the one running: {checks} will not run until the \
         app is rebuilt."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn unsupported(field: &str, value: &str) -> UnsupportedCheck {
        UnsupportedCheck {
            field: field.to_string(),
            value: value.to_string(),
            known: Vec::new(),
            notice: String::new(),
            declared: serde_yaml::Value::Null,
        }
    }

    #[test]
    fn the_notice_names_each_check_and_the_word_this_build_lacks() {
        let one = BTreeMap::from([(
            "landing-compile".to_string(),
            unsupported("when", "landing"),
        )]);
        assert_eq!(
            needs_newer_build_notice(&one),
            "This config needs a newer build than the one running: `landing-compile` \
             (`when: landing`) will not run until the app is rebuilt."
        );
        let three = BTreeMap::from([
            ("a".to_string(), unsupported("when", "landing")),
            ("b".to_string(), unsupported("budgetSeconds", "600")),
            ("c".to_string(), unsupported("tree", "slice")),
        ]);
        assert_eq!(
            needs_newer_build_notice(&three),
            "This config needs a newer build than the one running: `a` (`when: landing`), `b` \
             (`budgetSeconds: 600`) and `c` (`tree: slice`) will not run until the app is \
             rebuilt."
        );
    }

    #[test]
    fn the_refusal_says_what_landing_the_file_would_cost() {
        let refusal = unreadable_config_refusal(
            "cairn/9",
            "main",
            &UnreadableProjectConfig {
                cost: UnreadableConfigCost::EveryExecution,
                reason: "failed to parse project config at .cairn/config.yaml: bad".to_string(),
            },
        );
        assert!(
            refusal.starts_with("Refusing to merge `cairn/9` into `main`:"),
            "{refusal}"
        );
        assert!(
            refusal.contains("every batch, check lane, and terminal in this project is refused"),
            "{refusal}"
        );
        assert!(
            refusal.contains("This build says: failed to parse"),
            "{refusal}"
        );
        assert!(refusal.contains("`main` was not advanced"), "{refusal}");

        let settings = unreadable_config_refusal(
            "cairn/9",
            "main",
            &UnreadableProjectConfig {
                cost: UnreadableConfigCost::ChecksAndSettings,
                reason: "checks.frontend: missing field `command`".to_string(),
            },
        );
        assert!(settings.contains("no check is planned"), "{settings}");
    }
}
