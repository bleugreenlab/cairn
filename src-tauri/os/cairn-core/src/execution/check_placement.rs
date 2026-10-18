//! A request's placement selector for one configured check lane.
//!
//! A configured check already declares where it may run (`executor`) and whose
//! answer counts as its verdict (`verdictPlatforms`). A request — `cairn check
//! run --os linux`, an SDK call, or the pin of the batch whose shell started the
//! check — may NARROW both, and nothing else:
//!
//! - it changes where the lane is placed and which recorded platforms (or which
//!   machine) a cached verdict may be reused from;
//! - it never changes the check's identity. The cache key is computed from the
//!   configured definition, because configured trust already says a Linux
//!   answer and a macOS answer are the same verdict.
//!
//! A request that contradicts the configuration is refused when the caller
//! asked for it explicitly, and dropped (with a note) when it was inherited
//! from a batch pin: the batch was pinned for its own work, not to overrule a
//! suite's declaration.

use cairn_common::executor_protocol::{
    executor_names_match, CheckSelectorSource, ExecutorSelector,
};

use crate::config::project_settings::CheckCommand;

/// A validated request to narrow one check lane's placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckPlacementOverride {
    selector: ExecutorSelector,
    source: CheckSelectorSource,
}

impl CheckPlacementOverride {
    /// Accept a selector that states only a machine name or a platform.
    pub fn new(selector: ExecutorSelector, source: CheckSelectorSource) -> Result<Self, String> {
        let selector = ExecutorSelector {
            os: selector.os.map(|os| os.trim().to_ascii_lowercase()),
            ..selector
        };
        selector.validate_placement_pin()?;
        Ok(Self { selector, source })
    }

    pub fn selector(&self) -> &ExecutorSelector {
        &self.selector
    }

    pub fn source(&self) -> CheckSelectorSource {
        self.source
    }

    /// The pin as a reader writes it: `os=linux` or `name=fedora`.
    fn label(&self) -> String {
        match (&self.selector.name, &self.selector.os) {
            (Some(name), _) => format!("name={name}"),
            (None, Some(os)) => format!("os={os}"),
            (None, None) => "none".to_string(),
        }
    }
}

/// Where one check lane may run and whose answer counts, after a request's
/// override has been applied to the configured check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveCheckPlacement {
    /// The selector placement is asked with: the configured capability
    /// requirements, with the request's name or platform.
    pub selector: Option<ExecutorSelector>,
    /// The platforms whose observations may answer this request.
    pub verdict_platforms: Vec<String>,
    /// The machine a `name` request restricted the lane to. Reuse is then
    /// limited to that machine's own observations.
    pub executor_name: Option<String>,
    /// Why an inherited batch pin was not applied, for the caller to read.
    pub note: Option<String>,
}

/// Apply a request's override to a configured check.
///
/// The configured selector keeps its toolchains, sandbox and containment
/// requirements; the override supplies `name` or `os`. An `os` narrows the
/// verdict platforms to `{os}` (and must be one the check trusts); a `name`
/// leaves them as configured.
///
/// A named machine's platform is checked here too, through `machine_os`,
/// while the request's source is still known: placement would refuse an
/// incompatible machine as well, but by then an inherited pin could no longer
/// step aside for the configuration. A name `machine_os` cannot resolve is
/// left to placement, which refuses it with the fleet's own words.
pub(crate) fn resolve_check_placement(
    check_name: &str,
    check: &CheckCommand,
    request: Option<&CheckPlacementOverride>,
    machine_os: impl Fn(&str) -> Option<String>,
) -> Result<EffectiveCheckPlacement, String> {
    let configured_selector = check
        .executor
        .clone()
        .filter(|selector| !selector.is_empty());
    let configured_platforms = crate::execution::check_identity::verdict_platforms(check);
    let configured = EffectiveCheckPlacement {
        selector: configured_selector.clone(),
        verdict_platforms: configured_platforms.clone(),
        executor_name: None,
        note: None,
    };
    let Some(request) = request else {
        return Ok(configured);
    };
    match narrow(
        check_name,
        configured_selector.unwrap_or_default(),
        configured_platforms,
        request.selector(),
        &machine_os,
    ) {
        Ok(effective) => Ok(effective),
        Err(conflict) => match request.source() {
            CheckSelectorSource::Explicit => Err(conflict),
            CheckSelectorSource::Inherited => Ok(EffectiveCheckPlacement {
                note: Some(format!(
                    "batch pin {} not applied: {conflict}",
                    request.label()
                )),
                ..configured
            }),
        },
    }
}

fn narrow(
    check_name: &str,
    mut selector: ExecutorSelector,
    mut platforms: Vec<String>,
    pin: &ExecutorSelector,
    machine_os: &dyn Fn(&str) -> Option<String>,
) -> Result<EffectiveCheckPlacement, String> {
    let mut executor_name = None;
    if let Some(os) = &pin.os {
        if let Some(configured) = &selector.name {
            return Err(format!(
                "{check_name} is configured to run on executor {configured}, so a lane on os {os} cannot run it"
            ));
        }
        if let Some(configured) = selector.os.as_deref() {
            if !configured.eq_ignore_ascii_case(os) {
                return Err(format!(
                    "{check_name} is configured to run on os {configured}, so a lane on os {os} cannot run it"
                ));
            }
        }
        if !platforms.is_empty()
            && !platforms
                .iter()
                .any(|platform| platform.eq_ignore_ascii_case(os))
        {
            return Err(format!(
                "{check_name} counts its verdict only from verdictPlatforms [{}], so a lane on os {os} cannot produce it",
                platforms.join(", ")
            ));
        }
        selector.os = Some(os.clone());
        platforms = vec![os.clone()];
    }
    if let Some(name) = &pin.name {
        if let Some(configured) = &selector.name {
            if !executor_names_match(configured, name) {
                return Err(format!(
                    "{check_name} is configured to run on executor {configured}, so it cannot run on executor {name}"
                ));
            }
        }
        // Naming a machine settles its platform, and a selector never states
        // both. A configured platform therefore moves from the selector into
        // the trust set, where it still refuses a machine on another platform.
        if let Some(configured_os) = selector.os.take() {
            if platforms.is_empty() {
                platforms = vec![configured_os];
            } else {
                platforms.retain(|platform| platform.eq_ignore_ascii_case(&configured_os));
                if platforms.is_empty() {
                    return Err(format!(
                        "{check_name} is configured to run on os {configured_os}, which none of its verdictPlatforms counts"
                    ));
                }
            }
        }
        if let Some(os) = machine_os(name) {
            if !platforms.is_empty()
                && !platforms
                    .iter()
                    .any(|platform| platform.eq_ignore_ascii_case(&os))
            {
                return Err(format!(
                    "{check_name} counts its verdict only from verdictPlatforms [{}], and executor {name} runs {os}, so it cannot produce it",
                    platforms.join(", ")
                ));
            }
        }
        selector.name = Some(name.clone());
        executor_name = Some(name.clone());
    }
    Ok(EffectiveCheckPlacement {
        selector: (!selector.is_empty()).then_some(selector),
        verdict_platforms: platforms,
        executor_name,
        note: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(yaml: &str) -> CheckCommand {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn rust_tests() -> CheckCommand {
        check(
            "command: bun run test:rust\nverdictPlatforms: [linux, macos]\nexecutor:\n  requiresMacosNestedSandboxWrite: true\n",
        )
    }

    fn os(os: &str, source: CheckSelectorSource) -> CheckPlacementOverride {
        CheckPlacementOverride::new(
            ExecutorSelector {
                os: Some(os.into()),
                ..ExecutorSelector::default()
            },
            source,
        )
        .unwrap()
    }

    fn name(name: &str, source: CheckSelectorSource) -> CheckPlacementOverride {
        CheckPlacementOverride::new(
            ExecutorSelector {
                name: Some(name.into()),
                ..ExecutorSelector::default()
            },
            source,
        )
        .unwrap()
    }

    #[test]
    fn no_override_is_the_configured_placement() {
        let effective =
            resolve_check_placement("rust-tests", &rust_tests(), None, |_| None).unwrap();
        assert_eq!(effective.verdict_platforms, vec!["linux", "macos"]);
        assert!(
            effective
                .selector
                .unwrap()
                .requires_macos_nested_sandbox_write
        );
        assert_eq!(effective.executor_name, None);
        assert_eq!(effective.note, None);
    }

    #[test]
    fn an_os_override_narrows_platforms_and_keeps_configured_capabilities() {
        let configured = rust_tests();
        let effective = resolve_check_placement(
            "rust-tests",
            &configured,
            Some(&os("linux", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap();
        assert_eq!(effective.verdict_platforms, vec!["linux"]);
        let selector = effective.selector.unwrap();
        assert_eq!(selector.os.as_deref(), Some("linux"));
        assert!(selector.requires_macos_nested_sandbox_write);
        // The override narrows placement and trust; the configured definition
        // the content identity is computed from is untouched.
        assert_eq!(configured, rust_tests());
    }

    #[test]
    fn an_os_override_is_case_insensitive() {
        let request = os("Linux", CheckSelectorSource::Explicit);
        assert_eq!(request.selector().os.as_deref(), Some("linux"));
    }

    #[test]
    fn an_explicit_os_outside_the_trusted_set_is_refused_naming_both_sides() {
        let error = resolve_check_placement(
            "rust-tests",
            &rust_tests(),
            Some(&os("windows", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap_err();
        assert!(error.contains("verdictPlatforms [linux, macos]"), "{error}");
        assert!(error.contains("os windows"), "{error}");
    }

    #[test]
    fn an_inherited_conflict_is_dropped_with_a_note() {
        let effective = resolve_check_placement(
            "rust-tests",
            &rust_tests(),
            Some(&os("windows", CheckSelectorSource::Inherited)),
            |_| None,
        )
        .unwrap();
        assert_eq!(effective.verdict_platforms, vec!["linux", "macos"]);
        assert_eq!(effective.selector.unwrap().os, None);
        let note = effective.note.unwrap();
        assert!(
            note.starts_with("batch pin os=windows not applied: "),
            "{note}"
        );
        assert!(note.contains("verdictPlatforms [linux, macos]"), "{note}");
    }

    #[test]
    fn an_inherited_pin_that_agrees_is_applied() {
        let effective = resolve_check_placement(
            "rust-tests",
            &rust_tests(),
            Some(&os("linux", CheckSelectorSource::Inherited)),
            |_| None,
        )
        .unwrap();
        assert_eq!(effective.verdict_platforms, vec!["linux"]);
        assert_eq!(effective.note, None);
    }

    #[test]
    fn an_os_override_against_a_configured_os_must_agree() {
        let pinned =
            check("command: x\nverdictPlatforms: [linux, macos]\nexecutor:\n  os: macos\n");
        let error = resolve_check_placement(
            "x",
            &pinned,
            Some(&os("linux", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap_err();
        assert!(error.contains("configured to run on os macos"), "{error}");
        let agreed = resolve_check_placement(
            "x",
            &pinned,
            Some(&os("macos", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap();
        assert_eq!(agreed.verdict_platforms, vec!["macos"]);
    }

    #[test]
    fn an_undeclared_trust_set_admits_any_os_as_its_own() {
        let open = check("command: x\nverdictPlatforms: []\n");
        let effective = resolve_check_placement(
            "x",
            &open,
            Some(&os("windows", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap();
        assert_eq!(effective.verdict_platforms, vec!["windows"]);
    }

    #[test]
    fn a_name_override_keeps_the_configured_trust_set() {
        let effective = resolve_check_placement(
            "rust-tests",
            &rust_tests(),
            Some(&name("fedora", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap();
        assert_eq!(effective.verdict_platforms, vec!["linux", "macos"]);
        assert_eq!(effective.executor_name.as_deref(), Some("fedora"));
        assert_eq!(effective.selector.unwrap().name.as_deref(), Some("fedora"));
    }

    #[test]
    fn a_name_override_conflicting_with_a_configured_name_is_refused() {
        let named = check("command: x\nexecutor:\n  name: bgsrv\n");
        let error = resolve_check_placement(
            "x",
            &named,
            Some(&name("fedora", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap_err();
        assert!(error.contains("executor bgsrv"), "{error}");
        assert!(error.contains("executor fedora"), "{error}");
    }

    #[test]
    fn a_name_override_moves_a_configured_os_into_the_trust_set() {
        let pinned =
            check("command: x\nverdictPlatforms: [linux, macos]\nexecutor:\n  os: linux\n");
        let effective = resolve_check_placement(
            "x",
            &pinned,
            Some(&name("fedora", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .unwrap();
        let selector = effective.selector.unwrap();
        assert_eq!(selector.os, None);
        assert_eq!(selector.name.as_deref(), Some("fedora"));
        assert_eq!(effective.verdict_platforms, vec!["linux"]);
    }

    #[test]
    fn an_os_override_against_a_configured_name_is_a_conflict() {
        let named =
            check("command: x\nverdictPlatforms: [linux, macos]\nexecutor:\n  name: bgsrv\n");
        assert!(resolve_check_placement(
            "x",
            &named,
            Some(&os("linux", CheckSelectorSource::Explicit)),
            |_| None,
        )
        .is_err());
    }

    #[test]
    fn an_override_may_state_only_name_or_os() {
        for selector in [
            ExecutorSelector {
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            },
            ExecutorSelector {
                os: Some("linux".into()),
                required_toolchains: vec!["rust".into()],
                ..ExecutorSelector::default()
            },
            ExecutorSelector {
                os: Some("plan9".into()),
                ..ExecutorSelector::default()
            },
            ExecutorSelector {
                os: Some("linux".into()),
                name: Some("fedora".into()),
                ..ExecutorSelector::default()
            },
        ] {
            assert!(
                CheckPlacementOverride::new(selector.clone(), CheckSelectorSource::Explicit)
                    .is_err(),
                "{selector:?}"
            );
        }
    }

    /// The real contract: `executor-macos-check` counts only macOS. A batch
    /// pinned to a Linux machine by name must step aside for it, exactly as a
    /// batch pinned to `os: linux` does, rather than leave a name no macOS
    /// machine answers to.
    fn executor_macos_check() -> CheckCommand {
        check(
            "command: cargo check --manifest-path src-tauri/Cargo.toml --locked --all-targets -p cairn-exec -p cairn-common\nexecutor:\n  requiredToolchains:\n  - rust\nverdictPlatforms:\n- macos\n",
        )
    }

    fn fleet(name: &str) -> Option<String> {
        match name {
            "fedora" => Some("linux".into()),
            "bgsrv-mac" => Some("macos".into()),
            _ => None,
        }
    }

    #[test]
    fn an_inherited_name_on_an_untrusted_platform_steps_aside_with_a_note() {
        let effective = resolve_check_placement(
            "executor-macos-check",
            &executor_macos_check(),
            Some(&name("fedora", CheckSelectorSource::Inherited)),
            fleet,
        )
        .unwrap();
        assert_eq!(effective.verdict_platforms, vec!["macos"]);
        assert_eq!(effective.executor_name, None);
        let selector = effective.selector.unwrap();
        assert_eq!(
            selector.name, None,
            "the configured lane is placeable again"
        );
        assert_eq!(selector.required_toolchains, vec!["rust"]);
        let note = effective.note.unwrap();
        assert!(
            note.starts_with("batch pin name=fedora not applied: "),
            "{note}"
        );
        assert!(note.contains("executor fedora runs linux"), "{note}");
    }

    #[test]
    fn an_explicit_name_on_an_untrusted_platform_is_refused() {
        let error = resolve_check_placement(
            "executor-macos-check",
            &executor_macos_check(),
            Some(&name("fedora", CheckSelectorSource::Explicit)),
            fleet,
        )
        .unwrap_err();
        assert!(error.contains("verdictPlatforms [macos]"), "{error}");
        assert!(error.contains("executor fedora runs linux"), "{error}");
    }

    #[test]
    fn a_name_on_a_trusted_platform_or_an_unknown_name_is_kept() {
        let trusted = resolve_check_placement(
            "executor-macos-check",
            &executor_macos_check(),
            Some(&name("bgsrv-mac", CheckSelectorSource::Inherited)),
            fleet,
        )
        .unwrap();
        assert_eq!(trusted.executor_name.as_deref(), Some("bgsrv-mac"));
        assert_eq!(trusted.note, None);
        // A name the fleet cannot place is left to placement, which refuses
        // it with the fleet's own words.
        let unknown = resolve_check_placement(
            "executor-macos-check",
            &executor_macos_check(),
            Some(&name("nowhere", CheckSelectorSource::Explicit)),
            fleet,
        )
        .unwrap();
        assert_eq!(unknown.executor_name.as_deref(), Some("nowhere"));
    }
}
