use super::*;
use crate::execution::check_placement::EffectiveCheckPlacement;

fn placement(platforms: &[&str], name: Option<&str>) -> EffectiveCheckPlacement {
    EffectiveCheckPlacement {
        selector: None,
        verdict_platforms: platforms.iter().map(|p| p.to_string()).collect(),
        executor_name: name.map(Into::into),
        note: None,
    }
}

fn fedora() -> CheckMachine {
    CheckMachine {
        name: "fedora".into(),
        os: "linux".into(),
    }
}

#[test]
fn a_linux_verdict_does_not_answer_a_macos_demand() {
    let unmet = unmet_placement(&placement(&["macos"], None), Some(&fedora()), Some("linux"))
        .expect("a linux verdict must not stand in for a macos demand");
    assert!(unmet.contains("on fedora for linux"), "{unmet}");
    assert!(unmet.contains("counts only verdicts for macos"), "{unmet}");
}

#[test]
fn a_verdict_inside_the_demand_or_without_one_is_met() {
    assert_eq!(
        unmet_placement(&placement(&["linux"], None), Some(&fedora()), Some("Linux")),
        None
    );
    assert_eq!(
        unmet_placement(&placement(&[], None), Some(&fedora()), Some("linux")),
        None
    );
    assert_eq!(
        unmet_placement(&placement(&["macos"], None), None, None),
        None,
        "no recorded machine leaves nothing to compare"
    );
}

#[test]
fn a_named_machine_demand_is_met_only_by_that_machine() {
    assert!(unmet_placement(
        &placement(&[], Some("bglab-mac")),
        Some(&fedora()),
        Some("linux")
    )
    .is_some_and(|unmet| unmet.contains("named executor bglab-mac")));
    assert_eq!(
        unmet_placement(
            &placement(&[], Some("fedora")),
            Some(&fedora()),
            Some("linux")
        ),
        None
    );
}
