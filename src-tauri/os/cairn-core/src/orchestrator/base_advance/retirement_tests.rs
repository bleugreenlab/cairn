//! Retirement of the old runtime's queue against a real store: what it
//! records, what it never touches, and how the next exact demand recovers.

use super::*;

fn item(status: &str, outcome: Option<&str>, observed: Option<&str>) -> LegacyItem {
    LegacyItem {
        bookmark: "agent/x".to_string(),
        status: status.to_string(),
        outcome_kind: outcome.map(str::to_string),
        observed_tip: observed.map(str::to_string),
        open_session: false,
        replay_requested: false,
    }
}

#[test]
fn settlement_follows_the_accepted_install_and_the_literal_head() {
    let installed = item("graph_moved", Some("rebased_clean"), Some("h1"));
    let pending = settle(&installed, Some("h1")).unwrap();
    assert_eq!(
        (
            pending.outcome,
            pending.observed_tip.as_str(),
            pending.status
        ),
        (OUTCOME_EXPORT_PENDING, "h1", Some("completed"))
    );
    let moved_on = settle(&installed, Some("h2")).unwrap();
    assert_eq!(
        (moved_on.outcome, moved_on.observed_tip.as_str()),
        (OUTCOME_EXPORT_SUPERSEDED, "h1"),
        "the later operation owns the export; the record keeps the install it describes"
    );
    assert!(moved_on.note.contains("literal head h2"));
    assert!(
        settle(&item("graph_moved", Some("silent"), Some("h1")), Some("h1")).is_some(),
        "a silent install moved the branch too"
    );

    let unfinished = item("pending", Some("deferred"), Some("h0"));
    assert_eq!(
        settle(&unfinished, Some("h0")),
        None,
        "nothing was installed"
    );
    let maybe_installed = settle(&unfinished, Some("h1")).unwrap();
    assert_eq!(
        (
            maybe_installed.outcome,
            maybe_installed.observed_tip.as_str(),
            maybe_installed.status
        ),
        (OUTCOME_EXPORT_PENDING, "h1", None)
    );
    assert!(
        maybe_installed.note.contains("observed h0"),
        "the old coordinate is kept"
    );

    for terminal in [
        item("graph_moved", Some("conflicted"), Some("h0")),
        item("completed", Some("rebased_clean"), Some("h0")),
        item("suppressed", Some("quarantined"), Some("h0")),
    ] {
        assert!(
            !terminal.inspected(),
            "{terminal:?} installed nothing outstanding"
        );
    }
    let mut open = installed.clone();
    open.open_session = true;
    assert!(!open.inspected(), "an open session is left verbatim");
    let mut served = installed.clone();
    served.replay_requested = true;
    assert!(served.inspected(), "the install itself served the request");
    let mut requested = unfinished.clone();
    requested.replay_requested = true;
    assert!(
        !requested.inspected(),
        "an unserved request is the replay path's"
    );
    assert_eq!(
        settle(&installed, None),
        None,
        "a vanished bookmark exports nothing"
    );
}
