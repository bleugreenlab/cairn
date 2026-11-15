/// After the one capture, no stage of a manual check (contract, tree,
/// changed files, key, cache, observation, materialization, result) resolves
/// a moving name or captures again: each reads the commit it is handed.
#[test]
fn no_stage_after_the_capture_resolves_a_name_again() {
    let source = include_str!("manual.rs");
    // Anchored at a line start, so the markers quoted in this test never
    // match themselves: only the definition does.
    let body = |start: &str| {
        source
            .split_once(&format!("\n{start}"))
            .unwrap_or_else(|| panic!("{start} is gone"))
            .1
            .split_once("\n}\n")
            .unwrap()
            .0
            .to_string()
    };
    let snapshot = body("pub(super) async fn resolve_manual_check_contract_snapshot(");
    assert_eq!(
        snapshot.matches("capture_manual_check_coordinate(").count(),
        1,
        "exactly one capture per manual check"
    );
    for start in [
        "pub(crate) async fn resolve_check_contract_at_commit(",
        "pub(crate) async fn execute_check_contract(",
        "pub(super) async fn resolve_base_relative_change(",
    ] {
        let stage = body(start);
        for forbidden in [
            "resolve_coordinate(",
            "resolve_job_logical_head(",
            "capture_revision(",
            "prepare_branch(",
            "resolve_typed_revision(",
        ] {
            assert!(!stage.contains(forbidden), "{start} calls {forbidden}");
        }
    }
}
