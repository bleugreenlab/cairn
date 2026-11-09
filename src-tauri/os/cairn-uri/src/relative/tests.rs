use super::*;

/// The operator's sketch, keystroke for keystroke: each line is typed at the
/// prompt the previous line left behind.
#[test]
fn the_operators_sketch_resolves_step_by_step() {
    let project = resolve_relative(ROOT_URI, "p/cairn").unwrap();
    assert_eq!(project, "cairn://p/cairn");
    let issue = resolve_relative(&project, "4279").unwrap();
    assert_eq!(issue, "cairn://p/cairn/4279");
    let node = resolve_relative(&issue, "1/builder").unwrap();
    assert_eq!(node, "cairn://p/cairn/4279/1/builder");
}

#[test]
fn dot_dot_climbs_past_segment_prefixes_that_name_nothing() {
    // `cairn://p/cairn/4279/1` is not a resource, so a single pop would
    // strand the cursor; the climb lands on the issue.
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279/1/builder", "..").unwrap(),
        "cairn://p/cairn/4279"
    );
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279", "..").unwrap(),
        "cairn://p/cairn"
    );
    // `cairn://p` names nothing either, so the project climbs to the root.
    assert_eq!(resolve_relative("cairn://p/cairn", "..").unwrap(), ROOT_URI);
}

#[test]
fn dot_dot_at_the_root_is_an_error_not_a_silent_no_op() {
    assert!(resolve_relative(ROOT_URI, "..").is_err());
}

#[test]
fn dot_dot_composes_with_a_following_segment_path() {
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279", "../4036").unwrap(),
        "cairn://p/cairn/4036"
    );
}

#[test]
fn a_lone_dot_re_reads_the_current_location() {
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279", ".").unwrap(),
        "cairn://p/cairn/4279"
    );
}

#[test]
fn absolute_and_root_absolute_forms_ignore_the_cursor() {
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279", "cairn://p/cairn/issues").unwrap(),
        "cairn://p/cairn/issues"
    );
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279", "/posts").unwrap(),
        "cairn://posts"
    );
}

#[test]
fn home_shorthand_is_forwarded_for_the_host_to_resolve() {
    assert_eq!(
        resolve_relative(ROOT_URI, "cairn:~/todos").unwrap(),
        "cairn:~/todos"
    );
}

#[test]
fn project_shorthand_is_forwarded_for_the_host_to_resolve() {
    for target in [
        "cairn:@",
        "cairn:@/issues?status=active",
        "cairn:@4321",
        "cairn:@performance",
    ] {
        assert_eq!(resolve_relative(ROOT_URI, target).unwrap(), target);
    }
}

#[test]
fn a_query_string_rides_through_unchanged() {
    assert_eq!(
        resolve_relative("cairn://p/cairn", "4279?grep=Status").unwrap(),
        "cairn://p/cairn/4279?grep=Status"
    );
    assert_eq!(
        resolve_relative("cairn://p/cairn/4279", "..?limit=5").unwrap(),
        "cairn://p/cairn?limit=5"
    );
}

#[test]
fn an_input_that_names_no_resource_is_rejected() {
    // The parser is the sole judge: the shell never decides validity itself,
    // which is also why a segment under a project resolves generously (it
    // may name a thread) while these do not resolve at all.
    let error = resolve_relative(ROOT_URI, "nope").unwrap_err();
    assert!(error.contains("does not name a resource"), "{error}");
    assert!(resolve_relative("cairn://p/cairn/4279", "zz/builder").is_err());
    assert!(resolve_relative("cairn://p/cairn/4279/1/builder", "a/b/c/d").is_err());
}

#[test]
fn a_non_cairn_scheme_is_not_treated_as_a_segment_path() {
    assert!(resolve_relative(ROOT_URI, "file:src/lib.rs").is_err());
    assert!(resolve_relative(ROOT_URI, "https://example.com").is_err());
}

#[test]
fn segments_round_trip_through_the_root() {
    assert_eq!(uri_segments(ROOT_URI).unwrap(), Vec::<String>::new());
    assert_eq!(
        uri_segments("cairn://p/cairn/4279?grep=x").unwrap(),
        ["p", "cairn", "4279"]
    );
    assert!(uri_segments("file:src/lib.rs").is_err());
    assert_eq!(nearest_ancestor(ROOT_URI), None);
}
