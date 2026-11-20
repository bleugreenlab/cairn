use super::*;

fn is_dev(resource: &CairnResource) -> bool {
    matches!(
        resource,
        CairnResource::Dev
            | CairnResource::DevDb
            | CairnResource::DevPid
            | CairnResource::DevLaunchTicket
    )
}

fn is_launch_ticket(resource: &CairnResource) -> bool {
    matches!(resource, CairnResource::DevLaunchTicket)
}

fn unknown(refusal: Result<CairnResource, UriRefusal>) -> UnknownSegment {
    match refusal {
        Err(UriRefusal::Unknown(unknown)) => unknown,
        other => panic!("expected an unknown-segment refusal, got {other:?}"),
    }
}

fn malformed(refusal: Result<CairnResource, UriRefusal>) -> MalformedUri {
    match refusal {
        Err(UriRefusal::Malformed(fault)) => fault,
        other => panic!("expected a malformed refusal, got {other:?}"),
    }
}

#[test]
fn an_address_the_grammar_parses_is_not_refused() {
    assert_eq!(
        classify_uri("cairn://dev/launch-ticket"),
        Ok(CairnResource::DevLaunchTicket)
    );
    for sample in every_resource_variant() {
        let uri = sample.resource.to_uri();
        assert!(classify_uri(&uri).is_ok(), "{uri}");
    }
}

/// The specimen: a runner that predates the `dev` family is asked for a launch
/// ticket. It names the family it lacks and the ones it has, rather than
/// calling the address garbled.
#[test]
fn a_family_an_older_build_lacks_is_unknown_not_malformed() {
    let older = UriGrammar::without(is_dev);
    let refusal = unknown(older.classify("cairn://dev/launch-ticket"));
    assert_eq!(refusal.parent, "cairn://");
    assert_eq!(refusal.segment, "dev");
    assert!(refusal.is_family());
    assert_eq!(refusal.address(), "cairn://dev");
    assert!(!refusal.known.contains(&"dev".to_string()));
    for family in ["db", "models", "p", "posts", "skills"] {
        assert!(
            refusal.known.contains(&family.to_string()),
            "{family} missing from {:?}",
            refusal.known
        );
    }
    let mut sorted = refusal.known.clone();
    sorted.sort();
    assert_eq!(refusal.known, sorted);
    assert_eq!(refusal.nearest, None, "`db` is two edits from `dev`");
}

#[test]
fn a_sub_resource_an_older_build_lacks_names_its_siblings() {
    let older = UriGrammar::without(is_launch_ticket);
    let refusal = unknown(older.classify("cairn://dev/launch-ticket"));
    assert_eq!(refusal.parent, "cairn://dev");
    assert_eq!(refusal.segment, "launch-ticket");
    assert!(!refusal.is_family());
    assert_eq!(refusal.address(), "cairn://dev/launch-ticket");
    assert_eq!(refusal.known, vec!["db".to_string(), "pid".to_string()]);
}

#[test]
fn a_near_miss_suggests_the_nearest_known_name() {
    let family = unknown(classify_uri("cairn://modles/catalog"));
    assert_eq!(family.segment, "modles");
    assert_eq!(family.nearest.as_deref(), Some("models"));
    assert_eq!(family.address_of("models"), "cairn://models");

    let sub = unknown(classify_uri("cairn://dev/launch-tickte"));
    assert_eq!(sub.parent, "cairn://dev");
    assert_eq!(sub.nearest.as_deref(), Some("launch-ticket"));

    let vcs = unknown(classify_uri("cairn://p/cairn/vcs/blam"));
    assert_eq!(vcs.parent, "cairn://p/cairn/vcs");
    assert_eq!(vcs.nearest.as_deref(), Some("blame"));

    let unrelated = unknown(classify_uri("cairn://launchpad"));
    assert_eq!(unrelated.nearest, None);
}

#[test]
fn a_garbled_address_names_its_fault() {
    assert_eq!(
        malformed(classify_uri("cairn:/posts/1")),
        MalformedUri::Scheme
    );
    assert_eq!(malformed(classify_uri("cairn://")), MalformedUri::Empty);
    assert_eq!(
        malformed(classify_uri("cairn://posts//1")),
        MalformedUri::EmptySegment { position: 2 }
    );
    assert_eq!(
        malformed(classify_uri("cairn://models/")),
        MalformedUri::EmptySegment { position: 2 }
    );
    assert_eq!(
        malformed(classify_uri("cairn://db/issues%zz")),
        MalformedUri::PercentEscape {
            segment: "issues%zz".to_string()
        }
    );
    assert_eq!(
        malformed(classify_uri("cairn://posts/abc")),
        MalformedUri::InvalidValue {
            segment: Some("abc".to_string())
        }
    );
    assert_eq!(
        malformed(classify_uri("cairn://p/cairn/42/2/builder/memories/x")),
        MalformedUri::InvalidValue {
            segment: Some("x".to_string())
        }
    );
    assert!(matches!(
        malformed(classify_uri("cairn://p")),
        MalformedUri::Incomplete { .. }
    ));
}

#[test]
fn the_query_does_not_take_part_in_classification() {
    let refusal = unknown(classify_uri("cairn://modles?limit=5"));
    assert_eq!(refusal.segment, "modles");
}

/// A sample's data values (its project, issue number, node, thread) are
/// variables the parser takes any value for. If one were recorded as a fixed
/// word, a real address with a different value would be refused as unknown.
#[test]
fn sample_values_are_never_mistaken_for_fixed_words() {
    let literals: BTreeSet<&str> = UriGrammar::current()
        .shapes
        .iter()
        .flatten()
        .filter_map(|part| match part {
            Part::Literal(word) => Some(word.as_str()),
            Part::Variable => None,
        })
        .collect();
    for value in [
        "cairn",
        "42",
        "builder",
        "design-review",
        "review-rust",
        "12",
        "turso",
        "inbox",
    ] {
        assert!(
            !literals.contains(value),
            "`{value}` recorded as a fixed word"
        );
    }
}

#[test]
fn malformed_faults_read_as_sentences() {
    assert_eq!(
        MalformedUri::EmptySegment { position: 2 }.to_string(),
        "segment 2 is empty (a doubled or trailing `/`)"
    );
    assert!(MalformedUri::Incomplete {
        next: vec!["a".into(), "b".into()]
    }
    .to_string()
    .ends_with("one of: a, b"));
}

#[test]
fn edit_distance_counts_a_transposition_once() {
    assert_eq!(edit_distance("modles", "models"), 1);
    assert_eq!(edit_distance("dev", "db"), 2);
    assert_eq!(edit_distance("", "abc"), 3);
    assert_eq!(edit_distance("same", "same"), 0);
}
