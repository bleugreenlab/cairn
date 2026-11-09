use super::ResourceKind;

#[test]
fn slug_is_kebab_lowercase_and_roundtrips() {
    assert_eq!(ResourceKind::Issue.slug(), "issue");
    assert_eq!(ResourceKind::NodeArtifact.slug(), "node-artifact");
    assert_eq!(ResourceKind::ProjectIssues.slug(), "project-issues");
    // Every kind's slug resolves back to exactly that kind, so slugs are a
    // total, injective naming of the enum the help projection can rely on.
    for kind in ResourceKind::ALL {
        assert_eq!(ResourceKind::from_slug(&kind.slug()), Some(*kind));
    }
}

#[test]
fn from_slug_rejects_unknown() {
    assert_eq!(ResourceKind::from_slug("not-a-kind"), None);
}
