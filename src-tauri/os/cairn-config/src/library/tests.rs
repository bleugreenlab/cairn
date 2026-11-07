//! The resolution algorithm over a toy item, independent of any one kind's
//! file format. Every real kind's loader is exercised in cairn-core.

use super::test_support::*;
use super::*;
use serde_json::json;
use tempfile::TempDir;

/// A definition whose body is `prose|tools`.
#[derive(Debug, Clone)]
struct Toy {
    id: String,
    prose: String,
    tools: String,
}

impl LibraryItem for Toy {
    fn library_id(&self) -> &str {
        &self.id
    }
    fn authority(&self) -> Map<String, Value> {
        let mut map = Map::new();
        if !self.tools.is_empty() {
            map.insert("tools".into(), json!(self.tools));
        }
        map
    }
}

const KIND: LibraryKind = LibraryKind::Agent;

fn load(dir: &Path, name: &str, _origin: &Origin) -> Option<ConfigResult<Toy>> {
    let path = dir.join(format!("{name}.md"));
    let text = std::fs::read_to_string(&path).ok()?;
    let (prose, tools) = text.split_once('|').unwrap_or((&text, ""));
    Some(ConfigResult::Ok(Toy {
        id: name.to_string(),
        prose: prose.to_string(),
        tools: tools.to_string(),
    }))
}

fn enumerate(dir: &Path, origin: &Origin) -> Result<Vec<(String, ConfigResult<Toy>)>, String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        if let Some(item) = load(dir, &name, origin) {
            out.push((name, item));
        }
    }
    Ok(out)
}

struct Fixture {
    _temp: TempDir,
    workspace: PathBuf,
    project: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let workspace = temp.path().join("home");
        let project = temp.path().join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&project).unwrap();
        Fixture {
            _temp: temp,
            workspace,
            project,
        }
    }

    fn pack(&self, id: &str, name: &str, body: &str) {
        install_pack_lock(&self.workspace, id, &[]);
        write_item(&pack_root(&self.workspace, id), KIND, name, body);
    }

    fn roots(&self, trust: ProjectTrust) -> LibraryRoots {
        LibraryRoots::workspace(&self.workspace).with_project(project(&self.project, "demo", trust))
    }

    fn resolve(&self, trust: ProjectTrust, reference: &str) -> Result<Resolved<Toy>, ResolveError> {
        self.roots(trust)
            .resolve(KIND, &LibraryRef::parse(KIND, reference).unwrap(), load)
    }
}

fn trusted_with(name: &str, tools: &str) -> ProjectTrust {
    let mut projection = Map::new();
    projection.insert("tools".into(), json!(tools));
    trusted(&[(KIND, name, projection)])
}

#[test]
fn the_nearest_layer_wins_and_names_what_it_shadows() {
    let f = Fixture::new();
    f.pack("core", "build", "pack|Read");
    write_item(&f.workspace, KIND, "build", "mine|Read");
    write_item(&project_root(&f.project), KIND, "build", "repo|Read");
    let trust = trusted_with("build", "Read");

    let resolved = f.resolve(trust.clone(), "build").unwrap();
    assert_eq!(resolved.item.prose, "repo");
    assert_eq!(resolved.origin, Origin::Project { key: "demo".into() });
    assert_eq!(
        resolved.shadows,
        vec![Origin::Workspace, Origin::Pack { id: "core".into() }]
    );

    std::fs::remove_file(project_root(&f.project).join("agents/build.md")).unwrap();
    let resolved = f.resolve(trust.clone(), "build").unwrap();
    assert_eq!(resolved.origin, Origin::Workspace);
    assert_eq!(resolved.shadows, vec![Origin::Pack { id: "core".into() }]);

    std::fs::remove_file(f.workspace.join("agents/build.md")).unwrap();
    let resolved = f.resolve(trust, "build").unwrap();
    assert_eq!(resolved.origin, Origin::Pack { id: "core".into() });
    assert!(resolved.shadows.is_empty());
}

#[test]
fn an_untrusted_project_is_skipped_and_a_name_only_it_defines_is_named() {
    let f = Fixture::new();
    write_item(&f.workspace, KIND, "build", "mine|Read");
    write_item(&project_root(&f.project), KIND, "build", "repo|Read");
    write_item(&project_root(&f.project), KIND, "deploy", "repo|Bash");

    let resolved = f.resolve(ProjectTrust::Untrusted, "build").unwrap();
    assert_eq!(resolved.origin, Origin::Workspace);
    assert!(
        resolved.shadows.is_empty(),
        "a skipped definition shadows nothing"
    );

    let error = f.resolve(ProjectTrust::Untrusted, "deploy").unwrap_err();
    assert_eq!(
        error.to_string(),
        "agent `deploy` is defined only in project demo (.cairn/agents/deploy.md), which has not been trusted on this install. The operator trusts it in Settings → Project → Library definitions."
    );

    let listed = f
        .roots(ProjectTrust::Untrusted)
        .list(KIND, enumerate)
        .unwrap();
    let labels: Vec<(String, String)> = listed
        .iter()
        .map(|row| (row.name.clone(), row.origin_label()))
        .collect();
    assert_eq!(
        labels,
        vec![
            ("build".into(), "@, untrusted: skipped".into()),
            ("build".into(), "~".into()),
            ("deploy".into(), "@, untrusted: skipped".into()),
        ]
    );
    assert_eq!(active(listed).len(), 1);
}

#[test]
fn changed_authority_fails_closed_without_falling_back() {
    let f = Fixture::new();
    f.pack("core", "review", "pack|Read");
    write_item(&f.workspace, KIND, "review", "mine|Read");
    write_item(&project_root(&f.project), KIND, "review", "repo v2|Read");

    // Prose changed after confirmation: still in force.
    let resolved = f.resolve(trusted_with("review", "Read"), "review").unwrap();
    assert_eq!(resolved.item.prose, "repo v2");

    // A tool grant changed: refused, and neither lower layer stands in.
    write_item(&project_root(&f.project), KIND, "review", "repo v2|Bash");
    let error = f
        .resolve(trusted_with("review", "Read"), "review")
        .unwrap_err();
    assert!(
        matches!(&error, ResolveError::Unconfirmed { fields, previously_confirmed: true, .. } if fields == &["tools".to_string()])
    );
    assert_eq!(
        error.to_string(),
        "agent `review` resolves to project demo's .cairn/agents/review.md, whose tools changed since the operator confirmed them. It is not used, and nothing is used in its place, until the operator confirms the change in Settings → Project → Library definitions."
    );
    let listed = f
        .roots(trusted_with("review", "Read"))
        .list(KIND, enumerate)
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].origin_label(), "@, authority unconfirmed: tools");

    // Confirming the new content puts it in force.
    let resolved = f.resolve(trusted_with("review", "Bash"), "review").unwrap();
    assert_eq!(resolved.item.tools, "Bash");

    // A definition with no authority needs no confirmation at all.
    write_item(&project_root(&f.project), KIND, "notes", "just prose");
    assert!(f.resolve(trusted(&[]), "notes").is_ok());
    // ...but a new one carrying authority does.
    write_item(&project_root(&f.project), KIND, "fresh", "p|Bash");
    assert!(matches!(
        f.resolve(trusted(&[]), "fresh"),
        Err(ResolveError::Unconfirmed {
            previously_confirmed: false,
            ..
        })
    ));
}

#[test]
fn a_two_pack_collision_is_refused_naming_both_until_something_local_defines_it() {
    let f = Fixture::new();
    f.pack("alpha", "pdf", "a|");
    f.pack("beta", "pdf", "b|");
    let error = f.resolve(ProjectTrust::Untrusted, "pdf").unwrap_err();
    assert_eq!(
        error.to_string(),
        "agent `pdf` is defined by two installed packs and nowhere locally: cairn://packs/alpha/agents/pdf and cairn://packs/beta/agents/pdf. Define ~/.cairn/agents/pdf.md, or name one of those addresses."
    );
    let listed = f
        .roots(ProjectTrust::Untrusted)
        .list(KIND, enumerate)
        .unwrap();
    assert_eq!(
        listed[0].origin_label(),
        "refused: defined by pack:alpha and pack:beta"
    );
    assert!(active(listed).is_empty());

    // Either address still names its own pack's version.
    let pinned = f
        .resolve(ProjectTrust::Untrusted, "cairn://packs/beta/agents/pdf")
        .unwrap();
    assert_eq!(pinned.item.prose, "b");

    write_item(&f.workspace, KIND, "pdf", "mine|");
    let resolved = f.resolve(ProjectTrust::Untrusted, "pdf").unwrap();
    assert_eq!(resolved.origin, Origin::Workspace);
    assert_eq!(resolved.shadows.len(), 2);
}

#[test]
fn the_pack_address_pins_the_packs_version_and_a_removed_item_is_named() {
    let f = Fixture::new();
    f.pack("core", "build", "pack|Read");
    write_item(&f.workspace, KIND, "build", "mine|Read");
    let pinned = f
        .resolve(ProjectTrust::Untrusted, "cairn://packs/core/agents/build")
        .unwrap();
    assert_eq!(pinned.item.prose, "pack");
    assert_eq!(pinned.origin, Origin::Pack { id: "core".into() });

    install_pack_lock(&f.workspace, "core", &[(KIND, "build")]);
    let error = f
        .resolve(ProjectTrust::Untrusted, "cairn://packs/core/agents/build")
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "cairn://packs/core/agents/build was removed from pack core; restore it from the Library."
    );
    // The bare name falls to the workspace, and a removed item resolves for
    // nothing else either.
    std::fs::remove_file(f.workspace.join("agents/build.md")).unwrap();
    assert!(f
        .resolve(ProjectTrust::Untrusted, "build")
        .unwrap_err()
        .is_not_found());

    assert!(LibraryRef::parse(KIND, "cairn://packs/core/skills/build")
        .unwrap_err()
        .contains("names a skill, not a agent"));
    assert!(LibraryRef::parse(KIND, "cairn://packs/core").is_err());
}

#[test]
fn origin_marks_round_trip() {
    for origin in [
        Origin::Workspace,
        Origin::Project {
            key: "cairn".into(),
        },
        Origin::Pack { id: "core".into() },
    ] {
        let json = serde_json::to_string(&origin).unwrap();
        assert_eq!(serde_json::from_str::<Origin>(&json).unwrap(), origin);
    }
    assert_eq!(Origin::Project { key: "x".into() }.label(), "@");
    assert_eq!(Origin::Pack { id: "core".into() }.label(), "pack:core");
}

#[test]
fn the_registry_supplies_trust_and_an_unknown_path_is_untrusted() {
    let f = Fixture::new();
    let unknown = LibraryRoots::at(&f.workspace, Some(&f.project));
    assert_eq!(unknown.project.unwrap().trust, ProjectTrust::Untrusted);

    trust::set(&f.project, "demo", trusted(&[]));
    let known = LibraryRoots::at(&f.workspace, Some(&f.project));
    assert!(known.project.as_ref().unwrap().trust.is_trusted());
    assert_eq!(known.project.unwrap().key, "demo");

    trust::set(&f.project, "demo", ProjectTrust::Untrusted);
    assert!(!LibraryRoots::at(&f.workspace, Some(&f.project))
        .project
        .unwrap()
        .trust
        .is_trusted());
    trust::remove(&f.project);
}
