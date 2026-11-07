//! The authority projection of every library kind, field by field: a change to
//! each field the projection takes changes the digest a confirmation binds,
//! and a change to prose does not.
//!
//! Which fields are taken is decided in each kind's `LibraryItem::authority`,
//! which names every field of the definition type so that a new field does not
//! compile until it is classified there. This table is the behavioural half:
//! it reads real definition files through the real loaders.

use std::path::Path;

use super::library::{self, LibraryKind, LibraryRoots, ProjectTrust};
use super::project_definitions;

type Files = Vec<(&'static str, String)>;

/// The digest of `name`'s authority when the project holds `files` under its
/// `.cairn/`, or why it could not be read.
fn digest(kind: LibraryKind, name: &str, files: &Files) -> Result<String, String> {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("home");
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&workspace).unwrap();
    for (rel, body) in files {
        let path = library::test_support::project_root(&repo).join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, body).unwrap();
    }
    let roots = LibraryRoots::workspace(&workspace).with_project(library::test_support::project(
        Path::new(&repo),
        "demo",
        ProjectTrust::Untrusted,
    ));
    let definition = project_definitions(&roots)
        .into_iter()
        .find(|definition| definition.kind == kind && definition.name == name)
        .ok_or_else(|| format!("{kind} {name} is not listed"))?;
    definition
        .digest
        .ok_or_else(|| definition.error.unwrap_or_else(|| "no digest".into()))
}

/// `base` with `path`'s text edited by replacing `from` with `to`.
fn edit(base: &Files, path: &str, from: &str, to: &str) -> Files {
    base.iter()
        .map(|(rel, body)| {
            if *rel == path {
                assert!(body.contains(from), "{path} has no {from:?}");
                (*rel, body.replacen(from, to, 1))
            } else {
                (*rel, body.clone())
            }
        })
        .collect()
}

/// `base` with one more file.
fn plus(base: &Files, path: &'static str, body: &str) -> Files {
    let mut files = base.clone();
    files.push((path, body.to_string()));
    files
}

struct Table {
    kind: LibraryKind,
    name: &'static str,
    base: Files,
    authority: Vec<(&'static str, Files)>,
    prose: Vec<(&'static str, Files)>,
}

fn check(table: Table) {
    let base = digest(table.kind, table.name, &table.base)
        .unwrap_or_else(|error| panic!("{} base does not load: {error}", table.kind));
    for (field, files) in &table.authority {
        let changed = digest(table.kind, table.name, files).unwrap_or_else(|error| {
            panic!("{} {field} variant does not load: {error}", table.kind)
        });
        assert_ne!(
            changed, base,
            "{} {field} is authority, so changing it must change the digest",
            table.kind
        );
    }
    for (field, files) in &table.prose {
        let changed = digest(table.kind, table.name, files).unwrap_or_else(|error| {
            panic!("{} {field} variant does not load: {error}", table.kind)
        });
        assert_eq!(
            changed, base,
            "{} {field} is prose, so changing it must not change the digest",
            table.kind
        );
    }
}

#[test]
fn agent_authority_fields_change_the_digest_and_prose_does_not() {
    const P: &str = "agents/helper.md";
    let base: Files = vec![(
        P,
        "---\nname: helper\ndescription: Helps.\ntools: Read\n---\n\nHelp.\n".into(),
    )];
    let add = |line: &str| edit(&base, P, "tools: Read\n", &format!("tools: Read\n{line}\n"));
    check(Table {
        kind: LibraryKind::Agent,
        name: "helper",
        authority: vec![
            ("tools", edit(&base, P, "tools: Read", "tools: Read, Bash")),
            ("disallowedTools", add("disallowedTools: [Bash]")),
            ("skills", add("skills: [testing]")),
            ("hooks", add("hooks:\n  Stop: []")),
            ("models", add("models: [sm]")),
            ("seedBudgetTokens", add("seedBudgetTokens: 2000")),
            ("seedEras", add("seedEras: 2")),
        ],
        prose: vec![
            ("description", edit(&base, P, "Helps.", "Helps a lot.")),
            ("prompt", edit(&base, P, "\nHelp.", "\nHelp kindly.")),
            ("name", edit(&base, P, "name: helper", "name: Helper")),
            ("icon", add("icon: star")),
        ],
        base: base.clone(),
    });
}

#[test]
fn skill_authority_fields_change_the_digest_and_prose_does_not() {
    const P: &str = "skills/s/SKILL.md";
    let base: Files = vec![(P, "---\nname: s\ndescription: d\n---\n\nBody.\n".into())];
    check(Table {
        kind: LibraryKind::Skill,
        name: "s",
        authority: vec![
            (
                "allowedTools",
                edit(
                    &base,
                    P,
                    "description: d\n",
                    "description: d\nallowed-tools: Read\n",
                ),
            ),
            (
                "scripts",
                plus(&base, "skills/s/scripts/run.sh", "echo hi\n"),
            ),
        ],
        prose: vec![
            (
                "description",
                edit(&base, P, "description: d", "description: e"),
            ),
            ("prompt", edit(&base, P, "Body.", "Other body.")),
            (
                "references",
                plus(&base, "skills/s/references/r.md", "# R\n"),
            ),
            ("assets", plus(&base, "skills/s/assets/a.txt", "a\n")),
        ],
        base: base.clone(),
    });
}

#[test]
fn response_authority_fields_change_the_digest_and_prose_does_not() {
    const P: &str = "responses/r.md";
    let base: Files = vec![(P, "---\nname: R\ndescription: d\n---\nSay hello.\n".into())];
    let add = |line: &str| {
        edit(
            &base,
            P,
            "description: d\n",
            &format!("description: d\n{line}\n"),
        )
    };
    check(Table {
        kind: LibraryKind::Response,
        name: "r",
        authority: vec![
            ("models", add("models: [sm]")),
            ("timeout", add("timeout: 60s")),
        ],
        prose: vec![
            (
                "description",
                edit(&base, P, "description: d", "description: e"),
            ),
            ("name", edit(&base, P, "name: R", "name: Q")),
            ("template", edit(&base, P, "Say hello.", "Say goodbye.")),
            ("default timeout", add("timeout: 15s")),
        ],
        base: base.clone(),
    });
}

#[test]
fn route_authority_fields_change_the_digest_and_prose_does_not() {
    const P: &str = "routes/r.yaml";
    let base: Files = vec![(
        P,
        "name: R\ndescription: d\nenabled: true\nnodes:\n  - id: mention\n    type: trigger\n    when: { fact: github_comment }\n  - id: work\n    type: sink\n    sink:\n      kind: issue\n      labels: [github]\nedges:\n  - { from: mention, to: work }\ndedupe: 720h\n"
            .into(),
    )];
    check(Table {
        kind: LibraryKind::Route,
        name: "r",
        authority: vec![
            ("enabled", edit(&base, P, "enabled: true", "enabled: false")),
            ("dedupe", edit(&base, P, "dedupe: 720h", "dedupe: 1h")),
            (
                "trigger",
                edit(
                    &base,
                    P,
                    "when: { fact: github_comment }",
                    "when: { fact: channel_message }",
                ),
            ),
            (
                "sink",
                edit(&base, P, "labels: [github]", "labels: [other]"),
            ),
            (
                "edges",
                edit(
                    &base,
                    P,
                    "{ from: mention, to: work }",
                    "{ from: mention, to: work, when: { kind: issue } }",
                ),
            ),
        ],
        prose: vec![
            (
                "description",
                edit(&base, P, "description: d", "description: e"),
            ),
            ("name", edit(&base, P, "name: R", "name: Q")),
            (
                "node name",
                edit(&base, P, "  - id: work\n", "  - id: work\n    name: Work\n"),
            ),
        ],
        base: base.clone(),
    });
}

#[test]
fn recipe_authority_fields_change_the_digest_and_prose_does_not() {
    const P: &str = "recipes/r.yaml";
    let base: Files = vec![(
        P,
        "cairnVersion: 1\nname: R\ndescription: d\ntrigger: manual\nnodes:\n- id: trigger-1\n  type: trigger\n  name: Trigger\n  position: 0@0\n  config:\n    triggerType: manual\n    scope: issue\n- id: build-1\n  type: agent\n  name: Build\n  position: 0@100\n  config:\n    agent: build\nedges:\n- from: trigger-1@control-out\n  to: build-1@control-in\n  type: control\n"
            .into(),
    )];
    check(Table {
        kind: LibraryKind::Recipe,
        name: "r",
        authority: vec![
            ("agent", edit(&base, P, "agent: build", "agent: explore")),
            (
                "node models",
                edit(
                    &base,
                    P,
                    "agent: build\n",
                    "agent: build\n    models: [sm]\n",
                ),
            ),
            (
                "branchTargets",
                edit(
                    &base,
                    P,
                    "trigger: manual\n",
                    "trigger: manual\nbranchTargets: [new, base]\n",
                ),
            ),
            (
                "edges",
                edit(&base, P, "type: control\n", "type: context\n"),
            ),
            (
                "trigger scope",
                edit(&base, P, "scope: issue", "scope: project"),
            ),
        ],
        prose: vec![
            (
                "description",
                edit(&base, P, "description: d", "description: e"),
            ),
            ("name", edit(&base, P, "name: R", "name: Q")),
            ("node name", edit(&base, P, "name: Build", "name: Builder")),
            (
                "position",
                edit(&base, P, "position: 0@100", "position: 40@180"),
            ),
        ],
        base: base.clone(),
    });
}

#[test]
fn workflow_authority_fields_change_the_digest_and_prose_does_not() {
    const M: &str = "workflows/w/workflow.yaml";
    const S: &str = "workflows/w/main.ts";
    let base: Files = vec![
        (M, "name: W\ndescription: d\n".into()),
        (S, "export default async function main() {}\n".into()),
    ];
    check(Table {
        kind: LibraryKind::Workflow,
        name: "w",
        authority: vec![
            ("code", edit(&base, S, "{}", "{ return 1; }")),
            (
                "helper code",
                plus(&base, "workflows/w/lib.ts", "export const x = 1;\n"),
            ),
            (
                "script",
                plus(
                    &edit(
                        &base,
                        M,
                        "description: d\n",
                        "description: d\nscript: other.ts\n",
                    ),
                    "workflows/w/other.ts",
                    "export default async function main() {}\n",
                ),
            ),
            (
                "branch",
                edit(
                    &base,
                    M,
                    "description: d\n",
                    "description: d\nbranch: inherit\n",
                ),
            ),
        ],
        prose: vec![
            (
                "description",
                edit(&base, M, "description: d", "description: e"),
            ),
            ("name", edit(&base, M, "name: W", "name: V")),
        ],
        base: base.clone(),
    });
}
