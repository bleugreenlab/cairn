use super::*;
use crate::config::agents::resolve_agent;
use crate::config::library::{test_support, ResolveError};
use std::sync::Arc;
use tempfile::TempDir;

struct Fixture {
    _temp: TempDir,
    dbs: DbState,
    workspace: PathBuf,
    repo: PathBuf,
}

const PROJECT: &str = "local-trust-project";

fn operator() -> PrincipalRef {
    PrincipalRef::Machine {
        device_id: "desk-1".to_string(),
    }
}

fn agent(tools: &str, prompt: &str) -> String {
    format!("---\nname: helper\ndescription: d\ntools: {tools}\n---\n\n{prompt}")
}

async fn fixture() -> Fixture {
    let temp = TempDir::new().unwrap();
    let workspace = temp.path().join("home");
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&repo).unwrap();
    let db = Arc::new(
        LocalDb::open_migrated_for_test(temp.path().join("cairn.db"))
            .await
            .unwrap(),
    );
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
         VALUES (?1, 'default', 'Trust', 'trust', ?2, 'main', 1, 1)",
        (PROJECT, repo.to_string_lossy().to_string()),
    )
    .await
    .unwrap();
    let index =
        Arc::new(crate::storage::SearchIndex::open_or_create(temp.path().join("index")).unwrap());
    let dbs = DbState::new(db, index);
    Fixture {
        _temp: temp,
        dbs,
        workspace,
        repo,
    }
}

impl Fixture {
    fn write_agent(&self, tools: &str, prompt: &str) {
        test_support::write_item(
            &test_support::project_root(&self.repo),
            LibraryKind::Agent,
            "helper",
            &agent(tools, prompt),
        );
    }

    /// What the runner's resolver sees now, through the registry.
    fn resolve(&self) -> Result<library::Resolved<crate::config::agents::FileAgent>, ResolveError> {
        resolve_agent(
            &LibraryRoots::at(&self.workspace, Some(&self.repo)),
            "helper",
        )
    }

    async fn status(&self) -> ProjectLibraryStatus {
        status(&self.dbs, &self.workspace, PROJECT).await.unwrap()
    }

    async fn journal(&self) -> Vec<(String, String, Option<String>)> {
        self.dbs
            .local
            .query_all(
                "SELECT mutation, summary, principal_json FROM authorization_events
                  WHERE reason = 'project_library_trust' ORDER BY decided_at, rowid",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, Some(row.text(2)?))),
            )
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn a_definition_takes_effect_only_once_trusted_and_confirmed_and_asks_again_after_an_authority_edit(
) {
    let fx = fixture().await;
    fx.write_agent("Read", "Help.");
    refresh(&fx.dbs, None).await.unwrap();

    // An agent-created or freshly cloned project is untrusted.
    assert!(matches!(fx.resolve(), Err(ResolveError::Untrusted { .. })));
    assert!(!fx.status().await.trusted);

    // Trusted, but a definition carrying authority waits for confirmation.
    let trusted = trust(&fx.dbs, &fx.workspace, PROJECT, &operator())
        .await
        .unwrap();
    assert!(trusted.trusted);
    assert!(matches!(
        fx.resolve(),
        Err(ResolveError::Unconfirmed { .. })
    ));
    let definition = &trusted.definitions[0];
    assert_eq!(definition.status, "unconfirmed");

    // Confirmed as shown, it resolves from the project.
    let confirmed = confirm(
        &fx.dbs,
        &fx.workspace,
        PROJECT,
        &[Confirmation {
            kind: LibraryKind::Agent,
            name: "helper".to_string(),
            digest: definition.digest.clone().unwrap(),
        }],
        &operator(),
    )
    .await
    .unwrap();
    assert_eq!(confirmed.definitions[0].status, "active");
    assert!(fx.resolve().unwrap().origin.is_project());

    // Prose changes apply with no new confirmation.
    fx.write_agent("Read", "Help, more kindly.");
    assert!(fx.resolve().unwrap().origin.is_project());

    // A widened tool grant asks again, naming the field.
    fx.write_agent("Read, Bash", "Help, more kindly.");
    match fx.resolve() {
        Err(ResolveError::Unconfirmed {
            fields,
            previously_confirmed,
            ..
        }) => {
            assert_eq!(fields, vec!["tools".to_string()]);
            assert!(previously_confirmed);
        }
        other => panic!("expected an unconfirmed refusal, got {other:?}"),
    }

    // Revocation takes effect at the next resolution.
    revoke(&fx.dbs, &fx.workspace, PROJECT, &operator())
        .await
        .unwrap();
    assert!(matches!(fx.resolve(), Err(ResolveError::Untrusted { .. })));

    let journal = fx.journal().await;
    let mutations: Vec<&str> = journal.iter().map(|(m, _, _)| m.as_str()).collect();
    assert_eq!(mutations, ["create", "update", "delete"]);
    assert!(journal
        .iter()
        .all(|(_, _, actor)| actor.as_deref().is_some_and(|a| a.contains("desk-1"))));
    assert!(
        journal[1].1.contains("agent helper (") && journal[1].1.contains("tools; sha256 "),
        "{journal:?}"
    );
}

#[tokio::test]
async fn a_confirmation_of_a_projection_that_has_since_changed_is_refused() {
    let fx = fixture().await;
    fx.write_agent("Read", "Help.");
    let reviewed = trust(&fx.dbs, &fx.workspace, PROJECT, &operator())
        .await
        .unwrap()
        .definitions[0]
        .digest
        .clone()
        .unwrap();
    fx.write_agent("Read, Bash", "Help.");
    let error = confirm(
        &fx.dbs,
        &fx.workspace,
        PROJECT,
        &[Confirmation {
            kind: LibraryKind::Agent,
            name: "helper".to_string(),
            digest: reviewed,
        }],
        &operator(),
    )
    .await
    .unwrap_err();
    assert!(error.contains("changed since it was reviewed"), "{error}");
    assert!(matches!(
        fx.resolve(),
        Err(ResolveError::Unconfirmed { .. })
    ));
}

#[tokio::test]
async fn repointing_a_project_clears_its_trust_and_every_confirmation_and_journals_both_paths() {
    let fx = fixture().await;
    fx.write_agent("Read", "Help.");
    let digest = trust(&fx.dbs, &fx.workspace, PROJECT, &operator())
        .await
        .unwrap()
        .definitions[0]
        .digest
        .clone()
        .unwrap();
    confirm(
        &fx.dbs,
        &fx.workspace,
        PROJECT,
        &[Confirmation {
            kind: LibraryKind::Agent,
            name: "helper".to_string(),
            digest,
        }],
        &operator(),
    )
    .await
    .unwrap();
    assert!(fx.resolve().is_ok());

    // The same definitions at a new checkout are not the checkout the
    // operator trusted.
    let moved = fx.repo.with_file_name("moved");
    std::fs::rename(&fx.repo, &moved).unwrap();
    fx.dbs
        .local
        .execute(
            "UPDATE projects SET repo_path = ?1 WHERE id = ?2",
            (moved.to_string_lossy().to_string(), PROJECT),
        )
        .await
        .unwrap();
    let mover = PrincipalRef::Machine {
        device_id: "desk-2".to_string(),
    };
    refresh(&fx.dbs, Some(&mover)).await.unwrap();

    assert!(load_all(&fx.dbs.local).await.unwrap().is_empty());
    let at_new = resolve_agent(&LibraryRoots::at(&fx.workspace, Some(&moved)), "helper");
    assert!(matches!(at_new, Err(ResolveError::Untrusted { .. })));
    let (mutation, summary, actor) = fx.journal().await.pop().unwrap();
    assert_eq!(mutation, "delete");
    assert!(
        summary.contains(&fx.repo.to_string_lossy().to_string()),
        "{summary}"
    );
    assert!(
        summary.contains(&moved.to_string_lossy().to_string()),
        "{summary}"
    );
    assert!(actor.unwrap().contains("desk-2"));
}
