//! What a terminal `ref` names, for the two surfaces that wait on a terminal.
//!
//! `run {waitFor:{kind:"terminal"}}` and a `kind:"terminal"` wake subscription
//! take one `ref` vocabulary and must agree on what it means, so both resolve it
//! here. The scope written in the ref is the scope searched, and nothing falls
//! back to another: a project terminal URI names a project-owned row, a node or
//! task terminal URI names that job's row, and only the caller-relative forms
//! (`cairn:~/terminal/<slug>` and a bare slug) name the caller's own terminals.
//!
//! Slugs are unique only within one owner. A lookup by slug alone against the
//! caller's job therefore cannot see a project terminal at all, and when the
//! caller happens to own a same-slug terminal it binds that one instead and
//! reports success on a different process's output.

use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_db::turso::params;
use cairn_uri::{self as uri, CairnResource};

/// The owner a terminal row is keyed by in `job_terminals`: a job, or a project
/// (`job_id` NULL).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub(crate) enum TerminalOwner {
    Job { job_id: String },
    Project { project_id: String },
}

/// The agent a ref is resolved for: what its caller-relative forms mean.
pub(crate) struct TerminalCaller<'a> {
    pub job_id: &'a str,
    /// The caller's canonical home: a node, task, thread, or thread-task URI.
    pub home: &'a str,
    pub project_key: &'a str,
}

impl TerminalCaller<'_> {
    fn own_terminal_uri(&self, slug: &str) -> String {
        format!("{}/terminal/{slug}", self.home.trim_end_matches('/'))
    }

    fn own(&self, slug: &str) -> ResolvedTerminal {
        ResolvedTerminal {
            owner: TerminalOwner::Job {
                job_id: self.job_id.to_string(),
            },
            slug: slug.to_string(),
            uri: self.own_terminal_uri(slug),
        }
    }
}

/// One terminal, as a ref named it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedTerminal {
    pub owner: TerminalOwner,
    pub slug: String,
    /// The terminal's canonical URI. Exit and output wakes are routed by this
    /// exact string, so it must be the URI the terminal was created under.
    pub uri: String,
}

/// The ref forms a terminal wait or wake accepts, for refusals to name.
pub(crate) const TERMINAL_REF_FORMS: &str = "`cairn:~/terminal/<slug>` or a bare slug for this agent's own terminal, `cairn:@/terminal/<slug>` for a project terminal, or a canonical node, task, or project terminal URI";

/// The project key of an agent home. Every home is `cairn://p/<key>/...`.
pub(crate) fn project_key_of_home(home: &str) -> Result<String, String> {
    home.strip_prefix("cairn://p/")
        .and_then(|rest| rest.split('/').next())
        .filter(|key| !key.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("agent home {home} names no project"))
}

/// Resolve a terminal ref to the owner and slug it names, without checking that
/// the terminal exists; [`terminal_not_found`] words that refusal.
pub(crate) async fn resolve_terminal_ref(
    db: &LocalDb,
    caller: &TerminalCaller<'_>,
    reference: &str,
) -> Result<ResolvedTerminal, String> {
    let reference = reference.trim();
    let address = reference.split('?').next().unwrap_or(reference);
    if address.is_empty() {
        return Err(format!("terminal ref is empty; use {TERMINAL_REF_FORMS}"));
    }
    if let Some(slug) = address.strip_prefix("cairn:~/terminal/") {
        return slug_of(reference, slug).map(|slug| caller.own(slug));
    }
    if !address.contains(':') && !address.contains('/') {
        return Ok(caller.own(address));
    }
    let canonical = match address.strip_prefix("cairn:@/") {
        Some(rest) => format!("{}/{rest}", uri::build_project_uri(caller.project_key)),
        None => uri::canonicalize_uri_identity(address),
    };
    // The caller's own terminal spelled canonically. Matched before parsing
    // because a thread home's terminals have no parsed resource form.
    if let Some(slug) = canonical.strip_prefix(&caller.own_terminal_uri("")) {
        return slug_of(reference, slug).map(|slug| caller.own(slug));
    }

    let unrecognized = || format!("`{reference}` is not a terminal ref; use {TERMINAL_REF_FORMS}");
    let resource = uri::parse_uri(&canonical).ok_or_else(unrecognized)?;
    let (project, slug) = match &resource {
        CairnResource::NodeTerminal { project, slug, .. }
        | CairnResource::TaskTerminal { project, slug, .. }
        | CairnResource::ProjectTerminal { project, slug } => (project.clone(), slug.clone()),
        _ => return Err(unrecognized()),
    };
    let own_project = uri::canonical_project(caller.project_key);
    if project != own_project {
        return Err(format!(
            "`{reference}` names a terminal in project `{project}`, but terminal waits and wakes resolve within this agent's own project `{own_project}`"
        ));
    }
    let owner = owner_of(db, &resource).await?;
    Ok(ResolvedTerminal {
        owner,
        slug,
        uri: resource.to_uri(),
    })
}

fn slug_of<'a>(reference: &str, slug: &'a str) -> Result<&'a str, String> {
    if slug.is_empty() || slug.contains('/') {
        return Err(format!(
            "`{reference}` does not end in a terminal slug; use {TERMINAL_REF_FORMS}"
        ));
    }
    Ok(slug)
}

async fn owner_of(db: &LocalDb, resource: &CairnResource) -> Result<TerminalOwner, String> {
    let address = resource.to_uri();
    match resource {
        CairnResource::NodeTerminal {
            project,
            number,
            exec_seq,
            node_id,
            ..
        } => crate::jobs::queries::job_id_for_node_coordinate(
            db, project, *number, *exec_seq, node_id, None,
        )
        .await
        .map_err(|error| error.to_string())?
        .map(|job_id| TerminalOwner::Job { job_id })
        .ok_or_else(|| {
            format!(
                "No node owns the terminal {address}. {}",
                crate::resources::node_job_not_found_message(project, *number, *exec_seq, node_id)
            )
        }),
        CairnResource::TaskTerminal {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            ..
        } => crate::jobs::queries::job_id_for_node_coordinate(
            db,
            project,
            *number,
            *exec_seq,
            node_id,
            Some(task_name),
        )
        .await
        .map_err(|error| error.to_string())?
        .map(|job_id| TerminalOwner::Job { job_id })
        .ok_or_else(|| format!("No task owns the terminal {address}")),
        CairnResource::ProjectTerminal { project, .. } => project_id_for_key(db, project)
            .await
            .map_err(|error| error.to_string())?
            .map(|project_id| TerminalOwner::Project { project_id })
            .ok_or_else(|| format!("No project `{project}` owns the terminal {address}")),
        _ => Err(format!("{address} is not a terminal URI")),
    }
}

async fn project_id_for_key(db: &LocalDb, project_key: &str) -> DbResult<Option<String>> {
    let key = uri::canonical_project(project_key);
    db.read(|conn| {
        let key = key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
                    params![key.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
}

/// The slugs one owner holds, for a refusal that shows what the searched scope
/// does contain.
async fn owner_slugs(db: &LocalDb, owner: &TerminalOwner) -> DbResult<Vec<String>> {
    let owner = owner.clone();
    db.read(|conn| {
        let owner = owner.clone();
        Box::pin(async move {
            let mut rows = match &owner {
                TerminalOwner::Job { job_id } => {
                    conn.query(
                        "SELECT slug FROM job_terminals
                         WHERE job_id = ?1 AND slug IS NOT NULL ORDER BY slug",
                        params![job_id.as_str()],
                    )
                    .await?
                }
                TerminalOwner::Project { project_id } => {
                    conn.query(
                        "SELECT slug FROM job_terminals
                         WHERE project_id = ?1 AND job_id IS NULL AND slug IS NOT NULL
                         ORDER BY slug",
                        params![project_id.as_str()],
                    )
                    .await?
                }
            };
            let mut slugs = Vec::new();
            while let Some(row) = rows.next().await? {
                slugs.push(row.text(0)?);
            }
            Ok(slugs)
        })
    })
    .await
}

/// The refusal for a ref whose terminal does not exist. It names the scope that
/// was searched, and any same-slug terminal in the other scope the caller could
/// have meant by that slug, because "not found" is otherwise true of the query
/// and false of the world.
pub(crate) async fn terminal_not_found(
    db: &LocalDb,
    caller: &TerminalCaller<'_>,
    target: &ResolvedTerminal,
) -> String {
    let own = TerminalOwner::Job {
        job_id: caller.job_id.to_string(),
    };
    let searched = match &target.owner {
        owner if *owner == own => "this agent's own terminals".to_string(),
        TerminalOwner::Job { .. } => format!(
            "the terminals of {}",
            target
                .uri
                .rsplit_once("/terminal/")
                .map_or(target.uri.as_str(), |(node, _)| node)
        ),
        TerminalOwner::Project { .. } => format!(
            "the project terminals of `{}`",
            uri::canonical_project(caller.project_key)
        ),
    };
    let slugs = owner_slugs(db, &target.owner).await.unwrap_or_default();
    let holds = if slugs.is_empty() {
        "which holds no terminals".to_string()
    } else {
        format!("which holds: {}", slugs.join(", "))
    };
    let mut message = format!(
        "Terminal not found: no terminal `{}` at {}. Searched {searched}, {holds}.",
        target.slug, target.uri
    );

    let mut elsewhere = Vec::new();
    if target.owner != own
        && super::terminal::lookup_terminal_for_wake(db, &own, &target.slug)
            .await
            .ok()
            .flatten()
            .is_some()
    {
        elsewhere.push(caller.own_terminal_uri(&target.slug));
    }
    if !matches!(target.owner, TerminalOwner::Project { .. }) {
        if let Ok(Some(project_id)) = project_id_for_key(db, caller.project_key).await {
            let project = TerminalOwner::Project { project_id };
            if super::terminal::lookup_terminal_for_wake(db, &project, &target.slug)
                .await
                .ok()
                .flatten()
                .is_some()
            {
                elsewhere.push(uri::build_project_terminal_uri(
                    caller.project_key,
                    &target.slug,
                ));
            }
        }
    }
    if !elsewhere.is_empty() {
        message.push_str(&format!(
            " A different terminal with that slug exists at {}. A terminal ref resolves only in the scope it names, so reference that URI if it is the one you meant.",
            elsewhere.join(" and ")
        ));
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOME: &str = "cairn://p/cairn/7/1/builder";

    /// A node that owns a terminal slugged `dev`, beside a project terminal
    /// slugged `dev`, beside another node that also owns a `dev`. This is the
    /// state an agent reaches by following the dev-build skill in order: it
    /// tries a node terminal, learns it is on the wrong platform, and switches
    /// to the project terminal.
    async fn seed_same_slug_scopes(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','Cairn','cairn','/tmp/p',1,1);
             INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i7','p',7,'T','active',1,1);
             INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i8','p',8,'U','active',1,1);
             INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e7','recipe','i7','p','running',1,1);
             INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e8','recipe','i8','p','running',1,1);
             INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-7','e7','i7','p','Builder','running',1,1,'builder');
             INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-8','e8','i8','p','Builder','running',1,1,'builder');
             INSERT INTO job_terminals (id,job_id,session_id,command,status,created_at,slug,output_tail) VALUES ('t-node','job-7','s-node','bun dev:instance','running',1,'dev','node output');
             INSERT INTO job_terminals (id,job_id,session_id,command,status,created_at,slug,output_tail) VALUES ('t-other','job-8','s-other','bun dev:instance','running',1,'dev','other node output');
             INSERT INTO job_terminals (id,project_id,session_id,command,status,created_at,slug,output_tail) VALUES ('t-project','p','s-project','bun dev:instance','running',1,'dev','project output');",
        )
        .await
        .unwrap();
        db
    }

    fn caller() -> TerminalCaller<'static> {
        TerminalCaller {
            job_id: "job-7",
            home: HOME,
            project_key: "cairn",
        }
    }

    async fn session_of(db: &LocalDb, target: &ResolvedTerminal) -> Option<String> {
        super::super::terminal::lookup_terminal_for_wake(db, &target.owner, &target.slug)
            .await
            .unwrap()
            .and_then(|row| row.session_id)
    }

    #[tokio::test]
    async fn every_ref_resolves_in_the_scope_it_names_even_beside_a_same_slug_terminal() {
        let db = seed_same_slug_scopes("terminal_ref_scopes.db").await;
        let caller = caller();
        let project_owner = TerminalOwner::Project {
            project_id: "p".into(),
        };
        let own_owner = TerminalOwner::Job {
            job_id: "job-7".into(),
        };
        let cases = [
            (
                "cairn://p/cairn/terminal/dev",
                &project_owner,
                "cairn://p/cairn/terminal/dev",
                "s-project",
            ),
            (
                "cairn://p/CAIRN/terminal/dev",
                &project_owner,
                "cairn://p/cairn/terminal/dev",
                "s-project",
            ),
            (
                "cairn:@/terminal/dev",
                &project_owner,
                "cairn://p/cairn/terminal/dev",
                "s-project",
            ),
            (
                "cairn:~/terminal/dev",
                &own_owner,
                "cairn://p/cairn/7/1/builder/terminal/dev",
                "s-node",
            ),
            (
                "dev",
                &own_owner,
                "cairn://p/cairn/7/1/builder/terminal/dev",
                "s-node",
            ),
            (
                "cairn://p/cairn/7/1/builder/terminal/dev",
                &own_owner,
                "cairn://p/cairn/7/1/builder/terminal/dev",
                "s-node",
            ),
        ];
        for (reference, owner, uri, session) in cases {
            let target = resolve_terminal_ref(&db, &caller, reference).await.unwrap();
            assert_eq!(&target.owner, owner, "{reference}");
            assert_eq!(target.slug, "dev", "{reference}");
            assert_eq!(target.uri, uri, "{reference}");
            assert_eq!(
                session_of(&db, &target).await.as_deref(),
                Some(session),
                "{reference} must bind the terminal it names, not a same-slug one elsewhere"
            );
        }

        // Another node's terminal is that node's, not the caller's.
        let other = resolve_terminal_ref(&db, &caller, "cairn://p/cairn/8/1/builder/terminal/dev")
            .await
            .unwrap();
        assert_eq!(
            other.owner,
            TerminalOwner::Job {
                job_id: "job-8".into()
            }
        );
        assert_eq!(session_of(&db, &other).await.as_deref(), Some("s-other"));
    }

    #[tokio::test]
    async fn a_ref_outside_the_terminal_vocabulary_is_refused_by_name() {
        let db = seed_same_slug_scopes("terminal_ref_refusals.db").await;
        let caller = caller();
        for reference in [
            "cairn://p/cairn/7/1/builder/todos",
            "cairn:~/terminal/",
            "https://example.com/terminal/dev",
            "",
        ] {
            let error = resolve_terminal_ref(&db, &caller, reference)
                .await
                .unwrap_err();
            assert!(
                error.contains("cairn:@/terminal/<slug>"),
                "{reference}: {error}"
            );
        }
        let error = resolve_terminal_ref(&db, &caller, "cairn://p/other/terminal/dev")
            .await
            .unwrap_err();
        assert!(error.contains("project `other`"), "{error}");
        let error = resolve_terminal_ref(&db, &caller, "cairn://p/cairn/99/1/builder/terminal/dev")
            .await
            .unwrap_err();
        assert!(error.contains("No node owns the terminal"), "{error}");
    }

    #[tokio::test]
    async fn not_found_names_the_scope_searched_and_the_same_slug_terminal_elsewhere() {
        let db = crate::storage::migrated_test_db("terminal_ref_not_found.db").await;
        db.execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','Cairn','cairn','/tmp/p',1,1);
             INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at) VALUES ('i7','p',7,'T','active',1,1);
             INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES ('e7','recipe','i7','p','running',1,1);
             INSERT INTO jobs (id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES ('job-7','e7','i7','p','Builder','running',1,1,'builder');
             INSERT INTO job_terminals (id,job_id,session_id,command,status,created_at,slug) VALUES ('t-node','job-7','s-node','cargo test','running',1,'tests');
             INSERT INTO job_terminals (id,project_id,session_id,command,status,created_at,slug) VALUES ('t-project','p','s-project','bun dev','running',1,'dev');",
        )
        .await
        .unwrap();
        let caller = caller();

        // The node ref misses, and the refusal points at the project terminal.
        let own = resolve_terminal_ref(&db, &caller, "cairn:~/terminal/dev")
            .await
            .unwrap();
        let message = terminal_not_found(&db, &caller, &own).await;
        assert!(message.contains("Terminal not found"), "{message}");
        assert!(
            message.contains("Searched this agent's own terminals"),
            "{message}"
        );
        assert!(message.contains("which holds: tests"), "{message}");
        assert!(
            message.contains("exists at cairn://p/cairn/terminal/dev"),
            "{message}"
        );

        // The project ref misses, and the refusal points at the node's own.
        let project = resolve_terminal_ref(&db, &caller, "cairn://p/cairn/terminal/tests")
            .await
            .unwrap();
        let message = terminal_not_found(&db, &caller, &project).await;
        assert!(
            message.contains("Searched the project terminals of `cairn`, which holds: dev"),
            "{message}"
        );
        assert!(
            message.contains("exists at cairn://p/cairn/7/1/builder/terminal/tests"),
            "{message}"
        );

        // A slug nobody holds names no alternative.
        let nowhere = resolve_terminal_ref(&db, &caller, "nowhere").await.unwrap();
        let message = terminal_not_found(&db, &caller, &nowhere).await;
        assert!(!message.contains("A different terminal"), "{message}");
    }

    #[test]
    fn a_home_names_its_project() {
        assert_eq!(project_key_of_home(HOME).unwrap(), "cairn");
        assert_eq!(
            project_key_of_home("cairn://p/cairn/design-review").unwrap(),
            "cairn"
        );
        assert!(project_key_of_home("cairn:~/").is_err());
    }
}
