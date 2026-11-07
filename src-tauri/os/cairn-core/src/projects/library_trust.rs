//! The operator's trust in a project's library definitions.
//!
//! A project's `.cairn/` can define agents, recipes, skills, responses, routes
//! and workflows that shadow the workspace's and the packs'. Those files arrive
//! with a clone or a merge, so the resolver skips them until the operator trusts
//! the project (`cairn_config::library`), and even then takes a definition only
//! once the operator has confirmed the authority it carries: its authority
//! projection (tool grants, hooks, scripts, model selection), bound by digest.
//! Prose is not part of the projection, so a reworded prompt applies at once
//! while a widened tool grant waits.
//!
//! Trust is recorded here, in the private database, for one checkout: the path
//! the project had when it was trusted. When the project's checkout moves, the
//! trust and every confirmation are cleared and the clear is journaled with both
//! paths. Every act is the operator's (the runner commands check the caller's
//! credentials) and is journaled in `authorization_events`.
//!
//! The resolver reads trust synchronously from the in-memory registry in
//! `cairn_config::library::trust`; [`refresh`] rebuilds that registry from the
//! database and runs after every change here and at startup.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cairn_common::authorization::AuthorityMutation;
use cairn_common::identity::PrincipalRef;
use cairn_common::ids;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::config::library::{self, LibraryKind, LibraryRoots, ProjectLayer, ProjectTrust};
use crate::config::{project_definitions, ProjectDefinition};
use crate::db::DbState;
use crate::storage::{LocalDb, RowExt};

/// One trusted project as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct TrustRecord {
    pub project_id: String,
    pub project_key: String,
    /// The checkout that was trusted.
    pub project_path: String,
    pub trusted_at_ms: i64,
    /// The authority projection confirmed per definition. A stored row whose
    /// digest does not match its projection is left out, so it confirms nothing.
    pub confirmed: BTreeMap<(LibraryKind, String), Map<String, Value>>,
}

/// A definition the operator confirms, by the digest of the projection they
/// were shown.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Confirmation {
    pub kind: LibraryKind,
    pub name: String,
    pub digest: String,
}

/// What the trust panel shows for one project.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectLibraryStatus {
    pub project_id: String,
    pub project_key: String,
    /// The project's checkout on this machine, when it has one.
    pub path: Option<String>,
    pub trusted: bool,
    pub trusted_at_ms: Option<i64>,
    pub definitions: Vec<ProjectDefinition>,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn principal_json(actor: &PrincipalRef) -> Result<String, String> {
    serde_json::to_string(actor)
        .map_err(|error| format!("the operator cannot be recorded: {error}"))
}

fn same_path(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    match (Path::new(a).canonicalize(), Path::new(b).canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Every stored trust record, with its confirmations.
pub async fn load_all(db: &LocalDb) -> Result<Vec<TrustRecord>, String> {
    let trusts = db
        .query_all(
            "SELECT project_id, project_key, project_path, trusted_at_ms
               FROM project_library_trust",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.i64(3)?)),
        )
        .await
        .map_err(|error| format!("load project library trust: {error}"))?;
    let confirmations = db
        .query_all(
            "SELECT project_id, kind, name, authority_json, authority_digest
               FROM project_library_confirmations",
            (),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("load project library confirmations: {error}"))?;
    let mut records: BTreeMap<String, TrustRecord> = trusts
        .into_iter()
        .map(|(project_id, project_key, project_path, trusted_at_ms)| {
            (
                project_id.clone(),
                TrustRecord {
                    project_id,
                    project_key,
                    project_path,
                    trusted_at_ms,
                    confirmed: BTreeMap::new(),
                },
            )
        })
        .collect();
    for (project_id, kind, name, authority_json, digest) in confirmations {
        let (Some(record), Some(kind)) = (records.get_mut(&project_id), LibraryKind::parse(&kind))
        else {
            continue;
        };
        let Ok(Value::Object(authority)) = serde_json::from_str::<Value>(&authority_json) else {
            continue;
        };
        if library::authority_digest(&authority) != digest {
            log::warn!(
                "ignoring the confirmation of {kind} {name} in {}: its stored digest does not match its projection",
                record.project_key
            );
            continue;
        }
        record.confirmed.insert((kind, name), authority);
    }
    Ok(records.into_values().collect())
}

async fn load(db: &LocalDb, project_id: &str) -> Result<Option<TrustRecord>, String> {
    Ok(load_all(db)
        .await?
        .into_iter()
        .find(|record| record.project_id == project_id))
}

async fn delete_rows(db: &LocalDb, project_id: &str) -> Result<(), String> {
    db.execute(
        "DELETE FROM project_library_confirmations WHERE project_id = ?1",
        (project_id,),
    )
    .await
    .map_err(|error| format!("clear project library confirmations: {error}"))?;
    db.execute(
        "DELETE FROM project_library_trust WHERE project_id = ?1",
        (project_id,),
    )
    .await
    .map_err(|error| format!("clear project library trust: {error}"))?;
    Ok(())
}

/// Every project this install knows a checkout for, read from the private
/// database alone so startup needs no team replica: a local project's
/// `projects.repo_path`, and a team project's `project_routes.local_repo_path`.
/// Answers `(project id when local, key, path)`; a team project is known here
/// by its key.
async fn checkouts(
    db: &LocalDb,
) -> Result<(BTreeMap<String, (String, String)>, BTreeMap<String, String>), String> {
    let local = db
        .query_all("SELECT id, key, repo_path FROM projects", (), |row| {
            Ok((row.text(0)?, row.text(1)?, row.text(2)?))
        })
        .await
        .map_err(|error| format!("list projects: {error}"))?
        .into_iter()
        .filter(|(_, _, path)| !path.is_empty())
        .map(|(id, key, path)| (id, (key, path)))
        .collect();
    let team = db
        .query_all(
            "SELECT project_key, local_repo_path FROM project_routes
              WHERE team_id IS NOT NULL AND local_repo_path IS NOT NULL",
            (),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("list team project checkouts: {error}"))?
        .into_iter()
        .collect();
    Ok((local, team))
}

/// The checkout `project_id` has now, from the same private reads [`refresh`]
/// uses.
fn current_path(
    project_id: &str,
    project_key: &str,
    local: &BTreeMap<String, (String, String)>,
    team: &BTreeMap<String, String>,
) -> Option<String> {
    match ids::parse_route_scope(project_id) {
        Ok(ids::RouteScope::Local) => local.get(project_id).map(|(_, path)| path.clone()),
        Ok(ids::RouteScope::Team(_)) => team
            .get(&cairn_common::uri::canonical_project(project_key))
            .cloned(),
        Err(_) => None,
    }
}

/// Rebuild the resolver's trust registry from the database, first clearing the
/// trust of every project whose checkout moved or that no longer exists.
///
/// `cause` is the principal whose act this refresh follows (a clone that
/// repointed a team project, say), recorded on any clear it journals.
pub async fn refresh(dbs: &DbState, cause: Option<&PrincipalRef>) -> Result<(), String> {
    let db = &dbs.local;
    let (local, team) = checkouts(db).await?;
    let mut trusted = Vec::new();
    for record in load_all(db).await? {
        match current_path(&record.project_id, &record.project_key, &local, &team) {
            Some(path) if same_path(&path, &record.project_path) => trusted.push(record),
            moved => {
                delete_rows(db, &record.project_id).await?;
                let summary = match &moved {
                    Some(path) => format!(
                        "cleared the library trust of {} and its {} confirmation(s): its checkout moved from {} to {}",
                        record.project_key,
                        record.confirmed.len(),
                        record.project_path,
                        path
                    ),
                    None => format!(
                        "cleared the library trust of {} and its {} confirmation(s): the project no longer has a checkout here (it was at {})",
                        record.project_key,
                        record.confirmed.len(),
                        record.project_path
                    ),
                };
                log::info!("{summary}");
                crate::authorization::journal_project_library_act(
                    db,
                    &record.project_id,
                    AuthorityMutation::Delete,
                    summary,
                    cause.cloned(),
                )
                .await;
            }
        }
    }

    let mut entries: BTreeMap<PathBuf, (String, ProjectTrust)> = BTreeMap::new();
    for (key, path) in local.values() {
        entries.insert(PathBuf::from(path), (key.clone(), ProjectTrust::Untrusted));
    }
    for (key, path) in &team {
        entries
            .entry(PathBuf::from(path))
            .or_insert_with(|| (key.clone(), ProjectTrust::Untrusted));
    }
    for record in trusted {
        entries.insert(
            PathBuf::from(&record.project_path),
            (
                record.project_key,
                ProjectTrust::Trusted {
                    confirmed: record.confirmed,
                },
            ),
        );
    }
    let source = format!("{:p}", std::sync::Arc::as_ptr(&dbs.local));
    library::trust::replace_all(
        &source,
        entries
            .into_iter()
            .map(|(path, (key, trust))| (path, key, trust)),
    );
    Ok(())
}

/// The project's key and its checkout on this machine.
async fn project_checkout(
    dbs: &DbState,
    project_id: &str,
) -> Result<(String, Option<String>), String> {
    let (path, key) = crate::projects::crud::resolve_local_repo_path_and_key(dbs, project_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok((key, path.filter(|path| !path.is_empty())))
}

fn roots_for(
    config_dir: &Path,
    key: &str,
    path: &str,
    record: Option<&TrustRecord>,
) -> LibraryRoots {
    let trust = match record {
        Some(record) if same_path(&record.project_path, path) => ProjectTrust::Trusted {
            confirmed: record.confirmed.clone(),
        },
        _ => ProjectTrust::Untrusted,
    };
    LibraryRoots::workspace(config_dir).with_project(ProjectLayer {
        key: key.to_string(),
        path: PathBuf::from(path),
        trust,
    })
}

/// The project's trust and every definition it holds, for the operator to
/// review.
pub async fn status(
    dbs: &DbState,
    config_dir: &Path,
    project_id: &str,
) -> Result<ProjectLibraryStatus, String> {
    let (key, path) = project_checkout(dbs, project_id).await?;
    let record = load(&dbs.local, project_id).await?;
    let definitions = match &path {
        Some(path) => project_definitions(&roots_for(config_dir, &key, path, record.as_ref())),
        None => Vec::new(),
    };
    let trusted = match (&record, &path) {
        (Some(record), Some(path)) => same_path(&record.project_path, path),
        _ => false,
    };
    Ok(ProjectLibraryStatus {
        project_id: project_id.to_string(),
        project_key: key,
        path,
        trusted,
        trusted_at_ms: record
            .filter(|_| trusted)
            .map(|record| record.trusted_at_ms),
        definitions,
    })
}

/// The operator trusts the project's checkout. Its definitions that carry no
/// authority take effect at once; the rest wait for [`confirm`]. Trusting an
/// already-trusted project changes nothing.
pub async fn trust(
    dbs: &DbState,
    config_dir: &Path,
    project_id: &str,
    operator: &PrincipalRef,
) -> Result<ProjectLibraryStatus, String> {
    let (key, path) = project_checkout(dbs, project_id).await?;
    let path = path.ok_or_else(|| {
        format!("{key} has no checkout on this machine, so there is nothing to trust")
    })?;
    let existing = load(&dbs.local, project_id).await?;
    if !existing
        .as_ref()
        .is_some_and(|record| same_path(&record.project_path, &path))
    {
        delete_rows(&dbs.local, project_id).await?;
        dbs.local
            .execute(
                "INSERT INTO project_library_trust
                      (project_id, project_key, project_path, trusted_by_json, trusted_at_ms)
                      VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    project_id,
                    key.as_str(),
                    path.as_str(),
                    principal_json(operator)?.as_str(),
                    now_ms(),
                ),
            )
            .await
            .map_err(|error| format!("record project library trust: {error}"))?;
        crate::authorization::journal_project_library_act(
            &dbs.local,
            project_id,
            AuthorityMutation::Create,
            format!("trusted the library definitions of {key} at {path}"),
            Some(operator.clone()),
        )
        .await;
    }
    refresh(dbs, Some(operator)).await?;
    status(dbs, config_dir, project_id).await
}

/// The operator confirms the authority of the named definitions, each as the
/// digest they were shown. A definition whose projection has changed since is
/// refused, so a confirmation never covers authority the operator did not see.
/// All or nothing: one refusal confirms none.
pub async fn confirm(
    dbs: &DbState,
    config_dir: &Path,
    project_id: &str,
    items: &[Confirmation],
    operator: &PrincipalRef,
) -> Result<ProjectLibraryStatus, String> {
    let current = status(dbs, config_dir, project_id).await?;
    if !current.trusted {
        return Err(format!(
            "{} is not trusted; trust the project before confirming its definitions",
            current.project_key
        ));
    }
    let mut accepted = Vec::new();
    for item in items {
        let definition = current
            .definitions
            .iter()
            .find(|definition| definition.kind == item.kind && definition.name == item.name)
            .ok_or_else(|| {
                format!(
                    "{} has no {} definition named {}",
                    current.project_key, item.kind, item.name
                )
            })?;
        let (Some(authority), Some(digest)) = (&definition.authority, &definition.digest) else {
            return Err(format!(
                "{} {} does not parse, so its authority cannot be confirmed: {}",
                item.kind,
                item.name,
                definition.error.as_deref().unwrap_or("unreadable")
            ));
        };
        if *digest != item.digest {
            return Err(format!(
                "{} {} changed since it was reviewed; review its authority again before confirming",
                item.kind, item.name
            ));
        }
        accepted.push((item, authority.clone(), digest.clone()));
    }
    let by = principal_json(operator)?;
    let at = now_ms();
    for (item, authority, digest) in &accepted {
        let authority_json = serde_json::to_string(&crate::config::pack::canonical_json(
            Value::Object(authority.clone()),
        ))
        .map_err(|error| error.to_string())?;
        dbs.local
            .execute(
                "INSERT INTO project_library_confirmations
                      (project_id, kind, name, authority_json, authority_digest,
                       confirmed_by_json, confirmed_at_ms)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(project_id, kind, name) DO UPDATE SET
                      authority_json = excluded.authority_json,
                      authority_digest = excluded.authority_digest,
                      confirmed_by_json = excluded.confirmed_by_json,
                      confirmed_at_ms = excluded.confirmed_at_ms",
                (
                    project_id,
                    item.kind.as_str(),
                    item.name.as_str(),
                    authority_json.as_str(),
                    digest.as_str(),
                    by.as_str(),
                    at,
                ),
            )
            .await
            .map_err(|error| format!("record a confirmation: {error}"))?;
    }
    if !accepted.is_empty() {
        let listed: Vec<String> = accepted
            .iter()
            .map(|(item, authority, digest)| {
                let fields: Vec<&str> = authority.keys().map(String::as_str).collect();
                format!(
                    "{} {} ({}; sha256 {})",
                    item.kind,
                    item.name,
                    fields.join(", "),
                    &digest[..digest.len().min(12)]
                )
            })
            .collect();
        crate::authorization::journal_project_library_act(
            &dbs.local,
            project_id,
            AuthorityMutation::Update,
            format!(
                "confirmed the authority of {} in {}",
                listed.join(", "),
                current.project_key
            ),
            Some(operator.clone()),
        )
        .await;
    }
    refresh(dbs, Some(operator)).await?;
    status(dbs, config_dir, project_id).await
}

/// The operator withdraws trust from the project, and with it every
/// confirmation. Takes effect at the next resolution.
pub async fn revoke(
    dbs: &DbState,
    config_dir: &Path,
    project_id: &str,
    operator: &PrincipalRef,
) -> Result<ProjectLibraryStatus, String> {
    if let Some(record) = load(&dbs.local, project_id).await? {
        delete_rows(&dbs.local, project_id).await?;
        crate::authorization::journal_project_library_act(
            &dbs.local,
            project_id,
            AuthorityMutation::Delete,
            format!(
                "revoked the library trust of {} at {} and its {} confirmation(s)",
                record.project_key,
                record.project_path,
                record.confirmed.len()
            ),
            Some(operator.clone()),
        )
        .await;
    }
    refresh(dbs, Some(operator)).await?;
    status(dbs, config_dir, project_id).await
}

#[cfg(test)]
mod tests;
