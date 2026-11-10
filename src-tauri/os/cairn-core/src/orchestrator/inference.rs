//! The cairn-inference facet's host: the inventory in the orch's database, and
//! the writes that change it.
//!
//! Every record is Cairn's own and the operator writes it: the whole inventory
//! at once ([`Authoring::Replace`], a document in Cairn's record format, the
//! inverse of `cairn://inference?format=yaml`), or records added, replaced and
//! removed within it ([`Authoring::Patch`]). Either is validated as a whole and
//! refused naming every problem, and neither may take away what is running: a
//! deployment whose model occupies its machines is stopped before it is
//! removed or moved. Records mirrored from bginf before it retired are
//! read-only until [`adopt`] makes them Cairn's, keeping their ids; authoring
//! is refused until then, so nothing half-adopted is ever written.
//!
//! Which machines form a pool is not written here: placement detects pools
//! from the links executors report ([`crate::fleet::pools`]).

use std::collections::BTreeMap;
use std::fmt;

use cairn_db::turso::params;
use cairn_inference::control::Phase;
use cairn_inference::{
    DeploymentId, InferenceProfile, InferenceProfileId, Inventory, MachineName, RecordSource,
    ValidationError, YamlError,
};
use serde::Serialize;

use super::Orchestrator;
use crate::storage::{LocalDb, RowExt};

const MODELS: &str = "inference_models";
const PROFILES: &str = "inference_profiles";
const DEPLOYMENTS: &str = "inference_deployments";
const MACHINES: &str = "inference_machines";

/// Every read-modify-write of the inventory holds this, so two writes never
/// interleave.
pub(crate) static INVENTORY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn load_bodies(db: &LocalDb, table: &'static str) -> Result<Vec<(String, String)>, String> {
    db.query_all(
        format!("SELECT id, body_json FROM {table} ORDER BY id"),
        (),
        |row| Ok((row.text(0)?, row.text(1)?)),
    )
    .await
    .map_err(|error| format!("read {table}: {error}"))
}

fn parse_bodies<T: serde::de::DeserializeOwned>(
    table: &str,
    rows: Vec<(String, String)>,
) -> Vec<T> {
    rows.into_iter()
        .filter_map(|(id, body)| match serde_json::from_str(&body) {
            Ok(record) => Some(record),
            Err(error) => {
                log::warn!("inference: {table} row {id} does not parse and is skipped: {error}");
                None
            }
        })
        .collect()
}

/// The stored inventory.
pub async fn load_inventory(db: &LocalDb) -> Result<Inventory, String> {
    let mut inventory = Inventory::default();
    for model in parse_bodies(MODELS, load_bodies(db, MODELS).await?) {
        inventory.insert_model(model);
    }
    for profile in parse_bodies(PROFILES, load_bodies(db, PROFILES).await?) {
        inventory.insert_profile(profile);
    }
    for deployment in parse_bodies(DEPLOYMENTS, load_bodies(db, DEPLOYMENTS).await?) {
        inventory.insert_deployment(deployment);
    }
    for machine in parse_bodies(MACHINES, load_bodies(db, MACHINES).await?) {
        inventory.insert_machine(machine);
    }
    Ok(inventory)
}

/// One row to write: `body` `None` deletes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowChange {
    table: &'static str,
    id: String,
    source: &'static str,
    body: Option<String>,
}

fn diff_kind<K: Ord + ToString, T: Serialize + PartialEq>(
    table: &'static str,
    current: &BTreeMap<K, T>,
    next: &BTreeMap<K, T>,
    source_of: fn(&T) -> RecordSource,
    changes: &mut Vec<RowChange>,
) {
    for (id, record) in next {
        if current.get(id) != Some(record) {
            changes.push(RowChange {
                table,
                id: id.to_string(),
                source: source_of(record).as_str(),
                body: Some(serde_json::to_string(record).expect("inference records serialize")),
            });
        }
    }
    for (id, record) in current {
        if !next.contains_key(id) {
            changes.push(RowChange {
                table,
                id: id.to_string(),
                source: source_of(record).as_str(),
                body: None,
            });
        }
    }
}

pub(crate) fn diff_rows(current: &Inventory, next: &Inventory) -> Vec<RowChange> {
    let mut changes = Vec::new();
    diff_kind(
        MODELS,
        &current.models,
        &next.models,
        |m| m.source,
        &mut changes,
    );
    diff_kind(
        PROFILES,
        &current.profiles,
        &next.profiles,
        |r| r.source,
        &mut changes,
    );
    diff_kind(
        DEPLOYMENTS,
        &current.deployments,
        &next.deployments,
        |d| d.source,
        &mut changes,
    );
    diff_kind(
        MACHINES,
        &current.machines,
        &next.machines,
        |m| m.source,
        &mut changes,
    );
    changes
}

/// Write every changed row in one transaction.
pub(crate) async fn save_rows(
    db: &LocalDb,
    changes: Vec<RowChange>,
    at: i64,
) -> Result<(), String> {
    db.write(move |conn| {
        let changes = changes.clone();
        Box::pin(async move { save_rows_in(conn, changes, at, None).await })
    })
    .await
    .map_err(|error| format!("write inference records: {error}"))
}

/// Persist validated inventory changes in the caller's private transaction,
/// so domain authority can advance atomically with the records it authorizes.
pub(crate) async fn save_rows_in(
    conn: &crate::storage::TrackedConnection,
    changes: Vec<RowChange>,
    at: i64,
    generation: Option<&str>,
) -> crate::storage::DbResult<()> {
    for change in changes {
        if change.table == PROFILES {
            if change.body.is_some() {
                // Deletion must not launder a generated ID into ordinary inventory.
                // Only the draft transaction that owns the ID may recreate it.
                let mut rows = conn
                    .query(
                        "SELECT o.generation_id FROM inference_generated_profiles o
                     LEFT JOIN inference_profiles p ON p.id = o.profile_id
                     WHERE o.profile_id = ?1 AND p.id IS NULL",
                        (change.id.as_str(),),
                    )
                    .await?;
                let owner = rows.next().await?.map(|row| row.text(0)).transpose()?;
                drop(rows);
                if let Some(owner) = owner.filter(|owner| generation != Some(owner.as_str())) {
                    return Err(crate::storage::DbError::Row(format!(
                        "profile ID {} is reserved by generation {owner}, even after deletion; choose a fresh profile ID such as {}-new",
                        change.id, change.id
                    )));
                }
            }
            // Preserve the stale prompt coordinate so the approval door can
            // withdraw it, but never let an edit-and-undo restore consent.
            conn.execute(
                "UPDATE inference_generations SET approved_surface = NULL, approval_revision = NULL
                 WHERE draft_profile = ?1 AND state = 'running'",
                (change.id.as_str(),),
            )
            .await?;
            conn.execute(
                "UPDATE inference_generated_profiles SET promoted_verification_id = NULL,
                 inventory_revision = inventory_revision + 1,
                 active = CASE WHEN ?2 = 1 THEN 0 ELSE active END WHERE profile_id = ?1",
                params![change.id.as_str(), i64::from(change.body.is_none())],
            )
            .await?;
        }
        match change.body {
            Some(body) => {
                conn.execute(
                    &format!(
                        "INSERT INTO {} (id, source, body_json, created_at, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?4)
                         ON CONFLICT(id) DO UPDATE SET source = excluded.source,
                           body_json = excluded.body_json, updated_at = excluded.updated_at",
                        change.table
                    ),
                    params![change.id, change.source, body, at],
                )
                .await?;
            }
            None => {
                conn.execute(
                    &format!("DELETE FROM {} WHERE id = ?1", change.table),
                    params![change.id],
                )
                .await?;
            }
        }
    }
    Ok(())
}

/// Make every record of `from` Cairn's own, keeping its id and contents, and
/// say which. Adopting again adopts nothing and says so.
pub async fn adopt(db: &LocalDb, from: RecordSource) -> Result<String, String> {
    let _held = INVENTORY.lock().await;
    let current = load_inventory(db).await?;
    let mut next = current.clone();
    let adopted = next.adopt(from);
    if adopted.is_empty() {
        return Ok(format!(
            "Nothing to adopt: no record is a {} record.",
            from.as_str()
        ));
    }
    save_rows(db, diff_rows(&current, &next), now_ms()).await?;
    Ok(format!(
        "Adopted {} {} records as Cairn's own, keeping their ids:\n- {}",
        adopted.len(),
        from.as_str(),
        adopted.join("\n- ")
    ))
}

/// Records to take out of the inventory, by kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Removals {
    pub models: Vec<String>,
    pub profiles: Vec<InferenceProfileId>,
    pub deployments: Vec<DeploymentId>,
    pub machines: Vec<MachineName>,
}

/// A change the operator writes to Cairn's own records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authoring {
    /// The whole inventory, as a document in Cairn's record format.
    Replace(String),
    /// Add or replace the records of a document in Cairn's record format,
    /// then remove these.
    Patch {
        upsert: Option<String>,
        remove: Removals,
    },
}

/// One reason a write is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The inventory the write would leave does not cohere.
    Invalid(ValidationError),
    /// The document written does not parse.
    Unparsed(String),
    /// A profile written as new under an id another profile has.
    Taken(InferenceProfileId),
    /// Anything else, in words: a record that is not Cairn's, one to remove
    /// that is not there, a running deployment it would take away.
    Said(String),
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(problem) => problem.fmt(f),
            Self::Unparsed(error) => write!(f, "the document does not parse: {error}"),
            Self::Taken(id) => write!(f, "there is already a profile {id}"),
            Self::Said(said) => f.write_str(said),
        }
    }
}

/// Why a write did not land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// Refused, with every reason; nothing was written.
    Refused(Vec<Refusal>),
    /// The records could not be read or written.
    Failed(String),
}

impl From<WriteError> for String {
    fn from(error: WriteError) -> Self {
        match error {
            WriteError::Refused(refusals) => refused(refusals),
            WriteError::Failed(error) => error,
        }
    }
}

fn document_refusals(error: YamlError) -> Vec<Refusal> {
    match error {
        YamlError::Parse(error) => vec![Refusal::Unparsed(error)],
        YamlError::Invalid(problems) => problems.into_iter().map(Refusal::Invalid).collect(),
    }
}

fn remove_each<K: Ord + fmt::Display, T>(
    kind: &str,
    records: &mut BTreeMap<K, T>,
    ids: &[K],
    problems: &mut Vec<Refusal>,
) {
    for id in ids {
        if records.remove(id).is_none() {
            problems.push(Refusal::Said(format!("there is no {kind} {id} to remove")));
        }
    }
}

/// The inventory `authoring` leaves, or every reason it is refused.
fn authored(current: &Inventory, authoring: &Authoring) -> Result<Inventory, Vec<Refusal>> {
    let foreign = current.foreign_records();
    if !foreign.is_empty() {
        return Err(vec![Refusal::Said(format!(
            "{} records are still mirrored from bginf and read-only ({}); adopt them first: patch cairn://inference {{adopt:\"bginf\"}}",
            foreign.len(),
            foreign.join(", ")
        ))]);
    }
    let mut problems = Vec::new();
    let next = match authoring {
        Authoring::Replace(text) => match Inventory::from_yaml(text) {
            Ok(next) => next,
            Err(YamlError::Invalid(found)) => {
                // Still name any record that is not Cairn's, below.
                problems.extend(found.into_iter().map(Refusal::Invalid));
                Inventory::parse_yaml(text).unwrap_or_default()
            }
            Err(error) => return Err(document_refusals(error)),
        },
        Authoring::Patch { upsert, remove } => {
            let mut next = current.clone();
            if let Some(text) = upsert {
                next.upsert(Inventory::parse_yaml(text).map_err(document_refusals)?);
            }
            remove_each("model", &mut next.models, &remove.models, &mut problems);
            remove_each(
                "profile",
                &mut next.profiles,
                &remove.profiles,
                &mut problems,
            );
            remove_each(
                "deployment",
                &mut next.deployments,
                &remove.deployments,
                &mut problems,
            );
            remove_each(
                "machine",
                &mut next.machines,
                &remove.machines,
                &mut problems,
            );
            problems.extend(next.problems().into_iter().map(Refusal::Invalid));
            next
        }
    };
    problems.extend(next.foreign_records().into_iter().map(|record| {
        Refusal::Said(format!("{record} is not Cairn's; a record written here says `source: cairn` or names no source"))
    }));
    if problems.is_empty() {
        Ok(next)
    } else {
        Err(problems)
    }
}

/// What `next` would take away that is running: a deployment whose model
/// occupies its machines, removed or moved to other machines.
fn running_refusals(
    current: &Inventory,
    next: &Inventory,
    phases: &BTreeMap<DeploymentId, Phase>,
) -> Vec<String> {
    let mut refusals = Vec::new();
    for (id, phase) in phases.iter().filter(|(_, phase)| phase.reserves()) {
        let Some(before) = current.deployments.get(id) else {
            continue;
        };
        let page = format!("cairn://inference/deployments/{id}");
        match next.deployments.get(id) {
            None => refusals.push(format!(
                "deployment {id} is {} on {}; stop it first ({page}), or its machines would be released with the model still on them",
                phase.label(),
                before.machines.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
            )),
            Some(after) if after.machines != before.machines || after.head != before.head => {
                refusals.push(format!(
                    "deployment {id} is {}; stop it ({page}) before moving it to other machines",
                    phase.label()
                ))
            }
            Some(_) => {}
        }
    }
    refusals
}

fn refused(problems: Vec<Refusal>) -> String {
    let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
    format!("Refused; nothing was written:\n- {}", problems.join("\n- "))
}

/// Write what `authoring` makes of the current records, holding the inventory
/// while it reads them, decides and writes: validated as a whole, refused with
/// every problem, and never taking away what is running.
async fn write_checked(
    db: &LocalDb,
    authoring: impl FnOnce(&Inventory) -> Result<Authoring, Vec<Refusal>>,
) -> Result<String, WriteError> {
    let held = INVENTORY.lock().await;
    let current = load_inventory(db).await.map_err(WriteError::Failed)?;
    let authoring = authoring(&current).map_err(WriteError::Refused)?;
    let (changes, summary) = prepare_inventory_write(db, &current, &authoring, &held).await?;
    save_rows(db, changes, now_ms())
        .await
        .map_err(WriteError::Failed)?;
    Ok(summary)
}

/// Validate against the snapshot protected by the inventory lock without
/// opening a nested write transaction. The caller may persist the returned
/// changes together with its domain ledger in one transaction.
pub(crate) async fn prepare_inventory_write(
    db: &LocalDb,
    current: &Inventory,
    authoring: &Authoring,
    held: &tokio::sync::MutexGuard<'_, ()>,
) -> Result<(Vec<RowChange>, String), WriteError> {
    prepare_inventory_write_for_generation(db, current, authoring, held, None).await
}

/// Only the generated-draft door may author generation provenance. All other
/// writers use the ordinary preparation path, including imports and editors.
pub(crate) async fn prepare_inventory_write_for_generation(
    db: &LocalDb,
    current: &Inventory,
    authoring: &Authoring,
    _held: &tokio::sync::MutexGuard<'_, ()>,
    generation: Option<&str>,
) -> Result<(Vec<RowChange>, String), WriteError> {
    let next = authored(current, authoring).map_err(WriteError::Refused)?;
    let protected =
        super::inference_generate::promotion::inventory_refusals(db, current, &next, generation)
            .await
            .map_err(WriteError::Failed)?;
    if !protected.is_empty() {
        return Err(WriteError::Refused(
            protected.into_iter().map(Refusal::Said).collect(),
        ));
    }
    let phases = super::inference_control::deployment_phases(db)
        .await
        .map_err(WriteError::Failed)?;
    let running = running_refusals(current, &next, &phases);
    if !running.is_empty() {
        return Err(WriteError::Refused(
            running.into_iter().map(Refusal::Said).collect(),
        ));
    }
    let report = Inventory::changes(current, &next);
    let summary = if !report.has_changes() {
        "The inventory is unchanged.".to_string()
    } else {
        format!(
            "Wrote the inventory:\n- {}\nRead cairn://inference?format=yaml for the whole of it.",
            report.lines().join("\n- ")
        )
    };
    Ok((diff_rows(current, &next), summary))
}

/// Write Cairn's own records, or refuse with every problem and write nothing.
pub async fn write_inventory(db: &LocalDb, authoring: &Authoring) -> Result<String, String> {
    Ok(write_checked(db, |_| Ok(authoring.clone())).await?)
}

/// Write within an inventory epoch already held by a domain operation.
pub(crate) async fn write_inventory_held(
    db: &LocalDb,
    authoring: &Authoring,
    held: &tokio::sync::MutexGuard<'_, ()>,
) -> Result<String, String> {
    let current = load_inventory(db).await?;
    let (changes, summary) = prepare_inventory_write(db, &current, authoring, held).await?;
    save_rows(db, changes, now_ms()).await?;
    Ok(summary)
}

/// One profile in Cairn's record format, as its editor shows it.
pub async fn profile_yaml(db: &LocalDb, id: &str) -> Result<String, String> {
    let inventory = load_inventory(db).await?;
    let profile = inventory
        .profiles
        .get(&InferenceProfileId::from(id))
        .ok_or_else(|| format!("there is no profile {id}"))?;
    serde_yaml::to_string(profile)
        .map_err(|error| format!("profile {id} does not serialize: {error}"))
}

/// Write one profile from its record YAML, in place of `replacing` (its id
/// before the edit, so an edit may rename it), or as a new profile when
/// `replacing` is `None`, which refuses an id already in use. It is validated
/// with the whole inventory, as every write is.
pub async fn save_profile(
    db: &LocalDb,
    replacing: Option<&str>,
    yaml: &str,
) -> Result<String, WriteError> {
    let profile: InferenceProfile = serde_yaml::from_str(yaml)
        .map_err(|error| WriteError::Refused(vec![Refusal::Unparsed(error.to_string())]))?;
    let replacing = replacing.map(InferenceProfileId::from);
    write_checked(db, move |current| {
        let id = profile.id.clone();
        if replacing.as_ref() != Some(&id) && current.profiles.contains_key(&id) {
            return Err(vec![Refusal::Taken(id)]);
        }
        let mut records = Inventory::default();
        records.insert_profile(profile);
        Ok(Authoring::Patch {
            upsert: Some(records.to_yaml()),
            remove: Removals {
                // An edit under a new id renames it.
                profiles: replacing.into_iter().filter(|old| *old != id).collect(),
                ..Removals::default()
            },
        })
    })
    .await
}

/// Remove a profile and every deployment of it, refused while one of those
/// deployments runs.
pub async fn delete_profile(db: &LocalDb, id: &str) -> Result<String, WriteError> {
    let id = InferenceProfileId::from(id);
    write_checked(db, move |current| {
        let deployments = current
            .deployments
            .values()
            .filter(|deployment| deployment.profile == id)
            .map(|deployment| deployment.id.clone())
            .collect();
        Ok(Authoring::Patch {
            upsert: None,
            remove: Removals {
                profiles: vec![id],
                deployments,
                ..Removals::default()
            },
        })
    })
    .await
}

/// Everything the `cairn://inference` pages render.
#[derive(Debug, Clone)]
pub struct InferenceView {
    pub inventory: Inventory,
    /// Whether settings.yaml still names the retired bginf link.
    pub retired_link_setting: bool,
    /// Enrolled executors by name: `true` when attached now.
    pub executors: BTreeMap<String, bool>,
    /// How the attached executors are reached now, which a machine record
    /// overrides.
    pub network: cairn_inference::Network,
    /// What Cairn has done to the deployments it acts on.
    pub deployment_control: super::inference_control::ControlView,
    /// What placement holds and decides: the detected pools with their
    /// residents, the requests and why each waits.
    pub models: crate::fleet::models::ModelsView,
    /// Every verification, newest first.
    pub verifications: Vec<super::inference_verify::VerificationRow>,
    pub now_ms: i64,
}

pub async fn load_view(orch: &Orchestrator) -> Result<InferenceView, String> {
    let captured_at = crate::fleet::unix_time_ms();
    let mut executors: BTreeMap<String, bool> = orch
        .fleet
        .unattached_enrolled_remotes()
        .into_iter()
        .map(|remote| (remote.name, false))
        .collect();
    for executor in orch.fleet.inspect_executors(captured_at) {
        executors.insert(executor.name, true);
    }
    let inventory = load_inventory(&orch.db.local).await?;
    Ok(InferenceView {
        inventory,
        retired_link_setting: crate::config::settings::load_inference(&orch.config_dir)
            .retired_bginf_link,
        executors,
        network: orch.fleet.network(),
        deployment_control: super::inference_control::load_control_view(orch).await?,
        models: crate::fleet::models::view(orch).await?,
        verifications: super::inference_verify::load_verifications(&orch.db.local).await?,
        now_ms: now_ms(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Sparks and halo as bginf mirrored them, written as the retired
    /// poller wrote them.
    const MIRRORED: &str = r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
  - {name: Qwen/Qwen3-Embedding-4B-GGUF (Q8_0)}
profiles:
  - id: dsv4f-tp2
    source: bginf
    model: {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
    servedName: deepseek-v4-flash-vision-exp
    engine: {kind: command, start: ./start-deepseek.sh, stop: ./stop-deepseek.sh}
    requirements: {machines: 2}
  - id: qwen3-embed-4b
    source: bginf
    model: {name: Qwen/Qwen3-Embedding-4B-GGUF (Q8_0)}
    servedName: qwen3-embedding-4b
    engine: {kind: command, start: podman run bginf-embed, stop: podman rm -f bginf-embed}
    requirements: {machines: 1}
deployments:
  - {id: dsv4f-tp2, source: bginf, profile: dsv4f-tp2, machines: [bginf-sp1, bginf-sp2], head: bginf-sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, source: bginf, profile: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
machines:
  - {name: bginf-sp1, source: bginf, address: 10.20.20.40}
  - {name: fedora, source: bginf, address: 10.20.20.43}
"#;

    fn mirrored() -> Inventory {
        Inventory::from_yaml(MIRRORED).unwrap()
    }

    fn adopted() -> Inventory {
        let mut inventory = mirrored();
        inventory.adopt(RecordSource::Bginf);
        inventory
    }

    async fn seeded(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        save_rows(&db, diff_rows(&Inventory::default(), &mirrored()), 1)
            .await
            .unwrap();
        db
    }

    async fn sources(db: &LocalDb) -> Vec<String> {
        let mut sources = Vec::new();
        for table in [PROFILES, DEPLOYMENTS, MACHINES] {
            sources.extend(
                db.query_all(
                    format!("SELECT id || ' ' || source FROM {table} ORDER BY id"),
                    (),
                    |row| row.text(0),
                )
                .await
                .unwrap(),
            );
        }
        sources
    }

    #[tokio::test]
    async fn inventory_rows_roll_back_with_their_domain_transaction() {
        let db = crate::storage::migrated_test_db("inventory-domain-rollback").await;
        let held = INVENTORY.lock().await;
        let current = load_inventory(&db).await.unwrap();
        let (changes, _) = prepare_inventory_write(
            &db,
            &current,
            &Authoring::Replace(adopted().to_yaml()),
            &held,
        )
        .await
        .unwrap();
        assert!(!changes.is_empty());
        let result: crate::storage::DbResult<()> = db
            .write(move |conn| {
                let changes = changes.clone();
                Box::pin(async move {
                    save_rows_in(conn, changes, 1, None).await?;
                    Err(crate::storage::DbError::Row(
                        "domain binding refused".into(),
                    ))
                })
            })
            .await;
        assert!(result.is_err());
        assert_eq!(load_inventory(&db).await.unwrap(), current);
    }

    #[tokio::test]
    async fn a_stored_profile_that_still_names_aliases_loads_without_them() {
        let db = crate::storage::migrated_test_db("inference-aliases").await;
        db.execute(
            "INSERT INTO inference_profiles (id, source, body_json, created_at, updated_at)
             VALUES ('r', 'cairn', ?1, 0, 0)",
            params![r#"{"id":"r","source":"cairn","model":{"name":"org/Model"},"servedName":"m","engine":{"kind":"command","start":"./s"},"requirements":{"machines":1},"aliases":["big"]}"#],
        )
        .await
        .unwrap();
        let inventory = load_inventory(&db).await.unwrap();
        assert_eq!(
            inventory.profiles[&InferenceProfileId::from("r")]
                .model
                .name,
            "org/Model"
        );
        assert!(
            !serde_json::to_string(&inventory.profiles[&InferenceProfileId::from("r")])
                .unwrap()
                .contains("aliases")
        );
    }

    #[tokio::test]
    async fn adopting_re_sources_every_mirrored_record_once_keeping_ids() {
        let db = seeded("inference-adopt").await;
        assert_eq!(
            load_inventory(&db).await.unwrap().foreign_records().len(),
            6
        );

        let said = adopt(&db, RecordSource::Bginf).await.unwrap();
        assert!(
            said.starts_with("Adopted 6 bginf records as Cairn's own, keeping their ids:"),
            "{said}"
        );
        assert!(said.contains("- deployment qwen3-embed-4b"), "{said}");
        let stored = load_inventory(&db).await.unwrap();
        assert!(stored.foreign_records().is_empty());
        assert_eq!(stored, adopted(), "every record kept, but Cairn's");
        assert!(
            sources(&db).await.iter().all(|row| row.ends_with(" cairn")),
            "{:?}",
            sources(&db).await
        );

        assert_eq!(
            adopt(&db, RecordSource::Bginf).await.unwrap(),
            "Nothing to adopt: no record is a bginf record."
        );
        assert_eq!(load_inventory(&db).await.unwrap(), adopted());
    }

    #[tokio::test]
    async fn authoring_waits_for_the_adoption() {
        let db = seeded("inference-author-before-adopt").await;
        let refusal = write_inventory(&db, &Authoring::Replace(adopted().to_yaml()))
            .await
            .unwrap_err();
        assert!(
            refusal.contains("6 records are still mirrored from bginf and read-only"),
            "{refusal}"
        );
        assert!(refusal.contains("adopt them first"), "{refusal}");
        assert_eq!(load_inventory(&db).await.unwrap(), mirrored());
    }

    #[tokio::test]
    async fn the_yaml_read_replaces_back_unchanged_and_an_edit_lands() {
        let db = seeded("inference-replace").await;
        adopt(&db, RecordSource::Bginf).await.unwrap();
        let read = load_inventory(&db).await.unwrap().to_yaml();
        assert_eq!(
            write_inventory(&db, &Authoring::Replace(read.clone()))
                .await
                .unwrap(),
            "The inventory is unchanged."
        );

        let edited = read.replace("podman run bginf-embed", "podman run cairn-embed");
        let said = write_inventory(&db, &Authoring::Replace(edited.clone()))
            .await
            .unwrap();
        assert!(
            said.starts_with("Wrote the inventory:\n- updated profile qwen3-embed-4b: engine"),
            "{said}"
        );
        assert_eq!(load_inventory(&db).await.unwrap().to_yaml(), edited);
    }

    #[tokio::test]
    async fn a_refused_replace_names_every_problem_and_writes_nothing() {
        let db = seeded("inference-replace-refused").await;
        adopt(&db, RecordSource::Bginf).await.unwrap();
        let before = load_inventory(&db).await.unwrap();
        let broken = before
            .to_yaml()
            .replace("head: fedora", "head: bginf-sp1")
            .replace("address: 10.20.20.43", "address: ' '")
            .replacen("source: cairn", "source: bginf", 1);
        let refusal = write_inventory(&db, &Authoring::Replace(broken))
            .await
            .unwrap_err();
        assert!(
            refusal.starts_with("Refused; nothing was written:"),
            "{refusal}"
        );
        for problem in [
            "deployment qwen3-embed-4b has head bginf-sp1, which is not one of its machines",
            "machine fedora has an empty address",
            "model Qwen/Qwen3-Embedding-4B-GGUF (Q8_0) (bginf) is not Cairn's",
        ] {
            assert!(refusal.contains(problem), "{problem}: {refusal}");
        }
        let parse = write_inventory(&db, &Authoring::Replace("profiles: {".into()))
            .await
            .unwrap_err();
        assert!(parse.contains("the document does not parse"), "{parse}");
        assert_eq!(load_inventory(&db).await.unwrap(), before);
    }

    #[tokio::test]
    async fn a_patch_adds_replaces_and_removes_records() {
        let db = seeded("inference-patch").await;
        adopt(&db, RecordSource::Bginf).await.unwrap();
        let upsert = r#"
models:
  - {name: nvidia/Qwen3.6-35B-A3B-NVFP4}
profiles:
  - {id: qwen36-fast, model: {name: nvidia/Qwen3.6-35B-A3B-NVFP4}, servedName: qwen3.6-35b, engine: {kind: command, start: ./serve-qwen36.sh}, requirements: {machines: 1}}
deployments:
  - {id: qwen36-fast, profile: qwen36-fast, machines: [bginf-sp1], head: bginf-sp1, apiPort: 8001, apiPath: /v1, healthPath: /v1/models}
machines:
  - {name: bginf-sp2, address: 10.20.20.41}
"#;
        let said = write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some(upsert.into()),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
        for line in [
            "added model nvidia/Qwen3.6-35B-A3B-NVFP4",
            "added profile qwen36-fast",
            "added deployment qwen36-fast",
            "added machine bginf-sp2",
        ] {
            assert!(said.contains(line), "{line}: {said}");
        }
        let stored = load_inventory(&db).await.unwrap();
        assert_eq!(stored.machines[&"bginf-sp2".into()].address, "10.20.20.41");

        let removal = Removals {
            profiles: vec!["qwen36-fast".into()],
            ..Removals::default()
        };
        let refusal = write_inventory(
            &db,
            &Authoring::Patch {
                upsert: None,
                remove: removal,
            },
        )
        .await
        .unwrap_err();
        assert!(
            refusal.contains("deployment qwen36-fast names unknown profile qwen36-fast"),
            "{refusal}"
        );

        let said = write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some("machines:\n  - {name: bginf-sp2, address: 10.20.20.42}\n".into()),
                remove: Removals {
                    profiles: vec!["qwen36-fast".into()],
                    deployments: vec!["qwen36-fast".into(), "nope".into()],
                    ..Removals::default()
                },
            },
        )
        .await
        .unwrap_err();
        assert!(
            said.contains("there is no deployment nope to remove"),
            "{said}"
        );
    }

    #[test]
    fn a_running_deployment_is_not_taken_away() {
        let current = adopted();
        let healthy = Phase::Healthy {
            since_ms: 0,
            load_ms: None,
        };
        let phases = BTreeMap::from([
            (DeploymentId::from("qwen3-embed-4b"), healthy),
            (
                DeploymentId::from("dsv4f-tp2"),
                Phase::Stopped { since_ms: 0 },
            ),
        ]);

        let mut without = current.clone();
        without
            .deployments
            .remove(&DeploymentId::from("qwen3-embed-4b"));
        without.deployments.remove(&DeploymentId::from("dsv4f-tp2"));
        assert_eq!(
            running_refusals(&current, &without, &phases),
            vec![
                "deployment qwen3-embed-4b is healthy on fedora; stop it first (cairn://inference/deployments/qwen3-embed-4b), or its machines would be released with the model still on them",
            ],
            "a stopped deployment goes freely"
        );

        let mut moved = current.clone();
        let embed = moved
            .deployments
            .get_mut(&DeploymentId::from("qwen3-embed-4b"))
            .unwrap();
        embed.machines = vec!["bginf-sp2".into()];
        embed.head = "bginf-sp2".into();
        assert_eq!(
            running_refusals(&current, &moved, &phases),
            vec!["deployment qwen3-embed-4b is healthy; stop it (cairn://inference/deployments/qwen3-embed-4b) before moving it to other machines"]
        );

        let mut restarted = current.clone();
        restarted
            .profiles
            .get_mut(&InferenceProfileId::from("qwen3-embed-4b"))
            .unwrap()
            .notes = Some("a new start command takes effect at the next start".into());
        assert!(running_refusals(&current, &restarted, &phases).is_empty());
    }

    fn refusals(result: Result<String, WriteError>) -> Vec<Refusal> {
        match result {
            Err(WriteError::Refused(refusals)) => refusals,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_profile_saves_from_its_yaml_and_a_new_id_duplicates_or_renames_it() {
        let db = seeded("inference-save-profile").await;
        adopt(&db, RecordSource::Bginf).await.unwrap();
        let yaml = profile_yaml(&db, "qwen3-embed-4b").await.unwrap();
        assert!(yaml.starts_with("id: qwen3-embed-4b\n"), "{yaml}");

        let edited = yaml.replace("podman run bginf-embed", "podman run cairn-embed");
        let said = save_profile(&db, Some("qwen3-embed-4b"), &edited)
            .await
            .unwrap();
        assert!(
            said.contains("updated profile qwen3-embed-4b: engine"),
            "{said}"
        );
        assert_eq!(profile_yaml(&db, "qwen3-embed-4b").await.unwrap(), edited);

        // A new profile may not take an id in use: duplicating needs a new one.
        assert_eq!(
            refusals(save_profile(&db, None, &edited).await),
            vec![Refusal::Taken("qwen3-embed-4b".into())]
        );
        let copy = edited.replace("id: qwen3-embed-4b", "id: embed-copy");
        let said = save_profile(&db, None, &copy).await.unwrap();
        assert!(said.contains("added profile embed-copy"), "{said}");

        let renamed = copy.replace("id: embed-copy", "id: embed-renamed");
        let said = save_profile(&db, Some("embed-copy"), &renamed)
            .await
            .unwrap();
        assert!(said.contains("added profile embed-renamed"), "{said}");
        assert!(said.contains("removed profile embed-copy"), "{said}");
        let profiles: Vec<String> = load_inventory(&db)
            .await
            .unwrap()
            .profiles
            .into_keys()
            .map(|id| id.to_string())
            .collect();
        assert_eq!(profiles, ["dsv4f-tp2", "embed-renamed", "qwen3-embed-4b"]);

        // Renaming one a deployment names would leave the deployment dangling.
        let refused = refusals(
            save_profile(
                &db,
                Some("qwen3-embed-4b"),
                &edited.replace("id: qwen3-embed-4b", "id: embed-elsewhere"),
            )
            .await,
        );
        assert_eq!(
            refused,
            vec![Refusal::Invalid(ValidationError::UnknownProfile {
                deployment: "qwen3-embed-4b".into(),
                profile: "qwen3-embed-4b".into(),
            })]
        );
    }

    #[tokio::test]
    async fn a_refused_profile_names_each_problem_and_writes_nothing() {
        let db = seeded("inference-save-profile-refused").await;
        adopt(&db, RecordSource::Bginf).await.unwrap();
        let before = load_inventory(&db).await.unwrap();
        assert!(matches!(
            refusals(save_profile(&db, Some("dsv4f-tp2"), "id: [").await).as_slice(),
            [Refusal::Unparsed(_)]
        ));
        let container = r#"
id: dsv4-container
model:
  name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp
  weights:
  - {repo: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp, revision: main, path: model.safetensors, sha256: ''}
servedName: dsv4
engine: {kind: container, image: 'vllm/vllm-openai:latest', server: vllm}
requirements: {machines: 1}
"#;
        assert_eq!(
            refusals(save_profile(&db, None, container).await),
            vec![
                Refusal::Invalid(ValidationError::BadWeightFile {
                    profile: "dsv4-container".into(),
                    index: 0,
                    problem: "sha256 is not 64 lowercase hex digits",
                }),
                Refusal::Invalid(ValidationError::BadImageDigest {
                    profile: "dsv4-container".into(),
                    image: "vllm/vllm-openai:latest".into(),
                }),
            ]
        );
        assert_eq!(load_inventory(&db).await.unwrap(), before);
    }

    async fn set_phase(db: &LocalDb, deployment: &str, phase: &Phase) {
        db.execute(
            "INSERT INTO inference_deployment_state (deployment_id, phase_json, updated_at)
             VALUES (?1, ?2, 0)
             ON CONFLICT(deployment_id) DO UPDATE SET phase_json = excluded.phase_json",
            params![deployment, serde_json::to_string(phase).unwrap()],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_profile_is_deleted_with_its_deployments_but_not_while_one_runs() {
        let db = seeded("inference-delete-profile").await;
        adopt(&db, RecordSource::Bginf).await.unwrap();
        set_phase(
            &db,
            "qwen3-embed-4b",
            &Phase::Healthy {
                since_ms: 0,
                load_ms: None,
            },
        )
        .await;
        let refused = refusals(delete_profile(&db, "qwen3-embed-4b").await);
        assert_eq!(refused.len(), 1);
        assert!(
            refused[0]
                .to_string()
                .starts_with("deployment qwen3-embed-4b is healthy on fedora; stop it first"),
            "{refused:?}"
        );
        assert_eq!(load_inventory(&db).await.unwrap(), adopted());

        set_phase(&db, "qwen3-embed-4b", &Phase::Stopped { since_ms: 1 }).await;
        let said = delete_profile(&db, "qwen3-embed-4b").await.unwrap();
        assert!(said.contains("removed profile qwen3-embed-4b"), "{said}");
        assert!(said.contains("removed deployment qwen3-embed-4b"), "{said}");
        let stored = load_inventory(&db).await.unwrap();
        assert!(!stored
            .profiles
            .contains_key(&InferenceProfileId::from("qwen3-embed-4b")));
        assert!(!stored
            .deployments
            .contains_key(&DeploymentId::from("qwen3-embed-4b")));

        assert_eq!(
            refusals(delete_profile(&db, "qwen3-embed-4b").await),
            vec![Refusal::Said(
                "there is no profile qwen3-embed-4b to remove".into()
            )]
        );
    }

    #[test]
    fn the_validation_error_for_a_duplicate_reads_plainly() {
        assert_eq!(
            ValidationError::DuplicateId {
                kind: "machine",
                id: "fedora".into()
            }
            .to_string(),
            "machine fedora appears twice"
        );
    }
}
