//! The cairn-inference facet's host: the inventory in the orch's database, and
//! the writes that change it.
//!
//! Every record is Cairn's own and the operator writes it: the whole inventory
//! at once ([`Authoring::Replace`], a document in Cairn's record format, the
//! inverse of `cairn://inference?format=yaml`), or records added, replaced and
//! removed within it ([`Authoring::Patch`]). Either is validated as a whole and
//! refused naming every problem, and neither may take away what is running: a
//! deployment the act side holds machines for is stopped before it is removed
//! or moved, and a pool the controller drives is handed back before it is
//! removed. Records mirrored from bginf before it retired are read-only until
//! [`adopt`] makes them Cairn's, keeping their ids; authoring is refused until
//! then, so nothing half-adopted is ever written.

use std::collections::BTreeMap;

use cairn_db::turso::params;
use cairn_inference::control::Phase;
use cairn_inference::pool_control::PoolControl;
use cairn_inference::{DeploymentId, Inventory, PoolId, RecipeId, RecordSource, YamlError};
use serde::Serialize;

use super::Orchestrator;
use crate::storage::{LocalDb, RowExt};

const RECIPES: &str = "inference_recipes";
const DEPLOYMENTS: &str = "inference_deployments";
const POOLS: &str = "inference_pools";

/// Every read-modify-write of the inventory holds this, so two writes never
/// interleave.
static INVENTORY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now_ms() -> i64 {
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
    for recipe in parse_bodies(RECIPES, load_bodies(db, RECIPES).await?) {
        inventory.insert_recipe(recipe);
    }
    for deployment in parse_bodies(DEPLOYMENTS, load_bodies(db, DEPLOYMENTS).await?) {
        inventory.insert_deployment(deployment);
    }
    for pool in parse_bodies(POOLS, load_bodies(db, POOLS).await?) {
        inventory.insert_pool(pool);
    }
    Ok(inventory)
}

/// One row to write: `body` `None` deletes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowChange {
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

fn diff_rows(current: &Inventory, next: &Inventory) -> Vec<RowChange> {
    let mut changes = Vec::new();
    diff_kind(
        RECIPES,
        &current.recipes,
        &next.recipes,
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
        POOLS,
        &current.pools,
        &next.pools,
        |p| p.source,
        &mut changes,
    );
    changes
}

/// Write every changed row in one transaction.
async fn save_rows(db: &LocalDb, changes: Vec<RowChange>, at: i64) -> Result<(), String> {
    db.write(move |conn| {
        let changes = changes.clone();
        Box::pin(async move {
            for change in changes {
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
        })
    })
    .await
    .map_err(|error| format!("write inference records: {error}"))
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
    pub recipes: Vec<RecipeId>,
    pub deployments: Vec<DeploymentId>,
    pub pools: Vec<PoolId>,
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

fn document_problems(error: YamlError) -> Vec<String> {
    match error {
        YamlError::Parse(error) => vec![format!("the document does not parse: {error}")],
        YamlError::Invalid(problems) => problems.iter().map(ToString::to_string).collect(),
    }
}

fn remove_each<K: Ord + std::fmt::Display, T>(
    kind: &str,
    records: &mut BTreeMap<K, T>,
    ids: &[K],
    problems: &mut Vec<String>,
) {
    for id in ids {
        if records.remove(id).is_none() {
            problems.push(format!("there is no {kind} {id} to remove"));
        }
    }
}

/// The inventory `authoring` leaves, or every reason it is refused.
fn authored(current: &Inventory, authoring: &Authoring) -> Result<Inventory, Vec<String>> {
    let foreign = current.foreign_records();
    if !foreign.is_empty() {
        return Err(vec![format!(
            "{} records are still mirrored from bginf and read-only ({}); adopt them first: patch cairn://inference {{adopt:\"bginf\"}}",
            foreign.len(),
            foreign.join(", ")
        )]);
    }
    let mut problems = Vec::new();
    let next = match authoring {
        Authoring::Replace(text) => match Inventory::from_yaml(text) {
            Ok(next) => next,
            Err(YamlError::Invalid(found)) => {
                // Still name any record that is not Cairn's, below.
                problems.extend(found.iter().map(ToString::to_string));
                Inventory::parse_yaml(text).unwrap_or_default()
            }
            Err(error) => return Err(document_problems(error)),
        },
        Authoring::Patch { upsert, remove } => {
            let mut next = current.clone();
            if let Some(text) = upsert {
                next.upsert(Inventory::parse_yaml(text).map_err(document_problems)?);
            }
            remove_each("recipe", &mut next.recipes, &remove.recipes, &mut problems);
            remove_each(
                "deployment",
                &mut next.deployments,
                &remove.deployments,
                &mut problems,
            );
            remove_each("pool", &mut next.pools, &remove.pools, &mut problems);
            problems.extend(next.problems().iter().map(ToString::to_string));
            next
        }
    };
    problems.extend(next.foreign_records().into_iter().map(|record| {
        format!("{record} is not Cairn's; a record written here says `source: cairn` or names no source")
    }));
    if problems.is_empty() {
        Ok(next)
    } else {
        Err(problems)
    }
}

/// What `next` would take away that is running: a deployment the act side
/// holds machines for, removed or moved to other machines, and a pool the
/// controller drives, removed.
fn running_refusals(
    current: &Inventory,
    next: &Inventory,
    phases: &BTreeMap<DeploymentId, Phase>,
    controls: &BTreeMap<PoolId, PoolControl>,
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
    for (pool, _) in controls.iter().filter(|(_, control)| control.controlled) {
        if current.pools.contains_key(pool) && !next.pools.contains_key(pool) {
            refusals.push(format!(
                "pool {pool} is under Cairn's control; hand it back first (patch cairn://inference/pools {{pool:\"{pool}\", controlled:false}})"
            ));
        }
    }
    refusals
}

fn refused(problems: Vec<String>) -> String {
    format!("Refused; nothing was written:\n- {}", problems.join("\n- "))
}

/// Write Cairn's own records, or refuse with every problem and write nothing.
pub async fn write_inventory(db: &LocalDb, authoring: &Authoring) -> Result<String, String> {
    let _held = INVENTORY.lock().await;
    let current = load_inventory(db).await?;
    let next = authored(&current, authoring).map_err(refused)?;
    let phases = super::pool_control::deployment_phases(db).await?;
    let controls = super::pool_control::load_controls(db).await?;
    let running = running_refusals(&current, &next, &phases, &controls);
    if !running.is_empty() {
        return Err(refused(running));
    }
    let report = Inventory::changes(&current, &next);
    if !report.has_changes() {
        return Ok("The inventory is unchanged.".to_string());
    }
    save_rows(db, diff_rows(&current, &next), now_ms()).await?;
    Ok(format!(
        "Wrote the inventory:\n- {}\nRead cairn://inference?format=yaml for the whole of it.",
        report.lines().join("\n- ")
    ))
}

/// Everything the `cairn://inference` pages render.
#[derive(Debug, Clone)]
pub struct InferenceView {
    pub inventory: Inventory,
    /// Whether settings.yaml still names the retired bginf link.
    pub retired_link_setting: bool,
    /// Enrolled executors by name: `true` when attached now.
    pub executors: BTreeMap<String, bool>,
    /// What Cairn has done to the deployments it acts on.
    pub deployment_control: super::inference_control::ControlView,
    /// The pool controller's records and plans.
    pub control: super::pool_control::ControlView,
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
    let control = super::pool_control::load_control_view(orch, &inventory).await?;
    Ok(InferenceView {
        inventory,
        retired_link_setting: crate::config::settings::load_inference(&orch.config_dir)
            .retired_bginf_link,
        executors,
        deployment_control: super::inference_control::load_control_view(orch).await?,
        control,
        now_ms: now_ms(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_inference::ValidationError;

    /// The Sparks and halo as bginf mirrored them, written as the retired
    /// poller wrote them.
    const MIRRORED: &str = r#"
recipes:
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
  - {id: dsv4f-tp2, source: bginf, recipe: dsv4f-tp2, machines: [bginf-sp1, bginf-sp2], head: bginf-sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen3-embed-4b, source: bginf, recipe: qwen3-embed-4b, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
pools:
  - {id: sparks, source: bginf, machines: [{name: bginf-sp1, address: 10.20.20.40}, {name: bginf-sp2}], default: [dsv4f-tp2], pin: []}
  - {id: halo, source: bginf, machines: [{name: fedora, address: 10.20.20.43}], default: [qwen3-embed-4b]}
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
        for table in [RECIPES, DEPLOYMENTS, POOLS] {
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
            said.starts_with("Wrote the inventory:\n- updated recipe qwen3-embed-4b: engine"),
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
            .replace("- qwen3-embed-4b\n", "- nope\n")
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
            "pool halo default names unknown deployment nope",
            "recipe dsv4f-tp2 (bginf) is not Cairn's",
        ] {
            assert!(refusal.contains(problem), "{problem}: {refusal}");
        }
        let parse = write_inventory(&db, &Authoring::Replace("recipes: {".into()))
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
recipes:
  - {id: qwen36-fast, model: {name: nvidia/Qwen3.6-35B-A3B-NVFP4}, servedName: qwen3.6-35b, engine: {kind: command, start: ./serve-qwen36.sh}, requirements: {machines: 1}}
deployments:
  - {id: qwen36-fast, recipe: qwen36-fast, machines: [bginf-sp1], head: bginf-sp1, apiPort: 8001, apiPath: /v1, healthPath: /v1/models}
pools:
  - {id: sparks, machines: [{name: bginf-sp1, address: 10.20.20.40}, {name: bginf-sp2, address: 10.20.20.41}], default: [qwen36-fast]}
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
            "added recipe qwen36-fast",
            "added deployment qwen36-fast",
            "updated pool sparks: default, machines, pin",
        ] {
            assert!(said.contains(line), "{line}: {said}");
        }
        let stored = load_inventory(&db).await.unwrap();
        assert_eq!(stored.address_of(&"bginf-sp2".into()), Some("10.20.20.41"));

        let removal = Removals {
            recipes: vec!["qwen36-fast".into()],
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
            refusal.contains("deployment qwen36-fast names unknown recipe qwen36-fast"),
            "{refusal}"
        );

        let said = write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some("pools:\n  - {id: sparks, machines: [{name: bginf-sp1, address: 10.20.20.40}, {name: bginf-sp2, address: 10.20.20.41}], default: [dsv4f-tp2]}\n".into()),
                remove: Removals {
                    recipes: vec!["qwen36-fast".into()],
                    deployments: vec!["qwen36-fast".into(), "nope".into()],
                    pools: Vec::new(),
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
    fn a_running_deployment_or_a_controlled_pool_is_not_taken_away() {
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
        let halo = &current.pools[&PoolId::from("halo")];
        let controls = BTreeMap::from([(
            halo.id.clone(),
            PoolControl::begin(halo, &current.deployments, &phases, 0),
        )]);

        let mut without_halo = current.clone();
        without_halo.pools.remove(&PoolId::from("halo"));
        without_halo
            .deployments
            .remove(&DeploymentId::from("qwen3-embed-4b"));
        without_halo
            .deployments
            .remove(&DeploymentId::from("dsv4f-tp2"));
        assert_eq!(
            running_refusals(&current, &without_halo, &phases, &controls),
            vec![
                "deployment qwen3-embed-4b is healthy on fedora; stop it first (cairn://inference/deployments/qwen3-embed-4b), or its machines would be released with the model still on them",
                "pool halo is under Cairn's control; hand it back first (patch cairn://inference/pools {pool:\"halo\", controlled:false})",
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
            running_refusals(&current, &moved, &phases, &controls),
            vec!["deployment qwen3-embed-4b is healthy; stop it (cairn://inference/deployments/qwen3-embed-4b) before moving it to other machines"]
        );

        let mut restarted = current.clone();
        restarted
            .recipes
            .get_mut(&RecipeId::from("qwen3-embed-4b"))
            .unwrap()
            .notes = Some("a new start command takes effect at the next start".into());
        assert!(running_refusals(&current, &restarted, &phases, &controls).is_empty());
    }

    #[test]
    fn the_validation_error_for_a_duplicate_reads_plainly() {
        assert_eq!(
            ValidationError::DuplicateId {
                kind: "pool",
                id: "halo".into()
            }
            .to_string(),
            "pool halo appears twice"
        );
    }
}
