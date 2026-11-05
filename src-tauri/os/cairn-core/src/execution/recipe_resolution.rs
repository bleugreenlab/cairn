//! Which recipe an execution create starts, and why.
//!
//! One rule: if no recipe is named, Cairn chooses. A named recipe is honoured
//! exactly, never substituted. An unnamed one is answered by the recipe
//! chooser installed in this seam, if one decides, else by the workspace
//! setting `defaultRecipe`, else the start is refused naming that setting.
//!
//! Nothing is installed in the seam. The judgment thread installs a chooser
//! only once it has measured one on the records every resolution leaves
//! (`recipe_resolutions`, tenets 1222 and 1243); until then `defaultRecipe` is
//! the only answerer, and the reply says so. A chooser can also be installed
//! in shadow, where its pick is recorded beside what ran without deciding
//! anything, so it can be measured before it is trusted.
//!
//! Issue create, an executions append and the route dispatcher all resolve
//! here, so the reply and the record never differ between them.

use std::sync::Arc;

use cairn_db::turso::params;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

/// A recipe the creator could have started, as its author described it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecipeCandidate {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

/// What a chooser reads about the issue the execution is for.
#[derive(Debug, Clone, Default)]
pub struct IssueFacts {
    pub title: String,
    pub description: Option<String>,
    pub labels: Vec<String>,
    /// The parent's canonical URI, issue or thread.
    pub parent: Option<String>,
}

pub struct ChooserInput<'a> {
    pub issue: &'a IssueFacts,
    /// The recipes this creator may start here, with their descriptions
    /// (tenet 1328: users' own options as described, nothing keyed to a
    /// built-in name).
    pub candidates: &'a [RecipeCandidate],
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChooserVerdict {
    Pick {
        recipe: String,
        reason: String,
        confidence: Option<f64>,
        /// The answerer: a rule's name, or `decide:<model>@<question-version>`.
        measured_by: String,
    },
    Abstain {
        measured_by: String,
        reason: String,
    },
}

/// Chooses a recipe for an execution whose creator named none.
///
/// A chooser answered by a model reads the issue's own text, so every endpoint
/// serving it must be zero-data-retention (tenet 1329). Its confidence is
/// `decide`'s published formula (cairn/5056), never a model's self-report.
#[async_trait::async_trait]
pub trait RecipeChooser: Send + Sync {
    async fn choose(&self, input: ChooserInput<'_>) -> ChooserVerdict;
}

/// A chooser in the seam, and whether its pick decides or is only recorded.
#[derive(Clone)]
pub struct InstalledChooser {
    pub chooser: Arc<dyn RecipeChooser>,
    pub decides: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipeSource {
    Named,
    Chosen,
    WorkspaceDefault,
    DefaultAfterAbstain,
}

impl RecipeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Named => "named",
            Self::Chosen => "chosen",
            Self::WorkspaceDefault => "workspace_default",
            Self::DefaultAfterAbstain => "default_after_abstain",
        }
    }
}

/// What the installed chooser said, whether or not it decided. `recipe` is
/// `None` for an abstain, and for a pick outside the candidate set.
#[derive(Debug, Clone, PartialEq)]
pub struct ChooserOpinion {
    pub recipe: Option<String>,
    pub reason: String,
    pub confidence: Option<f64>,
    pub measured_by: String,
}

const NAMED_BY_CREATOR: &str = "named by creator";
const DEFAULT_SETTING: &str = "defaultRecipe setting";

#[derive(Debug, Clone, PartialEq)]
pub struct RecipeResolution {
    pub recipe: String,
    pub source: RecipeSource,
    pub reason: String,
    pub measured_by: String,
    pub confidence: Option<f64>,
    pub abstain_reason: Option<String>,
    /// `None` while no chooser is installed.
    pub chooser: Option<ChooserOpinion>,
    /// Sorted by id.
    pub candidates: Vec<RecipeCandidate>,
    pub candidates_hash: String,
}

impl RecipeResolution {
    /// The reply's first line: the recipe, and what decided it.
    pub fn line(&self) -> String {
        let why = match self.source {
            RecipeSource::Named => "named".to_string(),
            RecipeSource::Chosen => {
                let confidence = self
                    .confidence
                    .map(|confidence| format!(", {confidence:.2}"))
                    .unwrap_or_default();
                format!("{} ({}{confidence})", self.reason, self.measured_by)
            }
            RecipeSource::WorkspaceDefault => format!("workspace default ({})", self.measured_by),
            RecipeSource::DefaultAfterAbstain => format!(
                "workspace default, chooser abstained: {}",
                self.abstain_reason.as_deref().unwrap_or("no reason given")
            ),
        };
        format!("recipe: {} · {why}", self.recipe)
    }

    fn candidates_json(&self) -> String {
        serde_json::to_string(&self.candidates).unwrap_or_else(|_| "[]".to_string())
    }
}

/// The sha256 of the candidates as stored, so the hash addresses exactly the
/// option set a resolution chose among.
pub fn candidates_hash(candidates: &[RecipeCandidate]) -> String {
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));
    let json = serde_json::to_string(&sorted).unwrap_or_default();
    format!("{:x}", Sha256::digest(json.as_bytes()))
}

fn ids(candidates: &[RecipeCandidate]) -> String {
    if candidates.is_empty() {
        "none".to_string()
    } else {
        candidates
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn opinion_of(verdict: ChooserVerdict, candidates: &[RecipeCandidate]) -> ChooserOpinion {
    match verdict {
        ChooserVerdict::Pick {
            recipe,
            reason,
            confidence,
            measured_by,
        } if candidates.iter().any(|candidate| candidate.id == recipe) => ChooserOpinion {
            recipe: Some(recipe),
            reason,
            confidence,
            measured_by,
        },
        ChooserVerdict::Pick {
            recipe,
            confidence,
            measured_by,
            ..
        } => {
            log::warn!(
                "recipe chooser {measured_by} picked '{recipe}', which is not a candidate; read as an abstain"
            );
            ChooserOpinion {
                recipe: None,
                reason: format!("picked '{recipe}', which is not a recipe this creator may start"),
                confidence,
                measured_by,
            }
        }
        ChooserVerdict::Abstain {
            measured_by,
            reason,
        } => ChooserOpinion {
            recipe: None,
            reason,
            confidence: None,
            measured_by,
        },
    }
}

/// The decision itself, apart from where its inputs come from.
///
/// `named` is the creator's recipe already resolved to its file's id;
/// `verdict` is the installed chooser's answer and whether it decides;
/// `default_loads` turns the `defaultRecipe` id into the id of a recipe that
/// starts in this project, or says why it does not.
fn settle(
    named: Option<String>,
    mut candidates: Vec<RecipeCandidate>,
    verdict: Option<(ChooserVerdict, bool)>,
    default: Option<String>,
    default_loads: impl FnOnce(&str) -> Result<String, String>,
) -> Result<RecipeResolution, String> {
    candidates.sort_by(|a, b| a.id.cmp(&b.id));
    let decides = verdict.as_ref().is_some_and(|(_, decides)| *decides);
    let chooser = verdict.map(|(verdict, _)| opinion_of(verdict, &candidates));
    let candidates_hash = candidates_hash(&candidates);
    let resolution = |recipe: String, source, reason: String, measured_by: String| {
        let candidates = candidates.clone();
        RecipeResolution {
            recipe,
            source,
            reason,
            measured_by,
            confidence: None,
            abstain_reason: None,
            chooser: chooser.clone(),
            candidates,
            candidates_hash: candidates_hash.clone(),
        }
    };

    if let Some(recipe) = named {
        return Ok(resolution(
            recipe,
            RecipeSource::Named,
            "named".to_string(),
            NAMED_BY_CREATOR.to_string(),
        ));
    }
    if let Some(opinion) = chooser.as_ref().filter(|_| decides) {
        if let Some(recipe) = &opinion.recipe {
            let mut chosen = resolution(
                recipe.clone(),
                RecipeSource::Chosen,
                opinion.reason.clone(),
                opinion.measured_by.clone(),
            );
            chosen.confidence = opinion.confidence;
            return Ok(chosen);
        }
    }
    let abstained = chooser
        .as_ref()
        .filter(|opinion| decides && opinion.recipe.is_none())
        .map(|opinion| opinion.reason.clone());
    let Some(default) = default else {
        let after = abstained
            .as_deref()
            .map(|reason| format!(" (the recipe chooser abstained: {reason})"))
            .unwrap_or_default();
        return Err(format!(
            "No recipe named and no default to choose with{after}: set the workspace setting \
             defaultRecipe (a recipe id) or name a recipe. Recipes here: {}",
            ids(&candidates)
        ));
    };
    let recipe = default_loads(&default).map_err(|why| {
        format!(
            "The workspace setting defaultRecipe names '{default}', which cannot start here: \
             {why}. Set defaultRecipe to a recipe that starts in this project, or name a \
             recipe. Recipes here: {}",
            ids(&candidates)
        )
    })?;
    let (source, reason) = match &abstained {
        Some(_) => (
            RecipeSource::DefaultAfterAbstain,
            "workspace default after the chooser abstained",
        ),
        None => (RecipeSource::WorkspaceDefault, "workspace default"),
    };
    let mut defaulted = resolution(
        recipe,
        source,
        reason.to_string(),
        DEFAULT_SETTING.to_string(),
    );
    defaulted.abstain_reason = abstained;
    Ok(defaulted)
}

/// Resolve the recipe an execution create for `issue` starts in `project_id`.
pub(crate) async fn resolve_execution_recipe(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    issue: &IssueFacts,
    named: Option<&str>,
) -> Result<RecipeResolution, String> {
    // The candidate set is recorded on every row, named ones too. A listing
    // that fails records an empty set rather than refusing a start that
    // needs no listing.
    let candidates: Vec<RecipeCandidate> = orch
        .list_picker_recipes(project_id)
        .unwrap_or_else(|error| {
            log::warn!("recipe candidates for project {project_id} could not be listed: {error}");
            Vec::new()
        })
        .into_iter()
        .map(|recipe| RecipeCandidate {
            id: recipe.id,
            name: recipe.name,
            description: recipe.description,
        })
        .collect();
    let named =
        crate::execution::recipe::resolve_filed_recipe_id(&orch.config_dir, db, project_id, named)
            .await?;
    let verdict = match orch.recipe_chooser() {
        Some(installed) => Some((
            installed
                .chooser
                .choose(ChooserInput {
                    issue,
                    candidates: &candidates,
                })
                .await,
            installed.decides,
        )),
        None => None,
    };
    let project_path = match named {
        Some(_) => None,
        None => crate::execution::recipe::project_repo_path(db, project_id)
            .await?
            .map(std::path::PathBuf::from),
    };
    let default = orch.get_settings().default_recipe;
    settle(named, candidates, verdict, default, |id| {
        let project_path = project_path
            .as_deref()
            .ok_or_else(|| format!("project {project_id} has no checkout"))?;
        crate::execution::recipe::get_selected_recipe(
            &orch.config_dir,
            project_path,
            project_id,
            id,
        )
        .map(|recipe| recipe.id)
    })
}

/// The issue facts a chooser reads, from the issue's row.
pub(crate) async fn issue_facts(db: &LocalDb, issue_id: &str) -> Result<IssueFacts, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            use crate::storage::RowExt;
            let mut rows = conn
                .query(
                    "SELECT i.title, i.description, i.parent_issue_id,
                            (SELECT p.key FROM threads t JOIN projects p ON p.id = t.project_id
                              WHERE t.id = i.parent_thread_id),
                            (SELECT t.name FROM threads t WHERE t.id = i.parent_thread_id)
                       FROM issues i WHERE i.id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Err(crate::storage::DbError::Row(format!(
                    "Issue {issue_id} not found"
                )));
            };
            let title = row.text(0)?;
            let description = row.opt_text(1)?.filter(|text| !text.is_empty());
            let parent_issue = row.opt_text(2)?;
            let thread = match (row.opt_text(3)?, row.opt_text(4)?) {
                (Some(key), Some(name)) => Some(cairn_common::uri::build_thread_uri(&key, &name)),
                _ => None,
            };
            drop(rows);
            let parent = match parent_issue {
                Some(id) => crate::issues::relations::issue_uri_for_id(conn, &id)
                    .await
                    .ok(),
                None => thread,
            };
            let mut labels = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT l.name FROM issue_labels il JOIN labels l ON l.id = il.label_id
                      WHERE il.issue_id = ?1 ORDER BY LOWER(l.name)",
                    params![issue_id.as_str()],
                )
                .await?;
            while let Some(row) = rows.next().await? {
                labels.push(row.text(0)?);
            }
            Ok(IssueFacts {
                title,
                description,
                labels,
                parent,
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Who filed an execution, for the record: a person naming a recipe is a
/// stronger label than a coordinator passing its default through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CreatorKind {
    Cli,
    Thread,
    Agent,
    RouteDispatcher,
    #[default]
    System,
}

impl CreatorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Thread => "thread",
            Self::Agent => "agent",
            Self::RouteDispatcher => "route_dispatcher",
            Self::System => "system",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolutionCreator {
    pub kind: CreatorKind,
    /// The filing node's or thread's canonical URI.
    pub uri: Option<String>,
}

impl ResolutionCreator {
    pub fn system() -> Self {
        Self {
            kind: CreatorKind::System,
            uri: None,
        }
    }

    pub fn route_dispatcher() -> Self {
        Self {
            kind: CreatorKind::RouteDispatcher,
            uri: None,
        }
    }

    /// The creator of a resource write: the calling thread or agent node, or
    /// the CLI when the write carries no run.
    pub(crate) async fn of_request(
        orch: &Orchestrator,
        request: &cairn_common::protocol::CallbackRequest,
    ) -> Self {
        if request.run_id.is_none() {
            return Self {
                kind: CreatorKind::Cli,
                uri: None,
            };
        }
        match crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request).await {
            Ok(home) => Self {
                kind: match cairn_common::uri::parse_uri(&home) {
                    Some(cairn_common::uri::CairnResource::Thread { .. }) => CreatorKind::Thread,
                    _ => CreatorKind::Agent,
                },
                uri: Some(home),
            },
            Err(_) => Self {
                kind: CreatorKind::Agent,
                uri: None,
            },
        }
    }
}

/// Everything a resolution row records besides the resolution itself.
pub(crate) struct ResolutionRecord<'a> {
    pub(crate) resolution: &'a RecipeResolution,
    pub(crate) project_id: &'a str,
    pub(crate) issue_id: &'a str,
    pub(crate) execution_id: &'a str,
    pub(crate) creator: &'a ResolutionCreator,
    pub(crate) overrides: Option<&'a serde_json::Value>,
    pub(crate) preview: &'a serde_json::Value,
    pub(crate) supersedes_execution_id: Option<&'a str>,
}

/// Write one resolution row, and its option set if this project has not
/// recorded that set before.
pub(crate) async fn record_resolution(
    db: &LocalDb,
    record: ResolutionRecord<'_>,
) -> Result<(), String> {
    let resolution = record.resolution;
    let chooser = resolution.chooser.clone();
    let values = (
        cairn_common::ids::mint_child(record.execution_id),
        record.project_id.to_string(),
        record.issue_id.to_string(),
        record.execution_id.to_string(),
        chrono::Utc::now().timestamp(),
        record.creator.uri.clone(),
        record.creator.kind.as_str(),
        resolution.source.as_str(),
        resolution.recipe.clone(),
        resolution.measured_by.clone(),
        resolution.reason.clone(),
        resolution.confidence,
        resolution.abstain_reason.clone(),
    );
    let chooser_values = (
        chooser.as_ref().and_then(|opinion| opinion.recipe.clone()),
        chooser.as_ref().and_then(|opinion| opinion.confidence),
        chooser.as_ref().map(|opinion| opinion.reason.clone()),
        chooser.as_ref().map(|opinion| opinion.measured_by.clone()),
    );
    let rest = (
        resolution.candidates_hash.clone(),
        resolution.candidates_json(),
        record.overrides.map(|value| value.to_string()),
        record.preview.to_string(),
        record.supersedes_execution_id.map(str::to_string),
    );
    db.write(move |conn| {
        let values = values.clone();
        let chooser_values = chooser_values.clone();
        let rest = rest.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO recipe_option_sets (project_id, candidates_hash, candidates, created_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (project_id, candidates_hash) DO NOTHING",
                params![
                    values.1.as_str(),
                    rest.0.as_str(),
                    rest.1.as_str(),
                    values.4
                ],
            )
            .await?;
            conn.execute(
                "INSERT INTO recipe_resolutions (
                    id, project_id, issue_id, execution_id, created_at, created_by,
                    created_by_kind, source, resolved_recipe, measured_by, reason, confidence,
                    abstain_reason, chooser_recipe, chooser_confidence, chooser_reason,
                    chooser_measured_by, candidates_hash, overrides, preview,
                    supersedes_execution_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                         ?17, ?18, ?19, ?20, ?21)",
                params![
                    values.0.as_str(),
                    values.1.as_str(),
                    values.2.as_str(),
                    values.3.as_str(),
                    values.4,
                    values.5.as_deref(),
                    values.6,
                    values.7,
                    values.8.as_str(),
                    values.9.as_str(),
                    values.10.as_str(),
                    values.11,
                    values.12.as_deref(),
                    chooser_values.0.as_deref(),
                    chooser_values.1,
                    chooser_values.2.as_deref(),
                    chooser_values.3.as_deref(),
                    rest.0.as_str(),
                    rest.2.as_deref(),
                    rest.3.as_str(),
                    rest.4.as_deref()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to record the recipe resolution: {error}"))
}

/// The execution read's `## resolution` block: the recipe line the create
/// reply showed, who filed it, and every live edit to an agent since. Empty
/// for an execution created before resolutions were recorded, or by the
/// composer.
pub(crate) async fn resolution_block(
    conn: &crate::storage::TrackedConnection,
    execution_id: &str,
) -> String {
    use crate::storage::RowExt;
    let line = stored_resolution_line(conn, execution_id).await;
    let mut edits = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "SELECT agent_id, changed, actor, before, after FROM execution_agent_edits
              WHERE execution_id = ?1 ORDER BY created_at, rowid",
            params![execution_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(agent), Ok(changed), Ok(before), Ok(after)) =
                (row.text(0), row.text(1), row.text(3), row.text(4))
            else {
                continue;
            };
            let changed: Vec<String> = serde_json::from_str(&changed).unwrap_or_default();
            let actor = row
                .opt_text(2)
                .ok()
                .flatten()
                .unwrap_or_else(|| "cli".into());
            edits.push(format!(
                "- {agent} edited ({}) by {actor}: {before} → {after}",
                changed.join(", ")
            ));
        }
    }
    if line.is_none() && edits.is_empty() {
        return String::new();
    }
    let mut block = String::from("\n## resolution\n");
    if let Some(line) = line {
        block.push_str(&line);
        block.push('\n');
    }
    if !edits.is_empty() {
        block.push_str("agent edits:\n");
        block.push_str(&edits.join("\n"));
        block.push('\n');
    }
    block
}

async fn stored_resolution_line(
    conn: &crate::storage::TrackedConnection,
    execution_id: &str,
) -> Option<String> {
    use crate::storage::RowExt;
    let mut rows = conn
        .query(
            "SELECT resolved_recipe, source, reason, measured_by, confidence, abstain_reason,
                    created_by_kind, created_by, supersedes_execution_id
               FROM recipe_resolutions WHERE execution_id = ?1",
            params![execution_id],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    let source = match row.text(1).ok()?.as_str() {
        "named" => RecipeSource::Named,
        "chosen" => RecipeSource::Chosen,
        "default_after_abstain" => RecipeSource::DefaultAfterAbstain,
        _ => RecipeSource::WorkspaceDefault,
    };
    let resolution = RecipeResolution {
        recipe: row.text(0).ok()?,
        source,
        reason: row.text(2).ok()?,
        measured_by: row.text(3).ok()?,
        confidence: row.opt_f64(4).ok().flatten(),
        abstain_reason: row.opt_text(5).ok().flatten(),
        chooser: None,
        candidates: Vec::new(),
        candidates_hash: String::new(),
    };
    let by = match row.opt_text(7).ok().flatten() {
        Some(uri) => format!("{} {uri}", row.text(6).ok()?),
        None => row.text(6).ok()?,
    };
    Some(format!("{} · filed by {by}", resolution.line()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, description: &str) -> RecipeCandidate {
        RecipeCandidate {
            id: id.to_string(),
            name: id.to_uppercase(),
            description: Some(description.to_string()),
        }
    }

    fn candidates() -> Vec<RecipeCandidate> {
        vec![
            candidate("planbuild", "plan, then build"),
            candidate("build", "build it"),
        ]
    }

    fn pick(recipe: &str) -> ChooserVerdict {
        ChooserVerdict::Pick {
            recipe: recipe.to_string(),
            reason: "a small fix".to_string(),
            confidence: Some(0.82),
            measured_by: "decide:qwen@v1".to_string(),
        }
    }

    fn abstain() -> ChooserVerdict {
        ChooserVerdict::Abstain {
            measured_by: "decide:qwen@v1".to_string(),
            reason: "too close to call".to_string(),
        }
    }

    fn loads(id: &str) -> Result<String, String> {
        Ok(id.to_string())
    }

    #[test]
    fn a_named_recipe_is_untouched_and_says_its_creator_named_it() {
        let resolved = settle(
            Some("memory-triage".into()),
            candidates(),
            None,
            Some("build".into()),
            |_| panic!("a named start never reads the default"),
        )
        .unwrap();
        assert_eq!(resolved.recipe, "memory-triage");
        assert_eq!(resolved.source, RecipeSource::Named);
        assert_eq!(resolved.measured_by, "named by creator");
        assert_eq!(resolved.line(), "recipe: memory-triage · named");
        // The candidates are recorded on the named path too, sorted by id.
        assert_eq!(resolved.candidates[0].id, "build");
        assert_eq!(resolved.chooser, None);
    }

    #[test]
    fn an_unnamed_start_takes_the_workspace_default() {
        let resolved = settle(None, candidates(), None, Some("build".into()), loads).unwrap();
        assert_eq!(resolved.recipe, "build");
        assert_eq!(resolved.source, RecipeSource::WorkspaceDefault);
        assert_eq!(
            resolved.line(),
            "recipe: build · workspace default (defaultRecipe setting)"
        );
    }

    #[test]
    fn no_default_refuses_naming_the_setting_and_the_recipes_here() {
        let refused = settle(None, candidates(), None, None, loads).unwrap_err();
        assert!(
            refused.starts_with("No recipe named and no default to choose with: set the workspace setting defaultRecipe"),
            "{refused}"
        );
        assert!(
            refused.ends_with("Recipes here: build, planbuild"),
            "{refused}"
        );
    }

    #[test]
    fn a_default_that_does_not_load_refuses_naming_the_setting_and_the_id() {
        let refused = settle(None, candidates(), None, Some("gone".into()), |_| {
            Err("Recipe not found: gone".to_string())
        })
        .unwrap_err();
        assert!(
            refused.contains(
                "defaultRecipe names 'gone', which cannot start here: Recipe not found: gone"
            ),
            "{refused}"
        );
    }

    #[test]
    fn a_deciding_choosers_pick_in_the_set_is_chosen_with_its_answerer() {
        let resolved = settle(
            None,
            candidates(),
            Some((pick("planbuild"), true)),
            Some("build".into()),
            loads,
        )
        .unwrap();
        assert_eq!(resolved.recipe, "planbuild");
        assert_eq!(resolved.source, RecipeSource::Chosen);
        assert_eq!(resolved.measured_by, "decide:qwen@v1");
        assert_eq!(resolved.confidence, Some(0.82));
        assert_eq!(
            resolved.line(),
            "recipe: planbuild · a small fix (decide:qwen@v1, 0.82)"
        );
    }

    #[test]
    fn an_abstain_falls_back_to_the_default_labelled_as_a_fallback() {
        let resolved = settle(
            None,
            candidates(),
            Some((abstain(), true)),
            Some("build".into()),
            loads,
        )
        .unwrap();
        assert_eq!(resolved.source, RecipeSource::DefaultAfterAbstain);
        assert_eq!(
            resolved.abstain_reason.as_deref(),
            Some("too close to call")
        );
        assert_eq!(
            resolved.line(),
            "recipe: build · workspace default, chooser abstained: too close to call"
        );
    }

    #[test]
    fn a_pick_outside_the_candidate_set_is_read_as_an_abstain() {
        let resolved = settle(
            None,
            candidates(),
            Some((pick("memory-triage"), true)),
            Some("build".into()),
            loads,
        )
        .unwrap();
        assert_eq!(resolved.recipe, "build");
        assert_eq!(resolved.source, RecipeSource::DefaultAfterAbstain);
        let opinion = resolved.chooser.unwrap();
        assert_eq!(opinion.recipe, None);
        assert!(
            opinion.reason.contains("'memory-triage'"),
            "{}",
            opinion.reason
        );
    }

    /// A chooser in shadow decides nothing, but its pick is recorded beside
    /// what ran, a named start's included, so an override is a row where the
    /// two differ.
    #[test]
    fn a_shadow_chooser_is_recorded_beside_what_ran() {
        let unnamed = settle(
            None,
            candidates(),
            Some((pick("planbuild"), false)),
            Some("build".into()),
            loads,
        )
        .unwrap();
        assert_eq!(unnamed.recipe, "build");
        assert_eq!(unnamed.source, RecipeSource::WorkspaceDefault);
        assert_eq!(
            unnamed.chooser.as_ref().unwrap().recipe.as_deref(),
            Some("planbuild")
        );

        let named = settle(
            Some("build".into()),
            candidates(),
            Some((pick("planbuild"), true)),
            None,
            loads,
        )
        .unwrap();
        assert_eq!(named.recipe, "build");
        assert_eq!(named.source, RecipeSource::Named);
        assert_eq!(
            named.chooser.as_ref().unwrap().recipe.as_deref(),
            Some("planbuild")
        );
    }

    /// The doors themselves, over the gate-sweep world: a project `prj` whose
    /// workspace has the `memory-triage` recipe on disk (one agent step,
    /// Integrator), written to by a live coordinator's run.
    mod doors {
        use crate::execution::gates::sweep::tests::{count, Env};
        use crate::mcp::types::McpCallbackRequest;
        use serde_json::{json, Value};

        /// The sweep world, numbering new issues past the ones it seeds.
        async fn env(name: &str) -> Env {
            let env = crate::execution::gates::sweep::tests::env(name).await;
            env.db
                .execute(
                    "UPDATE projects SET next_issue_number = 100 WHERE id = 'p'",
                    (),
                )
                .await
                .unwrap();
            env
        }

        async fn write(env: &Env, target: &str, mode: &str, payload: Value) -> Value {
            let request = McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: "/".to_string(),
                run_id: Some("run-coord".to_string()),
                tool: "write".to_string(),
                payload: json!({ "changes": [{ "target": target, "mode": mode, "payload": payload }] }),
                tool_use_id: None,
                origin: None,
            };
            let raw = crate::mcp::handlers::write::handle_write(&env.orch, &request).await;
            serde_json::from_str(&raw).unwrap_or_else(|_| panic!("{raw}"))
        }

        fn summary(report: &Value) -> String {
            report["applied"][0]["summary"]
                .as_str()
                .unwrap_or_else(|| panic!("nothing applied: {report}"))
                .to_string()
        }

        fn failure(report: &Value) -> String {
            report["failures"][0]["error"]
                .as_str()
                .unwrap_or_else(|| panic!("nothing refused: {report}"))
                .to_string()
        }

        fn default_recipe(env: &Env, recipe: Option<&str>) {
            env.orch
                .update_settings(crate::models::UpdateSettings {
                    default_recipe: Some(recipe.map(str::to_string)),
                    ..Default::default()
                })
                .unwrap();
        }

        async fn text(env: &Env, sql: &str) -> String {
            env.db.query_opt_text(sql, ()).await.unwrap().unwrap()
        }

        /// The reply's resolution block: a recipe line, one line per agent
        /// step, then the fix line.
        fn assert_resolution(reply: &str, recipe_line: &str) {
            let lines: Vec<&str> = reply.lines().collect();
            let at = lines
                .iter()
                .position(|line| *line == recipe_line)
                .unwrap_or_else(|| panic!("no `{recipe_line}` in:\n{reply}"));
            assert!(
                lines[at + 1].starts_with("Integrator "),
                "no step line in:\n{reply}"
            );
            assert!(
                lines[at + 2].starts_with("change a step: patch cairn://p/prj/")
                    && lines[at + 2].contains("{agent:\"integrator\",snapshot:{models:[...]}}")
                    && lines[at + 2].contains("re-run: append {recipe:\"memory-triage\""),
                "no fix line in:\n{reply}"
            );
        }

        /// A step that has launched never selects again, so the hint for it
        /// is a `selection` patch (which moves it from its next turn), never a
        /// `models` patch that would be recorded and change nothing.
        #[tokio::test(flavor = "multi_thread")]
        async fn the_fix_hint_for_a_launched_step_moves_its_selection() {
            let env = env("resolution-launched-hint.db").await;
            summary(
                &write(
                    &env,
                    "cairn://p/prj/4/executions",
                    "append",
                    json!({ "recipe": "memory-triage" }),
                )
                .await,
            );
            let execution = text(&env, "SELECT id FROM executions WHERE issue_id = 'i4'").await;
            let preview = || async {
                crate::execution::launch_preview::preview_execution(
                    &env.orch, &env.db, &execution, "p",
                )
                .await
            };
            // Store the step's prediction as its launch would.
            let record = preview().await.unwrap()[0].record().cloned().unwrap();
            env.db
                .execute(
                    "UPDATE jobs SET selection = ?1, model = ?2 WHERE execution_id = ?3",
                    cairn_db::turso::params![
                        serde_json::to_string(&record).unwrap(),
                        record.chosen.model.as_str(),
                        execution.as_str()
                    ],
                )
                .await
                .unwrap();

            let filing = crate::mcp::handlers::executions::resolve_filing(
                &env.orch,
                "prj",
                4,
                Some("memory-triage"),
                Default::default(),
                None,
            )
            .await
            .unwrap();
            let steps = preview().await;
            let reply = crate::mcp::handlers::executions::resolution_lines(
                &filing,
                &steps,
                "cairn://p/prj/4",
                1,
            );
            let fix = reply.lines().last().unwrap();
            assert!(reply.contains("\nIntegrator chose: "), "{reply}");
            assert!(
                fix.starts_with(
                    "move a launched or pinned step: patch cairn://p/prj/4/executions/1 \
                     {agent:\"integrator\",snapshot:{selection:"
                ),
                "{fix}"
            );
            assert!(!fix.contains("snapshot:{models"), "{fix}");
            assert!(
                fix.contains(" · re-run: append {recipe:\"memory-triage\""),
                "{fix}"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn execution_true_with_a_default_starts_and_says_the_default_chose() {
            let env = env("resolution-true.db").await;
            default_recipe(&env, Some("memory-triage"));
            let reply = summary(
                &write(
                    &env,
                    "cairn://p/prj/issues",
                    "append",
                    json!({ "title": "Child", "execution": true }),
                )
                .await,
            );
            assert_resolution(
                &reply,
                "recipe: memory-triage · workspace default (defaultRecipe setting)",
            );
            assert_eq!(
                text(
                    &env,
                    "SELECT source || ' ' || resolved_recipe || ' ' || measured_by || ' ' || created_by_kind
                       FROM recipe_resolutions"
                )
                .await,
                "workspace_default memory-triage defaultRecipe setting agent"
            );
            // The preview is stored, one entry per agent step.
            let preview: Value =
                serde_json::from_str(&text(&env, "SELECT preview FROM recipe_resolutions").await)
                    .unwrap();
            assert_eq!(preview.as_array().map(Vec::len), Some(1), "{preview}");
            // Node ids are minted when the recipe loads; the model is the
            // step's prediction.
            assert!(preview[0]["model"].is_string(), "{preview}");
            // The option set is stored once, under the row's hash.
            assert_eq!(
                count(
                    &env,
                    "SELECT COUNT(*) FROM recipe_option_sets o JOIN recipe_resolutions r
                       ON r.project_id = o.project_id AND r.candidates_hash = o.candidates_hash"
                )
                .await,
                1
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_named_recipe_is_run_exactly_and_says_so() {
            let env = env("resolution-named.db").await;
            let reply = summary(
                &write(
                    &env,
                    "cairn://p/prj/issues",
                    "append",
                    json!({ "title": "Child", "execution": { "recipe": "Memory-Triage" } }),
                )
                .await,
            );
            assert_resolution(&reply, "recipe: memory-triage · named");
            assert_eq!(
                text(
                    &env,
                    "SELECT source || ' ' || measured_by FROM recipe_resolutions"
                )
                .await,
                "named named by creator"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn execution_true_without_a_default_is_refused_and_files_no_issue() {
            let env = env("resolution-refused.db").await;
            let issues = count(&env, "SELECT COUNT(*) FROM issues").await;
            let refused = failure(
                &write(
                    &env,
                    "cairn://p/prj/issues",
                    "append",
                    json!({ "title": "Child", "execution": true }),
                )
                .await,
            );
            assert!(refused.contains("defaultRecipe"), "{refused}");
            assert_eq!(count(&env, "SELECT COUNT(*) FROM issues").await, issues);

            // `false` is not a way to start.
            let refused = failure(
                &write(
                    &env,
                    "cairn://p/prj/issues",
                    "append",
                    json!({ "title": "Child", "execution": false }),
                )
                .await,
            );
            assert!(refused.contains("must be true"), "{refused}");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn an_append_without_a_recipe_resolves_the_same_way() {
            let env = env("resolution-append.db").await;
            let refused =
                failure(&write(&env, "cairn://p/prj/1/executions", "append", json!({})).await);
            assert!(refused.contains("defaultRecipe"), "{refused}");
            assert_eq!(
                count(
                    &env,
                    "SELECT COUNT(*) FROM executions WHERE issue_id = 'i1'"
                )
                .await,
                0
            );

            default_recipe(&env, Some("memory-triage"));
            let reply =
                summary(&write(&env, "cairn://p/prj/1/executions", "append", json!({})).await);
            assert_resolution(
                &reply,
                "recipe: memory-triage · workspace default (defaultRecipe setting)",
            );

            // A re-run is linked to the execution it superseded.
            let first = text(&env, "SELECT execution_id FROM recipe_resolutions").await;
            summary(
                &write(
                    &env,
                    "cairn://p/prj/1/executions",
                    "append",
                    json!({ "recipe": "memory-triage", "overrides": { "agents": { "integrator": { "models": ["lg"] } } } }),
                )
                .await,
            );
            assert_eq!(
                text(
                    &env,
                    "SELECT supersedes_execution_id || ' ' || overrides FROM recipe_resolutions
                      WHERE supersedes_execution_id IS NOT NULL"
                )
                .await,
                format!("{first} {{\"agents\":{{\"integrator\":{{\"models\":[\"lg\"]}}}}}}")
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_held_filing_carries_the_resolution_too() {
            let env = env("resolution-held.db").await;
            let reply = summary(
                &write(
                    &env,
                    "cairn://p/prj/2/executions",
                    "append",
                    json!({
                        "recipe": "memory-triage",
                        "after": [{ "fact": "clock", "notBefore": "2099-01-01T00:00:00Z" }]
                    }),
                )
                .await,
            );
            assert!(reply.contains("HELD"), "{reply}");
            assert_resolution(&reply, "recipe: memory-triage · named");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_live_edit_is_recorded_and_the_execution_read_shows_it() {
            let env = env("resolution-edit.db").await;
            summary(
                &write(
                    &env,
                    "cairn://p/prj/3/executions",
                    "append",
                    json!({ "recipe": "memory-triage" }),
                )
                .await,
            );
            summary(
                &write(
                    &env,
                    "cairn://p/prj/3/executions/1",
                    "patch",
                    // The Integrator's own allotment is [lg].
                    json!({ "agent": "integrator", "snapshot": { "models": ["sm"] } }),
                )
                .await,
            );
            assert_eq!(
                text(
                    &env,
                    "SELECT changed || ' ' || before || ' ' || after FROM execution_agent_edits"
                )
                .await,
                "[\"models\"] {\"models\":[\"lg\"],\"selection\":null} \
                 {\"models\":[\"sm\"],\"selection\":null}"
            );
            // What the execution read renders under `## resolution`.
            let read = env
                .db
                .read(|conn| {
                    Box::pin(async move {
                        let mut rows = conn
                            .query("SELECT id FROM executions WHERE issue_id = 'i3'", ())
                            .await?;
                        let id = crate::storage::RowExt::text(&rows.next().await?.unwrap(), 0)?;
                        drop(rows);
                        Ok(super::super::resolution_block(conn, &id).await)
                    })
                })
                .await
                .unwrap();
            assert!(
                read.contains("\n## resolution\nrecipe: memory-triage · named · filed by agent "),
                "{read}"
            );
            assert!(
                read.contains("- integrator edited (models) by cairn://p/prj/"),
                "{read}"
            );
        }
    }

    #[test]
    fn the_candidates_hash_ignores_order_and_moves_with_a_description() {
        let mut reversed = candidates();
        reversed.reverse();
        assert_eq!(candidates_hash(&candidates()), candidates_hash(&reversed));
        let mut edited = candidates();
        edited[0].description = Some("plan, then build, then review".into());
        assert_ne!(candidates_hash(&candidates()), candidates_hash(&edited));
    }
}
