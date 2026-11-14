pub mod codemap;
pub mod crud;
pub mod default_branch;
pub mod export;
pub mod human_base_merge_policy;
pub mod library_trust;
pub mod migrate;
pub mod remote;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::config::project_settings::{CheckCommand, UnsupportedCheck};
use crate::orchestrator::Orchestrator;

/// The declarations in force at one resolved default-branch tip, never a checkout edit.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeclaredProjectChecks {
    pub branch: String,
    pub commit: String,
    pub checks: HashMap<String, CheckCommand>,
    pub unsupported_checks: BTreeMap<String, UnsupportedCheck>,
    pub unsupported_declarations: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_unsupported_declarations: Option<BTreeMap<String, serde_json::Value>>,
    pub local_error: Option<String>,
}

pub async fn declared_checks_at_branch(
    orch: &Orchestrator,
    repo: &Path,
    branch: &str,
) -> Result<DeclaredProjectChecks, String> {
    use crate::execution::checks::CommitChecksContractError;

    let store = crate::jj::project_store_dir(&orch.config_dir, repo);
    let commit = cairn_vcs::resolve_coordinate(&store, branch)
        .await
        .map_err(|error| format!("Unavailable: `{branch}` did not resolve: {error}"))?;
    let contract = match crate::execution::checks::load_checks_contract_at_commit_detailed(
        repo, &commit,
    )
    .await
    {
        Ok(loaded) => loaded.contract,
        Err(CommitChecksContractError::Missing | CommitChecksContractError::NoChecks) => {
            Default::default()
        }
        Err(error) => return Err(error.manual_diagnostic(&commit)),
    };
    let unsupported_declarations = contract
        .unsupported
        .iter()
        .map(|(name, check)| {
            serde_json::to_value(&check.declared)
                .map(|declared| (name.clone(), declared))
                .map_err(|error| {
                    format!("Cannot represent declaration {name} at {commit}: {error}")
                })
        })
        .collect::<Result<_, _>>()?;
    Ok(DeclaredProjectChecks {
        branch: branch.to_string(),
        commit,
        checks: contract.checks,
        unsupported_checks: contract.unsupported,
        unsupported_declarations,
        local_unsupported_declarations: None,
        local_error: None,
    })
}

pub async fn declared_checks(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<DeclaredProjectChecks, String> {
    let project = crud::resolve_project_repository(&orch.db, project_id, &orch.config_dir)
        .await
        .map_err(|error| error.to_string())?;
    let config = match crate::config::project_settings::load_project_settings_strict(
        &project.local_repo_path,
    ) {
        Ok((config, _)) => config,
        Err(crate::config::project_settings::ProjectConfigError::Missing { .. }) => {
            Default::default()
        }
        Err(error) => {
            let mut declared =
                declared_checks_at_branch(orch, &project.local_repo_path, &project.default_branch)
                    .await?;
            declared.local_error = Some(error.to_string());
            declared.local_unsupported_declarations = Some(Default::default());
            return Ok(declared);
        }
    };
    let branch = crate::config::project_settings::resolve_default_branch(
        &config,
        Some(&project.default_branch),
    );
    let mut declared = declared_checks_at_branch(orch, &project.local_repo_path, &branch).await?;
    declared.local_unsupported_declarations = Some(
        config
            .unsupported_checks
            .iter()
            .map(|(name, check)| {
                serde_json::to_value(&check.declared)
                    .map(|value| (name.clone(), value))
                    .map_err(|error| format!("Cannot represent local declaration {name}: {error}"))
            })
            .collect::<Result<_, _>>()?,
    );
    Ok(declared)
}
