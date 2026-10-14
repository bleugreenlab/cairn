use crate::services::GitClient;
use std::path::Path;

/// Delete a local branch after the owning issue's landed/closed policy has
/// authorized removal. Missing branches are already in the desired state.
pub fn delete_with_services(
    git: &dyn GitClient,
    repository: &Path,
    branch: &str,
) -> Result<(), String> {
    if !git.branch_exists(repository, branch)? {
        log::info!("Branch {branch} does not exist, skipping deletion");
        return Ok(());
    }

    git.delete_branch(repository, branch, true)?;
    log::info!("Deleted local branch {branch}");
    Ok(())
}
