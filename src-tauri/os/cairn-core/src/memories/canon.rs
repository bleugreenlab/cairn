use std::path::{Path, PathBuf};

use crate::models::MemoryScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonDestinationKind {
    WorkspaceAgentPrompt,
    ProjectAgentPrompt,
    ProjectAgentsMd,
    ProjectSkillsDir,
    WorkspaceAgentsMd,
    WorkspaceSkillsDir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonDestinationCandidate {
    path: PathBuf,
    kind: CanonDestinationKind,
    exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonDestination {
    candidates: Vec<CanonDestinationCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleCanonHome {
    Workspace,
    Project { project_id: String },
}

fn candidate(path: PathBuf, kind: CanonDestinationKind) -> CanonDestinationCandidate {
    let exists = path.exists();
    CanonDestinationCandidate { path, kind, exists }
}

pub(crate) fn resolve_role_canon_home<'a, I>(
    config_dir: &Path,
    role: &str,
    project_paths: I,
) -> RoleCanonHome
where
    I: IntoIterator<Item = (&'a str, &'a Path)>,
{
    for (project_id, repo_path) in project_paths {
        let destination =
            resolve_canon_destination(config_dir, MemoryScope::Role, role, Some(repo_path));
        if destination.candidates.iter().any(|candidate| {
            candidate.kind == CanonDestinationKind::ProjectAgentPrompt && candidate.exists
        }) {
            return RoleCanonHome::Project {
                project_id: project_id.to_string(),
            };
        }
    }

    RoleCanonHome::Workspace
}

fn resolve_canon_destination(
    config_dir: &Path,
    scope: MemoryScope,
    scope_value: &str,
    repo_path: Option<&Path>,
) -> CanonDestination {
    let candidates = match scope {
        MemoryScope::Role => {
            let role_file = format!("{scope_value}.md");
            let mut candidates = vec![candidate(
                config_dir.join("agents").join(&role_file),
                CanonDestinationKind::WorkspaceAgentPrompt,
            )];
            if let Some(repo_path) = repo_path {
                candidates.push(candidate(
                    repo_path.join(".cairn").join("agents").join(role_file),
                    CanonDestinationKind::ProjectAgentPrompt,
                ));
            }
            candidates
        }
        MemoryScope::Project => repo_path
            .map(|repo_path| {
                vec![
                    candidate(
                        repo_path.join("AGENTS.md"),
                        CanonDestinationKind::ProjectAgentsMd,
                    ),
                    candidate(
                        repo_path.join(".cairn").join("skills"),
                        CanonDestinationKind::ProjectSkillsDir,
                    ),
                ]
            })
            .unwrap_or_default(),
        MemoryScope::Workspace => vec![
            candidate(
                config_dir.join("AGENTS.md"),
                CanonDestinationKind::WorkspaceAgentsMd,
            ),
            candidate(
                config_dir.join("skills"),
                CanonDestinationKind::WorkspaceSkillsDir,
            ),
        ],
    };

    CanonDestination { candidates }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn role_returns_workspace_and_project_live_prompts() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(config.join("agents")).unwrap();
        std::fs::create_dir_all(repo.join(".cairn/agents")).unwrap();
        std::fs::write(config.join("agents/integrator.md"), "prompt").unwrap();

        let dest = resolve_canon_destination(&config, MemoryScope::Role, "integrator", Some(&repo));
        assert_eq!(dest.candidates.len(), 2);
        assert_eq!(
            dest.candidates[0].kind,
            CanonDestinationKind::WorkspaceAgentPrompt
        );
        assert!(dest.candidates[0].exists);
        assert_eq!(
            dest.candidates[1].kind,
            CanonDestinationKind::ProjectAgentPrompt
        );
        assert!(!dest.candidates[1].exists);
    }

    #[test]
    fn role_home_prefers_project_prompt_when_present() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(config.join("agents")).unwrap();
        std::fs::create_dir_all(repo.join(".cairn/agents")).unwrap();
        std::fs::write(config.join("agents/integrator.md"), "workspace").unwrap();
        std::fs::write(repo.join(".cairn/agents/integrator.md"), "project").unwrap();

        let home = resolve_role_canon_home(&config, "integrator", [("project-1", repo.as_path())]);
        assert_eq!(
            home,
            RoleCanonHome::Project {
                project_id: "project-1".to_string()
            }
        );
    }

    #[test]
    fn role_home_falls_back_to_workspace_prompt() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(config.join("agents")).unwrap();
        std::fs::create_dir_all(repo.join(".cairn/agents")).unwrap();
        std::fs::write(config.join("agents/integrator.md"), "workspace").unwrap();

        let home = resolve_role_canon_home(&config, "integrator", [("project-1", repo.as_path())]);
        assert_eq!(home, RoleCanonHome::Workspace);
    }

    #[test]
    fn project_returns_agents_md_and_project_skills() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let repo = temp.path().join("repo");
        std::fs::create_dir_all(repo.join(".cairn/skills")).unwrap();

        let dest =
            resolve_canon_destination(&config, MemoryScope::Project, "project-1", Some(&repo));
        assert_eq!(dest.candidates.len(), 2);
        assert_eq!(dest.candidates[0].path, repo.join("AGENTS.md"));
        assert_eq!(
            dest.candidates[1].kind,
            CanonDestinationKind::ProjectSkillsDir
        );
        assert!(dest.candidates[1].exists);
    }

    #[test]
    fn workspace_returns_workspace_agents_md_and_skills() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir_all(config.join("skills")).unwrap();

        let dest = resolve_canon_destination(&config, MemoryScope::Workspace, "workspace", None);
        assert_eq!(dest.candidates.len(), 2);
        assert_eq!(dest.candidates[0].path, config.join("AGENTS.md"));
        assert_eq!(
            dest.candidates[1].kind,
            CanonDestinationKind::WorkspaceSkillsDir
        );
        assert!(dest.candidates[1].exists);
    }
}
