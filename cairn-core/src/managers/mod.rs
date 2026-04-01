//! Manager business logic.

use std::path::PathBuf;

pub mod branch;
pub mod completion;
pub mod context;
pub mod crud;
pub mod identity;
pub mod mailbox;
pub mod snapshot;
pub mod wake;

/// Return the relative path to a manager's dashboard file.
///
/// The path is `.cairn/managers/{slug}.md` where `{slug}` is the slugified
/// manager name.  Multiple managers can share a branch, so the name must be
/// part of the path.
pub fn dashboard_path(manager_name: &str) -> PathBuf {
    let slug = crate::config::slugify(manager_name);
    PathBuf::from(".cairn/managers").join(format!("{}.md", slug))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_path_simple_name() {
        let path = dashboard_path("My Manager");
        assert_eq!(path, PathBuf::from(".cairn/managers/my-manager.md"));
    }

    #[test]
    fn test_dashboard_path_special_chars() {
        let path = dashboard_path("Auth & Session Manager!");
        assert_eq!(
            path,
            PathBuf::from(".cairn/managers/auth-session-manager.md")
        );
    }

    #[test]
    fn test_dashboard_path_already_slug() {
        let path = dashboard_path("simple");
        assert_eq!(path, PathBuf::from(".cairn/managers/simple.md"));
    }
}
