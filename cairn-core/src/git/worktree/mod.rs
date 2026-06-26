use crate::services::{
    FileSystem, GitClient, ProcessSpawner, RealFileSystem, RealGitClient, SpawnConfig,
};
use std::path::{Path, PathBuf};

mod lifecycle;
mod naming;
mod populate;
mod setup;

pub use lifecycle::{
    create_worktree_with_services, delete_branch_with_services, remove_worktree,
    remove_worktree_with_services,
};
pub use naming::{
    branch_exists_with_git, get_worktree_for_branch_with_git, parse_worktree_list_for_branch,
};
pub use populate::{populate_worktree, PopulateResult};
pub use setup::{
    run_setup_commands_with_process, run_setup_commands_with_process_streaming, SetupError,
};
