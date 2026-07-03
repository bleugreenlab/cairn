#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TargetFamily {
    Resource,
    File,
}

pub(super) fn invalid_target_error(target: &str) -> String {
    format!(
        "Invalid target: expected cairn://... or file:...; use file:relative/path (worktree-relative), file:/absolute/path, or bare file: for the worktree root instead of '{target}'"
    )
}

pub(super) fn target_family(target: &str) -> Result<TargetFamily, String> {
    // One classification rule, shared with the validator in cairn-common.
    use cairn_common::change_validation::{classify_target, TargetKind};
    match classify_target(target) {
        TargetKind::Resource => Ok(TargetFamily::Resource),
        TargetKind::File => Ok(TargetFamily::File),
        TargetKind::Invalid => Err(invalid_target_error(target)),
    }
}
