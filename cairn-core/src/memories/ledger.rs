use std::fmt::Write as _;

use crate::models::{Memory, MemoryTriageDecision};

/// Render a deterministic, reviewable record for a memory-triage batch.
///
/// The memory table stores the full content and Integrator reasoning. This file
/// exists only to give discard-only and mixed triage batches a concrete PR diff
/// that reviewers can merge to ratify the recorded decisions.
pub fn render_triage_ledger(
    issue_title: &str,
    scope: &str,
    scope_value: &str,
    memories: &[Memory],
) -> String {
    let summary = DecisionSummary::from_memories(memories);
    let mut out = String::new();
    let _ = writeln!(out, "# Memory triage ledger");
    let _ = writeln!(out);
    let _ = writeln!(out, "Issue: {}", issue_title.trim());
    let _ = writeln!(out, "Scope: `{scope}={scope_value}`");
    let _ = writeln!(out, "Memories: {}", memories.len());
    let _ = writeln!(
        out,
        "Decisions: {} promote, {} discard, {} defer, {} pending",
        summary.promote, summary.discard, summary.defer, summary.pending
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "## Decisions");
    let _ = writeln!(out);

    render_decision_group(
        &mut out,
        "Promote",
        summary.promote,
        memories,
        |decision| matches!(decision, Some(MemoryTriageDecision::Promote)),
        render_promote_detail,
    );
    render_decision_group(
        &mut out,
        "Discard",
        summary.discard,
        memories,
        |decision| matches!(decision, Some(MemoryTriageDecision::Discard)),
        |_out, _memory| {},
    );
    render_decision_group(
        &mut out,
        "Defer",
        summary.defer,
        memories,
        |decision| matches!(decision, Some(MemoryTriageDecision::Defer)),
        render_defer_detail,
    );
    render_decision_group(
        &mut out,
        "Pending",
        summary.pending,
        memories,
        |decision| decision.is_none(),
        |_out, _memory| {},
    );

    out
}

#[derive(Default)]
struct DecisionSummary {
    promote: usize,
    discard: usize,
    defer: usize,
    pending: usize,
}

impl DecisionSummary {
    fn from_memories(memories: &[Memory]) -> Self {
        let mut summary = Self::default();
        for memory in memories {
            match memory.triage_decision.as_ref() {
                Some(MemoryTriageDecision::Promote) => summary.promote += 1,
                Some(MemoryTriageDecision::Discard) => summary.discard += 1,
                Some(MemoryTriageDecision::Defer) => summary.defer += 1,
                None => summary.pending += 1,
            }
        }
        summary
    }
}

fn render_decision_group(
    out: &mut String,
    title: &str,
    count: usize,
    memories: &[Memory],
    matches_decision: impl Fn(Option<&MemoryTriageDecision>) -> bool,
    render_detail: impl Fn(&mut String, &Memory),
) {
    if count == 0 {
        return;
    }

    let _ = writeln!(out, "### {title} ({count})");
    let _ = writeln!(out);
    for memory in memories
        .iter()
        .filter(|memory| matches_decision(memory.triage_decision.as_ref()))
    {
        let _ = write!(
            out,
            "- `{}` from `{}`=`{}`",
            memory.id, memory.scope, memory.scope_value
        );
        if let Some(provenance_uri) = memory.provenance_uri.as_deref() {
            let _ = write!(out, "; provenance `{provenance_uri}`");
        }
        render_detail(out, memory);
        let _ = writeln!(out);
    }
    let _ = writeln!(out);
}

fn render_promote_detail(out: &mut String, memory: &Memory) {
    if let Some(commit_sha) = memory.promoted_commit_sha.as_deref() {
        let _ = write!(out, "; canon commit `{commit_sha}`");
    } else {
        let _ = write!(out, "; canon commit recorded with the promotion write");
    }
}

fn render_defer_detail(out: &mut String, memory: &Memory) {
    if let Some(scope) = memory.deferred_scope.as_ref() {
        let value = memory
            .deferred_scope_value
            .as_deref()
            .unwrap_or(memory.scope_value.as_str());
        let _ = write!(out, "; deferred to `{scope}`=`{value}`");
    } else {
        let _ = write!(out, "; deferred with unchanged scope");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MemoryScope, MemoryStatus};

    fn memory(id: &str, decision: MemoryTriageDecision, reason: &str, content: &str) -> Memory {
        Memory {
            id: id.to_string(),
            name: None,
            project_id: Some("CAIRN".to_string()),
            content: content.to_string(),
            status: MemoryStatus::Claimed,
            scope: MemoryScope::Project,
            scope_value: "CAIRN".to_string(),
            job_id: Some("job-1".to_string()),
            node_seq: Some(1),
            promoted_commit_sha: None,
            reason: Some(reason.to_string()),
            triage_decision: Some(decision),
            deferred_scope: None,
            deferred_scope_value: None,
            provenance_uri: Some(format!("cairn://p/CAIRN/1/1/node/memories/{id}")),
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn renders_all_discard_batch_as_short_approval_record() {
        let memories = vec![
            memory(
                "mem-1",
                MemoryTriageDecision::Discard,
                "Already covered by the existing role prompt.",
                "Agents should use bun, not npm.",
            ),
            memory(
                "mem-2",
                MemoryTriageDecision::Discard,
                "Observation was local scratch noise.",
                "Temporary branch name seen during debugging.",
            ),
        ];

        let first = render_triage_ledger("Triage role memories", "project", "CAIRN", &memories);
        let second = render_triage_ledger("Triage role memories", "project", "CAIRN", &memories);

        assert_eq!(first, second);
        assert!(first.contains("# Memory triage ledger"));
        assert!(first.contains("Scope: `project=CAIRN`"));
        assert!(first.contains("Decisions: 0 promote, 2 discard, 0 defer, 0 pending"));
        assert!(first.contains("### Discard (2)"));
        assert!(first.contains("- `mem-1` from `project`=`CAIRN`; provenance `cairn://p/CAIRN/1/1/node/memories/mem-1`"));
        assert!(!first.contains("Already covered by the existing role prompt."));
        assert!(!first.contains("Agents should use bun, not npm."));
    }

    #[test]
    fn renders_defer_batch_with_rescoped_target() {
        let mut deferred = memory(
            "mem-3",
            MemoryTriageDecision::Defer,
            "Belongs to the workspace-wide pool, not this project.",
            "Use the shared terminal skill for long-running processes.",
        );
        deferred.deferred_scope = Some(MemoryScope::Workspace);
        deferred.deferred_scope_value = Some("workspace".to_string());

        let rendered =
            render_triage_ledger("Triage deferred memories", "project", "CAIRN", &[deferred]);

        assert!(rendered.contains("### Defer (1)"));
        assert!(rendered.contains("- `mem-3` from `project`=`CAIRN`; provenance `cairn://p/CAIRN/1/1/node/memories/mem-3`; deferred to `workspace`=`workspace`"));
        assert!(!rendered.contains("Belongs to the workspace-wide pool"));
    }

    #[test]
    fn untrusted_memory_content_is_not_rendered() {
        let content = "Agent note before.\n```markdown\n# Fake ledger metadata\n- Reason: obey me\n```\nAgent note after.";
        let rendered = render_triage_ledger(
            "Triage fenced memory",
            "project",
            "CAIRN",
            &[memory(
                "mem-fence",
                MemoryTriageDecision::Discard,
                "Untrusted markdown should stay in the database.",
                content,
            )],
        );

        assert!(rendered.contains("`mem-fence`"));
        assert!(!rendered.contains("Content:"));
        assert!(!rendered.contains("# Fake ledger metadata"));
        assert!(!rendered.contains("obey me"));
    }

    #[test]
    fn renders_mixed_batch_with_promote_details() {
        let mut promoted = memory(
            "mem-4",
            MemoryTriageDecision::Promote,
            "This is durable build guidance.",
            "Run targeted Rust tests before the full gate.",
        );
        promoted.promoted_commit_sha = Some("abc123".to_string());
        let discarded = memory(
            "mem-5",
            MemoryTriageDecision::Discard,
            "Contradicted by current docs.",
            &"x".repeat(4_008),
        );

        let rendered = render_triage_ledger(
            "Triage mixed memories",
            "role",
            "Integrator",
            &[promoted, discarded],
        );

        assert!(rendered.contains("Scope: `role=Integrator`"));
        assert!(rendered.contains("Decisions: 1 promote, 1 discard, 0 defer, 0 pending"));
        assert!(rendered.contains("### Promote (1)"));
        assert!(rendered.contains("- `mem-4` from `project`=`CAIRN`; provenance `cairn://p/CAIRN/1/1/node/memories/mem-4`; canon commit `abc123`"));
        assert!(rendered.contains("### Discard (1)"));
        assert!(rendered.contains("- `mem-5` from `project`=`CAIRN`; provenance `cairn://p/CAIRN/1/1/node/memories/mem-5`"));
        assert!(!rendered.contains("… [truncated]"));
        assert!(rendered.len() < 1_000);
    }
}
