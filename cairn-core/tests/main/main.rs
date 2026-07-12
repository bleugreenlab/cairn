//! Single merged integration-test binary for cairn-core.
//!
//! Each module below was previously its own top-level `tests/*.rs` file and
//! therefore its own integration-test crate — 49 separate link steps per
//! build. Merging them into one binary cuts that to a single link. Test
//! names gain the module prefix (e.g. `todos::create_and_list`); nextest
//! substring filters keep matching, and `--test main` selects the binary.

mod common;

mod account;
mod accumulator;
mod action_runs;
mod artifact_seen;
mod condition;
mod config_scope;
mod continue_job_queue;
mod coordinator_attention;
mod ctx_self_lifecycle;
mod effects_checkpoint;
mod effects_checkpoint_loop;
mod embeddings_db;
mod execution_dag;
mod execution_job_creation;
mod execution_lock;
mod flail_dedup;
mod github_credentials;
mod issue_outcome;
mod issue_status_resolution;
mod issues;
mod job_continue_recovery;
mod lifecycle_stop;
mod mcp_change_parity;
mod mcp_config_resources;
mod mcp_file_changes;
mod mcp_issue_resources;
mod mcp_node_permission_answer;
mod mcp_node_question_answer;
mod mcp_openrouter_suspend;
mod mcp_pr_action_dispatch;
mod mcp_registry_fence;
mod mcp_run_cas_acceptance;
mod mcp_run_commit_hygiene;
mod mcp_run_execution;
mod mcp_skill_resources;
mod memories;
mod pr_actions;
mod project_prs;
mod projects;
mod prompt_resume_event;
mod queued_direct_messages;
mod read_fence_grep_parity;
mod run_fence_suspend_resume;
mod runs;
mod runtime;
mod team_connect;
mod team_sync_loop;
mod terminal_exit_wakes;
mod todos;
mod transitions;
mod turso_sync_roundtrip;
mod watch_attention;
