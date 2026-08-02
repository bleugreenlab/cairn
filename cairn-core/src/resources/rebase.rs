//! `cairn:~/rebase` — the active conflict resolution session for a node's branch.
//!
//! When a base advance cannot replay a branch, the rebase is rolled back and the
//! branch is left untouched. That is the safety property; the cost it used to
//! carry was an information-free tree. This resource pays that cost back: it
//! names what arrived, shows both sides of the merge, lists the incoming change's
//! COMPLETE file set flagged conflicting versus clean-on-retry, states whether
//! markers are actually present in the checkout, and offers the one sanctioned
//! next action.
//!
//! Both sides are recomputed from immutable commits rather than stored, so the
//! session never serves an aging patch. When an object is unavailable the read
//! says so and keeps the stored identity and inventory — it never substitutes the
//! rolled-back current tree, which answers a different question and would read as
//! an authority.

use cairn_common::query::QueryParam;

use super::common::{connect_and_find_node_job, find_query_value, node_branch};
use crate::orchestrator::conflict_session::{
    load_active_session, ConflictSession, MarkerState, SessionFile,
};
use crate::orchestrator::Orchestrator;

/// Hard ceiling on patch lines served in one read, regardless of `limit`. A
/// merge side can be enormous; a resource that streams all of it into a context
/// window is not more useful than one that pages.
const MAX_PATCH_LINES: usize = 400;
const DEFAULT_PATCH_LINES: usize = 200;
/// Hard ceiling on inventory rows in the summary. The counts stay exact.
const MAX_FILE_ROWS: usize = 100;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum RebaseView {
    Summary,
    BaseOurs,
    BaseTheirs,
}

#[derive(Debug)]
struct RebaseRequest<'a> {
    view: RebaseView,
    file: Option<&'a str>,
    offset: usize,
    limit: usize,
}

fn parse_rebase_request<'a>(params: &'a [QueryParam]) -> Result<RebaseRequest<'a>, String> {
    if let Some(param) = params
        .iter()
        .find(|param| !matches!(param.key.as_str(), "view" | "file" | "offset" | "limit"))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for node rebase. Expected view, file, offset, or limit.",
            param.key
        ));
    }
    let view = match find_query_value(params, "view") {
        None => RebaseView::Summary,
        Some("base-ours") => RebaseView::BaseOurs,
        Some("base-theirs") => RebaseView::BaseTheirs,
        Some(value) => {
            return Err(format!(
                "Invalid node rebase view '{value}'. Expected base-ours or base-theirs."
            ));
        }
    };
    let file = find_query_value(params, "file").filter(|value| !value.is_empty());
    if file.is_some() && view == RebaseView::Summary {
        return Err("file=PATH is only valid with view=base-ours or view=base-theirs".to_string());
    }
    let parse_number = |key: &str| -> Result<Option<usize>, String> {
        match find_query_value(params, key).filter(|value| !value.is_empty()) {
            None => Ok(None),
            Some(value) => value
                .parse::<usize>()
                .map(Some)
                .map_err(|_| format!("Invalid {key} '{value}' for node rebase; expected a number")),
        }
    };
    let offset = parse_number("offset")?.unwrap_or(0);
    // The cap is applied here rather than trusted from the caller, so no request
    // can ask the server for an unbounded read.
    let limit = parse_number("limit")?
        .unwrap_or(DEFAULT_PATCH_LINES)
        .clamp(1, MAX_PATCH_LINES);
    Ok(RebaseRequest {
        view,
        file,
        offset,
        limit,
    })
}

pub(super) async fn read_node_rebase(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    params: &[QueryParam],
) -> String {
    let request = match parse_rebase_request(params) {
        Ok(request) => request,
        Err(error) => return error,
    };
    let db = orch.db.for_project(project).await;
    let (conn, job) = match connect_and_find_node_job(&db, project, number, exec_seq, node_id).await
    {
        Ok(found) => found,
        Err(error) => return error,
    };
    let branch = match node_branch(&conn, &job.id).await {
        Ok(Some(branch)) => branch,
        Ok(None) => {
            return "This node has no branch, so it cannot have a rebase session.".to_string()
        }
        Err(error) => return error,
    };
    let session = match load_active_session(&db, &branch).await {
        Ok(Some(session)) => session,
        Ok(None) => return render_no_session(&branch),
        Err(error) => return error,
    };

    match request.view {
        RebaseView::Summary => render_summary(project, number, exec_seq, node_id, &session),
        RebaseView::BaseOurs | RebaseView::BaseTheirs => {
            render_side(orch, project, number, exec_seq, node_id, &session, &request)
        }
    }
}

fn render_no_session(branch: &str) -> String {
    format!(
        "# Rebase session\n\nNo open conflict resolution session for `{branch}`.\n\nThis is the \
         ordinary state: the branch either has never hit a conflicting base advance, or the last \
         one was resolved and closed. Nothing needs doing here."
    )
}

fn incoming_line(session: &ConflictSession) -> String {
    let what = match (
        session.incoming.pr_number,
        session.incoming.issue.as_deref(),
    ) {
        (Some(pr), Some(issue)) => format!("PR #{pr} ({issue})"),
        (Some(pr), None) => format!("PR #{pr}"),
        (None, Some(issue)) => issue.to_string(),
        (None, None) => "an external advance".to_string(),
    };
    let onto = if session.incoming.base_branch.is_empty() {
        session.target_branch.clone()
    } else {
        session.incoming.base_branch.clone()
    };
    format!("{what} landed on `{onto}`")
}

fn marker_line(session: &ConflictSession) -> String {
    match session.marker_state {
        MarkerState::Materialized => {
            let bearing: Vec<&str> = session
                .files
                .iter()
                .filter(|file| file.marker_disposition.as_deref() == Some("markers"))
                .map(|file| file.path.as_str())
                .collect();
            if bearing.is_empty() {
                "**Markers:** materialized, and none were needed — the three-way merge resolved \
                 every conflicting file on its own. Review the merged content, then commit it."
                    .to_string()
            } else {
                format!(
                    "**Markers:** present in your checkout, in {}. Resolve them with ordinary \
                     file writes and commit; a file still bearing markers cannot be committed.",
                    bearing.join(", ")
                )
            }
        }
        MarkerState::Pending => "**Markers:** requested but NOT yet confirmed present. Do not go \
             looking for them yet; the durable worker retries and this line changes when the \
             executor confirms."
            .to_string(),
        MarkerState::Failed => format!(
            "**Markers:** could not be projected into your checkout ({}). Read both sides below \
             instead — they carry the same information.",
            session
                .marker_diagnostic
                .as_deref()
                .unwrap_or("no diagnostic recorded")
        ),
        MarkerState::NotMaterialized => {
            "**Markers:** not materialized. Read both sides of the merge below.".to_string()
        }
    }
}

fn coordinates_block(session: &ConflictSession) -> String {
    let field = |value: &Option<String>| value.clone().unwrap_or_else(|| "unavailable".to_string());
    format!(
        "| side | commit |\n|---|---|\n| base (fork point) | `{}` |\n| ours (your branch) | \
         `{}` |\n| theirs (incoming) | `{}` |\n",
        field(&session.base),
        field(&session.ours),
        field(&session.theirs)
    )
}

fn file_table(title: &str, files: &[&SessionFile]) -> String {
    if files.is_empty() {
        return String::new();
    }
    let shown = files.len().min(MAX_FILE_ROWS);
    let mut out = format!(
        "\n## {title} ({})\n\n| file | change |\n|---|---|\n",
        files.len()
    );
    for file in files.iter().take(shown) {
        out.push_str(&format!("| `{}` | {} |\n", file.path, file.status));
    }
    if files.len() > shown {
        out.push_str(&format!(
            "\n… and {} more.\n",
            files.len().saturating_sub(shown)
        ));
    }
    out
}

fn render_summary(
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    session: &ConflictSession,
) -> String {
    let base = format!("cairn://p/{project}/{number}/{exec_seq}/{node_id}/rebase");
    let conflicting: Vec<&SessionFile> = session.conflicting().collect();
    let clean: Vec<&SessionFile> = session.clean_on_retry().collect();

    // A session written by a different build may mean something else by these
    // columns. Say so rather than interpreting it with this build's rules.
    let version_note = if session.version_is_current() {
        String::new()
    } else {
        format!(
            "\n⚠️ This session was recorded by a different build (diagnostic version {}, this build \
             reads {}). Its identity and file list are shown as stored; treat the interpretation \
             below with suspicion, and prefer requesting a fresh replay.\n",
            session.diagnostic_version,
            crate::orchestrator::conflict_session::CONFLICT_DIAGNOSTIC_VERSION
        )
    };

    let (condition, replay_payload) = if session.is_base_drift() {
        (
            "**Condition: base drift.** Every conflicting file is already byte-identical between your \
             branch and the new base, so there is nothing to merge — what is stale is your branch's \
             ANCESTRY. Editing files will not clear this, and you cannot repair it from your checkout: \
             slot refs are downstream exports of the runner's private jj store. Request the replay \
             below.",
            "action:\"replay\"",
        )
    } else {
        (
            "**Condition: content conflict.** The two sides genuinely disagree. Read both versions of \
             each conflicting file, write the merged result with ordinary edits, and commit it on your \
             branch. Then request the replay below so the branch actually moves onto the base — \
             resolving the content does not by itself change your branch's ancestry.",
            "action:\"replay\",resolution:\"take-committed-tip\"",
        )
    };

    format!(
        "# Rebase session for `{branch}`\n{version_note}\n{incoming}, and the automatic replay of your branch \
         onto it recorded a conflict, so it was rolled back. Your branch is untouched and on its \
         own content; nothing was lost.\n\n{condition}\n\n{markers}\n\n## Three-way \
         coordinates\n\n{coords}\nThese are immutable commits. Read any file as of either side \
         with `?branch=<commit>`, or read the whole side as a patch:\n\n- `{base}?view=base-ours` \
         — what your branch did\n- `{base}?view=base-theirs` — what arrived\n\nBoth accept \
         `file=PATH`, `offset=N`, and `limit=N`.\n{conflicting_table}{clean_table}\n## Next \
         action\n\n```\nwrite({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",payload:{{{replay_payload}}}}}]}})\n```\n\nThis \
         asks the store to replay your branch onto `{dest}`. It is the only way the branch's \
         ancestry moves — never rebase, reset, or force-push by hand. A clean replay publishes the \
         branch and closes this session; a conflicting one refreshes this page.\n\nSession \
         fingerprint `{fingerprint}`, recorded {recorded}.\n",
        branch = session.bookmark,
        incoming = incoming_line(session),
        markers = marker_line(session),
        coords = coordinates_block(session),
        conflicting_table = file_table("Conflicting files, yours to merge", &conflicting),
        clean_table = file_table(
            "Also arriving with this change, cleanly on replay",
            &clean
        ),
        dest = session.destination_commit,
        fingerprint = session.fingerprint(),
        recorded = crate::clock::stamp(session.updated_at)
            .unwrap_or_else(|| "at an unrecorded time".to_string()),
        replay_payload = replay_payload,
    )
}

fn render_side(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    session: &ConflictSession,
    request: &RebaseRequest<'_>,
) -> String {
    let (label, to) = match request.view {
        RebaseView::BaseTheirs => ("base→theirs (what arrived)", session.theirs.as_deref()),
        _ => ("base→ours (what your branch did)", session.ours.as_deref()),
    };
    let view_key = match request.view {
        RebaseView::BaseTheirs => "base-theirs",
        _ => "base-ours",
    };
    let uri = format!("cairn://p/{project}/{number}/{exec_seq}/{node_id}/rebase");

    let (Some(from), Some(to)) = (session.base.as_deref(), to) else {
        return format!(
            "# {label}\n\nThis side cannot be rendered: the session did not record both \
             coordinates for it. The stored identity and file inventory are still intact — read \
             `{uri}` for them. Nothing here has been substituted from your current tree, which \
             describes a different range."
        );
    };

    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = std::path::PathBuf::from(&session.store_path);
    let patch = match crate::jj::merge_side_patch(&jj, &store, from, to, request.file) {
        Ok(patch) => patch,
        Err(error) => {
            return format!(
                "# {label}\n\nThis side is unavailable: {error}\n\nThe conflict's identity and \
                 complete file inventory are still recorded — read `{uri}` for them. This read \
                 deliberately does NOT fall back to your current tree: that describes a different \
                 range and would answer a question you did not ask."
            );
        }
    };

    let lines: Vec<&str> = patch.lines().collect();
    let total = lines.len();
    if total == 0 {
        return format!("# {label}\n\n`{from}` → `{to}` — no changes on this side.");
    }
    let start = request.offset.min(total);
    let end = start.saturating_add(request.limit).min(total);
    let body = lines[start..end].join("\n");

    let scope = request
        .file
        .map(|file| format!(" scoped to `{file}`"))
        .unwrap_or_default();
    let mut out = format!("# {label}\n\n`{from}` → `{to}`{scope}\n\n```diff\n{body}\n```\n");
    if end < total {
        let file_param = request
            .file
            .map(|file| format!("&file={file}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "\n[lines {}–{} of {} — continue: {uri}?view={view_key}&offset={}&limit={}{file_param}]\n",
            start + 1,
            end,
            total,
            end,
            request.limit
        ));
    } else {
        out.push_str(&format!("\n[lines {}–{} of {}]\n", start + 1, end, total));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> Vec<QueryParam> {
        pairs
            .iter()
            .map(|(key, value)| QueryParam {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    /// An unknown key is a typo or a wrong mental model, and either way silently
    /// ignoring it serves a page the caller did not ask for.
    #[test]
    fn an_unknown_query_key_is_refused_by_name() {
        let typo = params(&[("vue", "base-ours")]);
        let error = parse_rebase_request(&typo).unwrap_err();
        assert!(error.contains("vue"), "{error}");
        assert!(error.contains("view"), "names what is accepted: {error}");
    }

    #[test]
    fn an_unknown_view_is_refused() {
        let bad = params(&[("view", "both")]);
        assert!(parse_rebase_request(&bad).is_err());
    }

    /// The two sides page independently. Scoping to a file only means something
    /// against a side, so asking for it on the summary is a mistake worth naming
    /// rather than quietly dropping.
    #[test]
    fn file_scoping_requires_a_side() {
        let bare_file = params(&[("file", "a.rs")]);
        assert!(parse_rebase_request(&bare_file).is_err());

        let scoped = params(&[("view", "base-theirs"), ("file", "a.rs")]);
        let request = parse_rebase_request(&scoped).unwrap();
        assert_eq!(request.view, RebaseView::BaseTheirs);
        assert_eq!(request.file, Some("a.rs"));
    }

    /// The cap is the server's, not the caller's. A request for a million lines
    /// is answered with the ceiling rather than honored.
    #[test]
    fn the_server_caps_the_page_size_whatever_is_asked_for() {
        let huge = params(&[("view", "base-ours"), ("limit", "100000")]);
        assert_eq!(parse_rebase_request(&huge).unwrap().limit, MAX_PATCH_LINES);

        let zero = params(&[("view", "base-ours"), ("limit", "0")]);
        assert_eq!(
            parse_rebase_request(&zero).unwrap().limit,
            1,
            "a zero-line page is not a page"
        );

        let bare = params(&[("view", "base-ours")]);
        let request = parse_rebase_request(&bare).unwrap();
        assert_eq!(request.limit, DEFAULT_PATCH_LINES);
        assert_eq!(request.offset, 0);
    }

    #[test]
    fn a_non_numeric_window_is_refused_rather_than_defaulted() {
        let bad = params(&[("view", "base-ours"), ("offset", "soon")]);
        assert!(parse_rebase_request(&bad).is_err());
    }
}
