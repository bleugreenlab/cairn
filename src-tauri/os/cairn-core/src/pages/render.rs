//! The page resources as agents read them.

use cairn_common::query::QueryParam;
use serde_json::Value;

use super::headless::{PageReadAnswerer, RenderLimits, RenderOutcome, RenderRequest, RenderedPage};
use super::index::{list_project_pages, PageIndexFilter};
use super::log::{list as list_calls, PageCallOutcome, PageCallVerb};
use super::read::{is_page_read, PageReader};
use super::store::{list_owner_pages, load_page, PageAddress, PageOwner, PageRow};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;

const INDEX_DEFAULT_LIMIT: usize = 50;
const INDEX_MAX_LIMIT: usize = 200;
const LOG_DEFAULT_LIMIT: usize = 50;
const LOG_MAX_LIMIT: usize = 500;

fn value<'a>(params: &'a [QueryParam], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|param| param.key == key)
        .map(|param| param.value.as_str())
        .filter(|value| !value.is_empty())
}

/// What a read of one page asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageView {
    /// The page rendered as text: what the operator sees.
    Rendered,
    /// Exactly what is stored.
    Source,
}

fn page_view(params: &[QueryParam]) -> Result<PageView, String> {
    if params.iter().any(|param| param.key == "screenshot") {
        return Err(
            "?screenshot is a later slice; read the page as text (the default), or open it in cairn:~/browser"
                .to_string(),
        );
    }
    match value(params, "format") {
        None | Some("text") => Ok(PageView::Rendered),
        Some("json") => Ok(PageView::Source),
        Some(other) => Err(format!(
            "page format must be text (the rendered page, the default) or json (its stored source), not {other}"
        )),
    }
}

fn wants_json(params: &[QueryParam], what: &str) -> Result<bool, String> {
    match value(params, "format") {
        None => Ok(false),
        Some("json") => Ok(true),
        Some(other) => Err(format!("{what} format must be json, not {other}")),
    }
}

fn limit(params: &[QueryParam], default: usize, max: usize) -> Result<usize, String> {
    match value(params, "limit") {
        None => Ok(default),
        Some(raw) => match raw.parse::<usize>() {
            Ok(n) if n >= 1 => Ok(n.min(max)),
            _ => Err(format!("limit must be a positive integer, not {raw}")),
        },
    }
}

fn str_field<'a>(data: &'a Value, key: &str) -> Option<&'a str> {
    data.get(key).and_then(Value::as_str)
}

/// `.../pages/{name}`: the page rendered as text (what the operator sees), or
/// `?format=json` for exactly what is stored; `?version=N` for an earlier
/// version.
pub(crate) async fn read_page(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    db: &LocalDb,
    address: &PageAddress,
    params: &[QueryParam],
) -> String {
    let view = match page_view(params) {
        Ok(view) => view,
        Err(error) => return error,
    };
    if view == PageView::Rendered && is_page_read(request) {
        return "a page cannot read a page's rendered view; read it with ?format=json for its source"
            .to_string();
    }
    let version = match value(params, "version") {
        None => None,
        Some(raw) => match raw.parse::<i32>() {
            Ok(n) if n >= 1 => Some(n),
            _ => return format!("version must be a positive integer, not {raw}"),
        },
    };
    let uri = address.uri();
    let (owner, row) = match load_page(db, address, version).await {
        Ok(Some(found)) => found,
        Ok(None) => {
            return match version {
                Some(version) => format!("{uri} has no version {version}"),
                None => format!(
                    "No page at {uri}. Write one with mode `create`; read {} for the pages here.",
                    cairn_common::uri::build_node_pages_uri(
                        &address.project,
                        address.number,
                        address.exec_seq,
                        &address.node_id
                    )
                ),
            }
        }
        Err(error) => return error,
    };
    if view == PageView::Source {
        return serde_json::to_string_pretty(&row.data)
            .unwrap_or_else(|error| format!("Failed to render page: {error}"));
    }
    let latest = if version.is_some() {
        load_page(db, address, None)
            .await
            .ok()
            .flatten()
            .map_or(row.version, |(_, latest)| latest.version)
    } else {
        row.version
    };

    let project_id =
        match crate::mcp::handlers::run_context::project_id_by_key(db, &address.project).await {
            Ok(id) => id,
            Err(error) => return error,
        };
    let reader = PageReader::for_request(orch, request).await;
    let rendered = super::headless::render(
        orch,
        RenderRequest {
            address,
            project_id: &project_id,
            row: &row,
            reader,
        },
        RenderLimits::default(),
        &PageReadAnswerer,
    )
    .await;
    rendered_body(
        address,
        &owner,
        &row,
        latest,
        &rendered,
        RenderLimits::default(),
    )
}

/// The rendered page, then what it could not show, then where it came from.
fn rendered_body(
    address: &PageAddress,
    owner: &PageOwner,
    row: &PageRow,
    latest: i32,
    rendered: &RenderedPage,
    limits: RenderLimits,
) -> String {
    let uri = address.uri();
    let mut sections: Vec<String> = Vec::new();
    match &rendered.outcome {
        RenderOutcome::Settled | RenderOutcome::Deadline { .. } => {
            // The page's text is agent-authored output headed for a model's
            // context: scrub it as the untrusted payload it is.
            let policy = cairn_common::security::sanitize::RedactionPolicy::default();
            let mut markdown = rendered.markdown.clone();
            cairn_common::security::sanitize::Sanitizer::structural(&policy)
                .text_in_place(&mut markdown);
            if markdown.trim().is_empty() {
                markdown = "_The page rendered no text._".to_string();
            }
            sections.push(markdown);
        }
        RenderOutcome::Failed { message }
        | RenderOutcome::Stopped { message }
        | RenderOutcome::Refused { message } => sections.push(message.clone()),
    }
    if !rendered.refused.is_empty() {
        let mut list = String::from("### Refused reads\n");
        for refusal in &rendered.refused {
            for line in refusal.lines() {
                let line: String = line.chars().take(500).collect();
                list.push_str(&format!("\n- {line}"));
            }
        }
        sections.push(list);
    }
    if !rendered.diagnostics.is_empty() {
        let mut list = String::from("### Diagnostics\n");
        for diagnostic in &rendered.diagnostics {
            list.push_str(&format!("\n- {diagnostic}"));
        }
        sections.push(list);
    }

    let owner = match owner {
        PageOwner::Thread { .. } => {
            format!("thread `{}`", address.thread_name().unwrap_or_default())
        }
        PageOwner::Node { .. } => format!("draft on {}", address.home_uri()),
    };
    let promoted = row.data.get("promoted").and_then(Value::as_bool) == Some(true);
    let seconds = rendered.elapsed.as_secs_f64();
    let render = match &rendered.outcome {
        RenderOutcome::Settled => format!(
            "rendered in {seconds:.1} s, {} reads · settled",
            rendered.reads
        ),
        RenderOutcome::Deadline { still_reading } => {
            let mut text = format!(
                "rendered at the {} s deadline, {} reads",
                limits.settle.as_secs(),
                rendered.reads
            );
            if !still_reading.is_empty() {
                let list: Vec<String> =
                    still_reading.iter().map(|uri| format!("`{uri}`")).collect();
                text.push_str(&format!("; still reading: {}", list.join(", ")));
            }
            text
        }
        RenderOutcome::Failed { .. } => {
            format!("failed after {seconds:.1} s, {} reads", rendered.reads)
        }
        RenderOutcome::Stopped { .. } => {
            format!("stopped after {seconds:.1} s, {} reads", rendered.reads)
        }
        RenderOutcome::Refused { .. } => "not rendered".to_string(),
    };
    sections.push(format!(
        "---\nv{} of {latest} · written {} · {owner} · {} · {render}\nSource: {uri}?format=json · Call log: {uri}/log",
        row.version,
        crate::clock::age(row.updated_at),
        if promoted { "promoted" } else { "not promoted" },
    ));
    let mut out = sections.join("\n\n");
    out.push('\n');
    out
}

/// `.../pages`: the pages a thread (or, as drafts, an issue node) owns.
pub(crate) async fn read_pages(
    db: &LocalDb,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> String {
    let address = PageAddress {
        project: project.to_string(),
        number,
        exec_seq,
        node_id: node_id.to_string(),
        name: String::new(),
    };
    let home = address.home_uri();
    let pages = match list_owner_pages(db, &address).await {
        Ok(pages) => pages,
        Err(error) => return error,
    };
    let mut out = format!("# Pages — {home}\n\n");
    if address.thread_name().is_none() {
        out.push_str(
            "Pages on an issue node are drafts: they render on the node and are never listed in the project's Pages index.\n\n",
        );
    }
    if pages.is_empty() {
        out.push_str(&format!(
            "No pages. Write one with mode `create` at {home}/pages/<name>.\n"
        ));
        return out;
    }
    for (name, row) in pages {
        let uri = PageAddress {
            name: name.clone(),
            ..address.clone()
        }
        .uri();
        let title = str_field(&row.data, "title").unwrap_or(&name);
        let promoted = row.data.get("promoted").and_then(Value::as_bool) == Some(true);
        out.push_str(&format!(
            "- [{name}]({uri}) · {title} · v{} · {} · written {}\n",
            row.version,
            if promoted { "promoted" } else { "not promoted" },
            crate::clock::age(row.updated_at)
        ));
    }
    out
}

fn actor_label(actor: &Value) -> String {
    match str_field(actor, "kind") {
        Some("human") => format!(
            "{} ({})",
            str_field(actor, "subject").unwrap_or("unknown"),
            str_field(actor, "issuer").unwrap_or("unknown issuer")
        ),
        Some("desktop_operator") => "the desktop operator".to_string(),
        Some("agent") => format!("agent {}", str_field(actor, "node").unwrap_or("unknown")),
        _ => "anonymous".to_string(),
    }
}

/// `.../pages/{name}/log`: what open frames of the page did, newest first.
pub(crate) async fn read_page_log(
    local: &LocalDb,
    db: &LocalDb,
    address: &PageAddress,
    params: &[QueryParam],
) -> String {
    let limit = match limit(params, LOG_DEFAULT_LIMIT, LOG_MAX_LIMIT) {
        Ok(limit) => limit,
        Err(error) => return error,
    };
    let verb = match value(params, "verb") {
        None => None,
        Some(raw) => match PageCallVerb::parse(raw) {
            Some(verb) => Some(verb),
            None => {
                return format!(
                    "verb must be read|subscribe|refresh|diag|navigation|write|service, not {raw}"
                )
            }
        },
    };
    let outcome = match value(params, "outcome") {
        None => None,
        Some(raw) => match PageCallOutcome::parse(raw) {
            Some(outcome) => Some(outcome),
            None => return format!("outcome must be ok|refused|error, not {raw}"),
        },
    };
    let uri = address.uri();
    match load_page(db, address, None).await {
        Ok(Some(_)) => {}
        Ok(None) => return format!("No page at {uri}"),
        Err(error) => return error,
    }
    let calls = match list_calls(local, &uri, limit, verb, outcome).await {
        Ok(calls) => calls,
        Err(error) => return error,
    };
    let mut out = format!("# Call log — {uri}\n\n");
    if calls.is_empty() {
        out.push_str("No calls recorded.\n");
        return out;
    }
    out.push_str(&format!("{} call(s), newest first\n\n", calls.len()));
    for call in calls {
        let at = crate::clock::stamp(call.created_at / 1000).unwrap_or_default();
        out.push_str(&format!(
            "- {at} · {} · {} · v{} · viewer: {}\n",
            call.verb,
            call.outcome,
            call.page_version,
            actor_label(&call.actor)
        ));
        if !call.targets.is_empty() {
            let targets: Vec<String> = call.targets.iter().map(|t| format!("`{t}`")).collect();
            out.push_str(&format!("  targets: {}\n", targets.join(", ")));
        }
        if !call.detail.is_empty() {
            for line in call.detail.lines() {
                out.push_str(&format!("  > {line}\n"));
            }
        }
    }
    out
}

/// `cairn://p/{project}/pages`: the project's Pages index.
pub(crate) async fn read_project_pages(
    db: &LocalDb,
    project: &str,
    params: &[QueryParam],
) -> String {
    let json = match wants_json(params, "pages") {
        Ok(json) => json,
        Err(error) => return error,
    };
    let limit = match limit(params, INDEX_DEFAULT_LIMIT, INDEX_MAX_LIMIT) {
        Ok(limit) => limit,
        Err(error) => return error,
    };
    let project_id = match crate::mcp::handlers::run_context::project_id_by_key(db, project).await {
        Ok(id) => id,
        Err(error) => return error,
    };
    let filter = PageIndexFilter {
        thread: value(params, "thread").map(str::to_string),
        search: value(params, "search").map(str::to_string),
        limit: Some(limit),
    };
    let pages = match list_project_pages(db, &project_id, &filter).await {
        Ok(pages) => pages,
        Err(error) => return error,
    };
    if json {
        return serde_json::to_string_pretty(&pages)
            .unwrap_or_else(|error| format!("Failed to render pages: {error}"));
    }
    let mut out = format!("# Pages — {}\n\n", canonical(project));
    if pages.is_empty() {
        out.push_str("No promoted pages. A thread lists one by writing `promoted: true` on it.\n");
        return out;
    }
    for page in pages {
        out.push_str(&format!(
            "- [{}]({}) · {} · thread `{}` · v{} · updated {}\n",
            page.title, page.uri, page.name, page.thread_name, page.version, page.updated_at
        ));
        if let Some(description) = page.description {
            out.push_str(&format!("  {description}\n"));
        }
    }
    out
}

fn canonical(project: &str) -> String {
    cairn_common::uri::canonical_project(project)
}
