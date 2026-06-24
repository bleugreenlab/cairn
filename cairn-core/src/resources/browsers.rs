//! Browser resource rendering and scope resolution (Tauri-free).
//!
//! Unlike terminals, browser reads do NOT route through `read_resource`: the
//! durable url/title/status in the `job_browsers` row (kept fresh by the
//! app-side navigation handlers) is the read surface. No page content this
//! slice. Scope resolution is shared by the read renderer and the write
//! dispatch arms.

use std::time::Duration;

use cairn_common::query::QueryParam;
use cairn_common::read::ImageBlock;
use cairn_common::uri::CairnResource;
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;

use crate::browsers::{
    ensure_open_browser, find_browser_by_scope_and_slug, get_browser, BridgeFormat, BridgeRequest,
    BridgeResponse, BrowserCommand, BrowserNavEvent, BrowserScope, ConsoleEntry,
    InteractiveElement, NavPhase, NetworkEntry, STATUS_OPEN,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// Default budget for an extract/interaction bridge round-trip with the page.
/// A `waitFor` action passes a larger budget that must exceed its own timeout.
pub(crate) const BRIDGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Resolve a browser resource URI to its owning scope plus slug. Node/task
/// browsers resolve to the backing job; project browsers to the project row.
pub(crate) async fn resolve_browser_scope(
    db: &LocalDb,
    resource: &CairnResource,
) -> Result<(BrowserScope, String), String> {
    let resource = resource.clone();
    db.read(|conn| {
        Box::pin(async move {
            match resource {
                CairnResource::NodeBrowser {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    slug,
                } => {
                    let job_id =
                        find_node_job_id(conn, &project, number, exec_seq, &node_id)
                            .await?
                            .ok_or_else(|| {
                                DbError::Row(format!(
                                    "No node job found for browser target {project}-{number}/{exec_seq}/{node_id}"
                                ))
                            })?;
                    Ok((BrowserScope::Job(job_id), slug))
                }
                CairnResource::TaskBrowser {
                    project,
                    number,
                    exec_seq,
                    node_id,
                    task_name,
                    slug,
                } => {
                    let job_id = find_task_job_id(
                        conn, &project, number, exec_seq, &node_id, &task_name,
                    )
                    .await?
                    .ok_or_else(|| {
                        DbError::Row(format!(
                            "No task job found for browser target {project}-{number}/{exec_seq}/{node_id}/task/{task_name}"
                        ))
                    })?;
                    Ok((BrowserScope::Job(job_id), slug))
                }
                CairnResource::ProjectBrowser { project, slug } => {
                    let lookup_key = project.to_uppercase();
                    let mut rows = conn
                        .query(
                            "SELECT id FROM projects WHERE key = ?1 LIMIT 1",
                            (lookup_key.as_str(),),
                        )
                        .await?;
                    let row = rows.next().await?.ok_or_else(|| {
                        DbError::Row(format!("No project found for key {project}"))
                    })?;
                    Ok((BrowserScope::Project(row.text(0)?), slug))
                }
                _ => Err(DbError::Row("Resource is not a browser URI".to_string())),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn find_node_job_id(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_id: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT j.id
            FROM jobs j
            JOIN issues i ON j.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            JOIN executions e ON j.execution_id = e.id
            WHERE p.key = ?1
              AND i.number = ?2
              AND e.seq = ?3
              AND j.parent_job_id IS NULL
              AND j.uri_segment = ?4
            LIMIT 1
            ",
            (lookup_key.as_str(), issue_number, exec_seq, node_id),
        )
        .await?;
    rows.next().await?.map(|row| row.text(0)).transpose()
}

async fn find_task_job_id(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    parent_node_id: &str,
    task_name: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT child.id
            FROM jobs parent
            JOIN jobs child ON child.parent_job_id = parent.id
            JOIN issues i ON parent.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            JOIN executions e ON parent.execution_id = e.id
            WHERE p.key = ?1
              AND i.number = ?2
              AND e.seq = ?3
              AND parent.parent_job_id IS NULL
              AND parent.uri_segment = ?4
              AND child.uri_segment = ?5
            LIMIT 1
            ",
            (
                lookup_key.as_str(),
                issue_number,
                exec_seq,
                parent_node_id,
                task_name,
            ),
        )
        .await?;
    rows.next().await?.map(|row| row.text(0)).transpose()
}

/// How a browser read should be rendered. The default is live page content
/// (markdown/text via the content-script bridge); `?screenshot` switches to a
/// host-native image capture; `?console`/`?network` return the page's captured
/// runtime buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserReadMode {
    Content(BridgeFormat),
    Screenshot,
    Console { limit: Option<u32> },
    Network { limit: Option<u32> },
    Interactive { limit: Option<u32> },
}

/// Parse the query parameters a browser read accepts: `?format=markdown|text`
/// (default markdown) for live content, `?screenshot` for a native image
/// capture, or `?console`/`?network` for the page's captured runtime buffers.
/// The content/screenshot/console/network facets are mutually exclusive.
///
/// `?offset` and `?limit` are the universal read line-window: the read view
/// layer consumes them centrally (`read.rs`) to window any text body and to
/// emit the `continue:` footer, so a long content read pages like every other
/// resource. On `?console`/`?network` `limit` additionally caps the page-side
/// buffer the producer returns (pre-existing double meaning). Any other
/// parameter is rejected.
pub(crate) fn parse_browser_read_mode(params: &[QueryParam]) -> Result<BrowserReadMode, String> {
    let mut format = BridgeFormat::Markdown;
    let mut screenshot = false;
    let mut console = false;
    let mut network = false;
    let mut interactive = false;
    let mut limit: Option<u32> = None;
    for param in params {
        match param.key.as_str() {
            "screenshot" => screenshot = true,
            "console" => console = true,
            "network" => network = true,
            "interactive" => interactive = true,
            // `offset` is a universal line-window applied centrally by the read
            // view layer (read.rs consumes it before the producer runs); accept
            // and ignore it here so a paged content read — and the `continue:`
            // footer URI the view itself emits — both parse rather than erroring.
            "offset" => {}
            "limit" => {
                let value = param.value.trim();
                limit = Some(value.parse::<u32>().map_err(|_| {
                    format!("Invalid browser read limit '{value}'; expected a positive integer")
                })?);
            }
            "format" => {
                format = match param.value.as_str() {
                    "" | "markdown" => BridgeFormat::Markdown,
                    "text" => BridgeFormat::Text,
                    other => {
                        return Err(format!(
                            "Unsupported browser format '{other}'; expected markdown or text"
                        ))
                    }
                };
            }
            other => {
                return Err(format!(
                    "Unsupported query parameter '{other}' on browser resources; only ?format=markdown|text, ?screenshot, ?console, ?network, ?interactive, ?offset, and ?limit (a line window for content; a buffer cap for ?console/?network/?interactive) are accepted"
                ))
            }
        }
    }
    let selected =
        u8::from(screenshot) + u8::from(console) + u8::from(network) + u8::from(interactive);
    if selected > 1 {
        return Err(
            "Browser read facets ?screenshot, ?console, ?network, and ?interactive are mutually exclusive; pick one"
                .to_string(),
        );
    }
    if console {
        Ok(BrowserReadMode::Console { limit })
    } else if network {
        Ok(BrowserReadMode::Network { limit })
    } else if interactive {
        Ok(BrowserReadMode::Interactive { limit })
    } else if screenshot {
        Ok(BrowserReadMode::Screenshot)
    } else {
        // For content/screenshot `limit` is a view line-window consumed
        // centrally; the content producer ignores it here.
        Ok(BrowserReadMode::Content(format))
    }
}

/// Send a bridge request to a live browser webview and await the matching
/// response (keyed by a freshly generated request id). Mirrors the orchestrator's
/// other blocking-resume channels: subscribe BEFORE sending so a fast page reply
/// cannot race the awaiter. Returns an error when no webview layer is wired
/// (headless/server) or the page does not respond within `timeout`.
pub(crate) async fn browser_bridge_roundtrip(
    orch: &Orchestrator,
    browser_id: &str,
    request: &BridgeRequest,
    timeout: Duration,
) -> Result<BridgeResponse, String> {
    crate::browsers::dispatch_browser_bridge(orch, browser_id, request, timeout).await
}

/// Await the next navigation event for `browser_id` up to `timeout`, draining a
/// subscription the caller created BEFORE the triggering action (so a fast nav
/// cannot race the awaiter). Returns the new url, or `None` on timeout/channel
/// close. When `require_finished`, only a `Finished`-phase event resolves it
/// (waitForLoad); otherwise the first Started OR Finished resolves it (click
/// nav-confirmation / waitForNavigation).
pub(crate) async fn await_browser_nav(
    rx: &mut tokio::sync::broadcast::Receiver<BrowserNavEvent>,
    browser_id: &str,
    timeout: Duration,
    require_finished: bool,
) -> Option<String> {
    tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Ok(event)
                    if event.browser_id == browser_id
                        && (!require_finished || event.phase == NavPhase::Finished) =>
                {
                    return Some(event.url)
                }
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// The host's reply to a [`BrowserCommand::Capture`], parsed from the
/// reqId-keyed payload string. `data_b64` carries the PNG on success.
#[derive(Debug, Clone, Default, Deserialize)]
struct CaptureReply {
    #[serde(default)]
    ok: bool,
    #[serde(default, rename = "dataB64")]
    data_b64: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// Request a native screenshot of a live browser webview and await the matching
/// reply (keyed by a freshly generated request id). A near-twin of
/// [`browser_bridge_roundtrip`], but the producer is the host's snapshot
/// completion handler rather than the page content script, so it works on
/// `about:blank` and pages where the content script is blocked. Returns the
/// base64 PNG on success. Same headless fallback: errors cleanly when no webview
/// layer is wired.
pub(crate) async fn browser_capture_roundtrip(
    orch: &Orchestrator,
    browser_id: &str,
    timeout: Duration,
) -> Result<String, String> {
    let tx = orch
        .browser_command_tx
        .as_ref()
        .ok_or_else(|| "no live webview on this host (headless/server)".to_string())?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut rx = orch.browser_bridge_responses.subscribe();
    tx.send(BrowserCommand::Capture {
        id: browser_id.to_string(),
        request_id: request_id.clone(),
    })
    .map_err(|error| format!("failed to dispatch capture command: {error}"))?;

    let payload = tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Ok((resp_id, payload)) if resp_id == request_id => return Ok(payload),
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return Err("browser capture channel closed".to_string()),
            }
        }
    })
    .await
    .map_err(|_| {
        "browser capture timed out waiting for the host snapshot (is the webview live?)".to_string()
    })??;

    let reply: CaptureReply = serde_json::from_str(&payload)
        .map_err(|error| format!("invalid capture reply from host: {error}"))?;
    if reply.ok {
        reply
            .data_b64
            .ok_or_else(|| "capture reply was ok but carried no image data".to_string())
    } else {
        Err(reply
            .error
            .unwrap_or_else(|| "unknown capture error".to_string()))
    }
}

/// The host's reply to a [`BrowserCommand::ClearData`], parsed from the
/// reqId-keyed payload string.
#[derive(Debug, Clone, Default, Deserialize)]
struct ClearReply {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

/// Clear the live webview's website data via the host-native primitive and await
/// the host's `{ ok, error }` reply (keyed by a freshly generated request id). A
/// near-twin of [`browser_capture_roundtrip`]: the producer is the host's clear
/// completion rather than the page content script, so it is independent of the
/// content-script bridge and works on `about:blank`. Same headless fallback:
/// errors cleanly when no webview layer is wired. `kinds` are lowercase bucket
/// names (`cookies`/`cache`/`storage`); empty means the host default.
pub(crate) async fn browser_clear_data_roundtrip(
    orch: &Orchestrator,
    browser_id: &str,
    kinds: Vec<String>,
    timeout: Duration,
) -> Result<(), String> {
    let tx = orch
        .browser_command_tx
        .as_ref()
        .ok_or_else(|| "no live webview on this host (headless/server)".to_string())?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut rx = orch.browser_bridge_responses.subscribe();
    tx.send(BrowserCommand::ClearData {
        id: browser_id.to_string(),
        request_id: request_id.clone(),
        kinds,
    })
    .map_err(|error| format!("failed to dispatch clear-data command: {error}"))?;

    let payload = tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Ok((resp_id, payload)) if resp_id == request_id => return Ok(payload),
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return Err("browser clear channel closed".to_string()),
            }
        }
    })
    .await
    .map_err(|_| {
        "browser clear timed out waiting for the host (is the webview live?)".to_string()
    })??;

    let reply: ClearReply = serde_json::from_str(&payload)
        .map_err(|error| format!("invalid clear reply from host: {error}"))?;
    if reply.ok {
        Ok(())
    } else {
        Err(reply
            .error
            .unwrap_or_else(|| "unknown clear error".to_string()))
    }
}

/// Render a browser screenshot read: the url/title/status header text plus, when
/// a webview is live, a PNG [`ImageBlock`] from the host-native capture. The
/// caller (the resource read path) lifts the image into the read envelope so the
/// agent sees the rendered page. Mirrors [`render_browser`]'s zero-ceremony row
/// ensure and webview rehydrate.
pub(crate) async fn render_browser_screenshot(
    orch: &Orchestrator,
    resource: &CairnResource,
) -> (String, Option<ImageBlock>) {
    let db = &orch.db.local;
    let (scope, slug) = match resolve_browser_scope(db, resource).await {
        Ok(pair) => pair,
        Err(error) => return (error, None),
    };
    let browser = match find_browser_by_scope_and_slug(db, scope.clone(), &slug).await {
        Ok(Some(browser)) => browser,
        Ok(None) => {
            let now = chrono::Utc::now().timestamp();
            match ensure_open_browser(db, scope, &slug, None, now).await {
                Ok((browser, _)) => {
                    emit_browser_db_change(orch, "insert");
                    browser
                }
                Err(error) => return (error, None),
            }
        }
        Err(error) => return (error, None),
    };
    let header = format_browser_header(&browser);

    if orch.browser_command_tx.is_none() {
        return (
            format!("{header}\n\n(Screenshot is unavailable on this host: no webview layer.)"),
            None,
        );
    }
    if browser.status != STATUS_OPEN {
        return (
            format!("{header}\n\n(Browser is closed; no screenshot.)"),
            None,
        );
    }

    // Ensure the webview is live before the capture round-trip (rehydrate after a
    // restart wiped the registry; a live webview no-ops), exactly like the
    // content read in `render_browser`.
    if let Some(tx) = &orch.browser_command_tx {
        let _ = tx.send(BrowserCommand::Open {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
            url: browser.url.clone(),
        });
    }

    match browser_capture_roundtrip(orch, &browser.id, BRIDGE_TIMEOUT).await {
        Ok(data) => (
            header,
            Some(ImageBlock {
                mime_type: "image/png".to_string(),
                data,
            }),
        ),
        Err(error) => (
            format!("{header}\n\n(Could not capture screenshot: {error})"),
            None,
        ),
    }
}

/// Render a browser resource read: the live url/title/status from the row plus,
/// when a webview is live, an extraction of the current page content (markdown
/// from `outerHTML` via the shared htmd converter, or raw `innerText` for text).
pub(crate) async fn render_browser(
    orch: &Orchestrator,
    resource: &CairnResource,
    format: BridgeFormat,
) -> String {
    let db = &orch.db.local;
    let (scope, slug) = match resolve_browser_scope(db, resource).await {
        Ok(pair) => pair,
        Err(error) => return error,
    };
    // Zero-ceremony read: auto-create a blank row when none exists so the read
    // always succeeds (and the user's pane appears). The row is the read surface;
    // a live webview, when present, adds page content below.
    let browser = match find_browser_by_scope_and_slug(db, scope.clone(), &slug).await {
        Ok(Some(browser)) => browser,
        Ok(None) => {
            let now = chrono::Utc::now().timestamp();
            match ensure_open_browser(db, scope, &slug, None, now).await {
                Ok((browser, _)) => {
                    emit_browser_db_change(orch, "insert");
                    browser
                }
                Err(error) => return error,
            }
        }
        Err(error) => return error,
    };
    let header = format_browser_header(&browser);

    if orch.browser_command_tx.is_none() {
        return format!(
            "{header}\n\n(Live page content is unavailable on this host: no webview layer.)"
        );
    }
    if browser.status != STATUS_OPEN {
        return format!("{header}\n\n(Browser is closed; no live page content.)");
    }

    // Ensure the webview is live before the bridge round-trip. A host with a
    // command channel but a webview wiped by a restart rehydrates here (Open is
    // create-or-reuse), so the read retries against a live page rather than
    // timing out; a live webview no-ops.
    if let Some(tx) = &orch.browser_command_tx {
        let _ = tx.send(BrowserCommand::Open {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
            url: browser.url.clone(),
        });
    }

    match browser_bridge_roundtrip(
        orch,
        &browser.id,
        &BridgeRequest::Extract { format },
        BRIDGE_TIMEOUT,
    )
    .await
    {
        Ok(response) if response.ok => {
            let content = response.content.unwrap_or_default();
            let body = if response.format.as_deref() == Some("html") {
                crate::mcp::handlers::fetch_web::convert_body("text/html", content)
            } else {
                content
            };
            let body = body.trim();
            if body.is_empty() {
                format!("{header}\n\n(Page returned no content.)")
            } else {
                format!("{header}\n\n---\n\n{body}")
            }
        }
        Ok(response) => format!(
            "{header}\n\n(Could not read page content: {})",
            response
                .error
                .unwrap_or_else(|| "unknown error".to_string())
        ),
        Err(error) => format!("{header}\n\n(Could not read page content: {error})"),
    }
}

/// The outcome of resolving + ensuring a browser row for a live-data read
/// (console/network): either the live browser id and its header text, or an
/// early message to surface to the agent (resolve error, headless host, or a
/// closed pane). Mirrors the preamble [`render_browser`] runs inline.
enum LiveBrowser {
    Ready { id: String, header: String },
    Message(String),
}

async fn prepare_live_browser(orch: &Orchestrator, resource: &CairnResource) -> LiveBrowser {
    let db = &orch.db.local;
    let (scope, slug) = match resolve_browser_scope(db, resource).await {
        Ok(pair) => pair,
        Err(error) => return LiveBrowser::Message(error),
    };
    let browser = match find_browser_by_scope_and_slug(db, scope.clone(), &slug).await {
        Ok(Some(browser)) => browser,
        Ok(None) => {
            let now = chrono::Utc::now().timestamp();
            match ensure_open_browser(db, scope, &slug, None, now).await {
                Ok((browser, _)) => {
                    emit_browser_db_change(orch, "insert");
                    browser
                }
                Err(error) => return LiveBrowser::Message(error),
            }
        }
        Err(error) => return LiveBrowser::Message(error),
    };
    let header = format_browser_header(&browser);

    if orch.browser_command_tx.is_none() {
        return LiveBrowser::Message(format!(
            "{header}\n\n(Live page data is unavailable on this host: no webview layer.)"
        ));
    }
    if browser.status != STATUS_OPEN {
        return LiveBrowser::Message(format!(
            "{header}\n\n(Browser is closed; no live page data.)"
        ));
    }

    // Rehydrate the webview before the bridge round-trip (a restart may have
    // wiped the registry); a live webview no-ops. Same pattern as render_browser.
    if let Some(tx) = &orch.browser_command_tx {
        let _ = tx.send(BrowserCommand::Open {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
            url: browser.url.clone(),
        });
    }
    LiveBrowser::Ready {
        id: browser.id,
        header,
    }
}

/// Best-effort rehydrate of a known browser id's webview before a bridge
/// round-trip driven from the frontend (where the browser id is already known
/// and a header is not needed).
async fn rehydrate_browser(orch: &Orchestrator, browser_id: &str) {
    let Some(tx) = &orch.browser_command_tx else {
        return;
    };
    if let Ok(Some(browser)) = get_browser(&orch.db.local, browser_id).await {
        let _ = tx.send(BrowserCommand::Open {
            id: browser.id,
            label: browser.webview_label,
            url: browser.url,
        });
    }
}

async fn console_roundtrip(
    orch: &Orchestrator,
    browser_id: &str,
    limit: Option<u32>,
) -> Result<Vec<ConsoleEntry>, String> {
    let response = browser_bridge_roundtrip(
        orch,
        browser_id,
        &BridgeRequest::GetConsole { limit },
        BRIDGE_TIMEOUT,
    )
    .await?;
    if response.ok {
        Ok(response.logs.unwrap_or_default())
    } else {
        Err(response
            .error
            .unwrap_or_else(|| "unknown error reading console".to_string()))
    }
}

async fn network_roundtrip(
    orch: &Orchestrator,
    browser_id: &str,
    limit: Option<u32>,
) -> Result<Vec<NetworkEntry>, String> {
    let response = browser_bridge_roundtrip(
        orch,
        browser_id,
        &BridgeRequest::GetNetwork { limit },
        BRIDGE_TIMEOUT,
    )
    .await?;
    if response.ok {
        Ok(response.requests.unwrap_or_default())
    } else {
        Err(response
            .error
            .unwrap_or_else(|| "unknown error reading network".to_string()))
    }
}

/// Fetch a live browser's captured console buffer by id (frontend panel path).
/// Rehydrates the webview first, then round-trips. Headless hosts and pages
/// without the content script surface as errors.
pub async fn fetch_browser_console(
    orch: &Orchestrator,
    browser_id: &str,
    limit: Option<u32>,
) -> Result<Vec<ConsoleEntry>, String> {
    rehydrate_browser(orch, browser_id).await;
    console_roundtrip(orch, browser_id, limit).await
}

/// Fetch a live browser's captured network buffer by id (frontend panel path).
pub async fn fetch_browser_network(
    orch: &Orchestrator,
    browser_id: &str,
    limit: Option<u32>,
) -> Result<Vec<NetworkEntry>, String> {
    rehydrate_browser(orch, browser_id).await;
    network_roundtrip(orch, browser_id, limit).await
}

/// Render a browser console read: the url/title/status header plus the page's
/// captured `console.*` output and uncaught errors as a markdown list.
pub(crate) async fn render_browser_console(
    orch: &Orchestrator,
    resource: &CairnResource,
    limit: Option<u32>,
) -> String {
    let (id, header) = match prepare_live_browser(orch, resource).await {
        LiveBrowser::Ready { id, header } => (id, header),
        LiveBrowser::Message(message) => return message,
    };
    match console_roundtrip(orch, &id, limit).await {
        Ok(entries) => format!(
            "{header}\n\n---\n\n### Console ({} entries)\n\n{}",
            entries.len(),
            format_console_body(&entries)
        ),
        Err(error) => format!("{header}\n\n(Could not read console: {error})"),
    }
}

/// Render a browser network read: the url/title/status header plus the page's
/// captured request summaries as a compact markdown table.
pub(crate) async fn render_browser_network(
    orch: &Orchestrator,
    resource: &CairnResource,
    limit: Option<u32>,
) -> String {
    let (id, header) = match prepare_live_browser(orch, resource).await {
        LiveBrowser::Ready { id, header } => (id, header),
        LiveBrowser::Message(message) => return message,
    };
    match network_roundtrip(orch, &id, limit).await {
        Ok(entries) => format!(
            "{header}\n\n---\n\n### Network ({} requests)\n\n{}",
            entries.len(),
            format_network_body(&entries)
        ),
        Err(error) => format!("{header}\n\n(Could not read network: {error})"),
    }
}

/// Render a browser interactive read: the url/title/status header plus the
/// page's actionable elements, each a compact list item with its handle,
/// descriptor, context, and selector. Mirrors the console/network renderers.
pub(crate) async fn render_browser_interactive(
    orch: &Orchestrator,
    resource: &CairnResource,
    limit: Option<u32>,
) -> String {
    let (id, header) = match prepare_live_browser(orch, resource).await {
        LiveBrowser::Ready { id, header } => (id, header),
        LiveBrowser::Message(message) => return message,
    };
    let response = match browser_bridge_roundtrip(
        orch,
        &id,
        &BridgeRequest::ListInteractive { limit },
        BRIDGE_TIMEOUT,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return format!("{header}\n\n(Could not read interactive elements: {error})"),
    };
    if !response.ok {
        return format!(
            "{header}\n\n(Could not read interactive elements: {})",
            response
                .error
                .unwrap_or_else(|| "unknown error".to_string())
        );
    }
    let elements = response.elements.unwrap_or_default();
    format!(
        "{header}\n\n---\n\n### Interactive elements ({})\n\n{}",
        elements.len(),
        format_interactive_body(&elements)
    )
}

/// A short human label for an interactive element: its aria-label, else its
/// visible-text snippet, else its input name or link href.
fn interactive_label(el: &InteractiveElement) -> Option<String> {
    if let Some(aria) = el.meta.aria_label.as_deref() {
        let aria = aria.trim();
        if !aria.is_empty() {
            return Some(aria.to_string());
        }
    }
    let snippet = el.snippet.trim();
    if !snippet.is_empty() {
        return Some(snippet.chars().take(60).collect());
    }
    if let Some(name) = el.meta.input_name.as_deref() {
        return Some(name.to_string());
    }
    el.meta.href.clone()
}

/// Format the actionable elements as a compact markdown list:
/// `` - `e7` button "Sign In" — under heading "Account" · selector: `nav > button` ``
fn format_interactive_body(elements: &[InteractiveElement]) -> String {
    if elements.is_empty() {
        return "(No actionable elements found on this page.)".to_string();
    }
    let mut out = String::new();
    for el in elements {
        let tag = el.meta.tag.as_deref().unwrap_or("element");
        let mut line = format!("- `{}` {tag}", el.ref_);
        if let Some(label) = interactive_label(el) {
            line.push_str(&format!(" \"{label}\""));
        }
        if let Some(hint) = el.meta.context_hint.as_deref() {
            line.push_str(&format!(" — {hint}"));
        }
        line.push_str(&format!(" · selector: `{}`", el.selector));
        out.push_str(&line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

/// Format captured console entries as a markdown list. Each entry is one item
/// (`- **LEVEL** message`); a stack, when present, follows as indented lines.
fn format_console_body(entries: &[ConsoleEntry]) -> String {
    if entries.is_empty() {
        return "(No console output captured on this page.)".to_string();
    }
    let mut out = String::new();
    for entry in entries {
        let level = entry.level.to_uppercase();
        let message = entry.message.trim();
        out.push_str(&format!("- **{level}** {message}\n"));
        if let Some(stack) = &entry.stack {
            for line in stack.lines() {
                if !line.trim().is_empty() {
                    out.push_str(&format!("      {}\n", line.trim_end()));
                }
            }
        }
    }
    out.trim_end().to_string()
}

fn format_size(size: Option<i64>) -> String {
    match size {
        None => "–".to_string(),
        Some(bytes) if bytes < 1024 => format!("{bytes} B"),
        Some(bytes) if bytes < 1024 * 1024 => format!("{:.1} kB", bytes as f64 / 1024.0),
        Some(bytes) => format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0)),
    }
}

/// Format captured network summaries as a compact markdown table. A failed
/// request shows `ERR` in the status column and its error message inline.
fn format_network_body(entries: &[NetworkEntry]) -> String {
    if entries.is_empty() {
        return "(No network activity captured on this page yet.)".to_string();
    }
    let mut out = String::from(
        "| Method | Type | URL | Status | Duration | Size |\n|---|---|---|---|---|---|\n",
    );
    for entry in entries {
        let status = match (&entry.error, entry.status) {
            (Some(error), _) => format!("ERR ({})", error.trim()),
            (None, Some(code)) => code.to_string(),
            (None, None) => "–".to_string(),
        };
        let duration = match entry.duration_ms {
            Some(ms) => format!("{ms} ms"),
            None => "–".to_string(),
        };
        let kind = entry.kind.as_deref().unwrap_or("–");
        // Escape pipes so a URL with a query string never breaks the table.
        let url = entry.url.replace('|', "%7C");
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            entry.method,
            kind,
            url,
            status,
            duration,
            format_size(entry.size),
        ));
    }
    out.trim_end().to_string()
}

fn emit_browser_db_change(orch: &Orchestrator, action: &str) {
    if let Err(error) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_browsers", "action": action}),
    ) {
        log::error!("Failed to emit db-change event: {error}");
    }
}

fn format_browser_header(browser: &crate::browsers::JobBrowser) -> String {
    let url = browser.url.as_deref().unwrap_or("(blank)");
    let title = browser.title.as_deref().unwrap_or("(untitled)");
    format!(
        "# Browser {slug}\n\n- URL: {url}\n- Title: {title}\n- Status: {status}",
        slug = browser.slug,
        status = browser.status,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browsers::{insert_browser, BrowserScope, JobBrowser};
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn orch_with_db() -> (Orchestrator, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let local = LocalDb::open(dir.path().join("browser-render.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-brw', 'default', 'Browsers', 'BRW', '/tmp/brw', 1, 1)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let worktree = tempfile::tempdir().unwrap();
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .build();
        (orch, dir)
    }

    #[tokio::test]
    async fn render_browser_falls_back_to_row_only_without_webview() {
        let (orch, _dir) = orch_with_db().await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let browser = JobBrowser::new(&scope, "main", Some("https://example.com".to_string()), 10);
        insert_browser(&orch.db.local, browser).await.unwrap();

        let resource = CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "main".to_string(),
        };
        let rendered = render_browser(&orch, &resource, BridgeFormat::Markdown).await;
        assert!(rendered.contains("# Browser main"));
        assert!(rendered.contains("URL: https://example.com"));
        assert!(rendered.contains("no webview layer"));
        // The Slice A placeholder footer is retired.
        assert!(!rendered.contains("Page content is not available"));
    }

    #[tokio::test]
    async fn render_browser_auto_creates_missing_slug() {
        let (orch, _dir) = orch_with_db().await;
        let resource = CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "fresh".to_string(),
        };
        let rendered = render_browser(&orch, &resource, BridgeFormat::Markdown).await;
        // Zero-ceremony: the read auto-creates a blank row instead of erroring.
        // The headless harness (tx == None) keeps returning the row-only message,
        // proving the headless branch survives while the row is created.
        assert!(rendered.contains("# Browser fresh"), "{rendered}");
        assert!(rendered.contains("no webview layer"), "{rendered}");
        let found = find_browser_by_scope_and_slug(
            &orch.db.local,
            BrowserScope::Project("p-brw".to_string()),
            "fresh",
        )
        .await
        .unwrap();
        assert!(found.is_some(), "read auto-created the row");
    }

    #[test]
    fn parse_mode_accepts_format_screenshot_and_rejects_others() {
        let none: Vec<QueryParam> = vec![];
        assert_eq!(
            parse_browser_read_mode(&none).unwrap(),
            BrowserReadMode::Content(BridgeFormat::Markdown)
        );
        let text = vec![QueryParam {
            key: "format".to_string(),
            value: "text".to_string(),
        }];
        assert_eq!(
            parse_browser_read_mode(&text).unwrap(),
            BrowserReadMode::Content(BridgeFormat::Text)
        );
        let shot = vec![QueryParam {
            key: "screenshot".to_string(),
            value: String::new(),
        }];
        assert_eq!(
            parse_browser_read_mode(&shot).unwrap(),
            BrowserReadMode::Screenshot
        );
        let bad = vec![QueryParam {
            key: "format".to_string(),
            value: "pdf".to_string(),
        }];
        assert!(parse_browser_read_mode(&bad).is_err());
        // `?offset` is the universal line-window; it parses to a content read
        // (consumed centrally) rather than erroring, so the continue-footer URI
        // a truncated body emits actually works.
        let offset = vec![QueryParam {
            key: "offset".to_string(),
            value: "300".to_string(),
        }];
        assert_eq!(
            parse_browser_read_mode(&offset).unwrap(),
            BrowserReadMode::Content(BridgeFormat::Markdown)
        );
        // A genuinely-unknown parameter is still rejected.
        let other = vec![QueryParam {
            key: "bogus".to_string(),
            value: "3".to_string(),
        }];
        assert!(parse_browser_read_mode(&other).is_err());
    }

    #[test]
    fn parse_mode_accepts_console_network_and_limit() {
        let console = vec![QueryParam {
            key: "console".to_string(),
            value: String::new(),
        }];
        assert_eq!(
            parse_browser_read_mode(&console).unwrap(),
            BrowserReadMode::Console { limit: None }
        );
        let network_limited = vec![
            QueryParam {
                key: "network".to_string(),
                value: String::new(),
            },
            QueryParam {
                key: "limit".to_string(),
                value: "25".to_string(),
            },
        ];
        assert_eq!(
            parse_browser_read_mode(&network_limited).unwrap(),
            BrowserReadMode::Network { limit: Some(25) }
        );
        // Mutually exclusive facets are rejected.
        let both = vec![
            QueryParam {
                key: "console".to_string(),
                value: String::new(),
            },
            QueryParam {
                key: "network".to_string(),
                value: String::new(),
            },
        ];
        assert!(parse_browser_read_mode(&both).is_err());
        // `?limit` without console/network is the universal line-window on a
        // content read (consumed centrally), so it parses rather than erroring.
        let content_limit = vec![QueryParam {
            key: "limit".to_string(),
            value: "300".to_string(),
        }];
        assert_eq!(
            parse_browser_read_mode(&content_limit).unwrap(),
            BrowserReadMode::Content(BridgeFormat::Markdown)
        );
        // a non-numeric limit is still rejected on a content read.
        let bad_content_limit = vec![QueryParam {
            key: "limit".to_string(),
            value: "lots".to_string(),
        }];
        assert!(parse_browser_read_mode(&bad_content_limit).is_err());
        // ?interactive is its own facet, carries an optional element-cap limit,
        // and is mutually exclusive with the other facets.
        let interactive = vec![QueryParam {
            key: "interactive".to_string(),
            value: String::new(),
        }];
        assert_eq!(
            parse_browser_read_mode(&interactive).unwrap(),
            BrowserReadMode::Interactive { limit: None }
        );
        let interactive_limited = vec![
            QueryParam {
                key: "interactive".to_string(),
                value: String::new(),
            },
            QueryParam {
                key: "limit".to_string(),
                value: "50".to_string(),
            },
        ];
        assert_eq!(
            parse_browser_read_mode(&interactive_limited).unwrap(),
            BrowserReadMode::Interactive { limit: Some(50) }
        );
        let interactive_and_console = vec![
            QueryParam {
                key: "interactive".to_string(),
                value: String::new(),
            },
            QueryParam {
                key: "console".to_string(),
                value: String::new(),
            },
        ];
        assert!(parse_browser_read_mode(&interactive_and_console).is_err());
        // a non-numeric limit is rejected.
        let bad_limit = vec![
            QueryParam {
                key: "console".to_string(),
                value: String::new(),
            },
            QueryParam {
                key: "limit".to_string(),
                value: "lots".to_string(),
            },
        ];
        assert!(parse_browser_read_mode(&bad_limit).is_err());
    }

    #[test]
    fn format_console_body_lists_entries_with_stacks() {
        assert_eq!(
            format_console_body(&[]),
            "(No console output captured on this page.)"
        );
        let entries = vec![
            ConsoleEntry {
                ts: 1,
                level: "log".to_string(),
                message: "hello world".to_string(),
                stack: None,
            },
            ConsoleEntry {
                ts: 2,
                level: "error".to_string(),
                message: "boom".to_string(),
                stack: Some("at foo\nat bar".to_string()),
            },
        ];
        let body = format_console_body(&entries);
        assert!(body.contains("- **LOG** hello world"), "{body}");
        assert!(body.contains("- **ERROR** boom"), "{body}");
        assert!(body.contains("      at foo"), "{body}");
        assert!(body.contains("      at bar"), "{body}");
    }

    #[test]
    fn format_network_body_tabulates_requests_and_errors() {
        assert!(
            format_network_body(&[]).starts_with("(No network activity captured"),
            "{}",
            format_network_body(&[])
        );
        let entries = vec![
            NetworkEntry {
                ts: 1,
                method: "GET".to_string(),
                url: "https://api.example.com/data?a=1|2".to_string(),
                kind: Some("fetch".to_string()),
                status: Some(200),
                duration_ms: Some(12),
                ok: Some(true),
                error: None,
                size: Some(2048),
            },
            NetworkEntry {
                ts: 2,
                method: "POST".to_string(),
                url: "https://api.example.com/send".to_string(),
                kind: None,
                status: None,
                duration_ms: Some(5),
                ok: Some(false),
                error: Some("connection refused".to_string()),
                size: None,
            },
        ];
        let body = format_network_body(&entries);
        assert!(
            body.contains("| Method | Type | URL | Status | Duration | Size |"),
            "{body}"
        );
        assert!(body.contains("| GET |"), "{body}");
        assert!(body.contains("| fetch |"), "{body}");
        assert!(body.contains("| 200 |"), "{body}");
        assert!(body.contains("2.0 kB"), "{body}");
        assert!(body.contains("ERR (connection refused)"), "{body}");
        // The pipe in the query string is escaped so the table stays intact.
        assert!(!body.contains("a=1|2"), "{body}");
        assert!(body.contains("a=1%7C2"), "{body}");
    }

    #[tokio::test]
    async fn capture_roundtrip_headless_fallback_errors_cleanly() {
        let (orch, _dir) = orch_with_db().await;
        // No browser_command_tx wired (TestServices) → headless fallback.
        let result = browser_capture_roundtrip(&orch, "any-id", Duration::from_millis(50)).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("no live webview"),
            "headless capture should name the missing webview layer"
        );
    }

    /// Build an orchestrator with a browser command channel wired, returning the
    /// receiver so a test can stand in for the host drain task.
    async fn orch_with_browser_tx() -> (
        Orchestrator,
        tokio::sync::mpsc::UnboundedReceiver<BrowserCommand>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let local = LocalDb::open(dir.path().join("capture.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let worktree = tempfile::tempdir().unwrap();
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .browser_command_tx(Some(tx))
        .build();
        (orch, rx, dir)
    }

    #[tokio::test]
    async fn capture_roundtrip_correlates_reply_and_returns_base64() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        // Stand in for the host: when the Capture command arrives, publish a
        // matching-reqId ok reply carrying base64 image data.
        let responses = orch.browser_bridge_responses.clone();
        tokio::spawn(async move {
            if let Some(BrowserCommand::Capture { request_id, .. }) = rx.recv().await {
                let payload =
                    serde_json::json!({ "ok": true, "dataB64": "UE5HX0RBVEE=" }).to_string();
                let _ = responses.send((request_id, payload));
            }
        });
        let result = browser_capture_roundtrip(&orch, "bid", Duration::from_secs(2)).await;
        assert_eq!(result.unwrap(), "UE5HX0RBVEE=");
    }

    #[tokio::test]
    async fn capture_roundtrip_surfaces_host_error_reply() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        let responses = orch.browser_bridge_responses.clone();
        tokio::spawn(async move {
            if let Some(BrowserCommand::Capture { request_id, .. }) = rx.recv().await {
                let payload =
                    serde_json::json!({ "ok": false, "error": "no live webview for browser bid" })
                        .to_string();
                let _ = responses.send((request_id, payload));
            }
        });
        let err = browser_capture_roundtrip(&orch, "bid", Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(err.contains("no live webview"), "{err}");
    }

    #[tokio::test]
    async fn capture_roundtrip_times_out_when_host_never_replies() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        // Drain the command but never reply, so the awaiter must time out.
        tokio::spawn(async move {
            let _ = rx.recv().await;
        });
        let err = browser_capture_roundtrip(&orch, "bid", Duration::from_millis(80))
            .await
            .unwrap_err();
        assert!(err.contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn clear_roundtrip_forwards_kinds_and_returns_ok() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        // Stand in for the host: confirm the kinds rode the command, then ack.
        let responses = orch.browser_bridge_responses.clone();
        tokio::spawn(async move {
            if let Some(BrowserCommand::ClearData {
                request_id, kinds, ..
            }) = rx.recv().await
            {
                assert_eq!(kinds, vec!["cookies".to_string(), "cache".to_string()]);
                let _ = responses.send((request_id, "{\"ok\":true}".to_string()));
            }
        });
        let result = browser_clear_data_roundtrip(
            &orch,
            "bid",
            vec!["cookies".to_string(), "cache".to_string()],
            Duration::from_secs(2),
        )
        .await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn clear_roundtrip_surfaces_host_error_reply() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        let responses = orch.browser_bridge_responses.clone();
        tokio::spawn(async move {
            if let Some(BrowserCommand::ClearData { request_id, .. }) = rx.recv().await {
                let payload = serde_json::json!({
                    "ok": false,
                    "error": "clearing browser data is not yet supported on this platform"
                })
                .to_string();
                let _ = responses.send((request_id, payload));
            }
        });
        let err = browser_clear_data_roundtrip(&orch, "bid", vec![], Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(err.contains("not yet supported"), "{err}");
    }

    #[tokio::test]
    async fn clear_roundtrip_headless_fallback_errors_cleanly() {
        let (orch, _dir) = orch_with_db().await;
        let err = browser_clear_data_roundtrip(&orch, "any-id", vec![], Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(err.contains("no live webview"), "{err}");
    }

    #[tokio::test]
    async fn screenshot_headless_returns_header_without_image() {
        let (orch, _dir) = orch_with_db().await;
        let resource = CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "main".to_string(),
        };
        let (header, image) = render_browser_screenshot(&orch, &resource).await;
        assert!(header.contains("# Browser main"), "{header}");
        assert!(header.contains("no webview layer"), "{header}");
        assert!(image.is_none(), "headless host yields no image");
    }
}
