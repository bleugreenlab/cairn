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
use cairn_common::uri::{CairnResource, DEFAULT_BROWSER_SLUG};
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;

use crate::browsers::{
    ensure_open_browser, find_browser_by_scope_and_slug, find_most_recently_active_open_browser,
    get_browser, BridgeFormat, BridgeRequest, BridgeResponse, BrowserCommand, BrowserNavEvent,
    BrowserScope, ConsoleEntry, InteractiveElement, JobBrowser, NavPhase, NetworkEntry,
    STATUS_OPEN,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// Default budget for an extract/interaction bridge round-trip with the page.
/// A `waitFor` action passes a larger budget that must exceed its own timeout.
pub(crate) const BRIDGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Budget for waiting for a still-loading page to become interactive before a
/// content-script read. Distinct from — and larger than — [`BRIDGE_TIMEOUT`],
/// which now measures only post-ready bridge responsiveness. A slow first
/// compile (Vite dep-optimization, 5–15s) should read as "still loading" and be
/// re-read, not be masked by an inflated bridge timeout.
pub(crate) const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a read waits for the host to ack an `OpenForRead` before proceeding.
/// Best-effort: on timeout the read continues (degrading to the pre-fix sampling
/// behavior, still protected by the readiness wait + post-failure retry), so a
/// wedged main thread can never hang a read indefinitely.
pub(crate) const OPEN_ACK_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Resolve a browser resource URI to its owning scope plus a CONCRETE slug,
/// applying focus-following for the bare/default form. The bare `cairn:~/browser`
/// URI collapses to [`DEFAULT_BROWSER_SLUG`] at parse time, so resolution treats
/// that slug as the scope's focus-following alias: it picks the most-recently
/// user-active open browser in the scope, landing the agent on the tab the user
/// is actually looking at. When none is open it falls back to the literal
/// `default` slug, which the downstream `ensure_open_browser` creates — preserving
/// the zero-ceremony contract. An explicit non-default slug resolves exactly,
/// bypassing focus-following. Returning a concrete slug keeps every downstream
/// `find_browser_by_scope_and_slug`/`ensure_open_browser` step unchanged.
pub(crate) async fn resolve_browser_target(
    db: &LocalDb,
    resource: &CairnResource,
) -> Result<(BrowserScope, String), String> {
    let (scope, slug) = resolve_browser_scope(db, resource).await?;
    if slug == DEFAULT_BROWSER_SLUG {
        if let Some(active) = find_most_recently_active_open_browser(db, scope.clone()).await? {
            return Ok((scope, active.slug));
        }
    }
    Ok((scope, slug))
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
    let (scope, slug) = match resolve_browser_target(db, resource).await {
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

    // Sample the loading flag BEFORE the open so we can tell a cold-create open
    // (the case this gate exists for) from a page that was already loading. The
    // freshly-created webview path is the ONLY open path that flips the flag to
    // loading synchronously inside the open closure; the reuse/no-window paths ack
    // without touching it. Subscribe before the open too, so a fast cold-load
    // Finished cannot race the awaiter.
    let was_loading = orch.is_browser_loading(&browser.id);
    let mut nav_rx = orch.browser_nav_events.subscribe();
    open_for_read(orch, &browser, OPEN_ACK_TIMEOUT).await;

    // A capture of a still-loading page is useful and already works, so the
    // screenshot path is never gated into a hard failure. The two cases differ:
    //   * Cold rehydrate: the open just CREATED the webview (not-loading → loading
    //     across the ack), so wait the readiness budget for the first paint before
    //     capturing — without it the frame would be blank. Note partiality only if
    //     it never finishes in time.
    //   * Already-loading live page: capture an immediate partial frame and note
    //     it, preserving the screenshot's value as a quick visual sample of a slow
    //     page rather than blocking on the readiness budget.
    let now_loading = orch.is_browser_loading(&browser.id);
    let cold_create = !was_loading && now_loading;
    let partial = if cold_create {
        await_browser_nav(&mut nav_rx, &browser.id, READINESS_TIMEOUT, true)
            .await
            .is_none()
    } else {
        now_loading
    };
    let header = if partial {
        format!("{header}\n\n(Note: the page is still loading; this screenshot may be partial.)")
    } else {
        header
    };

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

/// Soft "page still loading" message: a page that has not become interactive
/// within the readiness budget. The promised recovery (read again) is something
/// the agent can actually do, and the readiness wait already gave the page time,
/// so this is deliberately NOT a hard error.
fn still_loading_message(header: &str, url: &str) -> String {
    format!(
        "{header}\n\n(Page is still loading at {url} — it has not become interactive after {}s. Read again shortly once it settles.)",
        READINESS_TIMEOUT.as_secs()
    )
}

/// Hard "loaded but bridge failed" message: the page reached a loaded state yet
/// the content-script bridge did not respond. Names the url and the bridge
/// budget so the failure is actionable rather than a bare timeout string.
fn bridge_failed_message(header: &str, url: &str) -> String {
    format!(
        "{header}\n\n(The page finished loading but its content bridge did not respond within {}s at {url}. Reload the pane and retry.)",
        BRIDGE_TIMEOUT.as_secs()
    )
}

/// Send a read-path `OpenForRead` and await the host's ack on the reqId
/// broadcast, so the caller knows the rehydrated webview is registered and (for a
/// fresh create) its loading flag is established before it samples readiness,
/// primes, bridges, or captures. This closes the cold-rehydrate race: the bare
/// `Open` only QUEUES async main-thread webview creation, so a caller that read
/// the loading flag immediately could sample it before the open closure ran.
/// Best-effort: returns on ack, on `ack_timeout`, or when no webview layer is
/// wired (headless/server). The `{}` ack payload is intentionally ignored; only
/// the reqId match matters, and a fresh uuid avoids cross-talk with concurrent
/// bridge/capture awaiters on the same broadcast.
async fn open_for_read(orch: &Orchestrator, browser: &JobBrowser, ack_timeout: Duration) {
    let Some(tx) = &orch.browser_command_tx else {
        return;
    };
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut rx = orch.browser_bridge_responses.subscribe();
    if tx
        .send(BrowserCommand::OpenForRead {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
            url: browser.url.clone(),
            request_id: request_id.clone(),
        })
        .is_err()
    {
        return;
    }
    let _ = tokio::time::timeout(ack_timeout, async {
        loop {
            match rx.recv().await {
                Ok((resp_id, _)) if resp_id == request_id => return,
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            }
        }
    })
    .await;
}

/// Run a content-script bridge round-trip for an agent read, gating on page
/// readiness so a still-compiling page reads as "still loading" rather than a
/// hard bridge timeout — without inflating [`BRIDGE_TIMEOUT`]. Sends the
/// rehydrate `Open`, waits (bounded) for a loading page to finish, runs the
/// round-trip (which the dispatch layer brackets with off-screen render
/// priming), and on a transport failure RE-checks the loading flag to close the
/// rehydrate/cold-start race — a reload may have begun loading AFTER the first
/// check. Returns the bridge response on a reply (the caller inspects
/// `response.ok`), or a classified user-facing message (soft still-loading vs
/// hard loaded-but-broken) already prefixed with `header`.
///
/// `readiness_timeout`/`bridge_timeout` are parameters rather than the consts
/// directly so the soft/hard classification is unit-testable with tiny budgets.
async fn read_bridge_with_readiness(
    orch: &Orchestrator,
    browser: &JobBrowser,
    header: &str,
    request: &BridgeRequest,
    readiness_timeout: Duration,
    bridge_timeout: Duration,
    open_ack_timeout: Duration,
) -> Result<BridgeResponse, String> {
    let url = browser.url.as_deref().unwrap_or("(blank)");
    // Subscribe BEFORE the rehydrate Open so a fast page-load Finished cannot
    // race the readiness awaiter.
    let mut nav_rx = orch.browser_nav_events.subscribe();

    // Rehydrate the webview before the round-trip (recreate one a restart wiped,
    // loading the stored url; a live webview no-ops). Awaiting the open ack makes
    // the loading-flag sample below happen AFTER the host's open closure ran: on a
    // cold rehydrate that closure both registers the webview (so BeginRenderPriming
    // and the bridge find it) and flags a freshly created one loading (so the gate
    // waits for the load instead of bridging a not-yet-loaded page).
    open_for_read(orch, browser, open_ack_timeout).await;

    // If the page is loading, wait (bounded) for it to become interactive before
    // bridging. No Finished within the readiness budget → soft still-loading.
    if orch.is_browser_loading(&browser.id)
        && await_browser_nav(&mut nav_rx, &browser.id, readiness_timeout, true)
            .await
            .is_none()
    {
        return Err(still_loading_message(header, url));
    }

    match browser_bridge_roundtrip(orch, &browser.id, request, bridge_timeout).await {
        Ok(response) => Ok(response),
        Err(_) => {
            // Transport failed. Re-check the loading flag to close the
            // rehydrate/cold-start race: the Open above may have kicked off a
            // reload that started loading AFTER the first check.
            if orch.is_browser_loading(&browser.id) {
                if await_browser_nav(&mut nav_rx, &browser.id, readiness_timeout, true)
                    .await
                    .is_none()
                {
                    return Err(still_loading_message(header, url));
                }
                // Retry once after a confirmed Finished; a second failure on a
                // now-loaded page is a genuine bridge failure.
                return browser_bridge_roundtrip(orch, &browser.id, request, bridge_timeout)
                    .await
                    .map_err(|_| bridge_failed_message(header, url));
            }
            // Not loading: the page loaded but the bridge genuinely failed.
            Err(bridge_failed_message(header, url))
        }
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
    let (scope, slug) = match resolve_browser_target(db, resource).await {
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

    // The readiness-gated round-trip sends the rehydrate Open, primes off-screen
    // render inside the dispatch layer, and classifies a still-loading page vs a
    // loaded-but-broken bridge.
    let response = match read_bridge_with_readiness(
        orch,
        &browser,
        &header,
        &BridgeRequest::Extract { format },
        READINESS_TIMEOUT,
        BRIDGE_TIMEOUT,
        OPEN_ACK_TIMEOUT,
    )
    .await
    {
        Ok(response) => response,
        Err(message) => return message,
    };
    if response.ok {
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
    } else {
        format!(
            "{header}\n\n(Could not read page content: {})",
            response
                .error
                .unwrap_or_else(|| "unknown error".to_string())
        )
    }
}

/// The outcome of resolving + ensuring a browser row for a live-data read
/// (console/network): either the live browser id and its header text, or an
/// early message to surface to the agent (resolve error, headless host, or a
/// closed pane). Mirrors the preamble [`render_browser`] runs inline.
enum LiveBrowser {
    // Boxed so the populated variant doesn't bloat every `LiveBrowser` (the row
    // is ~256 bytes vs the `Message` string's handful).
    Ready {
        browser: Box<JobBrowser>,
        header: String,
    },
    Message(String),
}

async fn prepare_live_browser(orch: &Orchestrator, resource: &CairnResource) -> LiveBrowser {
    let db = &orch.db.local;
    let (scope, slug) = match resolve_browser_target(db, resource).await {
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

    // The rehydrate Open is sent by `read_bridge_with_readiness` so the readiness
    // gate can subscribe to nav events BEFORE it (no fast-Finished race).
    LiveBrowser::Ready {
        browser: Box::new(browser),
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
    let (browser, header) = match prepare_live_browser(orch, resource).await {
        LiveBrowser::Ready { browser, header } => (browser, header),
        LiveBrowser::Message(message) => return message,
    };
    match read_bridge_with_readiness(
        orch,
        &browser,
        &header,
        &BridgeRequest::GetConsole { limit },
        READINESS_TIMEOUT,
        BRIDGE_TIMEOUT,
        OPEN_ACK_TIMEOUT,
    )
    .await
    {
        Ok(response) if response.ok => {
            let entries = response.logs.unwrap_or_default();
            format!(
                "{header}\n\n---\n\n### Console ({} entries)\n\n{}",
                entries.len(),
                format_console_body(&entries)
            )
        }
        Ok(response) => format!(
            "{header}\n\n(Could not read console: {})",
            response
                .error
                .unwrap_or_else(|| "unknown error".to_string())
        ),
        Err(message) => message,
    }
}

/// Render a browser network read: the url/title/status header plus the page's
/// captured request summaries as a compact markdown table.
pub(crate) async fn render_browser_network(
    orch: &Orchestrator,
    resource: &CairnResource,
    limit: Option<u32>,
) -> String {
    let (browser, header) = match prepare_live_browser(orch, resource).await {
        LiveBrowser::Ready { browser, header } => (browser, header),
        LiveBrowser::Message(message) => return message,
    };
    match read_bridge_with_readiness(
        orch,
        &browser,
        &header,
        &BridgeRequest::GetNetwork { limit },
        READINESS_TIMEOUT,
        BRIDGE_TIMEOUT,
        OPEN_ACK_TIMEOUT,
    )
    .await
    {
        Ok(response) if response.ok => {
            let entries = response.requests.unwrap_or_default();
            format!(
                "{header}\n\n---\n\n### Network ({} requests)\n\n{}",
                entries.len(),
                format_network_body(&entries)
            )
        }
        Ok(response) => format!(
            "{header}\n\n(Could not read network: {})",
            response
                .error
                .unwrap_or_else(|| "unknown error".to_string())
        ),
        Err(message) => message,
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
    let (browser, header) = match prepare_live_browser(orch, resource).await {
        LiveBrowser::Ready { browser, header } => (browser, header),
        LiveBrowser::Message(message) => return message,
    };
    let response = match read_bridge_with_readiness(
        orch,
        &browser,
        &header,
        &BridgeRequest::ListInteractive { limit },
        READINESS_TIMEOUT,
        BRIDGE_TIMEOUT,
        OPEN_ACK_TIMEOUT,
    )
    .await
    {
        Ok(response) => response,
        Err(message) => return message,
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

    /// Insert an open project browser with an explicit `last_active_at`.
    async fn seed_browser(orch: &Orchestrator, slug: &str, last_active: i64, open: bool) {
        let scope = BrowserScope::Project("p-brw".to_string());
        let mut browser = JobBrowser::new(&scope, slug, None, last_active);
        browser.last_active_at = Some(last_active);
        if !open {
            browser.status = crate::browsers::STATUS_CLOSED.to_string();
            browser.closed_at = Some(last_active);
        }
        insert_browser(&orch.db.local, browser).await.unwrap();
    }

    fn default_project_browser() -> CairnResource {
        CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "default".to_string(),
        }
    }

    #[tokio::test]
    async fn resolve_target_focus_follows_most_recently_active() {
        let (orch, _dir) = orch_with_db().await;
        seed_browser(&orch, "tab-a", 10, true).await;
        seed_browser(&orch, "tab-b", 20, true).await;
        // The bare/default form lands on the most-recently user-active open tab.
        let (_scope, slug) = resolve_browser_target(&orch.db.local, &default_project_browser())
            .await
            .unwrap();
        assert_eq!(slug, "tab-b");
    }

    #[tokio::test]
    async fn resolve_target_no_open_falls_back_to_default() {
        let (orch, _dir) = orch_with_db().await;
        // No open browser in scope ⇒ the literal `default` slug (created downstream).
        let (_scope, slug) = resolve_browser_target(&orch.db.local, &default_project_browser())
            .await
            .unwrap();
        assert_eq!(slug, "default");
    }

    #[tokio::test]
    async fn resolve_target_ignores_closed_more_recent() {
        let (orch, _dir) = orch_with_db().await;
        seed_browser(&orch, "tab-a", 10, true).await;
        seed_browser(&orch, "tab-b", 99, false).await; // closed but most recent
        let (_scope, slug) = resolve_browser_target(&orch.db.local, &default_project_browser())
            .await
            .unwrap();
        assert_eq!(slug, "tab-a", "a closed browser never focus-follows");
    }

    #[tokio::test]
    async fn resolve_target_ignores_agent_created_null_row() {
        let (orch, _dir) = orch_with_db().await;
        // The user's active tab carries a real activation timestamp.
        seed_browser(&orch, "browser-a", 100, true).await;
        // An agent creates an explicit missing browser LATER via the shared
        // ensure path, so it has a NULL last_active_at and must not steal focus.
        ensure_open_browser(
            &orch.db.local,
            BrowserScope::Project("p-brw".to_string()),
            "scratch",
            None,
            200,
        )
        .await
        .unwrap();
        let (_scope, slug) = resolve_browser_target(&orch.db.local, &default_project_browser())
            .await
            .unwrap();
        assert_eq!(slug, "browser-a", "agent insert must not steal focus");
    }

    #[tokio::test]
    async fn resolve_target_explicit_slug_bypasses_focus_following() {
        let (orch, _dir) = orch_with_db().await;
        seed_browser(&orch, "tab-a", 99, true).await; // a more-recent OTHER tab
        let resource = CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "explicit".to_string(),
        };
        let (_scope, slug) = resolve_browser_target(&orch.db.local, &resource)
            .await
            .unwrap();
        assert_eq!(slug, "explicit", "an explicit slug resolves exactly");
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

    // ===== Render priming + readiness classification (Parts 1 & 2) =====

    fn browser_with_id(id: &str, url: &str) -> JobBrowser {
        JobBrowser {
            id: id.to_string(),
            job_id: None,
            project_id: Some("p-brw".to_string()),
            slug: "main".to_string(),
            webview_label: format!("browser:{id}"),
            url: Some(url.to_string()),
            title: None,
            status: STATUS_OPEN.to_string(),
            created_at: 1,
            closed_at: None,
            last_active_at: None,
        }
    }

    /// Part 1: a content-script bridge round-trip brackets the dispatch with
    /// BeginRenderPriming → Bridge → EndRenderPriming, in that order, so a
    /// backgrounded webview is held realized off-screen for the round-trip.
    #[tokio::test]
    async fn bridge_roundtrip_brackets_with_render_priming() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected2 = collected.clone();
        let responses = orch.browser_bridge_responses.clone();
        let task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let BrowserCommand::Bridge { ref request_id, .. } = cmd {
                    let _ = responses.send((
                        request_id.clone(),
                        "{\"ok\":true,\"content\":\"hi\",\"format\":\"text\"}".to_string(),
                    ));
                }
                collected2.lock().unwrap().push(cmd);
            }
        });
        let result = browser_bridge_roundtrip(
            &orch,
            "bid",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_secs(2),
        )
        .await;
        assert!(result.is_ok(), "{result:?}");
        // End is sent on the guard's drop after the round-trip returns.
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(orch);
        let _ = task.await;
        let cmds = collected.lock().unwrap().clone();
        assert!(
            matches!(
                cmds.as_slice(),
                [
                    BrowserCommand::BeginRenderPriming { .. },
                    BrowserCommand::Bridge { .. },
                    BrowserCommand::EndRenderPriming { .. }
                ]
            ),
            "expected Begin, Bridge, End in order, got {cmds:?}"
        );
    }

    /// Part 1: EndRenderPriming is emitted even when the round-trip times out —
    /// the RAII guard must release the off-screen hold on every exit path.
    #[tokio::test]
    async fn bridge_roundtrip_emits_end_priming_even_on_timeout() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected2 = collected.clone();
        // Drain commands but never reply, so the round-trip times out.
        let task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                collected2.lock().unwrap().push(cmd);
            }
        });
        let err = browser_bridge_roundtrip(
            &orch,
            "bid",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_millis(80),
        )
        .await
        .unwrap_err();
        assert!(err.contains("timed out"), "{err}");
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(orch);
        let _ = task.await;
        let cmds = collected.lock().unwrap().clone();
        assert!(
            cmds.iter()
                .any(|c| matches!(c, BrowserCommand::BeginRenderPriming { .. })),
            "Begin must be emitted, got {cmds:?}"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c, BrowserCommand::EndRenderPriming { .. })),
            "End must be emitted even on timeout, got {cmds:?}"
        );
    }

    /// Part 2: a loading page that never becomes interactive within the
    /// readiness budget reads as a SOFT still-loading message naming the url —
    /// not a hard bridge failure.
    #[tokio::test]
    async fn read_readiness_still_loading_is_soft() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        orch.set_browser_loading("bid", true);
        // Drain commands; never reply, never publish Finished.
        let task = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let browser = browser_with_id("bid", "http://localhost:5173/app");
        let res = read_bridge_with_readiness(
            &orch,
            &browser,
            "# Browser main",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_millis(60),
            Duration::from_millis(60),
            Duration::from_millis(20),
        )
        .await;
        let err = res.unwrap_err();
        assert!(err.contains("still loading"), "{err}");
        assert!(err.contains("http://localhost:5173/app"), "{err}");
        drop(orch);
        let _ = task.await;
    }

    /// Part 2: a loading page that finishes within the readiness budget proceeds
    /// to a successful bridge read.
    #[tokio::test]
    async fn read_readiness_finishes_then_succeeds() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        orch.set_browser_loading("bid", true);
        let responses = orch.browser_bridge_responses.clone();
        let task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let BrowserCommand::Bridge { request_id, .. } = cmd {
                    let _ = responses.send((
                        request_id,
                        "{\"ok\":true,\"content\":\"hello\",\"format\":\"text\"}".to_string(),
                    ));
                }
            }
        });
        // Publish Finished shortly after the helper subscribes.
        let nav = orch.browser_nav_events.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _ = nav.send(BrowserNavEvent {
                browser_id: "bid".to_string(),
                url: "http://x".to_string(),
                phase: NavPhase::Finished,
            });
        });
        let browser = browser_with_id("bid", "http://x");
        let res = read_bridge_with_readiness(
            &orch,
            &browser,
            "# Browser main",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_millis(20),
        )
        .await;
        let resp = res.expect("a finished page should read successfully");
        assert!(resp.ok);
        assert_eq!(resp.content.as_deref(), Some("hello"));
        drop(orch);
        let _ = task.await;
    }

    /// Part 2: a page that is NOT loading but whose bridge fails reads as a HARD
    /// loaded-but-broken message — the still-loading path must never mask it.
    #[tokio::test]
    async fn read_readiness_loaded_but_broken_is_hard() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        // loading flag false (default). Drain but never reply → bridge times out.
        let task = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let browser = browser_with_id("bid", "http://x/app");
        let res = read_bridge_with_readiness(
            &orch,
            &browser,
            "# Browser main",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_millis(60),
            Duration::from_millis(60),
            Duration::from_millis(20),
        )
        .await;
        let err = res.unwrap_err();
        assert!(
            !err.contains("still loading"),
            "a loaded page must not read as loading: {err}"
        );
        assert!(err.contains("did not respond"), "{err}");
        assert!(err.contains("http://x/app"), "{err}");
        drop(orch);
        let _ = task.await;
    }

    /// Part 2: the rehydrate race — not loading at entry, but a reload starts
    /// AFTER the first bridge attempt fails. The post-failure re-check waits for
    /// the new load to finish and retries once, succeeding.
    #[tokio::test]
    async fn read_readiness_handles_rehydrate_race_then_retries() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        let responses = orch.browser_bridge_responses.clone();
        let loading = orch.browser_loading.clone();
        let task = tokio::spawn(async move {
            let mut first = true;
            while let Some(cmd) = rx.recv().await {
                if let BrowserCommand::Bridge { request_id, .. } = cmd {
                    if first {
                        first = false;
                        // A rehydrate reload begins AFTER the first readiness check.
                        loading.lock().unwrap().insert("bid".to_string(), true);
                        // No reply → the first attempt times out.
                    } else {
                        let _ = responses.send((
                            request_id,
                            "{\"ok\":true,\"content\":\"late\",\"format\":\"text\"}".to_string(),
                        ));
                    }
                }
            }
        });
        // Publish Finished after the first attempt has timed out and the
        // post-failure re-check is awaiting readiness.
        let nav = orch.browser_nav_events.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = nav.send(BrowserNavEvent {
                browser_id: "bid".to_string(),
                url: "http://x".to_string(),
                phase: NavPhase::Finished,
            });
        });
        let browser = browser_with_id("bid", "http://x");
        let res = read_bridge_with_readiness(
            &orch,
            &browser,
            "# Browser main",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_secs(2),
            Duration::from_millis(60),
            Duration::from_millis(20),
        )
        .await;
        let resp = res.expect("the retry after the race should succeed");
        assert!(resp.ok);
        assert_eq!(resp.content.as_deref(), Some("late"));
        drop(orch);
        let _ = task.await;
    }

    /// Part 3: the cold-creation ordering the OpenForRead ack exists to fix. On a
    /// cold rehydrate the freshly created webview reads as loading only AFTER the
    /// host's open closure runs; awaiting the ack makes the gate sample the loading
    /// flag after that closure, so it waits for the load and bridges the loaded
    /// page. The stand-in's Bridge arm answers ok only once the page has finished
    /// loading, so an early bridge (the bug — sampling before the open ran) would
    /// get ok:false and fail the read: this exercises the fix rather than passing
    /// regardless.
    #[tokio::test]
    async fn read_readiness_cold_open_waits_for_registration_and_load() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        // Cold: the loading flag starts unset (registry wiped). The stand-in flags
        // it only when OpenForRead arrives, mimicking open_browser's fresh-create
        // path, then clears it and publishes Finished after a short delay.
        let responses = orch.browser_bridge_responses.clone();
        let nav = orch.browser_nav_events.clone();
        let loading = orch.browser_loading.clone();
        let task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BrowserCommand::OpenForRead { request_id, .. } => {
                        loading.lock().unwrap().insert("bid".to_string(), true);
                        let _ = responses.send((request_id, "{}".to_string()));
                        let (nav2, loading2) = (nav.clone(), loading.clone());
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(40)).await;
                            loading2.lock().unwrap().insert("bid".to_string(), false);
                            let _ = nav2.send(BrowserNavEvent {
                                browser_id: "bid".to_string(),
                                url: "http://x".to_string(),
                                phase: NavPhase::Finished,
                            });
                        });
                    }
                    BrowserCommand::Bridge { request_id, .. } => {
                        // ok only once the page has finished loading; an early
                        // bridge sees an unloaded page and gets ok:false.
                        let loaded = loading.lock().unwrap().get("bid").copied() == Some(false);
                        let body = if loaded {
                            "{\"ok\":true,\"content\":\"ready\",\"format\":\"text\"}"
                        } else {
                            "{\"ok\":false,\"format\":\"text\"}"
                        };
                        let _ = responses.send((request_id, body.to_string()));
                    }
                    _ => {}
                }
            }
        });
        let browser = browser_with_id("bid", "http://x");
        let res = read_bridge_with_readiness(
            &orch,
            &browser,
            "# Browser main",
            &BridgeRequest::Extract {
                format: BridgeFormat::Text,
            },
            Duration::from_secs(2),
            Duration::from_millis(200),
            Duration::from_secs(1),
        )
        .await;
        let resp = res.expect("cold read should wait for the open ack + load, then bridge");
        assert!(resp.ok);
        assert_eq!(resp.content.as_deref(), Some("ready"));
        drop(orch);
        let _ = task.await;
    }

    /// Part 3: the screenshot cold path. A cold rehydrate waits for the load before
    /// capturing, so it returns a real frame with NO partiality note. The stand-in
    /// replies to Capture with image data only once the page has finished loading,
    /// so an early capture (the bug) would yield no image and fail the assertion.
    #[tokio::test]
    async fn screenshot_cold_open_waits_for_load_then_captures_real_frame() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        // Seed the project + a known browser row so the screenshot path resolves to
        // the id "bid" the stand-in keys on.
        orch.db
            .local
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
        insert_browser(&orch.db.local, browser_with_id("bid", "http://x"))
            .await
            .unwrap();

        let responses = orch.browser_bridge_responses.clone();
        let nav = orch.browser_nav_events.clone();
        let loading = orch.browser_loading.clone();
        let task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BrowserCommand::OpenForRead { request_id, .. } => {
                        loading.lock().unwrap().insert("bid".to_string(), true);
                        let _ = responses.send((request_id, "{}".to_string()));
                        let (nav2, loading2) = (nav.clone(), loading.clone());
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(40)).await;
                            loading2.lock().unwrap().insert("bid".to_string(), false);
                            let _ = nav2.send(BrowserNavEvent {
                                browser_id: "bid".to_string(),
                                url: "http://x".to_string(),
                                phase: NavPhase::Finished,
                            });
                        });
                    }
                    BrowserCommand::Capture { request_id, .. } => {
                        // Image data only once the page has finished loading.
                        let loaded = loading.lock().unwrap().get("bid").copied() == Some(false);
                        let payload = if loaded {
                            serde_json::json!({ "ok": true, "dataB64": "UE5HX0RBVEE=" }).to_string()
                        } else {
                            serde_json::json!({ "ok": false, "error": "not loaded" }).to_string()
                        };
                        let _ = responses.send((request_id, payload));
                    }
                    _ => {}
                }
            }
        });
        let resource = CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "main".to_string(),
        };
        let (header, image) = render_browser_screenshot(&orch, &resource).await;
        assert!(
            image.is_some(),
            "cold screenshot should capture a real frame: {header}"
        );
        assert!(
            !header.contains("still loading"),
            "a waited-for cold capture must not be noted partial: {header}"
        );
        drop(orch);
        let _ = task.await;
    }

    /// Part 3: a warm, already-loading live page must yield an IMMEDIATE partial
    /// screenshot (the quick-visual-sample contract), not block on the readiness
    /// budget. Only a cold-create open (not-loading → loading across the ack)
    /// waits; a page already loading before the read captures at once with the
    /// partiality note. The stand-in keeps the page loading forever and never
    /// sends Finished, so a gate that wrongly waited would block the full
    /// READINESS_TIMEOUT — the 5s bound trips that regression.
    #[tokio::test]
    async fn screenshot_warm_already_loading_returns_promptly_with_partial_note() {
        let (orch, mut rx, _dir) = orch_with_browser_tx().await;
        orch.db
            .local
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
        insert_browser(&orch.db.local, browser_with_id("bid", "http://x"))
            .await
            .unwrap();
        // The live webview is already mid-load when the read arrives.
        orch.set_browser_loading("bid", true);

        let responses = orch.browser_bridge_responses.clone();
        let task = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    // Reuse branch: ack without touching the loading flag and
                    // without ever publishing Finished (the page keeps loading).
                    BrowserCommand::OpenForRead { request_id, .. } => {
                        let _ = responses.send((request_id, "{}".to_string()));
                    }
                    BrowserCommand::Capture { request_id, .. } => {
                        let payload = serde_json::json!({ "ok": true, "dataB64": "UE5HX0RBVEE=" })
                            .to_string();
                        let _ = responses.send((request_id, payload));
                    }
                    _ => {}
                }
            }
        });
        let resource = CairnResource::ProjectBrowser {
            project: "BRW".to_string(),
            slug: "main".to_string(),
        };
        // If the gate wrongly waited the readiness budget (30s), this 5s bound trips.
        let (header, image) = tokio::time::timeout(
            Duration::from_secs(5),
            render_browser_screenshot(&orch, &resource),
        )
        .await
        .expect(
            "warm already-loading screenshot must return promptly, not wait the readiness budget",
        );
        assert!(
            image.is_some(),
            "a partial frame is still captured: {header}"
        );
        assert!(
            header.contains("still loading"),
            "an already-loading screenshot must carry the partiality note: {header}"
        );
        drop(orch);
        let _ = task.await;
    }

    /// Part 2: the classified messages name the url and the relevant budget so an
    /// agent can act on them.
    #[test]
    fn classified_messages_name_url_and_budget() {
        let soft = still_loading_message("# Browser main", "http://localhost:5173");
        assert!(soft.contains("# Browser main"), "{soft}");
        assert!(soft.contains("still loading"), "{soft}");
        assert!(soft.contains("http://localhost:5173"), "{soft}");
        assert!(
            soft.contains(&format!("{}s", READINESS_TIMEOUT.as_secs())),
            "{soft}"
        );
        let hard = bridge_failed_message("# Browser main", "http://localhost:5173");
        assert!(hard.contains("# Browser main"), "{hard}");
        assert!(hard.contains("finished loading"), "{hard}");
        assert!(hard.contains("http://localhost:5173"), "{hard}");
        assert!(
            hard.contains(&format!("{}s", BRIDGE_TIMEOUT.as_secs())),
            "{hard}"
        );
    }
}
