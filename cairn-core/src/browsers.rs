//! Inline browser (Slice A) persistence and the plain-data command channel.
//!
//! This module is Tauri-free. It owns the durable `job_browsers` mirror of a
//! native webview pane and the [`BrowserCommand`] enum that bridges core
//! dispatch/teardown to the app-side `BrowserRegistry` (which holds the live
//! `Webview` handles — a Tauri type that cannot live in cairn-core). The row is
//! the readable mirror; the live webview is the volatile native layer.

use std::time::Duration;

use cairn_db::turso::params;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// The durable mirror of a native webview pane. No run_id/session_id/exit_code/
/// command — those are PTY concepts. `webview_label` identifies the current
/// webview generation and is used as the `BrowserRegistry` key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobBrowser {
    pub id: String,
    pub job_id: Option<String>,
    pub project_id: Option<String>,
    pub slug: String,
    pub webview_label: String,
    pub url: Option<String>,
    pub title: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub closed_at: Option<i64>,
    /// Wall-clock of the last USER-initiated activation (create/navigate/focus).
    /// A non-NULL value is the load-bearing signal that a HUMAN activated this
    /// pane: only such rows are focus-following candidates for the bare/default
    /// browser URI. `None` means "never user-activated" — covering both rows
    /// predating the column and agent-created rows — and is deliberately never
    /// set by agent reads/actions or by generic storage insertion.
    pub last_active_at: Option<i64>,
}

pub const STATUS_OPEN: &str = "open";
pub const STATUS_CLOSED: &str = "closed";

fn fresh_webview_label(browser_id: &str) -> String {
    format!("browser:{browser_id}:{}", uuid::Uuid::new_v4())
}

/// The owning scope of a browser: a job (node/task browsers) or a project
/// (the user's own persistent project browsers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserScope {
    Job(String),
    Project(String),
}

impl JobBrowser {
    /// Build a fresh open browser row for a scope. The browser id remains durable,
    /// while the webview label includes an opaque generation that rotates on
    /// reopen so late events from a destroyed page can be rejected.
    pub fn new(scope: &BrowserScope, slug: &str, url: Option<String>, now: i64) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let webview_label = fresh_webview_label(&id);
        let (job_id, project_id) = match scope {
            BrowserScope::Job(job_id) => (Some(job_id.clone()), None),
            BrowserScope::Project(project_id) => (None, Some(project_id.clone())),
        };
        Self {
            id,
            job_id,
            project_id,
            slug: slug.to_string(),
            webview_label,
            url,
            title: None,
            status: STATUS_OPEN.to_string(),
            created_at: now,
            closed_at: None,
            // Storage-level construction does NOT imply user activation: this
            // constructor backs `ensure_open_browser`, which agent reads/actions
            // call to materialize a missing row. Stamping here would let an agent
            // create steal the focus-following alias from the user's active tab.
            // The user-facing create/navigate/focus paths stamp explicitly via
            // `touch_browser_active`.
            last_active_at: None,
        }
    }
}

const BROWSER_SELECT: &str = "
    SELECT id, job_id, project_id, slug, webview_label, url, title, status,
           created_at, closed_at, last_active_at
    FROM job_browsers";

fn browser_from_row(row: &cairn_db::turso::Row) -> DbResult<JobBrowser> {
    Ok(JobBrowser {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        project_id: row.opt_text(2)?,
        slug: row.text(3)?,
        webview_label: row.text(4)?,
        url: row.opt_text(5)?,
        title: row.opt_text(6)?,
        status: row.text(7)?,
        created_at: row.i64(8)?,
        closed_at: row.opt_i64(9)?,
        last_active_at: row.opt_i64(10)?,
    })
}

pub async fn insert_browser(db: &LocalDb, browser: JobBrowser) -> Result<(), String> {
    db.write(|conn| {
        let browser = browser.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO job_browsers (
                    id, job_id, project_id, slug, webview_label, url, title,
                    status, created_at, closed_at, last_active_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ",
                params![
                    browser.id.as_str(),
                    browser.job_id.as_deref(),
                    browser.project_id.as_deref(),
                    browser.slug.as_str(),
                    browser.webview_label.as_str(),
                    browser.url.as_deref(),
                    browser.title.as_deref(),
                    browser.status.as_str(),
                    browser.created_at,
                    browser.closed_at,
                    browser.last_active_at
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_browsers_by_scope(
    db: &LocalDb,
    scope: BrowserScope,
) -> Result<Vec<JobBrowser>, String> {
    db.read(|conn| {
        let scope = scope.clone();
        Box::pin(async move {
            let (where_clause, key) = match &scope {
                BrowserScope::Job(job_id) => ("WHERE job_id = ?1", job_id.clone()),
                BrowserScope::Project(project_id) => (
                    "WHERE project_id = ?1 AND job_id IS NULL",
                    project_id.clone(),
                ),
            };
            let sql = format!("{BROWSER_SELECT} {where_clause} ORDER BY created_at ASC");
            let mut rows = conn.query(&sql, params![key.as_str()]).await?;
            let mut browsers = Vec::new();
            while let Some(row) = rows.next().await? {
                browsers.push(browser_from_row(&row)?);
            }
            Ok(browsers)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_running_browsers(db: &LocalDb) -> Result<Vec<JobBrowser>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let sql = format!("{BROWSER_SELECT} WHERE status = ?1 ORDER BY created_at ASC");
            let mut rows = conn.query(&sql, params![STATUS_OPEN]).await?;
            let mut browsers = Vec::new();
            while let Some(row) = rows.next().await? {
                browsers.push(browser_from_row(&row)?);
            }
            Ok(browsers)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn get_browser(db: &LocalDb, id: &str) -> Result<Option<JobBrowser>, String> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let sql = format!("{BROWSER_SELECT} WHERE id = ?1 LIMIT 1");
            let mut rows = conn.query(&sql, params![id.as_str()]).await?;
            rows.next()
                .await?
                .map(|row| browser_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn find_browser_by_scope_and_slug(
    db: &LocalDb,
    scope: BrowserScope,
    slug: &str,
) -> Result<Option<JobBrowser>, String> {
    let slug = slug.to_string();
    db.read(|conn| {
        let scope = scope.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let (where_clause, key) = match &scope {
                BrowserScope::Job(job_id) => ("WHERE job_id = ?1 AND slug = ?2", job_id.clone()),
                BrowserScope::Project(project_id) => (
                    "WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2",
                    project_id.clone(),
                ),
            };
            let sql = format!("{BROWSER_SELECT} {where_clause} LIMIT 1");
            let mut rows = conn
                .query(&sql, params![key.as_str(), slug.as_str()])
                .await?;
            rows.next()
                .await?
                .map(|row| browser_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Find the most-recently USER-active OPEN browser in a scope, if any. Only rows
/// with a non-NULL `last_active_at` — ones a human actually activated — are
/// candidates; ties break by `created_at`. This is the focus-following lookup
/// behind the bare/default `cairn:~/browser` resource: it lands the agent on the
/// tab the user is looking at.
///
/// The `IS NOT NULL` filter is what makes focus-following safe alongside agent
/// activity. Agent-created rows (and rows predating the column) carry NULL and
/// are never picked, so an agent reading or creating an explicit missing browser
/// can't outrank the user's active tab. When no user-activated row exists the
/// caller falls back to the literal `default` slug — no worse than the prior
/// always-default behavior, and self-healing the moment the user activates a tab.
pub async fn find_most_recently_active_open_browser(
    db: &LocalDb,
    scope: BrowserScope,
) -> Result<Option<JobBrowser>, String> {
    db.read(|conn| {
        let scope = scope.clone();
        Box::pin(async move {
            let (where_clause, key) = match &scope {
                BrowserScope::Job(job_id) => ("WHERE job_id = ?1", job_id.clone()),
                BrowserScope::Project(project_id) => (
                    "WHERE project_id = ?1 AND job_id IS NULL",
                    project_id.clone(),
                ),
            };
            let sql = format!(
                "{BROWSER_SELECT} {where_clause} AND status = '{STATUS_OPEN}' \
                 AND last_active_at IS NOT NULL \
                 ORDER BY last_active_at DESC, created_at DESC LIMIT 1"
            );
            let mut rows = conn.query(&sql, params![key.as_str()]).await?;
            rows.next()
                .await?
                .map(|row| browser_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Stamp a browser's `last_active_at`. Called only from USER-initiated paths
/// (pane create/focus, URL-bar navigate) so focus-following resolution reflects
/// the user's attention; agent reads/actions never call this.
pub async fn touch_browser_active(db: &LocalDb, id: &str, now: i64) -> Result<(), String> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE job_browsers SET last_active_at = ?1 WHERE id = ?2",
                params![now, id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Atomically ensure an OPEN browser row exists for `(scope, slug)`, returning
/// the resolved row plus whether it was newly inserted (so callers can pick the
/// right db-change action). This is the zero-ceremony primitive behind the
/// always-works contract: it never errors on an existing slug.
///
/// The whole find-or-reopen-or-insert runs inside ONE `db.write` transaction.
/// Concurrent ensures for the same default slug (an agent read, the user pane,
/// and the startup reconcile can all race after a restart) therefore converge on
/// a single row: the loser's commit hits a write-write conflict, retries, and on
/// retry its fresh snapshot sees the committed row and takes the reuse branch.
/// The per-scope unique indexes (one row per scope+slug across all statuses) are
/// the storage-level backstop. When `url` is `Some` it overwrites the stored
/// url; when `None` the existing url is preserved.
pub async fn ensure_open_browser(
    db: &LocalDb,
    scope: BrowserScope,
    slug: &str,
    url: Option<String>,
    now: i64,
) -> Result<(JobBrowser, bool), String> {
    let slug = slug.to_string();
    db.write(|conn| {
        let scope = scope.clone();
        let slug = slug.clone();
        let url = url.clone();
        Box::pin(async move {
            let (where_clause, key) = match &scope {
                BrowserScope::Job(job_id) => ("WHERE job_id = ?1 AND slug = ?2", job_id.clone()),
                BrowserScope::Project(project_id) => (
                    "WHERE project_id = ?1 AND job_id IS NULL AND slug = ?2",
                    project_id.clone(),
                ),
            };
            let select = format!("{BROWSER_SELECT} {where_clause} LIMIT 1");
            let mut rows = conn
                .query(&select, params![key.as_str(), slug.as_str()])
                .await?;
            let existing = rows
                .next()
                .await?
                .map(|row| browser_from_row(&row))
                .transpose()?;
            drop(rows);

            match existing {
                Some(mut browser) => {
                    let webview_label = if browser.status == STATUS_CLOSED {
                        fresh_webview_label(&browser.id)
                    } else {
                        browser.webview_label.clone()
                    };
                    // Reopen-if-closed and overwrite-url-if-given in one UPDATE so
                    // the result is open regardless of the row's prior status. A
                    // reopened browser receives a new webview generation label.
                    conn.execute(
                        "UPDATE job_browsers
                         SET status = ?1, closed_at = NULL, url = COALESCE(?2, url), webview_label = ?3
                         WHERE id = ?4",
                        params![
                            STATUS_OPEN,
                            url.as_deref(),
                            webview_label.as_str(),
                            browser.id.as_str()
                        ],
                    )
                    .await?;
                    browser.status = STATUS_OPEN.to_string();
                    browser.closed_at = None;
                    browser.webview_label = webview_label;
                    if url.is_some() {
                        browser.url = url.clone();
                    }
                    Ok((browser, false))
                }
                None => {
                    let browser = JobBrowser::new(&scope, &slug, url.clone(), now);
                    conn.execute(
                        "
                        INSERT INTO job_browsers (
                            id, job_id, project_id, slug, webview_label, url, title,
                            status, created_at, closed_at, last_active_at
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                        ",
                        params![
                            browser.id.as_str(),
                            browser.job_id.as_deref(),
                            browser.project_id.as_deref(),
                            browser.slug.as_str(),
                            browser.webview_label.as_str(),
                            browser.url.as_deref(),
                            browser.title.as_deref(),
                            browser.status.as_str(),
                            browser.created_at,
                            browser.closed_at,
                            browser.last_active_at
                        ],
                    )
                    .await?;
                    Ok((browser, true))
                }
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Update the live url/title/status of a browser row from the app-side
/// navigation handlers. Each argument is `None` to leave that column untouched.
pub async fn update_browser_url_title(
    db: &LocalDb,
    id: &str,
    url: Option<String>,
    title: Option<String>,
    status: Option<String>,
) -> Result<(), String> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        let url = url.clone();
        let title = title.clone();
        let status = status.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE job_browsers
                 SET url = COALESCE(?1, url),
                     title = COALESCE(?2, title),
                     status = COALESCE(?3, status)
                 WHERE id = ?4",
                params![
                    url.as_deref(),
                    title.as_deref(),
                    status.as_deref(),
                    id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Mark a browser closed (keeps the row, sets status + closed_at).
pub async fn mark_browser_closed(db: &LocalDb, id: &str, now: i64) -> Result<(), String> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE job_browsers SET status = ?1, closed_at = ?2 WHERE id = ?3",
                params![STATUS_CLOSED, now, id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Delete a browser row outright (used by teardown).
pub async fn delete_browser(db: &LocalDb, id: &str) -> Result<(), String> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM job_browsers WHERE id = ?1",
                params![id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// All open node-scoped browsers for the given jobs (used by execution teardown
/// to close their live webviews and delete the rows).
pub async fn list_running_browsers_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<Vec<JobBrowser>, String> {
    if job_ids.is_empty() {
        return Ok(Vec::new());
    }
    let job_ids = job_ids.to_vec();
    db.read(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            let placeholders = (1..=job_ids.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "{BROWSER_SELECT} WHERE job_id IN ({placeholders}) AND status = '{STATUS_OPEN}' ORDER BY created_at ASC"
            );
            let params: Vec<cairn_db::turso::Value> = job_ids
                .iter()
                .map(|id| cairn_db::turso::Value::Text(id.clone()))
                .collect();
            let mut rows = conn.query(&sql, params).await?;
            let mut browsers = Vec::new();
            while let Some(row) = rows.next().await? {
                browsers.push(browser_from_row(&row)?);
            }
            Ok(browsers)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Delete every browser row owned by the given jobs.
pub async fn delete_all_browser_rows_for_jobs(
    db: &LocalDb,
    job_ids: &[String],
) -> Result<(), String> {
    if job_ids.is_empty() {
        return Ok(());
    }
    let job_ids = job_ids.to_vec();
    db.write(|conn| {
        let job_ids = job_ids.clone();
        Box::pin(async move {
            let placeholders = (1..=job_ids.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("DELETE FROM job_browsers WHERE job_id IN ({placeholders})");
            let params: Vec<cairn_db::turso::Value> = job_ids
                .iter()
                .map(|id| cairn_db::turso::Value::Text(id.clone()))
                .collect();
            conn.execute(&sql, params).await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The phase of a live webview navigation, broadcast on the orchestrator's
/// `browser_nav_events` channel. `Started` fires when a navigation begins
/// (the app's `on_navigation` handler); `Finished` fires when the page load
/// completes (`on_page_load` with `!loading`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NavPhase {
    Started,
    Finished,
}

/// A navigation lifecycle event for a live browser webview. Broadcast so a
/// cairn-core action can await a REAL navigation (click nav-confirmation,
/// `waitForNavigation`/`waitForLoad`) rather than guess from a fixed delay.
/// Plain data (no Tauri types), so it is safe to publish from the app handlers
/// and await in core. The app's nav/page-load closures publish; the interaction
/// path subscribes before triggering so a fast nav cannot race the awaiter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserNavEvent {
    pub browser_id: String,
    pub url: String,
    pub phase: NavPhase,
}

/// A plain-data command sent over the [`BrowserCommandTx`] channel from
/// cairn-core dispatch/teardown to the app-side drain task. Carries NO Tauri
/// types, so it is safe to hold on the Orchestrator (TRAP 1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BrowserCommand {
    Open {
        id: String,
        label: String,
        url: Option<String>,
    },
    /// Like [`BrowserCommand::Open`] but for a READ: the app replies on the
    /// reqId-keyed `browser_bridge_responses` broadcast once its open closure has
    /// run — the webview is now in the registry and, for a freshly created one,
    /// flagged loading. The readiness gate awaits this ack so it samples the
    /// loading flag AFTER the async open, closing the cold-rehydrate race where
    /// the drain would otherwise check liveness/loading before the open landed
    /// (and prime/ bridge a not-yet-registered, not-yet-loaded webview).
    OpenForRead {
        id: String,
        label: String,
        url: Option<String>,
        request_id: String,
    },
    Navigate {
        id: String,
        url: String,
    },
    Back {
        id: String,
    },
    Forward {
        id: String,
    },
    Reload {
        id: String,
        /// The page the row last settled on (`job_browsers.url`). The native
        /// handler reloads in place when the live webview is already on this page
        /// (preserving back/forward history) and re-navigates to it when the live
        /// location has drifted off the page, so a reload never strands the tab on
        /// the app shell `/`. `None` falls back to a bare native reload.
        url: Option<String>,
    },
    /// A page-content read or interaction request routed to the live webview's
    /// injected content script. `request_json` is a serialized [`BridgeRequest`];
    /// the app evals `window.__cairnBridge.dispatch(request_id, request_json, id)`
    /// and the page invokes `browser_bridge_message` back, keyed by `request_id`.
    Bridge {
        id: String,
        request_id: String,
        request_json: String,
    },
    /// Capture the live webview as a PNG via the host-native snapshot primitive.
    /// The host publishes the result on `browser_bridge_responses` keyed by
    /// `request_id` (a JSON `{ ok, dataB64, error }` payload), so a failed
    /// capture is a normal reply rather than a round-trip timeout. Independent of
    /// the page content-script bridge, so it works on `about:blank` too.
    Capture {
        id: String,
        request_id: String,
    },
    /// Clear the live webview's website data (cookies/cache/storage) via the
    /// host-native primitive. `kinds` are lowercase bucket names (`cookies`,
    /// `cache`, `storage`); empty means the host default (cookies+cache). The
    /// host publishes a JSON `{ ok, error }` reply on `browser_bridge_responses`
    /// keyed by `request_id`, mirroring [`BrowserCommand::Capture`].
    ClearData {
        id: String,
        request_id: String,
        kinds: Vec<String>,
    },
    /// Begin holding a hidden webview realized off-screen-but-visible for the
    /// lifetime of a content-script bridge round-trip, so a backgrounded pane's
    /// throttled `requestAnimationFrame`/timers run and the bridge's `postResult`
    /// can fire. Idempotent and refcounted host-side (paired Begin/End nest), so
    /// no `request_id`. On macOS the 0→1 transition realizes a hidden view
    /// off-screen and the 1→0 transition restores it; on a visible (on-screen)
    /// pane it is a no-op. Non-macOS hosts treat it as a no-op.
    BeginRenderPriming {
        id: String,
    },
    /// End one [`BrowserCommand::BeginRenderPriming`] hold (see its docs). The
    /// `dispatch_browser_bridge` RAII guard emits this on EVERY exit path so a
    /// webview can never be left rendering off-screen.
    EndRenderPriming {
        id: String,
    },
    Close {
        id: String,
        label: String,
    },
}

/// Sender half of the browser command channel. The app owns the receiver and
/// drains it into its `BrowserRegistry`.
pub type BrowserCommandTx = tokio::sync::mpsc::UnboundedSender<BrowserCommand>;

/// The extraction format for a [`BridgeRequest::Extract`]. `Markdown` serializes
/// the live DOM (`outerHTML`) for host-side htmd conversion; `Text` returns the
/// browser-computed `innerText` unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BridgeFormat {
    Markdown,
    Text,
}

/// Rich per-element descriptor captured by the content script's `describeElement`
/// at author time. The host is a pass-through: it carries these on the wire when
/// pushing the annotation set to the page. The descriptor is what lets the agent
/// identify the exact element ("the Sign In button in the top nav") when the
/// annotation is delivered inside the chat message (serialized alongside the
/// comment). Unknown JS fields (e.g. `rect`) are ignored on the way in.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aria_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_value: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_hint: Option<String>,
}

/// One annotation pushed host->page for rendering (and accepted as a Tauri
/// command arg from the frontend store). The durable record lives in the app
/// webview's localStorage; this is the projection the live page needs to draw
/// the highlight + numbered marker and to re-resolve the anchor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationWire {
    pub id: String,
    pub selector: String,
    pub snippet: String,
    pub comment: String,
    pub number: i64,
    #[serde(default)]
    pub meta: AnnotationMeta,
}

/// A typed bridge request. Serializes to the JSON the injected content script
/// dispatches on; `kind` is the discriminant for its single entrypoint. Built
/// host-side so callers never hand-format the wire JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BridgeRequest {
    /// Read live page content. `markdown` returns `outerHTML` (converted to
    /// markdown host-side); `text` returns `innerText`.
    Extract { format: BridgeFormat },
    /// Click an element resolved by CSS `selector`, visible `text`, or a
    /// `handle` (ref from a `ListInteractive` read, resolved via the anchor).
    Click {
        #[serde(skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        handle: Option<String>,
    },
    /// Set the value of an input/textarea/contenteditable; optionally submit.
    Type {
        #[serde(skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        handle: Option<String>,
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        submit: Option<bool>,
    },
    /// Scroll: `scrollIntoView` for a selector/text/handle, or window scroll via
    /// `to` (top|bottom) or a `by` pixel delta.
    Scroll {
        #[serde(skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        handle: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        to: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        by: Option<i64>,
    },
    /// Poll for `selector` up to `timeout_ms` milliseconds.
    WaitFor {
        selector: String,
        #[serde(rename = "timeoutMs")]
        timeout_ms: u64,
    },
    /// Toggle in-page annotate (element-pick) mode. Enabling arms the content
    /// script's pick handler + authoring pill and carries the per-session
    /// authoring `token` the page must echo on every annotation event — the host
    /// rejects events whose token does not match the one it minted for this pane,
    /// so untrusted page JS cannot forge user-authored annotations. Disabling
    /// tears the UI down (token omitted).
    SetAnnotateMode {
        enabled: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    /// Push the URL-matching annotation set so the page (re)renders the highlight
    /// + numbered markers, re-resolving each anchor (selector then text).
    RenderAnnotations { annotations: Vec<AnnotationWire> },
    /// Return the page's captured console ring buffer (oldest first), capped to
    /// the most recent `limit` entries when given.
    GetConsole {
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    /// List the page's actionable elements, each with an ordinal handle, a
    /// durable selector+snippet anchor, and a descriptor. Caps to `limit`
    /// (default 200 page-side).
    ListInteractive {
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
}

/// One actionable element from a [`BridgeRequest::ListInteractive`] read: an
/// ordinal handle (`ref`, e.g. `"e7"`) plus the durable anchor (selector +
/// snippet) and the rich descriptor. The handle resolves back through the
/// shared anchor ladder when used as a click/type/scroll locator. Reuses
/// [`AnnotationMeta`] verbatim — one descriptor type for annotations and
/// interaction alike.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InteractiveElement {
    #[serde(rename = "ref")]
    pub ref_: String,
    pub selector: String,
    pub snippet: String,
    #[serde(default)]
    pub meta: AnnotationMeta,
}

/// One captured console entry from the page's ring buffer. `ts` is the
/// `Date.now()` epoch-millis timestamp of the call; `stack` is present for
/// uncaught errors and unhandled rejections.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsoleEntry {
    pub ts: i64,
    pub level: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

/// The result the content script posts back over IPC, parsed from the
/// `browser_bridge_message` payload string. All fields are optional so a partial
/// or error result still deserializes.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BridgeResponse {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    /// Extracted page content (`outerHTML` for markdown, `innerText` for text).
    #[serde(default)]
    pub content: Option<String>,
    /// `"html"` or `"text"` — how `content` should be treated host-side.
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub matched: Option<bool>,
    #[serde(default)]
    pub found: Option<bool>,
    #[serde(default, rename = "timedOut")]
    pub timed_out: Option<bool>,
    /// Captured console entries for a `GetConsole` request.
    #[serde(default)]
    pub logs: Option<Vec<ConsoleEntry>>,
    /// Actionable elements for a `ListInteractive` request.
    #[serde(default)]
    pub elements: Option<Vec<InteractiveElement>>,
}

/// RAII bracket around a content-script bridge round-trip. Sends
/// [`BrowserCommand::BeginRenderPriming`] on construction and
/// [`BrowserCommand::EndRenderPriming`] on drop, so the off-screen render hold is
/// released on every exit path of [`dispatch_browser_bridge`] — a leaked Begin
/// would keep a hidden webview rendering off-screen indefinitely.
struct RenderPrimingGuard<'a> {
    tx: &'a BrowserCommandTx,
    id: String,
}

impl<'a> RenderPrimingGuard<'a> {
    fn begin(tx: &'a BrowserCommandTx, id: &str) -> Self {
        let _ = tx.send(BrowserCommand::BeginRenderPriming { id: id.to_string() });
        Self {
            tx,
            id: id.to_string(),
        }
    }
}

impl Drop for RenderPrimingGuard<'_> {
    fn drop(&mut self) {
        let _ = self.tx.send(BrowserCommand::EndRenderPriming {
            id: self.id.clone(),
        });
    }
}

/// Send a typed [`BridgeRequest`] to a live browser webview and await the
/// matching response (keyed by a freshly generated request id). The single
/// round-trip implementation: the resource read path and the Tauri app's
/// frontend-driven bridge commands both call here. Subscribe BEFORE sending so a
/// fast page reply cannot race the awaiter. Errors cleanly when no webview layer
/// is wired (headless/server) or the page does not reply within `timeout`.
pub async fn dispatch_browser_bridge(
    orch: &Orchestrator,
    browser_id: &str,
    request: &BridgeRequest,
    timeout: Duration,
) -> Result<BridgeResponse, String> {
    let tx = orch
        .browser_command_tx
        .as_ref()
        .ok_or_else(|| "no live webview on this host (headless/server)".to_string())?;
    let request_json = serde_json::to_string(request)
        .map_err(|error| format!("failed to encode bridge request: {error}"))?;
    let request_id = uuid::Uuid::new_v4().to_string();
    let mut rx = orch.browser_bridge_responses.subscribe();
    // Hold the webview realized off-screen for the whole round-trip so a
    // backgrounded pane still pumps frames and the page can `postResult`. The
    // guard's Drop emits EndRenderPriming on every exit path below (success,
    // page error, timeout, channel close, parse error) so the hold can't leak.
    // Begin is sent before Bridge; the in-order drain lands the realize first.
    let _priming = RenderPrimingGuard::begin(tx, browser_id);
    tx.send(BrowserCommand::Bridge {
        id: browser_id.to_string(),
        request_id: request_id.clone(),
        request_json,
    })
    .map_err(|error| format!("failed to dispatch bridge command: {error}"))?;

    let payload = tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Ok((resp_id, payload)) if resp_id == request_id => return Ok(payload),
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return Err("browser bridge channel closed".to_string()),
            }
        }
    })
    .await
    .map_err(|_| {
        "browser bridge timed out waiting for the page (is the webview live and loaded?)"
            .to_string()
    })??;

    serde_json::from_str::<BridgeResponse>(&payload)
        .map_err(|error| format!("invalid bridge response from page: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("browsers-crud.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_project(db: &LocalDb, project_id: &str) {
        let project_id = project_id.to_string();
        db.write(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES (?1, 'default', 'Browsers', 'BRW', '/tmp/brw', 1, 1)",
                    params![project_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn runner_restart_restores_open_browser_capture_generation() {
        let db = test_db().await;
        seed_project(&db, "p-restart").await;
        let scope = BrowserScope::Project("p-restart".to_string());
        let (browser, _) = ensure_open_browser(&db, scope, "main", None, 10)
            .await
            .unwrap();

        // A restarted runner has a fresh runtime archive while the desktop's
        // webview and its persisted open row retain the existing generation.
        let archive = crate::browser_network::BrowserNetworkArchive::default();
        assert_eq!(
            crate::browser_network::restore_open_generations(&db, &archive)
                .await
                .unwrap(),
            1
        );
        archive
            .insert_json_for_generation(
                &browser.id,
                &browser.webview_label,
                r#"{"id":"restart-1","ts":1,"method":"GET","url":"https://example.test/data"}"#,
                &crate::browser_network::RedactionPolicy::default(),
            )
            .unwrap();
        assert!(archive.get(&browser.id, "restart-1").is_some());
    }

    #[tokio::test]
    async fn reopening_rotates_the_webview_generation_label() {
        let db = test_db().await;
        seed_project(&db, "p-reopen").await;
        let scope = BrowserScope::Project("p-reopen".to_string());
        let (opened, inserted) = ensure_open_browser(&db, scope.clone(), "main", None, 10)
            .await
            .unwrap();
        assert!(inserted);
        mark_browser_closed(&db, &opened.id, 11).await.unwrap();
        let (reopened, inserted) = ensure_open_browser(&db, scope, "main", None, 12)
            .await
            .unwrap();
        assert!(!inserted);
        assert_eq!(reopened.id, opened.id);
        assert_ne!(reopened.webview_label, opened.webview_label);
    }

    #[tokio::test]
    async fn insert_and_list_by_project_scope() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let browser = JobBrowser::new(&scope, "main", Some("https://example.com".to_string()), 10);
        let label = browser.webview_label.clone();
        insert_browser(&db, browser).await.unwrap();

        let listed = list_browsers_by_scope(&db, scope).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].slug, "main");
        assert_eq!(listed[0].url.as_deref(), Some("https://example.com"));
        assert_eq!(listed[0].status, STATUS_OPEN);
        assert_eq!(listed[0].webview_label, label);
        assert!(label.starts_with("browser:"));
    }

    #[tokio::test]
    async fn ensure_open_browser_is_idempotent_and_reuses_one_row() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());

        let (first, inserted) = ensure_open_browser(&db, scope.clone(), "main", None, 10)
            .await
            .unwrap();
        assert!(inserted, "first ensure inserts");

        // A second ensure reuses the same row (no error, no duplicate) and can
        // overwrite the url.
        let (second, inserted) = ensure_open_browser(
            &db,
            scope.clone(),
            "main",
            Some("https://example.com".into()),
            20,
        )
        .await
        .unwrap();
        assert!(!inserted, "second ensure reuses");
        assert_eq!(second.id, first.id);
        assert_eq!(second.url.as_deref(), Some("https://example.com"));

        let listed = list_browsers_by_scope(&db, scope).await.unwrap();
        assert_eq!(listed.len(), 1, "exactly one row for the slug");
        assert_eq!(listed[0].url.as_deref(), Some("https://example.com"));
    }

    #[tokio::test]
    async fn ensure_open_browser_reopens_a_closed_row() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let (browser, _) = ensure_open_browser(&db, scope.clone(), "main", None, 10)
            .await
            .unwrap();
        mark_browser_closed(&db, &browser.id, 15).await.unwrap();

        let (reopened, inserted) = ensure_open_browser(&db, scope, "main", None, 20)
            .await
            .unwrap();
        assert!(!inserted, "reopen reuses the existing row");
        assert_eq!(reopened.id, browser.id);
        assert_eq!(reopened.status, STATUS_OPEN);
        let got = get_browser(&db, &browser.id).await.unwrap().unwrap();
        assert_eq!(got.status, STATUS_OPEN);
        assert_eq!(got.closed_at, None, "closed_at cleared on reopen");
    }

    #[tokio::test]
    async fn update_url_title_and_running_set() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let browser = JobBrowser::new(&scope, "main", None, 10);
        let id = browser.id.clone();
        insert_browser(&db, browser).await.unwrap();

        update_browser_url_title(
            &db,
            &id,
            Some("https://cairn.computer".to_string()),
            Some("Cairn".to_string()),
            None,
        )
        .await
        .unwrap();

        let got = get_browser(&db, &id).await.unwrap().unwrap();
        assert_eq!(got.url.as_deref(), Some("https://cairn.computer"));
        assert_eq!(got.title.as_deref(), Some("Cairn"));

        let running = list_running_browsers(&db).await.unwrap();
        assert_eq!(running.len(), 1);
    }

    #[tokio::test]
    async fn mark_closed_drops_from_running_set() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let browser = JobBrowser::new(&scope, "main", None, 10);
        let id = browser.id.clone();
        insert_browser(&db, browser).await.unwrap();

        mark_browser_closed(&db, &id, 20).await.unwrap();
        let running = list_running_browsers(&db).await.unwrap();
        assert!(running.is_empty());
        let got = get_browser(&db, &id).await.unwrap().unwrap();
        assert_eq!(got.status, STATUS_CLOSED);
        assert_eq!(got.closed_at, Some(20));
        let _ = scope;
    }

    #[tokio::test]
    async fn ensure_never_stamps_last_active() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());

        // The insert branch does NOT stamp: ensure_open_browser is the shared
        // agent path, so a fresh row is never user-active until a user path
        // (create/navigate/focus) calls touch_browser_active.
        let (browser, inserted) = ensure_open_browser(&db, scope.clone(), "main", None, 10)
            .await
            .unwrap();
        assert!(inserted);
        assert_eq!(browser.last_active_at, None, "insert does not stamp");

        // A later user activation stamps it.
        touch_browser_active(&db, &browser.id, 50).await.unwrap();
        // The reuse branch leaves the stamped value untouched (an agent ensure
        // must not move the user's activity signal).
        let (_, inserted) = ensure_open_browser(&db, scope, "main", None, 99)
            .await
            .unwrap();
        assert!(!inserted);
        let got = get_browser(&db, &browser.id).await.unwrap().unwrap();
        assert_eq!(got.last_active_at, Some(50), "reuse does not re-stamp");
    }

    /// The review's failure mode: an agent creating an explicit missing browser
    /// must not steal the focus-following alias from the user's active tab, even
    /// though it is created later (higher created_at). The NULL last_active_at on
    /// the agent row keeps it out of the candidate set entirely.
    #[tokio::test]
    async fn agent_insert_does_not_steal_focus_from_user_tab() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());

        // User-active tab at t=100.
        let (user_tab, _) = ensure_open_browser(&db, scope.clone(), "browser-a", None, 5)
            .await
            .unwrap();
        touch_browser_active(&db, &user_tab.id, 100).await.unwrap();

        // Agent creates an explicit missing browser LATER (t=200) — NULL stamp.
        let (scratch, _) = ensure_open_browser(&db, scope.clone(), "scratch", None, 200)
            .await
            .unwrap();
        assert_eq!(scratch.last_active_at, None);

        let found = find_most_recently_active_open_browser(&db, scope)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id, user_tab.id, "agent insert must not steal focus");
    }

    #[tokio::test]
    async fn touch_browser_active_updates_timestamp() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let (browser, _) = ensure_open_browser(&db, scope, "main", None, 10)
            .await
            .unwrap();
        touch_browser_active(&db, &browser.id, 50).await.unwrap();
        let got = get_browser(&db, &browser.id).await.unwrap().unwrap();
        assert_eq!(got.last_active_at, Some(50));
    }

    #[tokio::test]
    async fn find_most_recently_active_picks_recent_open_ignoring_closed() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());

        let (older, _) = ensure_open_browser(&db, scope.clone(), "tab-a", None, 10)
            .await
            .unwrap();
        let (newer, _) = ensure_open_browser(&db, scope.clone(), "tab-b", None, 20)
            .await
            .unwrap();
        // Stamp both as user-active (ensure no longer stamps on insert).
        touch_browser_active(&db, &older.id, 10).await.unwrap();
        touch_browser_active(&db, &newer.id, 20).await.unwrap();
        // tab-b is the most-recently active open browser.
        let found = find_most_recently_active_open_browser(&db, scope.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id, newer.id);
        assert_eq!(found.slug, "tab-b");

        // A closed-but-more-recent browser is ignored (status filter): close the
        // newest and the open older one wins.
        mark_browser_closed(&db, &newer.id, 30).await.unwrap();
        let found = find_most_recently_active_open_browser(&db, scope.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id, older.id);

        // No open browser at all ⇒ None.
        mark_browser_closed(&db, &older.id, 40).await.unwrap();
        let found = find_most_recently_active_open_browser(&db, scope)
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn bridge_request_serializes_to_content_script_json() {
        use super::{BridgeFormat, BridgeRequest};
        assert_eq!(
            serde_json::to_value(BridgeRequest::Extract {
                format: BridgeFormat::Markdown
            })
            .unwrap(),
            serde_json::json!({"kind": "extract", "format": "markdown"})
        );
        assert_eq!(
            serde_json::to_value(BridgeRequest::Extract {
                format: BridgeFormat::Text
            })
            .unwrap(),
            serde_json::json!({"kind": "extract", "format": "text"})
        );
        assert_eq!(
            serde_json::to_value(BridgeRequest::Click {
                selector: Some("button.go".into()),
                text: None,
                handle: None
            })
            .unwrap(),
            serde_json::json!({"kind": "click", "selector": "button.go"})
        );
        // A handle locator serializes; an absent one is omitted.
        assert_eq!(
            serde_json::to_value(BridgeRequest::Click {
                selector: None,
                text: None,
                handle: Some("e7".into())
            })
            .unwrap(),
            serde_json::json!({"kind": "click", "handle": "e7"})
        );
        assert_eq!(
            serde_json::to_value(BridgeRequest::Type {
                selector: Some("#q".into()),
                text: None,
                handle: None,
                value: "hi".into(),
                submit: Some(true)
            })
            .unwrap(),
            serde_json::json!({"kind": "type", "selector": "#q", "value": "hi", "submit": true})
        );
        assert_eq!(
            serde_json::to_value(BridgeRequest::Scroll {
                selector: None,
                text: None,
                handle: None,
                to: Some("bottom".into()),
                by: None
            })
            .unwrap(),
            serde_json::json!({"kind": "scroll", "to": "bottom"})
        );
        // ListInteractive carries an optional element cap.
        assert_eq!(
            serde_json::to_value(BridgeRequest::ListInteractive { limit: Some(120) }).unwrap(),
            serde_json::json!({"kind": "listInteractive", "limit": 120})
        );
        assert_eq!(
            serde_json::to_value(BridgeRequest::WaitFor {
                selector: ".ready".into(),
                timeout_ms: 3000
            })
            .unwrap(),
            serde_json::json!({"kind": "waitFor", "selector": ".ready", "timeoutMs": 3000})
        );
        assert_eq!(
            serde_json::to_value(BridgeRequest::GetConsole { limit: Some(50) }).unwrap(),
            serde_json::json!({"kind": "getConsole", "limit": 50})
        );
        // An absent limit is omitted from the wire JSON.
        assert_eq!(
            serde_json::to_value(BridgeRequest::GetConsole { limit: None }).unwrap(),
            serde_json::json!({"kind": "getConsole"})
        );
    }

    #[test]
    fn bridge_response_parses_console_buffer() {
        use super::BridgeResponse;
        let console: BridgeResponse = serde_json::from_str(
            r#"{"ok":true,"logs":[{"ts":1700000000000,"level":"error","message":"boom","stack":"at x"},{"ts":1700000000001,"level":"log","message":"hi"}]}"#,
        )
        .unwrap();
        let logs = console.logs.unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].level, "error");
        assert_eq!(logs[0].stack.as_deref(), Some("at x"));
        assert_eq!(logs[1].message, "hi");
        assert_eq!(logs[1].stack, None);
    }

    #[test]
    fn bridge_response_parses_partial_and_renamed_fields() {
        use super::BridgeResponse;
        let extracted: BridgeResponse =
            serde_json::from_str(r#"{"ok":true,"format":"html","content":"<html></html>"}"#)
                .unwrap();
        assert!(extracted.ok);
        assert_eq!(extracted.format.as_deref(), Some("html"));
        assert_eq!(extracted.content.as_deref(), Some("<html></html>"));

        let timed: BridgeResponse =
            serde_json::from_str(r#"{"ok":false,"found":false,"timedOut":true}"#).unwrap();
        assert!(!timed.ok);
        assert_eq!(timed.found, Some(false));
        assert_eq!(timed.timed_out, Some(true));
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let db = test_db().await;
        seed_project(&db, "p-brw").await;
        let scope = BrowserScope::Project("p-brw".to_string());
        let browser = JobBrowser::new(&scope, "main", None, 10);
        let id = browser.id.clone();
        insert_browser(&db, browser).await.unwrap();
        delete_browser(&db, &id).await.unwrap();
        assert!(get_browser(&db, &id).await.unwrap().is_none());
    }
}
