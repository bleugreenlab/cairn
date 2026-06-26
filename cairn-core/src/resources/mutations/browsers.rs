//! Browser resource mutations.
//!
//! Each arm resolves the resource to its scope, applies the durable row
//! mutation, and pushes a plain-data [`BrowserCommand`] over the orchestrator's
//! channel so the app-side drain task drives the live native webview. On hosts
//! without a webview layer (headless/server) the channel is `None` and only the
//! row mutation applies.

use std::time::Duration;

use cairn_common::uri::CairnResource;

use crate::browsers::{
    ensure_open_browser, find_browser_by_scope_and_slug, mark_browser_closed, BridgeRequest,
    BrowserCommand, JobBrowser,
};
use crate::orchestrator::Orchestrator;
use crate::resources::browsers::{
    await_browser_nav, browser_bridge_roundtrip, browser_clear_data_roundtrip,
    resolve_browser_target, BRIDGE_TIMEOUT,
};

/// Default page-side budget for a `waitFor` poll when the caller omits one.
const DEFAULT_WAIT_FOR_MS: u64 = 5000;

/// Default budget for `waitForNavigation`/`waitForLoad` when the caller omits one.
const DEFAULT_WAIT_NAV_MS: u64 = 10_000;

/// How long the host watches for a navigation after a click (or submit-typing)
/// before reporting "no navigation occurred". A heuristic window: long enough
/// for a real navigation to start, short enough not to stall a no-op click.
const NAV_CONFIRM_WINDOW: Duration = Duration::from_millis(1200);

/// Parsed interaction arguments for a browser patch action. Bundled so the
/// dispatch layer passes one value and `apply_browser_patch` stays under the
/// argument-count lint.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct BrowserInteractionArgs {
    pub selector: Option<String>,
    pub text: Option<String>,
    /// Element handle (ref e1..eN) from the last ?interactive read; a third
    /// locator for click/type/scroll, resolved via the durable anchor.
    pub handle: Option<String>,
    pub value: Option<String>,
    pub to: Option<String>,
    pub by: Option<i64>,
    pub timeout_ms: Option<u64>,
    pub submit: Option<bool>,
    pub kinds: Option<Vec<String>>,
}

fn emit_db_change(orch: &Orchestrator, action: &str) {
    if let Err(error) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_browsers", "action": action}),
    ) {
        log::error!("Failed to emit db-change event: {error}");
    }
}

fn send_command(orch: &Orchestrator, command: BrowserCommand) {
    if let Some(tx) = &orch.browser_command_tx {
        if let Err(error) = tx.send(command) {
            log::error!("Failed to send browser command: {error}");
        }
    }
}

/// Zero-ceremony create/navigate: ensure an open browser row for the resource
/// (idempotent, self-healing across restart) and drive the live webview to the
/// effective url. Routed from BOTH `mode=create` and `mode=patch` with a url, so
/// neither ever errors on an existing slug.
pub(super) async fn apply_browser_ensure(
    orch: &Orchestrator,
    resource: &CairnResource,
    url: Option<String>,
) -> Result<String, String> {
    let (scope, slug) = resolve_browser_target(&orch.db.local, resource).await?;
    let now = chrono::Utc::now().timestamp();
    let (browser, inserted) =
        ensure_open_browser(&orch.db.local, scope, &slug, url.clone(), now).await?;
    // Open is create-or-rehydrate-or-reuse host-side: it recreates a webview a
    // restart wiped (loading the stored url) and no-ops a live one. The follow-up
    // Navigate drives a live webview to the requested url (a harmless reload on a
    // freshly recreated one); both branches converge on the right page.
    send_command(
        orch,
        BrowserCommand::Open {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
            url: browser.url.clone(),
        },
    );
    if let Some(url) = url.clone() {
        send_command(
            orch,
            BrowserCommand::Navigate {
                id: browser.id.clone(),
                url,
            },
        );
    }
    emit_db_change(orch, if inserted { "insert" } else { "update" });
    Ok(match (inserted, &url) {
        (true, Some(url)) => format!("Opened browser '{slug}' at {url}"),
        (true, None) => format!("Opened browser '{slug}'"),
        (false, Some(url)) => format!("Navigated browser '{slug}' to {url}"),
        (false, None) => format!("Browser '{slug}' ready"),
    })
}

/// Drive a browser history/interaction action. Ensures the row is open and the
/// live webview exists (rehydrating one a restart wiped) before driving it, so
/// interactions work after a restart with zero ceremony too.
pub(crate) async fn apply_browser_action(
    orch: &Orchestrator,
    resource: &CairnResource,
    action: String,
    args: BrowserInteractionArgs,
) -> Result<String, String> {
    let (scope, slug) = resolve_browser_target(&orch.db.local, resource).await?;
    let now = chrono::Utc::now().timestamp();
    let (browser, _) = ensure_open_browser(&orch.db.local, scope, &slug, None, now).await?;
    send_command(
        orch,
        BrowserCommand::Open {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
            url: browser.url.clone(),
        },
    );
    // History actions are fire-and-forget host-native; interaction actions
    // round-trip through the injected content script for a real result.
    match action.as_str() {
        "back" => {
            send_command(orch, BrowserCommand::Back { id: browser.id.clone() });
            Ok(format!("Browser '{slug}': back"))
        }
        "forward" => {
            send_command(orch, BrowserCommand::Forward { id: browser.id.clone() });
            Ok(format!("Browser '{slug}': forward"))
        }
        "reload" => {
            // Carry the stored page so the host reloads the navigated url rather
            // than whatever the live webview drifted to (e.g. the app shell `/`).
            send_command(
                orch,
                BrowserCommand::Reload {
                    id: browser.id.clone(),
                    url: browser.url.clone(),
                },
            );
            Ok(format!("Browser '{slug}': reload"))
        }
        "click" | "type" | "scroll" | "waitFor" => {
            run_browser_interaction(orch, &browser, &slug, &action, args).await
        }
        // Host-observed waits: no content-script op, just await the nav channel.
        "waitForNavigation" => wait_for_navigation(orch, &browser, &slug, args, false).await,
        "waitForLoad" => wait_for_navigation(orch, &browser, &slug, args, true).await,
        "clearData" => {
            let kinds = args.kinds.unwrap_or_default();
            let summary = if kinds.is_empty() {
                "cookies + cache".to_string()
            } else {
                kinds.join(" + ")
            };
            browser_clear_data_roundtrip(orch, &browser.id, kinds, BRIDGE_TIMEOUT).await?;
            Ok(format!("Browser '{slug}': cleared {summary}"))
        }
        other => Err(format!(
            "unknown browser action '{other}'; expected back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad|clearData"
        )),
    }
}

/// Translate an interaction action + args into a typed [`BridgeRequest`] and the
/// host-side await budget. Pure (no orchestrator) so the arg validation is unit
/// testable. The `waitFor` budget exceeds the page's own timeout so the host
/// awaiter outlives the poll rather than reporting a false bridge timeout.
pub(crate) fn build_interaction_request(
    action: &str,
    args: BrowserInteractionArgs,
) -> Result<(BridgeRequest, Duration), String> {
    match action {
        "click" => {
            if args.selector.is_none() && args.text.is_none() && args.handle.is_none() {
                return Err("click requires selector, text, or handle".to_string());
            }
            Ok((
                BridgeRequest::Click {
                    selector: args.selector,
                    text: args.text,
                    handle: args.handle,
                },
                BRIDGE_TIMEOUT,
            ))
        }
        "type" => {
            let value = args
                .value
                .ok_or_else(|| "type requires value".to_string())?;
            if args.selector.is_none() && args.text.is_none() && args.handle.is_none() {
                return Err(
                    "type requires selector, text, or handle to locate the field".to_string(),
                );
            }
            Ok((
                BridgeRequest::Type {
                    selector: args.selector,
                    text: args.text,
                    handle: args.handle,
                    value,
                    submit: args.submit,
                },
                BRIDGE_TIMEOUT,
            ))
        }
        "scroll" => {
            if args.selector.is_none()
                && args.text.is_none()
                && args.handle.is_none()
                && args.to.is_none()
                && args.by.is_none()
            {
                return Err(
                    "scroll requires selector, text, handle, to (top|bottom), or by".to_string(),
                );
            }
            if let Some(to) = &args.to {
                if to != "top" && to != "bottom" {
                    return Err(format!("scroll to must be top or bottom, got '{to}'"));
                }
            }
            Ok((
                BridgeRequest::Scroll {
                    selector: args.selector,
                    text: args.text,
                    handle: args.handle,
                    to: args.to,
                    by: args.by,
                },
                BRIDGE_TIMEOUT,
            ))
        }
        "waitFor" => {
            let selector = args
                .selector
                .ok_or_else(|| "waitFor requires selector".to_string())?;
            let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_WAIT_FOR_MS);
            let budget = Duration::from_millis(timeout_ms.saturating_add(5000));
            Ok((
                BridgeRequest::WaitFor {
                    selector,
                    timeout_ms,
                },
                budget,
            ))
        }
        other => Err(format!("unknown browser interaction action '{other}'")),
    }
}

async fn run_browser_interaction(
    orch: &Orchestrator,
    browser: &JobBrowser,
    slug: &str,
    action: &str,
    args: BrowserInteractionArgs,
) -> Result<String, String> {
    // A click, or typing that submits, can trigger a navigation. Subscribe to
    // the nav channel BEFORE the bridge round-trip so a fast navigation cannot
    // race the awaiter; after the interaction lands we confirm (or deny) it.
    let expects_nav = action == "click" || (action == "type" && args.submit == Some(true));
    let mut nav_rx = expects_nav.then(|| orch.browser_nav_events.subscribe());

    let (request, timeout) = build_interaction_request(action, args)?;
    let response = browser_bridge_roundtrip(orch, &browser.id, &request, timeout).await?;
    if response.ok {
        let detail = match action {
            "waitFor" => "element appeared",
            "click" => "clicked",
            "type" => "typed",
            "scroll" => "scrolled",
            _ => "ok",
        };
        if let Some(rx) = nav_rx.as_mut() {
            return Ok(
                match await_browser_nav(rx, &browser.id, NAV_CONFIRM_WINDOW, false).await {
                    Some(url) => format!("Browser '{slug}': {detail}; navigated to {url}"),
                    None => format!("Browser '{slug}': {detail}; no navigation occurred"),
                },
            );
        }
        Ok(format!("Browser '{slug}': {detail}"))
    } else if response.timed_out == Some(true) {
        Err(format!(
            "Browser '{slug}': {action} timed out waiting for the selector"
        ))
    } else {
        Err(format!(
            "Browser '{slug}': {action} failed: {}",
            response
                .error
                .unwrap_or_else(|| "no element matched".to_string())
        ))
    }
}

/// Host-observed navigation wait. Subscribes to the orchestrator's nav channel
/// and awaits the next navigation for this browser up to the caller's
/// `timeoutMs` (default [`DEFAULT_WAIT_NAV_MS`]). `require_finished` scopes
/// `waitForLoad` to a completed page load; `waitForNavigation` resolves on the
/// first navigation start or finish. A clean timeout is a normal result, not an
/// error — "the page didn't navigate" is a valid answer.
async fn wait_for_navigation(
    orch: &Orchestrator,
    browser: &JobBrowser,
    slug: &str,
    args: BrowserInteractionArgs,
    require_finished: bool,
) -> Result<String, String> {
    let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_WAIT_NAV_MS);
    let mut rx = orch.browser_nav_events.subscribe();
    let timeout = Duration::from_millis(timeout_ms);
    match await_browser_nav(&mut rx, &browser.id, timeout, require_finished).await {
        Some(url) if require_finished => Ok(format!("Browser '{slug}': loaded {url}")),
        Some(url) => Ok(format!("Browser '{slug}': navigated to {url}")),
        None => {
            let what = if require_finished {
                "load"
            } else {
                "navigation"
            };
            Ok(format!("Browser '{slug}': no {what} within {timeout_ms}ms"))
        }
    }
}

pub(super) async fn apply_browser_delete(
    orch: &Orchestrator,
    resource: &CairnResource,
) -> Result<String, String> {
    // Focus-follow the bare/default form here too, so closing `cairn:~/browser`
    // targets the same tab a bare read/action resolves to rather than a literal
    // `default` row.
    let (scope, slug) = resolve_browser_target(&orch.db.local, resource).await?;
    let browser = find_browser_by_scope_and_slug(&orch.db.local, scope, &slug)
        .await?
        .ok_or_else(|| format!("No browser open at slug '{slug}'"))?;
    let now = chrono::Utc::now().timestamp();
    mark_browser_closed(&orch.db.local, &browser.id, now).await?;
    send_command(
        orch,
        BrowserCommand::Close {
            id: browser.id.clone(),
            label: browser.webview_label.clone(),
        },
    );
    emit_db_change(orch, "delete");
    Ok(format!("Closed browser '{slug}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn click_requires_a_locator() {
        assert!(build_interaction_request("click", BrowserInteractionArgs::default()).is_err());
        let (request, _) = build_interaction_request(
            "click",
            BrowserInteractionArgs {
                selector: Some("button".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            request,
            BridgeRequest::Click {
                selector: Some("button".into()),
                text: None,
                handle: None
            }
        );
    }

    #[test]
    fn handle_is_a_valid_locator() {
        // A handle alone satisfies the click locator requirement and threads
        // through to the bridge request.
        let (request, _) = build_interaction_request(
            "click",
            BrowserInteractionArgs {
                handle: Some("e7".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            request,
            BridgeRequest::Click {
                selector: None,
                text: None,
                handle: Some("e7".into())
            }
        );
    }

    #[test]
    fn type_requires_value_and_locator() {
        assert!(build_interaction_request(
            "type",
            BrowserInteractionArgs {
                selector: Some("#q".into()),
                ..Default::default()
            }
        )
        .is_err());
        let (request, _) = build_interaction_request(
            "type",
            BrowserInteractionArgs {
                selector: Some("#q".into()),
                value: Some("hi".into()),
                submit: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            request,
            BridgeRequest::Type {
                selector: Some("#q".into()),
                text: None,
                handle: None,
                value: "hi".into(),
                submit: Some(true)
            }
        );
    }

    #[test]
    fn scroll_validates_to_and_requires_some_target() {
        assert!(build_interaction_request("scroll", BrowserInteractionArgs::default()).is_err());
        assert!(build_interaction_request(
            "scroll",
            BrowserInteractionArgs {
                to: Some("sideways".into()),
                ..Default::default()
            }
        )
        .is_err());
        let (request, _) = build_interaction_request(
            "scroll",
            BrowserInteractionArgs {
                to: Some("bottom".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            request,
            BridgeRequest::Scroll {
                selector: None,
                text: None,
                handle: None,
                to: Some("bottom".into()),
                by: None
            }
        );
    }

    #[test]
    fn wait_for_requires_selector_and_budgets_over_timeout() {
        assert!(build_interaction_request("waitFor", BrowserInteractionArgs::default()).is_err());
        let (request, budget) = build_interaction_request(
            "waitFor",
            BrowserInteractionArgs {
                selector: Some(".ready".into()),
                timeout_ms: Some(3000),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            request,
            BridgeRequest::WaitFor {
                selector: ".ready".into(),
                timeout_ms: 3000
            }
        );
        assert!(budget > Duration::from_millis(3000));
    }

    use crate::browsers::{
        ensure_open_browser, list_browsers_by_scope, BrowserNavEvent, BrowserScope, NavPhase,
    };
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;
    use tokio::sync::mpsc::UnboundedReceiver;

    /// Build an orchestrator wired to a real browser command channel so tests can
    /// drain the `BrowserCommand`s the mutations emit. (The render-side harness
    /// uses `tx == None`; here we need to observe the commands.)
    async fn orch_with_tx() -> (
        Orchestrator,
        UnboundedReceiver<BrowserCommand>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let local = LocalDb::open(dir.path().join("browser-mut.db"))
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .browser_command_tx(Some(tx))
        .build();
        (orch, rx, dir)
    }

    fn drain(rx: &mut UnboundedReceiver<BrowserCommand>) -> Vec<BrowserCommand> {
        let mut out = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            out.push(cmd);
        }
        out
    }

    fn default_browser() -> CairnResource {
        CairnResource::ProjectBrowser {
            project: "BRW".into(),
            slug: "default".into(),
        }
    }

    #[tokio::test]
    async fn ensure_emits_open_is_idempotent_and_navigates() {
        let (orch, mut rx, _dir) = orch_with_tx().await;
        let resource = default_browser();

        let msg = apply_browser_ensure(&orch, &resource, None).await.unwrap();
        assert!(msg.contains("Opened browser 'default'"), "{msg}");
        let cmds = drain(&mut rx);
        assert!(
            matches!(cmds.as_slice(), [BrowserCommand::Open { .. }]),
            "create emits exactly one Open (the dead-webview rehydrate signal), got {cmds:?}"
        );

        // A second ensure reuses the row (no error) and, with a url, navigates.
        let msg = apply_browser_ensure(&orch, &resource, Some("https://example.com".into()))
            .await
            .unwrap();
        assert!(msg.contains("Navigated browser 'default'"), "{msg}");
        let cmds = drain(&mut rx);
        assert!(
            matches!(
                cmds.as_slice(),
                [BrowserCommand::Open { .. }, BrowserCommand::Navigate { .. }]
            ),
            "navigate emits Open then Navigate, got {cmds:?}"
        );

        let rows = list_browsers_by_scope(&orch.db.local, BrowserScope::Project("p-brw".into()))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "idempotent: exactly one row");
        assert_eq!(rows[0].url.as_deref(), Some("https://example.com"));
    }

    #[tokio::test]
    async fn action_ensures_live_before_driving() {
        let (orch, mut rx, _dir) = orch_with_tx().await;
        let msg = apply_browser_action(
            &orch,
            &default_browser(),
            "back".into(),
            BrowserInteractionArgs::default(),
        )
        .await
        .unwrap();
        assert!(msg.contains("back"), "{msg}");
        let cmds = drain(&mut rx);
        assert!(
            matches!(
                cmds.as_slice(),
                [BrowserCommand::Open { .. }, BrowserCommand::Back { .. }]
            ),
            "action ensures the webview is live (Open) before driving it, got {cmds:?}"
        );
    }

    /// The advertised browser action vocabulary (cairn-common's `BROWSER_ACTIONS`,
    /// surfaced in the structured affordance) must match the set
    /// `apply_browser_action` actually handles — every advertised verb is
    /// recognized, and an unadvertised verb is rejected. Pins the affordance so it
    /// can't drift back to under-advertising the interaction actions.
    #[tokio::test]
    async fn advertised_actions_match_handled_set() {
        let (orch, mut rx, _dir) = orch_with_tx().await;
        // clearData accepts empty args, so (unlike the interaction verbs) it
        // round-trips to the host instead of failing fast at validation. Drain
        // the command channel and auto-reply to its ClearData so the await
        // resolves promptly rather than waiting out the full bridge timeout.
        let replies = orch.browser_bridge_responses.clone();
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                if let BrowserCommand::ClearData { request_id, .. } = command {
                    let _ = replies.send((request_id, "{\"ok\":true}".to_string()));
                }
            }
        });
        for action in cairn_common::contract::BROWSER_ACTIONS {
            // Default (empty) args make interaction verbs fail fast at argument
            // validation, before any bridge round-trip — a recognized verb never
            // returns the "unknown browser action" error. The host-observed wait
            // verbs have no fail-fast validation, so give them a tiny timeout to
            // resolve the no-navigation case promptly instead of blocking.
            let args = if action.starts_with("waitForNav") || *action == "waitForLoad" {
                BrowserInteractionArgs {
                    timeout_ms: Some(20),
                    ..Default::default()
                }
            } else {
                BrowserInteractionArgs::default()
            };
            let result =
                apply_browser_action(&orch, &default_browser(), (*action).to_string(), args).await;
            let err = result.err().unwrap_or_default();
            assert!(
                !err.contains("unknown browser action"),
                "advertised action '{action}' is not handled by apply_browser_action: {err}"
            );
        }
        let err = apply_browser_action(
            &orch,
            &default_browser(),
            "frobnicate".into(),
            BrowserInteractionArgs::default(),
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("unknown browser action"),
            "an unadvertised verb must be rejected, got {err}"
        );
    }

    #[tokio::test]
    async fn reload_carries_the_stored_url_so_it_never_drifts_to_root() {
        let (orch, mut rx, _dir) = orch_with_tx().await;
        let resource = default_browser();
        // Navigate the tab to a real page so the row stores that url.
        apply_browser_ensure(&orch, &resource, Some("https://example.com/login".into()))
            .await
            .unwrap();
        drain(&mut rx);

        // A reload must carry the stored page, not an empty url that would let the
        // live webview reload wherever it drifted (e.g. the app shell `/`).
        apply_browser_action(
            &orch,
            &resource,
            "reload".into(),
            BrowserInteractionArgs::default(),
        )
        .await
        .unwrap();
        let cmds = drain(&mut rx);
        let reload_url = cmds.iter().find_map(|cmd| match cmd {
            BrowserCommand::Reload { url, .. } => Some(url.clone()),
            _ => None,
        });
        assert_eq!(
            reload_url,
            Some(Some("https://example.com/login".to_string())),
            "reload must carry the stored url, got {cmds:?}"
        );
    }

    #[tokio::test]
    async fn wait_for_navigation_times_out_cleanly() {
        let (orch, _rx, _dir) = orch_with_tx().await;
        let (browser, _) = ensure_open_browser(
            &orch.db.local,
            BrowserScope::Project("p-brw".into()),
            "default",
            None,
            10,
        )
        .await
        .unwrap();
        // No nav published: a clean timeout is a normal Ok result, not an error.
        let msg = wait_for_navigation(
            &orch,
            &browser,
            "default",
            BrowserInteractionArgs {
                timeout_ms: Some(20),
                ..Default::default()
            },
            false,
        )
        .await
        .unwrap();
        assert!(msg.contains("no navigation within 20ms"), "{msg}");
    }

    #[tokio::test]
    async fn wait_for_navigation_resolves_on_published_event() {
        let (orch, _rx, _dir) = orch_with_tx().await;
        let (browser, _) = ensure_open_browser(
            &orch.db.local,
            BrowserScope::Project("p-brw".into()),
            "default",
            None,
            10,
        )
        .await
        .unwrap();
        let tx = orch.browser_nav_events.clone();
        let id = browser.id.clone();
        let waiter = tokio::spawn(async move {
            wait_for_navigation(
                &orch,
                &browser,
                "default",
                BrowserInteractionArgs {
                    timeout_ms: Some(2000),
                    ..Default::default()
                },
                false,
            )
            .await
        });
        // Let the waiter subscribe before publishing so the event isn't missed.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(BrowserNavEvent {
            browser_id: id,
            url: "https://example.com/next".into(),
            phase: NavPhase::Started,
        });
        let msg = waiter.await.unwrap().unwrap();
        assert!(
            msg.contains("navigated to https://example.com/next"),
            "{msg}"
        );
    }
}
