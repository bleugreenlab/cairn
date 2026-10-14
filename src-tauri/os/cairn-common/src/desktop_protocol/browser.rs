use serde::{Deserialize, Serialize};

/// The phase of a live webview navigation, broadcast on the orchestrator's
/// `browser_nav_events` channel. `Started` fires when a navigation begins
/// (the app's `on_navigation` handler); `Finished` fires when the page load
/// completes (`on_page_load` with `!loading`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NavPhase {
    Started,
    Finished,
}

/// A plain-data command sent over the browser command channel from
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
    /// injected content script. `request_json` is a serialized bridge request;
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

pub const MAX_CAPTURE_PAYLOAD_BYTES: usize = 256 * 1024;

pub const MAX_CAPTURES_PER_BATCH: usize = 128;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn browser_command_wire_keeps_camel_case_tags_and_snake_case_fields() {
        let cases = [
            (
                BrowserCommand::OpenForRead {
                    id: "b".into(),
                    label: "tab".into(),
                    url: None,
                    request_id: "r".into(),
                },
                json!({"kind":"openForRead", "id":"b", "label":"tab", "url":null, "request_id":"r"}),
            ),
            (
                BrowserCommand::Bridge {
                    id: "b".into(),
                    request_id: "r".into(),
                    request_json: "{}".into(),
                },
                json!({"kind":"bridge", "id":"b", "request_id":"r", "request_json":"{}"}),
            ),
            (
                BrowserCommand::ClearData {
                    id: "b".into(),
                    request_id: "r".into(),
                    kinds: vec!["cookies".into()],
                },
                json!({"kind":"clearData", "id":"b", "request_id":"r", "kinds":["cookies"]}),
            ),
        ];
        for (command, wire) in cases {
            assert_eq!(serde_json::to_value(&command).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<BrowserCommand>(wire).unwrap(),
                command
            );
        }
    }
}
