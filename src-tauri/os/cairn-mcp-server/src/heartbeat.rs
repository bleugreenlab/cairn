//! Progress heartbeats for held-open tool calls.
//!
//! A Cairn tool call can stay open for hours: a `run` on declared timeouts, a
//! `waitFor`, a `write` waiting on its checks or on a sub-agent or a user's
//! answer. The Claude CLI abandons a call that has neither answered nor
//! reported progress within its idle bound (`CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT`,
//! by default 300 s over streamable HTTP and 1800 s over stdio), and a progress
//! notification resets that bound. So while a call that carried
//! `_meta.progressToken` is open, the server reports `notifications/progress`
//! on that token every [`HEARTBEAT_INTERVAL`]. A call without a token (Codex
//! sends none) gets no heartbeat, and a call the client has cancelled stops
//! beating.
//!
//! A heartbeat proves only that this server process is alive and still
//! waiting; it never extends a call. Cairn's own bounds still end it: the
//! callback socket bound in `cairn_common::callback_contract`, the run
//! ceiling, and parking.

use std::future::Future;
use std::time::Duration;

use rmcp::model::{ProgressNotificationParam, ProgressToken};
use rmcp::service::Peer;
use rmcp::RoleServer;
use tokio::time::{Instant, MissedTickBehavior};

/// How often a held call reports progress: well inside the Claude CLI's
/// shortest default idle bound (300 s, over streamable HTTP).
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Drive `work` to completion, reporting progress to `beat`'s peer on its
/// token every `every` until it finishes or the client cancels the request
/// (`beat`'s third member resolves). The first beat comes one interval in, so
/// a short call sends nothing.
///
/// The beats run inside this future rather than a spawned task, and each one
/// is awaited until the transport has taken it before `work` is polled again.
/// So when `work` finishes no beat is in flight, and the response can never be
/// preceded on the wire by a notification queued after it.
pub(crate) async fn with_heartbeat<F: Future, C: Future<Output = ()>>(
    work: F,
    beat: Option<(Peer<RoleServer>, ProgressToken, C)>,
    every: Duration,
    tool: &str,
) -> F::Output {
    let Some((peer, token, cancelled)) = beat else {
        return work.await;
    };
    let start = Instant::now();
    let mut ticker = tokio::time::interval_at(start + every, every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::pin!(work);
    tokio::pin!(cancelled);
    let mut beats: u32 = 0;
    loop {
        tokio::select! {
            biased;
            out = &mut work => return out,
            () = &mut cancelled => break,
            _ = ticker.tick() => {}
        }
        beats += 1;
        // Progress must rise with every notification, even with no total.
        let mut progress = ProgressNotificationParam::new(token.clone(), f64::from(beats));
        progress.message = Some(format!(
            "{tool} still running ({}s elapsed)",
            start.elapsed().as_secs()
        ));
        // A dead client is the call's own problem: a beat that cannot be
        // delivered never fails the call it accompanies.
        match tokio::time::timeout(every, peer.notify_progress(progress)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::debug!(%error, tool, "progress heartbeat not delivered"),
            Err(_) => tracing::debug!(tool, "progress heartbeat not taken within an interval"),
        }
    }
    // The client cancelled: to it the call is over, and a beat would name a
    // token it has forgotten. rmcp lets a cancelled handler run on (and drops
    // its response), so the work is still awaited, just silently.
    work.await
}
