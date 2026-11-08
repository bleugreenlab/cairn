//! The stdio server's route to the host: each tool call is POSTed to the
//! runner's `/api/mcp` callback with the machine's MCP secret.
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cairn_common::protocol::{CallbackRequest, CallbackResponse};
use cairn_mcp_server::output::{
    http_status_error_message, truncate_chars, CallbackFailure, CallbackFailureKind,
};
use cairn_mcp_server::{CairnMcpServer, CallbackOutcome, CallbackSink, Caller};

use cairn_common::callback_contract::{socket_timeout, stated_answer_bound_ms};

/// The server one `cairn-cmd` process speaks through: a fixed run identity
/// from its environment, with every call forwarded over the HTTP callback.
pub(crate) fn fixed_server(
    callback_url: String,
    cwd: String,
    run_id: Option<String>,
    mcp_secret: Option<String>,
) -> CairnMcpServer {
    CairnMcpServer::new(
        Arc::new(HttpCallbackSink {
            label: callback_url.clone(),
            callback_url,
            socket: None,
            mcp_secret,
            #[cfg(test)]
            timeout_override: None,
        }),
        Caller::Fixed { run_id, cwd },
    )
}

/// The server one `cairn-cmd` process speaks through when it was named a
/// lineage callback socket: every call goes to that socket's `/api/mcp` with no
/// credential, since the server identifies this process by its lineage.
#[cfg(unix)]
pub(crate) fn fixed_server_on_socket(
    socket: std::path::PathBuf,
    cwd: String,
    run_id: Option<String>,
) -> CairnMcpServer {
    CairnMcpServer::new(
        Arc::new(HttpCallbackSink {
            callback_url: SOCKET_CALLBACK_URL.to_string(),
            label: socket.display().to_string(),
            socket: Some(socket),
            mcp_secret: None,
            #[cfg(test)]
            timeout_override: None,
        }),
        Caller::Fixed { run_id, cwd },
    )
}

/// The request URL on a callback socket. The authority is never resolved; the
/// socket alone decides where the request goes.
#[cfg(unix)]
const SOCKET_CALLBACK_URL: &str = "http://localhost/api/mcp";

/// The lineage callback socket this process was named, if any. On Windows
/// there is none.
pub(crate) fn callback_socket() -> Option<std::path::PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os(cairn_common::lineage::CALLBACK_SOCKET_ENV)
            .filter(|path| !path.is_empty())
            .map(std::path::PathBuf::from)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

struct HttpCallbackSink {
    callback_url: String,
    /// Where calls go, as diagnostics name it: the URL, or the socket's path.
    label: String,
    /// The lineage callback socket every request is sent over, instead of
    /// resolving `callback_url`'s host.
    socket: Option<std::path::PathBuf>,
    /// Shared secret (base64-encoded string from env var, sent directly as bearer token)
    mcp_secret: Option<String>,
    /// Replaces the contract socket bound in tests that need a short one.
    #[cfg(test)]
    timeout_override: Option<Duration>,
}

impl HttpCallbackSink {
    fn socket_bound(&self, request: &CallbackRequest) -> Duration {
        #[cfg(test)]
        if let Some(bound) = self.timeout_override {
            return bound;
        }
        socket_timeout(request)
    }
}

fn failed(result: String, kind: CallbackFailureKind, elapsed: Duration) -> CallbackOutcome {
    CallbackOutcome {
        result,
        failure: Some(CallbackFailure { kind, elapsed }),
        ..Default::default()
    }
}

/// The class of a send failure, judged from the error and its whole source
/// chain. reqwest's own `Display` for every one of these is the same
/// "error sending request for url (...)", so the class must come from here.
fn send_failure_kind(error: &reqwest::Error) -> CallbackFailureKind {
    if error.is_connect() {
        return CallbackFailureKind::Connect;
    }
    if error.is_timeout() {
        return CallbackFailureKind::Timeout;
    }
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            ) {
                return CallbackFailureKind::Reset;
            }
        }
        if cause
            .to_string()
            .contains("connection closed before message completed")
        {
            return CallbackFailureKind::Reset;
        }
        source = cause.source();
    }
    if error.is_body() {
        return CallbackFailureKind::RequestBody;
    }
    CallbackFailureKind::Request
}

/// The whole source chain of an error, innermost last, each distinct message
/// once (hyper and reqwest often repeat their child's text).
fn source_chain(error: &reqwest::Error) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        let text = cause.to_string();
        if !parts.iter().any(|part| part == &text) {
            parts.push(text);
        }
        source = cause.source();
    }
    if parts.is_empty() {
        "no further cause".to_string()
    } else {
        parts.join(": ")
    }
}

/// A send failure as text an agent and a log can both classify:
/// `Error calling Tauri: <class>: <cause chain> [after <elapsed>; <reqwest>]`.
fn send_failure(error: &reqwest::Error, elapsed: Duration) -> (CallbackFailureKind, String) {
    let kind = send_failure_kind(error);
    let text = format!(
        "Error calling Tauri: {}: {} [after {:.1}s; {}]",
        kind.word(),
        source_chain(error),
        elapsed.as_secs_f64(),
        error
    );
    (kind, text)
}

#[async_trait]
impl CallbackSink for HttpCallbackSink {
    fn describe(&self) -> &str {
        &self.label
    }

    async fn call(&self, request: &CallbackRequest) -> CallbackOutcome {
        let started = Instant::now();
        #[allow(unused_mut)]
        let mut builder = reqwest::Client::builder().timeout(self.socket_bound(request));
        #[cfg(unix)]
        if let Some(socket) = &self.socket {
            builder = builder.unix_socket(socket.as_path());
        }
        let client = match builder.build() {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("Failed to build HTTP client: {}", e);
                return failed(
                    format!("Error building HTTP client: {}", e),
                    CallbackFailureKind::ClientBuild,
                    started.elapsed(),
                );
            }
        };
        // State the bound this socket was sized from, so the host answers
        // inside it (the socket itself adds the relay and hop margins).
        let mut request = request.clone();
        request.answer_bound_ms = stated_answer_bound_ms(&request);
        let request = &request;
        let mut req = client.post(self.callback_url.as_str()).json(request);

        if let Some(secret) = &self.mcp_secret {
            req = req.header("Authorization", format!("Bearer {}", secret));
        }

        let request_bytes = serde_json::to_vec(request).map(|v| v.len()).unwrap_or(0);
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let http_ok = status.is_success();
                match resp.text().await {
                    // A non-2xx callback is reported by its status and request
                    // size, since a proxy's or the body limit's answer (HTTP 413)
                    // is no `CallbackResponse`. A refusal the runner itself
                    // writes (a 401 naming what a credential may do) is one, and
                    // its sentence is the only account of why, so it is kept.
                    Ok(text) if !http_ok => {
                        tracing::error!(
                            "MCP callback returned HTTP {} for tool {}: {}",
                            status,
                            request.tool,
                            truncate_chars(&text, 500)
                        );
                        let mut message = http_status_error_message(
                            status.as_u16(),
                            status.canonical_reason().unwrap_or("error"),
                            request_bytes,
                        );
                        if let Some(refusal) = serde_json::from_str::<CallbackResponse>(&text)
                            .ok()
                            .map(|response| response.result)
                            .filter(|result| !result.trim().is_empty())
                        {
                            message.push_str(" The runner said: ");
                            message.push_str(&refusal);
                        }
                        failed(
                            message,
                            CallbackFailureKind::HttpStatus(status.as_u16()),
                            started.elapsed(),
                        )
                    }
                    Ok(text) => match serde_json::from_str::<CallbackResponse>(&text) {
                        Ok(r) => CallbackOutcome {
                            result: r.result,
                            reminders: r.reminders,
                            transport_ok: http_ok,
                            failure: None,
                        },
                        Err(e) => {
                            tracing::error!(
                                "Failed to parse response (status {}): {} - body: {}",
                                status,
                                e,
                                truncate_chars(&text, 500)
                            );
                            failed(
                                format!(
                                    "Error parsing response: {} (body: {})",
                                    e,
                                    truncate_chars(&text, 200)
                                ),
                                CallbackFailureKind::ResponseParse,
                                started.elapsed(),
                            )
                        }
                    },
                    Err(e) => {
                        let (_, detail) = send_failure(&e, started.elapsed());
                        failed(
                            format!("Error reading response body: {detail}"),
                            CallbackFailureKind::ResponseBody,
                            started.elapsed(),
                        )
                    }
                }
            }
            Err(e) => {
                let elapsed = started.elapsed();
                let (kind, text) = send_failure(&e, elapsed);
                tracing::error!("MCP callback for tool {} failed: {text}", request.tool);
                let stopped = (kind == CallbackFailureKind::Connect)
                    .then(|| crate::cli::operator_stop_explanation(&self.callback_url))
                    .flatten();
                failed(stopped.unwrap_or(text), kind, elapsed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    fn sink(url: String, timeout: Option<Duration>) -> HttpCallbackSink {
        HttpCallbackSink {
            label: url.clone(),
            callback_url: url,
            socket: None,
            mcp_secret: None,
            timeout_override: timeout,
        }
    }

    /// A process named a callback socket reaches it, and presents nothing: the
    /// request on the wire carries no `Authorization` header, even with a
    /// credential in this process's environment.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_socket_call_carries_no_credential_and_is_answered() {
        use tokio::io::AsyncWriteExt;
        // Short, since macOS caps a socket path at 104 bytes.
        let dir = std::path::PathBuf::from(format!("/tmp/cs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 4096];
            while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                assert!(read > 0, "the request ended early");
                seen.extend_from_slice(&buf[..read]);
            }
            let body = br#"{"result":"answered"}"#;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(body).await.unwrap();
            String::from_utf8_lossy(&seen).to_ascii_lowercase()
        });
        let sink = HttpCallbackSink {
            callback_url: SOCKET_CALLBACK_URL.to_string(),
            label: path.display().to_string(),
            socket: Some(path.clone()),
            mcp_secret: None,
            timeout_override: Some(Duration::from_secs(10)),
        };
        assert_eq!(sink.describe(), path.display().to_string());
        let answered = sink.call(&request()).await;
        assert!(answered.failure.is_none(), "{}", answered.result);
        assert_eq!(answered.result, "answered");
        let head = server.await.unwrap();
        assert!(head.starts_with("post /api/mcp "), "{head}");
        assert!(!head.contains("authorization"), "{head}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn request() -> CallbackRequest {
        CallbackRequest {
            cwd: "/tmp".into(),
            tool: "run".into(),
            payload: serde_json::json!({"commands": [{"command": "true"}]}),
            ..Default::default()
        }
    }

    /// A port nothing listens on: bind, read the port, and close.
    async fn refused_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}/api/mcp")
    }

    /// A server that accepts one connection, reads a little of the request,
    /// and then does whatever `respond` does with the socket.
    async fn serving<F>(respond: F) -> String
    where
        F: FnOnce(
                tokio::net::TcpStream,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = stream.read(&mut buf).await;
            respond(stream).await;
        });
        format!("http://{addr}/api/mcp")
    }

    async fn outcome(url: String, timeout: Option<Duration>) -> CallbackOutcome {
        sink(url, timeout).call(&request()).await
    }

    fn kind(outcome: &CallbackOutcome) -> CallbackFailureKind {
        outcome.failure.expect("a failure").kind
    }

    #[tokio::test]
    async fn a_refused_connection_is_connect() {
        let refused = outcome(refused_url().await, None).await;
        assert_eq!(kind(&refused), CallbackFailureKind::Connect);
        assert!(!refused.transport_ok);
    }

    #[tokio::test]
    async fn a_peer_that_accepts_then_drops_is_reset() {
        let url = serving(|stream| Box::pin(async move { drop(stream) })).await;
        let dropped = outcome(url, None).await;
        assert_eq!(
            kind(&dropped),
            CallbackFailureKind::Reset,
            "{}",
            dropped.result
        );
    }

    #[tokio::test]
    async fn a_peer_that_never_answers_is_timeout() {
        let url = serving(|stream| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                drop(stream);
            })
        })
        .await;
        let silent = outcome(url, Some(Duration::from_millis(100))).await;
        assert_eq!(
            kind(&silent),
            CallbackFailureKind::Timeout,
            "{}",
            silent.result
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_linger_zero_close_is_reset() {
        use std::os::fd::AsRawFd;
        let url = serving(|stream| {
            Box::pin(async move {
                let linger = libc::linger {
                    l_onoff: 1,
                    l_linger: 0,
                };
                // SAFETY: a valid socket fd and a correctly sized option value.
                unsafe {
                    libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_LINGER,
                        &linger as *const libc::linger as *const libc::c_void,
                        std::mem::size_of::<libc::linger>() as libc::socklen_t,
                    );
                }
                drop(stream);
            })
        })
        .await;
        let reset = outcome(url, None).await;
        assert_eq!(kind(&reset), CallbackFailureKind::Reset, "{}", reset.result);
    }

    #[tokio::test]
    async fn every_class_renders_distinctly_with_its_cause_and_elapsed() {
        let refused = outcome(refused_url().await, None).await;
        let dropped = outcome(
            serving(|stream| Box::pin(async move { drop(stream) })).await,
            None,
        )
        .await;
        let silent = outcome(
            serving(|stream| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    drop(stream);
                })
            })
            .await,
            Some(Duration::from_millis(100)),
        )
        .await;
        let renderings = [&refused, &dropped, &silent].map(|o| o.result.clone());
        for (text, class) in renderings.iter().zip(["connect", "reset", "timeout"]) {
            // The operator-stop override may replace a connect failure's text
            // on a machine with a stop marker; the class still holds.
            if class == "connect" && !text.starts_with("Error calling Tauri:") {
                continue;
            }
            assert!(
                text.starts_with(&format!("Error calling Tauri: {class}: ")),
                "{text}"
            );
            assert!(text.contains("[after "), "{text}");
            assert!(text.contains("error sending request for url"), "{text}");
            let cause = text
                .split_once(&format!("{class}: "))
                .map(|(_, rest)| rest)
                .unwrap();
            assert!(!cause.starts_with("no further cause"), "{text}");
        }
        assert_ne!(renderings[0], renderings[1]);
        assert_ne!(renderings[1], renderings[2]);
        assert_ne!(renderings[0], renderings[2]);
    }

    #[tokio::test]
    async fn a_non_2xx_answer_is_http_status() {
        let url = serving(|mut stream| {
            Box::pin(async move {
                use tokio::io::AsyncWriteExt;
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 504 Gateway Timeout\r\ncontent-length: 2\r\nconnection: close\r\n\r\nno",
                    )
                    .await;
            })
        })
        .await;
        let gateway = outcome(url, None).await;
        assert_eq!(kind(&gateway), CallbackFailureKind::HttpStatus(504));
        assert!(
            !gateway.result.contains("The runner said"),
            "{}",
            gateway.result
        );
    }

    #[tokio::test]
    async fn a_refusal_the_runner_writes_reaches_the_caller() {
        let url = serving(|mut stream| {
            Box::pin(async move {
                use tokio::io::AsyncWriteExt;
                // Closing with request bytes still unread resets the
                // connection, which the client may see before the answer.
                let mut seen = Vec::new();
                let mut buf = [0u8; 4096];
                while !seen.ends_with(b"}") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => seen.extend_from_slice(&buf[..n]),
                    }
                }
                let body = r#"{"result":"the machine credential speaks for no run"}"#;
                let head = format!(
                    "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body.as_bytes()).await;
            })
        })
        .await;
        let refused = outcome(url, None).await;
        assert_eq!(kind(&refused), CallbackFailureKind::HttpStatus(401));
        assert!(refused.result.contains("HTTP 401"), "{}", refused.result);
        assert!(
            refused
                .result
                .ends_with("The runner said: the machine credential speaks for no run"),
            "{}",
            refused.result
        );
    }

    #[tokio::test]
    async fn the_request_states_the_bound_its_socket_was_sized_from() {
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let mut seen = String::new();
            while !seen.contains("answer_bound_ms") {
                let n = stream.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                seen.push_str(&String::from_utf8_lossy(&buf[..n]));
            }
            let _ = tx.send(seen);
        });
        let _ = outcome(
            format!("http://{addr}/api/mcp"),
            Some(Duration::from_millis(500)),
        )
        .await;
        let body = rx.await.unwrap();
        let expected = stated_answer_bound_ms(&request()).unwrap();
        assert!(
            body.contains(&format!("\"answer_bound_ms\":{expected}")),
            "{body}"
        );
    }
}
