//! One server, two transports.
//!
//! The same [`CairnMcpServer`] is served over a stdio-style byte stream and
//! over streamable HTTP, and every observable answer must match: the tool
//! inventory, each tool result (text, reminders, images), and the callback the
//! host receives. These tests are the behavioral half of the parity guarantee;
//! the structural half (no second tool router anywhere) lives in cairn-core's
//! source-structure suite.
use std::sync::Arc;

use axum::Router;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientInfo, ProtocolVersion, RequestMetaObject,
};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{ClientHandler, ClientLifecycleMode, ClientServiceExt, ServiceExt};

use crate::output::CallbackOutcome;
use crate::server::{CairnMcpServer, Caller};
use crate::sink::RunPrincipal;
use crate::test_support::RecordingSink;

const RUN: &str = "run-parity";

#[derive(Clone)]
struct VersionedClient(ProtocolVersion);

impl ClientHandler for VersionedClient {
    fn get_info(&self) -> ClientInfo {
        let mut info = ClientInfo::default();
        info.protocol_version = self.0.clone();
        info
    }
}

type Client = RunningService<RoleClient, VersionedClient>;

/// Serve a per-request server at `/mcp`, attaching `principal` to every request
/// the way the orchestrator's bearer middleware does.
async fn serve_http(sink: Arc<RecordingSink>, principal: Option<RunPrincipal>) -> String {
    let service = StreamableHttpService::new(
        move || Ok(CairnMcpServer::new(sink.clone(), Caller::PerRequest)),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );
    let mut router = Router::new().route_service("/mcp", service);
    if let Some(principal) = principal {
        router = router.layer(axum::Extension(principal));
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{address}/mcp")
}

async fn http_client(url: &str, version: ProtocolVersion) -> Client {
    VersionedClient(version)
        .serve(StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(url.to_string()),
        ))
        .await
        .expect("HTTP client initializes")
}

async fn stdio_client(sink: Arc<RecordingSink>) -> Client {
    let server = CairnMcpServer::new(
        sink,
        Caller::Fixed {
            run_id: Some(RUN.to_string()),
            cwd: String::new(),
        },
    );
    let (server_side, client_side) = tokio::io::duplex(1 << 20);
    tokio::spawn(async move {
        let _ = server
            .serve(crate::classic_stdio_transport(server_side))
            .await
            .unwrap()
            .waiting()
            .await;
    });
    VersionedClient(ProtocolVersion::V_2025_11_25)
        .serve(client_side)
        .await
        .expect("stdio client initializes")
}

fn args(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().unwrap().clone()
}

fn call(name: &'static str, arguments: serde_json::Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name).with_arguments(args(arguments))
}

fn as_json(result: &CallToolResult) -> serde_json::Value {
    serde_json::to_value(result).unwrap()
}

/// What the host answers every call with: an envelope carrying text and an
/// image, plus two reminders, so every post-processing step is exercised.
fn host_answer() -> CallbackOutcome {
    CallbackOutcome {
        result: serde_json::to_string(&cairn_common::read::ReadBatchEnvelope {
            text: "=== cairn:~/ ===\nbody".to_string(),
            images: vec![cairn_common::read::ImageBlock::inline(
                "image/png",
                "aGVsbG8=",
            )],
            segments: vec![],
            affordances: vec![],
            bodies: None,
        })
        .unwrap(),
        reminders: vec![
            "[Direct message from planner] hello".to_string(),
            "The worktree has uncommitted changes.".to_string(),
        ],
        transport_ok: true,
    }
}

#[tokio::test]
async fn http_and_stdio_serve_identical_tools_results_and_callbacks() {
    let stdio_sink = RecordingSink::answering(host_answer());
    let http_sink = RecordingSink::answering(host_answer());
    let stdio = stdio_client(stdio_sink.clone()).await;
    let url = serve_http(
        http_sink.clone(),
        Some(RunPrincipal {
            run_id: RUN.to_string(),
        }),
    )
    .await;
    let http = http_client(&url, ProtocolVersion::V_2025_11_25).await;

    let stdio_tools = serde_json::to_value(stdio.list_tools(None).await.unwrap().tools).unwrap();
    let http_tools = serde_json::to_value(http.list_tools(None).await.unwrap().tools).unwrap();
    assert_eq!(
        stdio_tools, http_tools,
        "tool names, descriptions, and schemas"
    );
    let names: Vec<&str> = http_tools
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 3);
    for verb in ["read", "write", "run"] {
        assert!(names.contains(&verb), "{names:?}");
    }

    let calls = [
        call(
            "read",
            serde_json::json!({"paths": ["cairn:~/", "file:src/lib.rs"]}),
        ),
        call(
            "run",
            serde_json::json!({"commands": [{"command": "hostname"}]}),
        ),
        call(
            "write",
            serde_json::json!({"changes": [{
                "target": "cairn:~/todos",
                "mode": "append",
                "payload": {"todos": [{"content": "x", "status": "pending"}]}
            }]}),
        ),
    ];
    for params in calls {
        let name = params.name.clone();
        let over_stdio = stdio.call_tool(params.clone()).await.unwrap();
        let over_http = http.call_tool(params).await.unwrap();
        assert_eq!(as_json(&over_stdio), as_json(&over_http), "{name}");
        let text = as_json(&over_http).to_string();
        assert!(text.contains("<system-reminder>"), "{name}: {text}");
    }
    // The image-bearing verbs lift the envelope's image into its own block.
    let read = http
        .call_tool(call("read", serde_json::json!({"paths": ["cairn:~/"]})))
        .await
        .unwrap();
    assert!(read.content.iter().any(|block| block.as_image().is_some()));

    // And the host saw the same callbacks, attributed to the same run.
    let strip = |sink: &RecordingSink| {
        sink.requests()
            .into_iter()
            .take(3)
            .map(|request| (request.run_id, request.tool, request.payload))
            .collect::<Vec<_>>()
    };
    assert_eq!(strip(&stdio_sink), strip(&http_sink));
    assert!(http_sink
        .requests()
        .iter()
        .all(|request| request.run_id.as_deref() == Some(RUN)));

    stdio.cancel().await.unwrap();
    http.cancel().await.unwrap();
}

#[tokio::test]
async fn http_initializes_at_the_protocol_versions_the_clis_use() {
    let sink = RecordingSink::answering(host_answer());
    let url = serve_http(
        sink.clone(),
        Some(RunPrincipal {
            run_id: RUN.to_string(),
        }),
    )
    .await;

    // Codex speaks 2025-06-18; Claude falls back to 2025-11-25.
    for version in [ProtocolVersion::V_2025_06_18, ProtocolVersion::V_2025_11_25] {
        let client = http_client(&url, version.clone()).await;
        assert_eq!(client.peer_info().unwrap().protocol_version, version);
        assert_eq!(client.list_tools(None).await.unwrap().tools.len(), 3);
        client.cancel().await.unwrap();
    }

    // Claude tries `server/discover` at 2026-07-28 first. The server declines
    // it, which is what sends the client to classic initialization.
    let auto = VersionedClient(ProtocolVersion::V_2025_11_25)
        .serve_with_lifecycle(
            StreamableHttpClientTransport::from_config(
                StreamableHttpClientTransportConfig::with_uri(url.clone()),
            ),
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: Some(ProtocolVersion::V_2025_11_25),
            },
        )
        .await
        .expect("an auto client falls back to classic initialization");
    assert_eq!(
        auto.peer_info().unwrap().protocol_version,
        ProtocolVersion::V_2025_11_25
    );
    assert_eq!(auto.list_tools(None).await.unwrap().tools.len(), 3);
    assert!(auto
        .list_resources(None)
        .await
        .unwrap()
        .resources
        .is_empty());
    let result = auto
        .call_tool(call("read", serde_json::json!({"paths": ["cairn:~/"]})))
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));
    assert_eq!(sink.requests().len(), 1);
    assert_eq!(sink.requests()[0].run_id.as_deref(), Some(RUN));
    auto.cancel().await.unwrap();
}

#[tokio::test]
async fn a_request_without_an_authenticated_run_is_refused() {
    let sink = RecordingSink::answering(host_answer());
    let url = serve_http(sink.clone(), None).await;
    let client = http_client(&url, ProtocolVersion::V_2025_11_25).await;

    let error = client
        .call_tool(call("read", serde_json::json!({"paths": ["cairn:~/"]})))
        .await
        .expect_err("a call with no principal must fail closed");
    let text = error.to_string();
    assert!(text.contains("no authenticated Cairn run"), "{text}");
    assert!(sink.requests().is_empty(), "nothing reached the host");
    client.cancel().await.unwrap();
}

#[tokio::test]
async fn a_client_chosen_thread_id_never_reaches_the_host_over_http() {
    let sink = RecordingSink::answering(host_answer());
    let url = serve_http(
        sink.clone(),
        Some(RunPrincipal {
            run_id: RUN.to_string(),
        }),
    )
    .await;
    let client = http_client(&url, ProtocolVersion::V_2025_11_25).await;

    let mut meta = RequestMetaObject::default();
    meta.insert(
        "threadId".to_string(),
        serde_json::Value::String("another-runs-thread".to_string()),
    );
    let mut params = call("read", serde_json::json!({"paths": ["cairn:~/"]}));
    params.meta = Some(meta);
    client.call_tool(params).await.unwrap();

    let request = sink.requests().pop().expect("the call reached the host");
    assert_eq!(request.run_id.as_deref(), Some(RUN));
    assert_eq!(request.thread_id, None);
    assert_eq!(request.cwd, "", "a remote caller reports no residence");
    client.cancel().await.unwrap();
}

/// Raw JSON-RPC: no client SDK may add metadata or reopen the connection.
#[tokio::test]
async fn discovery_fallback_lists_and_calls_on_the_same_stdio_connection() {
    stdio_wire_lifecycle(true, "2025-11-25").await;
}

#[tokio::test]
async fn direct_classic_stdio_lists_and_calls_without_inline_metadata() {
    for version in ["2024-11-05", "2025-06-18", "2025-11-25"] {
        stdio_wire_lifecycle(false, version).await;
    }
}

async fn stdio_wire_lifecycle(discover: bool, version: &str) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let sink = RecordingSink::answering(host_answer());
    let server = CairnMcpServer::new(
        sink.clone(),
        Caller::Fixed {
            run_id: Some(RUN.into()),
            cwd: String::new(),
        },
    );
    let (server_side, client_side) = tokio::io::duplex(1 << 20);
    let task = tokio::spawn(async move {
        server
            .serve(crate::classic_stdio_transport(server_side))
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (reader, mut writer) = tokio::io::split(client_side);
    let mut lines = BufReader::new(reader).lines();
    let requests = [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientCapabilities":{},
            "io.modelcontextprotocol/clientInfo":{"name":"wire-probe","version":"1"}
        }}}),
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{
            "protocolVersion":version,"capabilities":{},"clientInfo":{"name":"wire-probe","version":"1"}
        }}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}),
        serde_json::json!({"jsonrpc":"2.0","id":4,"method":"resources/list","params":{}}),
        serde_json::json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{
            "name":"read","arguments":{"paths":["cairn:~/"]}
        }}),
    ];
    for request in requests.into_iter().skip(usize::from(!discover)) {
        writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let line = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["id"], request["id"]);
        match request["id"].as_u64().unwrap() {
            1 => assert_eq!(response["error"]["code"], -32601, "{response}"),
            2 => {
                assert_eq!(response["result"]["protocolVersion"], version, "{response}");
                writer
                    .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
                    .await
                    .unwrap();
            }
            3 => {
                let mut names: Vec<_> = response["result"]["tools"]
                    .as_array()
                    .unwrap_or_else(|| panic!("tools/list failed: {response}"))
                    .iter()
                    .map(|tool| tool["name"].as_str().unwrap())
                    .collect();
                names.sort();
                assert_eq!(names, ["read", "run", "write"]);
            }
            4 => assert_eq!(
                response["result"]["resources"],
                serde_json::json!([]),
                "{response}"
            ),
            5 => {
                assert!(response.get("error").is_none(), "{response}");
                assert_eq!(response["result"]["isError"], false, "{response}");
            }
            _ => unreachable!(),
        }
    }
    let requests = sink.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].tool, "read_batch");
    assert_eq!(requests[0].run_id.as_deref(), Some(RUN));
    drop(writer);
    drop(lines);
    task.await.unwrap();
}
