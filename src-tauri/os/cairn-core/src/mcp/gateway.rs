//! External MCP gateway trait.
//!
//! Cairn is an MCP *client/gateway*: a spawned agent reaches configured
//! external servers (Playwright, Linear, ...) through the `cairn://mcp/...` URI
//! family without those servers being injected into the agent's own MCP config.
//!
//! Core stays rmcp-free. The trait is defined here; the concrete rmcp client
//! implementation lives in the host crates (Tauri app, cairn-server) and is set
//! on the `Orchestrator` after construction, mirroring `EffectExecutor`.
//!
//! ## Connection model
//!
//! Implementations pool connections keyed by `(session_key, server)`. The
//! `session_key` is the run's job id, so two concurrent agents each get their
//! own server process (isolation — e.g. separate Playwright browsers).
//! Connections are spawned lazily on first use, kept warm across calls within a
//! session, and torn down via `close_session` when the job completes.

use async_trait::async_trait;
use cairn_common::read::ImageBlock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::security::BrokeredMcpConfig;

/// A bidirectional byte stream suitable for an MCP client transport.
pub trait DuplexIo: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> DuplexIo for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

/// Factory for a machine-local MCP facade reached through service placement.
///
/// The gateway asks for a stream only when its machine-scoped pool has no live
/// connection. Core owns placement; transport owns the MCP protocol lifecycle.
#[async_trait]
pub trait PlacedFacade: Send + Sync {
    /// Stable machine identity used by the gateway connection pool.
    fn key(&self) -> &str;

    /// Open a fresh byte stream to the facade process on that machine.
    async fn open(&self) -> Result<Box<dyn DuplexIo>, String>;
}

/// The result of a proxied external MCP `tools/call`.
///
/// `text` is the flattened textual content. `images` carries any image content
/// blocks the server returned, preserved rather than flattened to a placeholder
/// so they can reach the agent as real image content blocks — mirroring the read
/// path's text/image split (`cairn_common::read`).
///
/// `links` carries the resources the server named by *reference* — MCP
/// `resource_link` content blocks. A reference stays a reference: its bytes are
/// never inlined, nothing fetches it while the result is rendered or forwarded,
/// and the metadata the server supplied rides along so the agent can judge
/// whether retrieving it (an explicit later `read`) is worth it. The same
/// reference is also rendered into `text` at the position it occupied, so text
/// and link blocks keep their relative order. Image blocks do not participate in
/// that ordering: they are lifted out of the stream here and delivered after the
/// composed text, which is how they have always reached the agent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct McpToolCallResult {
    pub text: String,
    pub images: Vec<ImageBlock>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<McpResourceDef>,
}

impl McpToolCallResult {
    /// A text-only result: what a producer with nothing to carry alongside the
    /// text returns.
    pub fn from_text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            ..Self::default()
        }
    }
}

/// One protocol round of an external MCP tool call. Intermediate outcomes are
/// deliberately serializable: core persists them before yielding the agent turn,
/// then a later host can reconnect and continue without retaining an rmcp value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpCallOutcome {
    Complete(McpToolCallResult),
    InputRequired {
        input_requests: serde_json::Value,
        request_state: Option<String>,
    },
    Task {
        task_id: String,
        poll_interval_ms: Option<u64>,
        ttl_ms: Option<u64>,
    },
}

/// Durable result of polling an MCP task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum McpTaskOutcome {
    Working { poll_interval_ms: Option<u64> },
    InputRequired { input_requests: serde_json::Value },
    Complete(McpToolCallResult),
    Failed { message: String },
    Cancelled,
}

/// A tool advertised by an external MCP server.
///
/// `Deserialize` lets the persisted MCP tool store (`config::mcp_tools`) round-
/// trip these defs through its sidecar JSON file, so the agent affordance block
/// can render terse per-tool contracts synchronously without spawning servers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolDef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema for the tool's arguments (the `inputSchema`), surfaced so the
    /// agent can construct a correct `run` payload.
    pub input_schema: serde_json::Value,
}

/// A complete `tools/list` catalog and its server-provided cache hints.
#[derive(Debug, Clone, Default)]
pub struct McpToolCatalog {
    pub tools: Vec<McpToolDef>,
    pub ttl_ms: Option<u64>,
    pub cache_scope: Option<String>,
}

/// A resource an external MCP server named rather than inlined.
///
/// One mirror of MCP's `Resource` shape serves both places a server names one:
/// a `resources/list` entry, and a `resource_link` content block inside a tool
/// result. They are the same schema and the same thing — an address plus what
/// the server is willing to say about it — so they get one type rather than two
/// that drift.
///
/// Every field is untrusted server text that ends up in a transcript, so bounds
/// are applied once at the adapter that builds one ([`McpResourceDef::bounded`])
/// rather than at each place that renders one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpResourceDef {
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Size in bytes of the resource's raw content, as *declared* by the server.
    /// Declared, never measured: nothing on this path reads the payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Set when the server's URI exceeded [`MAX_RESOURCE_URI_CHARS`], leaving
    /// `uri` holding a prefix kept only so the reference can be recognized in a
    /// transcript. Such a reference names something without addressing it: a
    /// prefix is not a shorter address, it is a different one, and asking a
    /// server for it would ask for something it never named. Nothing may present
    /// the `uri` of such a reference as a retrieval address — see
    /// [`McpResourceDef::is_retrievable`].
    #[serde(default, skip_serializing_if = "is_false")]
    pub uri_truncated: bool,
}

/// Bounds on the untrusted text one resource reference may carry into a
/// transcript. A reference is a fixed-cost object: nothing about it may scale
/// with the payload it names, and a `data:` URI is exactly how an unbounded one
/// would arrive.
///
/// The URI bound is set far above any real address on purpose. A presigned or
/// bearer-capability URL can be long, and shortening one destroys it, so the
/// bound is not a display limit — it is the line past which a string has stopped
/// being an address and started being a payload wearing a `data:` prefix.
/// Crossing it withdraws the address rather than shortening it.
const MAX_RESOURCE_URI_CHARS: usize = 8192;
const MAX_RESOURCE_LABEL_CHARS: usize = 256;
const MAX_RESOURCE_DESCRIPTION_CHARS: usize = 1024;

fn is_false(value: &bool) -> bool {
    !*value
}

fn clamp(value: &mut String, max: usize) {
    if value.chars().count() > max {
        let kept: String = value.chars().take(max).collect();
        *value = format!("{kept}…[truncated]");
    }
}

fn clamp_opt(value: &mut Option<String>, max: usize) {
    if let Some(value) = value {
        clamp(value, max);
    }
}

fn push_field(out: &mut String, label: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        out.push_str(&format!("\n  {label}: {value}"));
    }
}

impl McpResourceDef {
    /// Whether this reference can be handed back to a server as an address.
    /// A reference whose URI was too long to carry cannot be: retrieving it
    /// would ask for a string the server never named.
    pub fn is_retrievable(&self) -> bool {
        !self.uri_truncated
    }

    /// Clamp every untrusted field to its bound. Called where a server's
    /// `Resource` is adapted, so nothing downstream has to re-check.
    pub fn bounded(mut self) -> Self {
        if self.uri.chars().count() > MAX_RESOURCE_URI_CHARS {
            clamp(&mut self.uri, MAX_RESOURCE_URI_CHARS);
            self.uri_truncated = true;
        }
        clamp_opt(&mut self.name, MAX_RESOURCE_LABEL_CHARS);
        clamp_opt(&mut self.title, MAX_RESOURCE_LABEL_CHARS);
        clamp_opt(&mut self.mime_type, MAX_RESOURCE_LABEL_CHARS);
        clamp_opt(&mut self.description, MAX_RESOURCE_DESCRIPTION_CHARS);
        self
    }

    /// One agent-facing block for a resource a tool returned by reference: the
    /// address, and every metadatum the server supplied. Bounded by
    /// construction — no field here scales with the size of the payload the link
    /// names — and free of I/O: a reference is forwarded, never resolved.
    pub fn render_link(&self) -> String {
        let mut out = if self.uri_truncated {
            // Say plainly that what follows is not an address, so neither the
            // agent nor a later renderer treats the prefix as one.
            format!(
                "[linked resource — address too long to carry, not retrievable] {}",
                self.uri
            )
        } else {
            format!("[linked resource] {}", self.uri)
        };
        push_field(&mut out, "name", self.name.as_deref());
        push_field(&mut out, "title", self.title.as_deref());
        push_field(&mut out, "type", self.mime_type.as_deref());
        if let Some(size) = self.size {
            push_field(&mut out, "size", Some(&format!("{size} bytes")));
        }
        push_field(&mut out, "description", self.description.as_deref());
        out
    }
}

/// Host-implemented bridge to external MCP servers.
///
/// All methods take a `session_key` (the run's job id) so connections are
/// pooled and isolated per agent session; the gateway connects lazily on first
/// use.
///
/// `config` arrives as a [`BrokeredMcpConfig`] rather than an
/// `McpServerConfig` because an expanded configuration carries resolved
/// credentials in `env`, `headers`, `url`, and `args`. The brokered carrier has
/// no `Debug`, no `serde`, and no `Clone`, so a gateway implementation can
/// connect with it but cannot log it, persist it, or hold it in a struct that
/// later gets serialized. Reach the fields with
/// [`BrokeredMcpConfig::resolved`] at the point of connecting, and no earlier.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait McpGateway: Send + Sync {
    /// List tools through a machine-scoped placed facade.
    async fn list_placed_tools(
        &self,
        _facade: Arc<dyn PlacedFacade>,
    ) -> Result<McpToolCatalog, String> {
        Err("this host cannot reach placed MCP facades".to_string())
    }

    /// Execute one tool-call round through a machine-scoped placed facade.
    async fn call_placed_tool_once(
        &self,
        _facade: Arc<dyn PlacedFacade>,
        _tool: &str,
        _args: serde_json::Value,
        _timeout_ms: Option<u32>,
    ) -> Result<McpCallOutcome, String> {
        Err("this host cannot reach placed MCP facades".to_string())
    }

    /// Tear down the pooled connection for one placed machine.
    async fn close_placed(&self, _key: &str) {}

    /// List the tools advertised by `server`.
    async fn list_tools(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
    ) -> Result<McpToolCatalog, String>;

    /// List the resources advertised by `server` (empty if unsupported).
    async fn list_resources(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
    ) -> Result<Vec<McpResourceDef>, String>;

    /// Proxy a `resources/read` for an external resource `uri`.
    async fn read_resource(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        uri: &str,
    ) -> Result<String, String>;

    /// Execute exactly one `tools/call` protocol round. The caller, rather than
    /// the transport, owns MRTR's persisted round bound and task lifecycle.
    async fn call_tool_once(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        input_responses: Option<serde_json::Value>,
        request_state: Option<String>,
        timeout_ms: Option<u32>,
        operation_id: Option<&str>,
    ) -> Result<McpCallOutcome, String> {
        let _ = operation_id;
        if input_responses.is_some() || request_state.is_some() {
            return Err("this MCP host does not support continuation rounds".to_string());
        }
        self.call_tool(session_key, server, config, tool, args, timeout_ms)
            .await
            .map(McpCallOutcome::Complete)
    }

    /// Compatibility convenience for callers that only accept a final result.
    async fn call_tool(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        timeout_ms: Option<u32>,
    ) -> Result<McpToolCallResult, String> {
        match self
            .call_tool_once(
                session_key,
                server,
                config,
                tool,
                args,
                None,
                None,
                timeout_ms,
                None,
            )
            .await?
        {
            McpCallOutcome::Complete(result) => Ok(result),
            McpCallOutcome::InputRequired { .. } => Err("MCP tool requires user input".to_string()),
            McpCallOutcome::Task { task_id, .. } => Err(format!("MCP tool started task {task_id}")),
        }
    }

    async fn get_task(
        &self,
        _session_key: &str,
        _server: &str,
        _config: &BrokeredMcpConfig,
        task_id: &str,
    ) -> Result<McpTaskOutcome, String> {
        Err(format!(
            "MCP task '{task_id}' cannot be polled by this host"
        ))
    }

    async fn update_task(
        &self,
        _session_key: &str,
        _server: &str,
        _config: &BrokeredMcpConfig,
        task_id: &str,
        input_responses: serde_json::Value,
        operation_id: Option<&str>,
    ) -> Result<(), String> {
        let _ = (input_responses, operation_id);
        Err(format!(
            "MCP task '{task_id}' cannot accept input in this host"
        ))
    }

    /// Tear down all pooled connections for a finished session.
    async fn close_session(&self, session_key: &str);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_outcomes_round_trip_all_continuation_fields() {
        let outcomes = [
            McpCallOutcome::InputRequired {
                input_requests: serde_json::json!([{"id": "approval", "type": "boolean"}]),
                request_state: Some("round-2".to_string()),
            },
            McpCallOutcome::Task {
                task_id: "task-7".to_string(),
                poll_interval_ms: Some(250),
                ttl_ms: Some(30_000),
            },
        ];

        for outcome in outcomes {
            let json = serde_json::to_value(&outcome).unwrap();
            let decoded: McpCallOutcome = serde_json::from_value(json.clone()).unwrap();
            match decoded {
                McpCallOutcome::InputRequired {
                    input_requests,
                    request_state,
                } => {
                    assert_eq!(json["kind"], "input_required");
                    assert_eq!(input_requests[0]["id"], "approval");
                    assert_eq!(request_state.as_deref(), Some("round-2"));
                }
                McpCallOutcome::Task {
                    task_id,
                    poll_interval_ms,
                    ttl_ms,
                } => {
                    assert_eq!(json["kind"], "task");
                    assert_eq!(task_id, "task-7");
                    assert_eq!(poll_interval_ms, Some(250));
                    assert_eq!(ttl_ms, Some(30_000));
                }
                McpCallOutcome::Complete(_) => panic!("unexpected complete outcome"),
            }
        }
    }

    #[test]
    fn task_outcomes_round_trip_non_terminal_and_terminal_states() {
        let outcomes = [
            McpTaskOutcome::Working {
                poll_interval_ms: Some(500),
            },
            McpTaskOutcome::InputRequired {
                input_requests: serde_json::json!([{"id": "choice"}]),
            },
            McpTaskOutcome::Complete(McpToolCallResult::from_text("done")),
            McpTaskOutcome::Failed {
                message: "broken".to_string(),
            },
            McpTaskOutcome::Cancelled,
        ];

        for outcome in outcomes {
            let json = serde_json::to_value(&outcome).unwrap();
            let status = json["status"].as_str().unwrap().to_string();
            let decoded: McpTaskOutcome = serde_json::from_value(json).unwrap();
            match decoded {
                McpTaskOutcome::Working { poll_interval_ms } => {
                    assert_eq!(status, "working");
                    assert_eq!(poll_interval_ms, Some(500));
                }
                McpTaskOutcome::InputRequired { input_requests } => {
                    assert_eq!(status, "input_required");
                    assert_eq!(input_requests[0]["id"], "choice");
                }
                McpTaskOutcome::Complete(result) => {
                    assert_eq!(status, "complete");
                    assert_eq!(result.text, "done");
                }
                McpTaskOutcome::Failed { message } => {
                    assert_eq!(status, "failed");
                    assert_eq!(message, "broken");
                }
                McpTaskOutcome::Cancelled => assert_eq!(status, "cancelled"),
            }
        }
    }

    fn report_link() -> McpResourceDef {
        McpResourceDef {
            uri: "https://files.example/report.pdf".to_string(),
            name: Some("report.pdf".to_string()),
            title: Some("Q3 report".to_string()),
            description: Some("Rendered quarterly report".to_string()),
            mime_type: Some("application/pdf".to_string()),
            size: Some(524_288_000),
            uri_truncated: false,
        }
    }

    #[test]
    fn a_reference_survives_the_durable_carrier_intact() {
        // A complete outcome is persisted before an agent's turn yields, so the
        // reference has to round-trip as a reference rather than degrading to
        // whatever prose happened to be rendered beside it.
        let outcome = McpCallOutcome::Complete(McpToolCallResult {
            text: "see attached".to_string(),
            images: vec![],
            links: vec![report_link()],
        });
        let json = serde_json::to_value(&outcome).unwrap();
        let McpCallOutcome::Complete(decoded) = serde_json::from_value(json).unwrap() else {
            panic!("expected a complete outcome");
        };
        assert_eq!(decoded.links, vec![report_link()]);

        // A result with nothing linked says nothing about links.
        let plain = serde_json::to_value(McpToolCallResult::from_text("just text")).unwrap();
        assert!(plain.get("links").is_none(), "{plain}");
    }

    #[test]
    fn a_rendered_reference_names_only_what_the_server_supplied() {
        let rendered = report_link().render_link();
        assert_eq!(
            rendered,
            "[linked resource] https://files.example/report.pdf\n  name: report.pdf\n  title: Q3 report\n  type: application/pdf\n  size: 524288000 bytes\n  description: Rendered quarterly report"
        );

        // A server that supplies only an address gets a line with only an
        // address — no invented labels, no empty fields.
        let bare = McpResourceDef {
            uri: "res://x".to_string(),
            name: Some("   ".to_string()),
            ..McpResourceDef::default()
        };
        assert_eq!(bare.render_link(), "[linked resource] res://x");
    }

    #[test]
    fn every_untrusted_field_of_a_reference_is_bounded() {
        // The pathological shape: a `data:` URI that is the payload, and a
        // description used as a byte channel.
        let flood = "A".repeat(100_000);
        let bounded = McpResourceDef {
            uri: format!("data:application/pdf;base64,{flood}"),
            name: Some(flood.clone()),
            title: Some(flood.clone()),
            description: Some(flood.clone()),
            mime_type: Some(flood.clone()),
            size: Some(100_000),
            uri_truncated: false,
        }
        .bounded();

        let marker = "…[truncated]";
        assert_eq!(
            bounded.uri.chars().count(),
            MAX_RESOURCE_URI_CHARS + marker.chars().count()
        );
        // A prefix is a different address, not a shorter one, so the reference
        // reports itself unretrievable rather than offering a URI that would
        // ask the server for something it never named.
        assert!(bounded.uri_truncated);
        assert!(!bounded.is_retrievable());
        assert!(
            bounded.render_link().starts_with(
                "[linked resource — address too long to carry, not retrievable] data:"
            ),
            "{}",
            bounded.render_link()
        );
        assert_eq!(
            bounded.name.as_deref().unwrap().chars().count(),
            MAX_RESOURCE_LABEL_CHARS + marker.chars().count()
        );
        assert_eq!(
            bounded.description.as_deref().unwrap().chars().count(),
            MAX_RESOURCE_DESCRIPTION_CHARS + marker.chars().count()
        );
        assert!(bounded.title.as_deref().unwrap().ends_with(marker));
        assert!(bounded.mime_type.as_deref().unwrap().ends_with(marker));

        // A reference that already fits is left exactly as the server sent it —
        // including a long-but-real bearer URL, which shortening would destroy.
        assert_eq!(report_link().bounded(), report_link());
        let presigned = McpResourceDef {
            uri: format!("https://bucket.example/o?sig={}", "a".repeat(4_000)),
            ..McpResourceDef::default()
        }
        .bounded();
        assert!(presigned.is_retrievable());
        assert!(!presigned.uri.contains("truncated"));
    }
}
