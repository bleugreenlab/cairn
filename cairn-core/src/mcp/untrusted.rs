//! The boundary that treats every external MCP server as hostile.
//!
//! Cairn hands configured MCP servers real credentials: a `${VAR}` token in an
//! `env` map, an `Authorization: Bearer` header, an OAuth access token. A server
//! therefore *possesses* secrets, and the protocol gives it a dozen ways to hand
//! them back — a tool result, a resource body, a tool description in a catalog,
//! a JSON input request, an error message quoting the request that failed.
//! Several of those land in durable sinks (the tool cache, a continuation row)
//! as well as in the agent's transcript.
//!
//! Nothing about a third party's software makes an echo unlikely. A server that
//! logs its own configuration into an error string does it by accident; one that
//! wants to exfiltrate does it on purpose. Either way the value crosses here.
//!
//! [`UntrustedGateway`] wraps the host's real gateway so every value returned by
//! an external server is sanitized before any Cairn code sees it. The wrap
//! happens in `Orchestrator::set_mcp_gateway`, which is the only way a gateway
//! is installed, so an unwrapped gateway is not reachable rather than merely
//! discouraged.
//!
//! # Exact, not structural
//!
//! Sanitization here is [`SanitizeMode::ExactOnly`]: registered credentials and
//! their bounded derived forms, and nothing else.
//!
//! It is tempting to use structural heuristics on a payload from an untrusted
//! party, the way browser network capture does. That would be wrong here. A
//! browser capture is diagnostic exhaust nobody reads for content, so a false
//! positive costs nothing. An MCP result is the *work product the agent asked
//! for* — the issue body, the query result, the page. Redacting every
//! `Authorization:` line inside a fetched document, or every high-entropy string
//! in a query result, silently corrupts the answer, and a corrupted answer that
//! looks complete is worse than a visibly missing one.
//!
//! # Sanitizing a round-trip field changes what Cairn sends
//!
//! Two fields here are not output. `request_state` and a task id are opaque
//! handles the server issues and Cairn hands straight back on the next protocol
//! round. Scrubbing one turns a disclosure into a protocol break: the server
//! receives `[REDACTED]` where its handle should be and the round fails, with
//! nothing in the failure explaining why.
//!
//! They are still scrubbed. A handle that embeds a live credential — a server
//! that HMACs the request including its own key, or packs configuration into an
//! opaque continuation blob — is persisted verbatim into a durable continuation
//! row before the agent's turn yields, and durable plaintext is worse than a
//! failed round. But a detection on one of these gets its own log line naming
//! the consequence, so the resulting failure is diagnosable instead of a
//! mystery.
//!
//! # What this does not cover
//!
//! Image blocks pass through untouched. A credential rendered into pixels, or
//! embedded in image bytes, is not detectable by substring matching, and
//! pretending otherwise would be a false guarantee. The same holds for a server
//! that transforms a credential before echoing it: only the encodings in
//! `security::secret::MatchRule` are recognized. Redaction is defense in depth
//! behind brokered use and short-lived credentials, not a containment boundary.

use std::sync::Arc;

use async_trait::async_trait;

use crate::mcp::gateway::{
    McpCallOutcome, McpGateway, McpResourceDef, McpTaskOutcome, McpToolCallResult, McpToolCatalog,
    PlacedFacade,
};
use crate::security::{BrokeredMcpConfig, Crossing, DetectionReport, Sanitizer};

/// Sanitize one value coming back from an external server, reporting anything
/// that matched.
///
/// A detection here is louder than one at the model crossing: it means a server
/// Cairn authenticated to has just handed a live credential back, which is
/// either a badly behaved integration or an exfiltration attempt, and either way
/// an operator wants to know. The report carries the credential's identity and
/// never its value.
fn scrub<T>(what: &str, value: &mut T, apply: impl FnOnce(&mut Sanitizer<'static>, &mut T)) {
    let mut sanitizer = Sanitizer::exact();
    if sanitizer.is_noop() {
        return;
    }
    apply(&mut sanitizer, value);
    let detections = sanitizer.into_detections();
    for report in DetectionReport::from_detections(&detections, Crossing::ExternalTool, None, None)
    {
        log::warn!("external MCP server echoed a registered credential in {what}");
        report.log();
    }
}

fn scrub_text(what: &str, text: &mut String) {
    scrub(what, text, |sanitizer, text| sanitizer.text_in_place(text));
}

fn scrub_opt_text(what: &str, text: &mut Option<String>) {
    scrub(what, text, |sanitizer, text| {
        sanitizer.opt_text_in_place(text)
    });
}

fn scrub_json(what: &str, value: &mut serde_json::Value) {
    scrub(what, value, |sanitizer, value| sanitizer.json(value));
}

/// Sanitize a value the server issued and Cairn hands back to it verbatim.
///
/// Distinct from [`scrub_text`] only in what it says when something matches: a
/// replacement here does not merely redact what the agent sees, it changes the
/// bytes the next protocol round sends, and the server will reject the handle it
/// no longer recognizes. See the module docs.
fn scrub_round_trip(what: &str, value: &mut Option<String>) {
    let before = value.clone();
    scrub_opt_text(what, value);
    if before != *value {
        log::warn!(
            "an external MCP server put a registered credential in {what}, which Cairn hands \
             back verbatim; the redacted handle will not be recognized and this round will fail"
        );
    }
}

/// Sanitize an error string on its way back from a server.
///
/// Errors are the likeliest echo of all: a server that rejects a request very
/// often quotes the request — headers included — in the message explaining why.
fn scrub_error<T>(what: &str, result: Result<T, String>) -> Result<T, String> {
    result.map_err(|mut message| {
        scrub_text(what, &mut message);
        message
    })
}

fn scrub_call_result(result: &mut McpToolCallResult) {
    scrub_text("a tool result", &mut result.text);
    // `images` is deliberately untouched; see the module docs.
}

fn scrub_call_outcome(outcome: &mut McpCallOutcome) {
    match outcome {
        McpCallOutcome::Complete(result) => scrub_call_result(result),
        McpCallOutcome::InputRequired {
            input_requests,
            request_state,
        } => {
            // Both are persisted verbatim into the continuation row before the
            // agent's turn yields, so this is a durable sink as well as a model
            // one.
            scrub_json("a tool input request", input_requests);
            scrub_round_trip("a tool request state", request_state);
        }
        McpCallOutcome::Task { task_id, .. } => {
            let mut id = Some(std::mem::take(task_id));
            scrub_round_trip("a task id", &mut id);
            *task_id = id.unwrap_or_default();
        }
    }
}

fn scrub_task_outcome(outcome: &mut McpTaskOutcome) {
    match outcome {
        McpTaskOutcome::Complete(result) => scrub_call_result(result),
        McpTaskOutcome::InputRequired { input_requests } => {
            scrub_json("a task input request", input_requests)
        }
        McpTaskOutcome::Failed { message } => scrub_text("a task failure", message),
        McpTaskOutcome::Working { .. } | McpTaskOutcome::Cancelled => {}
    }
}

fn scrub_catalog(catalog: &mut McpToolCatalog) {
    for tool in &mut catalog.tools {
        // A catalog is cached to disk and rendered into the agent's affordance
        // block, so a credential parked in a tool description outlives the call
        // that fetched it.
        scrub_text("a tool name", &mut tool.name);
        scrub_opt_text("a tool description", &mut tool.description);
        scrub_json("a tool input schema", &mut tool.input_schema);
    }
}

fn scrub_resources(resources: &mut [McpResourceDef]) {
    for resource in resources {
        scrub_text("a resource uri", &mut resource.uri);
        scrub_opt_text("a resource name", &mut resource.name);
        scrub_opt_text("a resource description", &mut resource.description);
    }
}

/// A gateway that sanitizes everything the server on the other side returns.
///
/// Every trait method is implemented explicitly, including the ones the trait
/// provides a default for. A defaulted method here would delegate to *this*
/// type's other methods rather than the inner gateway's overrides, quietly
/// changing behavior; and a method left to a future default would be an
/// unsanitized path that compiles.
pub struct UntrustedGateway {
    inner: Arc<dyn McpGateway>,
}

impl UntrustedGateway {
    pub fn wrap(inner: Arc<dyn McpGateway>) -> Arc<dyn McpGateway> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl McpGateway for UntrustedGateway {
    async fn list_placed_tools(
        &self,
        facade: Arc<dyn PlacedFacade>,
    ) -> Result<McpToolCatalog, String> {
        let mut catalog = scrub_error(
            "a placed catalog error",
            self.inner.list_placed_tools(facade).await,
        )?;
        scrub_catalog(&mut catalog);
        Ok(catalog)
    }

    async fn call_placed_tool_once(
        &self,
        facade: Arc<dyn PlacedFacade>,
        tool: &str,
        args: serde_json::Value,
        timeout_ms: Option<u32>,
    ) -> Result<McpCallOutcome, String> {
        let mut outcome = scrub_error(
            "a placed tool error",
            self.inner
                .call_placed_tool_once(facade, tool, args, timeout_ms)
                .await,
        )?;
        scrub_call_outcome(&mut outcome);
        Ok(outcome)
    }

    async fn close_placed(&self, key: &str) {
        self.inner.close_placed(key).await
    }

    async fn list_tools(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
    ) -> Result<McpToolCatalog, String> {
        let mut catalog = scrub_error(
            "a tools/list error",
            self.inner.list_tools(session_key, server, config).await,
        )?;
        scrub_catalog(&mut catalog);
        Ok(catalog)
    }

    async fn list_resources(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
    ) -> Result<Vec<McpResourceDef>, String> {
        let mut resources = scrub_error(
            "a resources/list error",
            self.inner.list_resources(session_key, server, config).await,
        )?;
        scrub_resources(&mut resources);
        Ok(resources)
    }

    async fn read_resource(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        uri: &str,
    ) -> Result<String, String> {
        let mut body = scrub_error(
            "a resources/read error",
            self.inner
                .read_resource(session_key, server, config, uri)
                .await,
        )?;
        scrub_text("a resource body", &mut body);
        Ok(body)
    }

    #[allow(clippy::too_many_arguments)]
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
        let mut outcome = scrub_error(
            "a tools/call error",
            self.inner
                .call_tool_once(
                    session_key,
                    server,
                    config,
                    tool,
                    args,
                    input_responses,
                    request_state,
                    timeout_ms,
                    operation_id,
                )
                .await,
        )?;
        scrub_call_outcome(&mut outcome);
        Ok(outcome)
    }

    async fn call_tool(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        timeout_ms: Option<u32>,
    ) -> Result<McpToolCallResult, String> {
        let mut result = scrub_error(
            "a tools/call error",
            self.inner
                .call_tool(session_key, server, config, tool, args, timeout_ms)
                .await,
        )?;
        scrub_call_result(&mut result);
        Ok(result)
    }

    async fn get_task(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        task_id: &str,
    ) -> Result<McpTaskOutcome, String> {
        let mut outcome = scrub_error(
            "a task poll error",
            self.inner
                .get_task(session_key, server, config, task_id)
                .await,
        )?;
        scrub_task_outcome(&mut outcome);
        Ok(outcome)
    }

    async fn update_task(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        task_id: &str,
        input_responses: serde_json::Value,
        operation_id: Option<&str>,
    ) -> Result<(), String> {
        scrub_error(
            "a task update error",
            self.inner
                .update_task(
                    session_key,
                    server,
                    config,
                    task_id,
                    input_responses,
                    operation_id,
                )
                .await,
        )
    }

    async fn close_session(&self, session_key: &str) {
        self.inner.close_session(session_key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::mcp_servers::McpServerConfig;
    use crate::mcp::gateway::McpToolDef;
    use crate::security::{registry, SecretCategory, SecretGuard, SecretId, SecretMaterial};
    use cairn_common::read::ImageBlock;
    use std::collections::HashMap;

    /// A value long and varied enough to register, unique to this module so it
    /// cannot collide with another test's needle in the process registry.
    const ECHOED: &str = "mcp-echo-Zx91Qw82Lm73Pv";

    fn register() -> SecretGuard<'static> {
        registry()
            .register(
                SecretId::new("test:untrusted-gateway"),
                SecretCategory::ConfiguredMcp,
                "test",
                SecretMaterial::from_string(ECHOED.to_string()),
            )
            .expect("fixture value is registerable")
    }

    fn config() -> BrokeredMcpConfig {
        let authored: McpServerConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        crate::security::broker::mcp_server("test", &authored, &HashMap::new(), "test")
    }

    /// A server that puts the credential it was given into every field the
    /// protocol lets it return.
    struct EchoingServer;

    #[async_trait]
    impl McpGateway for EchoingServer {
        async fn list_tools(
            &self,
            _: &str,
            _: &str,
            _: &BrokeredMcpConfig,
        ) -> Result<McpToolCatalog, String> {
            Ok(McpToolCatalog {
                tools: vec![McpToolDef {
                    name: "search".to_string(),
                    description: Some(format!("authenticate with {ECHOED}")),
                    input_schema: serde_json::json!({
                        "properties": {"token": {"default": ECHOED}}
                    }),
                }],
                ttl_ms: None,
                cache_scope: None,
            })
        }

        async fn list_resources(
            &self,
            _: &str,
            _: &str,
            _: &BrokeredMcpConfig,
        ) -> Result<Vec<McpResourceDef>, String> {
            Ok(vec![McpResourceDef {
                uri: format!("https://example.test/{ECHOED}"),
                name: Some(ECHOED.to_string()),
                description: Some(format!("holds {ECHOED}")),
                mime_type: None,
            }])
        }

        async fn read_resource(
            &self,
            _: &str,
            _: &str,
            _: &BrokeredMcpConfig,
            _: &str,
        ) -> Result<String, String> {
            Ok(format!("document body mentioning {ECHOED}"))
        }

        #[allow(clippy::too_many_arguments)]
        async fn call_tool_once(
            &self,
            _: &str,
            _: &str,
            _: &BrokeredMcpConfig,
            tool: &str,
            _: serde_json::Value,
            _: Option<serde_json::Value>,
            _: Option<String>,
            _: Option<u32>,
            _: Option<&str>,
        ) -> Result<McpCallOutcome, String> {
            match tool {
                "fail" => Err(format!("rejected request with header Bearer {ECHOED}")),
                "defer" => Ok(McpCallOutcome::Task {
                    task_id: format!("task-{ECHOED}"),
                    poll_interval_ms: None,
                    ttl_ms: None,
                }),
                "ask" => Ok(McpCallOutcome::InputRequired {
                    input_requests: serde_json::json!({
                        "prompts": [{"id": "confirm", "detail": {"seen": ECHOED}}]
                    }),
                    request_state: Some(format!("round-2-{ECHOED}")),
                }),
                _ => Ok(McpCallOutcome::Complete(McpToolCallResult {
                    text: format!("result carrying {ECHOED}"),
                    images: vec![ImageBlock::inline("image/png", "base64png")],
                })),
            }
        }

        async fn get_task(
            &self,
            _: &str,
            _: &str,
            _: &BrokeredMcpConfig,
            _: &str,
        ) -> Result<McpTaskOutcome, String> {
            Ok(McpTaskOutcome::Failed {
                message: format!("task died holding {ECHOED}"),
            })
        }

        async fn close_session(&self, _: &str) {}
    }

    fn wrapped() -> Arc<dyn McpGateway> {
        UntrustedGateway::wrap(Arc::new(EchoingServer))
    }

    #[tokio::test]
    async fn a_tool_result_cannot_carry_a_registered_credential_back() {
        let _guard = register();
        let outcome = wrapped()
            .call_tool_once(
                "s",
                "srv",
                &config(),
                "go",
                json_args(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let McpCallOutcome::Complete(result) = outcome else {
            panic!("expected a complete outcome");
        };
        assert!(!result.text.contains(ECHOED), "{}", result.text);
        assert!(result.text.contains("[REDACTED]"), "{}", result.text);
    }

    /// Errors are the likeliest echo: a server that rejects a request usually
    /// quotes the request, headers included.
    #[tokio::test]
    async fn an_error_message_cannot_quote_the_credential_that_failed() {
        let _guard = register();
        let error = wrapped()
            .call_tool_once(
                "s",
                "srv",
                &config(),
                "fail",
                json_args(),
                None,
                None,
                None,
                None,
            )
            .await
            .expect_err("the fixture fails this tool");
        assert!(!error.contains(ECHOED), "{error}");
    }

    /// An input request and its opaque continuation state are persisted into a
    /// durable row before the agent's turn yields, so they are a durable sink as
    /// well as a model-visible one.
    #[tokio::test]
    async fn a_recursive_input_request_is_scrubbed_before_it_is_persisted() {
        let _guard = register();
        let outcome = wrapped()
            .call_tool_once(
                "s",
                "srv",
                &config(),
                "ask",
                json_args(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let McpCallOutcome::InputRequired {
            input_requests,
            request_state,
        } = outcome
        else {
            panic!("expected an input-required outcome");
        };
        let encoded = serde_json::to_string(&input_requests).unwrap();
        assert!(!encoded.contains(ECHOED), "{encoded}");
        assert!(!request_state.unwrap().contains(ECHOED));
    }

    /// A catalog is cached to disk and rendered into the agent's affordance
    /// block, so a credential parked in a tool description outlives the call.
    #[tokio::test]
    async fn a_tool_catalog_cannot_smuggle_a_credential_into_the_cache() {
        let _guard = register();
        let catalog = wrapped().list_tools("s", "srv", &config()).await.unwrap();
        let encoded = serde_json::to_string(&catalog.tools).unwrap();
        assert!(!encoded.contains(ECHOED), "{encoded}");
    }

    #[tokio::test]
    async fn resource_listings_and_bodies_are_scrubbed() {
        let _guard = register();
        let gateway = wrapped();
        let resources = gateway.list_resources("s", "srv", &config()).await.unwrap();
        let encoded = serde_json::to_string(&resources).unwrap();
        assert!(!encoded.contains(ECHOED), "{encoded}");

        let body = gateway
            .read_resource("s", "srv", &config(), "res://x")
            .await
            .unwrap();
        assert!(!body.contains(ECHOED), "{body}");
    }

    #[tokio::test]
    async fn a_failed_task_cannot_report_the_credential_it_held() {
        let _guard = register();
        let outcome = wrapped()
            .get_task("s", "srv", &config(), "task-1")
            .await
            .unwrap();
        let McpTaskOutcome::Failed { message } = outcome else {
            panic!("expected a failed task");
        };
        assert!(!message.contains(ECHOED), "{message}");
    }

    /// A task id is a handle Cairn hands back to the server on the next round,
    /// so scrubbing it trades a durable disclosure for a failed round.
    ///
    /// The trade is deliberate — the id is persisted into the continuation row
    /// before the agent's turn yields, and durable plaintext is worse — but it
    /// is the one place in this boundary where sanitizing changes what Cairn
    /// *sends* rather than what it shows. Pinned so the behavior is a decision
    /// rather than an accident.
    #[tokio::test]
    async fn a_round_trip_handle_is_scrubbed_even_though_it_breaks_the_next_round() {
        let _guard = register();
        let outcome = wrapped()
            .call_tool_once(
                "s",
                "srv",
                &config(),
                "defer",
                json_args(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let McpCallOutcome::Task { task_id, .. } = outcome else {
            panic!("expected a task outcome");
        };
        assert!(!task_id.contains(ECHOED), "{task_id}");
        assert!(task_id.contains("[REDACTED]"), "{task_id}");
    }

    /// The documented limitation, pinned so it stays a known gap rather than an
    /// assumed guarantee: image bytes pass through untouched.
    #[tokio::test]
    async fn image_blocks_pass_through_unscrubbed() {
        let _guard = register();
        let outcome = wrapped()
            .call_tool_once(
                "s",
                "srv",
                &config(),
                "go",
                json_args(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let McpCallOutcome::Complete(result) = outcome else {
            panic!("expected a complete outcome");
        };
        assert_eq!(result.images.len(), 1);
    }

    /// The wrapper replaces registered values and nothing else, so wrapping
    /// every gateway does not mangle ordinary results. Asserted on a value that
    /// *looks* credential-shaped and is not registered, because exact-only
    /// matching is the deliberate choice here — see the module docs.
    #[tokio::test]
    async fn an_unregistered_credential_shaped_value_is_left_alone() {
        let _guard = register();
        let body = wrapped()
            .read_resource("s", "srv", &config(), "res://x")
            .await
            .unwrap();
        assert!(body.starts_with("document body mentioning "), "{body}");

        let mut sanitizer = Sanitizer::exact();
        let untouched = "Authorization: Bearer sk-live-someone-elses-9fA3xQ2m";
        assert_eq!(sanitizer.text(untouched), untouched);
    }

    fn json_args() -> serde_json::Value {
        serde_json::json!({})
    }

    /// The wrapper is *installed*, not merely available.
    ///
    /// Every test above proves `UntrustedGateway` sanitizes when it is used.
    /// None of them would notice if `set_mcp_gateway` stopped wrapping, which is
    /// the single edit that would silently reopen this boundary for the whole
    /// system. So this one goes through the orchestrator's real installation
    /// path and asks the gateway it hands back.
    #[tokio::test]
    async fn the_orchestrator_installs_the_wrapper_around_every_gateway() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let _guard = register();
        let config_dir = tempfile::tempdir().unwrap().keep();
        let db = crate::storage::migrated_test_db("untrusted-gateway.db").await;
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(config_dir.join("search-index.db")).unwrap()),
        ));
        let orch = crate::orchestrator::Orchestrator::builder(
            db_state,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build();

        // The raw echoing server, installed exactly as a host installs one.
        orch.set_mcp_gateway(Arc::new(EchoingServer))
            .map_err(|_| "gateway already set")
            .unwrap();

        let outcome = orch
            .mcp_gateway()
            .expect("a gateway is installed")
            .call_tool_once(
                "s",
                "srv",
                &config(),
                "go",
                json_args(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let McpCallOutcome::Complete(result) = outcome else {
            panic!("expected a complete outcome");
        };
        assert!(
            !result.text.contains(ECHOED),
            "the orchestrator handed back an unwrapped gateway: {}",
            result.text
        );
    }
}
