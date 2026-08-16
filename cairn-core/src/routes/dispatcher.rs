use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use cairn_common::uri::{parse_uri, CairnResource};
use serde_json::{Map, Value};

use super::{
    matches, ArgumentBinding, ChannelDestination, Fact, Presence, RouteFact, RouteGraph, RouteNode,
    RouteNodeConfig, RouteSink,
};
use crate::{
    config::{self, ConfigResult},
    orchestrator::Orchestrator,
    responses::{self, ResponseCaller},
    storage::NewRouteFiring,
};

const MAX_FIRINGS_PER_FACT: usize = 100;

#[derive(Debug, Clone)]
pub struct RouteContext<'a> {
    pub project_id: Option<&'a str>,
    pub project_path: Option<&'a Path>,
}

fn resolve_message_target(target: &str) -> Result<(String, Option<i32>, String), String> {
    match parse_uri(target) {
        Some(CairnResource::Project { project }) => {
            let canonical = cairn_common::uri::build_project_uri(&project);
            Ok((project, None, canonical))
        }
        Some(CairnResource::Issue { project, number }) => {
            let canonical = cairn_common::uri::build_issue_uri(&project, number);
            Ok((project, Some(number), canonical))
        }
        _ => Err(format!(
            "message target must resolve to a project or issue: {target}"
        )),
    }
}

/// One route's attempt on one fact, as the journal records it.
struct Firing<'a> {
    route_id: &'a str,
    scope_key: &'a str,
    project_id: Option<&'a str>,
    fact: &'a RouteFact,
    sink: &'a RouteSink,
    created_at: i64,
}

/// What came of the attempt. `payload` is the content the firing carried to its
/// sink and is kept whether or not delivery succeeded: an operator reading a
/// failed firing wants to see what did not arrive, and `status` already says
/// that it did not.
struct Outcome {
    status: &'static str,
    drop_reason: Option<&'static str>,
    sink_ref: Option<String>,
    payload: Option<String>,
    error: Option<String>,
}

impl Outcome {
    fn dropped(reason: &'static str) -> Self {
        Self {
            status: "dropped",
            drop_reason: Some(reason),
            sink_ref: None,
            payload: None,
            error: None,
        }
    }

    fn delivery(result: Result<String, String>, payload: Option<String>) -> Self {
        match result {
            Ok(sink_ref) => Self {
                status: "fired",
                drop_reason: None,
                sink_ref: Some(sink_ref),
                payload,
                error: None,
            },
            Err(error) => Self {
                status: "failed",
                drop_reason: None,
                sink_ref: None,
                payload,
                error: Some(error),
            },
        }
    }
}

/* ------------------------------------------------------------- graph walk */

/// How a response node's work is actually done. It is injected rather than
/// called directly so the part worth testing — which content reaches which sink
/// along which path — can be exercised without a model backend.
type ResponseFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
type RunResponse<'f> = &'f (dyn Fn(&str, Value) -> ResponseFuture + Sync);

/// What one sink node receives: the content its own path produced, and the
/// record of the response nodes that produced it.
struct Delivery<'a> {
    node_id: &'a str,
    sink: &'a RouteSink,
    /// Ordinal among the route's sinks, which is what numbers the channel
    /// delivery key.
    position: usize,
    fields: BTreeMap<String, Value>,
    transforms: Vec<Value>,
}

/// Everything one fact makes of one route's graph.
struct Walk<'a> {
    deliveries: Vec<Delivery<'a>>,
    /// Each response node's output, by node index, so a downstream binding can
    /// read a specific node rather than only the content flowing into it.
    outputs: HashMap<usize, String>,
    nodes: Vec<Value>,
}

/// How a node's argument bindings resolve on the path that reached it.
struct Bindings<'a> {
    graph: &'a RouteGraph<'a>,
    fields: &'a BTreeMap<String, Value>,
    outputs: &'a HashMap<usize, String>,
}

impl Bindings<'_> {
    fn value(&self, binding: &ArgumentBinding) -> Value {
        match binding {
            ArgumentBinding::Field { field } => {
                self.fields.get(field).cloned().unwrap_or(Value::Null)
            }
            ArgumentBinding::Node { from } => self
                .graph
                .index_of(from)
                .and_then(|index| self.outputs.get(&index))
                .map(|text| Value::String(text.clone()))
                .unwrap_or(Value::Null),
            ArgumentBinding::Value { value } => value.clone(),
        }
    }

    fn string(&self, binding: &ArgumentBinding) -> Result<String, String> {
        if let ArgumentBinding::Field { field } = binding {
            if !self.fields.contains_key(field) {
                return Err(format!("route field '{field}' is missing"));
            }
        }
        self.value(binding)
            .as_str()
            .map(str::to_owned)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "issue binding must resolve to a non-empty string".into())
    }
}

fn trigger_matches(node: &RouteNode, fact: &RouteFact, presence: Presence) -> bool {
    match &node.config {
        RouteNodeConfig::Trigger { when } => matches(
            std::slice::from_ref(when),
            &Fact {
                source: &fact.source,
                fields: &fact.fields,
                presence,
            },
        ),
        _ => false,
    }
}

fn text_of(fields: &BTreeMap<String, Value>) -> &str {
    fields
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// Walk the route's graph for one fact.
///
/// A fact enters at every matching trigger and flows down the edges. Each node
/// is evaluated once, in topological order, so a prefix shared by several
/// branches — the expensive part, since a response node is a model call — is
/// computed once no matter how many sinks hang off it. A node's content is
/// whatever its one producing predecessor made (validation guarantees there is
/// at most one), which is what lets two sinks below the same trigger receive
/// different things.
async fn walk<'a>(
    graph: &'a RouteGraph<'a>,
    fact: &RouteFact,
    presence: Presence,
    run: RunResponse<'_>,
) -> Walk<'a> {
    let mut reached = vec![false; graph.nodes().len()];
    let mut outputs: HashMap<usize, String> = HashMap::new();
    let mut histories: HashMap<usize, Vec<Value>> = HashMap::new();
    let mut deliveries = Vec::new();
    let mut nodes = Vec::new();
    let positions: HashMap<usize, usize> = graph
        .sinks()
        .enumerate()
        .map(|(position, index)| (index, position))
        .collect();

    for &index in graph.order() {
        let node = graph.node(index);
        if node.config.is_trigger() {
            reached[index] = trigger_matches(node, fact, presence);
            let RouteNodeConfig::Trigger { when } = &node.config else {
                unreachable!()
            };
            nodes.push(serde_json::json!({"nodeId":node.id,"kind":"trigger","reached":reached[index],"output":fact.fields,"failures":super::explain_clause(when, &Fact { source: &fact.source, fields: &fact.fields, presence })}));
            continue;
        }
        if !graph.incoming(index).iter().any(|&from| reached[from]) {
            nodes.push(serde_json::json!({"nodeId":node.id,"kind":if node.config.is_sink(){"sink"}else{"response"},"reached":false}));
            continue;
        }
        reached[index] = true;

        let producer = graph.producers(index).find(|&from| reached[from]);
        let mut fields = fact.fields.clone();
        if let Some(text) = producer.and_then(|from| outputs.get(&from)) {
            fields.insert("text".into(), Value::String(text.clone()));
        }
        // Only what a step produced is recorded: its input is the content above
        // it on this path — the fact at the top, the previous step's output
        // after that.
        let mut history = producer
            .and_then(|from| histories.get(&from))
            .cloned()
            .unwrap_or_default();

        match &node.config {
            RouteNodeConfig::Response { response, args } => {
                let bindings = Bindings {
                    graph,
                    fields: &fields,
                    outputs: &outputs,
                };
                let arguments = args
                    .iter()
                    .map(|(name, binding)| (name.clone(), bindings.value(binding)))
                    .collect::<Map<_, _>>();
                match run(response, Value::Object(arguments)).await {
                    Ok(text) => {
                        history.push(serde_json::json!({
                            "response": response,
                            "status": "ok",
                            "output": crate::storage::firing_snapshot(&text),
                        }));
                        outputs.insert(index, text);
                        nodes.push(serde_json::json!({"nodeId":node.id,"kind":"response","reached":true,"status":"ok","output":outputs[&index]}));
                    }
                    Err(error) => {
                        history.push(serde_json::json!({
                            "response": response,
                            "status": "failed",
                            "error": error,
                        }));
                        // A failed step is recorded and the path continues with
                        // the content that reached it.
                        outputs.insert(index, text_of(&fields).to_owned());
                        nodes.push(serde_json::json!({"nodeId":node.id,"kind":"response","reached":true,"status":"failed","output":outputs[&index],"error":error}));
                    }
                }
                histories.insert(index, history);
            }
            RouteNodeConfig::Sink { sink } => {
                nodes.push(serde_json::json!({"nodeId":node.id,"kind":"sink","reached":true,"output":fields}));
                deliveries.push(Delivery {
                    node_id: &node.id,
                    sink,
                    position: positions[&index],
                    fields,
                    transforms: history,
                });
            }
            RouteNodeConfig::Trigger { .. } => unreachable!("triggers are handled above"),
        }
    }
    Walk {
        deliveries,
        outputs,
        nodes,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteTestResult {
    pub matched: bool,
    pub nodes: Vec<Value>,
    pub sink_previews: Vec<Value>,
}

/// Execute a supplied draft through real Responses while stopping before every
/// externally visible delivery and before the route firing journal.
pub async fn test_definition(
    orch: &Orchestrator,
    definition: &super::RouteDefinition,
    fact: RouteFact,
    presence: Presence,
    context: RouteContext<'_>,
) -> Result<RouteTestResult, String> {
    definition.validate(&super::FactRegistry::default())?;
    let graph = RouteGraph::new(definition)?;
    let run = |response: &str, args: Value| -> ResponseFuture {
        let (orch, response, project_id, project_path) = (
            orch.clone(),
            response.to_owned(),
            context.project_id.map(str::to_owned),
            context.project_path.map(Path::to_owned),
        );
        Box::pin(async move {
            responses::invoke(
                &orch,
                &response,
                &args,
                ResponseCaller::Agent {
                    label: Some("route editor test".into()),
                    run_id: uuid::Uuid::new_v4().to_string(),
                    project_id,
                    project_path,
                },
            )
            .await
            .map(|outcome| outcome.text)
            .map_err(|error| error.to_string())
        })
    };
    let walked = walk(&graph, &fact, presence, &run).await;
    let matched = graph
        .triggers()
        .any(|index| trigger_matches(graph.node(index), &fact, presence));
    let sink_previews = walked.deliveries.iter().map(|delivery| {
        let fields = &delivery.fields;
        let bindings = Bindings { graph: &graph, fields, outputs: &walked.outputs };
        match delivery.sink {
            RouteSink::Channel { destination, initiated_by } => serde_json::json!({"nodeId":delivery.node_id,"kind":"channel","destination":destination,"initiatedBy":initiated_by,"text":text_of(fields),"context":fields.get("context").and_then(Value::as_str).unwrap_or("[Cairn]"),"jobId":fields.get("jobId").cloned()}),
            RouteSink::Message { target } => serde_json::json!({"nodeId":delivery.node_id,"kind":"message","target":target,"text":text_of(fields)}),
            RouteSink::Label { issue, labels } => match bindings.string(issue) { Ok(issue) => serde_json::json!({"nodeId":delivery.node_id,"kind":"label","issue":issue,"labels":labels}), Err(error) => serde_json::json!({"nodeId":delivery.node_id,"kind":"label","labels":labels,"error":error}) },
            RouteSink::Issue { labels, recipe } => serde_json::json!({"nodeId":delivery.node_id,"kind":"issue","project":fields.get("project"),"title":fields.get("title").or_else(||fields.get("text")),"description":fields.get("body").or_else(||fields.get("description")),"labels":labels,"recipe":recipe}),
        }
    }).collect();
    Ok(RouteTestResult {
        matched,
        nodes: walked.nodes,
        sink_previews,
    })
}

/* ---------------------------------------------------------------- delivery */

async fn fire_issue_sink(
    orch: &Orchestrator,
    fields: &BTreeMap<String, Value>,
    labels: &[String],
    recipe: Option<&str>,
    origin: Option<&crate::issues::crud::IssueAuthorship>,
) -> Result<String, String> {
    let origin = origin
        .cloned()
        .ok_or("issue sink requires typed origin authorship")?;
    let project = fields
        .get("project")
        .and_then(Value::as_str)
        .ok_or("issue sink requires the fact's project field")?;
    let title = fields
        .get("title")
        .or_else(|| fields.get("text"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or("issue sink requires a non-empty title or text field")?;
    let description = fields
        .get("body")
        .or_else(|| fields.get("description"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let outcome = crate::mcp::handlers::issues::create_issue_in_project(
        orch,
        project,
        title.to_owned(),
        description,
        Some(labels.to_vec()),
        None,
        None,
        None,
        origin,
    )
    .await?;
    if let Some(recipe) = recipe {
        crate::mcp::handlers::executions::start_execution_from_collection(
            orch,
            project,
            outcome.number,
            Some(recipe),
            None,
            None,
            None,
        )
        .await
        .map_err(|error| {
            format!(
                "{} was created, but execution start failed: {error}",
                outcome.uri
            )
        })?;
    }
    Ok(outcome.uri)
}

async fn fire_label_sink(
    orch: &Orchestrator,
    bindings: Bindings<'_>,
    issue: &ArgumentBinding,
    labels: &[String],
) -> Result<String, String> {
    let uri = bindings.string(issue)?;
    let Some(CairnResource::Issue { project, number }) = parse_uri(&uri) else {
        return Err(format!(
            "label sink target must be a canonical issue URI: {uri}"
        ));
    };
    let canonical = cairn_common::uri::build_issue_uri(&project, number);
    let db = orch.db.for_project(&project).await;
    let target = canonical.clone();
    let labels = labels.to_vec();
    db.write(|conn| {
        let target = target.clone();
        let labels = labels.clone();
        Box::pin(async move {
            let resolved = crate::issues::relations::resolve_issue_uri(conn, &target)
                .await?
                .ok_or_else(|| {
                    crate::storage::DbError::Row(format!("issue not found: {target}"))
                })?;
            crate::labels::attach::add_issue_labels(
                conn,
                &resolved.issue_id,
                &labels,
                chrono::Utc::now().timestamp(),
            )
            .await
            .map_err(crate::storage::DbError::Row)?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())?;
    orch.notifier.emit_change("labels");
    orch.notifier.emit_change("issue_labels");
    orch.notifier.emit_change("issues");
    Ok(canonical)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSubmission {
    pub route_id: String,
    pub scope_key: String,
    pub project_id: Option<String>,
    pub fact: RouteFact,
    pub transforms_json: Option<String>,
    pub created_at: i64,
    pub binding_ref: String,
    pub text: String,
    pub context: String,
    pub job_id: Option<String>,
    pub initiated_by: Option<String>,
    #[serde(default)]
    pub destination: Option<crate::channels::ConversationAddress>,
}

pub async fn dispatch(
    orch: &Orchestrator,
    fact: RouteFact,
    presence: Presence,
    context: RouteContext<'_>,
) -> Result<Vec<ChannelSubmission>, String> {
    if fact.is_route_generated() {
        return Ok(Vec::new());
    }
    let observed_at = chrono::Utc::now().timestamp_millis();
    let mut scopes = vec!["workspace".to_string()];
    if let Some(project_id) = context.project_id {
        scopes.push(format!("project:{project_id}"));
    }
    let fields_json = serde_json::to_string(&fact.fields).map_err(|error| error.to_string())?;
    for scope_key in scopes {
        crate::storage::upsert_route_fact_sample(
            &orch.db.local,
            crate::storage::RouteFactSample {
                scope_key,
                source: fact.source.clone(),
                identity: fact.identity.clone(),
                fields_json: fields_json.clone(),
                summary: fact.summary.clone(),
                observed_at,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    let routes = config::routes::list_routes(&orch.config_dir, context.project_path)?;
    let mut submissions = Vec::new();
    let mut firing_count = 0;
    for result in routes {
        let ConfigResult::Ok(route) = result else {
            continue;
        };
        if !route.definition.enabled {
            continue;
        }
        let graph = RouteGraph::new(&route.definition)?;
        // Whether the route fires at all is settled before any response node
        // runs: a trigger that does not match must cost nothing.
        if !graph
            .triggers()
            .any(|index| trigger_matches(graph.node(index), &fact, presence))
        {
            continue;
        }
        if firing_count == MAX_FIRINGS_PER_FACT {
            break;
        }
        firing_count += 1;
        let scope_key = if route.is_project_scoped {
            format!("project:{}", context.project_id.unwrap_or("unknown"))
        } else {
            "workspace".to_string()
        };
        let now = chrono::Utc::now().timestamp_millis();
        if let Some(window) = route.definition.dedupe {
            let since =
                now.saturating_sub(window.duration().as_millis().min(i64::MAX as u128) as i64);
            if crate::storage::has_recent_fact(
                &orch.db.local,
                &scope_key,
                &route.id,
                &fact.identity,
                since,
            )
            .await
            .map_err(|e| e.to_string())?
            {
                // A dedupe drop is one decision for the whole route: the fact
                // never reached any sink, so it records once, not once per sink.
                let first = graph
                    .sinks()
                    .next()
                    .expect("a validated route declares at least one sink");
                let RouteNodeConfig::Sink { sink } = &graph.node(first).config else {
                    unreachable!("sinks() yields sink nodes")
                };
                record(
                    orch,
                    Firing {
                        route_id: &route.id,
                        scope_key: &scope_key,
                        project_id: context.project_id,
                        fact: &fact,
                        sink,
                        created_at: now,
                    },
                    None,
                    Outcome::dropped("dedupe"),
                )
                .await?;
                continue;
            }
        }

        let route_id = route.id.clone();
        let run = |response: &str, args: Value| -> ResponseFuture {
            let (orch, response, label) = (
                orch.clone(),
                response.to_owned(),
                format!("route:{route_id}"),
            );
            Box::pin(async move {
                responses::invoke(&orch, &response, &args, ResponseCaller::Internal { label })
                    .await
                    .map(|outcome| outcome.text)
                    .map_err(|error| error.to_string())
            })
        };
        let walked = walk(&graph, &fact, presence, &run).await;

        // Each sink records its own outcome on its own path's content: one sink
        // failing neither blocks the others nor speaks for them, and the journal
        // keeps a row per delivery.
        for delivery in &walked.deliveries {
            let firing = Firing {
                route_id: &route.id,
                scope_key: &scope_key,
                project_id: context.project_id,
                fact: &fact,
                sink: delivery.sink,
                created_at: now,
            };
            let transforms_json = serde_json::to_string(&delivery.transforms).ok();
            let fields = &delivery.fields;
            match delivery.sink {
                RouteSink::Channel {
                    destination,
                    initiated_by,
                } => {
                    submissions.push(ChannelSubmission {
                        // The once-only delivery key. A second channel sink needs
                        // its own, so the position is appended — but the FIRST
                        // keeps the historical form, because fact identities are
                        // deliberately stable across observations and a new key
                        // shape would miss the ledger row for every fact already
                        // delivered, re-notifying all of them once on upgrade.
                        binding_ref: match delivery.position {
                            0 => format!("route:{}:{}", route.id, fact.identity),
                            position => {
                                format!("route:{}:{}:{position}", route.id, fact.identity)
                            }
                        },
                        route_id: route.id.clone(),
                        scope_key: scope_key.clone(),
                        project_id: context.project_id.map(str::to_owned),
                        fact: fact.clone(),
                        transforms_json,
                        created_at: now,
                        text: text_of(fields).to_owned(),
                        context: fields
                            .get("context")
                            .and_then(Value::as_str)
                            .unwrap_or("[Cairn]")
                            .to_string(),
                        job_id: fields
                            .get("jobId")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        initiated_by: initiated_by.clone(),
                        destination: match destination {
                            ChannelDestination::Subscriptions => None,
                            ChannelDestination::Conversation(address) => Some(address.clone()),
                        },
                    });
                }
                RouteSink::Message { target } => {
                    let delivered = match resolve_message_target(target) {
                        Ok((project, number, canonical)) => {
                            super::with_provenance(route.id.clone(), async {
                                crate::mcp::handlers::messages::append_project_or_issue_message(
                                    orch,
                                    crate::mcp::handlers::messages::MessageAuthor::Route(&route.id),
                                    &project,
                                    number,
                                    text_of(fields),
                                )
                                .await
                            })
                            .await
                            .map(|_| canonical)
                        }
                        Err(error) => Err(error),
                    };
                    record(
                        orch,
                        firing,
                        transforms_json,
                        Outcome::delivery(delivered, text_payload(fields)),
                    )
                    .await?;
                }
                RouteSink::Label { issue, labels } => {
                    let bindings = Bindings {
                        graph: &graph,
                        fields,
                        outputs: &walked.outputs,
                    };
                    let result = super::with_provenance(
                        route.id.clone(),
                        fire_label_sink(orch, bindings, issue, labels),
                    )
                    .await;
                    record(
                        orch,
                        firing,
                        transforms_json,
                        Outcome::delivery(
                            result,
                            crate::storage::firing_snapshot(&format!(
                                "Added labels: {}",
                                labels.join(", ")
                            )),
                        ),
                    )
                    .await?;
                }
                RouteSink::Issue { labels, recipe } => {
                    let result = super::with_provenance(
                        route.id.clone(),
                        fire_issue_sink(
                            orch,
                            fields,
                            labels,
                            recipe.as_deref(),
                            fact.origin.as_ref(),
                        ),
                    )
                    .await;
                    record(
                        orch,
                        firing,
                        transforms_json,
                        Outcome::delivery(result, issue_payload(fields)),
                    )
                    .await?;
                }
            }
        }
    }
    Ok(submissions)
}

pub async fn record_channel_outcome(
    orch: &Orchestrator,
    submission: &ChannelSubmission,
    result: Result<String, String>,
) -> Result<(), String> {
    let sink = RouteSink::Channel {
        destination: submission.destination.clone().map_or(
            ChannelDestination::Subscriptions,
            ChannelDestination::Conversation,
        ),
        initiated_by: submission.initiated_by.clone(),
    };
    record(
        orch,
        Firing {
            route_id: &submission.route_id,
            scope_key: &submission.scope_key,
            project_id: submission.project_id.as_deref(),
            fact: &submission.fact,
            sink: &sink,
            created_at: submission.created_at,
        },
        submission.transforms_json.clone(),
        Outcome::delivery(result, crate::storage::firing_snapshot(&submission.text)),
    )
    .await?;
    Ok(())
}

async fn record(
    orch: &Orchestrator,
    firing: Firing<'_>,
    transforms_json: Option<String>,
    outcome: Outcome,
) -> Result<crate::storage::RouteFiringRecord, String> {
    // A sink's declared address is the fallback ref; a firing that reached the
    // sink reports the address it actually landed on.
    let (sink_kind, declared_ref) = match firing.sink {
        RouteSink::Channel { destination, .. } => (
            "channel",
            Some(match destination {
                ChannelDestination::Subscriptions => "subscriptions".to_string(),
                ChannelDestination::Conversation(address) => address.to_string(),
            }),
        ),
        RouteSink::Message { target } => ("message", Some(target.clone())),
        RouteSink::Issue { .. } => ("issue", None),
        RouteSink::Label { .. } => ("label", None),
    };
    crate::storage::insert_route_firing(
        &orch.db.local,
        NewRouteFiring {
            route_id: firing.route_id.into(),
            scope_key: firing.scope_key.into(),
            project_id: firing.project_id.map(str::to_owned),
            trigger_source: firing.fact.source.clone(),
            fact_identity: firing.fact.identity.clone(),
            fact_summary: firing
                .fact
                .summary
                .as_deref()
                .and_then(crate::storage::firing_snapshot),
            status: outcome.status.into(),
            drop_reason: outcome.drop_reason.map(str::to_owned),
            transforms_json,
            sink_kind: sink_kind.into(),
            sink_ref: outcome.sink_ref.or(declared_ref),
            payload_text: outcome.payload,
            error: outcome.error,
            created_at: firing.created_at,
        },
    )
    .await
    .map_err(|e| e.to_string())
}

/// The post-transform content a text sink hands over.
fn text_payload(fields: &BTreeMap<String, Value>) -> Option<String> {
    fields
        .get("text")
        .and_then(Value::as_str)
        .and_then(crate::storage::firing_snapshot)
}

/// An issue sink's payload is the issue it proposes, which is its title.
fn issue_payload(fields: &BTreeMap<String, Value>) -> Option<String> {
    fields
        .get("title")
        .or_else(|| fields.get("text"))
        .and_then(Value::as_str)
        .and_then(crate::storage::firing_snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::routes::{parse_definition, FactRegistry, RouteDefinition};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{migrated_test_db, SearchIndex};
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
        VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
    };
    use std::sync::Arc;

    fn definition(yaml: &str) -> RouteDefinition {
        parse_definition(yaml, &FactRegistry::default()).expect("valid route")
    }

    fn thread_fact(text: &str) -> RouteFact {
        RouteFact {
            source: "thread_stream".into(),
            identity: "event:1".into(),
            fields: BTreeMap::from([("text".into(), Value::String(text.into()))]),
            origin: None,
            summary: Some(text.into()),
            route_provenance: None,
        }
    }

    fn external_authorship() -> crate::issues::crud::IssueAuthorship {
        let principal = PrincipalRef::External {
            provider: "telegram".into(),
            namespace: "channel_sender".into(),
            id: "sender-42".into(),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::ChannelAllowlist,
            VerificationStatus::Verified,
            None,
            None,
            None,
            None,
            VerificationStrength::new("allowlist").unwrap(),
            41,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::ChannelReply,
            Address::Channel {
                provider: "telegram".into(),
                conversation: "chat-7".into(),
                sender: "sender-42".into(),
                observed_alias: Some("Ada".into()),
            },
            verification,
            42,
            None,
        )
        .unwrap();
        crate::issues::crud::IssueAuthorship::new(
            principal.clone(),
            AppearanceSnapshot::new(principal, evidence, Vec::new(), None).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn response_transforms_preserve_the_exact_origin_snapshot() {
        let definition = definition(
            "name: Preserve origin\ndescription: transforms content only\nnodes:\n  - id: input\n    type: trigger\n    when: { fact: thread_stream }\n  - id: transform\n    type: response\n    response: conveyor\n  - id: issue\n    type: sink\n    sink: { kind: issue, labels: [routed] }\nedges:\n  - { from: input, to: transform }\n  - { from: transform, to: issue }\n",
        );
        let graph = RouteGraph::new(&definition).unwrap();
        let mut fact = thread_fact("original");
        fact.origin = Some(external_authorship());
        let expected = fact.origin.clone();
        let run: RunResponse =
            &|_, _| -> ResponseFuture { Box::pin(async { Ok("rewritten".into()) }) };
        let walked = walk(&graph, &fact, Presence::Away, run).await;
        assert_eq!(text_of(&walked.deliveries[0].fields), "rewritten");
        assert_eq!(fact.origin, expected);
    }

    #[tokio::test]
    async fn issue_sink_refuses_missing_origin_before_reading_mutable_fields() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _) = orchestrator(temp.path(), &[]).await;
        let error = fire_issue_sink(&orch, &BTreeMap::new(), &[], None, None)
            .await
            .unwrap_err();
        assert_eq!(error, "issue sink requires typed origin authorship");
    }

    #[test]
    fn message_targets_resolve_to_useful_canonical_history_refs() {
        assert_eq!(
            resolve_message_target("cairn://p/cairn/42").unwrap(),
            ("cairn".into(), Some(42), "cairn://p/cairn/42".into())
        );
        assert!(resolve_message_target("cairn://p/cairn/42/messages").is_err());
    }

    /// The motivating branch: one trigger, one edge through a response node to a
    /// channel sink and another straight to a message sink. The two sinks are
    /// meant to receive different things.
    #[tokio::test]
    async fn two_branches_off_one_trigger_carry_their_own_content_to_their_own_sink() {
        let definition = definition(
            "name: Split\ndescription: condensed to the phone, full text to the stream\nnodes:\n  - id: thread\n    type: trigger\n    when: { fact: thread_stream }\n  - id: condense\n    type: response\n    response: conveyor\n  - id: phone\n    type: sink\n    sink: { kind: channel, register: notify }\n  - id: stream\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/1' }\nedges:\n  - { from: thread, to: condense }\n  - { from: condense, to: phone }\n  - { from: thread, to: stream }\n",
        );
        let graph = RouteGraph::new(&definition).unwrap();
        let run: RunResponse = &|_response: &str, _args: Value| -> ResponseFuture {
            Box::pin(async { Ok("condensed".to_string()) })
        };

        let walked = walk(
            &graph,
            &thread_fact("the whole long message"),
            Presence::Away,
            run,
        )
        .await;

        let content = |position: usize| {
            let delivery = walked
                .deliveries
                .iter()
                .find(|delivery| delivery.position == position)
                .expect("sink was delivered to");
            (
                text_of(&delivery.fields).to_owned(),
                delivery.transforms.len(),
            )
        };
        assert_eq!(content(0), ("condensed".to_string(), 1));
        assert_eq!(content(1), ("the whole long message".to_string(), 0));
    }

    #[tokio::test]
    async fn a_shared_prefix_runs_once_however_many_sinks_hang_off_it() {
        let definition = definition(
            "name: Fan out\ndescription: one condense, two deliveries\nnodes:\n  - id: thread\n    type: trigger\n    when: { fact: thread_stream }\n  - id: condense\n    type: response\n    response: conveyor\n  - id: phone\n    type: sink\n    sink: { kind: channel, register: notify }\n  - id: stream\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/1' }\nedges:\n  - { from: thread, to: condense }\n  - { from: condense, to: phone }\n  - { from: condense, to: stream }\n",
        );
        let graph = RouteGraph::new(&definition).unwrap();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let run: RunResponse = &|_response: &str, _args: Value| -> ResponseFuture {
            Box::pin(async { Ok("condensed".to_string()) })
        };
        let counted: RunResponse = &|response: &str, args: Value| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            run(response, args)
        };

        let walked = walk(&graph, &thread_fact("long"), Presence::Away, counted).await;
        assert_eq!(walked.deliveries.len(), 2);
        assert!(walked
            .deliveries
            .iter()
            .all(|delivery| text_of(&delivery.fields) == "condensed"));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_response_records_itself_and_passes_its_input_through() {
        let definition = definition(
            "name: Chain\ndescription: two steps\nnodes:\n  - id: thread\n    type: trigger\n    when: { fact: thread_stream }\n  - id: first\n    type: response\n    response: broken\n  - id: second\n    type: response\n    response: conveyor\n    args:\n      text: { field: text }\n  - id: stream\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/1' }\nedges:\n  - { from: thread, to: first }\n  - { from: first, to: second }\n  - { from: second, to: stream }\n",
        );
        let graph = RouteGraph::new(&definition).unwrap();
        let seen: std::sync::Mutex<Vec<Value>> = std::sync::Mutex::new(Vec::new());
        let run: RunResponse = &|response: &str, args: Value| -> ResponseFuture {
            let response = response.to_owned();
            Box::pin(async move {
                match response.as_str() {
                    "broken" => Err("backend unavailable".into()),
                    _ => Ok(format!(
                        "summary of {}",
                        args["text"].as_str().unwrap_or("")
                    )),
                }
            })
        };
        let recording: RunResponse = &|response: &str, args: Value| {
            seen.lock().unwrap().push(args.clone());
            run(response, args)
        };

        let walked = walk(&graph, &thread_fact("original"), Presence::Away, recording).await;
        // The second step saw the first step's input, because the first failed.
        assert_eq!(
            seen.lock().unwrap()[1]["text"],
            Value::String("original".into())
        );
        let delivery = &walked.deliveries[0];
        assert_eq!(text_of(&delivery.fields), "summary of original");
        assert_eq!(delivery.transforms.len(), 2);
        assert_eq!(
            delivery.transforms[0]["status"],
            Value::String("failed".into())
        );
        assert_eq!(delivery.transforms[1]["status"], Value::String("ok".into()));
    }

    #[tokio::test]
    async fn only_the_branches_below_a_matching_trigger_are_walked() {
        let definition = definition(
            "name: Two triggers\ndescription: each with its own sink\nnodes:\n  - id: thread\n    type: trigger\n    when: { fact: thread_stream }\n  - id: attention\n    type: trigger\n    when: { fact: attention }\n  - id: stream\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/1' }\n  - id: board\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/2' }\nedges:\n  - { from: thread, to: stream }\n  - { from: attention, to: board }\n",
        );
        let graph = RouteGraph::new(&definition).unwrap();
        let run: RunResponse =
            &|_: &str, _: Value| -> ResponseFuture { Box::pin(async { Ok(String::new()) }) };
        let walked = walk(&graph, &thread_fact("hello"), Presence::Away, run).await;
        assert_eq!(walked.deliveries.len(), 1);
        assert_eq!(walked.deliveries[0].position, 0);
    }

    async fn orchestrator(
        temp: &Path,
        routes: &[(&str, &str)],
    ) -> (Orchestrator, Arc<crate::storage::LocalDb>) {
        let config = temp.join("config");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        for (name, body) in routes {
            std::fs::write(config.join(format!("routes/{name}.yaml")), body).unwrap();
        }
        let db = Arc::new(
            migrated_test_db(&format!(
                "route-{}.db",
                temp.file_name().unwrap().to_string_lossy()
            ))
            .await,
        );
        let search = Arc::new(SearchIndex::open_or_create(temp.join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config,
        )
        .build();
        (orch, db)
    }

    const PROJECT_FIXTURE: &str =
        "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('i', 'p', 42, 'Target', 'active', 1, 1);";

    fn message_route(name: &str, extra: &str) -> String {
        format!("name: {name}\ndescription: Routes a thread update into an issue stream\nnodes:\n  - id: thread\n    type: trigger\n    when: {{ fact: thread_stream }}\n  - id: stream\n    type: sink\n    sink: {{ kind: message, target: 'cairn://p/cairn/42' }}\nedges:\n  - {{ from: thread, to: stream }}\n{extra}")
    }

    #[tokio::test]
    async fn message_sink_uses_normal_issue_stream_and_records_route_authorship() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(
            temp.path(),
            &[("message-test", &message_route("Message test", ""))],
        )
        .await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();

        dispatch(
            &orch,
            thread_fact("routed update"),
            Presence::Away,
            RouteContext {
                project_id: Some("p"),
                project_path: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            db.query_text(
                "SELECT sender_name || ':' || content FROM messages WHERE channel_type = 'issue' AND channel_id = 'cairn/42'",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("route:message-test:routed update")
        );
        let firing = crate::storage::list_route_firings(&db, "workspace", "message-test", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(firing.status, "fired");
        assert_eq!(firing.sink_ref.as_deref(), Some("cairn://p/cairn/42"));
        // The journal keeps the content, not just the address it landed on, so
        // the firing still reads after the target's messages are compacted.
        assert_eq!(firing.fact_summary.as_deref(), Some("routed update"));
        assert_eq!(firing.payload_text.as_deref(), Some("routed update"));
    }

    /// Each sink journals its own row on its own path: the branch through a
    /// response node records that step, the branch that skipped it does not.
    #[tokio::test]
    async fn every_sink_journals_its_own_row_for_the_path_that_reached_it() {
        let temp = tempfile::tempdir().unwrap();
        let route = "name: Split\ndescription: one branch transformed, one not\nnodes:\n  - id: thread\n    type: trigger\n    when: { fact: thread_stream }\n  - id: condense\n    type: response\n    response: conveyor\n  - id: transformed\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/42' }\n  - id: verbatim\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/43' }\nedges:\n  - { from: thread, to: condense }\n  - { from: condense, to: transformed }\n  - { from: thread, to: verbatim }\n";
        let (orch, db) = orchestrator(temp.path(), &[("split", route)]).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        db.execute_batch(
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('i2', 'p', 43, 'Second', 'active', 1, 1);",
        )
        .await
        .unwrap();

        dispatch(
            &orch,
            thread_fact("routed update"),
            Presence::Away,
            RouteContext {
                project_id: Some("p"),
                project_path: None,
            },
        )
        .await
        .unwrap();

        let firings = crate::storage::list_route_firings(&db, "workspace", "split", 10)
            .await
            .unwrap();
        assert_eq!(firings.len(), 2, "one row per delivery");
        let row = |target: &str| {
            firings
                .iter()
                .find(|firing| firing.sink_ref.as_deref() == Some(target))
                .unwrap_or_else(|| panic!("a row for {target}"))
        };
        // There is no model backend in a test, so the response step fails and its
        // branch carries the fact through — but the record of that step belongs
        // to that branch alone.
        assert!(row("cairn://p/cairn/42")
            .transforms_json
            .as_deref()
            .unwrap()
            .contains("conveyor"));
        assert_eq!(
            row("cairn://p/cairn/43").transforms_json.as_deref(),
            Some("[]")
        );
    }

    #[tokio::test]
    async fn a_deduped_firing_still_says_what_the_fact_was() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(
            temp.path(),
            &[(
                "dedupe-test",
                &message_route("Dedupe test", "dedupe: 10m\n"),
            )],
        )
        .await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        let context = || RouteContext {
            project_id: Some("p"),
            project_path: None,
        };

        dispatch(
            &orch,
            thread_fact("routed update"),
            Presence::Away,
            context(),
        )
        .await
        .unwrap();
        dispatch(
            &orch,
            thread_fact("routed update"),
            Presence::Away,
            context(),
        )
        .await
        .unwrap();

        let dropped = crate::storage::list_route_firings(&db, "workspace", "dedupe-test", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            (dropped.status.as_str(), dropped.drop_reason.as_deref()),
            ("dropped", Some("dedupe"))
        );
        assert_eq!(dropped.fact_summary.as_deref(), Some("routed update"));
        assert_eq!(
            dropped.payload_text, None,
            "a firing that never reached its sink carried no payload"
        );
    }

    /// A dedupe drop is one decision for the whole route, so a branching route
    /// still records exactly one drop.
    #[tokio::test]
    async fn dedupe_records_one_drop_however_many_sinks_the_route_has() {
        let temp = tempfile::tempdir().unwrap();
        let route = "name: Two sinks\ndescription: both deduped together\nnodes:\n  - id: thread\n    type: trigger\n    when: { fact: thread_stream }\n  - id: first\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/42' }\n  - id: second\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn/43' }\nedges:\n  - { from: thread, to: first }\n  - { from: thread, to: second }\ndedupe: 10m\n";
        let (orch, db) = orchestrator(temp.path(), &[("two-sinks", route)]).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        db.execute_batch(
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
               VALUES('i2', 'p', 43, 'Second', 'active', 1, 1);",
        )
        .await
        .unwrap();
        let context = || RouteContext {
            project_id: Some("p"),
            project_path: None,
        };

        dispatch(&orch, thread_fact("update"), Presence::Away, context())
            .await
            .unwrap();
        dispatch(&orch, thread_fact("update"), Presence::Away, context())
            .await
            .unwrap();

        let firings = crate::storage::list_route_firings(&db, "workspace", "two-sinks", 10)
            .await
            .unwrap();
        assert_eq!(
            firings.iter().filter(|f| f.status == "dropped").count(),
            1,
            "the fact reached no sink, so the drop is recorded once"
        );
        assert_eq!(firings.iter().filter(|f| f.status == "fired").count(), 2);
    }

    #[tokio::test]
    async fn route_provenance_prevents_sink_reentry() {
        let temp = tempfile::tempdir().unwrap();
        let route = "name: loop\ndescription: guard\nnodes:\n  - id: attention\n    type: trigger\n    when: { fact: attention }\n  - id: out\n    type: sink\n    sink: { kind: message, target: 'cairn://p/cairn' }\nedges:\n  - { from: attention, to: out }\n";
        let (orch, db) = orchestrator(temp.path(), &[("loop", route)]).await;
        let submissions = dispatch(
            &orch,
            RouteFact {
                source: "attention".into(),
                identity: "route-produced".into(),
                fields: BTreeMap::new(),
                origin: None,
                summary: None,
                route_provenance: Some("origin".into()),
            },
            Presence::Away,
            RouteContext {
                project_id: None,
                project_path: None,
            },
        )
        .await
        .unwrap();
        assert!(submissions.is_empty());
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn test_definition_previews_without_delivering_or_journaling() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        let draft = definition(&message_route("Dry run", ""));

        let result = test_definition(
            &orch,
            &draft,
            thread_fact("preview only"),
            Presence::Away,
            RouteContext {
                project_id: Some("p"),
                project_path: None,
            },
        )
        .await
        .unwrap();

        assert!(result.matched);
        assert_eq!(result.sink_previews.len(), 1);
        assert_eq!(result.sink_previews[0]["text"], "preview only");
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM messages", ())
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn firing_limit_counts_non_channel_sinks() {
        let temp = tempfile::tempdir().unwrap();
        let routes: Vec<(String, String)> = (0..=MAX_FIRINGS_PER_FACT)
            .map(|index| {
                (
                    format!("cap-{index:03}"),
                    message_route(&format!("cap {index}"), ""),
                )
            })
            .collect();
        let borrowed: Vec<(&str, &str)> = routes
            .iter()
            .map(|(name, body)| (name.as_str(), body.as_str()))
            .collect();
        let (orch, db) = orchestrator(temp.path(), &borrowed).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();

        dispatch(
            &orch,
            thread_fact("bounded"),
            Presence::Away,
            RouteContext {
                project_id: Some("p"),
                project_path: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                .await
                .unwrap(),
            Some(MAX_FIRINGS_PER_FACT as i64)
        );
    }
}
