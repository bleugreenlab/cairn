use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use cairn_uri::{parse_uri, CairnResource};
use serde_json::{Map, Value};

use super::decision::{self as decisions, DecisionRun};
use super::definition::Bindable;
use super::{
    explain_clause, matches, ArgumentBinding, ChannelDestination, Fact, GraphEdge, Presence,
    RouteFact, RouteGraph, RouteNode, RouteNodeConfig, RouteSink,
};
use crate::{
    models::decision::DecisionNode,
    orchestrator::Orchestrator,
    responses::{self, ResponseCaller},
    storage::NewRouteFiring,
};

const MAX_FIRINGS_PER_FACT: usize = 100;

/// The most addresses one message sink delivers to on one firing when its
/// target is bound to a list (a decision's admitted picks). Each address past
/// it is journaled as dropped rather than silently ignored.
pub const MAX_TARGETS_PER_SINK: usize = 8;

/// A response whose backend never answers, for proving that a hung route
/// delays nothing but its own row.
#[cfg(test)]
pub(crate) const HANGING_RESPONSE: &str = "test-hang";

#[derive(Debug, Clone)]
pub struct RouteContext<'a> {
    pub project_id: Option<&'a str>,
    pub project_path: Option<&'a Path>,
    /// The fact log row this dispatch acts on, when it is log-driven.
    pub event: Option<LoggedEvent<'a>>,
}

/// A fact log row as a dispatch names it: `id` joins every firing the row
/// produced, and `uri` is what sink bodies and bindings read as
/// [`EVENT_FIELD`](super::EVENT_FIELD). The row, not the firing, is the
/// correlation key: a firing is journaled after delivery, one per address, in
/// a ring buffer, so it neither exists when a body renders nor lasts.
#[derive(Debug, Clone, Copy)]
pub struct LoggedEvent<'a> {
    pub id: i64,
    pub uri: &'a str,
}

/// What a dry run binds as `event` when its sample has no fact log row. It is
/// deliberately not a URI, so it cannot be mistaken for one in a preview.
pub const EVENT_PLACEHOLDER: &str = "<event: this dry run has no fact log row>";

/// Where a message sink delivers: a project or issue stream, or a thread.
#[derive(Debug, Clone, PartialEq)]
enum MessageTarget {
    Stream {
        project: String,
        number: Option<i32>,
    },
    Thread {
        project: String,
        name: String,
    },
}

/// A message target and the canonical URI the journal records for it.
fn resolve_message_target(target: &str) -> Result<(MessageTarget, String), String> {
    match parse_uri(target) {
        Some(CairnResource::Project { project }) => {
            let canonical = cairn_uri::build_project_uri(&project);
            Ok((
                MessageTarget::Stream {
                    project,
                    number: None,
                },
                canonical,
            ))
        }
        Some(CairnResource::Issue { project, number }) => {
            let canonical = cairn_uri::build_issue_uri(&project, number);
            Ok((
                MessageTarget::Stream {
                    project,
                    number: Some(number),
                },
                canonical,
            ))
        }
        Some(CairnResource::Thread {
            project,
            name,
            path,
        }) if path.is_empty() => {
            let canonical = cairn_uri::build_thread_uri(&project, &name);
            Ok((MessageTarget::Thread { project, name }, canonical))
        }
        _ => Err(format!(
            "message target must resolve to a project, issue, or thread: {target}"
        )),
    }
}

/// Append a route's message to `target`, authored as the route.
async fn deliver_message(
    orch: &Orchestrator,
    route_id: &str,
    target: &MessageTarget,
    text: &str,
) -> Result<(), String> {
    match target {
        MessageTarget::Stream { project, number } => {
            crate::mcp::handlers::messages::append_project_or_issue_message(
                orch,
                crate::mcp::handlers::messages::MessageAuthor::Route(route_id),
                project,
                *number,
                text,
            )
            .await
            .map(|_| ())
        }
        MessageTarget::Thread { project, name } => {
            let db = orch.db.for_project(project).await;
            let project_id = db
                .query_text(
                    "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1) LIMIT 1",
                    (project.clone(),),
                )
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("Project not found: {project}"))?;
            let thread = crate::threads::crud::list(&db, &project_id)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|thread| thread.name == *name)
                .ok_or_else(|| {
                    format!(
                        "Thread not found: {}",
                        cairn_uri::build_thread_uri(project, name)
                    )
                })?;
            crate::messages::delivery::append_thread_message(
                orch,
                &db,
                &thread.id,
                None,
                &format!("route:{route_id}"),
                text,
            )
            .await
            .map(|_| ())
        }
    }
}

/// What a firing's journal row names as its sink: a sink node, or the
/// decision whose answers matched no outgoing edge.
#[derive(Clone, Copy)]
enum FiringSink<'a> {
    Route(&'a RouteSink),
    Decision(&'a str),
}

/// One route's attempt on one fact, as the journal records it.
#[derive(Clone, Copy)]
struct Firing<'a> {
    route_id: &'a str,
    scope_key: &'a str,
    project_id: Option<&'a str>,
    fact: &'a RouteFact,
    sink: FiringSink<'a>,
    created_at: i64,
    fact_log_id: Option<i64>,
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

    fn sink_drop(drop: SinkDrop, payload: Option<String>) -> Self {
        Self {
            sink_ref: drop.sink_ref,
            payload,
            error: Some(drop.error),
            ..Self::dropped(drop.reason)
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

/// Why a sink will not deliver to an address it resolved on a firing.
#[derive(Debug, Clone, PartialEq)]
struct SinkDrop {
    /// `unbound_target`, `undeliverable`, or `fan_out_limit`.
    reason: &'static str,
    /// The address it resolved to, when it resolved to one.
    sink_ref: Option<String>,
    error: String,
}

impl SinkDrop {
    fn new(reason: &'static str, sink_ref: Option<String>, error: String) -> Self {
        Self {
            reason,
            sink_ref,
            error,
        }
    }

    fn preview(&self) -> Value {
        serde_json::json!({"address": self.sink_ref, "dropped": self.reason, "error": self.error})
    }
}

/// One address a message sink resolved to on a firing, or why not.
#[derive(Debug, Clone, PartialEq)]
enum TargetPlan {
    Deliver {
        target: MessageTarget,
        canonical: String,
    },
    SinkDrop(SinkDrop),
}

fn describe_binding(binding: &ArgumentBinding) -> String {
    match binding {
        ArgumentBinding::Field { field } => format!("field '{field}'"),
        ArgumentBinding::Node { from } => format!("node '{from}'"),
        ArgumentBinding::Value { .. } => "the literal".to_string(),
    }
}

/// The addresses a bound sink address resolved to on one firing's fields.
fn bound_addresses(
    bindings: &Bindings<'_>,
    binding: &ArgumentBinding,
) -> Result<Vec<String>, SinkDrop> {
    let unbound = |error: String| SinkDrop::new("unbound_target", None, error);
    let described = describe_binding(binding);
    let addresses = match bindings.value(binding) {
        Value::String(address) => vec![address],
        Value::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Value::String(address) => Ok(address),
                other => Err(unbound(format!(
                    "{described} resolved to a list holding {other}, which is not an address"
                ))),
            })
            .collect::<Result<_, _>>()?,
        Value::Null => return Err(unbound(format!("{described} resolved to nothing"))),
        other => {
            return Err(unbound(format!(
                "{described} resolved to {other}, which is not an address"
            )))
        }
    };
    let mut seen = HashSet::new();
    let addresses: Vec<String> = addresses
        .into_iter()
        .map(|address| address.trim().to_string())
        .filter(|address| !address.is_empty() && seen.insert(address.clone()))
        .collect();
    if addresses.is_empty() {
        return Err(unbound(format!("{described} resolved to no address")));
    }
    Ok(addresses)
}

/// Where a message sink delivers on one firing, address by address: its
/// literal, or every address its binding resolved to up to
/// [`MAX_TARGETS_PER_SINK`].
fn message_targets(bindings: &Bindings<'_>, target: &Bindable<String>) -> Vec<TargetPlan> {
    let addresses = match target {
        Bindable::Fixed(target) => vec![target.clone()],
        Bindable::Bound(binding) => match bound_addresses(bindings, binding) {
            Ok(addresses) => addresses,
            Err(drop) => return vec![TargetPlan::SinkDrop(drop)],
        },
    };
    addresses
        .into_iter()
        .enumerate()
        .map(|(position, address)| {
            if position >= MAX_TARGETS_PER_SINK {
                return TargetPlan::SinkDrop(SinkDrop::new(
                    "fan_out_limit",
                    Some(address),
                    format!(
                        "a message sink delivers to at most {MAX_TARGETS_PER_SINK} addresses per firing"
                    ),
                ));
            }
            match resolve_message_target(&address) {
                Ok((target, canonical)) => TargetPlan::Deliver { target, canonical },
                Err(error) => TargetPlan::SinkDrop(SinkDrop::new("undeliverable", Some(address), error)),
            }
        })
        .collect()
}

/// The conversation a bound channel destination resolved to on one firing:
/// one address, parsed, among the configured conversations, on a provider
/// that can append to it.
async fn bound_conversation(
    orch: &Orchestrator,
    bindings: &Bindings<'_>,
    binding: &ArgumentBinding,
) -> Result<crate::channels::ConversationAddress, SinkDrop> {
    let described = describe_binding(binding);
    let value = match bindings.value(binding) {
        Value::String(value) if !value.trim().is_empty() => value,
        Value::Array(_) => {
            return Err(SinkDrop::new(
                "unbound_target",
                None,
                format!(
                "a channel destination takes one conversation, and {described} resolved to a list"
            ),
            ))
        }
        Value::Null | Value::String(_) => {
            return Err(SinkDrop::new(
                "unbound_target",
                None,
                format!("{described} resolved to nothing"),
            ))
        }
        other => {
            return Err(SinkDrop::new(
                "unbound_target",
                None,
                format!("{described} resolved to {other}, which is not a conversation address"),
            ))
        }
    };
    let address: crate::channels::ConversationAddress = value.parse().map_err(|error| {
        SinkDrop::new(
            "undeliverable",
            Some(value.clone()),
            format!("'{value}' is not a conversation address: {error}"),
        )
    })?;
    let rendered = address.to_string();
    let configured = crate::resources::channels::configured_conversations(orch).await;
    match configured
        .iter()
        .find(|row| row.address == rendered && !row.placeholder)
    {
        None => Err(SinkDrop::new(
            "undeliverable",
            Some(rendered.clone()),
            format!("{rendered} is not a configured conversation"),
        )),
        Some(row) if !row.capabilities.append_to_conversation => Err(SinkDrop::new(
            "undeliverable",
            Some(rendered),
            format!("{} does not support append_to_conversation", row.provider),
        )),
        Some(_) => Ok(address),
    }
}

/* ------------------------------------------------------------- graph walk */

/// How a response node's work is actually done. It is injected rather than
/// called directly so the part worth testing — which content reaches which sink
/// along which path — can be exercised without a model backend.
type ResponseFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
type RunResponse<'f> = &'f (dyn Fn(&str, Value) -> ResponseFuture + Sync);

/// How a decision node's questions are asked, injected for the same reason.
/// The production closure resolves option sources and calls the facet.
type DecisionFuture = Pin<Box<dyn Future<Output = DecisionRun> + Send>>;
type RunDecision<'f> = &'f (dyn Fn(&DecisionNode, String) -> DecisionFuture + Sync);

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

/// A decision whose answers matched none of its outgoing edges: the firing
/// stops there and journals the answers, never a silent no-op.
struct Unrouted<'a> {
    node_id: &'a str,
    /// The path's record up to and including the decision.
    history: Vec<Value>,
    /// What the decision judged.
    input: String,
}

/// Everything one fact makes of one route's graph.
struct Walk<'a> {
    deliveries: Vec<Delivery<'a>>,
    /// Each response node's output, by node index, so a downstream binding can
    /// read a specific node rather than only the content flowing into it.
    outputs: HashMap<usize, String>,
    nodes: Vec<Value>,
    /// Every edge's verdict: whether its source was reached, whether it was
    /// taken, and the failing tests when it was not.
    edges: Vec<Value>,
    unrouted: Vec<Unrouted<'a>>,
}

/// The content and record a path carries into the node below it.
#[derive(Debug, Clone, Default)]
struct Carried {
    /// The content a response produced on this path; `None` keeps the fact's
    /// own `text`.
    text: Option<String>,
    history: Vec<Value>,
}

/// The fields a node works on: the fact's, the content arriving on its path as
/// `text`, the `answers.*` of every decision that dominates it, nearest last so
/// it wins a shared question name, and `event`, the fact log row's URI, when
/// the dispatch has one. This is the one definition of what a node can read,
/// shared by edge clauses, bindings, and sink addresses.
pub(crate) fn node_fields(
    graph: &RouteGraph<'_>,
    index: usize,
    fact_fields: &BTreeMap<String, Value>,
    text: Option<&str>,
    answers: &HashMap<usize, BTreeMap<String, Value>>,
    event: Option<&str>,
) -> BTreeMap<String, Value> {
    let mut fields = fact_fields.clone();
    if let Some(text) = text {
        fields.insert("text".into(), Value::String(text.to_owned()));
    }
    for (decision, _) in graph.dominating_decisions(index, false) {
        if let Some(own) = answers.get(&decision) {
            fields.extend(own.iter().map(|(key, value)| (key.clone(), value.clone())));
        }
    }
    if let Some(event) = event {
        fields.insert(super::EVENT_FIELD.into(), Value::String(event.to_owned()));
    }
    fields
}

/// Whether a fact standing at an edge's source with `fields` takes the edge,
/// and, when it does not, each failing test.
fn edge_verdict(
    edge: &GraphEdge<'_>,
    fact: &RouteFact,
    presence: Presence,
    fields: &BTreeMap<String, Value>,
) -> (bool, Vec<String>) {
    if edge.when.is_empty() {
        return (true, Vec::new());
    }
    let at = Fact {
        source: &fact.source,
        fields,
        presence,
    };
    if matches(edge.when, &at) {
        return (true, Vec::new());
    }
    let failures = match edge.when {
        [only] => explain_clause(only, &at, None),
        clauses => clauses
            .iter()
            .enumerate()
            .flat_map(|(position, clause)| {
                explain_clause(clause, &at, None)
                    .into_iter()
                    .map(move |failure| format!("clause {}: {failure}", position + 1))
            })
            .collect(),
    };
    (false, failures)
}

/// A bound value as text for a decision to judge.
fn text_value(value: Value) -> String {
    match value {
        Value::String(text) => text,
        Value::Null => String::new(),
        other => other.to_string(),
    }
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
///
/// An edge is taken when its source was reached and its `when` holds over the
/// source's fields; a node is reached when any edge into it was taken. A
/// decision passes the content that reached it on unchanged and adds its
/// answers to the fields of everything it dominates. A decision none of whose
/// edges is taken is collected as unrouted, so the firing journals it.
async fn walk<'a>(
    graph: &'a RouteGraph<'a>,
    fact: &RouteFact,
    presence: Presence,
    run: RunResponse<'_>,
    decide: RunDecision<'_>,
    event: Option<&str>,
) -> Walk<'a> {
    let mut outputs: HashMap<usize, String> = HashMap::new();
    let mut carried: HashMap<usize, Carried> = HashMap::new();
    let mut answers: HashMap<usize, BTreeMap<String, Value>> = HashMap::new();
    let mut taken: HashSet<(usize, usize)> = HashSet::new();
    let mut deliveries = Vec::new();
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut unrouted = Vec::new();
    let positions: HashMap<usize, usize> = graph
        .sinks()
        .enumerate()
        .map(|(position, index)| (index, position))
        .collect();

    for &index in graph.order() {
        let node = graph.node(index);
        let mut decision_input: Option<String> = None;
        let reached = match &node.config {
            RouteNodeConfig::Trigger { when } => {
                let matched = trigger_matches(node, fact, presence);
                nodes.push(serde_json::json!({"nodeId":node.id,"kind":"trigger","reached":matched,"output":fact.fields,"failures":explain_clause(when, &Fact { source: &fact.source, fields: &fact.fields, presence }, None)}));
                if matched {
                    carried.insert(index, Carried::default());
                }
                matched.then(|| fact.fields.clone())
            }
            config => {
                let arrivals: Vec<usize> = graph
                    .incoming(index)
                    .iter()
                    .copied()
                    .filter(|&from| taken.contains(&(from, index)))
                    .collect();
                if arrivals.is_empty() {
                    nodes.push(serde_json::json!({"nodeId":node.id,"kind":config.type_name(),"reached":false}));
                    None
                } else {
                    // The path that carried content wins; validation allows at
                    // most one producer, so only plain fact paths can tie.
                    let inherited = arrivals
                        .iter()
                        .filter_map(|from| carried.get(from))
                        .max_by_key(|path| (path.text.is_some(), path.history.len()))
                        .cloned()
                        .unwrap_or_default();
                    let mut fields = node_fields(
                        graph,
                        index,
                        &fact.fields,
                        inherited.text.as_deref(),
                        &answers,
                        event,
                    );
                    let mut history = inherited.history.clone();
                    match config {
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
                                    // A failed step is recorded and the path
                                    // continues with the content that reached it.
                                    outputs.insert(index, text_of(&fields).to_owned());
                                    nodes.push(serde_json::json!({"nodeId":node.id,"kind":"response","reached":true,"status":"failed","output":outputs[&index],"error":error}));
                                }
                            }
                            carried.insert(
                                index,
                                Carried {
                                    text: Some(outputs[&index].clone()),
                                    history,
                                },
                            );
                        }
                        RouteNodeConfig::Decision { decision, input } => {
                            let input = match input {
                                Some(binding) => text_value(
                                    Bindings {
                                        graph,
                                        fields: &fields,
                                        outputs: &outputs,
                                    }
                                    .value(binding),
                                ),
                                None => text_of(&fields).to_owned(),
                            };
                            let asked = decide(decision, input.clone()).await;
                            let outcomes = decisions::outcomes(&asked.asked, &asked.result);
                            let summary = decisions::summary(&outcomes);
                            let error = asked.result.as_ref().err().map(ToString::to_string);
                            // The questions as asked carry the resolved
                            // options and their revisions, so every firing
                            // beneath records which versions were chosen among.
                            history.push(serde_json::json!({
                                "decision": node.id,
                                "status": if error.is_some() { "failed" } else { "ok" },
                                "asked": asked.asked,
                                "answers": summary,
                                "error": error,
                            }));
                            nodes.push(serde_json::json!({"nodeId":node.id,"kind":"decision","reached":true,"input":crate::storage::firing_snapshot(&input),"asked":asked.asked,"answers":summary,"error":error}));
                            let own = decisions::answer_fields(&outcomes);
                            fields.extend(
                                own.iter().map(|(key, value)| (key.clone(), value.clone())),
                            );
                            answers.insert(index, own);
                            carried.insert(
                                index,
                                Carried {
                                    text: inherited.text.clone(),
                                    history,
                                },
                            );
                            decision_input = Some(input);
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
                            continue;
                        }
                        RouteNodeConfig::Trigger { .. } => {
                            unreachable!("triggers are handled above")
                        }
                    }
                    Some(fields)
                }
            }
        };

        let mut routed = false;
        for edge in graph.outgoing_edges(index) {
            let to = &graph.node(edge.to).id;
            let Some(fields) = &reached else {
                edges.push(
                    serde_json::json!({"from":node.id,"to":to,"reached":false,"taken":false}),
                );
                continue;
            };
            let (matched, failures) = edge_verdict(edge, fact, presence, fields);
            if matched {
                taken.insert((edge.from, edge.to));
                routed = true;
            }
            edges.push(serde_json::json!({"from":node.id,"to":to,"reached":true,"taken":matched,"failures":failures}));
        }
        if let (Some(input), false) = (decision_input, routed) {
            unrouted.push(Unrouted {
                node_id: &node.id,
                history: carried
                    .get(&index)
                    .map(|path| path.history.clone())
                    .unwrap_or_default(),
                input,
            });
        }
    }
    Walk {
        deliveries,
        outputs,
        nodes,
        edges,
        unrouted,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteTestResult {
    pub matched: bool,
    pub nodes: Vec<Value>,
    /// Every edge's verdict: `{from, to, reached, taken, failures}`.
    pub edges: Vec<Value>,
    pub sink_previews: Vec<Value>,
}

/// A decision runner that resolves option sources against `project_id` and
/// asks through the orchestrator's one decide path.
fn production_decide(
    orch: &Orchestrator,
    project_id: Option<&str>,
) -> impl Fn(&DecisionNode, String) -> DecisionFuture + Sync {
    let (orch, project_id) = (orch.clone(), project_id.map(str::to_owned));
    move |decision: &DecisionNode, input: String| -> DecisionFuture {
        let (orch, decision, project_id) = (orch.clone(), decision.clone(), project_id.clone());
        Box::pin(
            async move { decisions::run(&orch, project_id.as_deref(), &decision, input).await },
        )
    }
}

/// Prepare once, before address resolution and fanout. The snapshot and the
/// delivery must describe the same string, while absent bodies keep the
/// upstream text/snapshot conventions (including missing or empty text).
#[derive(Debug)]
struct PreparedText {
    text: String,
    payload: Option<String>,
}

fn prepare_sink_text(
    sink: &RouteSink,
    fields: &BTreeMap<String, Value>,
) -> Result<PreparedText, crate::text_template::UnboundName> {
    let body = match sink {
        RouteSink::Message { body, .. } | RouteSink::Channel { body, .. } => body.as_deref(),
        _ => None,
    };
    match body {
        Some(body) => {
            let fields = fields
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            let text = crate::text_template::render(body, &fields)?;
            let payload = crate::storage::firing_snapshot(&text);
            Ok(PreparedText { text, payload })
        }
        None => Ok(PreparedText {
            text: text_of(fields).to_owned(),
            payload: text_payload(fields),
        }),
    }
}

/// Whether a sink's body template names `event`.
fn body_names_event(sink: &RouteSink) -> bool {
    let (RouteSink::Message {
        body: Some(body), ..
    }
    | RouteSink::Channel {
        body: Some(body), ..
    }) = sink
    else {
        return false;
    };
    crate::text_template::tokens(body).iter().any(|token| {
        matches!(token, crate::text_template::Token::Placeholder { name, .. } if *name == super::EVENT_FIELD)
    })
}

/// What a sink would do with one firing, without doing it: the address it
/// resolved, or why it would drop.
async fn sink_preview(
    orch: &Orchestrator,
    delivery: &Delivery<'_>,
    bindings: &Bindings<'_>,
) -> Value {
    let fields = &delivery.fields;
    let node_id = delivery.node_id;
    let prepared = match prepare_sink_text(delivery.sink, fields) {
        Ok(prepared) => prepared,
        Err(error) => {
            let kind = match delivery.sink {
                RouteSink::Channel { .. } => "channel",
                _ => "message",
            };
            return serde_json::json!({"nodeId": node_id, "kind": kind, "dropped": "unbound_template", "error": error.name});
        }
    };
    match delivery.sink {
        RouteSink::Channel {
            destination,
            initiated_by,
            ..
        } => {
            let mut preview = serde_json::json!({"nodeId":node_id,"kind":"channel","initiatedBy":initiated_by,"text":prepared.text,"context":fields.get("context").and_then(Value::as_str).unwrap_or("[Cairn]"),"jobId":fields.get("jobId").cloned()});
            match destination {
                Bindable::Fixed(destination) => {
                    preview["destination"] = serde_json::json!(destination)
                }
                Bindable::Bound(binding) => match bound_conversation(orch, bindings, binding).await
                {
                    Ok(address) => preview["destination"] = Value::String(address.to_string()),
                    Err(drop) => {
                        preview["destination"] = serde_json::json!(drop.sink_ref);
                        preview["dropped"] = Value::String(drop.reason.into());
                        preview["error"] = Value::String(drop.error);
                    }
                },
            }
            preview
        }
        RouteSink::Message { target, .. } => {
            let plans = message_targets(bindings, target);
            let targets: Vec<Value> = plans
                .iter()
                .map(|plan| match plan {
                    TargetPlan::Deliver { canonical, .. } => {
                        serde_json::json!({"address": canonical})
                    }
                    TargetPlan::SinkDrop(drop) => drop.preview(),
                })
                .collect();
            let first = plans.iter().find_map(|plan| match plan {
                TargetPlan::Deliver { canonical, .. } => Some(canonical.clone()),
                TargetPlan::SinkDrop(_) => None,
            });
            serde_json::json!({"nodeId":node_id,"kind":"message","target":first,"targets":targets,"text":prepared.text})
        }
        RouteSink::Label { issue, labels } => match bindings.string(issue) {
            Ok(issue) => {
                serde_json::json!({"nodeId":node_id,"kind":"label","issue":issue,"labels":labels})
            }
            Err(error) => {
                serde_json::json!({"nodeId":node_id,"kind":"label","labels":labels,"error":error})
            }
        },
        RouteSink::Issue { labels, recipe } => {
            serde_json::json!({"nodeId":node_id,"kind":"issue","project":fields.get("project"),"title":fields.get("title").or_else(||fields.get("text")),"description":fields.get("body").or_else(||fields.get("description")),"labels":labels,"recipe":recipe})
        }
    }
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
                None,
                None,
            )
            .await
            .map(|outcome| outcome.text)
            .map_err(|error| error.to_string())
        })
    };
    let decide = production_decide(orch, context.project_id);
    // The sample is the source's latest log row, so the dry run binds that
    // row's URI. A sample with no row binds a marked placeholder instead: a
    // body naming `event` always renders in production, so the preview must
    // not report it unbound.
    let event = context.event.map_or(EVENT_PLACEHOLDER, |event| event.uri);
    let walked = walk(&graph, &fact, presence, &run, &decide, Some(event)).await;
    let matched = graph
        .triggers()
        .any(|index| trigger_matches(graph.node(index), &fact, presence));
    let mut sink_previews = Vec::with_capacity(walked.deliveries.len());
    for delivery in &walked.deliveries {
        let bindings = Bindings {
            graph: &graph,
            fields: &delivery.fields,
            outputs: &walked.outputs,
        };
        let mut preview = sink_preview(orch, delivery, &bindings).await;
        if context.event.is_none() && body_names_event(delivery.sink) {
            preview["note"] = Value::String(format!(
                "{{{{event}}}} rendered as {EVENT_PLACEHOLDER}: this sample has no fact log row; a live firing binds the row's URI"
            ));
        }
        sink_previews.push(preview);
    }
    Ok(RouteTestResult {
        matched,
        nodes: walked.nodes,
        edges: walked.edges,
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
        let filing = crate::mcp::handlers::executions::resolve_filing(
            orch,
            project,
            outcome.number,
            Some(recipe),
            crate::execution::recipe_resolution::ResolutionCreator::route_dispatcher(),
            None,
        );
        let started = match filing.await {
            Ok(filing) => {
                crate::mcp::handlers::executions::start_execution_from_collection(
                    orch,
                    project,
                    outcome.number,
                    filing,
                    None,
                    None,
                    None,
                    // A freshly dispatched issue has no prior attempt to
                    // supersede.
                    crate::execution::recipe::LaunchIntent::NewAttempt,
                    Vec::new(),
                    crate::execution::recipe::QueuePlace::Tail,
                )
                .await
            }
            Err(error) => Err(error),
        };
        started.map_err(|error| {
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
    let canonical = cairn_uri::build_issue_uri(&project, number);
    let db = orch.db.for_project(&project).await;
    let target = canonical.clone();
    let labels = labels.to_vec();
    let (issue_id, project_id) = db
        .write(|conn| {
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
                let project_id = crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT project_id FROM issues WHERE id = ?1",
                    cairn_db::turso::params![resolved.issue_id.as_str()],
                )
                .await?
                .ok_or_else(|| {
                    crate::storage::DbError::Row(format!("issue not found: {target}"))
                })?;
                Ok((resolved.issue_id, project_id))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    // Attaching may mint labels, which are workspace rows.
    orch.notifier.emit_change("labels");
    let emitter = &orch.services.emitter;
    let _ = emitter.emit(
        "db-change",
        crate::notify::issue_labels_db_change("insert", &issue_id, &project_id),
    );
    let _ = emitter.emit(
        "db-change",
        crate::notify::issue_db_change_ids("update", &issue_id, Some(&project_id)),
    );
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
    /// The fact log row this submission came from, so its outcome joins the row.
    #[serde(default)]
    pub fact_log_id: Option<i64>,
    /// The one provider an undirected submission is addressed to, when the
    /// fact's audience is narrower than every provider (a followed thread's
    /// stream reaches only the providers following it).
    #[serde(default)]
    pub provider: Option<String>,
}

/// Run every route in `routes` whose trigger matches one fact log row.
///
/// This is the route lane's body. Route files are never read here: the caller
/// hands in the cached [`RouteSet`](super::index::RouteSet) for the row's
/// scope. Channel submissions are collected into the caller's `submissions`
/// rather than delivered, as each route produces them, so whatever the routes
/// before a failure prepared is still the caller's to hand over -- whether the
/// failure is an error or the caller cancelling this future at its timeout.
pub(crate) async fn dispatch_row(
    orch: &Orchestrator,
    fact: &RouteFact,
    presence: Presence,
    context: RouteContext<'_>,
    routes: &super::index::RouteSet,
    submissions: &mut Vec<ChannelSubmission>,
) -> Result<(), String> {
    if fact.is_route_generated() {
        return Ok(());
    }
    dispatch_into(orch, fact, presence, context, routes, submissions).await
}

async fn dispatch_into(
    orch: &Orchestrator,
    fact: &RouteFact,
    presence: Presence,
    context: RouteContext<'_>,
    routes: &super::index::RouteSet,
    submissions: &mut Vec<ChannelSubmission>,
) -> Result<(), String> {
    let fact_log_id = context.event.map(|event| event.id);
    let event = context.event.map(|event| event.uri);
    let mut firing_count = 0;
    for route in routes.routes_for(&fact.source) {
        let graph = RouteGraph::new(&route.definition)?;
        // Whether the route fires at all is settled before any response node
        // runs: a trigger that does not match must cost nothing.
        if !graph
            .triggers()
            .any(|index| trigger_matches(graph.node(index), fact, presence))
        {
            continue;
        }
        if firing_count == MAX_FIRINGS_PER_FACT {
            break;
        }
        firing_count += 1;
        let scope_key = if route.is_project_scoped() {
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
                        fact,
                        sink: FiringSink::Route(sink),
                        created_at: now,
                        fact_log_id,
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
                #[cfg(test)]
                if response == HANGING_RESPONSE {
                    std::future::pending::<()>().await;
                }
                responses::invoke(
                    &orch,
                    &response,
                    &args,
                    ResponseCaller::Internal { label },
                    None,
                    None,
                )
                .await
                .map(|outcome| outcome.text)
                .map_err(|error| error.to_string())
            })
        };
        let decide = production_decide(orch, context.project_id);
        let walked = walk(&graph, fact, presence, &run, &decide, event).await;

        // A decision whose answers took no edge stops the firing there. It is
        // journaled once, with the answers and what was judged, so a refusal
        // or an abstention is visible rather than a silent nothing.
        for unrouted in &walked.unrouted {
            record(
                orch,
                Firing {
                    route_id: &route.id,
                    scope_key: &scope_key,
                    project_id: context.project_id,
                    fact,
                    sink: FiringSink::Decision(unrouted.node_id),
                    created_at: now,
                    fact_log_id,
                },
                serde_json::to_string(&unrouted.history).ok(),
                Outcome {
                    error: Some(format!(
                        "no edge out of decision '{}' matched its answers",
                        unrouted.node_id
                    )),
                    payload: crate::storage::firing_snapshot(&unrouted.input),
                    ..Outcome::dropped("unrouted")
                },
            )
            .await?;
        }

        // Each sink records its own outcome on its own path's content: one sink
        // failing neither blocks the others nor speaks for them, and the journal
        // keeps a row per delivery.
        for delivery in &walked.deliveries {
            let firing = Firing {
                route_id: &route.id,
                scope_key: &scope_key,
                project_id: context.project_id,
                fact,
                sink: FiringSink::Route(delivery.sink),
                created_at: now,
                fact_log_id,
            };
            let transforms_json = serde_json::to_string(&delivery.transforms).ok();
            let fields = &delivery.fields;
            let bindings = Bindings {
                graph: &graph,
                fields,
                outputs: &walked.outputs,
            };
            let prepared = match prepare_sink_text(delivery.sink, fields) {
                Ok(prepared) => prepared,
                Err(error) => {
                    record(
                        orch,
                        firing,
                        transforms_json,
                        Outcome {
                            error: Some(error.name),
                            ..Outcome::dropped("unbound_template")
                        },
                    )
                    .await?;
                    continue;
                }
            };
            match delivery.sink {
                RouteSink::Channel {
                    destination,
                    initiated_by,
                    ..
                } => {
                    let destination = match destination {
                        Bindable::Fixed(ChannelDestination::Subscriptions) => None,
                        Bindable::Fixed(ChannelDestination::Conversation(address)) => {
                            Some(address.clone())
                        }
                        Bindable::Bound(binding) => {
                            match bound_conversation(orch, &bindings, binding).await {
                                Ok(address) => Some(address),
                                Err(drop) => {
                                    record(
                                        orch,
                                        firing,
                                        transforms_json,
                                        Outcome::sink_drop(drop, prepared.payload.clone()),
                                    )
                                    .await?;
                                    continue;
                                }
                            }
                        }
                    };
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
                        text: prepared.text,
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
                        destination,
                        fact_log_id,
                        provider: None,
                    });
                }
                RouteSink::Message { target, .. } => {
                    // A bound list fans out here, one delivery and one journal
                    // row per address, so a decision asked once can reach
                    // several targets while every other sink still fires once.
                    for plan in message_targets(&bindings, target) {
                        let outcome = match plan {
                            TargetPlan::Deliver { target, canonical } => {
                                let delivered = super::with_provenance(
                                    route.id.clone(),
                                    deliver_message(orch, &route.id, &target, &prepared.text),
                                )
                                .await
                                .map(|()| canonical);
                                Outcome::delivery(delivered, prepared.payload.clone())
                            }
                            TargetPlan::SinkDrop(drop) => {
                                Outcome::sink_drop(drop, prepared.payload.clone())
                            }
                        };
                        record(orch, firing, transforms_json.clone(), outcome).await?;
                    }
                }
                RouteSink::Label { issue, labels } => {
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
    Ok(())
}

pub async fn record_channel_outcome(
    orch: &Orchestrator,
    submission: &ChannelSubmission,
    result: Result<String, String>,
) -> Result<(), String> {
    let sink = submission_sink(submission);
    record(
        orch,
        Firing {
            route_id: &submission.route_id,
            scope_key: &submission.scope_key,
            project_id: submission.project_id.as_deref(),
            fact: &submission.fact,
            sink: FiringSink::Route(&sink),
            created_at: submission.created_at,
            fact_log_id: submission.fact_log_id,
        },
        submission.transforms_json.clone(),
        Outcome::delivery(result, crate::storage::firing_snapshot(&submission.text)),
    )
    .await?;
    Ok(())
}

/// The channel sink a submission was addressed by, as resolved: a bound
/// destination is journaled as the conversation it resolved to.
fn submission_sink(submission: &ChannelSubmission) -> RouteSink {
    RouteSink::Channel {
        destination: Bindable::Fixed(submission.destination.clone().map_or(
            ChannelDestination::Subscriptions,
            ChannelDestination::Conversation,
        )),
        initiated_by: submission.initiated_by.clone(),
        body: None,
    }
}

/// Journal a channel submission that no provider was left to receive.
pub(crate) async fn record_channel_drop(
    orch: &Orchestrator,
    submission: &ChannelSubmission,
    reason: &'static str,
) -> Result<(), String> {
    let sink = submission_sink(submission);
    record(
        orch,
        Firing {
            route_id: &submission.route_id,
            scope_key: &submission.scope_key,
            project_id: submission.project_id.as_deref(),
            fact: &submission.fact,
            sink: FiringSink::Route(&sink),
            created_at: submission.created_at,
            fact_log_id: submission.fact_log_id,
        },
        submission.transforms_json.clone(),
        Outcome::dropped(reason),
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
        FiringSink::Route(RouteSink::Channel { destination, .. }) => (
            "channel",
            destination.fixed().map(|destination| match destination {
                ChannelDestination::Subscriptions => "subscriptions".to_string(),
                ChannelDestination::Conversation(address) => address.to_string(),
            }),
        ),
        FiringSink::Route(RouteSink::Message { target, .. }) => {
            ("message", target.fixed().cloned())
        }
        FiringSink::Route(RouteSink::Issue { .. }) => ("issue", None),
        FiringSink::Route(RouteSink::Label { .. }) => ("label", None),
        // `sink_kind` is free text in the journal, so a decision that routed
        // nowhere names itself without a schema change.
        FiringSink::Decision(node_id) => ("decision", Some(node_id.to_string())),
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
            fact_log_id: firing.fact_log_id,
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

    /// Run the route lane's body directly on a fact, through the same cached
    /// route set the worker uses.
    async fn dispatch(
        orch: &Orchestrator,
        fact: RouteFact,
        presence: Presence,
        context: RouteContext<'_>,
    ) -> Result<Vec<ChannelSubmission>, String> {
        let routes = orch.fact_log.index.routes(
            &orch.config_dir,
            context.project_path,
            orch.services.clock.now(),
        )?;
        let mut submissions = Vec::new();
        dispatch_row(orch, &fact, presence, context, &routes, &mut submissions)
            .await
            .map(|()| submissions)
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
        let walked = walk(&graph, &fact, Presence::Away, run, &never_decides, None).await;
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
            (
                MessageTarget::Stream {
                    project: "cairn".into(),
                    number: Some(42)
                },
                "cairn://p/cairn/42".into()
            )
        );
        assert_eq!(
            resolve_message_target("cairn://p/cairn/devops").unwrap(),
            (
                MessageTarget::Thread {
                    project: "cairn".into(),
                    name: "devops".into()
                },
                "cairn://p/cairn/devops".into()
            )
        );
        assert!(resolve_message_target("cairn://p/cairn/42/messages").is_err());
        assert!(resolve_message_target("cairn://p/cairn/devops/messages").is_err());
    }

    fn never_decides(_: &DecisionNode, _: String) -> DecisionFuture {
        unreachable!("this route asks no decision")
    }

    /// A decision runner that answers every question with `answer`, so the
    /// walk's routing is exercised without a provider.
    fn answering(
        answer: crate::models::decision::DecisionAnswer,
    ) -> impl Fn(&DecisionNode, String) -> DecisionFuture + Sync {
        move |decision: &DecisionNode, _input: String| -> DecisionFuture {
            let run = DecisionRun {
                asked: decision.clone(),
                result: Ok(crate::models::decision::Decision {
                    provider: "fixture".into(),
                    model: "jev".into(),
                    answers: vec![answer.clone()],
                    usage: Default::default(),
                }),
            };
            Box::pin(async move { run })
        }
    }

    fn target_choice(pick: &str, confidence: f64) -> crate::models::decision::DecisionAnswer {
        crate::models::decision::DecisionAnswer::Choice {
            name: "target".into(),
            measured_by: crate::models::decision::MeasuredBy::Native,
            pick: pick.into(),
            confidence: Some(confidence),
            probabilities: vec![],
        }
    }

    fn channel_fact(text: &str) -> RouteFact {
        RouteFact {
            source: "channel_message".into(),
            identity: "inbound:1".into(),
            fields: BTreeMap::from([
                ("project".into(), Value::String("cairn".into())),
                ("provider".into(), Value::String("discord".into())),
                ("conversation".into(), Value::String("discord:1/2".into())),
                ("bindingKind".into(), Value::String("ambient".into())),
                ("addressed".into(), Value::Bool(false)),
                ("target".into(), Value::String("cairn://p/cairn".into())),
                ("text".into(), Value::String(text.into())),
            ]),
            origin: None,
            summary: Some(text.into()),
            route_provenance: None,
        }
    }

    fn no_response() -> impl Fn(&str, Value) -> ResponseFuture + Sync {
        |_: &str, _: Value| -> ResponseFuture { Box::pin(async { Err("no responses".into()) }) }
    }

    #[tokio::test]
    async fn fixture_a_delivers_to_the_admitted_pick_with_the_answers_in_its_fields() {
        let definition = definition(crate::routes::definition::FIXTURE_A);
        let graph = RouteGraph::new(&definition).unwrap();
        let decide = answering(target_choice("cairn://p/cairn/34", 0.8));
        let walked = walk(
            &graph,
            &channel_fact("the build is red"),
            Presence::Away,
            &no_response(),
            &decide,
            None,
        )
        .await;
        assert!(walked.unrouted.is_empty());
        let [delivery] = walked.deliveries.as_slice() else {
            panic!("one delivery: {}", walked.deliveries.len());
        };
        assert_eq!(delivery.fields["answers.target.pick"], "cairn://p/cairn/34");
        assert_eq!(
            text_of(&delivery.fields),
            "the build is red",
            "a decision passes content on"
        );
        assert_eq!(delivery.transforms[0]["decision"], "pick");
        let bindings = Bindings {
            graph: &graph,
            fields: &delivery.fields,
            outputs: &walked.outputs,
        };
        let RouteSink::Message { target, .. } = delivery.sink else {
            panic!("a message sink");
        };
        assert_eq!(
            message_targets(&bindings, target),
            vec![TargetPlan::Deliver {
                target: MessageTarget::Stream {
                    project: "cairn".into(),
                    number: Some(34)
                },
                canonical: "cairn://p/cairn/34".into()
            }]
        );
        let edge = walked
            .edges
            .iter()
            .find(|edge| edge["from"] == "pick")
            .unwrap();
        assert_eq!(edge["taken"], true);
    }

    /// The route-side failure contract: a refusal, a pick below threshold, and
    /// answers no edge accepts all fire nothing and are collected as unrouted
    /// with the answers on the record.
    #[tokio::test]
    async fn a_refusal_a_low_pick_and_an_unmatched_edge_set_route_nowhere() {
        let fixture = definition(crate::routes::definition::FIXTURE_A);
        let narrowed = definition(&crate::routes::definition::FIXTURE_A.replace(
            "when: { answers.target.outcome: admitted, answers.target.confidence: { gte: 0.6 } }",
            "when: { answers.target.pick: 'cairn://p/cairn/12' }",
        ));
        let refusal = crate::models::decision::DecisionAnswer::Refusal {
            name: "target".into(),
            measured_by: crate::models::decision::MeasuredBy::Native,
            reason: Some("policy".into()),
        };
        for (route, answer, outcome) in [
            (&fixture, refusal, "refused"),
            (
                &fixture,
                target_choice("cairn://p/cairn/34", 0.3),
                "below_threshold",
            ),
            (
                &narrowed,
                target_choice("cairn://p/cairn/34", 0.9),
                "admitted",
            ),
        ] {
            let graph = RouteGraph::new(route).unwrap();
            let decide = answering(answer);
            let walked = walk(
                &graph,
                &channel_fact("hello"),
                Presence::Away,
                &no_response(),
                &decide,
                None,
            )
            .await;
            assert!(
                walked.deliveries.is_empty(),
                "{outcome}: nothing is delivered"
            );
            let [unrouted] = walked.unrouted.as_slice() else {
                panic!("{outcome}: one unrouted decision");
            };
            assert_eq!(unrouted.node_id, "pick");
            assert_eq!(unrouted.input, "hello");
            assert_eq!(
                unrouted.history[0]["answers"]["target"]["outcome"], outcome,
                "{:?}",
                unrouted.history
            );
            let edge = walked
                .edges
                .iter()
                .find(|edge| edge["from"] == "pick")
                .unwrap();
            assert_eq!(edge["taken"], false);
            assert!(!edge["failures"].as_array().unwrap().is_empty(), "{edge}");
        }
    }

    #[test]
    fn a_bound_list_fans_out_to_the_cap_and_drops_the_rest_by_name() {
        let definition = definition(crate::routes::definition::FIXTURE_A);
        let graph = RouteGraph::new(&definition).unwrap();
        let addresses: Vec<Value> = (1..=MAX_TARGETS_PER_SINK + 2)
            .map(|number| Value::String(format!("cairn://p/cairn/{number}")))
            .chain([Value::String("not a uri".into())])
            .collect();
        let fields =
            BTreeMap::from([("answers.target.picks".to_string(), Value::Array(addresses))]);
        let outputs = HashMap::new();
        let bindings = Bindings {
            graph: &graph,
            fields: &fields,
            outputs: &outputs,
        };
        let plans = message_targets(
            &bindings,
            &Bindable::Bound(ArgumentBinding::Field {
                field: "answers.target.picks".into(),
            }),
        );
        let reasons: Vec<&str> = plans
            .iter()
            .map(|plan| match plan {
                TargetPlan::Deliver { .. } => "deliver",
                TargetPlan::SinkDrop(drop) => drop.reason,
            })
            .collect();
        assert_eq!(
            reasons.iter().filter(|r| **r == "deliver").count(),
            MAX_TARGETS_PER_SINK
        );
        assert_eq!(reasons.iter().filter(|r| **r == "fan_out_limit").count(), 3);
        let missing = message_targets(
            &bindings,
            &Bindable::Bound(ArgumentBinding::Field {
                field: "answers.target.addresses".into(),
            }),
        );
        assert!(matches!(
            missing.as_slice(),
            [TargetPlan::SinkDrop(SinkDrop {
                reason: "unbound_target",
                ..
            })]
        ));
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
            &never_decides,
            None,
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

        let walked = walk(
            &graph,
            &thread_fact("long"),
            Presence::Away,
            counted,
            &never_decides,
            None,
        )
        .await;
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

        let walked = walk(
            &graph,
            &thread_fact("original"),
            Presence::Away,
            recording,
            &never_decides,
            None,
        )
        .await;
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
        let walked = walk(
            &graph,
            &thread_fact("hello"),
            Presence::Away,
            run,
            &never_decides,
            None,
        )
        .await;
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
                event: None,
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
                event: None,
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
            event: None,
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
            event: None,
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
                event: None,
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

    fn body_route(kind: &str, body: Option<&str>) -> String {
        let sink = match kind {
            "message" => "kind: message, target: 'cairn://p/cairn/42'",
            "channel" => "kind: channel, destination: 'discord:1/2'",
            _ => unreachable!(),
        };
        let body = body
            .map(|body| format!(", body: {}", serde_json::to_string(body).unwrap()))
            .unwrap_or_default();
        format!("name: Body\ndescription: deterministic text\nnodes:\n  - {{ id: input, type: trigger, when: {{ fact: thread_stream }} }}\n  - {{ id: out, type: sink, sink: {{ {sink}{body} }} }}\nedges:\n  - {{ from: input, to: out }}\n")
    }

    #[tokio::test]
    async fn sink_body_templates_live_preview_and_journal_agree() {
        for kind in ["message", "channel"] {
            let temp = tempfile::tempdir().unwrap();
            let route = body_route(kind, Some("Via {{text}}."));
            let (orch, db) = orchestrator(temp.path(), &[("body", &route)]).await;
            db.execute_batch(PROJECT_FIXTURE).await.unwrap();
            let preview = test_definition(
                &orch,
                &definition(&route),
                thread_fact("hello {{literal}}"),
                Presence::Away,
                in_project(),
            )
            .await
            .unwrap();
            assert_eq!(preview.sink_previews[0]["text"], "Via hello {{literal}}.");
            assert_eq!(
                db.query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                    .await
                    .unwrap(),
                Some(0)
            );
            assert_eq!(
                db.query_opt_i64("SELECT COUNT(*) FROM messages", ())
                    .await
                    .unwrap(),
                Some(0)
            );
            let submissions = dispatch(
                &orch,
                thread_fact("hello {{literal}}"),
                Presence::Away,
                in_project(),
            )
            .await
            .unwrap();
            if kind == "channel" {
                assert_eq!(submissions.len(), 1);
                assert_eq!(submissions[0].text, "Via hello {{literal}}.");
                assert_eq!(submissions[0].binding_ref, "route:body:event:1");
                record_channel_outcome(&orch, &submissions[0], Ok("discord:1/2".into()))
                    .await
                    .unwrap();
            } else {
                assert!(submissions.is_empty());
                assert_eq!(
                    issue_messages(&orch, 42).await.as_deref(),
                    Some("route:body:Via hello {{literal}}.")
                );
            }
            let firings = crate::storage::list_route_firings(&db, "workspace", "body", 10)
                .await
                .unwrap();
            assert_eq!(firings.len(), 1);
            assert_eq!(firings[0].status, "fired");
            assert_eq!(
                firings[0].payload_text.as_deref(),
                Some("Via hello {{literal}}.")
            );
        }
    }

    #[tokio::test]
    async fn sink_body_templates_unbound_drop_before_delivery_and_preview_has_no_payload() {
        for kind in ["message", "channel"] {
            for empty_list in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let route = body_route(kind, Some("Before {{text}} after"));
                let (orch, db) = orchestrator(temp.path(), &[("body", &route)]).await;
                db.execute_batch(PROJECT_FIXTURE).await.unwrap();
                let mut fact = thread_fact("upstream must never leak");
                if empty_list {
                    fact.fields.insert("text".into(), serde_json::json!([]));
                } else {
                    fact.fields.remove("text");
                }
                let preview = test_definition(
                    &orch,
                    &definition(&route),
                    fact.clone(),
                    Presence::Away,
                    in_project(),
                )
                .await
                .unwrap();
                assert_eq!(preview.sink_previews[0]["dropped"], "unbound_template");
                assert_eq!(preview.sink_previews[0]["error"], "text");
                assert!(preview.sink_previews[0].get("text").is_none());
                assert_eq!(
                    db.query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                        .await
                        .unwrap(),
                    Some(0)
                );
                let submissions = dispatch(&orch, fact, Presence::Away, in_project())
                    .await
                    .unwrap();
                assert!(submissions.is_empty(), "no external channel delivery");
                assert_eq!(
                    db.query_opt_i64("SELECT COUNT(*) FROM messages", ())
                        .await
                        .unwrap(),
                    Some(0),
                    "no message delivery"
                );
                let firings = crate::storage::list_route_firings(&db, "workspace", "body", 10)
                    .await
                    .unwrap();
                assert_eq!(firings.len(), 1);
                assert_eq!(firings[0].status, "dropped");
                assert_eq!(firings[0].drop_reason.as_deref(), Some("unbound_template"));
                assert_eq!(firings[0].error.as_deref(), Some("text"));
                assert_eq!(firings[0].payload_text, None);
            }
        }
    }

    /// Log `fact` as the worker would find it, in `project_id`, and return the
    /// row's id and the URI dispatch binds as `event`.
    async fn logged(
        orch: &Orchestrator,
        fact: &RouteFact,
        project_id: Option<&str>,
    ) -> (i64, String) {
        let id = crate::storage::insert_fact(
            &orch.db.local,
            crate::storage::NewFact {
                project_id: project_id.map(str::to_owned),
                source: fact.source.clone(),
                identity: fact.identity.clone(),
                record_ref: "cairn://p/cairn/42".into(),
                record_id: None,
                fields_json: serde_json::to_string(&fact.fields).unwrap(),
                summary: fact.summary.clone(),
                presence: "away".into(),
                actor_job_id: None,
                origin_principal_json: None,
                origin_appearance_json: None,
                route_provenance: None,
                minted_at: 1,
            },
            crate::storage::LogDedupe::Unique,
        )
        .await
        .unwrap()
        .expect("a fresh row");
        let row = crate::storage::get_fact(&orch.db.local, id)
            .await
            .unwrap()
            .unwrap();
        (id, crate::routes::event_uri(orch, &row).await)
    }

    fn at_event<'a>(project_id: Option<&'a str>, id: i64, uri: &'a str) -> RouteContext<'a> {
        RouteContext {
            project_id,
            project_path: None,
            event: Some(LoggedEvent { id, uri }),
        }
    }

    #[tokio::test]
    async fn sink_body_templates_name_the_fact_log_row_they_deliver_for() {
        for kind in ["message", "channel"] {
            let temp = tempfile::tempdir().unwrap();
            let route = body_route(kind, Some("Re {{event}}"));
            let (orch, db) = orchestrator(temp.path(), &[("body", &route)]).await;
            db.execute_batch(PROJECT_FIXTURE).await.unwrap();
            let fact = thread_fact("hello");
            let (id, uri) = logged(&orch, &fact, Some("p")).await;
            assert_eq!(uri, format!("cairn://p/cairn/events/{id}"));
            let expected = format!("Re {uri}");

            let submissions = dispatch(&orch, fact, Presence::Away, at_event(Some("p"), id, &uri))
                .await
                .unwrap();
            if kind == "channel" {
                assert_eq!(submissions.len(), 1);
                assert_eq!(submissions[0].text, expected);
                assert_eq!(submissions[0].fact_log_id, Some(id));
                record_channel_outcome(&orch, &submissions[0], Ok("discord:1/2".into()))
                    .await
                    .unwrap();
            } else {
                assert_eq!(
                    issue_messages(&orch, 42).await,
                    Some(format!("route:body:{expected}"))
                );
            }
            let firings = crate::storage::list_firings_for_fact(&db, id)
                .await
                .unwrap();
            let [firing] = firings.as_slice() else {
                panic!("one firing joins the row: {firings:?}");
            };
            assert_eq!(firing.status, "fired");
            assert_eq!(firing.payload_text.as_deref(), Some(expected.as_str()));
        }
    }

    #[tokio::test]
    async fn a_project_less_row_is_named_by_its_workspace_uri() {
        let temp = tempfile::tempdir().unwrap();
        let route = body_route("message", Some("Re {{event}}"));
        let (orch, db) = orchestrator(temp.path(), &[("body", &route)]).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        let fact = thread_fact("hello");
        let (id, uri) = logged(&orch, &fact, None).await;
        assert_eq!(uri, format!("cairn://routes/events/{id}"));
        dispatch(&orch, fact, Presence::Away, at_event(None, id, &uri))
            .await
            .unwrap();
        assert_eq!(
            issue_messages(&orch, 42).await,
            Some(format!("route:body:Re {uri}"))
        );
    }

    #[tokio::test]
    async fn the_dry_run_binds_the_sample_row_or_a_marked_placeholder() {
        let temp = tempfile::tempdir().unwrap();
        let route = body_route("message", Some("Re {{event}}"));
        let (orch, db) = orchestrator(temp.path(), &[]).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        let fact = thread_fact("hello");
        let (id, uri) = logged(&orch, &fact, Some("p")).await;

        let sampled = test_definition(
            &orch,
            &definition(&route),
            fact.clone(),
            Presence::Away,
            at_event(Some("p"), id, &uri),
        )
        .await
        .unwrap();
        let preview = &sampled.sink_previews[0];
        assert_eq!(preview["text"], format!("Re {uri}"));
        assert!(preview.get("note").is_none(), "{preview}");

        let rowless = test_definition(
            &orch,
            &definition(&route),
            fact,
            Presence::Away,
            in_project(),
        )
        .await
        .unwrap();
        let preview = &rowless.sink_previews[0];
        assert!(preview.get("dropped").is_none(), "{preview}");
        assert_eq!(preview["text"], format!("Re {EVENT_PLACEHOLDER}"));
        assert!(
            preview["note"]
                .as_str()
                .is_some_and(|note| note.contains("no fact log row")),
            "{preview}"
        );
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn sink_body_templates_rendered_empty_keeps_legacy_empty_content_path() {
        for kind in ["message", "channel"] {
            let temp = tempfile::tempdir().unwrap();
            let upstream = body_route(kind, None);
            let template = body_route(kind, Some("{{text}}"));
            let (orch, db) = orchestrator(
                temp.path(),
                &[("upstream", &upstream), ("template", &template)],
            )
            .await;
            db.execute_batch(PROJECT_FIXTURE).await.unwrap();
            let submissions = dispatch(&orch, thread_fact(""), Presence::Away, in_project())
                .await
                .unwrap();
            if kind == "channel" {
                assert_eq!(submissions.len(), 2);
                for submission in &submissions {
                    assert_eq!(submission.text, "");
                    record_channel_outcome(&orch, submission, Ok("discord:1/2".into()))
                        .await
                        .unwrap();
                }
            }
            let legacy = crate::storage::list_route_firings(&db, "workspace", "upstream", 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let rendered = crate::storage::list_route_firings(&db, "workspace", "template", 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(rendered.status, legacy.status);
            assert_eq!(rendered.drop_reason, legacy.drop_reason);
            assert_eq!(rendered.error, legacy.error);
            assert_eq!(rendered.payload_text, legacy.payload_text);
            assert_ne!(rendered.drop_reason.as_deref(), Some("unbound_template"));
        }
    }

    #[tokio::test]
    async fn sink_body_templates_render_transformed_text_and_projected_provenance() {
        let temp = tempfile::tempdir().unwrap();
        let (orch, _) = orchestrator(temp.path(), &[]).await;
        let template = "Routed to {{answers.thread.citations}}.\nVia {{conversationLabel}} from {{senderLabel}}. If this isn't yours, say so.\n{{text}}";
        let route = body_route("message", Some(template));
        // Exercise the delivery's existing flat fields, including producer text,
        // instead of looking answers up again from the graph.
        let definition = definition(&body_route("message", None));
        let graph = RouteGraph::new(&definition).unwrap();
        let sink: RouteSink = serde_json::from_value(
            serde_json::json!({"kind":"message","target":"cairn://p/cairn/42","body":template}),
        )
        .unwrap();
        let fields = BTreeMap::from([
            (
                "text".into(),
                serde_json::json!("transformed producer text"),
            ),
            (
                "conversationLabel".into(),
                serde_json::json!("#ambient · cairn · Discord"),
            ),
            ("senderLabel".into(), serde_json::json!("operator")),
            (
                "answers.thread.citations".into(),
                serde_json::json!([
                    "devops (0.82, native, rev 1a2b3c4d5e6f)",
                    "execution (0.61, native, rev 9f8e7d6c5b4a)"
                ]),
            ),
        ]);
        let outputs = HashMap::new();
        let bindings = Bindings {
            graph: &graph,
            fields: &fields,
            outputs: &outputs,
        };
        let delivery = Delivery {
            node_id: "out",
            sink: &sink,
            fields: fields.clone(),
            transforms: Vec::new(),
            position: 0,
        };
        let preview = sink_preview(&orch, &delivery, &bindings).await;
        assert_eq!(preview["text"], "Routed to devops (0.82, native, rev 1a2b3c4d5e6f), execution (0.61, native, rev 9f8e7d6c5b4a).\nVia #ambient · cairn · Discord from operator. If this isn't yours, say so.\ntransformed producer text");
        assert!(route.contains("body:"));
    }

    #[test]
    fn sink_body_templates_render_actual_decision_presentations() {
        use crate::models::decision::{
            Decision, DecisionAnswer, DecisionOption, DecisionQuestion, MeasuredBy,
            OptionProbability, QuestionKind, Thresholds,
        };
        use crate::orchestrator::DecisionFailure;
        let mut question = DecisionQuestion::choice(
            "thread",
            "Which thread?",
            vec![
                DecisionOption::new("execution", None),
                DecisionOption::new("devops", None),
            ],
        );
        if let QuestionKind::Choice { options, top_k, .. } = &mut question.kind {
            *top_k = Some(2);
            options[0].revision = Some("9f8e7d6c5b4a".into());
            options[1].revision = Some("1a2b3c4d5e6f".into());
        }
        let asked = DecisionNode {
            questions: vec![question],
            models: None,
            thresholds: Thresholds {
                native: Some(0.5),
                logprobs: None,
            },
        };
        let answered = |answer| {
            Ok(Decision {
                provider: "custom:x".into(),
                model: "jev".into(),
                answers: vec![answer],
                usage: Default::default(),
            })
        };
        let choice = |confidence, probability| {
            answered(DecisionAnswer::Choice {
                name: "thread".into(),
                measured_by: MeasuredBy::Native,
                pick: "devops".into(),
                confidence: Some(confidence),
                probabilities: vec![
                    OptionProbability {
                        value: "execution".into(),
                        probability: 0.61,
                    },
                    OptionProbability {
                        value: "devops".into(),
                        probability,
                    },
                ],
            })
        };
        let render = |template: &str, result: &Result<Decision, DecisionFailure>, native| {
            let mut asked = asked.clone();
            asked.thresholds.native = Some(native);
            let fields = decisions::answer_fields(&decisions::outcomes(&asked, result));
            let sink = RouteSink::Message {
                target: Bindable::Fixed("cairn://p/cairn/42".into()),
                body: Some(template.into()),
            };
            prepare_sink_text(&sink, &fields).map(|text| text.text)
        };
        assert_eq!(render("Routed to {{answers.thread.citations}}.", &choice(0.82, 0.82), 0.2).unwrap(),
            "Routed to devops (0.82, native, rev 1a2b3c4d5e6f), execution (0.61, native, rev 9f8e7d6c5b4a).");
        let mut low = choice(0.41, 0.41);
        if let Ok(result) = &mut low {
            if let DecisionAnswer::Choice { probabilities, .. } = &mut result.answers[0] {
                probabilities[0].probability = 0.2;
            }
        }
        assert_eq!(render("No thread claimed this (closest: {{answers.thread.citation}}). Start with @thread or #channel to route it.", &low, 0.5).unwrap(),
            "No thread claimed this (closest: devops (0.41, native, rev 1a2b3c4d5e6f)). Start with @thread or #channel to route it.");
        assert_eq!(
            render("Routed to {{answers.thread.citations}}.", &low, 0.5)
                .unwrap_err()
                .name,
            "answers.thread.citations"
        );
        for (result, reason) in [
            (
                answered(DecisionAnswer::Refusal {
                    name: "thread".into(),
                    measured_by: MeasuredBy::Native,
                    reason: Some(" policy text ".into()),
                }),
                " policy text ",
            ),
            (
                answered(DecisionAnswer::Refusal {
                    name: "thread".into(),
                    measured_by: MeasuredBy::Native,
                    reason: None,
                }),
                "the model refused to answer 'thread'",
            ),
            (
                Err(DecisionFailure::Unanswerable("no suitable offer".into())),
                "no suitable offer",
            ),
            (
                Err(DecisionFailure::Failed("provider failed".into())),
                "provider failed",
            ),
        ] {
            assert_eq!(render("Routing is unavailable: {{answers.thread.reason}}. Start with @thread or #channel to route it.", &result, 0.5).unwrap(),
                format!("Routing is unavailable: {reason}. Start with @thread or #channel to route it."));
        }
    }

    #[tokio::test]
    async fn sink_body_templates_use_response_output_instead_of_original_fact_text() {
        let route = body_route("message", Some("Via {{text}}."))
            .replace(
                "  - { id: out,",
                "  - { id: transform, type: response, response: conveyor }\n  - { id: out,",
            )
            .replace(
                "  - { from: input, to: out }",
                "  - { from: input, to: transform }\n  - { from: transform, to: out }",
            );
        let definition = definition(&route);
        let graph = RouteGraph::new(&definition).unwrap();
        let fact = thread_fact("original text");
        let run: RunResponse = &|_, _| -> ResponseFuture {
            Box::pin(async { Ok("transformed producer text".into()) })
        };
        let walked = walk(&graph, &fact, Presence::Away, run, &never_decides, None).await;
        assert_eq!(walked.deliveries.len(), 1);
        let delivery = &walked.deliveries[0];
        assert_eq!(
            prepare_sink_text(delivery.sink, &delivery.fields)
                .unwrap()
                .text,
            "Via transformed producer text."
        );
    }

    #[tokio::test]
    async fn sink_body_templates_keep_prepared_payload_on_address_drops() {
        for kind in ["message", "channel"] {
            let temp = tempfile::tempdir().unwrap();
            let route = body_route(kind, Some("Prepared {{text}}"))
                .replace(
                    "target: 'cairn://p/cairn/42'",
                    "target: { field: threadUri }",
                )
                .replace(
                    "destination: 'discord:1/2'",
                    "destination: { field: conversation }",
                );
            let route = if kind == "channel" {
                route.replace("fact: thread_stream", "fact: channel_message")
            } else {
                route
            };
            let (orch, db) = orchestrator(temp.path(), &[("body", &route)]).await;
            db.execute_batch(PROJECT_FIXTURE).await.unwrap();
            let mut fact = thread_fact("content");
            if kind == "channel" {
                fact.source = "channel_message".into();
            }
            let previews = test_definition(
                &orch,
                &definition(&route),
                fact.clone(),
                Presence::Away,
                in_project(),
            )
            .await
            .unwrap()
            .sink_previews;
            assert_eq!(previews[0]["text"], "Prepared content");
            let submissions = dispatch(&orch, fact, Presence::Away, in_project())
                .await
                .unwrap();
            assert!(submissions.is_empty());
            let firings = crate::storage::list_route_firings(&db, "workspace", "body", 10)
                .await
                .unwrap();
            assert_eq!(firings.len(), 1);
            assert_eq!(firings[0].status, "dropped");
            assert_eq!(firings[0].drop_reason.as_deref(), Some("unbound_target"));
            assert_eq!(firings[0].payload_text.as_deref(), Some("Prepared content"));
        }
    }

    #[tokio::test]
    async fn sink_body_templates_message_fanout_delivers_one_prepared_body_per_target() {
        let temp = tempfile::tempdir().unwrap();
        let route = body_route("message", Some("Prepared {{text}}")).replace(
            "target: 'cairn://p/cairn/42'",
            "target: { field: threadUri }",
        );
        let (orch, db) = orchestrator(temp.path(), &[("body", &route)]).await;
        db.execute_batch(PROJECT_FIXTURE).await.unwrap();
        db.execute_batch("INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at) VALUES('i43', 'p', 43, 'Second', 'active', 1, 1);").await.unwrap();
        let mut fact = thread_fact("{{text}}");
        fact.fields.insert(
            "threadUri".into(),
            serde_json::json!(["cairn://p/cairn/42", "cairn://p/cairn/43"]),
        );
        dispatch(&orch, fact, Presence::Away, in_project())
            .await
            .unwrap();
        for number in [42, 43] {
            assert_eq!(
                issue_messages(&orch, number).await.as_deref(),
                Some("route:body:Prepared {{text}}")
            );
        }
        let firings = crate::storage::list_route_firings(&db, "workspace", "body", 10)
            .await
            .unwrap();
        assert_eq!(firings.len(), 2);
        assert!(
            firings
                .iter()
                .all(|f| f.status == "fired"
                    && f.payload_text.as_deref() == Some("Prepared {{text}}"))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_body_templates_sourced_probability_absent_drops_both_sink_kinds() {
        let route = crate::routes::definition::FIXTURE_B
            .replace("kind: message, target:", "kind: message, body: 'Probability {{answers.thread.probability.devops}}', target:")
            .replace("kind: channel, destination:", "kind: channel, body: 'Probability {{answers.thread.probability.devops}}', destination:");
        let (orch, server) = deciding_workspace(
            vec![choice_reply("thread", "docs", &[("docs", 1.0)])],
            &[("body", &route)],
        )
        .await;
        orch.db.local.execute_batch("INSERT INTO threads(id, project_id, name, jurisdiction, status, attention, created_at, updated_at) VALUES
            ('t-devops', 'p', 'devops', 'Builds.', 'active', 'none', 1, 1),
            ('t-docs', 'p', 'docs', 'Documentation.', 'active', 'none', 1, 1);").await.unwrap();
        let submissions = dispatch(&orch, channel_fact("content"), Presence::Away, in_project())
            .await
            .unwrap();
        assert_eq!(server.calls(), 1);
        assert!(submissions.is_empty());
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM messages", ())
                .await
                .unwrap(),
            Some(0)
        );
        let firings = crate::storage::list_route_firings(&orch.db.local, "workspace", "body", 10)
            .await
            .unwrap();
        assert_eq!(firings.len(), 2);
        assert!(firings.iter().any(|f| f.sink_kind == "message"));
        assert!(firings.iter().any(|f| f.sink_kind == "channel"));
        for firing in firings {
            assert_eq!(firing.status, "dropped");
            assert_eq!(firing.drop_reason.as_deref(), Some("unbound_template"));
            assert_eq!(
                firing.error.as_deref(),
                Some("answers.thread.probability.devops")
            );
            assert_eq!(firing.payload_text, None);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_body_templates_below_threshold_journals_unbound_citations_for_both_kinds() {
        let route = crate::routes::definition::FIXTURE_B
            .replace(
                "    input: { field: text }",
                "    input: { field: text }\n    thresholds: { native: 0.6 }",
            )
            .replace(
                "kind: message, target:",
                "kind: message, body: 'Routed to {{answers.thread.citations}}.', target:",
            )
            .replace(
                "kind: channel, destination:",
                "kind: channel, body: 'Routed to {{answers.thread.citations}}.', destination:",
            )
            .replace(
                "from: pick, to: deliver, when: { answers.thread.outcome: admitted }",
                "from: pick, to: deliver, when: { answers.thread.outcome: below_threshold }",
            );
        let (orch, server) = deciding_workspace(
            vec![choice_reply(
                "thread",
                "devops",
                &[("devops", 0.41), ("docs", 0.2)],
            )],
            &[("body", &route)],
        )
        .await;
        orch.db.local.execute_batch("INSERT INTO threads(id, project_id, name, jurisdiction, status, attention, created_at, updated_at) VALUES
            ('t-devops', 'p', 'devops', 'Builds.', 'active', 'none', 1, 1),
            ('t-docs', 'p', 'docs', 'Documentation.', 'active', 'none', 1, 1);").await.unwrap();
        let submissions = dispatch(&orch, channel_fact("content"), Presence::Away, in_project())
            .await
            .unwrap();
        assert_eq!(server.calls(), 1);
        assert!(submissions.is_empty());
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM messages", ())
                .await
                .unwrap(),
            Some(0)
        );
        let firings = crate::storage::list_route_firings(&orch.db.local, "workspace", "body", 10)
            .await
            .unwrap();
        assert_eq!(firings.len(), 2);
        assert!(firings.iter().any(|f| f.sink_kind == "message"));
        assert!(firings.iter().any(|f| f.sink_kind == "channel"));
        for firing in firings {
            assert_eq!(firing.status, "dropped");
            assert_eq!(firing.drop_reason.as_deref(), Some("unbound_template"));
            assert_eq!(firing.error.as_deref(), Some("answers.thread.citations"));
            assert_eq!(firing.payload_text, None);
        }
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
                event: None,
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

    /* ------------------------------------------- decisions, end to end */

    /// A workspace whose one decision offer is a native Decisions server
    /// answering from `replies`, with the project, issues 12, 34 and 42, and
    /// `routes` installed. Selection, the call, and admission all run for real.
    async fn deciding_workspace(
        replies: Vec<Value>,
        routes: &[(&str, &str)],
    ) -> (
        Orchestrator,
        Arc<crate::orchestrator::decide_fixtures::Scripted>,
    ) {
        let (orch, scripted, _) = crate::orchestrator::decide_fixtures::workspace_with_server(
            crate::credentials::CustomProtocol::Decisions,
            Some("gateway-key"),
            "typesafe-ai/jev",
            replies,
        )
        .await;
        std::fs::create_dir_all(orch.config_dir.join("routes")).unwrap();
        for (name, body) in routes {
            std::fs::write(orch.config_dir.join(format!("routes/{name}.yaml")), body).unwrap();
        }
        orch.db.local.execute_batch(PROJECT_FIXTURE).await.unwrap();
        orch.db
            .local
            .execute_batch(
                "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
                   VALUES('i12', 'p', 12, 'Twelve', 'active', 1, 1), ('i34', 'p', 34, 'Thirty-four', 'active', 1, 1);",
            )
            .await
            .unwrap();
        (orch, scripted)
    }

    fn choice_reply(name: &str, choice: &str, probabilities: &[(&str, f64)]) -> Value {
        serde_json::json!({
            "model": "typesafe-ai/jev",
            "answers": [{
                "type": "choice", "name": name, "choice": choice,
                "probabilities": probabilities.iter().map(|(value, probability)| serde_json::json!({"value": value, "probability": probability})).collect::<Vec<_>>()
            }],
            "usage": {"input_tokens": 30, "output_tokens": 0}
        })
    }

    fn in_project() -> RouteContext<'static> {
        RouteContext {
            project_id: Some("p"),
            project_path: None,
            event: None,
        }
    }

    async fn issue_messages(orch: &Orchestrator, number: i32) -> Option<String> {
        orch.db
            .local
            .query_opt_text(
                "SELECT sender_name || ':' || content FROM messages WHERE channel_type = 'issue' AND channel_id = ?1",
                (format!("cairn/{number}"),),
            )
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fixture_a_fires_to_the_picked_issue_through_the_real_decide() {
        let (orch, server) = deciding_workspace(
            vec![choice_reply(
                "target",
                "cairn://p/cairn/34",
                &[("cairn://p/cairn/12", 0.05), ("cairn://p/cairn/34", 0.95)],
            )],
            &[("pick-issue", crate::routes::definition::FIXTURE_A)],
        )
        .await;
        dispatch(
            &orch,
            channel_fact("the build is red"),
            Presence::Away,
            in_project(),
        )
        .await
        .unwrap();
        assert_eq!(server.calls(), 1, "one decision, asked once");
        assert_eq!(
            issue_messages(&orch, 34).await.as_deref(),
            Some("route:pick-issue:the build is red")
        );
        assert_eq!(issue_messages(&orch, 12).await, None);
        let firing =
            crate::storage::list_route_firings(&orch.db.local, "workspace", "pick-issue", 5)
                .await
                .unwrap();
        let [firing] = firing.as_slice() else {
            panic!("one firing: {firing:?}");
        };
        assert_eq!(firing.status, "fired");
        assert_eq!(firing.sink_ref.as_deref(), Some("cairn://p/cairn/34"));
        assert!(firing
            .transforms_json
            .as_deref()
            .unwrap()
            .contains("\"outcome\":\"admitted\""));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_decision_delivers_nothing_and_journals_one_unrouted_row() {
        let (orch, _) = deciding_workspace(
            vec![serde_json::json!({
                "model": "typesafe-ai/jev",
                "answers": [{"type": "refusal", "name": "target"}],
                "usage": {"input_tokens": 30, "output_tokens": 0}
            })],
            &[("pick-issue", crate::routes::definition::FIXTURE_A)],
        )
        .await;
        dispatch(&orch, channel_fact("hello"), Presence::Away, in_project())
            .await
            .unwrap();
        assert_eq!(issue_messages(&orch, 34).await, None);
        assert_eq!(issue_messages(&orch, 12).await, None);
        let firings =
            crate::storage::list_route_firings(&orch.db.local, "workspace", "pick-issue", 5)
                .await
                .unwrap();
        let [row] = firings.as_slice() else {
            panic!("one row: {firings:?}");
        };
        assert_eq!(
            (
                row.status.as_str(),
                row.drop_reason.as_deref(),
                row.sink_kind.as_str(),
                row.sink_ref.as_deref()
            ),
            ("dropped", Some("unrouted"), "decision", Some("pick"))
        );
        assert_eq!(row.payload_text.as_deref(), Some("hello"));
        assert!(row
            .transforms_json
            .as_deref()
            .unwrap()
            .contains("\"outcome\":\"refused\""));
    }

    /// Fixture B (4896's shape): two chartered threads admitted by top-k, so
    /// the deliver sink fans out to both thread URIs while the bound
    /// acknowledgement goes to its conversation exactly once.
    #[tokio::test(flavor = "multi_thread")]
    async fn fixture_b_fans_out_to_each_admitted_thread_and_acknowledges_once() {
        let (orch, server) = deciding_workspace(
            vec![choice_reply(
                "thread",
                "devops",
                &[("devops", 0.5), ("docs", 0.4), ("ui", 0.1)],
            )],
            &[("ambient", crate::routes::definition::FIXTURE_B)],
        )
        .await;
        orch.db
            .local
            .execute_batch(
                "INSERT INTO threads(id, project_id, name, jurisdiction, status, attention, created_at, updated_at) VALUES
                   ('t-devops', 'p', 'devops', 'How the project builds and ships.', 'active', 'none', 1, 1),
                   ('t-docs', 'p', 'docs', 'The documentation.', 'active', 'none', 1, 1),
                   ('t-ui', 'p', 'ui', 'The interface.', 'active', 'none', 1, 1),
                   ('t-closed', 'p', 'old', 'Retired.', 'closed', 'none', 1, 1);",
            )
            .await
            .unwrap();
        // Merged into the settings the provider was saved in, not written
        // over them, so the decision offer survives.
        let mut settings = crate::config::settings::load_settings(&orch.config_dir);
        settings.channels.discord.enabled = true;
        crate::config::settings::save_settings(&orch.config_dir, &settings).unwrap();
        assert!(crate::channels::ledger::bind_target(
            &orch.db.local,
            "discord",
            "discord:1/2",
            "cairn://p/cairn",
            crate::channels::bindings::BindingKind::Ambient,
            crate::channels::bindings::MESSAGE_CLASS_AGENT_TEXT,
            1,
            0,
        )
        .await
        .unwrap());

        let submissions = dispatch(
            &orch,
            channel_fact("who owns the release?"),
            Presence::Away,
            in_project(),
        )
        .await
        .unwrap();
        assert_eq!(server.calls(), 1);
        let destinations: Vec<String> = submissions
            .iter()
            .map(|submission| {
                submission
                    .destination
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(
            destinations,
            ["discord:1/2"],
            "exactly one acknowledgement, to the bound conversation"
        );
        let firings =
            crate::storage::list_route_firings(&orch.db.local, "workspace", "ambient", 20)
                .await
                .unwrap();
        let mut delivered: Vec<(&str, &str)> = firings
            .iter()
            .filter(|firing| firing.sink_kind == "message")
            .map(|firing| {
                (
                    firing.status.as_str(),
                    firing.sink_ref.as_deref().unwrap_or_default(),
                )
            })
            .collect();
        delivered.sort();
        assert_eq!(
            delivered,
            [
                ("fired", "cairn://p/cairn/devops"),
                ("fired", "cairn://p/cairn/docs")
            ],
            "{firings:?}"
        );
        for firing in firings
            .iter()
            .filter(|firing| firing.sink_kind == "message")
        {
            let record = firing.transforms_json.as_deref().unwrap();
            assert!(
                record.contains(&format!(
                    "\"revision\":\"{}\"",
                    crate::routes::decision::revision("How the project builds and ships.")
                )),
                "each delivery journals the options' revisions: {record}"
            );
        }
        let thread_messages = orch
            .db
            .local
            .query_opt_i64(
                "SELECT COUNT(*) FROM messages WHERE channel_type = 'thread' AND sender_name = 'route:ambient'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(thread_messages, Some(2));
    }

    /// A top-k fan-out delivers one body to each admitted thread, and every
    /// copy names the same row: the event is the correlation key, not the
    /// per-address firing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fan_out_carries_one_event_to_every_address() {
        let route = crate::routes::definition::FIXTURE_B.replace(
            "target: { field: answers.thread.addresses } }",
            "target: { field: answers.thread.addresses }, body: 'Re {{event}}' }",
        );
        assert_ne!(route, crate::routes::definition::FIXTURE_B);
        let (orch, _) = deciding_workspace(
            vec![choice_reply(
                "thread",
                "devops",
                &[("devops", 0.5), ("docs", 0.4), ("ui", 0.1)],
            )],
            &[("ambient", &route)],
        )
        .await;
        orch.db
            .local
            .execute_batch(
                "INSERT INTO threads(id, project_id, name, jurisdiction, status, attention, created_at, updated_at) VALUES
                   ('t-devops', 'p', 'devops', 'How the project builds and ships.', 'active', 'none', 1, 1),
                   ('t-docs', 'p', 'docs', 'The documentation.', 'active', 'none', 1, 1),
                   ('t-ui', 'p', 'ui', 'The interface.', 'active', 'none', 1, 1);",
            )
            .await
            .unwrap();
        let fact = channel_fact("who owns the release?");
        let (id, uri) = logged(&orch, &fact, Some("p")).await;

        dispatch(&orch, fact, Presence::Away, at_event(Some("p"), id, &uri))
            .await
            .unwrap();

        let delivered = orch
            .db
            .local
            .query_all(
                "SELECT content FROM messages WHERE channel_type = 'thread' AND sender_name = 'route:ambient'",
                (),
                |row| crate::storage::RowExt::text(row, 0),
            )
            .await
            .unwrap();
        assert_eq!(delivered, vec![format!("Re {uri}"); 2]);
        let firings: Vec<_> = crate::storage::list_firings_for_fact(&orch.db.local, id)
            .await
            .unwrap()
            .into_iter()
            .filter(|firing| firing.sink_kind == "message")
            .collect();
        let mut addresses: Vec<_> = firings
            .iter()
            .map(|firing| {
                assert_eq!(firing.status, "fired");
                assert_eq!(
                    firing.payload_text.as_deref(),
                    Some(format!("Re {uri}").as_str())
                );
                firing.sink_ref.clone().unwrap_or_default()
            })
            .collect();
        addresses.sort();
        assert_eq!(
            addresses,
            ["cairn://p/cairn/devops", "cairn://p/cairn/docs"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_bound_conversation_that_is_not_configured_is_dropped_as_undeliverable() {
        let route = "name: ack\ndescription: acknowledge in the conversation\nnodes:\n  - { id: inbound, type: trigger, when: { fact: channel_message } }\n  - { id: ack, type: sink, sink: { kind: channel, destination: { field: conversation } } }\nedges:\n  - { from: inbound, to: ack }\n";
        let (orch, _) = deciding_workspace(vec![], &[("ack", route)]).await;
        let submissions = dispatch(&orch, channel_fact("hi"), Presence::Away, in_project())
            .await
            .unwrap();
        assert!(submissions.is_empty());
        let firings = crate::storage::list_route_firings(&orch.db.local, "workspace", "ack", 5)
            .await
            .unwrap();
        let [row] = firings.as_slice() else {
            panic!("one row: {firings:?}");
        };
        assert_eq!(
            (
                row.status.as_str(),
                row.drop_reason.as_deref(),
                row.sink_ref.as_deref()
            ),
            ("dropped", Some("undeliverable"), Some("discord:1/2"))
        );
        assert!(row
            .error
            .as_deref()
            .unwrap()
            .contains("not a configured conversation"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_dry_run_asks_the_decision_but_delivers_and_journals_nothing() {
        let (orch, server) = deciding_workspace(
            vec![choice_reply(
                "target",
                "cairn://p/cairn/34",
                &[("cairn://p/cairn/12", 0.05), ("cairn://p/cairn/34", 0.95)],
            )],
            &[],
        )
        .await;
        let draft = definition(crate::routes::definition::FIXTURE_A);
        let result = test_definition(
            &orch,
            &draft,
            channel_fact("preview"),
            Presence::Away,
            in_project(),
        )
        .await
        .unwrap();
        assert_eq!(server.calls(), 1);
        assert_eq!(result.sink_previews[0]["target"], "cairn://p/cairn/34");
        let decision_edge = result
            .edges
            .iter()
            .find(|edge| edge["from"] == "pick")
            .unwrap();
        assert_eq!(decision_edge["taken"], true);
        let decision = result
            .nodes
            .iter()
            .find(|node| node["nodeId"] == "pick")
            .unwrap();
        assert_eq!(decision["answers"]["target"]["outcome"], "admitted");
        assert_eq!(issue_messages(&orch, 34).await, None);
        assert_eq!(
            orch.db
                .local
                .query_opt_i64("SELECT COUNT(*) FROM route_firings", ())
                .await
                .unwrap(),
            Some(0)
        );
    }
}
