//! A route definition: a node+edge DAG from runtime facts to sinks.
//!
//! Routes share the conceptual node vocabulary recipes use — trigger, response,
//! sink — but they are a strictly loop-free, stateless, depth-one lifecycle, so
//! the config types here are siblings of the recipe ones rather than the same
//! types. Recipe-only config (checkpoint and artifact slots, branch and
//! execution settings) would make states no route can execute representable in
//! a route file, and validation would have to forbid them field by field.

use super::decision::{answer_field, check_clause_value, split_answer_key, AnswerField};
use super::graph::RouteGraph;
use super::predicate::{
    clause_value_problem, AddressKind, FactRegistry, TriggerClause, ANSWERS_PREFIX,
};
use crate::models::decision::{DecisionNode, DecisionQuestion, Thresholds};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteDefinition {
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub nodes: Vec<RouteNode>,
    pub edges: Vec<RouteEdge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe: Option<DedupeWindow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelDestination {
    Subscriptions,
    Conversation(crate::channels::ConversationAddress),
}

impl Serialize for ChannelDestination {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Subscriptions => serializer.serialize_str("subscriptions"),
            Self::Conversation(address) => address.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ChannelDestination {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "subscriptions" | "notify" => Ok(Self::Subscriptions),
            _ => value
                .parse()
                .map(Self::Conversation)
                .map_err(serde::de::Error::custom),
        }
    }
}

/// Where a node sits on the authoring canvas. Absent in a hand-written file:
/// the editor derives a layout from the graph's shape and writes positions back
/// on the next save, so hand-authored and canvas-authored routes are one object
/// rather than two formats.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NodePosition {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone)]
pub struct RouteNode {
    pub id: String,
    /// The operator's label for this node. Empty means "call it what it is",
    /// which is what a hand-written file usually wants.
    pub name: String,
    pub position: Option<NodePosition>,
    pub config: RouteNodeConfig,
}

#[derive(Debug, Clone)]
pub enum RouteNodeConfig {
    Trigger {
        when: TriggerClause,
    },
    Response {
        response: String,
        args: BTreeMap<String, ArgumentBinding>,
    },
    /// Ask typed questions about the content and make the answers known to
    /// everything below as `answers.<question>.*`. A decision selects and
    /// generates nothing: the content that reached it passes on unchanged,
    /// and its outgoing edges choose where by their `when` clauses.
    Decision {
        decision: DecisionNode,
        /// The text the questions judge. Unset reads the content arriving on
        /// the decision's path.
        input: Option<ArgumentBinding>,
    },
    Sink {
        sink: RouteSink,
    },
}

impl RouteNodeConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Trigger { .. } => "trigger",
            Self::Response { .. } => "response",
            Self::Decision { .. } => "decision",
            Self::Sink { .. } => "sink",
        }
    }

    pub fn is_trigger(&self) -> bool {
        matches!(self, Self::Trigger { .. })
    }

    pub fn is_decision(&self) -> bool {
        matches!(self, Self::Decision { .. })
    }

    pub fn is_sink(&self) -> bool {
        matches!(self, Self::Sink { .. })
    }

    /// Whether this node makes content of its own, as opposed to passing the
    /// fact through unchanged.
    pub fn produces_content(&self) -> bool {
        matches!(self, Self::Response { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteEdge {
    pub from: String,
    pub to: String,
    /// When a fact that reached `from` goes on to `to`: clauses in the
    /// trigger grammar, OR'd, over the fact's fields, the content (`text`),
    /// and the `answers.*` of every decision above. Empty is unconditional.
    /// One clause reads and writes as a bare map, several as a list.
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "edge_when")]
    pub when: Vec<TriggerClause>,
}

impl RouteEdge {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            when: Vec::new(),
        }
    }
}

mod edge_when {
    use super::TriggerClause;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        when: &[TriggerClause],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match when {
            [one] => one.serialize(serializer),
            many => many.serialize(serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<TriggerClause>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            One(TriggerClause),
            Many(Vec<TriggerClause>),
        }
        match Raw::deserialize(deserializer) {
            Ok(Raw::One(clause)) => Ok(vec![clause]),
            Ok(Raw::Many(clauses)) => Ok(clauses),
            Err(_) => Err(serde::de::Error::custom(
                "an edge's when is one clause map or a list of clause maps",
            )),
        }
    }
}

/// A sink address: a literal, written exactly as before, or bound like a
/// response argument to a fact field or an upstream node's output and
/// resolved per firing.
///
/// A literal serializes as the bare value, so a route that never binds an
/// address reads and re-saves byte for byte as it did before addresses could
/// be bound. `{ value: ... }` is the literal spelled as a binding, and reads
/// as the literal.
#[derive(Debug, Clone)]
pub enum Bindable<T> {
    Fixed(T),
    Bound(ArgumentBinding),
}

impl<T> Bindable<T> {
    pub fn fixed(&self) -> Option<&T> {
        match self {
            Self::Fixed(value) => Some(value),
            Self::Bound(_) => None,
        }
    }

    pub fn bound(&self) -> Option<&ArgumentBinding> {
        match self {
            Self::Fixed(_) => None,
            Self::Bound(binding) => Some(binding),
        }
    }
}

impl<T: Serialize> Serialize for Bindable<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Fixed(value) => value.serialize(serializer),
            Self::Bound(binding) => binding.serialize(serializer),
        }
    }
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for Bindable<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let fixed = |value: Value| {
            T::deserialize(value)
                .map(Self::Fixed)
                .map_err(D::Error::custom)
        };
        match Value::deserialize(deserializer)? {
            Value::Object(mut map) if map.len() == 1 && map.contains_key("value") => {
                fixed(map.remove("value").unwrap_or(Value::Null))
            }
            Value::Object(map) if map.contains_key("field") || map.contains_key("from") => {
                ArgumentBinding::deserialize(Value::Object(map))
                    .map(Self::Bound)
                    .map_err(D::Error::custom)
            }
            Value::Object(_) => Err(D::Error::custom(
                "an address is a literal, or a binding: { field: <fact or answer field> } or { from: <node> }",
            )),
            other => fixed(other),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ArgumentBinding {
    Field {
        field: String,
    },
    /// Another node's output. A chain already carries the node above it as
    /// `text`; this is how a node reads a specific upstream node instead.
    Node {
        from: String,
    },
    Value {
        value: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RouteSink {
    Channel {
        #[serde(alias = "register")]
        destination: Bindable<ChannelDestination>,
        #[serde(default, rename = "initiatedBy")]
        initiated_by: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<String>,
    },
    Message {
        target: Bindable<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<String>,
    },
    Issue {
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        recipe: Option<String>,
    },
    Label {
        issue: ArgumentBinding,
        labels: Vec<String>,
    },
}

/// A dedupe window that reads and writes as the same compact string (`10m`).
///
/// Editing any field of a route rewrites its whole YAML file, so an asymmetric
/// duration — parsed from `10m` but emitted as `{secs, nanos}` — turns a valid
/// route into an unparseable one on the next save. The string form is the only
/// wire form, in the config file and in the settings surface alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupeWindow(Duration);

impl DedupeWindow {
    pub fn duration(self) -> Duration {
        self.0
    }
}

impl std::fmt::Display for DedupeWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seconds = self.0.as_secs();
        match seconds {
            s if s > 0 && s % 3600 == 0 => write!(f, "{}h", s / 3600),
            s if s > 0 && s % 60 == 0 => write!(f, "{}m", s / 60),
            s => write!(f, "{s}s"),
        }
    }
}

impl Serialize for DedupeWindow {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DedupeWindow {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Window(String),
            /// `Duration`'s derived shape, which the asymmetric serializer above
            /// used to write into route files — making them unloadable on the
            /// next read. Accepting it lets such a file load and repair itself
            /// the next time the route is saved.
            Rewritten {
                secs: u64,
            },
        }
        match Raw::deserialize(deserializer)? {
            Raw::Window(raw) => parse_duration(&raw)
                .map(DedupeWindow)
                .map_err(serde::de::Error::custom),
            Raw::Rewritten { secs } => Ok(DedupeWindow(Duration::from_secs(secs))),
        }
    }
}

pub fn parse_definition(input: &str, registry: &FactRegistry) -> Result<RouteDefinition, String> {
    let definition: RouteDefinition =
        serde_yaml::from_str(input).map_err(|error| format!("invalid route YAML: {error}"))?;
    definition.validate(registry)?;
    Ok(definition)
}

impl RouteDefinition {
    /// The route's sink nodes in definition order, which is the order their
    /// deliveries are attempted.
    pub fn sinks(&self) -> impl Iterator<Item = &RouteSink> {
        self.nodes.iter().filter_map(|node| match &node.config {
            RouteNodeConfig::Sink { sink } => Some(sink),
            _ => None,
        })
    }

    pub fn triggers(&self) -> impl Iterator<Item = &TriggerClause> {
        self.nodes.iter().filter_map(|node| match &node.config {
            RouteNodeConfig::Trigger { when } => Some(when),
            _ => None,
        })
    }

    pub fn responses(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().filter_map(|node| match &node.config {
            RouteNodeConfig::Response { response, .. } => Some(response.as_str()),
            _ => None,
        })
    }

    pub fn validate(&self, registry: &FactRegistry) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("route name cannot be empty".into());
        }
        // Building the graph is the structural half: unique ids, resolvable
        // edges, no cycles. The route-specific invariants follow.
        let graph = RouteGraph::new(self)?;
        if graph.triggers().next().is_none() {
            return Err("route must declare at least one trigger node".into());
        }
        if graph.sinks().next().is_none() {
            return Err("route must declare at least one sink node".into());
        }
        for index in 0..self.nodes.len() {
            self.validate_node(&graph, index, registry)?;
        }
        validate_edges(&graph, registry)
    }

    fn validate_node(
        &self,
        graph: &RouteGraph<'_>,
        index: usize,
        registry: &FactRegistry,
    ) -> Result<(), String> {
        let node = graph.node(index);
        let id = &node.id;
        if !graph.is_reachable(index) {
            return Err(format!("node '{id}' is not reachable from any trigger"));
        }
        if !graph.reaches_sink(index) {
            return Err(format!("node '{id}' reaches no sink"));
        }
        // A node's content is whichever producing node fed it. Two of them would
        // leave no answer to "whose text", so a merge is refused here rather
        // than resolved arbitrarily at fire time.
        if graph.producers(index).len() > 1 {
            return Err(format!(
                "node '{id}' takes content from more than one response node, which leaves its input ambiguous"
            ));
        }
        match &node.config {
            RouteNodeConfig::Trigger { when } => {
                if !graph.incoming(index).is_empty() {
                    return Err(format!("trigger node '{id}' cannot have an incoming edge"));
                }
                let source = when
                    .get("fact")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("trigger node '{id}' requires a scalar 'fact'"))?;
                registry.validate_clause(source, when)?;
            }
            RouteNodeConfig::Response { response, args } => {
                if response.trim().is_empty() {
                    return Err(format!("response node '{id}' must name a response"));
                }
                for (argument, binding) in args {
                    validate_binding(
                        graph,
                        index,
                        registry,
                        binding,
                        &format!("response node '{id}' argument '{argument}'"),
                    )?;
                }
            }
            RouteNodeConfig::Decision { decision, input } => {
                let problems = decision.problems();
                if !problems.is_empty() {
                    return Err(format!("decision node '{id}': {}", problems.join("; ")));
                }
                if let Some(input) = input {
                    validate_binding(
                        graph,
                        index,
                        registry,
                        input,
                        &format!("decision node '{id}' input"),
                    )?;
                }
            }
            RouteNodeConfig::Sink { sink } => {
                if !graph.outgoing(index).is_empty() {
                    return Err(format!(
                        "sink node '{id}' is terminal and cannot have an outgoing edge"
                    ));
                }
                validate_sink(graph, index, registry, sink)?;
            }
        }
        Ok(())
    }
}

/// Why an `answers.*` key does not resolve at a node.
enum AnswerLookup {
    /// No decision guaranteed to have run there asks this question.
    Undeclared(String),
    Invalid(String),
}

/// The answer field `key` reads at `at`: a field of a question asked by a
/// decision that dominates `at` (or is `at`, when `include_self`). Being
/// merely above is not enough, for the same reason as a `{from}` binding: a
/// path around the decision would reach `at` with the answer never asked.
fn answer_at(
    graph: &RouteGraph<'_>,
    at: usize,
    include_self: bool,
    key: &str,
) -> Result<AnswerField, AnswerLookup> {
    let Some((question, rest)) = split_answer_key(key) else {
        return Err(AnswerLookup::Invalid(format!(
            "'{key}' is not an answer field; answer fields read answers.<question>.<field>"
        )));
    };
    let declaring: Vec<&DecisionQuestion> = graph
        .dominating_decisions(at, include_self)
        .into_iter()
        .filter_map(|(_, decision)| decision.questions.iter().find(|q| q.name == question))
        .collect();
    match declaring.as_slice() {
        [] => Err(AnswerLookup::Undeclared(question.to_string())),
        [only] => answer_field(only, rest).map_err(AnswerLookup::Invalid),
        _ => Err(AnswerLookup::Invalid(format!(
            "'{key}' is ambiguous: more than one decision above it asks '{question}'"
        ))),
    }
}

/// The refusal for an `answers.*` key that does not resolve, naming `what`.
fn answer_refusal(what: &str, key: &str, lookup: AnswerLookup) -> String {
    match lookup {
        AnswerLookup::Undeclared(question) => {
            format!("{what} reads {key}, but no decision above it asks '{question}'")
        }
        AnswerLookup::Invalid(problem) => format!("{what}: {problem}"),
    }
}

/// Every edge's condition, checked against what reaches the node it leaves:
/// a fact field must be carried by every fact source arriving there, and an
/// `answers.*` key must be a field of a question some decision guaranteed to
/// have run there asks. An edge leaving a decision must say when it is
/// taken; a refusal or a failure then acts on nothing unless an edge says so.
fn validate_edges(graph: &RouteGraph<'_>, registry: &FactRegistry) -> Result<(), String> {
    for edge in graph.edges() {
        let (from, to) = (graph.node(edge.from), graph.node(edge.to));
        let name = format!("edge '{}' → '{}'", from.id, to.id);
        if let RouteNodeConfig::Decision { decision, .. } = &from.config {
            if edge.when.is_empty() {
                let example = decision
                    .questions
                    .first()
                    .map(|question| {
                        format!(" (for example answers.{}.outcome: admitted)", question.name)
                    })
                    .unwrap_or_default();
                return Err(format!(
                    "{name} leaves a decision, so it needs a when clause{example}"
                ));
            }
        }
        for clause in edge.when {
            if clause.is_empty() {
                return Err(format!("{name} has an empty when clause"));
            }
            for (key, value) in clause {
                validate_edge_test(graph, edge.from, registry, &name, key, value)?;
            }
        }
    }
    Ok(())
}

fn validate_edge_test(
    graph: &RouteGraph<'_>,
    from: usize,
    registry: &FactRegistry,
    name: &str,
    key: &str,
    value: &Value,
) -> Result<(), String> {
    if let Some(problem) = clause_value_problem(key, value) {
        return Err(format!("{name}: {problem}"));
    }
    match key {
        "fact" => {
            let named = match value {
                Value::Array(items) => items.iter().all(Value::is_string),
                value => value.is_string(),
            };
            if !named {
                return Err(format!("{name}: 'fact' names a fact source"));
            }
        }
        "presence" => {
            if !matches!(value.as_str(), Some("active" | "away")) {
                return Err(format!("{name}: presence must be active or away"));
            }
        }
        key if key.starts_with(ANSWERS_PREFIX) => {
            let field = answer_at(graph, from, true, key)
                .map_err(|lookup| answer_refusal(name, key, lookup))?;
            check_clause_value(&field, value).map_err(|problem| format!("{name}: {problem}"))?;
        }
        key => {
            let field = key.strip_suffix("Prefix").unwrap_or(key);
            let available = graph
                .fact_sources(from)
                .iter()
                .all(|source| registry.source_has_field(source, field));
            if !available {
                return Err(format!(
                    "{name} tests '{key}', which is not available from every fact source reaching it"
                ));
            }
        }
    }
    Ok(())
}

/// A bound sink address must name something that can hold that kind of
/// address: a fact field the registry marks as one, an answer field that
/// carries message targets, or an upstream node's output, which is parsed
/// when the firing resolves it.
fn validate_bound_address(
    graph: &RouteGraph<'_>,
    index: usize,
    registry: &FactRegistry,
    binding: &ArgumentBinding,
    kind: AddressKind,
    what: &str,
) -> Result<(), String> {
    let carries = match kind {
        AddressKind::Conversation => "conversation address",
        AddressKind::Target => "message target",
    };
    match binding {
        ArgumentBinding::Field { field } if field.starts_with(ANSWERS_PREFIX) => {
            let answer = answer_at(graph, index, false, field)
                .map_err(|lookup| answer_refusal(what, field, lookup))?;
            if kind == AddressKind::Target && answer.address {
                Ok(())
            } else {
                Err(format!(
                    "{what} is bound to '{field}', which carries no {carries}"
                ))
            }
        }
        ArgumentBinding::Field { field } => {
            let carried = graph
                .fact_sources(index)
                .iter()
                .all(|source| registry.field_address(source, field) == Some(kind));
            if carried {
                Ok(())
            } else {
                Err(format!(
                    "{what} is bound to '{field}', which carries no {carries}"
                ))
            }
        }
        ArgumentBinding::Node { .. } => Ok(()),
        ArgumentBinding::Value { .. } => Err(format!(
            "{what} spells a literal as a binding; write the address itself"
        )),
    }
}

/// Every sink is validated on its own terms: one bad sink names itself
/// rather than failing the route with a message about "the sink".
fn validate_sink(
    graph: &RouteGraph<'_>,
    index: usize,
    registry: &FactRegistry,
    sink: &RouteSink,
) -> Result<(), String> {
    let id = &graph.node(index).id;
    let body = match sink {
        RouteSink::Message { body, .. } | RouteSink::Channel { body, .. } => body.as_deref(),
        _ => None,
    };
    if let Some(body) = body {
        if body.trim().is_empty() {
            return Err(format!("sink '{id}' body must not be blank"));
        }
        for token in crate::text_template::tokens(body) {
            let crate::text_template::Token::Placeholder { name, .. } = token else {
                continue;
            };
            let what = format!("sink '{id}' body");
            let binding = ArgumentBinding::Field { field: name.into() };
            if let Err(problem) = validate_binding(graph, index, registry, &binding, &what) {
                if name.starts_with(ANSWERS_PREFIX) {
                    return Err(problem);
                }
                return Err(format!(
                    "sink '{id}' body names {{{{{name}}}}}, which no field reaching it provides"
                ));
            }
        }
    }
    match sink {
        RouteSink::Channel {
            destination,
            initiated_by,
            ..
        } => match destination {
            Bindable::Fixed(ChannelDestination::Subscriptions) => {
                if initiated_by
                    .as_deref()
                    .is_some_and(|v| v != "operator_subscription" && v != "cairn_push")
                {
                    return Err("unknown channel initiatedBy value".into());
                }
            }
            Bindable::Fixed(ChannelDestination::Conversation(address)) => {
                if initiated_by.is_some() {
                    return Err("a conversation channel sink is always Cairn-initiated and cannot declare initiatedBy".into());
                }
                if !crate::channels::conversation_capabilities(address.provider())
                    .append_to_conversation
                {
                    return Err(format!(
                        "{} does not support append_to_conversation",
                        address.provider()
                    ));
                }
            }
            Bindable::Bound(binding) => {
                if initiated_by.is_some() {
                    return Err(format!("channel sink '{id}' binds its destination to a conversation, which is always Cairn-initiated and cannot declare initiatedBy"));
                }
                let what = format!("channel sink '{id}' destination");
                validate_binding(graph, index, registry, binding, &what)?;
                validate_bound_address(
                    graph,
                    index,
                    registry,
                    binding,
                    AddressKind::Conversation,
                    &what,
                )?;
            }
        },
        RouteSink::Message {
            target: Bindable::Fixed(target),
            ..
        } if !super::decision::is_message_target(target) => {
            return Err("message target must be a canonical project, issue, or thread URI".into())
        }
        RouteSink::Message {
            target: Bindable::Bound(binding),
            ..
        } => {
            let what = format!("message sink '{id}' target");
            validate_binding(graph, index, registry, binding, &what)?;
            validate_bound_address(graph, index, registry, binding, AddressKind::Target, &what)?;
        }
        RouteSink::Issue { labels, .. } | RouteSink::Label { labels, .. } => {
            if labels.is_empty() {
                return Err("issue and label sinks require at least one label".into());
            }
            if labels.iter().any(|label| label.trim().is_empty()) {
                return Err("sink labels must be non-empty strings".into());
            }
        }
        RouteSink::Message { .. } => {}
    }
    if let RouteSink::Label { issue, .. } = sink {
        if let ArgumentBinding::Value { value } = issue {
            let Some(target) = value.as_str() else {
                return Err("label sink issue value must be a canonical issue URI string".into());
            };
            if !matches!(
                cairn_common::uri::parse_uri(target),
                Some(cairn_common::uri::CairnResource::Issue { .. })
            ) {
                return Err("label sink issue value must be a canonical issue URI".into());
            }
        }
        validate_binding(
            graph,
            index,
            registry,
            issue,
            &format!("label sink '{}' issue address", graph.node(index).id),
        )?;
    }
    Ok(())
}

/// A binding is checked against what actually reaches this node: a field must
/// be carried by every trigger upstream of it, and a node reference must name a
/// producing node above it in the graph.
fn validate_binding(
    graph: &RouteGraph<'_>,
    index: usize,
    registry: &FactRegistry,
    binding: &ArgumentBinding,
    what: &str,
) -> Result<(), String> {
    match binding {
        ArgumentBinding::Field { field } if field.starts_with(ANSWERS_PREFIX) => {
            answer_at(graph, index, false, field)
                .map_err(|lookup| answer_refusal(what, field, lookup))?;
        }
        ArgumentBinding::Field { field } => {
            let available = graph
                .fact_sources(index)
                .iter()
                .all(|source| registry.source_has_field(source, field));
            if !available {
                return Err(format!(
                    "{what} binds field '{field}', which is not available from every fact source reaching it"
                ));
            }
        }
        ArgumentBinding::Node { from } => {
            let source = graph
                .index_of(from)
                .ok_or_else(|| format!("{what} reads unknown node '{from}'"))?;
            if !graph.node(source).config.produces_content() {
                return Err(format!(
                    "{what} reads node '{from}', which is not a response node"
                ));
            }
            // Being somewhere above the consumer is not enough: a response on one
            // branch leaves its output missing for a fact that arrives through a
            // different trigger. The referenced node has to run on every path
            // that reaches this one, or the binding resolves to nothing at fire
            // time — exactly the runtime-only failure this validation exists to
            // prevent.
            if !graph.dominates(source, index) {
                return Err(format!(
                    "{what} reads node '{from}', which does not run on every path that reaches it"
                ));
            }
        }
        ArgumentBinding::Value { .. } => {}
    }
    Ok(())
}

/* ------------------------------------------------------------ serialization */

impl Serialize for RouteNode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("type", self.config.type_name())?;
        if !self.name.is_empty() {
            map.serialize_entry("name", &self.name)?;
        }
        if let Some(position) = &self.position {
            map.serialize_entry("position", position)?;
        }
        match &self.config {
            RouteNodeConfig::Trigger { when } => map.serialize_entry("when", when)?,
            RouteNodeConfig::Response { response, args } => {
                map.serialize_entry("response", response)?;
                if !args.is_empty() {
                    map.serialize_entry("args", args)?;
                }
            }
            RouteNodeConfig::Decision { decision, input } => {
                if let Some(input) = input {
                    map.serialize_entry("input", input)?;
                }
                map.serialize_entry("questions", &decision.questions)?;
                if let Some(models) = &decision.models {
                    map.serialize_entry("models", models)?;
                }
                if !decision.thresholds.is_empty() {
                    map.serialize_entry("thresholds", &decision.thresholds)?;
                }
            }
            RouteNodeConfig::Sink { sink } => map.serialize_entry("sink", sink)?,
        }
        map.end()
    }
}

/// A node as written. Every config key is optional here and the conversion below
/// admits only the ones its type allows, so `deny_unknown_fields` still catches
/// a typo while `RouteNodeConfig` stays closed: a trigger carrying a sink is a
/// named error, not a representable value.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawNode {
    id: String,
    #[serde(rename = "type")]
    node_type: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    position: Option<NodePosition>,
    #[serde(default)]
    when: Option<TriggerClause>,
    #[serde(default)]
    response: Option<String>,
    #[serde(default)]
    args: Option<BTreeMap<String, ArgumentBinding>>,
    #[serde(default)]
    input: Option<ArgumentBinding>,
    #[serde(default)]
    questions: Option<Vec<DecisionQuestion>>,
    #[serde(default)]
    models: Option<Vec<cairn_config::allotment::AllotmentEntry>>,
    #[serde(default)]
    thresholds: Option<Thresholds>,
    #[serde(default)]
    sink: Option<RouteSink>,
}

impl RawNode {
    fn into_node(self) -> Result<RouteNode, String> {
        let declared = [
            ("when", self.when.is_some()),
            ("response", self.response.is_some()),
            ("args", self.args.is_some()),
            ("input", self.input.is_some()),
            ("questions", self.questions.is_some()),
            ("models", self.models.is_some()),
            ("thresholds", self.thresholds.is_some()),
            ("sink", self.sink.is_some()),
        ];
        let only = |allowed: &[&str]| -> Result<(), String> {
            match declared
                .iter()
                .find(|(key, present)| *present && !allowed.contains(key))
            {
                Some((key, _)) => Err(format!(
                    "{} node '{}' cannot declare '{key}'",
                    self.node_type, self.id
                )),
                None => Ok(()),
            }
        };
        let missing = |key: &str| format!("{} node '{}' requires '{key}'", self.node_type, self.id);
        let config = match self.node_type.as_str() {
            "trigger" => {
                only(&["when"])?;
                RouteNodeConfig::Trigger {
                    when: self.when.clone().ok_or_else(|| missing("when"))?,
                }
            }
            "response" => {
                only(&["response", "args"])?;
                RouteNodeConfig::Response {
                    response: self.response.clone().ok_or_else(|| missing("response"))?,
                    args: self.args.clone().unwrap_or_default(),
                }
            }
            "decision" => {
                only(&["input", "questions", "models", "thresholds"])?;
                RouteNodeConfig::Decision {
                    decision: DecisionNode {
                        questions: self.questions.clone().ok_or_else(|| missing("questions"))?,
                        models: self.models.clone(),
                        thresholds: self.thresholds.unwrap_or_default(),
                    },
                    input: self.input.clone(),
                }
            }
            "sink" => {
                only(&["sink"])?;
                RouteNodeConfig::Sink {
                    sink: self.sink.clone().ok_or_else(|| missing("sink"))?,
                }
            }
            other => {
                return Err(format!(
                "unknown route node type '{other}', expected trigger, response, decision, or sink"
            ))
            }
        };
        Ok(RouteNode {
            id: self.id,
            name: self.name,
            position: self.position,
            config,
        })
    }
}

impl<'de> Deserialize<'de> for RouteNode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        RawNode::deserialize(deserializer)?
            .into_node()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyTransform {
    response: String,
    #[serde(default)]
    args: BTreeMap<String, ArgumentBinding>,
}

/// A route as written, in either serialization. The graph form is the only one
/// emitted; the linear `when`/`transforms`/`to` form is read once and healed, so
/// a file written before the graph existed loads and re-saves as a graph.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawDefinition {
    name: String,
    description: String,
    #[serde(default = "enabled")]
    enabled: bool,
    #[serde(default)]
    nodes: Option<Vec<RouteNode>>,
    #[serde(default)]
    edges: Option<Vec<RouteEdge>>,
    #[serde(default)]
    when: Option<Vec<TriggerClause>>,
    #[serde(default)]
    transforms: Option<Vec<LegacyTransform>>,
    #[serde(default, deserialize_with = "deserialize_legacy_sinks")]
    to: Option<Vec<RouteSink>>,
    #[serde(default)]
    dedupe: Option<DedupeWindow>,
}

impl<'de> Deserialize<'de> for RouteDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawDefinition::deserialize(deserializer)?;
        let legacy = raw.when.is_some() || raw.transforms.is_some() || raw.to.is_some();
        if legacy && (raw.nodes.is_some() || raw.edges.is_some()) {
            return Err(serde::de::Error::custom(
                "a route declares either nodes and edges or the older when/transforms/to, not both",
            ));
        }
        let (nodes, edges) = if legacy {
            heal(
                raw.when.unwrap_or_default(),
                raw.transforms.unwrap_or_default(),
                raw.to.unwrap_or_default(),
            )
        } else {
            (raw.nodes.unwrap_or_default(), raw.edges.unwrap_or_default())
        };
        Ok(RouteDefinition {
            name: raw.name,
            description: raw.description,
            enabled: raw.enabled,
            nodes,
            edges,
            dedupe: raw.dedupe,
        })
    }
}

/// The linear form as a graph: triggers OR into the head of the transform chain,
/// the chain runs in order, and its tail fans out to every sink. That is exactly
/// what the old dispatcher did, so a healed route fires identically.
fn heal(
    when: Vec<TriggerClause>,
    transforms: Vec<LegacyTransform>,
    to: Vec<RouteSink>,
) -> (Vec<RouteNode>, Vec<RouteEdge>) {
    let bare = |id: String, config: RouteNodeConfig| RouteNode {
        id,
        name: String::new(),
        position: None,
        config,
    };
    let ids = |prefix: &str, count: usize| -> Vec<String> {
        (1..=count).map(|n| format!("{prefix}-{n}")).collect()
    };
    let (triggers, responses, sinks) = (
        ids("trigger", when.len()),
        ids("response", transforms.len()),
        ids("sink", to.len()),
    );

    let mut nodes = Vec::new();
    for (id, clause) in triggers.iter().zip(when) {
        nodes.push(bare(id.clone(), RouteNodeConfig::Trigger { when: clause }));
    }
    for (id, transform) in responses.iter().zip(transforms) {
        nodes.push(bare(
            id.clone(),
            RouteNodeConfig::Response {
                response: transform.response,
                args: transform.args,
            },
        ));
    }
    for (id, sink) in sinks.iter().zip(to) {
        nodes.push(bare(id.clone(), RouteNodeConfig::Sink { sink }));
    }

    let mut edges = Vec::new();
    let mut connect = |from: &str, to: &str| edges.push(RouteEdge::new(from, to));
    let after_triggers: Vec<&String> = match responses.first() {
        Some(head) => vec![head],
        None => sinks.iter().collect(),
    };
    for trigger in &triggers {
        for target in &after_triggers {
            connect(trigger, target);
        }
    }
    for pair in responses.windows(2) {
        connect(&pair[0], &pair[1]);
    }
    if let Some(tail) = responses.last() {
        for sink in &sinks {
            connect(tail, sink);
        }
    }
    (nodes, edges)
}

/// The legacy `to` key took one sink as a bare map or several as a sequence.
/// Dispatching on the shape rather than using an untagged enum keeps serde's own
/// precise message ("unknown variant `channl`", "missing field `target`"), which
/// is what the invalid-route panel shows an operator.
fn deserialize_legacy_sinks<'de, D>(deserializer: D) -> Result<Option<Vec<RouteSink>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Sinks;

    impl<'de> serde::de::Visitor<'de> for Sinks {
        type Value = Vec<RouteSink>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sink map or a sequence of sink maps")
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
            Deserialize::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            RouteSink::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                .map(|sink| vec![sink])
        }
    }

    deserializer.deserialize_any(Sinks).map(Some)
}

fn enabled() -> bool {
    true
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    let (number, unit) = raw.split_at(raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len()));
    let value: u64 = number
        .parse()
        .map_err(|_| format!("invalid duration '{raw}'"))?;
    let seconds = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 3600,
        _ => return Err(format!("invalid duration '{raw}'")),
    };
    Ok(Duration::from_secs(seconds))
}

/// The acceptance fixture: positions, literal URI options, and a message sink
/// bound to the pick, behind an edge condition with a comparison.
#[cfg(test)]
pub(crate) const FIXTURE_A: &str = r#"name: Pick an issue
description: Decide which issue should hear about an unaddressed channel message.
nodes:
  - { id: inbound, type: trigger, position: {x: 0, y: 0}, when: { fact: channel_message, addressed: false } }
  - id: pick
    type: decision
    position: {x: 240, y: 0}
    questions:
      - name: target
        type: choice
        instructions: Which issue should hear about this?
        options: [ { value: "cairn://p/cairn/12" }, { value: "cairn://p/cairn/34" } ]
    thresholds: { native: 0.6 }
  - { id: deliver, type: sink, position: {x: 480, y: 0}, sink: { kind: message, target: { field: answers.target.pick } } }
edges:
  - { from: inbound, to: pick }
  - { from: pick, to: deliver, when: { answers.target.outcome: admitted, answers.target.confidence: { gte: 0.6 } } }
"#;

/// cairn/4896's ambient-routing shape: chartered-thread options with top-k,
/// a delivery bound to the admitted addresses, and acknowledgements bound to
/// the conversation, one per outcome.
#[cfg(test)]
pub(crate) const FIXTURE_B: &str = r#"name: Ambient routing
description: Route unaddressed ambient channel messages to the threads that own them.
nodes:
  - { id: inbound, type: trigger, when: { fact: channel_message, bindingKind: ambient, addressed: false } }
  - id: pick
    type: decision
    input: { field: text }
    questions: [ { name: thread, type: choice, instructions: Which thread owns this?, source: chartered_threads, topK: 2 } ]
  - { id: deliver, type: sink, sink: { kind: message, target: { field: answers.thread.addresses } } }
  - { id: ack_routed, type: sink, sink: { kind: channel, destination: { field: conversation } } }
  - { id: ack_unrouted, type: sink, sink: { kind: channel, destination: { field: conversation } } }
  - { id: ack_refused, type: sink, sink: { kind: channel, destination: "discord:1/2" } }
  - { id: ack_error, type: sink, sink: { kind: channel, destination: { field: conversation } } }
edges:
  - { from: inbound, to: pick }
  - { from: pick, to: deliver, when: { answers.thread.outcome: admitted } }
  - { from: pick, to: ack_routed, when: { answers.thread.outcome: admitted } }
  - { from: pick, to: ack_unrouted, when: { answers.thread.outcome: below_threshold } }
  - { from: pick, to: ack_refused, when: { answers.thread.outcome: refused } }
  - { from: pick, to: ack_error, when: { answers.thread.outcome: [unanswerable, failed] } }
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::decision::{OptionSource, QuestionKind};

    #[test]
    fn fixture_a_parses_validates_and_round_trips_with_positions() {
        let parsed = parse(FIXTURE_A).unwrap();
        let pick = parsed.nodes.iter().find(|node| node.id == "pick").unwrap();
        assert_eq!(pick.config.type_name(), "decision");
        assert_eq!(pick.position, Some(NodePosition { x: 240.0, y: 0.0 }));
        let RouteNodeConfig::Decision { decision, input } = &pick.config else {
            panic!("a decision node");
        };
        assert!(input.is_none(), "an unset input reads the arriving content");
        assert_eq!(decision.thresholds.native, Some(0.6));
        assert_eq!(parsed.edges[1].when.len(), 1);
        assert!(matches!(
            sink_of(&parsed, "deliver"),
            RouteSink::Message {
                target: Bindable::Bound(ArgumentBinding::Field { .. }),
                ..
            }
        ));

        let emitted = serde_yaml::to_string(&parsed).unwrap();
        assert!(emitted.contains("type: decision"), "{emitted}");
        assert!(emitted.contains("field: answers.target.pick"), "{emitted}");
        assert!(emitted.contains("gte: 0.6"), "{emitted}");
        let reparsed = parse(&emitted).unwrap();
        assert_eq!(serde_yaml::to_string(&reparsed).unwrap(), emitted);
        // An unconditional edge writes no `when` at all.
        assert_eq!(
            serde_json::to_value(&reparsed.edges[0]).unwrap(),
            serde_json::json!({"from": "inbound", "to": "pick"})
        );
    }

    #[test]
    fn fixture_b_loads_with_positions_omitted_and_a_declared_option_source() {
        let parsed = parse(FIXTURE_B).unwrap();
        assert!(parsed.nodes.iter().all(|node| node.position.is_none()));
        let RouteNodeConfig::Decision { decision, input } = &parsed.nodes[1].config else {
            panic!("a decision node");
        };
        assert!(matches!(input, Some(ArgumentBinding::Field { field }) if field == "text"));
        let question = &decision.questions[0];
        assert_eq!(
            question.option_source(),
            Some(OptionSource::CharteredThreads)
        );
        assert_eq!(question.top_k(), 2);
        assert!(
            matches!(&question.kind, QuestionKind::Choice { options, .. } if options.is_empty())
        );
        assert!(matches!(
            sink_of(&parsed, "ack_refused"),
            RouteSink::Channel {
                destination: Bindable::Fixed(ChannelDestination::Conversation(_)),
                ..
            }
        ));
        let emitted = serde_yaml::to_string(&parsed).unwrap();
        assert!(!emitted.contains("options"), "{emitted}");
        assert!(emitted.contains("source: chartered_threads"), "{emitted}");
        assert_eq!(
            serde_yaml::to_string(&parse(&emitted).unwrap()).unwrap(),
            emitted
        );
    }

    #[test]
    fn answers_on_an_edge_no_declaring_decision_dominates_are_refused_by_name() {
        // The sink takes a literal here, so the edge is the only reader of
        // the answer that a path around the decision reaches.
        let skipped = FIXTURE_A
            .replace(
                "target: { field: answers.target.pick }",
                "target: 'cairn://p/cairn/12'",
            )
            .replace(
                "  - { from: inbound, to: pick }\n",
                "  - { from: inbound, to: pick }\n  - { from: inbound, to: deliver, when: { answers.target.outcome: admitted } }\n",
            );
        assert_eq!(
            parse(&skipped).unwrap_err(),
            "edge 'inbound' → 'deliver' reads answers.target.outcome, but no decision above it asks 'target'"
        );
        // The same key on the decision's own edge, under another name, is not
        // a question it asks either.
        let renamed = FIXTURE_A.replace("answers.target.outcome", "answers.goal.outcome");
        assert!(parse(&renamed)
            .unwrap_err()
            .contains("no decision above it asks 'goal'"));
        // A sink whose address reads the answer while a second path skips the
        // decision cannot be sure it was asked.
        let bypass = FIXTURE_A
            .replace(
                "  - { id: deliver,",
                "  - { id: direct, type: trigger, when: { fact: channel_message } }\n  - { id: deliver,",
            )
            .replace(
                "  - { from: inbound, to: pick }\n",
                "  - { from: inbound, to: pick }\n  - { from: direct, to: deliver }\n",
            );
        assert!(parse(&bypass)
            .unwrap_err()
            .contains("reads answers.target.pick, but no decision above it asks 'target'"));
        assert!(parse(
            &graph_route("{ kind: channel, register: notify }", "").replace(
                "when: { fact: attention }",
                "when: { fact: attention, answers.q.pick: a }"
            )
        )
        .unwrap_err()
        .contains("answers.* is only known below a decision"));
    }

    #[test]
    fn an_edge_leaving_a_decision_must_say_when_it_is_taken() {
        let unconditional = FIXTURE_A.replace(
            ", when: { answers.target.outcome: admitted, answers.target.confidence: { gte: 0.6 } }",
            "",
        );
        assert_eq!(
            parse(&unconditional).unwrap_err(),
            "edge 'pick' → 'deliver' leaves a decision, so it needs a when clause (for example answers.target.outcome: admitted)"
        );
    }

    #[test]
    fn answer_clause_values_are_checked_against_the_question() {
        let wrong_outcome = FIXTURE_A.replace("outcome: admitted", "outcome: raised");
        assert!(parse(&wrong_outcome)
            .unwrap_err()
            .contains("is one of admitted"));
        let compared_pick = FIXTURE_A.replace(
            "answers.target.confidence: { gte: 0.6 }",
            "answers.target.pick: { gte: 0.6 }",
        );
        assert!(parse(&compared_pick)
            .unwrap_err()
            .contains("is not a number"));
        let unknown_operator =
            FIXTURE_A.replace("confidence: { gte: 0.6 }", "confidence: { above: 0.6 }");
        assert!(parse(&unknown_operator)
            .unwrap_err()
            .contains("unknown comparison 'above'"));
        let undeclared_option = FIXTURE_A.replace(
            "answers.target.confidence: { gte: 0.6 }",
            "answers.target.probability.cairn://p/cairn/99: { gt: 0.5 }",
        );
        assert!(parse(&undeclared_option)
            .unwrap_err()
            .contains("has no answer"));
    }

    #[test]
    fn a_bound_address_must_name_a_field_that_carries_one() {
        let text_destination = FIXTURE_B.replacen(
            "destination: { field: conversation }",
            "destination: { field: text }",
            1,
        );
        assert!(parse(&text_destination)
            .unwrap_err()
            .contains("channel sink 'ack_routed' destination is bound to 'text', which carries no conversation address"));
        let answer_destination = FIXTURE_B.replacen(
            "destination: { field: conversation }",
            "destination: { field: answers.thread.address }",
            1,
        );
        assert!(parse(&answer_destination)
            .unwrap_err()
            .contains("carries no conversation address"));
        let pick_target = FIXTURE_B.replace(
            "target: { field: answers.thread.addresses }",
            "target: { field: answers.thread.pick }",
        );
        assert!(
            parse(&pick_target)
                .unwrap_err()
                .contains("is bound to 'answers.thread.pick', which carries no message target"),
            "a thread name is not an address"
        );
        let unknown_shape = FIXTURE_B.replace(
            "target: { field: answers.thread.addresses }",
            "target: { fiel: answers.thread.addresses }",
        );
        assert!(parse(&unknown_shape)
            .unwrap_err()
            .contains("an address is a literal, or a binding"));
        // A literal spelled as a binding reads as the literal.
        let spelled = parse(&graph_route(
            "{ kind: message, target: { value: 'cairn://p/cairn/1' } }",
            "",
        ))
        .unwrap();
        assert!(serde_yaml::to_string(&spelled)
            .unwrap()
            .contains("target: cairn://p/cairn/1"));
    }

    #[test]
    fn the_same_edge_with_two_conditions_is_refused_and_an_identical_one_folds() {
        let doubled = FIXTURE_A.replace(
            "  - { from: inbound, to: pick }\n",
            "  - { from: inbound, to: pick }\n  - { from: pick, to: deliver, when: { answers.target.outcome: below_threshold } }\n",
        );
        assert!(parse(&doubled)
            .unwrap_err()
            .contains("give one edge a list of when clauses instead"));
        let repeated = FIXTURE_A.replace(
            "  - { from: inbound, to: pick }\n",
            "  - { from: inbound, to: pick }\n  - { from: inbound, to: pick }\n",
        );
        assert!(parse(&repeated).is_ok());
    }

    #[test]
    fn sink_body_templates_round_trip_and_validate_reaching_fields() {
        for kind in [
            "message, target: 'cairn://p/cairn/1'",
            "channel, destination: 'discord:1/2'",
        ] {
            let yaml = graph_route(
                &format!("{{ kind: {kind}, body: 'Via {{text}}' }}").replace("{text}", "{{text}}"),
                "",
            );
            let parsed = parse(&yaml).unwrap();
            let emitted = serde_yaml::to_string(&parsed).unwrap();
            assert!(emitted.contains("body:"), "{emitted}");
            assert_eq!(
                serde_yaml::to_string(&parse(&emitted).unwrap()).unwrap(),
                emitted
            );
            let unknown = yaml.replace("{{text}}", "{{foo}}");
            assert_eq!(
                parse(&unknown).unwrap_err(),
                "sink 'sink-1' body names {{foo}}, which no field reaching it provides"
            );
            for blank in ["", "  "] {
                let blank = yaml.replace("Via {{text}}", blank);
                assert!(parse(&blank)
                    .unwrap_err()
                    .contains("sink 'sink-1' body must not be blank"));
            }
            assert!(parse(&yaml.replace("Via {{text}}", r"\{{foo}} {{ a b }}")).is_ok());
        }
    }

    #[test]
    fn sink_body_templates_use_all_sources_and_dominating_declared_answers() {
        let body = FIXTURE_B.replace(
            "kind: channel, destination:",
            "kind: channel, body: '{{answers.thread.probability.devops}} {{text}}', destination:",
        );
        assert!(
            parse(&body).is_ok(),
            "concrete probability of sourced choice is declared"
        );
        assert!(parse(&body.replace("probability.devops", "imaginary"))
            .unwrap_err()
            .contains("has no answer"));
        let bypass = body.replace(
            "  - { from: inbound, to: pick }",
            "  - { from: inbound, to: pick }\n  - { from: inbound, to: ack_routed }",
        );
        assert!(parse(&bypass)
            .unwrap_err()
            .contains("no decision above it asks 'thread'"));
        let all_sources = graph_route(
            "{ kind: message, target: 'cairn://p/cairn/1', body: '{{conversation}}' }",
            "",
        )
        .replace(
            "when: { fact: attention }",
            "when: { fact: channel_message }",
        )
        .replace(
            "  - id: sink-1",
            "  - { id: second, type: trigger, when: { fact: thread_stream } }\n  - id: sink-1",
        )
        .replace(
            "  - { from: trigger-1, to: sink-1 }",
            "  - { from: trigger-1, to: sink-1 }\n  - { from: second, to: sink-1 }",
        );
        assert_eq!(
            parse(&all_sources).unwrap_err(),
            "sink 'sink-1' body names {{conversation}}, which no field reaching it provides"
        );
        let ambiguous = body.replace("  - { from: inbound, to: pick }", "  - { from: inbound, to: earlier }\n  - { from: earlier, to: pick, when: { answers.thread.outcome: admitted } }")
            .replace("  - id: pick", "  - id: earlier\n    type: decision\n    questions: [ { name: thread, type: choice, instructions: First?, source: chartered_threads } ]\n  - id: pick");
        assert!(parse(&ambiguous).unwrap_err().contains("ambiguous"));
    }

    #[test]
    fn literal_sinks_and_the_core_pack_routes_load_and_re_save_unchanged() {
        for name in ["followed-thread-stream", "github-mention"] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../packs/core/routes/{name}.yaml"));
            let yaml = std::fs::read_to_string(path).unwrap();
            let parsed = parse(&yaml).unwrap();
            let emitted = serde_yaml::to_string(&parsed).unwrap();
            assert_eq!(
                serde_yaml::to_string(&parse(&emitted).unwrap()).unwrap(),
                emitted
            );
            assert!(!emitted.contains("when: []"), "{emitted}");
        }
        let literal = parse(&graph_route(
            "{ kind: message, target: 'cairn://p/cairn/1' }",
            "",
        ))
        .unwrap();
        let RouteNode { config, .. } = &literal.nodes[1];
        let RouteNodeConfig::Sink { sink } = config else {
            panic!("a sink");
        };
        // A literal serializes as the bare value it always was.
        assert_eq!(
            serde_json::to_value(sink).unwrap(),
            serde_json::json!({"kind": "message", "target": "cairn://p/cairn/1"})
        );
        let channel = parse(&graph_route(
            "{ kind: channel, destination: 'discord:12/34' }",
            "",
        ))
        .unwrap();
        let RouteNodeConfig::Sink { sink } = &channel.nodes[1].config else {
            panic!("a sink");
        };
        assert_eq!(
            serde_json::to_value(sink).unwrap(),
            serde_json::json!({"kind": "channel", "destination": "discord:12/34", "initiatedBy": null})
        );
        assert_eq!(
            serde_json::to_value(&literal.edges[0]).unwrap(),
            serde_json::json!({"from": "trigger-1", "to": "sink-1"})
        );
    }

    /// A one-trigger, one-sink graph over `sink`, with `tail` appended as
    /// further top-level keys — a `dedupe` window, say. Tests that need a
    /// different shape spell their graph out instead.
    fn graph_route(sink: &str, tail: &str) -> String {
        format!(
            "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: {{ fact: attention }}\n  - id: sink-1\n    type: sink\n    sink: {sink}\nedges:\n  - {{ from: trigger-1, to: sink-1 }}\n{tail}"
        )
    }

    fn parse(yaml: &str) -> Result<RouteDefinition, String> {
        parse_definition(yaml, &FactRegistry::default())
    }

    fn sink_of(definition: &RouteDefinition, id: &str) -> RouteSink {
        match &definition
            .nodes
            .iter()
            .find(|node| node.id == id)
            .expect("node exists")
            .config
        {
            RouteNodeConfig::Sink { sink } => sink.clone(),
            other => panic!("expected a sink node, got {}", other.type_name()),
        }
    }

    #[test]
    fn a_legacy_linear_file_heals_into_the_graph_and_then_round_trips() {
        let legacy = "name: test\ndescription: test\nwhen:\n  - fact: attention\n  - fact: thread_stream\ntransforms:\n  - response: conveyor\nto:\n  - { kind: channel, register: notify }\n  - { kind: message, target: 'cairn://p/cairn/1' }\ndedupe: 10m\n";
        let healed = parse(legacy).unwrap();
        assert_eq!(
            healed
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            ["trigger-1", "trigger-2", "response-1", "sink-1", "sink-2"]
        );
        assert_eq!(
            healed
                .edges
                .iter()
                .map(|edge| format!("{}->{}", edge.from, edge.to))
                .collect::<Vec<_>>(),
            [
                "trigger-1->response-1",
                "trigger-2->response-1",
                "response-1->sink-1",
                "response-1->sink-2",
            ]
        );

        // The healed definition is emitted in the graph serialization, and
        // re-reading it changes nothing: healing happens once, not every load.
        let emitted = serde_yaml::to_string(&healed).unwrap();
        assert!(!emitted.contains("transforms:"), "emitted: {emitted}");
        assert!(emitted.contains("dedupe: 10m"), "emitted: {emitted}");
        let reparsed = parse(&emitted).unwrap();
        assert_eq!(serde_yaml::to_string(&reparsed).unwrap(), emitted);
    }

    #[test]
    fn a_file_mixing_both_serializations_is_refused_rather_than_half_read() {
        let error = parse(
            "name: test\ndescription: test\nwhen:\n  - fact: attention\nnodes: []\nto: { kind: channel, register: notify }\n",
        )
        .unwrap_err();
        assert!(error.contains("not both"), "unhelpful error: {error}");
    }

    #[test]
    fn positions_round_trip_through_the_yaml() {
        let parsed = parse(&graph_route("{ kind: channel, register: notify }", "")).unwrap();
        // A hand-written file carries no positions, and none are invented.
        assert!(parsed.nodes.iter().all(|node| node.position.is_none()));
        assert!(!serde_yaml::to_string(&parsed).unwrap().contains("position"));

        let placed = parse(
            "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    name: Attention\n    position: { x: 12.5, y: -40 }\n    when: { fact: attention }\n  - id: sink-1\n    type: sink\n    position: { x: 12.5, y: 220 }\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: trigger-1, to: sink-1 }\n",
        )
        .unwrap();
        assert_eq!(
            placed.nodes[0].position,
            Some(NodePosition { x: 12.5, y: -40.0 })
        );
        assert_eq!(placed.nodes[0].name, "Attention");
        let emitted = serde_yaml::to_string(&placed).unwrap();
        let reparsed = parse(&emitted).unwrap();
        assert_eq!(reparsed.nodes[0].position, placed.nodes[0].position);
        assert_eq!(serde_yaml::to_string(&reparsed).unwrap(), emitted);
    }

    #[test]
    fn channel_destinations_round_trip_canonically_and_legacy_notify_heals() {
        let legacy = parse(&graph_route("{ kind: channel, register: notify }", "")).unwrap();
        let emitted = serde_yaml::to_string(&legacy).unwrap();
        assert!(emitted.contains("destination: subscriptions"), "{emitted}");
        assert!(!emitted.contains("register:"), "{emitted}");

        let directed = parse(&graph_route(
            "{ kind: channel, destination: ' DISCORD: 12/34 ' }",
            "",
        ))
        .unwrap();
        let emitted = serde_yaml::to_string(&directed).unwrap();
        assert!(emitted.contains("destination: discord:12/34"), "{emitted}");
        assert_eq!(
            serde_yaml::to_string(&parse(&emitted).unwrap()).unwrap(),
            emitted
        );
    }

    #[test]
    fn directed_channel_destinations_refuse_meaningless_initiators() {
        let error = parse(&graph_route(
            "{ kind: channel, destination: 'telegram:123', initiatedBy: operator_subscription }",
            "",
        ))
        .unwrap_err();
        assert!(error.contains("always Cairn-initiated"), "{error}");
    }

    #[test]
    fn a_malformed_sink_reports_what_is_wrong_with_it() {
        // The operator reads this string in the invalid-route panel, so it has to
        // name the mistake rather than say nothing matched.
        let error = parse(&graph_route("{ kind: channl, register: notify }", "")).unwrap_err();
        assert!(error.contains("channl"), "unhelpful error: {error}");
        let error = parse(&graph_route("{ kind: message }", "")).unwrap_err();
        assert!(error.contains("target"), "unhelpful error: {error}");
    }

    #[test]
    fn a_node_may_only_carry_the_config_its_type_has() {
        let error = parse(
            "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n    sink: { kind: channel, register: notify }\n",
        )
        .unwrap_err();
        assert!(error.contains("cannot declare 'sink'"), "error: {error}");
        let error = parse(
            "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    positon: { x: 1, y: 1 }\n    when: { fact: attention }\n",
        )
        .unwrap_err();
        assert!(
            error.contains("positon"),
            "a typo is not silently dropped: {error}"
        );
    }

    #[test]
    fn agent_is_an_unknown_type_and_decision_is_the_fourth() {
        let error =
            parse("name: test\ndescription: test\nnodes:\n  - id: judge\n    type: agent\n")
                .unwrap_err();
        assert!(
            error.contains(
                "unknown route node type 'agent', expected trigger, response, decision, or sink"
            ),
            "error: {error}"
        );
        let error = parse("name: test\ndescription: test\nnodes:\n  - id: n\n    type: sync\n")
            .unwrap_err();
        assert!(
            error.contains("trigger, response, decision, or sink"),
            "error: {error}"
        );
        assert!(parse(FIXTURE_A).is_ok());
        let error = parse("name: test\ndescription: test\nnodes:\n  - id: d\n    type: decision\n")
            .unwrap_err();
        assert!(
            error.contains("decision node 'd' requires 'questions'"),
            "{error}"
        );
        let error = parse("name: test\ndescription: test\nnodes:\n  - id: d\n    type: decision\n    questions: []\n    sink: { kind: channel, register: notify }\n")
            .unwrap_err();
        assert!(
            error.contains("decision node 'd' cannot declare 'sink'"),
            "{error}"
        );
    }

    #[test]
    fn a_route_must_have_a_trigger_and_a_sink_and_every_sink_is_validated() {
        assert!(parse("name: test\ndescription: test\nnodes: []\n")
            .unwrap_err()
            .contains("at least one trigger"));
        assert!(parse(
            "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n"
        )
        .unwrap_err()
        .contains("at least one sink"));
        // The second sink is the invalid one, and it is still rejected.
        let two_sinks = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\n  - id: sink-2\n    type: sink\n    sink: { kind: issue, labels: [] }\nedges:\n  - { from: trigger-1, to: sink-1 }\n  - { from: trigger-1, to: sink-2 }\n";
        assert!(parse(two_sinks).unwrap_err().contains("at least one label"));
    }

    #[test]
    fn the_graph_must_be_acyclic_with_terminal_sinks_and_resolvable_edges() {
        let cyclic = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: a\n    type: response\n    response: conveyor\n  - id: b\n    type: response\n    response: conveyor\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: trigger-1, to: a }\n  - { from: a, to: b }\n  - { from: b, to: a }\n  - { from: b, to: sink-1 }\n";
        assert!(parse(cyclic).unwrap_err().contains("acyclic"));

        let after_sink = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\n  - id: after\n    type: response\n    response: conveyor\nedges:\n  - { from: trigger-1, to: sink-1 }\n  - { from: sink-1, to: after }\n";
        assert!(parse(after_sink).unwrap_err().contains("terminal"));

        let dangling = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: trigger-1, to: nowhere }\n";
        assert!(parse(dangling)
            .unwrap_err()
            .contains("unknown node 'nowhere'"));
    }

    #[test]
    fn a_node_off_every_trigger_to_sink_path_is_rejected() {
        let orphan = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\n  - id: stranded\n    type: response\n    response: conveyor\nedges:\n  - { from: trigger-1, to: sink-1 }\n";
        assert!(parse(orphan)
            .unwrap_err()
            .contains("'stranded' is not reachable"));

        let discarded = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\n  - id: wasted\n    type: response\n    response: conveyor\nedges:\n  - { from: trigger-1, to: sink-1 }\n  - { from: trigger-1, to: wasted }\n";
        assert!(parse(discarded)
            .unwrap_err()
            .contains("'wasted' reaches no sink"));
    }

    #[test]
    fn a_node_fed_by_two_response_nodes_is_refused_as_ambiguous() {
        let merge = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: left\n    type: response\n    response: conveyor\n  - id: right\n    type: response\n    response: conveyor\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: trigger-1, to: left }\n  - { from: trigger-1, to: right }\n  - { from: left, to: sink-1 }\n  - { from: right, to: sink-1 }\n";
        assert!(parse(merge).unwrap_err().contains("ambiguous"));
    }

    #[test]
    fn a_response_argument_may_read_an_upstream_node_but_not_a_sibling() {
        let chained = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: first\n    type: response\n    response: conveyor\n  - id: second\n    type: response\n    response: conveyor\n    args:\n      text: { from: first }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: trigger-1, to: first }\n  - { from: first, to: second }\n  - { from: second, to: sink-1 }\n";
        assert!(parse(chained).is_ok());

        let sideways = chained.replace("{ from: first }", "{ from: sink-1 }");
        assert!(parse(&sideways)
            .unwrap_err()
            .contains("not a response node"));

        let missing = chained.replace("{ from: first }", "{ from: nowhere }");
        assert!(parse(&missing)
            .unwrap_err()
            .contains("unknown node 'nowhere'"));
    }

    /// Being above the consumer is not the same as running before it. A second
    /// trigger feeding the consumer directly means a fact can arrive with the
    /// referenced output never produced — so the binding is refused at load
    /// rather than resolving to nothing at fire time.
    #[test]
    fn a_node_binding_is_refused_when_a_second_trigger_can_skip_the_node_it_reads() {
        let skippable = "name: test\ndescription: test\nnodes:\n  - id: trigger-a\n    type: trigger\n    when: { fact: attention }\n  - id: trigger-b\n    type: trigger\n    when: { fact: thread_stream }\n  - id: condense\n    type: response\n    response: conveyor\n  - id: consumer\n    type: response\n    response: conveyor\n    args:\n      text: { from: condense }\n  - id: sink-1\n    type: sink\n    sink: { kind: channel, register: notify }\nedges:\n  - { from: trigger-a, to: condense }\n  - { from: condense, to: consumer }\n  - { from: trigger-b, to: consumer }\n  - { from: consumer, to: sink-1 }\n";
        assert!(parse(skippable)
            .unwrap_err()
            .contains("does not run on every path"));

        // Route the second trigger through the same node and the binding holds,
        // because now every path to the consumer runs it.
        let funnelled = skippable.replace(
            "  - { from: trigger-b, to: consumer }\n",
            "  - { from: trigger-b, to: condense }\n",
        );
        assert!(parse(&funnelled).is_ok());
    }

    /// The same rule protects a sink: a label sink whose issue address reads a
    /// node that may not have run would fail its delivery, not fall back.
    #[test]
    fn a_sink_binding_is_held_to_the_same_rule_as_a_response_argument() {
        let skippable = "name: test\ndescription: test\nnodes:\n  - id: trigger-a\n    type: trigger\n    when: { fact: attention }\n  - id: trigger-b\n    type: trigger\n    when: { fact: attention }\n  - id: address\n    type: response\n    response: conveyor\n  - id: sink-1\n    type: sink\n    sink: { kind: label, issue: { from: address }, labels: [signal] }\nedges:\n  - { from: trigger-a, to: address }\n  - { from: address, to: sink-1 }\n  - { from: trigger-b, to: sink-1 }\n";
        assert!(parse(skippable)
            .unwrap_err()
            .contains("does not run on every path"));
    }

    #[test]
    fn a_field_binding_must_be_carried_by_every_trigger_that_feeds_it() {
        let both = "name: test\ndescription: test\nnodes:\n  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n  - id: trigger-2\n    type: trigger\n    when: { fact: thread_stream }\n  - id: sink-1\n    type: sink\n    sink: { kind: label, issue: { field: threadUri }, labels: [signal] }\nedges:\n  - { from: trigger-1, to: sink-1 }\n  - { from: trigger-2, to: sink-1 }\n";
        // `threadUri` is a thread_stream field; the attention trigger also feeds
        // this sink, so the binding cannot be satisfied.
        assert!(parse(both).unwrap_err().contains("threadUri"));
        // With only the thread trigger feeding it, the same binding is fine.
        let narrowed = both
            .replace(
                "  - id: trigger-1\n    type: trigger\n    when: { fact: attention }\n",
                "",
            )
            .replace("  - { from: trigger-1, to: sink-1 }\n", "");
        assert!(parse(&narrowed).is_ok());
    }

    #[test]
    fn label_sink_requires_explicit_binding_and_non_empty_labels() {
        let parsed = parse(&graph_route(
            "{ kind: label, issue: { field: detailUri }, labels: [needs-review] }",
            "",
        ))
        .unwrap();
        assert!(matches!(
            sink_of(&parsed, "sink-1"),
            RouteSink::Label { .. }
        ));
        assert!(parse(&graph_route(
            "{ kind: label, issue: { value: 'cairn://p/cairn/1' }, labels: [] }",
            ""
        ))
        .unwrap_err()
        .contains("at least one label"));
        assert!(parse(&graph_route(
            "{ kind: label, issue: 'cairn://p/cairn/1', labels: [bug] }",
            ""
        ))
        .unwrap_err()
        .contains("invalid route YAML"));
    }

    #[test]
    fn message_sink_rejects_unsupported_project_subresources() {
        let error = parse(&graph_route(
            "{ kind: message, target: 'cairn://p/cairn/42/messages' }",
            "",
        ))
        .unwrap_err();
        assert!(error.contains("canonical project, issue, or thread URI"));
        assert!(parse(&graph_route(
            "{ kind: message, target: 'cairn://p/cairn/devops' }",
            "",
        ))
        .is_ok());
    }

    #[test]
    fn dedupe_window_round_trips_through_its_own_serialization() {
        let parsed = parse(&graph_route(
            "{ kind: channel, register: notify }",
            "dedupe: 10m\n",
        ))
        .unwrap();
        assert_eq!(
            parsed.dedupe.map(|d| d.duration()),
            Some(Duration::from_secs(600))
        );

        // Saving a route rewrites its file from the parsed definition, so the
        // emitted YAML has to parse back into the same window.
        let yaml = serde_yaml::to_string(&parsed).unwrap();
        assert!(yaml.contains("dedupe: 10m"), "emitted YAML was: {yaml}");
        assert_eq!(parse(&yaml).unwrap().dedupe, parsed.dedupe);

        for (input, expected) in [
            ("90s", "90s"),
            ("120s", "2m"),
            ("3600s", "1h"),
            ("45m", "45m"),
        ] {
            let parsed = parse(&graph_route(
                "{ kind: channel, register: notify }",
                &format!("dedupe: {input}\n"),
            ))
            .unwrap();
            assert_eq!(parsed.dedupe.unwrap().to_string(), expected);
        }
    }

    #[test]
    fn a_route_file_the_old_serializer_rewrote_loads_and_heals() {
        let parsed = parse(&graph_route(
            "{ kind: channel, register: notify }",
            "dedupe:\n  secs: 600\n  nanos: 0\n",
        ))
        .unwrap();
        assert_eq!(parsed.dedupe.unwrap().duration(), Duration::from_secs(600));
        assert!(serde_yaml::to_string(&parsed)
            .unwrap()
            .contains("dedupe: 10m"));
    }

    #[test]
    fn absent_dedupe_stays_absent_in_emitted_yaml() {
        let parsed = parse(&graph_route("{ kind: channel, register: notify }", "")).unwrap();
        assert!(!serde_yaml::to_string(&parsed).unwrap().contains("dedupe"));
    }
}
