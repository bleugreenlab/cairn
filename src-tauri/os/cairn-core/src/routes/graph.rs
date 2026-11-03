//! Adjacency over a route definition's node+edge DAG.
//!
//! Validation and the dispatcher both need the same view of a route's graph —
//! who feeds whom, in what order, and which content a node's work is done on —
//! so both read it from here rather than each walking `edges` on its own.
//!
//! Construction is the structural half of validation: node ids must be unique,
//! every edge must resolve, and the graph must be acyclic. A `RouteGraph` cannot
//! exist for a definition that fails any of those, which is what makes the
//! dispatcher's traversal total.

use super::definition::{RouteDefinition, RouteNode, RouteNodeConfig};
use super::predicate::TriggerClause;
use crate::models::decision::DecisionNode;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// One edge, resolved to node indexes, with the condition under which a fact
/// that reached `from` goes on to `to`. An empty `when` is unconditional.
#[derive(Debug, Clone, Copy)]
pub struct GraphEdge<'a> {
    pub from: usize,
    pub to: usize,
    pub when: &'a [TriggerClause],
}

pub struct RouteGraph<'a> {
    nodes: &'a [RouteNode],
    edges: Vec<GraphEdge<'a>>,
    incoming: Vec<Vec<usize>>,
    outgoing: Vec<Vec<usize>>,
    /// Every node exactly once, each after all of its predecessors.
    order: Vec<usize>,
    reachable: Vec<bool>,
    reaches_sink: Vec<bool>,
    dominators: Vec<HashSet<usize>>,
}

impl<'a> RouteGraph<'a> {
    pub fn new(definition: &'a RouteDefinition) -> Result<Self, String> {
        let nodes = definition.nodes.as_slice();
        let mut index: HashMap<&str, usize> = HashMap::new();
        for (position, node) in nodes.iter().enumerate() {
            if node.id.trim().is_empty() {
                return Err("every route node needs a non-empty id".into());
            }
            if index.insert(node.id.as_str(), position).is_some() {
                return Err(format!("duplicate route node id '{}'", node.id));
            }
        }

        let mut incoming = vec![Vec::new(); nodes.len()];
        let mut outgoing = vec![Vec::new(); nodes.len()];
        let mut edges: Vec<GraphEdge<'a>> = Vec::new();
        for edge in &definition.edges {
            let resolve = |id: &str| {
                index
                    .get(id)
                    .copied()
                    .ok_or_else(|| format!("route edge names unknown node '{id}'"))
            };
            let (from, to) = (resolve(&edge.from)?, resolve(&edge.to)?);
            // The same edge written twice is one edge, not a second delivery.
            // Written twice with different conditions, it would be two answers
            // to one question, and a list of clauses already says "either".
            if let Some(existing) = edges.iter().find(|e| e.from == from && e.to == to) {
                if existing.when == edge.when.as_slice() {
                    continue;
                }
                return Err(format!(
                    "edges '{}' → '{}' are declared twice with different when clauses; give one edge a list of when clauses instead",
                    edge.from, edge.to
                ));
            }
            edges.push(GraphEdge {
                from,
                to,
                when: &edge.when,
            });
            outgoing[from].push(to);
            incoming[to].push(from);
        }

        let mut pending: Vec<usize> = incoming.iter().map(Vec::len).collect();
        let mut queue: VecDeque<usize> = (0..nodes.len()).filter(|i| pending[*i] == 0).collect();
        let mut order = Vec::with_capacity(nodes.len());
        while let Some(current) = queue.pop_front() {
            order.push(current);
            for &next in &outgoing[current] {
                pending[next] -= 1;
                if pending[next] == 0 {
                    queue.push_back(next);
                }
            }
        }
        if order.len() != nodes.len() {
            let cycle = nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| pending[*i] > 0)
                .map(|(_, node)| node.id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "route graph must be acyclic, and these nodes form a cycle: {cycle}"
            ));
        }

        // A fact enters at a trigger and leaves at a sink, so both directions of
        // "is this node on a live path" fall out of one pass over the order.
        let mut reachable = vec![false; nodes.len()];
        for &current in &order {
            if nodes[current].config.is_trigger() || incoming[current].iter().any(|i| reachable[*i])
            {
                reachable[current] = true;
            }
        }
        let mut reaches_sink = vec![false; nodes.len()];
        for &current in order.iter().rev() {
            if nodes[current].config.is_sink() || outgoing[current].iter().any(|i| reaches_sink[*i])
            {
                reaches_sink[current] = true;
            }
        }

        // Which nodes are guaranteed to have run by the time this one does.
        // A node's guarantees are its own plus whatever ALL of its predecessors
        // guarantee, so a node fed by two different branches inherits only what
        // both of them ran. Topological order puts every predecessor first, so
        // one pass settles it.
        let mut dominators: Vec<HashSet<usize>> = vec![HashSet::new(); nodes.len()];
        for &current in &order {
            let mut settled = match incoming[current].split_first() {
                None => HashSet::new(),
                Some((first, rest)) => rest.iter().fold(dominators[*first].clone(), |set, prev| {
                    set.intersection(&dominators[*prev]).copied().collect()
                }),
            };
            settled.insert(current);
            dominators[current] = settled;
        }

        Ok(Self {
            nodes,
            edges,
            incoming,
            outgoing,
            order,
            reachable,
            reaches_sink,
            dominators,
        })
    }

    pub fn nodes(&self) -> &'a [RouteNode] {
        self.nodes
    }

    pub fn node(&self, index: usize) -> &'a RouteNode {
        &self.nodes[index]
    }

    pub fn order(&self) -> &[usize] {
        &self.order
    }

    pub fn incoming(&self, index: usize) -> &[usize] {
        &self.incoming[index]
    }

    pub fn outgoing(&self, index: usize) -> &[usize] {
        &self.outgoing[index]
    }

    pub fn triggers(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.nodes.len()).filter(|i| self.nodes[*i].config.is_trigger())
    }

    /// Sink nodes in definition order, which is the order their deliveries are
    /// attempted and the order their channel delivery keys are numbered.
    pub fn sinks(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.nodes.len()).filter(|i| self.nodes[*i].config.is_sink())
    }

    /// Every edge, in definition order, duplicates folded.
    pub fn edges(&self) -> &[GraphEdge<'a>] {
        &self.edges
    }

    /// The edges leaving `index`, in definition order.
    pub fn outgoing_edges(&self, index: usize) -> impl Iterator<Item = &GraphEdge<'a>> + '_ {
        self.edges.iter().filter(move |edge| edge.from == index)
    }

    /// The nodes above `index` that produce content of their own, looking
    /// through any decision between them: a decision passes the content that
    /// reached it on unchanged. A node's `text` is whichever of these fed it,
    /// which is why more than one is a validation error rather than a merge:
    /// there would be no answer to "whose text".
    pub fn producers(&self, index: usize) -> Vec<usize> {
        let mut found = BTreeSet::new();
        let mut seen = HashSet::new();
        let mut pending: Vec<usize> = self.incoming[index].clone();
        while let Some(current) = pending.pop() {
            if !seen.insert(current) {
                continue;
            }
            let config = &self.nodes[current].config;
            if config.produces_content() {
                found.insert(current);
            } else if config.is_decision() {
                pending.extend(self.incoming[current].iter().copied());
            }
        }
        found.into_iter().collect()
    }

    /// The decision nodes guaranteed to have run by the time `index` does,
    /// nearest last, and `index` itself when `include_self` and it is one.
    /// Their answers are what `answers.*` reads at `index`; conditional edges
    /// only remove paths, so a dominating decision has always run.
    pub fn dominating_decisions(
        &self,
        index: usize,
        include_self: bool,
    ) -> Vec<(usize, &'a DecisionNode)> {
        self.order
            .iter()
            .copied()
            .filter(|&candidate| {
                self.dominates(candidate, index) || (include_self && candidate == index)
            })
            .filter_map(|candidate| match &self.nodes[candidate].config {
                RouteNodeConfig::Decision { decision, .. } => Some((candidate, decision)),
                _ => None,
            })
            .collect()
    }

    pub fn is_reachable(&self, index: usize) -> bool {
        self.reachable[index]
    }

    pub fn reaches_sink(&self, index: usize) -> bool {
        self.reaches_sink[index]
    }

    pub fn index_of(&self, id: &str) -> Option<usize> {
        self.nodes.iter().position(|node| node.id == id)
    }

    /// Whether `candidate` runs on every path that reaches `node`.
    ///
    /// This is stricter than "is above it", and the difference is what makes a
    /// `{ from: ... }` binding mean something. A node merely above the consumer
    /// may sit on one branch while a second trigger feeds the consumer directly;
    /// a fact arriving on that second trigger would run the consumer with the
    /// referenced output missing.
    pub fn dominates(&self, candidate: usize, node: usize) -> bool {
        candidate != node && self.dominators[node].contains(&candidate)
    }

    /// Every node above this one, however far up.
    pub fn ancestors(&self, index: usize) -> Vec<usize> {
        let mut seen = vec![false; self.nodes.len()];
        let mut found = Vec::new();
        let mut queue = VecDeque::from([index]);
        seen[index] = true;
        while let Some(current) = queue.pop_front() {
            for &previous in &self.incoming[current] {
                if !std::mem::replace(&mut seen[previous], true) {
                    found.push(previous);
                    queue.push_back(previous);
                }
            }
        }
        found
    }

    /// The fact sources whose facts can arrive at this node. A field binding is
    /// only legal if every one of them carries the field — the graph narrows
    /// that question from "every trigger in the route" to "every trigger that
    /// actually feeds this node".
    pub fn fact_sources(&self, index: usize) -> BTreeSet<&'a str> {
        std::iter::once(index)
            .chain(self.ancestors(index))
            .filter_map(|i| match &self.nodes[i].config {
                RouteNodeConfig::Trigger { when } => {
                    when.get("fact").and_then(serde_json::Value::as_str)
                }
                _ => None,
            })
            .collect()
    }
}
