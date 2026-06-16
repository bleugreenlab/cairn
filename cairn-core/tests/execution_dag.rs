use cairn_core::internal::db_records::{DbRecipeEdge, DbRecipeNode};
use cairn_core::internal::execution::dag::find_reachable_nodes;

fn node(id: &str, node_type: &str) -> DbRecipeNode {
    DbRecipeNode {
        id: id.to_string(),
        recipe_id: "recipe-1".to_string(),
        node_type: node_type.to_string(),
        name: id.to_string(),
        position_x: 0.0,
        position_y: 0.0,
        config: None,
        created_at: 0,
        updated_at: 0,
        parent_id: None,
    }
}

fn edge(id: &str, source: &str, target: &str, edge_type: &str) -> DbRecipeEdge {
    DbRecipeEdge {
        id: id.to_string(),
        recipe_id: "recipe-1".to_string(),
        source_node_id: source.to_string(),
        target_node_id: target.to_string(),
        source_handle: String::new(),
        target_handle: String::new(),
        edge_type: edge_type.to_string(),
        created_at: 0,
    }
}

#[test]
fn reachable_nodes_follow_control_edges_in_parent_first_order() {
    let nodes = vec![
        node("trigger", "trigger"),
        node("planner", "agent"),
        node("builder", "agent"),
        node("reviewer", "agent"),
        node("orphan", "agent"),
    ];
    let edges = vec![
        edge("e1", "trigger", "planner", "control"),
        edge("e2", "planner", "builder", "control"),
        edge("e3", "planner", "reviewer", "control"),
    ];

    assert_eq!(
        find_reachable_nodes(&nodes, &edges),
        vec!["trigger", "planner", "builder", "reviewer"]
    );
}

#[test]
fn reachable_nodes_ignore_context_edges_and_stop_cycles() {
    let nodes = vec![
        node("trigger", "trigger"),
        node("planner", "agent"),
        node("context-only", "agent"),
        node("cycle", "agent"),
    ];
    let edges = vec![
        edge("e1", "trigger", "planner", "control"),
        edge("e2", "planner", "context-only", "context"),
        edge("e3", "planner", "cycle", "control"),
        edge("e4", "cycle", "planner", "control"),
    ];

    assert_eq!(
        find_reachable_nodes(&nodes, &edges),
        vec!["trigger", "planner", "cycle"]
    );
}

#[test]
fn reachable_nodes_include_each_trigger_component() {
    let nodes = vec![
        node("issue-trigger", "trigger"),
        node("event-trigger", "trigger"),
        node("issue-planner", "agent"),
        node("event-planner", "agent"),
        node("orphan", "agent"),
    ];
    let edges = vec![
        edge("e1", "issue-trigger", "issue-planner", "control"),
        edge("e2", "event-trigger", "event-planner", "control"),
    ];

    assert_eq!(
        find_reachable_nodes(&nodes, &edges),
        vec![
            "issue-trigger",
            "event-trigger",
            "issue-planner",
            "event-planner"
        ]
    );
}
