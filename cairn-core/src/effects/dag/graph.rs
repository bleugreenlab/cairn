use super::*;

pub(super) fn make_edge(
    source_id: &str,
    source_handle: &str,
    target_id: &str,
    target_handle: &str,
    edge_type: RecipeEdgeType,
) -> RecipeEdge {
    RecipeEdge {
        id: Uuid::new_v4().to_string(),
        edge_type,
        source_node_id: source_id.to_string(),
        source_handle: source_handle.to_string(),
        target_node_id: target_id.to_string(),
        target_handle: target_handle.to_string(),
    }
}

pub(super) fn push_edge_if_missing(
    edges: &mut Vec<RecipeEdge>,
    source_id: &str,
    source_handle: &str,
    target_id: &str,
    target_handle: &str,
    edge_type: RecipeEdgeType,
) {
    if edges.iter().any(|edge| {
        edge.source_node_id == source_id
            && edge.source_handle == source_handle
            && edge.target_node_id == target_id
            && edge.target_handle == target_handle
            && edge.edge_type == edge_type
    }) {
        return;
    }

    edges.push(make_edge(
        source_id,
        source_handle,
        target_id,
        target_handle,
        edge_type,
    ));
}
