use crate::config::slugify;

/// Resolve the canonical visible node segment for emitted cairn:// URIs.
///
/// Preference order:
/// 1. slugified human-readable node name
/// 2. raw node name when slugification is empty
/// 3. legacy recipe node id as a final fallback
pub(crate) fn visible_node_segment(
    recipe_node_id: Option<&str>,
    node_name: Option<&str>,
) -> Option<String> {
    if let Some(name) = node_name.map(str::trim).filter(|name| !name.is_empty()) {
        let slug = slugify(name);
        return Some(if slug.is_empty() {
            name.to_string()
        } else {
            slug
        });
    }

    recipe_node_id
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

pub(crate) fn visible_node_segment_or_name(
    recipe_node_id: Option<&str>,
    node_name: &str,
) -> String {
    visible_node_segment(recipe_node_id, Some(node_name)).unwrap_or_else(|| node_name.to_string())
}

pub(crate) fn matches_node_uri_segment(
    segment: &str,
    recipe_node_id: Option<&str>,
    node_name: Option<&str>,
) -> bool {
    node_name.is_some_and(|name| {
        name == segment
            || (!slugify(name).is_empty() && slugify(name) == segment)
            || visible_node_segment(recipe_node_id, Some(name)).as_deref() == Some(segment)
    }) || recipe_node_id.is_some_and(|id| id == segment)
}
