//! Role normalization: derive a display role from a raw `jobs.node_name` or
//! `agent_config_id`. Shared by the token and economics analytics.

/// Normalize a raw `jobs.node_name` into a display role: strip a trailing
/// `-<digits>` instance suffix, replace separators with spaces, title-case
/// each word. Empty / missing names become `Other`.
pub(super) fn normalize_role(node_name: Option<&str>) -> String {
    let raw = node_name.unwrap_or("").trim();
    if raw.is_empty() {
        return "Other".to_string();
    }
    let stripped = strip_instance_suffix(raw);
    let words: Vec<String> = stripped
        .split(|c: char| c == '-' || c == '_' || c.is_whitespace())
        .filter(|w| !w.is_empty())
        .map(title_word)
        .collect();
    if words.is_empty() {
        "Other".to_string()
    } else {
        words.join(" ")
    }
}

/// Drop a trailing `-<digits>` segment (e.g. `planner-0` -> `planner`).
fn strip_instance_suffix(name: &str) -> &str {
    if let Some(idx) = name.rfind('-') {
        let suffix = &name[idx + 1..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return &name[..idx];
        }
    }
    name
}

fn title_word(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_normalization() {
        assert_eq!(normalize_role(Some("planner-0")), "Planner");
        assert_eq!(normalize_role(Some("builder")), "Builder");
        assert_eq!(normalize_role(Some("cairn-setup")), "Cairn Setup");
        assert_eq!(normalize_role(Some("pr-review-2")), "Pr Review");
        assert_eq!(normalize_role(None), "Other");
        assert_eq!(normalize_role(Some("  ")), "Other");
    }
}
