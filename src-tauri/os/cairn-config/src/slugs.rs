use std::collections::HashSet;

/// ASCII-only slugify for cairn:// URI segments.
///
/// Mirrors `slugifyResourceSegment` in `packages/ui/src/uri.ts` exactly so that
/// URIs emitted by the backend and the browser are byte-identical for any
/// given input.
pub fn slugify_resource_segment(value: &str) -> String {
    value
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Derive a unique URI-safe slug for a delegated task.
///
/// This is the **single source of truth** for delegated task slugs. The
/// backend computes the slug once at materialization time and stores it in
/// `jobs.node_name`; the frontend reads that stored value rather than
/// recomputing.
///
/// Algorithm:
/// 1. Slugify the description (ASCII-only, URI-safe) and split on `-` into words.
/// 2. Start with the first `min(2, N)` words joined by `-`.
/// 3. While the candidate collides with the reserved set, append the next
///    word until either it is unique or words are exhausted.
/// 4. If still colliding, append `-2`, `-3`, … to the full word join.
/// 5. Empty / non-alphanumeric descriptions fall back to `task` with the same
///    numeric suffix disambiguation.
///
/// The reserved set must be prepopulated with the already-assigned slugs
/// (normalized through `slugify_resource_segment`) for sibling task jobs.
pub fn derive_unique_task_slug(description: &str, reserved: &HashSet<String>) -> String {
    let slugged = slugify_resource_segment(description);
    let words: Vec<&str> = slugged.split('-').filter(|s| !s.is_empty()).collect();

    if words.is_empty() {
        return next_available("task", reserved);
    }

    let initial = 2.min(words.len());
    for count in initial..=words.len() {
        let candidate = words[..count].join("-");
        if !reserved.contains(&candidate) {
            return candidate;
        }
    }

    next_available(&words.join("-"), reserved)
}

fn next_available(base: &str, reserved: &HashSet<String>) -> String {
    if !reserved.contains(base) {
        return base.to_string();
    }
    let mut n: u32 = 2;
    loop {
        let candidate = format!("{}-{}", base, n);
        if !reserved.contains(&candidate) {
            return candidate;
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slugify_resource_segment_matches_frontend_rules() {
        assert_eq!(slugify_resource_segment("Survey Search"), "survey-search");
        assert_eq!(slugify_resource_segment("Map Tauri"), "map-tauri");
        // Non-ASCII is stripped, mirroring JS `/[^a-z0-9]+/g`.
        assert_eq!(slugify_resource_segment("Caf\u{00e9}"), "caf");
        // Collapses runs of non-alphanumerics and trims ends.
        assert_eq!(
            slugify_resource_segment("  Fix bug: off-by-one!  "),
            "fix-bug-off-by-one"
        );
    }

    #[test]
    fn derive_unique_task_slug_takes_first_two_words() {
        let reserved = HashSet::new();
        assert_eq!(
            derive_unique_task_slug("Survey Search usage for migration complexity", &reserved),
            "survey-search"
        );
    }

    #[test]
    fn derive_unique_task_slug_grows_one_word_on_collision() {
        let mut reserved = HashSet::new();
        reserved.insert("survey-search".to_string());
        assert_eq!(
            derive_unique_task_slug("Survey Search usage for migration complexity", &reserved),
            "survey-search-usage"
        );
    }

    #[test]
    fn derive_unique_task_slug_grows_through_word_exhaustion() {
        let mut reserved = HashSet::new();
        reserved.insert("map-tauri".to_string());
        assert_eq!(
            derive_unique_task_slug("Map Tauri deployment", &reserved),
            "map-tauri-deployment"
        );
    }

    #[test]
    fn derive_unique_task_slug_appends_numeric_suffix_when_words_exhausted() {
        let mut reserved = HashSet::new();
        reserved.insert("map-tauri".to_string());
        reserved.insert("map-tauri-deployment".to_string());
        assert_eq!(
            derive_unique_task_slug("Map Tauri deployment", &reserved),
            "map-tauri-deployment-2"
        );
    }

    #[test]
    fn derive_unique_task_slug_single_word_no_collision() {
        let reserved = HashSet::new();
        assert_eq!(derive_unique_task_slug("Explore", &reserved), "explore");
    }

    #[test]
    fn derive_unique_task_slug_single_word_collision_appends_suffix() {
        let mut reserved = HashSet::new();
        reserved.insert("explore".to_string());
        assert_eq!(derive_unique_task_slug("Explore", &reserved), "explore-2");
    }

    #[test]
    fn derive_unique_task_slug_keeps_incrementing_suffix() {
        let mut reserved = HashSet::new();
        reserved.insert("explore".to_string());
        reserved.insert("explore-2".to_string());
        reserved.insert("explore-3".to_string());
        assert_eq!(derive_unique_task_slug("Explore", &reserved), "explore-4");
    }

    #[test]
    fn derive_unique_task_slug_empty_input_falls_back_to_task() {
        let reserved = HashSet::new();
        assert_eq!(derive_unique_task_slug("", &reserved), "task");
        assert_eq!(derive_unique_task_slug("   ", &reserved), "task");
        assert_eq!(derive_unique_task_slug("!!!???", &reserved), "task");
    }

    #[test]
    fn derive_unique_task_slug_empty_input_numeric_suffix() {
        let mut reserved = HashSet::new();
        reserved.insert("task".to_string());
        reserved.insert("task-2".to_string());
        assert_eq!(derive_unique_task_slug("", &reserved), "task-3");
    }

    #[test]
    fn derive_unique_task_slug_numeric_input() {
        let reserved = HashSet::new();
        assert_eq!(derive_unique_task_slug("123", &reserved), "123");
    }

    #[test]
    fn derive_unique_task_slug_punctuation_and_spaces() {
        let reserved = HashSet::new();
        assert_eq!(
            derive_unique_task_slug("  Fix bug: off-by-one!", &reserved),
            "fix-bug"
        );
    }

    #[test]
    fn derive_unique_task_slug_long_description_takes_two_words_no_collision() {
        let reserved = HashSet::new();
        let desc = "one two three four five six seven eight nine ten eleven twelve";
        assert_eq!(derive_unique_task_slug(desc, &reserved), "one-two");
    }
}
