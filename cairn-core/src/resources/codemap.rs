//! Rendering for `cairn://p/{project}/codemap`.
//!
//! The read path never walks a tree. It resolves the project's current base
//! commit, serves the map stored for that commit if there is one, and otherwise
//! arms a background refresh and serves the newest map the project has, flagged
//! stale. The producer — the walk, the churn join, the cache, and the base-advance
//! hook — lives in [`crate::projects::codemap`].
//!
//! Two renderings, because two readers want different things. An agent reading
//! this URI wants to know the shape of the codebase without several hundred
//! kilobytes of JSON landing in its context, so the default is a summary. The map
//! surface wants the payload itself, and asks for it with `?format=json`.

use crate::orchestrator::Orchestrator;
use crate::projects::codemap::{self, CodeMap, CHURN_WINDOW_DAYS};
use crate::storage::LocalDb;
use cairn_common::query::QueryParam;

use super::common::{connect_for_read, lookup_project_by_key};

/// How many files each summary table lists.
const SUMMARY_ROWS: usize = 10;

pub(crate) async fn read_codemap(
    orch: &Orchestrator,
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    let json = match params
        .iter()
        .find(|param| param.key != "format")
        .map(|param| param.key.as_str())
    {
        Some(unsupported) => {
            return format!("Unsupported code map query parameter: {unsupported}");
        }
        None => match params
            .iter()
            .find(|param| param.key == "format")
            .map(|param| param.value.as_str())
        {
            None => false,
            Some("json") => true,
            Some(other) => return format!("Code map format must be json, not '{other}'"),
        },
    };

    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project = match lookup_project_by_key(&conn, project_key).await {
        Ok(project) => project,
        Err(error) => return error,
    };
    drop(conn);

    let view = match codemap::current(orch, db, &project.project_id).await {
        Ok(view) => view,
        Err(error) => return error,
    };
    if view.unmappable {
        return format!(
            "Project '{project_key}' has no repository checkout, so it has no code map."
        );
    }
    let Some(map) = view.map else {
        return computing(project_key, view.head.as_deref());
    };
    if json {
        return serde_json::to_string(&map)
            .unwrap_or_else(|error| format!("Code map could not be serialized: {error}"));
    }
    render_summary(project_key, &map, view.stale, view.head.as_deref())
}

fn computing(project_key: &str, head: Option<&str>) -> String {
    let base = head
        .map(|sha| format!(" for base {}", short(sha)))
        .unwrap_or_default();
    format!(
        "# Code map: {project_key}\n\n\
         No code map computed yet{base}. One is being built now — read this URI again in a moment."
    )
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(12)]
}

fn render_summary(project_key: &str, map: &CodeMap, stale: bool, head: Option<&str>) -> String {
    let mut out = format!("# Code map: {project_key}\n\n");
    out.push_str(&format!(
        "- Base commit: `{}`\n- Computed: {}\n- Files: {} · Import edges: {}\n",
        short(&map.base_commit_sha),
        chrono::DateTime::from_timestamp(map.computed_at, 0)
            .map(|when| when.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| map.computed_at.to_string()),
        map.files.len(),
        map.imports.len(),
    ));
    if stale {
        let moved = head
            .map(|sha| format!(" (base is now `{}`)", short(sha)))
            .unwrap_or_default();
        out.push_str(&format!(
            "- Stale{moved}: the base advanced past this map and a recompute is running.\n"
        ));
    }

    let mut by_language: std::collections::BTreeMap<&str, (usize, u64)> = Default::default();
    for file in &map.files {
        let entry = by_language.entry(&file.language).or_default();
        entry.0 += 1;
        entry.1 += file.line_count as u64;
    }
    if !by_language.is_empty() {
        out.push_str("\n## Languages\n\n| Language | Files | Lines |\n| --- | --: | --: |\n");
        let mut rows: Vec<_> = by_language.into_iter().collect();
        rows.sort_by(|a, b| b.1 .0.cmp(&a.1 .0));
        for (language, (files, lines)) in rows {
            out.push_str(&format!("| {language} | {files} | {lines} |\n"));
        }
    }

    let mut churned: Vec<_> = map
        .files
        .iter()
        .filter(|file| file.churn_additions + file.churn_deletions > 0)
        .collect();
    churned.sort_by_key(|file| std::cmp::Reverse(file.churn_additions + file.churn_deletions));
    if !churned.is_empty() {
        out.push_str(&format!(
            "\n## Most churn (last {CHURN_WINDOW_DAYS} days of merged work)\n\n\
             | File | +/- | Lines |\n| --- | --: | --: |\n"
        ));
        for file in churned.iter().take(SUMMARY_ROWS) {
            out.push_str(&format!(
                "| {} | +{} −{} | {} |\n",
                file.path, file.churn_additions, file.churn_deletions, file.line_count
            ));
        }
    }

    let mut inbound: std::collections::HashMap<&str, usize> = Default::default();
    for (_, to) in &map.imports {
        *inbound.entry(to.as_str()).or_default() += 1;
    }
    if !inbound.is_empty() {
        let mut rows: Vec<_> = inbound.into_iter().collect();
        rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        out.push_str("\n## Most depended on\n\n| File | Importers |\n| --- | --: |\n");
        for (path, count) in rows.into_iter().take(SUMMARY_ROWS) {
            out.push_str(&format!("| {path} | {count} |\n"));
        }
    }

    out.push_str(&format!(
        "\nRead `cairn://p/{project_key}/codemap?format=json` for the full payload \
         (`base_commit_sha`, `computed_at`, `files[]`, `imports[]`).\n"
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::codemap::CodeMapFile;

    fn file(path: &str, language: &str, additions: i64, deletions: i64) -> CodeMapFile {
        CodeMapFile {
            path: path.to_string(),
            language: language.to_string(),
            line_count: 10,
            size_bytes: 100,
            churn_additions: additions,
            churn_deletions: deletions,
        }
    }

    fn map() -> CodeMap {
        CodeMap {
            base_commit_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
            computed_at: 1_700_000_000,
            files: vec![
                file("src/lib.rs", "rust", 40, 5),
                file("src/main.rs", "rust", 2, 0),
                file("web/app.tsx", "tsx", 0, 0),
            ],
            imports: vec![
                ("src/main.rs".to_string(), "src/lib.rs".to_string()),
                ("web/app.tsx".to_string(), "src/lib.rs".to_string()),
            ],
        }
    }

    #[test]
    fn the_summary_leads_with_the_base_and_the_totals() {
        let rendered = render_summary("cairn", &map(), false, None);
        assert!(rendered.contains("`0123456789ab`"), "{rendered}");
        assert!(
            rendered.contains("Files: 3 · Import edges: 2"),
            "{rendered}"
        );
        assert!(!rendered.contains("Stale"), "{rendered}");
    }

    #[test]
    fn a_stale_map_says_so_and_names_the_base_it_is_behind() {
        let rendered = render_summary("cairn", &map(), true, Some("fedcba9876543210"));
        assert!(rendered.contains("Stale"), "{rendered}");
        assert!(rendered.contains("`fedcba987654`"), "{rendered}");
    }

    #[test]
    fn churn_and_dependents_rank_by_weight() {
        let rendered = render_summary("cairn", &map(), false, None);
        let hottest = rendered.find("src/lib.rs | +40").expect("churn row");
        let cooler = rendered.find("src/main.rs | +2").expect("churn row");
        assert!(hottest < cooler, "{rendered}");
        // Both other files import lib.rs, and nothing imports them.
        assert!(rendered.contains("| src/lib.rs | 2 |"), "{rendered}");
    }

    #[test]
    fn a_file_with_no_merged_work_is_left_out_of_the_churn_table() {
        let rendered = render_summary("cairn", &map(), false, None);
        assert!(!rendered.contains("| web/app.tsx | +0"), "{rendered}");
        // ...but it is still counted in the language breakdown.
        assert!(rendered.contains("| tsx | 1 |"), "{rendered}");
    }
}
