//! `cairn://models/spend`: the Analytics page's spend-by-thread tree as a read.
//!
//! The data is `cairn_analytics::spend_by_thread`, the same function the
//! Analytics panel invokes, scoped by [`cairn_analytics::Window`] the way the
//! page's range selector scopes it, so the total matches the page's Cost card.
//! Parsing the query and rendering the tree are pure; only [`read_spend`]
//! touches the database.

use std::fmt::Write as _;

use cairn_analytics::{Bucket, Scope, SpendByThread, SpendNode, SpendNodeKind, TimeRange, Window};
use cairn_common::query::QueryParam;

use super::common::find_query_value;
use crate::orchestrator::Orchestrator;

const URI: &str = "cairn://models/spend";
const KEYS: &[&str] = &["by", "window", "thread", "project", "format"];
/// Models listed per node in the markdown tree.
const TOP_MODELS: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Markdown,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpendQuery {
    window: Window,
    thread: Option<String>,
    project: Option<String>,
    format: Format,
}

fn parse_query(params: &[QueryParam]) -> Result<SpendQuery, String> {
    if let Some(unknown) = params.iter().find(|p| !KEYS.contains(&p.key.as_str())) {
        return Err(format!(
            "Unsupported query parameter '{}' for {URI}; it reads {}.",
            unknown.key,
            KEYS.join(", ")
        ));
    }
    match find_query_value(params, "by") {
        None | Some("thread") => {}
        Some(other) => return Err(format!("Unknown by '{other}' for {URI}; expected thread.")),
    }
    let window = match find_query_value(params, "window") {
        None => Window::H24,
        Some(raw) => Window::parse(raw).ok_or_else(|| {
            let names: Vec<&str> = Window::ALL.iter().map(|w| w.as_str()).collect();
            format!(
                "Unknown window '{raw}' for {URI}; expected {}.",
                names.join(", ")
            )
        })?,
    };
    let format = match find_query_value(params, "format") {
        None | Some("markdown") => Format::Markdown,
        Some("json") => Format::Json,
        Some(other) => {
            return Err(format!(
                "Unknown format '{other}' for {URI}; expected markdown or json."
            ))
        }
    };
    let nonempty = |key: &str| {
        find_query_value(params, key)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
    };
    Ok(SpendQuery {
        window,
        thread: nonempty("thread"),
        project: nonempty("project"),
        format,
    })
}

pub(crate) async fn read_spend(orch: &Orchestrator, params: &[QueryParam]) -> String {
    let query = match parse_query(params) {
        Ok(query) => query,
        Err(error) => return error,
    };
    let project_id = match &query.project {
        None => None,
        Some(key) => match orch
            .db
            .local
            .query_text(
                "SELECT id FROM projects WHERE UPPER(key) = UPPER(?1) LIMIT 1",
                (key.clone(),),
            )
            .await
        {
            Ok(Some(id)) => Some(id),
            Ok(None) => return format!("Project not found: {key}"),
            Err(error) => return format!("Spend is unavailable: {error}"),
        },
    };
    let (range, bucket) = query.window.scope_at(chrono::Utc::now().timestamp());
    match cairn_analytics::spend_by_thread(&orch.db.local, &Scope::new(project_id), &range, bucket)
        .await
    {
        Ok(spend) => render(&query, range, bucket, &spend),
        Err(error) => format!("Spend is unavailable: {error}"),
    }
}

/// The thread name behind a thread node's canonical URI.
fn thread_name(node: &SpendNode) -> &str {
    node.id.rsplit('/').next().unwrap_or(&node.label)
}

fn qualified(node: &SpendNode) -> String {
    match &node.project_key {
        Some(key) => format!("{key}/{}", thread_name(node)),
        None => thread_name(node).to_string(),
    }
}

/// Resolve `?thread=` to one root: a thread by bare or `KEY/`-qualified name,
/// or the `unparented` / `other` roots.
fn find_root<'a>(spend: &'a SpendByThread, wanted: &str) -> Result<&'a SpendNode, String> {
    let lower = wanted.to_ascii_lowercase();
    if lower == "unparented" || lower == "other" {
        return spend
            .roots
            .iter()
            .find(|r| r.id == lower)
            .ok_or_else(|| format!("No {lower} spend in this window."));
    }
    let threads = spend
        .roots
        .iter()
        .filter(|r| r.kind == SpendNodeKind::Thread);
    let matches: Vec<&SpendNode> = match wanted.split_once('/') {
        Some((key, name)) => threads
            .filter(|r| {
                r.project_key
                    .as_deref()
                    .is_some_and(|k| k.eq_ignore_ascii_case(key))
                    && thread_name(r) == name
            })
            .collect(),
        None => threads.filter(|r| thread_name(r) == wanted).collect(),
    };
    match matches.as_slice() {
        [one] => Ok(one),
        [] => {
            let known: Vec<String> = spend
                .roots
                .iter()
                .filter(|r| r.kind == SpendNodeKind::Thread)
                .map(qualified)
                .collect();
            Err(if known.is_empty() {
                format!("No thread '{wanted}' has spend in this window; no thread does.")
            } else {
                format!(
                    "No thread '{wanted}' has spend in this window. Threads with spend: {}.",
                    known.join(", ")
                )
            })
        }
        many => {
            let candidates: Vec<String> = many.iter().map(|r| qualified(r)).collect();
            Err(format!(
                "Thread '{wanted}' is ambiguous; name one of {}.",
                candidates.join(", ")
            ))
        }
    }
}

fn usd(cost: f64) -> String {
    format!("${cost:.2}")
}

fn tokens(n: i64) -> String {
    let n = n as f64;
    if n >= 1e9 {
        format!("{:.1}B", n / 1e9)
    } else if n >= 1e6 {
        format!("{:.1}M", n / 1e6)
    } else if n >= 1e3 {
        format!("{:.1}k", n / 1e3)
    } else {
        format!("{n}")
    }
}

fn share(part: f64, whole: f64) -> String {
    if whole > 0.0 {
        format!("{:.1}%", part / whole * 100.0)
    } else {
        "—".to_string()
    }
}

fn bucket_name(bucket: Bucket) -> &'static str {
    match bucket {
        Bucket::Hour => "hour",
        Bucket::Day => "day",
        Bucket::Week => "week",
        Bucket::Month => "month",
    }
}

fn header(query: &SpendQuery, range: TimeRange, bucket: Bucket, spend: &SpendByThread) -> String {
    let from = match range.start() {
        Some(start) => chrono::DateTime::from_timestamp(start, 0)
            .map(|at| format!("since {}", at.format("%Y-%m-%d %H:%M UTC")))
            .unwrap_or_default(),
        None => "all time".to_string(),
    };
    let scope = query
        .project
        .as_deref()
        .map_or_else(|| "workspace".to_string(), |key| format!("project {key}"));
    format!(
        "Window {} ({from}, priced per {}) · {scope} · total {} · {} billable tokens. The total equals the Analytics Cost card for the same window and scope. Spend is attributed through the current parent edges, so reparenting an issue moves its history.\n",
        query.window.as_str(),
        bucket_name(bucket),
        usd(spend.total_cost_usd),
        tokens(spend.billable_tokens),
    )
}

fn render(query: &SpendQuery, range: TimeRange, bucket: Bucket, spend: &SpendByThread) -> String {
    let root = match &query.thread {
        None => None,
        Some(wanted) => match find_root(spend, wanted) {
            Ok(root) => Some(root),
            Err(error) => return error,
        },
    };
    if query.format == Format::Json {
        let value = match root {
            Some(root) => serde_json::to_string_pretty(root),
            None => serde_json::to_string_pretty(spend),
        };
        return value.unwrap_or_else(|error| format!("Spend could not be serialized: {error}"));
    }
    let mut out = match root {
        Some(root) => format!("# Spend by thread: {}\n\n", root.label),
        None => "# Spend by thread\n\n".to_string(),
    };
    out.push_str(&header(query, range, bucket, spend));
    match root {
        None => {
            if spend.roots.is_empty() {
                out.push_str("\nNo spend in this window.\n");
                return out;
            }
            out.push_str("\n| Thread | Cost | Tokens | Share |\n|---|---|---|---|\n");
            for node in &spend.roots {
                let _ = writeln!(
                    out,
                    "| {} | {} | {} | {} |",
                    node.label,
                    usd(node.cost_usd),
                    tokens(node.billable_tokens),
                    share(node.cost_usd, spend.total_cost_usd)
                );
            }
            out.push_str(
                "\nOne thread's nested tree: `?thread=<name>` (or `unparented`, `other`).\n",
            );
        }
        Some(root) => {
            let _ = writeln!(
                out,
                "\n{} · {} tokens · {} of the total{}\n",
                usd(root.cost_usd),
                tokens(root.billable_tokens),
                share(root.cost_usd, spend.total_cost_usd),
                models_suffix(root),
            );
            if root.children.is_empty() {
                out.push_str("No child blocks: all of it is the root's own spend.\n");
            }
            for child in &root.children {
                tree_lines(child, 0, &mut out);
            }
        }
    }
    out
}

fn models_suffix(node: &SpendNode) -> String {
    if node.models.is_empty() {
        return String::new();
    }
    let top: Vec<String> = node
        .models
        .iter()
        .take(TOP_MODELS)
        .map(|m| format!("{} {}", m.model, usd(m.cost_usd)))
        .collect();
    let more = node.models.len().saturating_sub(TOP_MODELS);
    let more = if more > 0 {
        format!(", +{more} more")
    } else {
        String::new()
    };
    format!(" ({}{more})", top.join(", "))
}

fn tree_lines(node: &SpendNode, depth: usize, out: &mut String) {
    let _ = writeln!(
        out,
        "{}- {} — {} · {} tokens{}",
        "  ".repeat(depth),
        node.label,
        usd(node.cost_usd),
        tokens(node.billable_tokens),
        models_suffix(node)
    );
    for child in &node.children {
        tree_lines(child, depth + 1, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_analytics::ModelSpend;

    fn params(raw: &[(&str, &str)]) -> Vec<QueryParam> {
        raw.iter()
            .map(|(key, value)| QueryParam {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    fn node(
        id: &str,
        kind: SpendNodeKind,
        label: &str,
        key: Option<&str>,
        cost: f64,
        children: Vec<SpendNode>,
    ) -> SpendNode {
        let tokens = (cost * 1_000.0) as i64;
        SpendNode {
            id: id.to_string(),
            kind,
            label: label.to_string(),
            project_key: key.map(str::to_string),
            number: None,
            title: None,
            cost_usd: cost,
            billable_tokens: tokens,
            models: vec![ModelSpend {
                model: "opus".to_string(),
                cost_usd: cost,
                billable_tokens: tokens,
            }],
            children,
        }
    }

    /// `models` in PRJ with #1 → #2 nested; `general` in both PRJ and TWO.
    fn spend() -> SpendByThread {
        let grandchild = node(
            "cairn://p/PRJ/2",
            SpendNodeKind::Issue,
            "#2 Grandchild",
            Some("PRJ"),
            3.0,
            vec![],
        );
        let own = node(
            "cairn://p/PRJ/1#own",
            SpendNodeKind::Own,
            "this issue's runs",
            Some("PRJ"),
            2.0,
            vec![],
        );
        let child = node(
            "cairn://p/PRJ/1",
            SpendNodeKind::Issue,
            "#1 Child",
            Some("PRJ"),
            5.0,
            vec![grandchild, own],
        );
        let session = node(
            "cairn://p/PRJ/models#own",
            SpendNodeKind::Own,
            "thread session",
            Some("PRJ"),
            1.0,
            vec![],
        );
        let roots = vec![
            node(
                "cairn://p/PRJ/models",
                SpendNodeKind::Thread,
                "PRJ/models",
                Some("PRJ"),
                6.0,
                vec![child, session],
            ),
            node(
                "cairn://p/PRJ/general",
                SpendNodeKind::Thread,
                "PRJ/general",
                Some("PRJ"),
                2.0,
                vec![],
            ),
            node(
                "cairn://p/TWO/general",
                SpendNodeKind::Thread,
                "TWO/general",
                Some("TWO"),
                1.5,
                vec![],
            ),
            node("other", SpendNodeKind::Other, "other", None, 0.5, vec![]),
        ];
        SpendByThread {
            total_cost_usd: 10.0,
            billable_tokens: 10_000,
            roots,
        }
    }

    fn rendered(raw: &[(&str, &str)]) -> String {
        let query = parse_query(&params(raw)).expect("parses");
        let (range, bucket) = query.window.scope_at(1_700_000_000);
        render(&query, range, bucket, &spend())
    }

    #[test]
    fn markdown_lists_the_roots_with_cost_tokens_and_share() {
        let out = rendered(&[]);
        assert!(out.starts_with("# Spend by thread\n"), "{out}");
        assert!(
            out.contains("Window 24h (since 2023-11-13 22:00 UTC, priced per hour)"),
            "{out}"
        );
        assert!(out.contains("total $10.00"), "{out}");
        assert!(out.contains("equals the Analytics Cost card"), "{out}");
        assert!(
            out.contains("| PRJ/models | $6.00 | 6.0k | 60.0% |"),
            "{out}"
        );
        assert!(out.contains("| other | $0.50 |"), "{out}");
    }

    #[test]
    fn one_thread_renders_its_nested_tree() {
        let out = rendered(&[("thread", "models"), ("window", "7d")]);
        assert!(out.starts_with("# Spend by thread: PRJ/models\n"), "{out}");
        assert!(out.contains("priced per day"), "{out}");
        assert!(
            out.contains("\n- #1 Child — $5.00 · 5.0k tokens (opus $5.00)\n"),
            "{out}"
        );
        assert!(out.contains("\n  - #2 Grandchild — $3.00"), "{out}");
        assert!(out.contains("\n  - this issue's runs — $2.00"), "{out}");
        assert!(out.contains("\n- thread session — $1.00"), "{out}");
        // A qualified name picks one of a repeated name.
        let two = rendered(&[("thread", "two/general")]);
        assert!(two.starts_with("# Spend by thread: TWO/general\n"), "{two}");
        assert!(rendered(&[("thread", "other")]).starts_with("# Spend by thread: other\n"));
    }

    #[test]
    fn json_serves_the_tree_or_one_root() {
        let all: SpendByThread =
            serde_json::from_str(&rendered(&[("format", "json")])).expect("json tree");
        assert_eq!(all, spend());
        let one: SpendNode =
            serde_json::from_str(&rendered(&[("format", "json"), ("thread", "models")]))
                .expect("json node");
        assert_eq!(one.id, "cairn://p/PRJ/models");
        assert_eq!(one.children[0].children[0].label, "#2 Grandchild");
    }

    #[test]
    fn an_ambiguous_or_unknown_thread_is_refused_with_names() {
        let out = rendered(&[("thread", "general")]);
        assert!(out.contains("ambiguous"), "{out}");
        assert!(
            out.contains("PRJ/general") && out.contains("TWO/general"),
            "{out}"
        );
        let out = rendered(&[("thread", "nope")]);
        assert!(out.contains("No thread 'nope'"), "{out}");
        assert!(out.contains("PRJ/models"), "{out}");
    }

    #[test]
    fn unknown_keys_and_values_are_refused() {
        let error = parse_query(&params(&[("provider", "claude")])).unwrap_err();
        assert!(
            error.contains("Unsupported query parameter 'provider'"),
            "{error}"
        );
        assert!(error.contains("window"), "{error}");
        assert!(parse_query(&params(&[("window", "1y")]))
            .unwrap_err()
            .contains("24h, 7d"));
        assert!(parse_query(&params(&[("by", "model")])).is_err());
        assert!(parse_query(&params(&[("format", "csv")])).is_err());
    }
}
