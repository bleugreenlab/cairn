//! How a library collection and a library item read: every row the resolver
//! lists, with the layer it resolves from (`@`, `~`, `pack:<id>`) and what it
//! shadows, and the project definitions that are not in force with why.
//!
//! One rendering for every kind, so `cairn://agents`, `cairn://recipes` and
//! their siblings say the same thing the same way.

use cairn_config::library::{self, LibraryKind, Listed, Origin};

use crate::config::ConfigResult;

/// The addresses a kind's items are read at.
pub(crate) struct Links {
    pub bare: fn(&str) -> String,
    pub project: fn(&str, &str) -> String,
}

/// An item's address in the layer it came from: the project's address for a
/// project definition, the pack-pinned address for a pack's, the bare address
/// otherwise.
pub(crate) fn item_link(
    kind: LibraryKind,
    id: &str,
    origin: &Origin,
    project_key: Option<&str>,
    links: &Links,
) -> String {
    match origin {
        Origin::Project { .. } => match project_key {
            Some(project) => (links.project)(project, id),
            None => (links.bare)(id),
        },
        Origin::Pack { id: pack } => library::pack_item_uri(pack, kind, id),
        Origin::Workspace => (links.bare)(id),
    }
}

fn row_line<T>(
    kind: LibraryKind,
    row: &Listed<T>,
    project_key: Option<&str>,
    links: &Links,
    describe: &impl Fn(&T) -> String,
) -> String {
    let description = match &row.entry {
        ConfigResult::Ok(item) => describe(item),
        ConfigResult::Err { error, .. } => format!("unreadable: {error}"),
    };
    format!(
        "- [{}]({}) [{}] — {}\n",
        row.name,
        item_link(kind, &row.name, &row.origin, project_key, links),
        row.origin_label(),
        description,
    )
}

/// A collection: the rows in force, then the rows that are not (a skipped
/// project definition, a name two packs collide on) with the operator's path.
/// `project_only` keeps only project definitions, for an explicit
/// `cairn://p/<key>/<kind>` read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn render_collection<T>(
    heading: &str,
    noun: &str,
    kind: LibraryKind,
    rows: Vec<Listed<T>>,
    project_key: Option<&str>,
    project_only: bool,
    links: &Links,
    describe: impl Fn(&T) -> String,
) -> String {
    let rows: Vec<Listed<T>> = rows
        .into_iter()
        .filter(|row| !project_only || row.origin.is_project())
        .collect();
    let (in_force, skipped): (Vec<_>, Vec<_>) =
        rows.into_iter().partition(|row| row.status.is_active());

    let mut out = match project_key {
        Some(key) => format!("# {heading} — {key} context\n\n"),
        None => format!("# {heading} — workspace\n\n"),
    };
    out.push_str(&format!("{} {noun}(s)\n\n", in_force.len()));
    if in_force.is_empty() {
        out.push_str(&format!("No {noun}s found.\n\n"));
    } else {
        for row in &in_force {
            out.push_str(&row_line(kind, row, project_key, links, &describe));
        }
        out.push('\n');
    }
    if !skipped.is_empty() {
        out.push_str("## Not in force\n\n");
        for row in &skipped {
            out.push_str(&row_line(kind, row, project_key, links, &describe));
        }
        out.push_str(&format!(
            "\nA project definition takes effect once the operator trusts the project and confirms its authority in {}. A name two packs define is refused until a local definition stands in front or one pack's item is removed.\n\n",
            library::TRUST_SETTINGS_PLACE
        ));
    }
    out
}

/// The `Origin:` and `Shadows:` lines of an item read.
pub(crate) fn origin_lines(
    kind: LibraryKind,
    id: &str,
    origin: &Origin,
    shadows: &[Origin],
) -> String {
    let mut out = format!("Origin: {}\n", origin.label());
    if !shadows.is_empty() {
        let shadows: Vec<String> = shadows
            .iter()
            .map(|origin| match origin {
                Origin::Pack { id: pack } => format!(
                    "{} ({})",
                    origin.label(),
                    library::pack_item_uri(pack, kind, id)
                ),
                _ => origin.label(),
            })
            .collect();
        out.push_str(&format!("Shadows: {}\n", shadows.join(", ")));
    }
    out
}
