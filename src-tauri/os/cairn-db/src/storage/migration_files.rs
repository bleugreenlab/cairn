//! The migrations that follow the cutover, read from their directory.
//!
//! Up to [`CUTOVER`] the two lineages are composed by hand in
//! `storage/migrations.rs` and pinned by its tests. That history is frozen. Every
//! migration after it is a file and nothing else: the file's stem gives its
//! identity, its header gives its lineage, and `build.rs` derives the tail both
//! lineages append from the directory listing. A branch that adds a migration
//! adds a file and touches no shared line, so two branches adding migrations
//! never meet in a registry.
//!
//! A branch names a new migration `NEXT_name.sql` (or `NEXT<k>_name.sql`, when it
//! carries several that must apply in a given order), and the store binds the
//! number when the branch lands on the default branch: it renames the file to
//! the tip's next free number and seals that rename into the landing. Until
//! then the provisional version is the whole stem, which is unique and never a
//! real number.
//!
//! This file is compiled twice: into the crate, where the migration tests read
//! the directory through it, and into `build.rs` by `#[path]`, which writes the
//! derived tail. One grammar, so the tail the build writes and the tail the tests
//! expect cannot disagree. It therefore depends on `std` alone.

use std::collections::BTreeMap;

/// The highest migration number composed by hand. Every numbered file above it,
/// and every `NEXT` file, is derived from the directory instead.
///
/// It never moves again once the derived tail is on the default branch: a file
/// above it with no `-- lineage:` header fails this crate's build, and one that
/// is also registered by hand in `migrations.rs` is refused by
/// `check:migrations`, so the frozen history cannot grow by hand.
pub const CUTOVER: u32 = 288;

/// Digits in a bound migration number.
pub const VERSION_DIGITS: usize = 4;

/// The prefix a provisional stem opens with.
pub const PROVISIONAL_PREFIX: &str = "NEXT";

/// Which lineages a post-cutover migration reaches, from its `-- lineage:`
/// header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileLineage {
    /// Both lineages: a change to a table the private and team databases share.
    Shared,
    /// The private (per-install) lineage only.
    Private,
    /// The team lineage only. Team-only migrations after the cutover live beside
    /// the private ones and share their number space.
    Team,
}

impl FileLineage {
    pub fn reaches_private(self) -> bool {
        matches!(self, Self::Shared | Self::Private)
    }

    pub fn reaches_team(self) -> bool {
        matches!(self, Self::Shared | Self::Team)
    }
}

/// One `RebuildCheck` a `-- rebuild-fk-off:` header declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildSpec {
    Conserved(String),
    Reshaped(String),
}

/// What a migration file's stem says about its place in the lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StemKind {
    /// `NNNN_name`: a number already bound.
    Bound { version: u32, name: String },
    /// `NEXT_name` or `NEXT<k>_name`: a number the landing will bind.
    Provisional { name: String },
}

impl StemKind {
    pub fn name(&self) -> &str {
        match self {
            Self::Bound { name, .. } | Self::Provisional { name } => name,
        }
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.split('_').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

/// Read a migration stem: `NNNN_name`, `NEXT_name`, or `NEXT<k>_name` with one
/// digit `k`. The name is lowercase words joined by single underscores.
pub fn parse_stem(stem: &str) -> Result<StemKind, String> {
    if let Some(rest) = stem.strip_prefix(PROVISIONAL_PREFIX) {
        let rest = match rest.as_bytes() {
            [b'_', ..] => &rest[1..],
            [digit, b'_', ..] if digit.is_ascii_digit() => &rest[2..],
            _ => {
                return Err(format!(
                "`{stem}` opens with NEXT but is not `NEXT_name` or `NEXT<k>_name` (one digit k)"
            ))
            }
        };
        if !valid_name(rest) {
            return Err(format!(
                "`{stem}` must name the migration in lowercase words joined by `_` after its prefix"
            ));
        }
        return Ok(StemKind::Provisional {
            name: rest.to_string(),
        });
    }
    let digits = stem.bytes().take_while(u8::is_ascii_digit).count();
    if digits != VERSION_DIGITS || stem.as_bytes().get(VERSION_DIGITS) != Some(&b'_') {
        return Err(format!(
            "`{stem}` must be NNNN_name (four digits) or NEXT_name, for example NEXT_add_issue_state"
        ));
    }
    let name = &stem[VERSION_DIGITS + 1..];
    if !valid_name(name) {
        return Err(format!(
            "`{stem}` must name the migration in lowercase words joined by `_` after its number"
        ));
    }
    Ok(StemKind::Bound {
        version: stem[..VERSION_DIGITS]
            .parse()
            .map_err(|_| format!("`{stem}` has an unreadable number"))?,
        name: name.to_string(),
    })
}

/// The order the tail applies in, and the order a landing binds provisional
/// stems in: bound numbers ascending, then provisional stems in lexical order.
/// The landing binder (`orchestrator/landing/bind.rs`) numbers a branch's
/// additions by this same rule, so binding never reorders a tail.
pub fn apply_order_key(stem: &str, kind: &StemKind) -> (u8, u32, String) {
    match kind {
        StemKind::Bound { version, .. } => (0, *version, String::new()),
        StemKind::Provisional { .. } => (1, 0, stem.to_string()),
    }
}

/// A migration's headers: its lineage, and the rebuild checks a table-rebuilding
/// migration declares (`None` for a migration applied in the ordinary
/// transaction).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Headers {
    pub lineage: FileLineage,
    pub rebuild_fk_off: Option<Vec<RebuildSpec>>,
}

/// Read the header lines a post-cutover migration opens with.
///
/// The headers are the file's leading lines of the form `-- key: value` with a
/// lowercase key. The first must be `-- lineage: shared|private|team`. The only
/// other key is `-- rebuild-fk-off:`, which declares a table rebuild run with
/// foreign keys off and the checks it must pass: `none`, or a comma-separated
/// list of `Conserved(table)` and `Reshaped(table)`. Anything else in the header
/// block is refused rather than ignored, because a misspelled key would
/// otherwise drop the declaration it carried.
pub fn parse_headers(file: &str, sql: &str) -> Result<Headers, String> {
    let mut lineage = None;
    let mut rebuild = None;
    for (index, line) in sql.lines().enumerate() {
        let Some((key, value)) = header_line(line) else {
            break;
        };
        match (index, key) {
            (0, "lineage") => {
                lineage = Some(match value {
                    "shared" => FileLineage::Shared,
                    "private" => FileLineage::Private,
                    "team" => FileLineage::Team,
                    other => {
                        return Err(format!(
                            "{file}: `-- lineage: {other}` names no lineage; write shared, private, or team"
                        ))
                    }
                })
            }
            (_, "lineage") => {
                return Err(format!(
                    "{file}: `-- lineage:` must be the file's first line, and appear once"
                ))
            }
            (_, "rebuild-fk-off") if rebuild.is_none() => {
                rebuild = Some(parse_rebuild_checks(file, value)?);
            }
            (_, "rebuild-fk-off") => {
                return Err(format!("{file}: `-- rebuild-fk-off:` appears twice"))
            }
            (_, other) => {
                return Err(format!(
                    "{file}: unknown header `-- {other}:`; the headers are `-- lineage:` and `-- rebuild-fk-off:`"
                ))
            }
        }
    }
    let Some(lineage) = lineage else {
        return Err(format!(
            "{file}: a migration after {CUTOVER:04} opens with `-- lineage: shared|private|team`"
        ));
    };
    Ok(Headers {
        lineage,
        rebuild_fk_off: rebuild,
    })
}

fn header_line(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix("-- ")?;
    let (key, value) = rest.split_once(':')?;
    let well_formed = !key.is_empty()
        && key.as_bytes()[0].is_ascii_lowercase()
        && key.bytes().all(|b| b.is_ascii_lowercase() || b == b'-');
    well_formed.then(|| (key, value.trim()))
}

fn parse_rebuild_checks(file: &str, value: &str) -> Result<Vec<RebuildSpec>, String> {
    if value == "none" {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(str::trim)
        .map(|item| {
            let parsed = item
                .strip_suffix(')')
                .and_then(|item| item.split_once('('))
                .filter(|(_, table)| {
                    !table.is_empty()
                        && table
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                });
            match parsed {
                Some(("Conserved", table)) => Ok(RebuildSpec::Conserved(table.to_string())),
                Some(("Reshaped", table)) => Ok(RebuildSpec::Reshaped(table.to_string())),
                _ => Err(format!(
                    "{file}: `-- rebuild-fk-off:` takes `none` or a comma-separated list of \
                     Conserved(table) and Reshaped(table); `{item}` is neither"
                )),
            }
        })
        .collect()
}

/// One derived migration, in apply order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailMigration {
    /// The file's stem, which is its whole identity.
    pub stem: String,
    pub headers: Headers,
}

/// The derived tail: every numbered file above [`CUTOVER`] and every provisional
/// file, in [`apply_order_key`] order, from `(file name, content)` pairs of one
/// directory. Files at or below the cutover are the hand-composed history and
/// are skipped; anything that is not a `.sql` file is ignored.
///
/// Refuses, naming the file: a stem this grammar does not read, a missing or
/// unknown header, a number two files share, and a name two files share (the
/// runner adopts a provisional row by name, so a name must identify one
/// migration).
pub fn derive_tail(files: &[(String, String)]) -> Result<Vec<TailMigration>, String> {
    let mut tail: Vec<((u8, u32, String), TailMigration)> = Vec::new();
    let mut numbers: BTreeMap<u32, &str> = BTreeMap::new();
    let mut names: BTreeMap<String, &str> = BTreeMap::new();
    for (file, content) in files {
        let Some(stem) = file.strip_suffix(".sql") else {
            continue;
        };
        if stem.starts_with('.') {
            continue;
        }
        let kind = parse_stem(stem).map_err(|error| format!("{file}: {error}"))?;
        if let StemKind::Bound { version, .. } = &kind {
            if let Some(other) = numbers.insert(*version, file) {
                return Err(format!(
                    "{other} and {file} both take migration {version:04}. A landing binds each \
                     number once; rename the one this branch added to NEXT_<name>.sql and the \
                     landing will number it"
                ));
            }
            if *version <= CUTOVER {
                continue;
            }
        }
        if let Some(other) = names.insert(kind.name().to_string(), file) {
            return Err(format!(
                "{other} and {file} both name a migration `{}`; a migration's name identifies it",
                kind.name()
            ));
        }
        let headers = parse_headers(file, content)?;
        tail.push((
            apply_order_key(stem, &kind),
            TailMigration {
                stem: stem.to_string(),
                headers,
            },
        ));
    }
    tail.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(tail.into_iter().map(|(_, migration)| migration).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, content: &str) -> (String, String) {
        (name.to_string(), content.to_string())
    }

    #[test]
    fn every_stem_form_is_read_and_anything_else_refused() {
        assert_eq!(
            parse_stem("0900_landing_allocations"),
            Ok(StemKind::Bound {
                version: 900,
                name: "landing_allocations".into()
            })
        );
        assert_eq!(
            parse_stem("NEXT_landing_allocations"),
            Ok(StemKind::Provisional {
                name: "landing_allocations".into()
            })
        );
        assert_eq!(
            parse_stem("NEXT2_second"),
            Ok(StemKind::Provisional {
                name: "second".into()
            })
        );
        for bad in [
            "NEXT",
            "NEXT_",
            "NEXT12_x",
            "NEXTx",
            "next_x",
            "284_x",
            "0284x",
            "0284_X",
            "0284_a__b",
        ] {
            assert!(parse_stem(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_tail_is_numbers_then_provisional_stems_in_lexical_order() {
        let header = "-- lineage: shared\nSELECT 1;\n";
        let tail = derive_tail(&[
            file("NEXT_b.sql", header),
            file("0901_c.sql", header),
            file("0003_frozen.sql", "SELECT 1;"),
            file("NEXT1_a.sql", header),
            file("0900_d.sql", header),
            file("README.md", ""),
        ])
        .unwrap();
        let stems: Vec<&str> = tail.iter().map(|m| m.stem.as_str()).collect();
        assert_eq!(stems, ["0900_d", "0901_c", "NEXT1_a", "NEXT_b"]);
    }

    #[test]
    fn headers_name_the_lineage_and_the_rebuild_checks() {
        assert_eq!(
            parse_headers("f.sql", "-- lineage: team\n-- why this exists\nSELECT 1;"),
            Ok(Headers {
                lineage: FileLineage::Team,
                rebuild_fk_off: None
            })
        );
        assert_eq!(
            parse_headers(
                "f.sql",
                "-- lineage: private\n-- rebuild-fk-off: Conserved(jobs), Reshaped(memories)\nSELECT 1;"
            ),
            Ok(Headers {
                lineage: FileLineage::Private,
                rebuild_fk_off: Some(vec![
                    RebuildSpec::Conserved("jobs".into()),
                    RebuildSpec::Reshaped("memories".into())
                ])
            })
        );
        assert_eq!(
            parse_headers("f.sql", "-- lineage: shared\n-- rebuild-fk-off: none\n")
                .unwrap()
                .rebuild_fk_off,
            Some(Vec::new())
        );
    }

    #[test]
    fn a_missing_or_unknown_header_names_the_file() {
        for (sql, needle) in [
            ("CREATE TABLE t (x);", "opens with `-- lineage:"),
            ("-- lineage: everywhere\n", "names no lineage"),
            (
                "-- lineage: shared\n-- linage: private\n",
                "unknown header `-- linage:`",
            ),
            (
                "-- rebuild-fk-off: none\n-- lineage: shared\n",
                "first line",
            ),
            (
                "-- lineage: shared\n-- rebuild-fk-off: Kept(jobs)\n",
                "Kept(jobs)",
            ),
        ] {
            let error = parse_headers("0284_x.sql", sql).unwrap_err();
            assert!(error.starts_with("0284_x.sql: "), "{error}");
            assert!(error.contains(needle), "{error}");
        }
    }

    #[test]
    fn two_files_on_one_number_or_one_name_are_refused() {
        let header = "-- lineage: shared\n";
        let error =
            derive_tail(&[file("0900_a.sql", header), file("0900_b.sql", header)]).unwrap_err();
        assert!(
            error.contains("both take migration 0900") && error.contains("NEXT_"),
            "{error}"
        );
        let error =
            derive_tail(&[file("0900_a.sql", header), file("NEXT_a.sql", header)]).unwrap_err();
        assert!(error.contains("both name a migration `a`"), "{error}");
        // History below the cutover is not re-read for headers, but still holds
        // its numbers: a post-cutover file cannot reuse one.
        let error = derive_tail(&[
            file("0007_old.sql", "SELECT 1;"),
            file("0007_new.sql", header),
        ])
        .unwrap_err();
        assert!(error.contains("both take migration 0007"), "{error}");
    }
}
