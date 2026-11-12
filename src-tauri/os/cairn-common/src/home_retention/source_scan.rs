//! The writers' half of the contract, read from their source: every literal a
//! production writer joins onto a Cairn home root names a registered entry.
//!
//! A new directory under the home is born at a `.join("<name>")` on the home,
//! so this is where a writer without a rule is caught, by name, before it
//! ships. Joins through a constant or a computed name are not seen; the
//! production-home fixture and the walk's unclaimed holdings catch those.
//!
//! It also refuses `.join(".cairn")` on the OS home directory: that spelling
//! ignores `CAIRN_HOME` and the dev suffix, so a dev build writes into the
//! production home (`paths::cairn_home` is the one resolver).
//!
//! Test code is skipped (test directories and files, and the body of each
//! `#[cfg(test)] mod`), because a test's home is a tempdir it owns.

use std::path::{Path, PathBuf};

use super::classify;

/// The receivers that are a Cairn home, as the code spells them.
const HOME_ROOTS: &[&str] = &[
    "cairn_home()",
    "get_config_dir()",
    "get_config_dir()?",
    "config_dir",
    "config_dir()",
    "data_dir",
    "executor_home",
    "paths.home()",
];

struct Exemption {
    /// The `src-tauri`-relative file.
    file: &'static str,
    /// A substring of the exempted line.
    needle: &'static str,
    reason: &'static str,
    /// The issue that retires the exemption.
    follow_up: &'static str,
}

/// Bounded, so the exemptions stay a short list of named debts rather than a
/// second registry.
const EXEMPTION_CAP: usize = 20;

const EXEMPTIONS: &[Exemption] = &[
    Exemption {
        file: "cairn-transport/src/state.rs",
        needle: ".join(\".cairn\")",
        reason:
            "the legacy agent residences were only ever under ~/.cairn/worktrees, whatever the mode",
        follow_up: "cairn://p/cairn/7026",
    },
    Exemption {
        file: "os/cairn-config/src/settings.rs",
        needle: "home.join(\".cairn\").join(\"worktrees\")",
        reason: "the {worktrees} template is documented as ~/.cairn/worktrees in both modes",
        follow_up: "cairn://p/cairn/7026",
    },
];

fn src_tauri() -> PathBuf {
    crate::paths::development_checkout_ancestor(|directory| {
        directory.join("os/cairn-common/Cargo.toml").is_file()
    })
    .expect("the scan runs beneath the src-tauri workspace")
}

fn is_test_path(relative: &Path) -> bool {
    relative.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("tests" | "examples" | "benches" | "target")
        )
    }) || relative
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| {
            stem == "tests"
                || stem == "testing"
                || stem == "test_support"
                || stem.ends_with("_tests")
                || stem.starts_with("test_")
        })
}

fn rust_files(root: &Path, directory: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let relative = path.strip_prefix(root).unwrap_or(&path);
        if kind.is_dir() {
            if !matches!(
                entry.file_name().to_str(),
                Some("target" | "node_modules" | ".git")
            ) {
                rust_files(root, &path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") && !is_test_path(relative) {
            out.push(path);
        }
    }
}

/// A source file's lines with each `#[cfg(test)] mod { ... }` body blanked, so
/// line numbers still match the file. Braces are counted naively, which a
/// brace inside a string literal can mislead; the cost is a missed line, never
/// a false refusal of production code outside a test module.
fn production_lines(source: &str) -> Vec<&str> {
    let mut lines = source.lines().collect::<Vec<_>>();
    let mut index = 0;
    while index < lines.len() {
        if lines[index].trim() != "#[cfg(test)]" {
            index += 1;
            continue;
        }
        let Some(module) = (index + 1..lines.len()).find(|&next| {
            let text = lines[next].trim();
            !text.is_empty() && !text.starts_with("#[")
        }) else {
            break;
        };
        let text = lines[module].trim_start();
        let is_inline_module = (text.starts_with("mod ") || text.starts_with("pub(crate) mod "))
            && !text.trim_end().ends_with(';');
        if !is_inline_module {
            index = module;
            continue;
        }
        let mut depth = 0_i64;
        let mut end = module;
        for (at, line) in lines.iter().enumerate().skip(module) {
            depth += line.matches('{').count() as i64 - line.matches('}').count() as i64;
            end = at;
            if depth <= 0 && line.contains('}') {
                break;
            }
        }
        for line in &mut lines[index..=end] {
            *line = "";
        }
        index = end + 1;
    }
    lines
}

fn ends_with_root(receiver: &str) -> bool {
    let receiver = receiver.trim_end();
    HOME_ROOTS.iter().any(|root| {
        receiver.strip_suffix(root).is_some_and(|before| {
            !before
                .chars()
                .last()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    })
}

struct Finding {
    file: String,
    line: usize,
    text: String,
    problem: String,
}

fn scan_file(relative: &str, source: &str, findings: &mut Vec<Finding>) {
    let lines = production_lines(source);
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            continue;
        }
        let mut rest = *line;
        let mut offset = 0;
        while let Some(at) = rest.find(".join(\"") {
            let before = &line[..offset + at];
            let receiver = if before.trim().is_empty() && index > 0 {
                lines[index - 1].trim_end()
            } else {
                before
            };
            let literal_start = offset + at + ".join(\"".len();
            let literal = line[literal_start..].split('"').next().unwrap_or_default();
            if ends_with_root(receiver) {
                let top = literal.split('/').next().unwrap_or_default();
                if classify(top, None).is_none() {
                    findings.push(Finding {
                        file: relative.to_string(),
                        line: index + 1,
                        text: line.trim().to_string(),
                        problem: format!(
                            "`{top}` is joined onto a Cairn home root but no HOME_RETENTION entry claims it"
                        ),
                    });
                }
            }
            if literal == ".cairn" {
                let window = lines[index.saturating_sub(2)..=index].join("\n");
                if window.contains("home_dir()") || window.contains("var(\"HOME\")") {
                    findings.push(Finding {
                        file: relative.to_string(),
                        line: index + 1,
                        text: line.trim().to_string(),
                        problem:
                            "`.join(\".cairn\")` on the OS home ignores CAIRN_HOME and the dev \
                                  suffix; resolve the home with paths::cairn_home()"
                                .to_string(),
                    });
                }
            }
            offset = literal_start;
            rest = &line[offset..];
        }
    }
}

#[test]
fn every_literal_joined_onto_a_home_root_is_registered() {
    assert!(
        EXEMPTIONS.len() <= EXEMPTION_CAP,
        "{} exemptions exceed the cap of {EXEMPTION_CAP}: register the entries or convert the writers",
        EXEMPTIONS.len()
    );
    let issue = regex::Regex::new(r"^cairn://p/[a-z0-9-]+/[0-9]+$").unwrap();
    for exemption in EXEMPTIONS {
        assert!(
            !exemption.reason.is_empty(),
            "{} has no reason",
            exemption.file
        );
        assert!(
            issue.is_match(exemption.follow_up),
            "{}'s exemption names no follow-up issue",
            exemption.file
        );
    }

    let root = src_tauri();
    let mut files = Vec::new();
    rust_files(&root, &root, &mut files);
    assert!(files.len() > 100, "the scan found the workspace's sources");
    let mut findings = Vec::new();
    for path in &files {
        let Ok(source) = std::fs::read_to_string(path) else {
            continue;
        };
        let relative = path
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        scan_file(&relative, &source, &mut findings);
    }

    let mut used = vec![false; EXEMPTIONS.len()];
    let refused = findings
        .into_iter()
        .filter(|finding| {
            match EXEMPTIONS
                .iter()
                .position(|e| e.file == finding.file && finding.text.contains(e.needle))
            {
                Some(at) => {
                    used[at] = true;
                    false
                }
                None => true,
            }
        })
        .map(|finding| {
            format!(
                "{}:{}: {}\n    {}",
                finding.file, finding.line, finding.problem, finding.text
            )
        })
        .collect::<Vec<_>>();
    assert!(
        refused.is_empty(),
        "register each entry in cairn-common/src/home_retention.rs with its writer and rule \
         (Undecided naming an issue when it has none):\n{}",
        refused.join("\n")
    );
    let stale = EXEMPTIONS
        .iter()
        .zip(used)
        .filter(|(_, used)| !used)
        .map(|(e, _)| format!("{} ({})", e.file, e.needle))
        .collect::<Vec<_>>();
    assert!(stale.is_empty(), "exemptions that match nothing: {stale:?}");
}

#[test]
fn the_scan_refuses_an_unregistered_join_and_an_os_home_cairn() {
    let mut findings = Vec::new();
    scan_file(
        "fixture.rs",
        "fn a() { let x = cairn_home().join(\"mystery\"); }\n\
         fn b() { let y = orch.config_dir.join(\"logs/today\"); }\n\
         fn c() {\n    let z = cairn_home()\n        .join(\"unheard-of\");\n}\n\
         fn d() { let w = dirs::home_dir().unwrap().join(\".cairn\"); }\n\
         fn e() { let v = project_config_dir.join(\"anything\"); }\n\
         #[cfg(test)]\nmod tests {\n    fn f() { cairn_home().join(\"in-a-test\"); }\n}\n\
         fn g() { cairn_home().join(\"after-the-tests\"); }\n",
        &mut findings,
    );
    let named = findings
        .iter()
        .map(|finding| {
            (
                finding.line,
                finding.problem.split('`').nth(1).unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        named,
        vec![
            (1, "mystery"),
            (5, "unheard-of"),
            (7, ".join(\".cairn\")"),
            (13, "after-the-tests"),
        ]
    );
}
