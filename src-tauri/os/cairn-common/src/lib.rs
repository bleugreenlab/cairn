pub mod abnormal_exit;
pub mod approvals;
pub mod archive;
pub mod auth;
pub mod authorization;
pub mod build_identity;
pub mod cache_distance;
pub mod callback_contract;
pub mod change_validation;
pub mod companion_protocol;
pub mod conflict_scaffolding;
pub mod contract;
pub mod cow;
pub mod desktop_protocol;
pub mod dev_instance_protocol;
pub mod executor_generation;
pub mod executor_paths;
pub mod executor_protocol;
pub mod executor_protocol_ledger;
pub mod git_credential;
pub mod headlines;
pub mod home_retention;
pub mod identity;
pub mod ids;
pub mod imports;
pub mod install_staging;
pub mod kernel_lock;
#[cfg(unix)]
pub mod lineage;
pub mod link_transport;
pub mod lock_proof;
pub mod logging;
pub mod maintenance;
pub mod memory;
pub mod operator_stop;
pub mod paths;
pub mod pipe_health;
pub mod process_identity;
pub mod protocol;
pub mod query;
pub mod read;
pub mod references;
pub mod requests;
pub mod retired_home;
pub mod run_contract;
pub mod run_grammar;
pub mod runtime;
pub mod scratch;
pub mod security;
pub mod service;
pub mod skip_ledger;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_seal;
pub mod token_meter;
pub mod toolchain_path;
pub mod toolchain_requirements;
pub mod uri_refusal;
pub mod volume;
pub mod wire_vocabulary;
pub mod write_contract;

/// The base modules of this crate do not name the executor wire.
///
/// The wire set below depends on the base modules, so a base module that
/// names one of them closes a cycle the crate boundary planned for this split
/// (cairn/7083, slice S5) would refuse to compile. Until that boundary exists,
/// this scan holds it: every `crate::<wire module>` or
/// `cairn_common::<wire module>` in a base module's source, doc comments and
/// tests included, fails here by file and line. The vocabulary both sides
/// speak lives in `wire_vocabulary`, which is a base module.
#[cfg(test)]
mod base_wire_edges {
    const WIRE_MODULES: &[&str] = &[
        "callback_contract",
        "dev_instance_protocol",
        "executor_generation",
        "executor_protocol",
        "executor_protocol_ledger",
        "link_transport",
        "toolchain_requirements",
    ];

    fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    fn names_wire_module(text: &str, prefix: &str) -> Option<&'static str> {
        WIRE_MODULES.iter().copied().find(|module| {
            let needle = format!("{prefix}{module}");
            text.match_indices(&needle).any(|(at, _)| {
                let before = text[..at].chars().next_back();
                let after = text[at + needle.len()..].chars().next();
                !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
                    && !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
            })
        })
    }

    #[test]
    fn base_modules_do_not_name_the_wire() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        rust_files(&src, &mut files);
        files.sort();
        let mut edges = Vec::new();
        for path in &files {
            let relative = path.strip_prefix(&src).unwrap();
            let first = relative.components().next().unwrap().as_os_str();
            let module = first.to_str().unwrap().trim_end_matches(".rs");
            if module == "lib" || WIRE_MODULES.contains(&module) {
                continue;
            }
            let source = std::fs::read_to_string(path).unwrap();
            // A `use crate::{a, b::c}` group names its modules without the prefix.
            let mut in_group = false;
            for (number, line) in source.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.contains("crate::{") || trimmed.contains("cairn_common::{") {
                    in_group = true;
                }
                let named = names_wire_module(line, "crate::")
                    .or_else(|| names_wire_module(line, "cairn_common::"))
                    .or_else(|| in_group.then(|| names_wire_module(line, "")).flatten());
                if let Some(wire) = named {
                    edges.push(format!(
                        "src/{}:{}: names {wire}: {}",
                        relative.display(),
                        number + 1,
                        line.trim()
                    ));
                }
                if in_group && line.contains(';') {
                    in_group = false;
                }
            }
        }
        assert!(
            edges.is_empty(),
            "base modules name the executor wire; import the vocabulary from \
             crate::wire_vocabulary instead:\n{}",
            edges.join("\n")
        );
    }
}
