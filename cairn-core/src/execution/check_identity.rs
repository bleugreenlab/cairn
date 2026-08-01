use sha2::{Digest, Sha256};

use crate::config::project_settings::CheckCommand;
use crate::execution::inputs::InputSelector;

pub const CHECK_RESULT_SCHEMA_VERSION: u32 = 1;
pub const CHECK_PARSER_VERSION: u32 = 1;
const CONTENT_IDENTITY_VERSION: &str = "check-content-v1";
const ENVIRONMENT_IDENTITY_VERSION: &str = "check-environment-v1";

/// Runtime declarations used by the Rust self-skip ledger. Their values change
/// whether a self-skip is authorized, so Rust checks include them even when the
/// project does not repeat them in configuration.
pub const RUST_SKIP_LEDGER_ENVIRONMENT: &[&str] =
    &["CAIRN_SYNC_TESTS_OPTIONAL", "CAIRN_TEST_SYNC_URL"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckContentIdentity {
    pub fingerprint: String,
    pub filtered_tree_hash: String,
    pub selector_definition: Vec<String>,
    pub result_schema_version: u32,
    pub parser_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashedEnvironmentVariable {
    pub name: String,
    pub value_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckEnvironmentIdentity {
    pub fingerprint: String,
    pub os: String,
    pub arch: String,
    pub executor_id: Option<String>,
    pub device_id: Option<String>,
    pub capabilities: Vec<String>,
    pub runner_build_id: Option<String>,
    pub variables: Vec<HashedEnvironmentVariable>,
}

fn field(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_be_bytes());
    hasher.update(value.as_bytes());
}

fn option(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            field(hasher, "some");
            field(hasher, value);
        }
        None => field(hasher, "none"),
    }
}

fn strings(hasher: &mut Sha256, values: &[String]) {
    field(hasher, &values.len().to_string());
    for value in values {
        field(hasher, value);
    }
}

fn digest(hasher: Sha256) -> String {
    format!("{:x}", hasher.finalize())
}

pub fn verdict_environment_names(check: &CheckCommand) -> Vec<String> {
    let mut names = check.verdict_environment.clone();
    if is_rust_check(&check.command) {
        names.extend(
            RUST_SKIP_LEDGER_ENVIRONMENT
                .iter()
                .map(|name| (*name).to_string()),
        );
    }
    names.sort();
    names.dedup();
    names
}

fn is_rust_check(command: &str) -> bool {
    command.contains("test:rust") || command.contains("cargo test") || command.contains("nextest")
}

pub(crate) fn content_identity(
    check: &CheckCommand,
    selector: &InputSelector,
    entries: Option<&[(String, String)]>,
    tree_hash: &str,
) -> CheckContentIdentity {
    let filtered_tree_hash = if selector.keys_on_whole_tree() {
        tree_hash.to_string()
    } else {
        entries
            .map(|entries| crate::execution::selection::check_input_hash(entries, selector))
            .unwrap_or_else(|| tree_hash.to_string())
    };
    let mut selector_definition = selector.definition().to_vec();
    selector_definition.sort();

    let mut hasher = Sha256::new();
    field(&mut hasher, CONTENT_IDENTITY_VERSION);
    field(&mut hasher, &filtered_tree_hash);
    field(&mut hasher, &check.command);
    strings(&mut hasher, &selector_definition);
    field(&mut hasher, check.policy.as_str());
    field(&mut hasher, check.when.as_str());
    field(&mut hasher, check.resource_class.as_str());
    option(
        &mut hasher,
        check.timeout.map(|value| value.to_string()).as_deref(),
    );
    if let Some(executor) = check.executor.as_ref() {
        field(&mut hasher, "executor");
        option(&mut hasher, executor.name.as_deref());
        option(&mut hasher, executor.os.as_deref());
        let mut required = executor.required_toolchains.clone();
        required.sort();
        strings(&mut hasher, &required);
    } else {
        field(&mut hasher, "no-executor");
    }
    field(&mut hasher, &CHECK_RESULT_SCHEMA_VERSION.to_string());
    field(&mut hasher, &CHECK_PARSER_VERSION.to_string());

    CheckContentIdentity {
        fingerprint: digest(hasher),
        filtered_tree_hash,
        selector_definition,
        result_schema_version: CHECK_RESULT_SCHEMA_VERSION,
        parser_version: CHECK_PARSER_VERSION,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckEnvironmentInput {
    pub os: String,
    pub arch: String,
    pub executor_id: Option<String>,
    pub device_id: Option<String>,
    pub capabilities: Vec<String>,
    pub runner_build_id: Option<String>,
    pub variable_names: Vec<String>,
}

pub fn environment_identity(
    input: CheckEnvironmentInput,
    read_variable: impl Fn(&str) -> Option<String>,
) -> CheckEnvironmentIdentity {
    let CheckEnvironmentInput {
        os,
        arch,
        executor_id,
        device_id,
        mut capabilities,
        runner_build_id,
        variable_names,
    } = input;

    capabilities.sort();
    capabilities.dedup();
    let mut names = variable_names;
    names.sort();
    names.dedup();
    let variables = names
        .into_iter()
        .map(|name| {
            let mut value = Sha256::new();
            match read_variable(&name) {
                Some(secret) => {
                    field(&mut value, "present");
                    field(&mut value, &secret);
                }
                None => field(&mut value, "missing"),
            }
            HashedEnvironmentVariable {
                name,
                value_hash: digest(value),
            }
        })
        .collect::<Vec<_>>();

    let mut hasher = Sha256::new();
    field(&mut hasher, ENVIRONMENT_IDENTITY_VERSION);
    field(&mut hasher, &os);
    field(&mut hasher, &arch);
    option(&mut hasher, executor_id.as_deref());
    option(&mut hasher, device_id.as_deref());
    strings(&mut hasher, &capabilities);
    option(&mut hasher, runner_build_id.as_deref());
    for variable in &variables {
        field(&mut hasher, &variable.name);
        field(&mut hasher, &variable.value_hash);
    }
    CheckEnvironmentIdentity {
        fingerprint: digest(hasher),
        os,
        arch,
        executor_id,
        device_id,
        capabilities,
        runner_build_id,
        variables,
    }
}

pub fn local_environment_identity(
    capabilities: impl IntoIterator<Item = String>,
    variable_names: impl IntoIterator<Item = String>,
) -> CheckEnvironmentIdentity {
    environment_identity(
        CheckEnvironmentInput {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            executor_id: None,
            device_id: None,
            capabilities: capabilities.into_iter().collect(),
            runner_build_id: cairn_common::build_identity::current_executable_build_id().ok(),
            variable_names: variable_names.into_iter().collect(),
        },
        |name| std::env::var(name).ok(),
    )
}

pub fn combined_result_key(
    content: &CheckContentIdentity,
    environment: &CheckEnvironmentIdentity,
) -> String {
    let mut hasher = Sha256::new();
    field(&mut hasher, "check-result-v6");
    field(&mut hasher, &content.fingerprint);
    field(&mut hasher, &environment.fingerprint);
    digest(hasher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn command(yaml: &str) -> CheckCommand {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn rust_checks_inherit_skip_ledger_environment_and_deduplicate_config() {
        let check = command(
            "command: bun run test:rust {changedFiles}\nverdictEnvironment:\n  - CUSTOM_FLAG\n  - CAIRN_TEST_SYNC_URL\n",
        );
        assert_eq!(
            verdict_environment_names(&check),
            vec![
                "CAIRN_SYNC_TESTS_OPTIONAL",
                "CAIRN_TEST_SYNC_URL",
                "CUSTOM_FLAG"
            ]
        );
        let non_rust = command("command: bunx tsc --noEmit\n");
        assert!(verdict_environment_names(&non_rust).is_empty());
    }

    #[test]
    fn environment_identity_sorts_capabilities_and_hashes_values() {
        let secrets = HashMap::from([("TOKEN", "secret"), ("MODE", "strict")]);
        let identity = environment_identity(
            CheckEnvironmentInput {
                os: "linux".into(),
                arch: "x86_64".into(),
                executor_id: Some("executor".into()),
                device_id: Some("device".into()),
                capabilities: vec!["rust=1.80".into(), "bun=1.2".into(), "rust=1.80".into()],
                runner_build_id: Some("build-1".into()),
                variable_names: vec!["TOKEN".into(), "MODE".into()],
            },
            |name| secrets.get(name).map(|value| (*value).to_string()),
        );
        assert_eq!(identity.capabilities, vec!["bun=1.2", "rust=1.80"]);
        assert_eq!(
            identity
                .variables
                .iter()
                .map(|v| v.name.as_str())
                .collect::<Vec<_>>(),
            vec!["MODE", "TOKEN"]
        );
        assert!(!format!("{identity:?}").contains("secret"));
        assert!(!format!("{identity:?}").contains("strict"));
    }

    #[test]
    fn environment_identity_changes_for_missing_and_changed_values() {
        let make = |value: Option<&str>| {
            environment_identity(
                CheckEnvironmentInput {
                    os: "macos".into(),
                    arch: "aarch64".into(),
                    executor_id: None,
                    device_id: None,
                    capabilities: Vec::new(),
                    runner_build_id: None,
                    variable_names: vec!["FLAG".into()],
                },
                |_| value.map(str::to_string),
            )
        };
        assert_ne!(make(None).fingerprint, make(Some("")).fingerprint);
        assert_ne!(make(Some("a")).fingerprint, make(Some("b")).fingerprint);
    }
}
