use sha2::{Digest, Sha256};

const ENVIRONMENT_IDENTITY_VERSION: &str = "check-environment-v1";
const VERDICT_ENVIRONMENT_IDENTITY_VERSION: &str = "verdict-environment-v1";

/// Source-sensitive identity embedded by `build.rs`, deliberately equal in every
/// runner and executor linked against the same workspace source. The build fails
/// rather than falling back to package semver when this identity cannot be made.
pub const fn implementation_identity() -> &'static str {
    env!("CAIRN_CHECK_IMPLEMENTATION_ID")
}

/// Hash only the environment variables a check declares as verdict inputs.
/// Platform, architecture, toolchains, and build identity are deliberately not
/// part of this value; they are recorded as separate execution facts.
pub fn verdict_environment_hash(
    variable_names: impl IntoIterator<Item = String>,
    read_variable: impl Fn(&str) -> Option<String>,
) -> String {
    let mut names = variable_names.into_iter().collect::<Vec<_>>();
    names.sort();
    names.dedup();

    let mut hasher = Sha256::new();
    field(&mut hasher, VERDICT_ENVIRONMENT_IDENTITY_VERSION);
    for name in names {
        field(&mut hasher, &name);
        match read_variable(&name) {
            Some(value) => {
                field(&mut hasher, "present");
                field(&mut hasher, &value);
            }
            None => field(&mut hasher, "missing"),
        }
    }
    digest(hasher)
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

pub fn fingerprint(
    os: &str,
    arch: &str,
    capabilities: impl IntoIterator<Item = String>,
    runner_build_id: Option<&str>,
    variable_names: impl IntoIterator<Item = String>,
    read_variable: impl Fn(&str) -> Option<String>,
) -> String {
    let mut capabilities = capabilities.into_iter().collect::<Vec<_>>();
    capabilities.sort();
    capabilities.dedup();
    let mut names = variable_names.into_iter().collect::<Vec<_>>();
    names.sort();
    names.dedup();

    let mut hasher = Sha256::new();
    field(&mut hasher, ENVIRONMENT_IDENTITY_VERSION);
    field(&mut hasher, os);
    field(&mut hasher, arch);
    option(&mut hasher, None);
    option(&mut hasher, None);
    strings(&mut hasher, &capabilities);
    option(&mut hasher, runner_build_id);
    for name in names {
        let mut value = Sha256::new();
        match read_variable(&name) {
            Some(secret) => {
                field(&mut value, "present");
                field(&mut value, &secret);
            }
            None => field(&mut value, "missing"),
        }
        field(&mut hasher, &name);
        field(&mut hasher, &digest(value));
    }
    digest(hasher)
}

pub fn toolchain_identity() -> &'static str {
    static IDENTITY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    IDENTITY
        .get_or_init(|| {
            fn version(program: &str, args: &[&str]) -> String {
                let path = crate::toolchain_path::agent_shell_path();
                let resolved = crate::toolchain_path::locate_program(program);
                match std::process::Command::new(program)
                    .args(args)
                    .env("PATH", &path)
                    .output()
                {
                    Ok(output) if output.status.success() => {
                        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
                        if value.is_empty() {
                            tracing::warn!(program, resolved = ?resolved, "toolchain probe returned empty output");
                            "unavailable".to_string()
                        } else {
                            value
                        }
                    }
                    Ok(output) => {
                        tracing::warn!(program, resolved = ?resolved, status = %output.status, stderr = %String::from_utf8_lossy(&output.stderr).trim(), "toolchain probe failed");
                        "unavailable".to_string()
                    }
                    Err(error) => {
                        tracing::warn!(program, resolved = ?resolved, %error, "toolchain probe could not start");
                        "unavailable".to_string()
                    }
                }
            }
            format!(
                "rustc={};bun={}",
                version("rustc", &["--version", "--verbose"]),
                version("bun", &["--version"])
            )
        })
        .as_str()
}

pub fn local_fingerprint(variable_names: impl IntoIterator<Item = String>) -> String {
    fingerprint(
        std::env::consts::OS,
        std::env::consts::ARCH,
        [toolchain_identity().to_string()],
        Some(implementation_identity()),
        variable_names,
        |name| std::env::var(name).ok(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_build_identity_participates_in_the_fingerprint() {
        let compose = |build: &str| {
            fingerprint(
                "linux",
                "x86_64",
                ["rustc=example;bun=example".to_string()],
                Some(build),
                Vec::<String>::new(),
                |_| None,
            )
        };
        assert_ne!(compose("source-v1:first"), compose("source-v1:second"));
    }

    #[test]
    fn verdict_environment_hash_is_order_independent_and_declared_only() {
        let read = |name: &str| Some(format!("value-{name}"));
        let first = verdict_environment_hash(["B".into(), "A".into(), "A".into()], read);
        let second = verdict_environment_hash(["A".into(), "B".into()], read);
        assert_eq!(first, second);
        assert_ne!(first, verdict_environment_hash(["A".into()], read));
    }

    #[test]
    fn verdict_environment_hash_distinguishes_missing_and_present_empty() {
        let missing = verdict_environment_hash(["A".into()], |_| None);
        let empty = verdict_environment_hash(["A".into()], |_| Some(String::new()));
        assert_ne!(missing, empty);
    }
}
