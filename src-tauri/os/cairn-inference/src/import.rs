//! Importing a published serving profile: compiling one service of a compose
//! file, pinned at a commit, into a container profile's engine.
//!
//! The importer is a parser, not a guesser. What it can read from the files it
//! reads; what it cannot (which values a machine needs, which variable holds
//! the port, where a host path lives) it refuses by name, listing every
//! missing key with what it means, so whoever imports (an agent, usually)
//! supplies them in the [`ImportSpec`] and tries again. Verification is the
//! gate on whether the result runs.
//!
//! Compose variables are interpolated as Compose does (`${V}`, `${V:-d}`,
//! `${V-d}`, `${V:+a}`, `${V+a}`, `${V:?e}`, `$$`), taking a value from the
//! spec's `env` first, then the env files (a later file over an earlier one),
//! then the compose default. A variable a spec rank sets is a rank variable: it
//! differs per machine, so it is never fixed at import. In the command it is
//! left for the container's own shell to expand (the command must be a shell's
//! for that, which a multi-machine compose command is), and in the environment
//! it lands in each rank's env.
//!
//! Some compose keys describe what Cairn decides itself and are dropped with
//! the reason said (`restart`: what runs after a reboot is placement's call;
//! `healthcheck`: Cairn checks health; `pull_policy`: the image is pinned by
//! digest; `stop_grace_period` and `container_name`: Cairn names and removes its
//! containers). Any other key it does not translate is refused, named.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::profile::{
    is_commit_sha, is_pinned_image, Accelerator, AcceleratorFamily, AcceleratorRuntime, Container,
    HostOptions, Mount, MountSource, Server, SourcePin,
};

/// What an import names: the compose file and service, and every value the
/// files cannot supply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportSpec {
    /// The repository (`https://github.com/<owner>/<repo>`).
    pub url: String,
    /// A ref the host resolves to a full commit sha before compiling.
    pub commit: String,
    /// The compose file's path in the repository.
    pub compose: String,
    pub service: String,
    /// Env files in the repository, read in order; a later one wins.
    #[serde(default)]
    pub env_files: Vec<String>,
    /// Values over everything the files say. May hold placeholders
    /// (`{port}`, `{head.link}`, ...).
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// One env map per machine, the head first, giving what differs per
    /// machine. Absent for a single-machine service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranks: Option<Vec<BTreeMap<String, String>>>,
    /// What a mount at a container path takes from the host, for a mount
    /// whose host side is an absolute path.
    #[serde(default)]
    pub host_paths: BTreeMap<String, HostPath>,
    /// The registered model the profile serves.
    pub model: String,
    /// The repository the weights come from, when not the model's own (a
    /// quantized derivative).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weights: Option<String>,
    /// The image pinned by digest, when its registry cannot be asked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// The profile id; derived from the model, quantization, server and
    /// machine count when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// What a host path at a container path becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HostPath {
    /// The machine's Hugging Face cache.
    HfCache,
    /// No mount: the container keeps its own path.
    Drop,
}

/// One value the import needs and the files do not give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Missing {
    /// Where in the spec it goes (`ranks`, `env.VLLM_PORT`, `hostPaths./tmp`).
    pub key: String,
    pub meaning: String,
}

/// Why an import did not compile: every missing key, and every refusal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportError {
    pub missing: Vec<Missing>,
    pub refused: Vec<String>,
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.missing.is_empty() {
            writeln!(f, "The import needs these keys in its spec:")?;
            for missing in &self.missing {
                writeln!(f, "- {}: {}", missing.key, missing.meaning)?;
            }
        }
        if !self.refused.is_empty() {
            writeln!(f, "Refused:")?;
            for refused in &self.refused {
                writeln!(f, "- {refused}")?;
            }
        }
        Ok(())
    }
}

/// The files an import read, by path in the repository.
#[derive(Debug, Clone, Default)]
pub struct Files {
    pub compose: String,
    /// Each env file's text, in the spec's order.
    pub env_files: Vec<(String, String)>,
}

/// A compiled service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled {
    pub container: Container,
    pub machines: u32,
    pub accelerator: Option<Accelerator>,
    /// An image named by tag, which the host resolves to its digest and
    /// writes into `container.image`.
    pub unpinned_image: Option<String>,
    /// What was dropped from the compose file, and why.
    pub notes: Vec<String>,
}

const DROPPED: &[(&str, &str)] = &[
    ("restart", "Cairn runs its containers with --restart no: what runs after a reboot is placement's decision"),
    ("healthcheck", "Cairn checks the deployment's health URL itself"),
    ("pull_policy", "the image is pinned by digest"),
    ("stop_grace_period", "Cairn stops a deployment by removing its containers"),
    ("container_name", "Cairn names its containers cairn-<deployment>-r<rank>"),
];

const TRANSLATED: &[&str] = &[
    "image",
    "entrypoint",
    "command",
    "environment",
    "volumes",
    "devices",
    "gpus",
    "deploy",
    "ipc",
    "network_mode",
    "shm_size",
    "ports",
    "ulimits",
];

/// Parse a dotenv file: `KEY=VALUE` lines, `#` comments, an optional
/// `export`, and one level of quotes.
pub fn parse_env_file(text: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim();
        let value = match (value.chars().next(), value.chars().last()) {
            (Some(q @ ('"' | '\'')), Some(end)) if value.len() >= 2 && q == end => {
                &value[1..value.len() - 1]
            }
            _ => value.split(" #").next().unwrap_or(value).trim_end(),
        };
        values.insert(key.trim().to_string(), value.to_string());
    }
    values
}

/// Where interpolation looks a variable up, and what it leaves alone.
struct Scope<'a> {
    values: &'a dyn Fn(&str) -> Option<String>,
    /// Rank variables: left as `${...}` for the container's shell.
    keep: Option<&'a BTreeSet<String>>,
}

/// Every variable interpolation read that had neither a value nor a default.
#[derive(Default)]
struct Unset(BTreeSet<String>);

fn is_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_name(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The end (exclusive) of a `${...}` body starting after its `{`, honouring
/// nested `${...}`.
fn closing(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 1;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'$' if bytes.get(i + 1) == Some(&b'{') => {
                depth += 1;
                i += 2;
                continue;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn interpolate(text: &str, scope: &Scope<'_>, unset: &mut Unset) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        if let Some(tail) = after.strip_prefix('$') {
            out.push('$');
            rest = tail;
            continue;
        }
        if let Some(body_and_rest) = after.strip_prefix('{') {
            let end =
                closing(body_and_rest).ok_or_else(|| format!("an unclosed ${{ in {text:?}"))?;
            let body = &body_and_rest[..end];
            out.push_str(&expand(body, scope, unset)?);
            rest = &body_and_rest[end + 1..];
            continue;
        }
        if after.starts_with(is_name_start) {
            let len = after.find(|c| !is_name(c)).unwrap_or(after.len());
            out.push_str(&expand(&after[..len], scope, unset)?);
            rest = &after[len..];
            continue;
        }
        out.push('$');
        rest = after;
    }
    out.push_str(rest);
    Ok(out)
}

fn expand(body: &str, scope: &Scope<'_>, unset: &mut Unset) -> Result<String, String> {
    let len = body.find(|c| !is_name(c)).unwrap_or(body.len());
    let (name, op) = body.split_at(len);
    if name.is_empty() || !name.starts_with(is_name_start) {
        return Err(format!(
            "${{{body}}} is not a variable Compose can interpolate"
        ));
    }
    if scope.keep.is_some_and(|keep| keep.contains(name)) {
        return Ok(format!("${{{body}}}"));
    }
    let value = (scope.values)(name);
    let (operator, word) = [":-", ":+", ":?", "-", "+", "?"]
        .iter()
        .find_map(|operator| op.strip_prefix(operator).map(|word| (*operator, word)))
        .unwrap_or(("", op));
    if operator.is_empty() && !op.is_empty() {
        return Err(format!(
            "${{{body}}} uses an operator Compose does not interpolate"
        ));
    }
    let set = value.is_some();
    let nonempty = value.as_deref().is_some_and(|v| !v.is_empty());
    let word = |unset: &mut Unset| interpolate(word, scope, unset);
    Ok(match operator {
        "" => match value {
            Some(value) => value,
            None => {
                unset.0.insert(name.to_string());
                String::new()
            }
        },
        ":-" => {
            if nonempty {
                value.unwrap()
            } else {
                word(unset)?
            }
        }
        "-" => {
            if set {
                value.unwrap()
            } else {
                word(unset)?
            }
        }
        ":+" => {
            if nonempty {
                word(unset)?
            } else {
                String::new()
            }
        }
        "+" => {
            if set {
                word(unset)?
            } else {
                String::new()
            }
        }
        ":?" | "?" => {
            let ok = if operator == ":?" { nonempty } else { set };
            if ok {
                value.unwrap()
            } else {
                unset.0.insert(name.to_string());
                String::new()
            }
        }
        _ => unreachable!(),
    })
}

/// Every variable `text` reads, at any depth.
fn variables(text: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut rest = text;
    while let Some(at) = rest.find('$') {
        let after = &rest[at + 1..];
        if let Some(tail) = after.strip_prefix('$') {
            rest = tail;
            continue;
        }
        let after = after.strip_prefix('{').unwrap_or(after);
        let len = after.find(|c| !is_name(c)).unwrap_or(after.len());
        if len > 0 && after.starts_with(is_name_start) {
            found.insert(after[..len].to_string());
        }
        rest = &after[len..];
    }
    found
}

/// Split a command string as Compose does: words, with quotes.
fn shell_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut quote: Option<char> = None;
    for c in text.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => word.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                in_word = true;
            }
            (None, c) if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            (None, c) => {
                word.push(c);
                in_word = true;
            }
        }
    }
    if in_word {
        words.push(word);
    }
    words
}

fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

fn words_of(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Sequence(items) => items.iter().map(scalar).collect(),
        Value::String(text) => Some(shell_words(text)),
        Value::Null => Some(Vec::new()),
        _ => None,
    }
}

/// The variable a flag's value comes from in the compose text (`--port
/// ${VLLM_PORT:-8888}` reads `VLLM_PORT`), or the literal it is fixed at.
enum FlagSource {
    Variable(String),
    Literal(String),
}

fn flag_source(raw: &str, flag: &str) -> Option<FlagSource> {
    let mut rest = raw;
    while let Some(at) = rest.find(flag) {
        let after = &rest[at + flag.len()..];
        rest = after;
        let Some(value) = after.strip_prefix([' ', '=', '\t', '\n']) else {
            continue;
        };
        let value = value.trim_start();
        if let Some(var) = value.strip_prefix("${").or_else(|| value.strip_prefix('$')) {
            let len = var.find(|c| !is_name(c)).unwrap_or(var.len());
            if len > 0 {
                return Some(FlagSource::Variable(var[..len].to_string()));
            }
        }
        let literal: String = value.chars().take_while(|c| !c.is_whitespace()).collect();
        return Some(FlagSource::Literal(literal));
    }
    None
}

/// What a named compiled argument must carry, and how the spec supplies it.
struct Binding {
    flag: &'static str,
    placeholder: &'static str,
    meaning: &'static str,
}

const BINDINGS: &[Binding] = &[
    Binding {
        flag: "--port",
        placeholder: "{port}",
        meaning: "the API port, which Cairn assigns per deployment",
    },
    Binding {
        flag: "--served-model-name",
        placeholder: "{servedName}",
        meaning: "the served name: Cairn serves every model under its real name",
    },
    Binding {
        flag: "--alias",
        placeholder: "{servedName}",
        meaning: "the served name: Cairn serves every model under its real name",
    },
    Binding {
        flag: "vllm serve",
        placeholder: "{model}",
        meaning: "the weights' repository, which the profile pins",
    },
];

/// Compile `spec`'s service from `files`.
pub fn compile(spec: &ImportSpec, files: &Files) -> Result<Compiled, ImportError> {
    let mut error = ImportError::default();
    let missing = |error: &mut ImportError, key: String, meaning: &str| {
        if !error.missing.iter().any(|m| m.key == key) {
            error.missing.push(Missing {
                key,
                meaning: meaning.to_string(),
            });
        }
    };
    if !is_commit_sha(&spec.commit) {
        error
            .refused
            .push(format!("commit {:?} is not a full 40-hex sha", spec.commit));
    }

    let document: Value = serde_yaml::from_str(&files.compose).map_err(|e| ImportError {
        refused: vec![format!("{} does not parse as YAML: {e}", spec.compose)],
        ..ImportError::default()
    })?;
    let services = document.get("services").and_then(Value::as_mapping);
    let Some(service) = services.and_then(|s| s.get(spec.service.as_str())) else {
        let names: Vec<String> = services
            .into_iter()
            .flat_map(|s| s.keys().filter_map(scalar))
            .collect();
        error.refused.push(format!(
            "{} has no service {}; it has {}",
            spec.compose,
            spec.service,
            names.join(", ")
        ));
        return Err(error);
    };
    let Some(service) = service.as_mapping() else {
        error
            .refused
            .push(format!("service {} is not a mapping", spec.service));
        return Err(error);
    };

    let mut notes = Vec::new();
    for key in service.keys().filter_map(scalar) {
        if let Some((_, why)) = DROPPED.iter().find(|(name, _)| *name == key) {
            notes.push(format!("dropped `{key}`: {why}"));
        } else if !TRANSLATED.contains(&key.as_str()) {
            error.refused.push(format!(
                "service key `{key}` has no translation into a Cairn profile"
            ));
        }
    }
    let get = |key: &str| service.get(key);

    let mut file_values: BTreeMap<String, String> = BTreeMap::new();
    for (_, text) in &files.env_files {
        file_values.extend(parse_env_file(text));
    }
    let base = |name: &str| -> Option<String> {
        spec.env
            .get(name)
            .or_else(|| file_values.get(name))
            .cloned()
    };
    let ranks: Vec<BTreeMap<String, String>> = spec.ranks.clone().unwrap_or_default();
    let rank_vars: BTreeSet<String> = ranks.iter().flat_map(|r| r.keys().cloned()).collect();
    let machines = ranks.len().max(1);

    let raw_command: String = ["entrypoint", "command"]
        .iter()
        .filter_map(|key| get(key).and_then(words_of))
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    let raw_vars = variables(&raw_command);
    let spans_machines =
        raw_command.contains("--nnodes") || raw_vars.iter().any(|v| v.contains("RANK"));
    if spans_machines && spec.ranks.is_none() {
        missing(
            &mut error,
            "ranks".into(),
            &format!(
                "one env map per machine, the head first, with the variables that differ per machine ({}); the command spans machines",
                raw_vars
                    .iter()
                    .filter(|v| v.contains("RANK") || v.contains("HEADLESS") || v.contains("HOST_IP"))
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }

    let mut unset = Unset::default();
    let resolve = |text: &str, unset: &mut Unset| -> Result<String, String> {
        interpolate(
            text,
            &Scope {
                values: &base,
                keep: None,
            },
            unset,
        )
    };

    // The image.
    let mut unpinned_image = None;
    let image = match (&spec.image, get("image").and_then(scalar)) {
        (Some(image), _) => {
            if !is_pinned_image(image) {
                error
                    .refused
                    .push(format!("spec image {image} is not pinned by digest"));
            }
            image.clone()
        }
        (None, Some(text)) => match resolve(&text, &mut unset) {
            Ok(image) if is_pinned_image(&image) => image,
            Ok(image) => {
                unpinned_image = Some(image.clone());
                image
            }
            Err(why) => {
                error.refused.push(format!("image: {why}"));
                String::new()
            }
        },
        (None, None) => {
            missing(
                &mut error,
                "image".into(),
                "the container image pinned by digest; the service names none",
            );
            String::new()
        }
    };

    // Entrypoint and command, rank variables left for the container's shell.
    let in_shell = |words: &[String]| {
        words
            .first()
            .is_some_and(|w| matches!(w.rsplit('/').next(), Some("bash" | "sh")))
            && words
                .get(1)
                .is_some_and(|flag| flag.starts_with('-') && flag.contains('c'))
    };
    let entrypoint_words = get("entrypoint").map(words_of);
    let command_words = get("command").map(words_of);
    let words_ok =
        |what: &str, words: Option<Option<Vec<String>>>, error: &mut ImportError| match words {
            None => None,
            Some(None) => {
                error
                    .refused
                    .push(format!("`{what}` is neither a list nor a string"));
                None
            }
            Some(Some(words)) => Some(words),
        };
    let entrypoint_words = words_ok("entrypoint", entrypoint_words, &mut error);
    let command_words = words_ok("command", command_words, &mut error);
    let shell = in_shell(entrypoint_words.as_deref().unwrap_or_default())
        || in_shell(command_words.as_deref().unwrap_or_default());
    let keep = (!rank_vars.is_empty()).then_some(&rank_vars);
    let compile_words = |words: Option<Vec<String>>, unset: &mut Unset, error: &mut ImportError| {
        words.map(|words| {
            words
                .iter()
                .map(|word| {
                    let used_ranks = variables(word).intersection(&rank_vars).count() > 0;
                    if used_ranks && !shell {
                        error.refused.push(format!(
                            "the command uses a variable that differs per machine outside a shell (`{word}`), so one command cannot serve every machine"
                        ));
                    }
                    interpolate(
                        word,
                        &Scope {
                            values: &base,
                            keep,
                        },
                        unset,
                    )
                    .unwrap_or_else(|why| {
                        error.refused.push(why);
                        String::new()
                    })
                })
                .collect::<Vec<_>>()
        })
    };
    let entrypoint = compile_words(entrypoint_words, &mut unset, &mut error);
    let args = compile_words(command_words, &mut unset, &mut error).unwrap_or_default();

    // The environment, per rank where it differs.
    let mut env = BTreeMap::new();
    let mut rank_env: Vec<BTreeMap<String, String>> = ranks.clone();
    let environment: Vec<(String, String)> = match get("environment") {
        None => Vec::new(),
        Some(Value::Mapping(map)) => map
            .iter()
            .filter_map(|(k, v)| Some((scalar(k)?, scalar(v)?)))
            .collect(),
        Some(Value::Sequence(items)) => items
            .iter()
            .filter_map(scalar)
            .map(|item| match item.split_once('=') {
                Some((k, v)) => (k.to_string(), v.to_string()),
                None => (item.clone(), format!("${{{item}}}")),
            })
            .collect(),
        Some(_) => {
            error
                .refused
                .push("`environment` is neither a mapping nor a list".into());
            Vec::new()
        }
    };
    for (key, value) in environment {
        if rank_vars.contains(&key) && value.trim() == format!("${{{key}}}") {
            continue;
        }
        let per_rank: Vec<String> = (0..machines)
            .map(|rank| {
                let lookup = |name: &str| {
                    ranks
                        .get(rank)
                        .and_then(|r| r.get(name).cloned())
                        .or_else(|| base(name))
                };
                interpolate(
                    &value,
                    &Scope {
                        values: &lookup,
                        keep: None,
                    },
                    &mut unset,
                )
                .unwrap_or_else(|why| {
                    error.refused.push(format!("environment {key}: {why}"));
                    String::new()
                })
            })
            .collect();
        if per_rank.iter().all(|v| *v == per_rank[0]) {
            env.insert(key, per_rank[0].clone());
        } else {
            for (rank, value) in per_rank.into_iter().enumerate() {
                rank_env[rank].insert(key.clone(), value);
            }
        }
    }
    // A variable a spec rank sets but the environment block never passed on
    // is still in the container's env: its shell reads it.
    for (rank, values) in ranks.iter().enumerate() {
        for (key, value) in values {
            rank_env[rank]
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
    if machines == 1 {
        if let Some(only) = rank_env.pop() {
            env.extend(only);
        }
    }

    // Mounts.
    let mut mounts = Vec::new();
    let mut uses_source = false;
    let volumes = get("volumes")
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default();
    for volume in volumes {
        let Some(text) = scalar(&volume) else {
            error
                .refused
                .push("a volume in the long syntax; only `source:target[:mode]` is read".into());
            continue;
        };
        let resolved = match resolve(&text, &mut Unset::default()) {
            Ok(resolved) => resolved,
            Err(why) => {
                error.refused.push(format!("volume {text}: {why}"));
                continue;
            }
        };
        let parts: Vec<&str> = resolved.splitn(3, ':').collect();
        let (source, target, mode) = match parts.as_slice() {
            [source, target] => (*source, *target, ""),
            [source, target, mode] => (*source, *target, *mode),
            _ => {
                error.refused.push(format!("volume {text} names no target"));
                continue;
            }
        };
        let read_only = mode.split(',').any(|m| m == "ro");
        match spec.host_paths.get(target) {
            Some(HostPath::Drop) => {
                notes.push(format!("dropped the mount at {target}, as the spec says"));
                continue;
            }
            Some(HostPath::HfCache) => {
                if !read_only {
                    notes.push(format!(
                        "the compose file mounts the Hugging Face cache at {target} writable; Cairn mounts only the pinned repositories' directories there, read-only, and fetches their files before the container starts"
                    ));
                }
                mounts.push(Mount {
                    from: MountSource::HfCache,
                    to: target.to_string(),
                    read_only: true,
                });
                continue;
            }
            None => {}
        }
        if source.starts_with("./") || source == "." {
            if source.split('/').any(|part| part == "..") {
                error
                    .refused
                    .push(format!("volume {text} reaches outside the repository"));
                continue;
            }
            uses_source = true;
            mounts.push(Mount {
                from: MountSource::Source {
                    path: source.to_string(),
                },
                to: target.to_string(),
                read_only,
            });
        } else if source.contains('/') {
            missing(
                &mut error,
                format!("hostPaths.{target}"),
                &format!(
                    "hfCache or drop: the compose file mounts the host path {source} there, which Cairn cannot know on every machine"
                ),
            );
        } else {
            error.refused.push(format!(
                "volume {text} mounts a named volume, which a profile does not carry"
            ));
        }
    }

    // Host options.
    let mut host = HostOptions::default();
    let mut accelerator = None;
    match get("ipc").and_then(scalar).as_deref() {
        None => {}
        Some("host") => host.ipc_host = true,
        Some(other) => error.refused.push(format!("ipc {other} is not translated")),
    }
    match get("network_mode").and_then(scalar).as_deref() {
        None => {}
        Some("host") => host.network_host = true,
        Some(other) => error
            .refused
            .push(format!("network_mode {other} is not translated")),
    }
    if get("ports").is_some() {
        notes.push(if host.network_host {
            "dropped `ports`: the container shares the host's network".into()
        } else {
            "dropped `ports`: Cairn publishes the deployment's API port".into()
        });
    }
    if let Some(size) = get("shm_size").and_then(scalar) {
        host.shm_size = Some(size);
    }
    let nvidia_deploy = get("deploy").is_some_and(|deploy| {
        serde_yaml::to_string(deploy).is_ok_and(|text| text.contains("nvidia"))
    });
    if get("deploy").is_some() && !nvidia_deploy {
        error
            .refused
            .push("`deploy` reserves something other than NVIDIA GPUs".into());
    }
    match (get("gpus").and_then(scalar).as_deref(), nvidia_deploy) {
        (Some("all"), _) | (None, true) => {
            host.gpus = true;
            accelerator = Some(Accelerator {
                family: AcceleratorFamily::Nvidia,
                runtime: AcceleratorRuntime::Cuda,
            });
        }
        (None, false) => {}
        (Some(other), _) => error
            .refused
            .push(format!("gpus {other}: only all is translated")),
    }
    for device in get("devices")
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default()
    {
        let Some(device) = scalar(&device) else {
            continue;
        };
        let device = resolve(&device, &mut unset).unwrap_or(device);
        if device.starts_with("/dev/dri") && accelerator.is_none() {
            host.gpus = true;
            accelerator = Some(Accelerator {
                family: AcceleratorFamily::Amd,
                runtime: AcceleratorRuntime::Vulkan,
            });
            continue;
        }
        if device.starts_with("/dev/kfd") {
            accelerator = Some(Accelerator {
                family: AcceleratorFamily::Amd,
                runtime: AcceleratorRuntime::Rocm,
            });
            continue;
        }
        host.devices.push(device);
    }
    if let Some(limits) = get("ulimits").and_then(Value::as_mapping) {
        for (name, limit) in limits {
            let Some(name) = scalar(name) else { continue };
            let value = match limit {
                Value::Mapping(pair) => format!(
                    "{}:{}",
                    pair.get("soft").and_then(scalar).unwrap_or_default(),
                    pair.get("hard").and_then(scalar).unwrap_or_default()
                ),
                other => match scalar(other) {
                    Some(v) => format!("{v}:{v}"),
                    None => continue,
                },
            };
            host.ulimits.insert(name, value);
        }
    }

    // The server, and the arguments Cairn assigns.
    let joined = entrypoint
        .iter()
        .flatten()
        .chain(args.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    let server = if joined.contains("vllm serve") {
        Some(Server::Vllm)
    } else if joined.contains("llama-server") || image.contains("llama.cpp") {
        Some(Server::LlamaCpp)
    } else {
        error.refused.push(
            "the command runs neither `vllm serve` nor `llama-server`, so Cairn cannot tell how to check and benchmark it"
                .into(),
        );
        None
    };
    for binding in BINDINGS {
        let Some(source) = flag_source(&raw_command, binding.flag) else {
            continue;
        };
        if joined.contains(binding.placeholder) {
            continue;
        }
        match source {
            FlagSource::Variable(var) => missing(
                &mut error,
                format!("env.{var}"),
                &format!("\"{}\": {}", binding.placeholder, binding.meaning),
            ),
            FlagSource::Literal(value) => error.refused.push(format!(
                "the compose file fixes `{} {value}`; Cairn needs it as {} ({})",
                binding.flag, binding.placeholder, binding.meaning
            )),
        }
    }
    if !joined.contains("{port}") && flag_source(&raw_command, "--port").is_none() {
        error
            .refused
            .push("the command names no --port, so Cairn cannot assign one".into());
    }
    if let Some(FlagSource::Variable(var)) = flag_source(&raw_command, "--host") {
        let host_value = base(&var).unwrap_or_default();
        let bound = interpolate(
            &raw_command[raw_command.find("--host").unwrap_or(0)..],
            &Scope {
                values: &base,
                keep,
            },
            &mut Unset::default(),
        )
        .ok()
        .and_then(|tail| tail.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or(host_value);
        if matches!(bound.as_str(), "127.0.0.1" | "localhost" | "::1") {
            missing(
                &mut error,
                format!("env.{var}"),
                "\"0.0.0.0\": the server binds loopback by default, and its health is checked at the head's address",
            );
        }
    }
    let service_text = serde_yaml::to_string(service).unwrap_or_default();
    for var in variables(&service_text)
        .iter()
        .filter(|v| v.contains("REVISION"))
    {
        if base(var).as_deref() != Some("{revision}") {
            missing(
                &mut error,
                format!("env.{var}"),
                "\"{revision}\": the weights' revision, which the profile pins",
            );
        }
    }
    for var in &unset.0 {
        if !rank_vars.contains(var) {
            missing(
                &mut error,
                format!("env.{var}"),
                "the compose file reads it with no default, and no env file sets it",
            );
        }
    }

    if !error.missing.is_empty() || !error.refused.is_empty() {
        return Err(error);
    }
    Ok(Compiled {
        container: Container {
            image,
            server: server.expect("refused above when unknown"),
            entrypoint,
            args,
            env,
            mounts,
            host,
            files: uses_source.then(|| SourcePin {
                url: spec.url.clone(),
                commit: spec.commit.clone(),
            }),
            ranks: if machines > 1 { rank_env } else { Vec::new() },
        },
        machines: machines as u32,
        accelerator,
        unpinned_image,
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPOSE: &str = include_str!("../tests/fixtures/import/docker-compose.dspark.yml");
    const ENV_EXAMPLE: &str = include_str!("../tests/fixtures/import/env.dspark.example");
    const COMMIT: &str = "f3d76450e075d5f89698aed2e865655f0b7eeb1c";

    fn files() -> Files {
        Files {
            compose: COMPOSE.into(),
            env_files: vec![(".env.dspark.example".into(), ENV_EXAMPLE.into())],
        }
    }

    fn pairs(list: &[(&str, &str)]) -> BTreeMap<String, String> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// What an agent supplies for the Sparks, from the operator's .env.dspark.
    fn dspark_spec() -> ImportSpec {
        ImportSpec {
            url: "https://github.com/MiaAI-Lab/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark".into(),
            commit: COMMIT.into(),
            compose: "docker-compose.dspark.yml".into(),
            service: "vllm-dspark".into(),
            env_files: vec![".env.dspark.example".into()],
            env: pairs(&[
                ("VLLM_HOST", "0.0.0.0"),
                ("VLLM_PORT", "{port}"),
                ("SERVED_MODEL_NAME", "{servedName}"),
                ("DSPARK_MODEL", "{model}"),
                ("DSPARK_REVISION", "{revision}"),
                ("MASTER_ADDR", "{head.link}"),
                ("NNODES", "{machines}"),
                ("GPU_MEMORY_UTILIZATION", "0.835"),
                ("MTP_NUM_TOKENS", "5"),
                ("HF_HUB_OFFLINE", "1"),
            ]),
            ranks: Some(vec![
                pairs(&[
                    ("NODE_RANK", "0"),
                    ("HEADLESS", ""),
                    ("VLLM_HOST_IP", "{self.link}"),
                ]),
                pairs(&[
                    ("NODE_RANK", "1"),
                    ("HEADLESS", "1"),
                    ("VLLM_HOST_IP", "{self.link}"),
                ]),
            ]),
            host_paths: BTreeMap::from([
                ("/cache/huggingface".into(), HostPath::HfCache),
                ("/tmp".into(), HostPath::Drop),
            ]),
            model: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
            weights: None,
            image: None,
            id: None,
        }
    }

    #[test]
    fn the_dspark_compose_compiles_into_a_two_rank_container() {
        let compiled = compile(&dspark_spec(), &files()).unwrap();
        let container = &compiled.container;
        assert_eq!(compiled.machines, 2);
        assert_eq!(container.server, Server::Vllm);
        assert_eq!(
            container.image,
            "ghcr.io/anemll/dspark-vllm-gx10:0.1.1@sha256:a83948492cf13df455170fb42885f5ef4db54fefe0feff0f841ecbff464ac9d8",
            "the env example pins the image"
        );
        assert_eq!(compiled.unpinned_image, None);
        assert_eq!(container.entrypoint, Some(vec![]));
        assert_eq!(&container.args[..2], ["bash", "-lc"]);
        let script = &container.args[2];
        for expected in [
            "exec /usr/local/bin/vllm serve {model}",
            "--served-model-name {servedName}",
            "--host 0.0.0.0",
            "--port {port}",
            "--gpu-memory-utilization 0.835",
            "--nnodes {machines}",
            "--node-rank ${NODE_RANK}",
            "--master-addr {head.link}",
            "${HEADLESS:+--headless}",
            "num_speculative_tokens\\\":${MTP_NUM_TOKENS:-6}",
        ] {
            assert!(script.contains(expected), "{expected} in:\n{script}");
        }
        assert!(!script.contains("$$"), "compose's $$ became the shell's $");
        assert_eq!(container.env["DSPARK_REVISION"], "{revision}");
        assert_eq!(container.env["MTP_NUM_TOKENS"], "5");
        assert_eq!(container.ranks.len(), 2);
        assert_eq!(container.ranks[0]["NODE_RANK"], "0");
        assert_eq!(container.ranks[1]["HEADLESS"], "1");
        assert_eq!(container.ranks[1]["VLLM_HOST_IP"], "{self.link}");
        assert!(!container.env.contains_key("NODE_RANK"));

        let patches = container
            .mounts
            .iter()
            .find(|m| m.to == "/opt/dspark-patches")
            .unwrap();
        assert_eq!(
            patches.from,
            MountSource::Source {
                path: "./patches".into()
            }
        );
        assert!(patches.read_only);
        assert!(container
            .mounts
            .iter()
            .any(|m| m.from == MountSource::HfCache
                && m.to == "/cache/huggingface"
                && m.read_only));
        assert!(
            compiled
                .notes
                .iter()
                .any(|note| note.contains("/cache/huggingface writable")),
            "{:?}",
            compiled.notes
        );
        assert!(!container.mounts.iter().any(|m| m.to == "/tmp"));
        assert_eq!(container.files.as_ref().unwrap().commit, COMMIT);
        assert!(container.host.gpus && container.host.ipc_host && container.host.network_host);
        assert_eq!(container.host.shm_size.as_deref(), Some("64gb"));
        assert_eq!(
            container.host.devices,
            vec!["/dev/infiniband:/dev/infiniband"]
        );
        assert_eq!(container.host.ulimits["memlock"], "-1:-1");
        assert_eq!(
            compiled.accelerator.map(|a| a.family),
            Some(AcceleratorFamily::Nvidia)
        );
        assert!(compiled
            .notes
            .iter()
            .any(|n| n.starts_with("dropped `restart`")));
        assert!(compiled
            .notes
            .iter()
            .any(|n| n.starts_with("dropped `healthcheck`")));
    }

    #[test]
    fn the_compiled_dspark_profile_is_valid_and_renders() {
        use crate::profile::{InferenceProfile, ModelRef, ProfileOrigin, Requirements, WeightFile};
        let compiled = compile(&dspark_spec(), &files()).unwrap();
        let profile = InferenceProfile {
            id: "dsv4".into(),
            source: Default::default(),
            model: ModelRef {
                name: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
                weights: vec![WeightFile {
                    repo: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
                    revision: "6821d6ad3681a4b137b066b76094fa82ebd0a380".into(),
                    path: "model-00001.safetensors".into(),
                    sha256: "0".repeat(64),
                    size: None,
                }],
                config_files: vec![],
            },
            served_name: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
            upstream_id: None,
            verbs: None,
            modalities: None,
            quantization: None,
            origin: ProfileOrigin::Authored,
            engine: crate::profile::Engine::Container(compiled.container),
            requirements: Requirements {
                machines: compiled.machines,
                accelerator: compiled.accelerator,
                interconnect: None,
                memory: None,
            },
            notes: None,
        };
        assert_eq!(profile.problems(), vec![]);
    }

    #[test]
    fn a_bare_spec_is_told_every_key_it_lacks() {
        let spec = ImportSpec {
            env: BTreeMap::new(),
            ranks: None,
            host_paths: BTreeMap::new(),
            ..dspark_spec()
        };
        let error = compile(&spec, &files()).unwrap_err();
        let keys: Vec<&str> = error.missing.iter().map(|m| m.key.as_str()).collect();
        for key in [
            "ranks",
            "hostPaths./cache/huggingface",
            "hostPaths./tmp",
            "env.VLLM_PORT",
            "env.SERVED_MODEL_NAME",
            "env.DSPARK_MODEL",
            "env.DSPARK_REVISION",
        ] {
            assert!(keys.contains(&key), "{key} missing from {keys:?}");
        }
        let rendered = error.to_string();
        assert!(
            rendered.starts_with("The import needs these keys in its spec:\n- "),
            "{rendered}"
        );
        assert!(error.refused.is_empty(), "{:?}", error.refused);
    }

    #[test]
    fn a_loopback_bind_and_unknown_keys_are_refused_by_name() {
        let mut spec = dspark_spec();
        spec.env.insert("VLLM_HOST".into(), "127.0.0.1".into());
        let error = compile(&spec, &files()).unwrap_err();
        assert!(
            error.missing.iter().any(|m| m.key == "env.VLLM_HOST"),
            "{error}"
        );

        let compose = "services:\n  s:\n    image: x@sha256:0000000000000000000000000000000000000000000000000000000000000000\n    command: [llama-server, --port, '${PORT}']\n    cap_add: [SYS_ADMIN]\n";
        let spec = ImportSpec {
            service: "s".into(),
            env: pairs(&[("PORT", "{port}")]),
            ranks: None,
            host_paths: BTreeMap::new(),
            env_files: vec![],
            ..dspark_spec()
        };
        let error = compile(
            &spec,
            &Files {
                compose: compose.into(),
                env_files: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(
            error.refused,
            vec!["service key `cap_add` has no translation into a Cairn profile"]
        );
        let error = compile(
            &ImportSpec {
                service: "nope".into(),
                ..spec
            },
            &Files {
                compose: compose.into(),
                env_files: vec![],
            },
        )
        .unwrap_err();
        assert!(error.refused[0].contains("has no service nope; it has s"));
    }

    #[test]
    fn interpolation_follows_compose() {
        let values = |name: &str| match name {
            "SET" => Some("v".to_string()),
            "EMPTY" => Some(String::new()),
            _ => None,
        };
        let keep = BTreeSet::from(["RANK".to_string()]);
        let scope = Scope {
            values: &values,
            keep: Some(&keep),
        };
        let run = |text: &str| interpolate(text, &scope, &mut Unset::default()).unwrap();
        assert_eq!(run("${SET}"), "v");
        assert_eq!(run("$SET/x"), "v/x");
        assert_eq!(run("${NOPE:-d}"), "d");
        assert_eq!(run("${EMPTY:-d}"), "d");
        assert_eq!(run("${EMPTY-d}"), "");
        assert_eq!(run("${NOPE-d}"), "d");
        assert_eq!(run("${SET:+a}${EMPTY:+b}${EMPTY+c}"), "ac");
        assert_eq!(run("${NOPE:-${SET}/y}"), "v/y");
        assert_eq!(run("$$HOME and $${PATH:-}"), "$HOME and ${PATH:-}");
        assert_eq!(
            run("--node-rank ${RANK} ${RANK:+x}"),
            "--node-rank ${RANK} ${RANK:+x}"
        );
        assert_eq!(run("cost $5"), "cost $5");
        let mut unset = Unset::default();
        interpolate("${NOPE} ${REQ:?must}", &scope, &mut unset).unwrap();
        assert_eq!(
            unset.0,
            BTreeSet::from(["NOPE".to_string(), "REQ".to_string()])
        );
        assert!(interpolate("${OPEN", &scope, &mut Unset::default()).is_err());
    }

    #[test]
    fn env_files_read_like_dotenv() {
        let values =
            parse_env_file("# c\nA=1\nexport B=\"two words\"\nC='x' \nD=y # note\n\nbad line\n");
        assert_eq!(
            values,
            pairs(&[("A", "1"), ("B", "two words"), ("C", "x"), ("D", "y")])
        );
        assert_eq!(shell_words("a 'b c' \"d\"e"), vec!["a", "b c", "de"]);
    }
}
