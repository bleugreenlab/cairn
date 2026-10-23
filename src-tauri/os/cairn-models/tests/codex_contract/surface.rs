//! The protocol surface Cairn depends on, checked against the JSON Schema the
//! real binary emits (`codex app-server generate-json-schema --experimental`).
//!
//! Two halves. What Cairn **reads** is hand-maintained in
//! `cairn-llm-wire/tests/fixtures/codex_protocol_surface.json`: notification
//! and response fields with their required-ness, the enum values and item
//! types Cairn matches on, and the methods it calls. What Cairn **sends** is
//! not listed anywhere: every key of every request Cairn's own param builders
//! produce, and every request a live session actually wrote, must be a
//! property of that method's params type. The only exemptions are the
//! fixture's `knownDead` entries, each one (method, key) pair naming the issue
//! that owns it.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fixture {
    pub requests: Vec<String>,
    pub notifications: Vec<String>,
    pub reads: Vec<Read>,
    pub enums: BTreeMap<String, Vec<String>>,
    pub item_types: Vec<String>,
    pub known_dead: Vec<KnownDead>,
}

#[derive(Debug, Deserialize)]
pub struct Read {
    #[serde(rename = "type")]
    pub ty: String,
    /// Dotted field path from `ty`, following `$ref`s.
    pub path: String,
    pub required: bool,
}

#[derive(Debug, Deserialize)]
pub struct KnownDead {
    pub method: String,
    pub key: String,
    pub issue: String,
}

impl Fixture {
    pub fn path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../cairn-llm-wire/tests/fixtures/codex_protocol_surface.json")
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        serde_json::from_str(&text)
            .map_err(|error| format!("{} is malformed: {error}", path.display()))
    }
}

pub struct Schema {
    /// Client request method to its params definition.
    requests: BTreeMap<String, Option<String>>,
    notifications: BTreeSet<String>,
    defs: Map<String, Value>,
}

fn methods(file: &Value) -> Vec<(String, Option<String>)> {
    file["oneOf"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|variant| {
            let method = variant
                .pointer("/properties/method/enum/0")
                .and_then(Value::as_str)?
                .to_string();
            let params = variant
                .pointer("/properties/params/$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.rsplit('/').next())
                .map(str::to_string);
            Some((method, params))
        })
        .collect()
}

impl Schema {
    /// Generate the schema with the real binary into `out` and load it.
    pub fn generate(codex: &Path, out: &Path) -> Result<Self, String> {
        let output = std::process::Command::new(codex)
            .args([
                "app-server",
                "generate-json-schema",
                "--experimental",
                "--out",
            ])
            .arg(out)
            .output()
            .map_err(|error| format!("cannot run {}: {error}", codex.display()))?;
        if !output.status.success() {
            return Err(format!(
                "generate-json-schema failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let read = |name: &str| -> Result<Value, String> {
            let path = out.join(name);
            let text = std::fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            serde_json::from_str(&text).map_err(|error| format!("{name}: {error}"))
        };
        let bundle = read("codex_app_server_protocol.v2.schemas.json")?;
        let client = read("ClientRequest.json")?;
        let notifications = read("ServerNotification.json")?;
        let server_requests = read("ServerRequest.json")?;
        let mut defs = bundle["definitions"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        for file in [&client, &notifications, &server_requests] {
            for (name, def) in file["definitions"].as_object().into_iter().flatten() {
                defs.entry(name.clone()).or_insert_with(|| def.clone());
            }
        }
        // The params and responses of server requests (the token refresh's
        // answer, for one) are emitted only as their own top-level files.
        let entries = std::fs::read_dir(out)
            .map_err(|error| format!("cannot list {}: {error}", out.display()))?;
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            if path.extension().and_then(|ext| ext.to_str()) != Some("json")
                || stem.starts_with("codex_app_server_protocol")
            {
                continue;
            }
            let file = read(&format!("{stem}.json"))?;
            for (name, def) in file["definitions"].as_object().into_iter().flatten() {
                defs.entry(name.clone()).or_insert_with(|| def.clone());
            }
            defs.entry(stem.to_string()).or_insert(file);
        }
        Ok(Self {
            requests: methods(&client).into_iter().collect(),
            notifications: methods(&notifications)
                .into_iter()
                .map(|(m, _)| m)
                .collect(),
            defs,
        })
    }

    /// `value` with `$ref`s followed and a nullable wrapper unwrapped.
    fn resolve<'a>(&'a self, mut value: &'a Value) -> &'a Value {
        for _ in 0..16 {
            if let Some(name) = value
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.rsplit('/').next())
            {
                match self.defs.get(name) {
                    Some(def) => {
                        value = def;
                        continue;
                    }
                    None => return value,
                }
            }
            let wrapped = ["anyOf", "oneOf", "allOf"].iter().find_map(|key| {
                let options: Vec<&Value> = value
                    .get(*key)?
                    .as_array()?
                    .iter()
                    .filter(|option| option.get("type").and_then(Value::as_str) != Some("null"))
                    .collect();
                (options.len() == 1).then(|| options[0])
            });
            match wrapped {
                Some(inner) => value = inner,
                None => return value,
            }
        }
        value
    }

    fn def(&self, name: &str) -> Option<&Value> {
        self.defs.get(name).map(|def| self.resolve(def))
    }

    /// The values an enum definition allows: a plain `enum`, or the
    /// single-value variants of a `oneOf`.
    fn enum_values(&self, name: &str) -> BTreeSet<String> {
        let Some(def) = self.def(name) else {
            return BTreeSet::new();
        };
        let mut values: BTreeSet<String> = def["enum"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        for variant in def["oneOf"].as_array().into_iter().flatten() {
            for value in variant["enum"].as_array().into_iter().flatten() {
                if let Some(value) = value.as_str() {
                    values.insert(value.to_string());
                }
            }
        }
        values
    }

    fn item_types(&self) -> BTreeSet<String> {
        let Some(def) = self.def("ThreadItem") else {
            return BTreeSet::new();
        };
        def["oneOf"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|variant| {
                variant
                    .pointer("/properties/type/enum/0")
                    .and_then(Value::as_str)
            })
            .map(str::to_string)
            .collect()
    }

    /// Everything the fixture says Cairn reads or calls that this Codex no
    /// longer has, or has with another required-ness.
    pub fn check_fixture(&self, fixture: &Fixture) -> Vec<String> {
        let mut problems = Vec::new();
        for dead in &fixture.known_dead {
            if !dead.issue.starts_with("cairn/") {
                problems.push(format!(
                    "the knownDead exemption for `{}` on `{}` names no owning issue",
                    dead.key, dead.method
                ));
            }
        }
        for method in &fixture.requests {
            if !self.requests.contains_key(method) {
                problems.push(format!(
                    "Cairn calls `{method}`, which this Codex does not offer"
                ));
            }
        }
        for method in &fixture.notifications {
            if !self.notifications.contains(method) {
                problems.push(format!(
                    "Cairn consumes `{method}`, which this Codex never sends"
                ));
            }
        }
        for read in &fixture.reads {
            if let Err(problem) = self.check_read(read) {
                problems.push(problem);
            }
        }
        for (name, wanted) in &fixture.enums {
            let have = self.enum_values(name);
            for value in wanted {
                if !have.contains(value) {
                    problems.push(format!(
                        "Cairn matches `{name}` value `{value}`, which this Codex no longer has (it has {have:?})"
                    ));
                }
            }
        }
        let items = self.item_types();
        for item in &fixture.item_types {
            if !items.contains(item) {
                problems.push(format!(
                    "Cairn reads `{item}` items, which this Codex no longer emits"
                ));
            }
        }
        problems
    }

    fn check_read(&self, read: &Read) -> Result<(), String> {
        let at = || format!("`{}.{}`", read.ty, read.path);
        let mut def = self.def(&read.ty).ok_or_else(|| {
            format!(
                "Cairn reads {}, but this Codex has no `{}` type",
                at(),
                read.ty
            )
        })?;
        let segments: Vec<&str> = read.path.split('.').collect();
        for (index, segment) in segments.iter().enumerate() {
            let field = def
                .pointer(&format!("/properties/{segment}"))
                .ok_or_else(|| {
                    format!("Cairn reads {}, which this Codex no longer defines", at())
                })?;
            if index + 1 == segments.len() {
                let required = def["required"]
                    .as_array()
                    .is_some_and(|required| required.iter().any(|name| name == segment));
                if required != read.required {
                    return Err(format!(
                        "Cairn reads {} as {}, but this Codex makes it {}",
                        at(),
                        if read.required {
                            "required"
                        } else {
                            "optional"
                        },
                        if required { "required" } else { "optional" },
                    ));
                }
            } else {
                def = self.resolve(field);
            }
        }
        Ok(())
    }

    /// Every key Cairn sends that this Codex does not define, and every
    /// required key Cairn omits, outside the fixture's `knownDead` pairs.
    pub fn check_requests(
        &self,
        requests: &[(String, Value)],
        known_dead: &[KnownDead],
    ) -> Vec<String> {
        let mut problems = BTreeSet::new();
        for (method, params) in requests {
            let Some(definition) = self.requests.get(method) else {
                problems.insert(format!(
                    "Cairn sends `{method}`, which this Codex does not offer"
                ));
                continue;
            };
            let Some(def) = definition.as_deref().and_then(|name| self.def(name)) else {
                continue;
            };
            let properties = def["properties"].as_object();
            for key in params.as_object().into_iter().flatten().map(|(key, _)| key) {
                let defined = properties.is_some_and(|properties| properties.contains_key(key));
                let exempt = known_dead
                    .iter()
                    .any(|dead| dead.method == *method && dead.key == *key);
                if !defined && !exempt {
                    problems.insert(format!(
                        "Cairn sends `{key}` on `{method}`, which this Codex does not define, so it is silently ignored"
                    ));
                }
            }
            for required in def["required"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if params.get(required).is_none() {
                    problems.insert(format!(
                        "Cairn omits `{required}`, which `{method}` requires"
                    ));
                }
            }
        }
        problems.into_iter().collect()
    }
}

/// The requests Cairn's line protocol carried: `(method, params)` for every
/// line with a method and an id.
pub fn sent_requests(lines: &[String]) -> Vec<(String, Value)> {
    lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| message.get("id").is_some())
        .filter_map(|message| {
            let method = message.get("method")?.as_str()?.to_string();
            Some((
                method,
                message.get("params").cloned().unwrap_or(Value::Null),
            ))
        })
        .collect()
}
