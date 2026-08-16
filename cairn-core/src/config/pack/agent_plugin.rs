//! Strict Agent Plugins 1.0.0 import and export codec.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::manifest::{PackAuthor, PackFormat, PackItem, PackItemKind, PackManifest};
use crate::config::mcp_servers::McpServerConfig;

pub const PLUGIN_SCHEMA_URI: &str = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json";
pub const MCP_SCHEMA_URI: &str = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json";
pub const PLUGIN_SCHEMA: &str = include_str!("schemas/plugin.schema.json");
pub const MCP_SCHEMA: &str = include_str!("schemas/mcp.schema.json");
pub const CAIRN_EXTENSION: &str = "dev.cairn";
const CAIRN_EXTENSION_MANIFEST: &str = "cairn-pack.yaml";
const CAIRN_EXTENSION_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CairnExtensionManifest {
    schema_version: u32,
    id: String,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    author: Option<PackAuthor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    homepage: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    keywords: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentPluginServer {
    pub config: McpServerConfig,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentPlugin {
    pub manifest: PackManifest,
    pub items: Vec<PackItem>,
    pub mcp_servers: BTreeMap<String, AgentPluginServer>,
    pub diagnostics: Vec<String>,
}

pub fn load(root: &Path) -> Result<AgentPlugin, String> {
    let root = root
        .canonicalize()
        .map_err(|e| format!("Failed to resolve plugin root: {e}"))?;
    if !root.is_dir() {
        return Err("Agent Plugin root is not a directory".into());
    }
    let mut value = read_json(
        &contained_file(&root, &root.join("plugin.json"))?,
        "plugin.json",
    )?;
    let object = value
        .as_object_mut()
        .ok_or("plugin.json must be an object")?;
    let allowed = [
        "$schema",
        "name",
        "version",
        "description",
        "author",
        "homepage",
        "repository",
        "license",
        "keywords",
        "extensions",
    ];
    let unknown = object
        .keys()
        .filter(|k| !allowed.contains(&k.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut diagnostics = Vec::new();
    for key in unknown {
        object.remove(&key);
        diagnostics.push(format!("ignored unknown plugin.json field '{key}'"));
    }
    if object.get("extensions").is_some_and(|v| !v.is_object()) {
        object.remove("extensions");
        diagnostics.push("ignored non-object plugin.json field 'extensions'".into());
    }
    validate(&value, PLUGIN_SCHEMA, "plugin.json")?;
    let id = value["name"].as_str().unwrap().to_string();

    let mut items = Vec::new();
    discover_skills(&root, &mut items, &mut diagnostics)?;
    let extension = discover_extension(&root, &value, &mut items, &mut diagnostics)?;
    let mcp_servers = read_mcp(&root, &mut diagnostics);
    items.extend(mcp_servers.keys().cloned().map(PackItem::mcp));

    let author: Option<PackAuthor> = value
        .get("author")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| e.to_string())?;
    let mut manifest = PackManifest {
        id: id.clone(),
        name: id,
        version: text(&value, "version").unwrap_or("0.0.0").into(),
        description: text(&value, "description").unwrap_or("").into(),
        author,
        homepage: text(&value, "homepage").map(Into::into),
        license: text(&value, "license").map(Into::into),
        keywords: value
            .get("keywords")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(Into::into).collect())
            .unwrap_or_default(),
        default: false,
        retired: Vec::new(),
        format: PackFormat::AgentPlugin,
        notes: diagnostics.clone(),
        root,
    };
    if let Some(extension) = extension {
        if extension.id != manifest.id {
            diagnostics.push(format!(
                "ignored dev.cairn extension metadata: id '{}' does not match plugin name '{}'",
                extension.id, manifest.id
            ));
        } else {
            manifest.name = extension.name;
            manifest.version = extension.version;
            manifest.description = extension.description;
            manifest.author = extension.author;
            manifest.homepage = extension.homepage;
            manifest.keywords = extension.keywords;
        }
    }
    manifest.notes = diagnostics.clone();
    Ok(AgentPlugin {
        manifest,
        items,
        mcp_servers,
        diagnostics,
    })
}

fn discover_skills(
    root: &Path,
    items: &mut Vec<PackItem>,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let dir = root.join("skills");
    if !dir.exists() {
        return Ok(());
    }
    if !fs::symlink_metadata(&dir)
        .map_err(|e| e.to_string())?
        .file_type()
        .is_dir()
    {
        notes.push("ignored skills/: expected a directory".into());
        return Ok(());
    }
    let mut entries = fs::read_dir(dir)
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let id = entry.file_name().to_string_lossy().to_string();
        match validate_skill(root, &id, &entry.path()) {
            Ok(()) => items.push(PackItem::file(
                PackItemKind::Skill,
                &id,
                format!("skills/{id}"),
            )),
            Err(e) => notes.push(format!("skipped skill '{id}': {e}")),
        }
    }
    Ok(())
}

fn read_cairn_extension_manifest(
    root: &Path,
    ext_root: &Path,
) -> Result<Option<CairnExtensionManifest>, String> {
    let path = ext_root.join(CAIRN_EXTENSION_MANIFEST);
    if !path.exists() {
        return Ok(None);
    }
    let path = contained_file(root, &path)?;
    let source = fs::read_to_string(path).map_err(|e| e.to_string())?;
    let metadata: CairnExtensionManifest =
        serde_yaml::from_str(&source).map_err(|e| format!("invalid cairn-pack.yaml: {e}"))?;
    if metadata.schema_version != CAIRN_EXTENSION_VERSION {
        return Err(format!(
            "unsupported cairn-pack.yaml schemaVersion {}; expected {}",
            metadata.schema_version, CAIRN_EXTENSION_VERSION
        ));
    }
    if !valid_plugin_name(&metadata.id)
        || metadata.name.trim().is_empty()
        || metadata.version.trim().is_empty()
        || metadata
            .keywords
            .iter()
            .any(|keyword| keyword.trim().is_empty())
    {
        return Err("cairn-pack.yaml contains invalid identity or display metadata".into());
    }
    Ok(Some(metadata))
}

fn validate_skill(root: &Path, id: &str, dir: &Path) -> Result<(), String> {
    let resolved = dir.canonicalize().map_err(|e| e.to_string())?;
    contained(root, &resolved)?;
    if !resolved.is_dir() {
        return Err("package is not a directory".into());
    }
    let file = contained_file(root, &dir.join("SKILL.md"))?;
    let source = fs::read_to_string(file).map_err(|e| e.to_string())?;
    let yaml = source
        .strip_prefix("---\n")
        .and_then(|s| s.split_once("\n---"))
        .map(|p| p.0)
        .ok_or("SKILL.md must begin with YAML frontmatter")?;
    let value: serde_yaml::Value = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
    let map = value.as_mapping().ok_or("frontmatter must be a mapping")?;
    let field = |key: &str| {
        map.get(serde_yaml::Value::String(key.into()))
            .and_then(ValueExt::yaml_str)
    };
    let name = field("name").ok_or("frontmatter requires string 'name'")?;
    if name != id {
        return Err(format!(
            "frontmatter name '{name}' does not match directory '{id}'"
        ));
    }
    if !valid_component_name(name) {
        return Err("invalid skill name".into());
    }
    let description = field("description").ok_or("frontmatter requires string 'description'")?;
    if description.is_empty() || description.len() > 1024 {
        return Err("description must contain 1-1024 bytes".into());
    }
    Ok(())
}

trait ValueExt {
    fn yaml_str(&self) -> Option<&str>;
}
impl ValueExt for serde_yaml::Value {
    fn yaml_str(&self) -> Option<&str> {
        self.as_str()
    }
}

fn discover_extension(
    root: &Path,
    manifest: &Value,
    items: &mut Vec<PackItem>,
    notes: &mut Vec<String>,
) -> Result<Option<CairnExtensionManifest>, String> {
    let Some(extension) = manifest.pointer("/extensions/dev.cairn") else {
        return Ok(None);
    };
    if extension.get("version").and_then(Value::as_u64) != Some(1) {
        notes.push("ignored dev.cairn extension: expected version 1".into());
        return Ok(None);
    }
    let ext_root = root.join(CAIRN_EXTENSION);
    if !ext_root.exists() {
        notes.push("ignored dev.cairn extension: directory missing".into());
        return Ok(None);
    }
    contained(root, &ext_root.canonicalize().map_err(|e| e.to_string())?)?;
    let metadata = match read_cairn_extension_manifest(root, &ext_root) {
        Ok(metadata) => metadata,
        Err(error) => {
            notes.push(format!("ignored dev.cairn extension metadata: {error}"));
            None
        }
    };
    for (dir, kind, suffix, marker) in [
        ("agents", PackItemKind::Agent, "md", None),
        ("recipes", PackItemKind::Recipe, "yaml", None),
        ("responses", PackItemKind::Response, "md", None),
        (
            "workflows",
            PackItemKind::Workflow,
            "",
            Some("workflow.yaml"),
        ),
    ] {
        let Ok(entries) = fs::read_dir(ext_root.join(dir)) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            let file = entry.file_name().to_string_lossy().to_string();
            let ok = marker.map_or_else(
                || p.is_file() && p.extension().and_then(|x| x.to_str()) == Some(suffix),
                |m| p.is_dir() && p.join(m).is_file(),
            );
            if ok {
                let id = p.file_stem().and_then(|x| x.to_str()).unwrap_or(&file);
                items.push(PackItem::file(
                    kind,
                    id,
                    format!("{CAIRN_EXTENSION}/{dir}/{file}"),
                ));
            }
        }
    }
    Ok(metadata)
}

fn read_mcp(root: &Path, notes: &mut Vec<String>) -> BTreeMap<String, AgentPluginServer> {
    let path = root.join("mcp.json");
    if !path.exists() {
        return BTreeMap::new();
    }
    let result = (|| {
        let value = read_json(&contained_file(root, &path)?, "mcp.json")?;
        let obj = value.as_object().ok_or("mcp.json must be an object")?;
        if obj.keys().any(|k| k != "$schema" && k != "mcpServers")
            || text(&value, "$schema") != Some(MCP_SCHEMA_URI)
            || !value["mcpServers"].is_object()
        {
            return Err("mcp.json envelope does not conform to Agent Plugins 1.0.0".to_string());
        }
        let schema: Value = serde_json::from_str(MCP_SCHEMA).map_err(|e| e.to_string())?;
        let server_schema =
            serde_json::json!({"$ref":"#/$defs/server","$defs":schema["$defs"].clone()});
        let mut result = BTreeMap::new();
        for (name, server) in value["mcpServers"].as_object().unwrap() {
            if !valid_component_name(name) {
                notes.push(format!("skipped MCP server '{name}': invalid name"));
                continue;
            }
            if let Err(e) = validate_value(server, &server_schema, "MCP server") {
                notes.push(format!("skipped MCP server '{name}': {e}"));
                continue;
            }
            match convert_server(root, server) {
                Ok(server) => {
                    result.insert(name.clone(), server);
                }
                Err(e) => notes.push(format!("skipped MCP server '{name}': {e}")),
            }
        }
        Ok(result)
    })();
    match result {
        Ok(v) => v,
        Err(e) => {
            notes.push(format!("disabled MCP: {e}"));
            BTreeMap::new()
        }
    }
}

fn convert_server(root: &Path, value: &Value) -> Result<AgentPluginServer, String> {
    let kind = text(value, "type").unwrap();
    let mut config = McpServerConfig {
        transport: if kind == "streamable-http" {
            "http"
        } else {
            kind
        }
        .into(),
        command: None,
        args: Vec::new(),
        env: HashMap::new(),
        cwd: None,
        url: None,
        headers: HashMap::new(),
        enabled: true,
        oauth: None,
        secrets: Vec::new(),
        agent_plugin_runtime: None,
    };
    if kind == "stdio" {
        let command = text(value, "command").unwrap();
        if command.starts_with("./") {
            validate_path(root, command)?;
        } else if command.contains('/') || command.contains('\\') || command.contains("${") {
            return Err("command must be a bare token or contained ./ path".into());
        }
        config.command = Some(command.into());
        config.args = strings(value.get("args"));
        config.env = string_map(value.get("env"));
        for key in config.env.keys() {
            if key.contains("${") {
                return Err("placeholders are forbidden in environment keys".into());
            }
        }
        config.cwd = text(value, "cwd").map(Into::into);
        if let Some(v) = &config.cwd {
            validate_path(root, v)?;
        }
    } else {
        let url = text(value, "url").unwrap();
        validate_url(url)?;
        config.url = Some(url.into());
        config.headers = string_map(value.get("headers"));
        if config
            .headers
            .iter()
            .any(|(k, v)| k.contains("${PLUGIN_") || v.contains("${PLUGIN_"))
        {
            return Err("placeholders are forbidden in headers".into());
        }
    }
    Ok(AgentPluginServer { config })
}

pub fn export(
    destination: &Path,
    manifest: &PackManifest,
    items: &[PackItem],
    servers: &BTreeMap<String, AgentPluginServer>,
) -> Result<(), String> {
    if destination.exists() {
        return Err("export destination already exists".into());
    }
    if !valid_plugin_name(&manifest.id) {
        return Err(format!("pack id '{}' is not portable", manifest.id));
    }
    let parent = destination.parent().ok_or("destination has no parent")?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let stage = tempfile::Builder::new()
        .prefix(".plugin-")
        .tempdir_in(parent)
        .map_err(|e| e.to_string())?;
    let extension = items
        .iter()
        .any(|i| !matches!(i.kind, PackItemKind::Skill | PackItemKind::Mcp));
    let mut out = Map::new();
    out.insert("$schema".into(), PLUGIN_SCHEMA_URI.into());
    out.insert("name".into(), manifest.id.clone().into());
    if manifest.version != "0.0.0" {
        out.insert("version".into(), manifest.version.clone().into());
    }
    if !manifest.description.is_empty() {
        out.insert("description".into(), manifest.description.clone().into());
    }
    if let Some(author) = &manifest.author {
        out.insert(
            "author".into(),
            serde_json::to_value(author).map_err(|e| e.to_string())?,
        );
    }
    if let Some(homepage) = &manifest.homepage {
        out.insert("homepage".into(), homepage.clone().into());
    }
    if !manifest.keywords.is_empty() {
        out.insert(
            "keywords".into(),
            serde_json::to_value(&manifest.keywords).map_err(|e| e.to_string())?,
        );
    }
    if extension {
        out.insert(
            "extensions".into(),
            serde_json::json!({CAIRN_EXTENSION:{"version":1}}),
        );
    }
    write_json(&stage.path().join("plugin.json"), &Value::Object(out))?;
    let canonical_root = manifest.root.canonicalize().map_err(|e| e.to_string())?;
    for item in items.iter().filter(|i| i.kind != PackItemKind::Mcp) {
        let rel = item.path.as_deref().ok_or("file item has no path")?;
        let source = manifest.root.join(rel);
        contained(
            &canonical_root,
            &source.canonicalize().map_err(|e| e.to_string())?,
        )?;
        let target = match item.kind {
            PackItemKind::Skill => {
                validate_skill(&canonical_root, &item.id, &source)?;
                PathBuf::from("skills").join(&item.id)
            }
            PackItemKind::Agent => PathBuf::from(CAIRN_EXTENSION)
                .join("agents")
                .join(source.file_name().unwrap()),
            PackItemKind::Recipe => PathBuf::from(CAIRN_EXTENSION)
                .join("recipes")
                .join(source.file_name().unwrap()),
            PackItemKind::Response => PathBuf::from(CAIRN_EXTENSION)
                .join("responses")
                .join(source.file_name().unwrap()),
            PackItemKind::Workflow => PathBuf::from(CAIRN_EXTENSION)
                .join("workflows")
                .join(&item.id),
            PackItemKind::Mcp => unreachable!(),
        };
        copy_tree(&source, &stage.path().join(target))?;
    }
    if extension {
        let path = stage.path().join(CAIRN_EXTENSION).join("cairn-pack.yaml");
        fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
        let metadata = CairnExtensionManifest {
            schema_version: CAIRN_EXTENSION_VERSION,
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            author: manifest.author.clone(),
            homepage: manifest.homepage.clone(),
            keywords: manifest.keywords.clone(),
        };
        fs::write(
            path,
            serde_yaml::to_string(&metadata).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }
    if !servers.is_empty() {
        let mut values = Map::new();
        for (name, server) in servers {
            values.insert(name.clone(), export_server(server)?);
        }
        write_json(
            &stage.path().join("mcp.json"),
            &serde_json::json!({"$schema":MCP_SCHEMA_URI,"mcpServers":values}),
        )?;
    }
    load(stage.path()).map_err(|e| format!("export validation failed: {e}"))?;
    fs::rename(stage.keep(), destination).map_err(|e| e.to_string())
}

fn export_server(server: &AgentPluginServer) -> Result<Value, String> {
    let c = &server.config;
    if !c.enabled || c.oauth.is_some() || !c.secrets.is_empty() {
        return Err("MCP server has non-portable state".into());
    }
    match c.transport.as_str() {
        "stdio" => {
            let command = c.command.as_deref().ok_or("stdio server lacks command")?;
            if command.contains("${") {
                return Err("command contains a variable reference".into());
            }
            let mut v = serde_json::json!({"type":"stdio","command":command});
            if !c.args.is_empty() {
                v["args"] = serde_json::to_value(&c.args).unwrap()
            }
            if !c.env.is_empty() {
                v["env"] = serde_json::to_value(&c.env).unwrap()
            }
            if let Some(cwd) = &c.cwd {
                v["cwd"] = cwd.clone().into()
            }
            Ok(v)
        }
        "http" | "sse" => {
            let url = c.url.as_deref().ok_or("remote server lacks URL")?;
            validate_url(url)?;
            let kind = if c.transport == "http" {
                "streamable-http"
            } else {
                "sse"
            };
            let mut v = serde_json::json!({"type":kind,"url":url});
            if !c.headers.is_empty() {
                v["headers"] = serde_json::to_value(&c.headers).unwrap()
            }
            Ok(v)
        }
        other => Err(format!("unsupported transport {other}")),
    }
}

fn validate(value: &Value, schema: &str, label: &str) -> Result<(), String> {
    let s = serde_json::from_str(schema).map_err(|e| e.to_string())?;
    validate_value(value, &s, label)
}
fn validate_value(value: &Value, schema: &Value, label: &str) -> Result<(), String> {
    let validator = jsonschema::validator_for(schema).map_err(|e| e.to_string())?;
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(format!(
            "{label} failed schema validation: {}",
            validator
                .iter_errors(value)
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        ))
    }
}
fn validate_path(root: &Path, value: &str) -> Result<(), String> {
    let rel = value
        .strip_prefix("./")
        .or_else(|| value.strip_prefix("${PLUGIN_ROOT}/"))
        .or_else(|| value.strip_prefix("${PLUGIN_DATA}/"))
        .ok_or("path must use ./, PLUGIN_ROOT, or PLUGIN_DATA")?;
    if rel
        .split('/')
        .any(|p| p.is_empty() || p == "." || p == "..")
        || rel.contains('\\')
    {
        return Err("path contains forbidden component".into());
    }
    let p = root.join(rel);
    if p.exists() {
        contained(root, &p.canonicalize().map_err(|e| e.to_string())?)?
    }
    Ok(())
}
fn validate_url(value: &str) -> Result<(), String> {
    let u = reqwest::Url::parse(value).map_err(|e| e.to_string())?;
    if u.username() != "" || u.password().is_some() || u.fragment().is_some() {
        return Err("URL contains userinfo or fragment".into());
    }
    let h = u.host_str().ok_or("URL lacks host")?;
    let local = h.eq_ignore_ascii_case("localhost")
        || h.parse::<std::net::IpAddr>().is_ok_and(|x| x.is_loopback());
    if u.scheme() != "https" && !(u.scheme() == "http" && local) {
        return Err("URL requires HTTPS except loopback".into());
    }
    Ok(())
}
fn valid_plugin_name(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && v.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && !v.starts_with('-')
        && !v.starts_with('.')
        && !v.ends_with('-')
        && !v.ends_with('.')
        && !v.contains("--")
        && !v.contains("..")
}
fn valid_component_name(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && v.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !v.starts_with('-')
        && !v.ends_with('-')
        && !v.contains("--")
}
fn contained_file(root: &Path, path: &Path) -> Result<PathBuf, String> {
    let p = path
        .canonicalize()
        .map_err(|e| format!("failed to resolve {path:?}: {e}"))?;
    contained(root, &p)?;
    if !p.is_file() {
        return Err("expected regular file".into());
    }
    Ok(p)
}
fn contained(root: &Path, path: &Path) -> Result<(), String> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(format!("path {path:?} escapes plugin root"))
    }
}
fn read_json(path: &Path, label: &str) -> Result<Value, String> {
    serde_json::from_slice(&fs::read(path).map_err(|e| format!("failed to read {label}: {e}"))?)
        .map_err(|e| format!("failed to parse {label}: {e}"))
}
fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p).map_err(|e| e.to_string())?
    }
    let mut b = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    b.push(b'\n');
    fs::write(path, b).map_err(|e| e.to_string())
}
fn text<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(Value::as_str)
}
fn strings(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(Into::into).collect())
        .unwrap_or_default()
}
fn string_map(v: Option<&Value>) -> HashMap<String, String> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.into())))
                .collect()
        })
        .unwrap_or_default()
}
fn copy_tree(src: &Path, dst: &Path) -> Result<(), String> {
    if src.is_file() {
        if let Some(p) = dst.parent() {
            fs::create_dir_all(p).map_err(|e| e.to_string())?
        }
        fs::copy(src, dst).map_err(|e| e.to_string())?;
        return Ok(());
    }
    fs::create_dir_all(dst).map_err(|e| e.to_string())?;
    for e in fs::read_dir(src).map_err(|e| e.to_string())? {
        let e = e.map_err(|e| e.to_string())?;
        if e.file_type().map_err(|e| e.to_string())?.is_symlink() {
            return Err("cannot export symlink".into());
        }
        copy_tree(&e.path(), &dst.join(e.file_name()))?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    fn write(p: &Path, s: &str) {
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, s).unwrap()
    }
    fn manifest(name: &str) -> String {
        format!(r#"{{"$schema":"{PLUGIN_SCHEMA_URI}","name":"{name}"}}"#)
    }
    #[test]
    fn minimal() {
        let t = TempDir::new().unwrap();
        write(&t.path().join("plugin.json"), &manifest("minimal"));
        let p = load(t.path()).unwrap();
        assert_eq!(p.manifest.format, PackFormat::AgentPlugin);
        assert!(p.items.is_empty())
    }
    #[test]
    fn unknown_is_reported_but_known_invalid_is_fatal() {
        let t = TempDir::new().unwrap();
        write(
            &t.path().join("plugin.json"),
            &format!(
                r#"{{"$schema":"{PLUGIN_SCHEMA_URI}","name":"valid","future":true,"extensions":"bad"}}"#
            ),
        );
        assert_eq!(load(t.path()).unwrap().diagnostics.len(), 2);
        write(&t.path().join("plugin.json"), &manifest("Not Valid"));
        assert!(load(t.path()).is_err())
    }
    #[test]
    fn bad_skill_is_isolated() {
        let t = TempDir::new().unwrap();
        write(&t.path().join("plugin.json"), &manifest("skills"));
        write(
            &t.path().join("skills/good/SKILL.md"),
            "---\nname: good\ndescription: Useful.\n---\n",
        );
        write(
            &t.path().join("skills/bad/SKILL.md"),
            "---\nname: other\ndescription: Broken.\n---\n",
        );
        let p = load(t.path()).unwrap();
        assert_eq!(
            p.items,
            vec![PackItem::file(PackItemKind::Skill, "good", "skills/good")]
        );
        assert!(p.diagnostics.iter().any(|n| n.contains("bad")))
    }
    #[test]
    fn bad_mcp_envelope_does_not_hide_skills() {
        let t = TempDir::new().unwrap();
        write(&t.path().join("plugin.json"), &manifest("mixed"));
        write(
            &t.path().join("skills/good/SKILL.md"),
            "---\nname: good\ndescription: Useful.\n---\n",
        );
        write(
            &t.path().join("mcp.json"),
            r#"{"$schema":"wrong","mcpServers":{}}"#,
        );
        let p = load(t.path()).unwrap();
        assert_eq!(p.items.len(), 1);
        assert!(p.diagnostics.iter().any(|n| n.contains("disabled MCP")))
    }
    #[test]
    fn one_bad_server_is_isolated() {
        let t = TempDir::new().unwrap();
        write(&t.path().join("plugin.json"), &manifest("mcp"));
        write(
            &t.path().join("mcp.json"),
            &format!(
                r#"{{"$schema":"{MCP_SCHEMA_URI}","mcpServers":{{"good":{{"type":"streamable-http","url":"https://example.com/mcp"}},"bad":{{"type":"stdio","command":"../x"}}}}}}"#
            ),
        );
        let p = load(t.path()).unwrap();
        assert_eq!(p.mcp_servers["good"].config.transport, "http");
        assert!(!p.mcp_servers.contains_key("bad"))
    }
    #[cfg(unix)]
    #[test]
    fn escaping_skill_symlink_is_refused() {
        use std::os::unix::fs::symlink;
        let t = TempDir::new().unwrap();
        let o = TempDir::new().unwrap();
        write(&t.path().join("plugin.json"), &manifest("links"));
        write(
            &t.path().join("skills/good/SKILL.md"),
            "---\nname: good\ndescription: Useful.\n---\n",
        );
        write(
            &o.path().join("SKILL.md"),
            "---\nname: bad\ndescription: Outside.\n---\n",
        );
        symlink(o.path(), t.path().join("skills/bad")).unwrap();
        let p = load(t.path()).unwrap();
        assert_eq!(p.items.len(), 1);
        assert!(p
            .diagnostics
            .iter()
            .any(|n| n.contains("escapes plugin root")))
    }
    #[test]
    fn export_round_trip() {
        let t = TempDir::new().unwrap();
        write(&t.path().join("plugin.json"), &manifest("round-trip"));
        write(
            &t.path().join("skills/good/SKILL.md"),
            "---\nname: good\ndescription: Useful.\n---\n",
        );
        write(
            &t.path().join("mcp.json"),
            &format!(
                r#"{{"$schema":"{MCP_SCHEMA_URI}","mcpServers":{{"remote":{{"type":"streamable-http","url":"https://example.com/mcp"}}}}}}"#
            ),
        );
        let p = load(t.path()).unwrap();
        let out = TempDir::new().unwrap();
        let dest = out.path().join("plugin");
        export(&dest, &p.manifest, &p.items, &p.mcp_servers).unwrap();
        let q = load(&dest).unwrap();
        assert_eq!(q.items, p.items);
        assert_eq!(q.mcp_servers, p.mcp_servers);
        assert_eq!(q.manifest.name, p.manifest.name);
        assert_eq!(q.manifest.version, p.manifest.version);
        assert_eq!(q.manifest.description, p.manifest.description);
        assert_eq!(q.manifest.author, p.manifest.author);
        assert_eq!(q.manifest.homepage, p.manifest.homepage);
        assert_eq!(q.manifest.keywords, p.manifest.keywords);
    }
}
