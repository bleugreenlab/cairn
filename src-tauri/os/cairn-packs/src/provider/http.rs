//! The declarative HTTP adapter: a request template and an explicit result
//! mapping, compiled once and shared by every provider that declares one.
//!
//! # The language
//!
//! A request is a literal method, a URL, header values and an optional JSON
//! body, any of which may interpolate `${query}` (web search), `${target}` (web
//! fetch), `${credential}`, or `${options.<key>}` (a declared option's stored
//! value, else its default). The URL's scheme and host are literal and checked
//! against the credential's `hosts` at load; every URL interpolation names its
//! encoding (`|query` percent-encodes everything outside the RFC 3986
//! unreserved set, `|raw` inserts the value as is), so the choice is never
//! implicit. A JSON scalar that is exactly `${x}` takes `x`'s typed value; one
//! embedded in a longer string interpolates as text. Header names and JSON keys
//! are literal.
//!
//! A web-search response names the dotted path to its results array and, for
//! each of `title`, `url`, `snippet` and `score`, an ordered list of paths
//! inside an item: the first value that is present, not null, and (for text)
//! not blank wins, untrimmed. A missing or null node means "absent", so a
//! missing results array is an empty list. A blank title falls back to the url;
//! that is the slot's rule, not something a provider declares.
//!
//! A fetch response is either `markdown: body`, the body itself handed to the
//! slot's content-type-aware conversion, or `markdown: {json: [paths]}`, an
//! ordered path list into a JSON body under the same first-present rule, whose
//! value must be text and is returned as is. A JSON response may add a success
//! gate, `success: {path, error: [paths]}`: on a 2xx response the value at
//! `path` must be boolean `true`, and `false` is a failed fetch carrying the
//! first `error` path's text. On a non-2xx response the same `error` paths name
//! the failure's detail.
//!
//! A stored option value of the wrong type, or a select value that is no
//! longer one of the declared choices, falls back to the declared default.
//!
//! # What it deliberately cannot do
//!
//! No conditionals, loops beyond the one results array, arithmetic, regex,
//! string functions, scripting, guessing at a response's shape, interpolation
//! into a host or a header name, multi-request flows, or reading response
//! headers. A provider that needs one of these is not a declarative provider:
//! it binds an MCP tool or ships its adapter as a pack tool instead.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use super::{CredentialDecl, ProviderDecl, Slot};
use crate::provider_options::{OptionControl, ProviderOption};

/// A parsed, validated HTTP adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct HttpAdapter {
    pub request: RequestTemplate,
    pub response: ResponseMapping,
}

/// The request half of an adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestTemplate {
    pub method: String,
    /// The literal host the URL names, lowercased.
    pub host: String,
    url: Template,
    headers: Vec<(String, Template)>,
    json: Option<JsonTemplate>,
}

/// How a slot reads the provider's response.
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseMapping {
    /// A web-search results array and its field paths.
    Search(SearchMapping),
    /// The response body itself, handed to the fetch slot's markdown
    /// conversion (content-type aware, binary refused).
    MarkdownBody,
    /// Markdown read out of a JSON body, optionally behind a success gate.
    MarkdownJson(JsonMarkdown),
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonMarkdown {
    paths: Vec<Vec<String>>,
    success: Option<SuccessGate>,
}

#[derive(Debug, Clone, PartialEq)]
struct SuccessGate {
    path: Vec<String>,
    error: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchMapping {
    results: Vec<String>,
    title: Vec<Vec<String>>,
    url: Vec<Vec<String>>,
    snippet: Vec<Vec<String>>,
    score: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Var {
    Query,
    Target,
    Credential,
    Option(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Filter {
    Query,
    Raw,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Piece {
    Lit(String),
    Var(Var, Option<Filter>),
}

pub(crate) type Template = Vec<Piece>;

/// A JSON value with interpolations: an HTTP body, or an MCP binding's tool
/// arguments.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JsonTemplate {
    Typed(Var),
    Text(Template),
    Literal(Value),
    Array(Vec<JsonTemplate>),
    Object(Vec<(String, JsonTemplate)>),
}

impl JsonTemplate {
    pub(crate) fn empty_object() -> Self {
        JsonTemplate::Object(Vec::new())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdapterFile {
    request: RequestFile,
    response: ResponseFile,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestFile {
    method: String,
    url: String,
    #[serde(default)]
    headers: serde_yaml::Mapping,
    #[serde(default)]
    json: Option<serde_yaml::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseFile {
    #[serde(default)]
    results: Option<String>,
    #[serde(default)]
    fields: Option<FieldsFile>,
    #[serde(default)]
    markdown: Option<serde_yaml::Value>,
    #[serde(default)]
    success: Option<SuccessFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SuccessFile {
    path: String,
    #[serde(default)]
    error: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarkdownJsonFile {
    json: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldsFile {
    #[serde(default)]
    title: Vec<String>,
    url: Vec<String>,
    #[serde(default)]
    snippet: Vec<String>,
    #[serde(default)]
    score: Vec<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum Place {
    Url,
    Header,
    Json,
}

struct Scope<'a> {
    slot: Slot,
    options: &'a [ProviderOption],
    /// Whether `${credential}` is defined: an HTTP adapter sends its pack
    /// credential, an MCP binding has none.
    credential: bool,
}

impl Scope<'_> {
    fn var(&self, name: &str) -> Result<Var, String> {
        match name {
            "query" if self.slot == Slot::WebSearch => Ok(Var::Query),
            "query" => Err("`${query}` is defined only for a web-search provider".to_string()),
            "target" if self.slot != Slot::WebSearch => Ok(Var::Target),
            "target" => Err("`${target}` is not defined for a web-search provider".to_string()),
            "credential" if self.credential => Ok(Var::Credential),
            "credential" => Err(
                "`${credential}` is not available to an `mcp` adapter: its server's own \
                 connection authenticates it"
                    .to_string(),
            ),
            other => match other.strip_prefix("options.") {
                Some(key) if self.options.iter().any(|option| option.key == key) => {
                    Ok(Var::Option(key.to_string()))
                }
                Some(key) => Err(format!("`${{options.{key}}}` names an undeclared option")),
                None => Err(format!(
                    "`${{{other}}}` is not a variable (query, target, credential, options.<key>)"
                )),
            },
        }
    }

    fn template(&self, text: &str, place: Place) -> Result<Template, String> {
        let mut pieces = Vec::new();
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            if start > 0 {
                pieces.push(Piece::Lit(rest[..start].to_string()));
            }
            let after = &rest[start + 2..];
            let end = after
                .find('}')
                .ok_or_else(|| format!("unterminated `${{` in `{text}`"))?;
            let inner = &after[..end];
            let (name, filter) = match inner.split_once('|') {
                Some((name, "query")) => (name, Some(Filter::Query)),
                Some((name, "raw")) => (name, Some(Filter::Raw)),
                Some((_, other)) => {
                    return Err(format!(
                        "unknown encoding `|{other}` in `{text}` (query, raw)"
                    ))
                }
                None => (inner, None),
            };
            let var = self.var(name.trim())?;
            match (place, filter) {
                (Place::Url, None) => {
                    return Err(format!(
                        "`${{{inner}}}` in `url` must name its encoding: `|query` or `|raw`"
                    ))
                }
                (Place::Header | Place::Json, Some(_)) => {
                    return Err(format!(
                        "`${{{inner}}}`: an encoding applies only inside `url`"
                    ))
                }
                _ => {}
            }
            pieces.push(Piece::Var(var, filter));
            rest = &after[end + 1..];
        }
        if !rest.is_empty() {
            pieces.push(Piece::Lit(rest.to_string()));
        }
        Ok(pieces)
    }

    fn json(&self, value: serde_yaml::Value) -> Result<JsonTemplate, String> {
        Ok(match value {
            serde_yaml::Value::String(text) => {
                let template = self.template(&text, Place::Json)?;
                match template.as_slice() {
                    [Piece::Var(var, None)] => JsonTemplate::Typed(var.clone()),
                    [] | [Piece::Lit(_)] => JsonTemplate::Literal(Value::String(text)),
                    _ => JsonTemplate::Text(template),
                }
            }
            serde_yaml::Value::Sequence(items) => JsonTemplate::Array(
                items
                    .into_iter()
                    .map(|item| self.json(item))
                    .collect::<Result<_, _>>()?,
            ),
            serde_yaml::Value::Mapping(map) => {
                let mut fields = Vec::new();
                for (key, value) in map {
                    let key = key
                        .as_str()
                        .ok_or_else(|| "a `json` key must be text".to_string())?
                        .to_string();
                    if key.contains("${") {
                        return Err(format!("`json` key `{key}` must be literal"));
                    }
                    fields.push((key, self.json(value)?));
                }
                JsonTemplate::Object(fields)
            }
            serde_yaml::Value::Tagged(_) => {
                return Err("a `json` value may not carry a YAML tag".to_string())
            }
            literal => JsonTemplate::Literal(
                serde_json::to_value(&literal).map_err(|e| format!("`json`: {e}"))?,
            ),
        })
    }
}

fn parse_path(text: &str) -> Result<Vec<String>, String> {
    let segments: Vec<String> = text.split('.').map(str::to_string).collect();
    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(format!("`{text}` is not a dotted path"));
    }
    Ok(segments)
}

fn parse_paths(paths: &[String]) -> Result<Vec<Vec<String>>, String> {
    paths.iter().map(|path| parse_path(path)).collect()
}

/// Check the URL's literal scheme and host, returning the host.
fn url_host(url: &str, credential: &CredentialDecl) -> Result<String, String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| format!("`url` must start with `https://`, not `{url}`"))?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    if authority.contains("${") {
        return Err("`url` interpolates into its host; the scheme and host must be literal".into());
    }
    if authority.contains('@') {
        return Err("`url` must not carry user information before its host".to_string());
    }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => host,
        Some(_) => return Err(format!("`url` has a malformed port in `{authority}`")),
        None => authority,
    }
    .to_ascii_lowercase();
    if host.is_empty() {
        return Err("`url` names no host".to_string());
    }
    if !credential.hosts.contains(&host) {
        return Err(format!(
            "would send credential `{}` ({}) to `{host}`, which is not in its `hosts`",
            credential.name,
            credential.var().unwrap_or("oauth")
        ));
    }
    Ok(host)
}

/// Parse a JSON template with the slot's variables and the provider's options,
/// and no credential: an MCP binding's tool arguments.
pub(crate) fn json_template(
    slot: Slot,
    options: &[ProviderOption],
    value: serde_yaml::Value,
) -> Result<JsonTemplate, String> {
    Scope {
        slot,
        options,
        credential: false,
    }
    .json(value)
}

fn parse_markdown(response: ResponseFile) -> Result<ResponseMapping, String> {
    if response.results.is_some() || response.fields.is_some() {
        return Err("a fetch response maps `markdown`, not `results`".to_string());
    }
    let paths = match response.markdown {
        Some(serde_yaml::Value::String(mode)) if mode == "body" => None,
        Some(value @ serde_yaml::Value::Mapping(_)) => {
            let file: MarkdownJsonFile =
                serde_yaml::from_value(value).map_err(|e| format!("`markdown`: {e}"))?;
            if file.json.is_empty() {
                return Err("`markdown.json` must name at least one path".to_string());
            }
            Some(parse_paths(&file.json)?)
        }
        _ => {
            return Err(
                "a fetch response needs `markdown: body` or `markdown: {json: [paths]}`"
                    .to_string(),
            )
        }
    };
    match (paths, response.success) {
        (None, None) => Ok(ResponseMapping::MarkdownBody),
        (None, Some(_)) => Err(
            "a `success` gate reads a JSON body, so it needs `markdown: {json: [paths]}`"
                .to_string(),
        ),
        (Some(paths), success) => Ok(ResponseMapping::MarkdownJson(JsonMarkdown {
            paths,
            success: success
                .map(|gate| {
                    Ok::<_, String>(SuccessGate {
                        path: parse_path(&gate.path)?,
                        error: parse_paths(&gate.error)?,
                    })
                })
                .transpose()?,
        })),
    }
}

pub(super) fn parse_adapter(
    slot: Slot,
    options: &[ProviderOption],
    credential: &CredentialDecl,
    body: serde_yaml::Value,
) -> Result<HttpAdapter, String> {
    let file: AdapterFile =
        serde_yaml::from_value(body).map_err(|e| format!("adapter `http`: {e}"))?;
    let scope = Scope {
        slot,
        options,
        credential: true,
    };

    let method = file.request.method.to_ascii_uppercase();
    if !["GET", "POST", "PUT", "PATCH", "DELETE"].contains(&method.as_str()) {
        return Err(format!("unsupported `method` `{}`", file.request.method));
    }
    let host = url_host(&file.request.url, credential)?;
    let url = scope.template(&file.request.url, Place::Url)?;
    let mut headers = Vec::new();
    for (name, value) in file.request.headers {
        let name = name
            .as_str()
            .ok_or_else(|| "a header name must be text".to_string())?
            .to_string();
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(format!(
                "header name `{name}` must be literal letters, digits or `-`"
            ));
        }
        let value = value
            .as_str()
            .ok_or_else(|| format!("header `{name}` must be text"))?;
        headers.push((name, scope.template(value, Place::Header)?));
    }
    let json = file.request.json.map(|json| scope.json(json)).transpose()?;
    if json.is_some() && method == "GET" {
        return Err("a `GET` request cannot carry a `json` body".to_string());
    }

    let response = file.response;
    let response = match slot {
        Slot::WebSearch => {
            if response.markdown.is_some() {
                return Err("a web-search response maps `results`, not `markdown`".to_string());
            }
            if response.success.is_some() {
                return Err(
                    "a `success` gate applies to a fetch response's `markdown: {json}`".to_string(),
                );
            }
            let results = response
                .results
                .ok_or_else(|| "a web-search response needs `results`".to_string())?;
            let fields = response
                .fields
                .ok_or_else(|| "a web-search response needs `fields`".to_string())?;
            if fields.url.is_empty() {
                return Err("`fields.url` must name at least one path".to_string());
            }
            ResponseMapping::Search(SearchMapping {
                results: parse_path(&results)?,
                title: parse_paths(&fields.title)?,
                url: parse_paths(&fields.url)?,
                snippet: parse_paths(&fields.snippet)?,
                score: parse_paths(&fields.score)?,
            })
        }
        Slot::WebFetch | Slot::Pdf => parse_markdown(response)?,
    };

    Ok(HttpAdapter {
        request: RequestTemplate {
            method,
            host,
            url,
            headers,
            json,
        },
        response,
    })
}

/// The values a request interpolates.
pub struct Inputs<'a> {
    pub query: Option<&'a str>,
    pub target: Option<&'a str>,
    pub credential: &'a str,
    /// The stored options, `<mapkey>.<pack>` in `settings.yaml`.
    pub options: &'a HashMap<String, serde_yaml::Value>,
}

/// A fully resolved request, separated from the network so it is testable.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}

/// One option's effective value: the stored value when it has the right type,
/// else the declared default. A stored non-integer for an integer option falls
/// back to the default, as a negative one clamps to zero, and a stored select
/// value that is not one of the declared choices falls back to the default.
fn option_value(option: &ProviderOption, stored: Option<&serde_yaml::Value>) -> Value {
    match &option.control {
        OptionControl::Text { default, .. } => Value::String(
            stored
                .and_then(serde_yaml::Value::as_str)
                .unwrap_or(default)
                .to_string(),
        ),
        OptionControl::Select { default, choices } => Value::String(
            stored
                .and_then(serde_yaml::Value::as_str)
                .filter(|value| choices.iter().any(|choice| choice.value == *value))
                .unwrap_or(default)
                .to_string(),
        ),
        OptionControl::Number {
            default, integer, ..
        } => {
            if *integer {
                let stored = stored.and_then(|value| {
                    value
                        .as_u64()
                        .or_else(|| value.as_i64().map(|i| i.max(0) as u64))
                });
                Value::from(stored.unwrap_or(*default as u64))
            } else {
                let stored = stored.and_then(serde_yaml::Value::as_f64);
                serde_json::Number::from_f64(stored.unwrap_or(*default))
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
        }
        OptionControl::Bool { default } => Value::Bool(
            stored
                .and_then(serde_yaml::Value::as_bool)
                .unwrap_or(*default),
        ),
    }
}

fn text_of(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Percent-encode a URL query component: the RFC 3986 unreserved set passes
/// through, every other byte becomes `%XX` in uppercase.
fn encode_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

struct Resolver<'a> {
    options: &'a [ProviderOption],
    inputs: &'a Inputs<'a>,
}

impl Resolver<'_> {
    fn value(&self, var: &Var) -> Value {
        match var {
            Var::Query => Value::String(self.inputs.query.unwrap_or_default().to_string()),
            Var::Target => Value::String(self.inputs.target.unwrap_or_default().to_string()),
            Var::Credential => Value::String(self.inputs.credential.to_string()),
            Var::Option(key) => self
                .options
                .iter()
                .find(|option| &option.key == key)
                .map(|option| option_value(option, self.inputs.options.get(key)))
                .unwrap_or(Value::Null),
        }
    }

    fn text(&self, template: &Template) -> String {
        let mut out = String::new();
        for piece in template {
            match piece {
                Piece::Lit(text) => out.push_str(text),
                Piece::Var(var, filter) => {
                    let text = text_of(&self.value(var));
                    match filter {
                        Some(Filter::Query) => out.push_str(&encode_component(&text)),
                        Some(Filter::Raw) | None => out.push_str(&text),
                    }
                }
            }
        }
        out
    }

    fn json(&self, template: &JsonTemplate) -> Value {
        match template {
            JsonTemplate::Typed(var) => self.value(var),
            JsonTemplate::Text(template) => Value::String(self.text(template)),
            JsonTemplate::Literal(value) => value.clone(),
            JsonTemplate::Array(items) => {
                Value::Array(items.iter().map(|item| self.json(item)).collect())
            }
            JsonTemplate::Object(fields) => Value::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), self.json(value)))
                    .collect(),
            ),
        }
    }
}

/// Resolve a JSON template against the provider's options and the inputs.
pub(crate) fn resolve_json(
    options: &[ProviderOption],
    template: &JsonTemplate,
    inputs: &Inputs<'_>,
) -> Value {
    Resolver { options, inputs }.json(template)
}

/// Build the provider's request. Pure: the credential is passed in, nothing is
/// sent, and every variable was checked when the declaration loaded.
pub fn prepare(
    provider: &ProviderDecl,
    adapter: &HttpAdapter,
    inputs: &Inputs<'_>,
) -> PreparedRequest {
    let request = &adapter.request;
    let resolver = Resolver {
        options: &provider.options,
        inputs,
    };
    PreparedRequest {
        method: request.method.clone(),
        url: resolver.text(&request.url),
        headers: request
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), resolver.text(value)))
            .collect(),
        body: request
            .json
            .as_ref()
            .map(|json| resolver.json(json).to_string()),
    }
}

/// One web-search result in the slot's contract.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub score: Option<f64>,
}

fn describe(path: &[String]) -> String {
    if path.is_empty() {
        "the response".to_string()
    } else {
        format!("`{}`", path.join("."))
    }
}

/// Follow `path` from `value`. A missing or null node is `None`; stepping into
/// a value that is neither an object nor a list is a shape error.
fn lookup<'a>(value: &'a Value, path: &[String]) -> Result<Option<&'a Value>, String> {
    let mut node = value;
    for (depth, segment) in path.iter().enumerate() {
        node = match node {
            Value::Null => return Ok(None),
            Value::Object(map) => match map.get(segment) {
                Some(next) => next,
                None => return Ok(None),
            },
            Value::Array(items) => match segment.parse::<usize>() {
                Ok(index) => match items.get(index) {
                    Some(next) => next,
                    None => return Ok(None),
                },
                Err(_) => {
                    return Err(format!(
                        "{} is a list, not an object",
                        describe(&path[..depth])
                    ))
                }
            },
            _ => return Err(format!("{} is not an object", describe(&path[..depth]))),
        };
    }
    Ok(match node {
        Value::Null => None,
        other => Some(other),
    })
}

fn first_text(
    item: &Value,
    paths: &[Vec<String>],
    field: &str,
    index: usize,
) -> Result<String, String> {
    for path in paths {
        match lookup(item, path)? {
            None => continue,
            Some(Value::String(text)) if text.trim().is_empty() => continue,
            Some(Value::String(text)) => return Ok(text.clone()),
            Some(_) => {
                return Err(format!(
                    "`{field}` of result {} (`{}`) is not text",
                    index + 1,
                    path.join(".")
                ))
            }
        }
    }
    Ok(String::new())
}

fn first_number(item: &Value, paths: &[Vec<String>], index: usize) -> Result<Option<f64>, String> {
    for path in paths {
        match lookup(item, path)? {
            None => continue,
            Some(Value::Number(number)) => return Ok(number.as_f64()),
            Some(_) => {
                return Err(format!(
                    "`score` of result {} (`{}`) is not a number",
                    index + 1,
                    path.join(".")
                ))
            }
        }
    }
    Ok(None)
}

/// Why a JSON markdown response yielded no markdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkdownFailure {
    /// The success gate read `false`; carries the declared error text, if any.
    Unsuccessful(Option<String>),
    /// No markdown path held text.
    Missing,
    /// The body did not have the declared shape.
    Shape(String),
}

fn gate_error(value: &Value, gate: &SuccessGate) -> Option<String> {
    gate.error
        .iter()
        .find_map(|path| match lookup(value, path) {
            Ok(Some(Value::String(text))) if !text.trim().is_empty() => Some(text.clone()),
            _ => None,
        })
}

/// Read the markdown out of a 2xx JSON body, through the success gate.
pub fn map_markdown(mapping: &JsonMarkdown, body: &str) -> Result<String, MarkdownFailure> {
    let value: Value =
        serde_json::from_str(body).map_err(|e| MarkdownFailure::Shape(e.to_string()))?;
    if let Some(gate) = &mapping.success {
        match lookup(&value, &gate.path).map_err(MarkdownFailure::Shape)? {
            Some(Value::Bool(true)) => {}
            Some(Value::Bool(false)) => {
                return Err(MarkdownFailure::Unsuccessful(gate_error(&value, gate)))
            }
            _ => {
                return Err(MarkdownFailure::Shape(format!(
                    "{} is not a boolean",
                    describe(&gate.path)
                )))
            }
        }
    }
    for path in &mapping.paths {
        match lookup(&value, path).map_err(MarkdownFailure::Shape)? {
            None => continue,
            Some(Value::String(text)) if text.trim().is_empty() => continue,
            Some(Value::String(text)) => return Ok(text.clone()),
            Some(_) => {
                return Err(MarkdownFailure::Shape(format!(
                    "{} is not text",
                    describe(path)
                )))
            }
        }
    }
    Err(MarkdownFailure::Missing)
}

/// The declared error text in a non-2xx JSON body, if the gate names one.
pub fn error_detail(mapping: &JsonMarkdown, body: &str) -> Option<String> {
    let gate = mapping.success.as_ref()?;
    let value: Value = serde_json::from_str(body).ok()?;
    gate_error(&value, gate)
}

/// Map a web-search response body into the slot's result list. The error is a
/// description of what did not fit, for the slot's shape-error message.
pub fn map_search(mapping: &SearchMapping, body: &str) -> Result<Vec<SearchHit>, String> {
    let value: Value = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let items = match lookup(&value, &mapping.results)? {
        None => return Ok(Vec::new()),
        Some(Value::Array(items)) => items,
        Some(_) => return Err(format!("{} is not a list", describe(&mapping.results))),
    };
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            if !item.is_object() {
                return Err(format!("result {} is not an object", index + 1));
            }
            let url = first_text(item, &mapping.url, "url", index)?;
            let title = first_text(item, &mapping.title, "title", index)?;
            Ok(SearchHit {
                title: if title.trim().is_empty() {
                    url.clone()
                } else {
                    title
                },
                snippet: first_text(item, &mapping.snippet, "snippet", index)?,
                score: first_number(item, &mapping.score, index)?,
                url,
            })
        })
        .collect()
}
