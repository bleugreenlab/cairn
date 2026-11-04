//! Claude Code's `.claude-plugin/marketplace.json`.
//!
//! A marketplace lists plugins, each with a `source`: a directory of the same
//! repository, or another repository. Cairn reads the subset it can verify:
//! relative directories, and git repositories pinned to a full commit `sha`.
//! An entry may also list explicit `skills`/`agents` paths (`strict: false` in
//! Claude Code terms), which is how `anthropics/skills` exposes its document
//! skills from a single repository root. Anything else becomes a note on the
//! index rather than an error, so one unusual plugin does not cost the rest.

use serde_json::Value;

use super::{is_full_commit, is_safe_relative};
use crate::manifest::PackAuthor;

pub const MARKETPLACE_FILE: &str = ".claude-plugin/marketplace.json";

/// Where one plugin's tree lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginSource {
    /// A directory of the marketplace's own repository (`""` is its root).
    Relative(String),
    /// A git repository at a pinned commit, optionally a subdirectory of it.
    Git {
        url: String,
        commit: String,
        path: String,
    },
}

/// One plugin entry Cairn can derive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSpec {
    pub name: String,
    pub source: PluginSource,
    /// Explicit skill directories, relative to the plugin root.
    pub skills: Option<Vec<String>>,
    /// Explicit agent files, relative to the plugin root.
    pub agents: Option<Vec<String>>,
    pub description: Option<String>,
    pub version: Option<String>,
    pub author: Option<PackAuthor>,
    pub license: Option<String>,
    pub homepage: Option<String>,
    pub keywords: Vec<String>,
    /// Components the entry declares that Cairn has no equivalent for.
    pub notes: Vec<String>,
}

impl PluginSpec {
    pub fn is_selection(&self) -> bool {
        self.skills.is_some() || self.agents.is_some()
    }
}

/// Parse a marketplace file into the plugins Cairn can derive, plus a note for
/// each one it skipped.
pub fn parse(text: &str) -> Result<(Vec<PluginSpec>, Vec<String>), String> {
    let value: Value =
        serde_json::from_str(text).map_err(|e| format!("{MARKETPLACE_FILE} is not JSON: {e}"))?;
    let plugins = value
        .get("plugins")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{MARKETPLACE_FILE} has no `plugins` array"))?;
    let plugin_root = value
        .pointer("/metadata/pluginRoot")
        .and_then(Value::as_str)
        .map(|root| strip_dot(root).trim_end_matches('/').to_string());

    let mut specs = Vec::new();
    let mut notes = Vec::new();
    for plugin in plugins {
        let name = plugin
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();
        match parse_plugin(plugin, &name, plugin_root.as_deref()) {
            Ok(spec) => specs.push(spec),
            Err(reason) => notes.push(format!("plugin `{name}` skipped: {reason}")),
        }
    }
    Ok((specs, notes))
}

fn parse_plugin(
    plugin: &Value,
    name: &str,
    plugin_root: Option<&str>,
) -> Result<PluginSpec, String> {
    if name == "<unnamed>" {
        return Err("it has no `name`".into());
    }
    let source = match plugin.get("source") {
        Some(Value::String(path)) => {
            let explicit = path.starts_with("./") || path == ".";
            let path = strip_dot(path).trim_end_matches('/').to_string();
            let path = match plugin_root {
                Some(root) if !explicit && !root.is_empty() => format!("{root}/{path}"),
                _ => path,
            };
            if !is_safe_relative(&path) {
                return Err(format!("its source path `{path}` leaves the repository"));
            }
            PluginSource::Relative(path)
        }
        Some(object @ Value::Object(_)) => parse_remote(object)?,
        _ => return Err("it has no `source`".into()),
    };
    let string = |key: &str| plugin.get(key).and_then(Value::as_str).map(str::to_string);
    let paths = |key: &str| -> Result<Option<Vec<String>>, String> {
        let Some(value) = plugin.get(key) else {
            return Ok(None);
        };
        let list = value
            .as_array()
            .ok_or_else(|| format!("its `{key}` is not an array"))?;
        list.iter()
            .map(|item| {
                let raw = item
                    .as_str()
                    .ok_or_else(|| format!("its `{key}` holds a non-string"))?;
                let path = strip_dot(raw).trim_end_matches('/').to_string();
                if path.is_empty() || !is_safe_relative(&path) {
                    return Err(format!("its `{key}` path `{raw}` leaves the plugin"));
                }
                Ok(path)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    };
    let mut notes = Vec::new();
    for component in [
        "commands",
        "hooks",
        "outputStyles",
        "mcpServers",
        "lspServers",
    ] {
        if plugin.get(component).is_some() {
            notes.push(format!(
                "ignored marketplace component: {component} (not read from a marketplace entry)"
            ));
        }
    }
    Ok(PluginSpec {
        name: name.to_string(),
        source,
        skills: paths("skills")?,
        agents: paths("agents")?,
        description: string("description"),
        version: string("version"),
        author: match plugin.get("author") {
            Some(Value::String(name)) => Some(PackAuthor {
                name: Some(name.clone()),
                ..Default::default()
            }),
            Some(object @ Value::Object(_)) => serde_json::from_value(object.clone()).ok(),
            _ => None,
        },
        license: string("license"),
        homepage: string("homepage").or_else(|| string("repository")),
        keywords: plugin
            .get("keywords")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        notes,
    })
}

fn parse_remote(object: &Value) -> Result<PluginSource, String> {
    let kind = object.get("source").and_then(Value::as_str).unwrap_or("");
    let field = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_string);
    let (url, path) = match kind {
        "github" => {
            let repo = field("repo").ok_or("its github source has no `repo`")?;
            let valid = repo.split('/').count() == 2
                && repo.split('/').all(|part| {
                    !part.is_empty()
                        && !part.starts_with('.')
                        && part
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
                });
            if !valid {
                return Err(format!("its github repo `{repo}` is not `owner/name`"));
            }
            (format!("https://github.com/{repo}"), String::new())
        }
        "url" => (
            field("url").ok_or("its url source has no `url`")?,
            String::new(),
        ),
        "git-subdir" => {
            let path = strip_dot(&field("path").ok_or("its git-subdir source has no `path`")?)
                .trim_end_matches('/')
                .to_string();
            if !is_safe_relative(&path) {
                return Err(format!(
                    "its git-subdir path `{path}` leaves the repository"
                ));
            }
            (
                field("url").ok_or("its git-subdir source has no `url`")?,
                path,
            )
        }
        other => {
            return Err(format!(
                "`{other}` sources are not supported; only relative directories and pinned git \
                 repositories are"
            ))
        }
    };
    if !url.starts_with("https://") {
        return Err(format!("its repository `{url}` is not https"));
    }
    let commit = field("sha").unwrap_or_default();
    if !is_full_commit(&commit) {
        return Err(format!(
            "its repository `{url}` is not pinned to a full commit `sha`; the commit is the \
             trust anchor"
        ));
    }
    Ok(PluginSource::Git { url, commit, path })
}

fn strip_dot(path: &str) -> &str {
    if path == "." {
        return "";
    }
    path.strip_prefix("./").unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "ca45fc4a0f3e2c81b2b8f7e0d9c6a5b4e3f2a1b0";

    #[test]
    fn relative_pinned_and_unsupported_sources() {
        let text = format!(
            r#"{{"name":"m","plugins":[
              {{"name":"linear","source":"./plugins/linear"}},
              {{"name":"document-skills","source":{{"source":"github","repo":"anthropics/skills","sha":"{SHA}"}},
                "strict":false,"skills":["./skills/pdf","./skills/docx"],"license":"Proprietary"}},
              {{"name":"sub","source":{{"source":"git-subdir","url":"https://example.com/r.git","path":"./p","sha":"{SHA}"}}}},
              {{"name":"floating","source":{{"source":"github","repo":"a/b"}}}},
              {{"name":"npm-one","source":{{"source":"npm","package":"x"}}}},
              {{"name":"escape","source":"../outside"}}
            ]}}"#
        );
        let (specs, notes) = parse(&text).unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(
            specs[0].source,
            PluginSource::Relative("plugins/linear".into())
        );
        assert!(!specs[0].is_selection());
        assert_eq!(
            specs[1].source,
            PluginSource::Git {
                url: "https://github.com/anthropics/skills".into(),
                commit: SHA.into(),
                path: String::new()
            }
        );
        assert_eq!(
            specs[1].skills.as_deref(),
            Some(&["skills/pdf".to_string(), "skills/docx".to_string()][..])
        );
        assert_eq!(
            specs[2].source,
            PluginSource::Git {
                url: "https://example.com/r.git".into(),
                commit: SHA.into(),
                path: "p".into()
            }
        );
        assert!(notes
            .iter()
            .any(|n| n.starts_with("plugin `floating` skipped") && n.contains("trust anchor")));
        assert!(notes
            .iter()
            .any(|n| n.starts_with("plugin `npm-one` skipped: `npm` sources are not supported")));
        assert!(notes
            .iter()
            .any(|n| n.starts_with("plugin `escape` skipped")));
    }
}
