//! bginf's configuration files: `nodes.yaml` and `profiles/*.yaml`.

use std::fmt;
use std::path::Path;

use serde::Deserialize;

use super::wire::{Profile, Profiles};

/// One node of `nodes.yaml`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct NodeEntry {
    pub ip: Option<String>,
    /// bginf's default pool name is `default`.
    pub pool: Option<String>,
    pub outlet: Option<String>,
    pub power_sensor: Option<String>,
    pub brick_idle_w: Option<f64>,
}

impl NodeEntry {
    pub fn pool_name(&self) -> &str {
        self.pool.as_deref().unwrap_or("default")
    }
}

/// bginf's configuration as its files hold it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BginfFiles {
    /// Nodes in file order, which is the order bginf lists pools in.
    pub nodes: Vec<(String, NodeEntry)>,
    pub profiles: Profiles,
}

#[derive(Debug)]
pub enum FilesError {
    Read { path: String, error: String },
    Parse { path: String, error: String },
}

impl fmt::Display for FilesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, error } => write!(f, "read {path}: {error}"),
            Self::Parse { path, error } => write!(f, "parse {path}: {error}"),
        }
    }
}

impl std::error::Error for FilesError {}

impl BginfFiles {
    /// Load `<dir>/nodes.yaml` and `<dir>/profiles/*.yaml`.
    pub fn load(dir: &Path) -> Result<BginfFiles, FilesError> {
        let read = |path: &Path| {
            std::fs::read_to_string(path).map_err(|err| FilesError::Read {
                path: path.display().to_string(),
                error: err.to_string(),
            })
        };
        let nodes_path = dir.join("nodes.yaml");
        let nodes = read(&nodes_path)?;
        let profiles_dir = dir.join("profiles");
        let entries = std::fs::read_dir(&profiles_dir).map_err(|err| FilesError::Read {
            path: profiles_dir.display().to_string(),
            error: err.to_string(),
        })?;
        let mut paths: Vec<_> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
            .collect();
        paths.sort();
        let mut profiles = Vec::new();
        for path in &paths {
            profiles.push((path.display().to_string(), read(path)?));
        }
        let profiles: Vec<(&str, &str)> = profiles
            .iter()
            .map(|(path, text)| (path.as_str(), text.as_str()))
            .collect();
        BginfFiles::parse(&nodes, &profiles)
    }

    /// Parse the files' text. Profiles come as `(name, text)` in the order
    /// bginf loads them; one without an `id` is skipped, as bginf skips it.
    pub fn parse(nodes: &str, profiles: &[(&str, &str)]) -> Result<BginfFiles, FilesError> {
        let parse_error = |path: &str, err: serde_yaml::Error| FilesError::Parse {
            path: path.to_string(),
            error: err.to_string(),
        };
        let mapping: Option<serde_yaml::Mapping> =
            serde_yaml::from_str(nodes).map_err(|err| parse_error("nodes.yaml", err))?;
        let mut node_entries = Vec::new();
        for (key, value) in mapping.unwrap_or_default() {
            let Some(name) = key.as_str() else {
                return Err(FilesError::Parse {
                    path: "nodes.yaml".into(),
                    error: format!("node key {key:?} is not a string"),
                });
            };
            let entry: NodeEntry =
                serde_yaml::from_value(value).map_err(|err| parse_error("nodes.yaml", err))?;
            node_entries.push((name.to_string(), entry));
        }
        let mut parsed = Profiles::new();
        for (path, text) in profiles {
            let profile: Option<Profile> =
                serde_yaml::from_str(text).map_err(|err| parse_error(path, err))?;
            let Some(profile) = profile else { continue };
            let Some(id) = profile.id.clone() else {
                continue;
            };
            parsed.insert(id, profile);
        }
        Ok(BginfFiles {
            nodes: node_entries,
            profiles: parsed,
        })
    }
}
