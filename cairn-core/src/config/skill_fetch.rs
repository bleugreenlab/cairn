//! Fetch and install skills from GitHub or Git repositories.
//!
//! Two-phase flow:
//! 1. Parse URL → fetch skill directory → return preview
//! 2. Install fetched files to target scope directory
//!
//! Two fetch strategies:
//! - GitHub: Contents API (fast, no clone)
//! - Generic git: shallow clone to temp dir

use base64::Engine;
use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::skills::parse_skill_markdown;

use super::skills::SkillMeta;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Parsed source of a skill URL.
#[derive(Debug, Clone)]
pub enum SkillSource {
    GitHub {
        owner: String,
        repo: String,
        /// Raw string after `tree/` or `blob/` — contains `{branch}/{path}` but branch
        /// names can contain `/`, so this is resolved during fetch by trying progressively
        /// longer branch prefixes against the API.
        ref_and_path: String,
        /// Whether this was a /blob/ link to SKILL.md (derive parent dir for the path).
        is_blob_skill_md: bool,
    },
    Git {
        url: String,
        path: Option<String>,
    },
}

/// A single file fetched from a remote skill.
#[derive(Debug, Clone)]
pub struct FetchedFile {
    /// Relative path within the skill directory (e.g. "SKILL.md", "scripts/run.py")
    pub relative_path: String,
    pub content: Vec<u8>,
    pub is_binary: bool,
}

/// Complete fetched skill ready for preview/install.
#[derive(Debug, Clone)]
pub struct FetchedSkill {
    pub source_url: String,
    pub skill_id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub allowed_tools: Option<Vec<String>>,
    pub files: Vec<FetchedFile>,
    pub has_scripts: bool,
}

// ---------------------------------------------------------------------------
// URL parsing
// ---------------------------------------------------------------------------

/// Parse a URL (or shorthand) into a `SkillSource`.
///
/// Supported formats:
/// - `https://github.com/{owner}/{repo}/tree/{branch}/{path}`
/// - `https://github.com/{owner}/{repo}/blob/{branch}/{path}` (SKILL.md link → parent dir)
/// - `owner/repo/path` shorthand (assumes GitHub, main branch)
/// - Generic git URL (contains `.git` or known git hosts)
pub fn parse_skill_url(url: &str) -> Result<SkillSource, String> {
    let url = url.trim();

    // Try GitHub URL first
    if let Some(gh) = try_parse_github_url(url) {
        return Ok(gh);
    }

    // Try shorthand: owner/repo/path (no protocol, no dots in first segment)
    // For shorthand, we know the branch is "main", so ref_and_path = "main/{path}"
    if !url.contains("://") && !url.contains(".git") {
        let parts: Vec<&str> = url.splitn(3, '/').collect();
        if parts.len() >= 2 && !parts[0].contains('.') {
            let owner = parts[0].to_string();
            let repo = parts[1].to_string();
            let ref_and_path = if parts.len() == 3 {
                format!("main/{}", parts[2])
            } else {
                "main".to_string()
            };
            return Ok(SkillSource::GitHub {
                owner,
                repo,
                ref_and_path,
                is_blob_skill_md: false,
            });
        }
    }

    // Generic git URL
    if url.contains(".git") || url.starts_with("git@") || url.starts_with("git://") {
        return Ok(SkillSource::Git {
            url: url.to_string(),
            path: None,
        });
    }

    Err(format!(
        "Could not parse skill URL: {}. Expected a GitHub URL, owner/repo shorthand, or git URL.",
        url
    ))
}

fn try_parse_github_url(url: &str) -> Option<SkillSource> {
    // Match: https://github.com/{owner}/{repo}/tree/{branch}/{path...}
    // or:    https://github.com/{owner}/{repo}/blob/{branch}/{path...}
    let stripped = url
        .strip_prefix("https://github.com/")
        .or_else(|| url.strip_prefix("http://github.com/"))?;

    let parts: Vec<&str> = stripped.splitn(4, '/').collect();
    if parts.len() < 2 {
        return None;
    }

    let owner = parts[0].to_string();
    let repo = parts[1].to_string();

    if parts.len() < 3 {
        // Just owner/repo — default to main, root
        return Some(SkillSource::GitHub {
            owner,
            repo,
            ref_and_path: "main".to_string(),
            is_blob_skill_md: false,
        });
    }

    let kind = parts[2]; // "tree" or "blob"
    if kind != "tree" && kind != "blob" {
        return None;
    }

    let ref_and_path = parts
        .get(3)
        .unwrap_or(&"")
        .trim_end_matches('/')
        .to_string();

    let ref_and_path = if ref_and_path.is_empty() {
        "main".to_string()
    } else {
        ref_and_path
    };

    let is_blob_skill_md = kind == "blob" && ref_and_path.ends_with("/SKILL.md");

    Some(SkillSource::GitHub {
        owner,
        repo,
        ref_and_path,
        is_blob_skill_md,
    })
}

// ---------------------------------------------------------------------------
// GitHub API fetch
// ---------------------------------------------------------------------------

/// GitHub Contents API response entry.
#[derive(Debug, Deserialize)]
struct GitHubContentEntry {
    name: String,
    path: String,
    #[serde(rename = "type")]
    entry_type: String, // "file" or "dir"
    #[serde(default)]
    content: Option<String>,
    download_url: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    size: usize,
}

/// Fetch files from a GitHub repository using the Contents API.
fn fetch_from_github(
    client: &reqwest::blocking::Client,
    owner: &str,
    repo: &str,
    branch: &str,
    path: &str,
    base_path: &str,
    depth: usize,
) -> Result<Vec<FetchedFile>, String> {
    if depth > 3 {
        return Ok(vec![]);
    }

    let api_url = if path.is_empty() {
        format!(
            "https://api.github.com/repos/{}/{}/contents?ref={}",
            owner, repo, branch
        )
    } else {
        format!(
            "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
            owner, repo, path, branch
        )
    };

    let response = client
        .get(&api_url)
        .header("Accept", "application/vnd.github.v3+json")
        .header("User-Agent", "cairn-desktop")
        .send()
        .map_err(|e| format!("GitHub API request failed: {}", e))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        return Err(format!(
            "GitHub API returned {}: {}",
            status,
            truncate_str(&body, 200)
        ));
    }

    // The API can return either a single object (for a file) or an array (for a directory).
    let text = response
        .text()
        .map_err(|e| format!("Failed to read response: {}", e))?;

    let entries: Vec<GitHubContentEntry> = if text.trim_start().starts_with('[') {
        serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse GitHub response: {}", e))?
    } else {
        // Single file
        let entry: GitHubContentEntry = serde_json::from_str(&text)
            .map_err(|e| format!("Failed to parse GitHub response: {}", e))?;
        vec![entry]
    };

    let mut files = Vec::new();

    for entry in entries {
        if entry.name.starts_with('.') && entry.name != ".meta.json" {
            continue; // Skip hidden files except .meta.json
        }

        if entry.entry_type == "file" {
            let relative_path = if base_path.is_empty() {
                entry.name.clone()
            } else {
                let full = &entry.path;
                full.strip_prefix(base_path)
                    .unwrap_or(full)
                    .trim_start_matches('/')
                    .to_string()
            };

            // Try to get content from the entry itself (base64 encoded, up to 1MB)
            let content = if let Some(ref b64) = entry.content {
                let cleaned: String = b64.chars().filter(|c| !c.is_whitespace()).collect();
                base64::engine::general_purpose::STANDARD
                    .decode(&cleaned)
                    .map_err(|e| format!("Failed to decode base64 for {}: {}", entry.name, e))?
            } else if let Some(ref download_url) = entry.download_url {
                // Fetch raw content for larger files
                let resp = client
                    .get(download_url)
                    .header("User-Agent", "cairn-desktop")
                    .send()
                    .map_err(|e| format!("Failed to download {}: {}", entry.name, e))?;
                resp.bytes()
                    .map_err(|e| format!("Failed to read {}: {}", entry.name, e))?
                    .to_vec()
            } else {
                continue; // Skip entries with no content source
            };

            let is_binary = is_binary_content(&content);

            files.push(FetchedFile {
                relative_path,
                content,
                is_binary,
            });
        } else if entry.entry_type == "dir" {
            let sub_files = fetch_from_github(
                client,
                owner,
                repo,
                branch,
                &entry.path,
                base_path,
                depth + 1,
            )?;
            files.extend(sub_files);
        }
    }

    Ok(files)
}

/// Resolve the branch/path boundary in a `ref_and_path` string by trying
/// progressively longer branch prefixes against the GitHub Contents API.
///
/// For `main/skills/docx`, tries: branch=`main`, path=`skills/docx` (succeeds).
/// For `feature/import/skills/docx`, tries:
///   1. branch=`feature`, path=`import/skills/docx` → 404
///   2. branch=`feature/import`, path=`skills/docx` → succeeds
fn resolve_and_fetch_github(
    client: &reqwest::blocking::Client,
    owner: &str,
    repo: &str,
    ref_and_path: &str,
    is_blob_skill_md: bool,
) -> Result<Vec<FetchedFile>, String> {
    // Collect all possible split points (at each `/`)
    let slash_positions: Vec<usize> = ref_and_path
        .char_indices()
        .filter(|(_, c)| *c == '/')
        .map(|(i, _)| i)
        .collect();

    if slash_positions.is_empty() {
        // No slash — entire string is the branch, path is root
        let branch = ref_and_path;
        let path = "";
        return fetch_from_github(client, owner, repo, branch, path, "", 0);
    }

    // Try each split point: branch gets longer, path gets shorter
    let mut last_error = String::new();
    for &pos in &slash_positions {
        let branch = &ref_and_path[..pos];
        let mut path = &ref_and_path[pos + 1..];

        // For /blob/ links to SKILL.md, derive the parent directory
        if is_blob_skill_md {
            if let Some(parent_end) = path.rfind('/') {
                path = &path[..parent_end];
            } else {
                path = "";
            }
        }

        let base_path = if path.is_empty() {
            String::new()
        } else {
            format!("{}/", path)
        };

        match fetch_from_github(client, owner, repo, branch, path, &base_path, 0) {
            Ok(files) => return Ok(files),
            Err(e) => {
                // If this looks like a ref-not-found error, try the next split
                if e.contains("404") || e.contains("Not Found") {
                    last_error = e;
                    continue;
                }
                // Other errors (network, auth) — bail immediately
                return Err(e);
            }
        }
    }

    // All splits failed — also try treating the entire string as branch (no path)
    match fetch_from_github(client, owner, repo, ref_and_path, "", "", 0) {
        Ok(files) => Ok(files),
        Err(_) => Err(format!(
            "Could not resolve branch/path from '{}': {}",
            ref_and_path, last_error
        )),
    }
}

// ---------------------------------------------------------------------------
// Git clone fetch
// ---------------------------------------------------------------------------

/// Fetch files from a generic git repo via shallow clone.
fn fetch_from_git(url: &str, subpath: Option<&str>) -> Result<Vec<FetchedFile>, String> {
    let temp_dir =
        tempfile::tempdir().map_err(|e| format!("Failed to create temp directory: {}", e))?;

    let output = std::process::Command::new("git")
        .args(["clone", "--depth", "1", url])
        .arg(temp_dir.path().join("repo"))
        .output()
        .map_err(|e| format!("Failed to run git clone: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git clone failed: {}", truncate_str(&stderr, 300)));
    }

    let base_dir = if let Some(sp) = subpath {
        temp_dir.path().join("repo").join(sp)
    } else {
        temp_dir.path().join("repo")
    };

    if !base_dir.exists() {
        return Err(format!(
            "Path '{}' not found in repository",
            subpath.unwrap_or(".")
        ));
    }

    collect_files_recursive(&base_dir, &base_dir)
}

fn collect_files_recursive(dir: &Path, base: &Path) -> Result<Vec<FetchedFile>, String> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|e| format!("Failed to read directory: {}", e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        // Skip .git and hidden files (except .meta.json)
        if name == ".git" || (name.starts_with('.') && name != ".meta.json") {
            continue;
        }

        if path.is_dir() {
            files.extend(collect_files_recursive(&path, base)?);
        } else {
            let relative = path
                .strip_prefix(base)
                .map_err(|_| "Path prefix error".to_string())?
                .to_string_lossy()
                .to_string();
            let content =
                std::fs::read(&path).map_err(|e| format!("Failed to read {}: {}", relative, e))?;
            let is_binary = is_binary_content(&content);
            files.push(FetchedFile {
                relative_path: relative,
                content,
                is_binary,
            });
        }
    }

    Ok(files)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

fn build_fetched_skill(files: Vec<FetchedFile>, source_url: &str) -> Result<FetchedSkill, String> {
    let skill_md = files
        .iter()
        .find(|f| f.relative_path == "SKILL.md")
        .ok_or_else(|| "No SKILL.md found in the fetched directory".to_string())?;

    let content_str = String::from_utf8(skill_md.content.clone())
        .map_err(|_| "SKILL.md is not valid UTF-8".to_string())?;

    let parsed = parse_skill_markdown(&content_str)?;

    let has_scripts = files
        .iter()
        .any(|f| f.relative_path.starts_with("scripts/"));

    Ok(FetchedSkill {
        source_url: source_url.to_string(),
        skill_id: parsed.id,
        name: parsed.name,
        description: parsed.description,
        prompt: parsed.prompt,
        allowed_tools: parsed.allowed_tools,
        files,
        has_scripts,
    })
}

/// Fetch a skill from a URL, returning all files and parsed metadata.
pub fn fetch_skill(source: &SkillSource, source_url: &str) -> Result<FetchedSkill, String> {
    let files = match source {
        SkillSource::GitHub {
            owner,
            repo,
            ref_and_path,
            is_blob_skill_md,
        } => {
            let client = reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

            resolve_and_fetch_github(&client, owner, repo, ref_and_path, *is_blob_skill_md)?
        }
        SkillSource::Git { url, path } => fetch_from_git(url, path.as_deref())?,
    };

    build_fetched_skill(files, source_url)
}

/// Install fetched skill files to the target directory.
///
/// Returns the path to the installed skill directory.
pub fn install_fetched_skill(
    skill: &FetchedSkill,
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    let skills_dir = if let Some(proj) = project_path {
        proj.join(".cairn").join("skills")
    } else {
        config_dir.join("skills")
    };

    // Auto-increment suffix if skill ID already exists
    let mut final_id = skill.skill_id.clone();
    let mut counter = 1;
    while skills_dir.join(&final_id).join("SKILL.md").exists() {
        final_id = format!("{}-{}", skill.skill_id, counter);
        counter += 1;
    }

    let target_dir = skills_dir.join(&final_id);
    std::fs::create_dir_all(&target_dir)
        .map_err(|e| format!("Failed to create skill directory: {}", e))?;

    // Write all files preserving directory structure
    for file in &skill.files {
        let dest = target_dir.join(&file.relative_path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        std::fs::write(&dest, &file.content)
            .map_err(|e| format!("Failed to write {}: {}", file.relative_path, e))?;
    }

    // Write .meta.json with source_url
    let now = chrono::Utc::now().to_rfc3339();
    let meta = SkillMeta {
        created_at: Some(now.clone()),
        updated_at: Some(now),
        source_url: Some(skill.source_url.clone()),
        ..Default::default()
    };
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| format!("Failed to serialize .meta.json: {}", e))?;
    std::fs::write(target_dir.join(".meta.json"), meta_json)
        .map_err(|e| format!("Failed to write .meta.json: {}", e))?;

    Ok(target_dir)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_binary_content(content: &[u8]) -> bool {
    // Check first 8KB for null bytes
    let check_len = content.len().min(8192);
    content[..check_len].contains(&0)
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn write_skill_md(dir: &std::path::Path, markdown: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("SKILL.md"), markdown).unwrap();
    }

    fn fetched_file(relative_path: &str, content: impl AsRef<[u8]>) -> FetchedFile {
        FetchedFile {
            relative_path: relative_path.to_string(),
            content: content.as_ref().to_vec(),
            is_binary: false,
        }
    }

    fn skill_md_content(skill_id: &str, description: &str, prompt: &str) -> Vec<u8> {
        format!("---\nname: {skill_id}\ndescription: {description}\n---\n\n{prompt}\n").into_bytes()
    }

    fn fetched_skill(
        source_url: &str,
        skill_id: &str,
        name: &str,
        description: &str,
        prompt: &str,
        files: Vec<FetchedFile>,
    ) -> FetchedSkill {
        let has_scripts = files
            .iter()
            .any(|file| file.relative_path.starts_with("scripts/"));
        FetchedSkill {
            source_url: source_url.to_string(),
            skill_id: skill_id.to_string(),
            name: name.to_string(),
            description: description.to_string(),
            prompt: prompt.to_string(),
            allowed_tools: None,
            files,
            has_scripts,
        }
    }

    fn fetched_markdown_skill(
        source_url: &str,
        skill_id: &str,
        name: &str,
        description: &str,
        prompt: &str,
    ) -> FetchedSkill {
        fetched_skill(
            source_url,
            skill_id,
            name,
            description,
            prompt,
            vec![fetched_file(
                "SKILL.md",
                skill_md_content(skill_id, description, prompt),
            )],
        )
    }

    fn assert_github_url(
        url: &str,
        expected_owner: &str,
        expected_repo: &str,
        expected_ref_and_path: &str,
        expected_blob_skill_md: Option<bool>,
    ) {
        let source = parse_skill_url(url).unwrap();
        match source {
            SkillSource::GitHub {
                owner,
                repo,
                ref_and_path,
                is_blob_skill_md,
            } => {
                assert_eq!(owner, expected_owner);
                assert_eq!(repo, expected_repo);
                assert_eq!(ref_and_path, expected_ref_and_path);
                if let Some(expected) = expected_blob_skill_md {
                    assert_eq!(is_blob_skill_md, expected);
                }
            }
            _ => panic!("Expected GitHub source"),
        }
    }

    #[test]
    fn test_parse_github_tree_url() {
        assert_github_url(
            "https://github.com/anthropics/skills/tree/main/skills/docx",
            "anthropics",
            "skills",
            "main/skills/docx",
            Some(false),
        );
    }

    #[test]
    fn test_parse_github_blob_skill_md() {
        assert_github_url(
            "https://github.com/owner/repo/blob/develop/my-skill/SKILL.md",
            "owner",
            "repo",
            "develop/my-skill/SKILL.md",
            Some(true),
        );
    }

    #[test]
    fn test_parse_github_repo_only() {
        assert_github_url(
            "https://github.com/owner/repo",
            "owner",
            "repo",
            "main",
            None,
        );
    }

    #[test]
    fn test_parse_shorthand() {
        assert_github_url(
            "anthropics/skills/skills/docx",
            "anthropics",
            "skills",
            "main/skills/docx",
            None,
        );
    }

    #[test]
    fn test_parse_git_url() {
        let source = parse_skill_url("git@github.com:user/repo.git").unwrap();
        match source {
            SkillSource::Git { url, path } => {
                assert_eq!(url, "git@github.com:user/repo.git");
                assert!(path.is_none());
            }
            _ => panic!("Expected Git source"),
        }
    }

    #[test]
    fn test_parse_https_git_url() {
        let source = parse_skill_url("https://gitlab.com/user/repo.git").unwrap();
        match source {
            SkillSource::Git { url, path } => {
                assert_eq!(url, "https://gitlab.com/user/repo.git");
                assert!(path.is_none());
            }
            _ => panic!("Expected Git source"),
        }
    }

    #[test]
    fn test_parse_invalid_url() {
        assert!(parse_skill_url("not-a-valid-url").is_err());
    }

    #[test]
    fn test_parse_github_tree_with_trailing_slash() {
        assert_github_url(
            "https://github.com/anthropics/skills/tree/main/skills/docx/",
            "anthropics",
            "skills",
            "main/skills/docx",
            None,
        );
    }

    #[test]
    fn test_parse_github_branch_with_slashes() {
        // The raw ref_and_path preserves the ambiguity; resolution happens at fetch time.
        assert_github_url(
            "https://github.com/org/repo/tree/feature/import-skills/skills/docx",
            "org",
            "repo",
            "feature/import-skills/skills/docx",
            None,
        );
    }

    #[test]
    fn test_install_fetched_skill() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path();
        std::fs::create_dir_all(config_dir.join("skills")).unwrap();

        let skill = fetched_skill(
            "https://github.com/test/repo/tree/main/my-skill",
            "my-skill",
            "My Skill",
            "A test skill",
            "Do stuff.",
            vec![
                fetched_file(
                    "SKILL.md",
                    skill_md_content("my-skill", "A test skill", "Do stuff."),
                ),
                fetched_file("scripts/run.py", b"print('hello')"),
                fetched_file("references/guide.md", b"# Guide\nSome docs."),
            ],
        );

        let result = install_fetched_skill(&skill, config_dir, None).unwrap();
        assert!(result.join("SKILL.md").exists());
        assert!(result.join("scripts/run.py").exists());
        assert!(result.join("references/guide.md").exists());
        assert!(result.join(".meta.json").exists());

        // Check .meta.json contains source_url
        let meta: SkillMeta =
            serde_json::from_str(&std::fs::read_to_string(result.join(".meta.json")).unwrap())
                .unwrap();
        assert_eq!(
            meta.source_url,
            Some("https://github.com/test/repo/tree/main/my-skill".to_string())
        );
    }

    #[test]
    fn test_install_auto_increments_on_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path();
        let skills_dir = config_dir.join("skills");

        // Create existing skill
        let existing = skills_dir.join("my-skill");
        write_skill_md(
            &existing,
            "---\nname: my-skill\ndescription: Existing\n---\n\nOld.",
        );

        let skill = fetched_markdown_skill(
            "https://github.com/test/repo",
            "my-skill",
            "My Skill",
            "New one",
            "Do stuff.",
        );

        let result = install_fetched_skill(&skill, config_dir, None).unwrap();
        // Should be installed as my-skill-1
        assert!(result.ends_with("my-skill-1"));
        assert!(result.join("SKILL.md").exists());
        // Original should still exist
        assert!(existing.join("SKILL.md").exists());
    }

    #[test]
    fn test_install_to_project_scope() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_path = temp.path().join("project");
        std::fs::create_dir_all(config_dir.join("skills")).unwrap();
        std::fs::create_dir_all(project_path.join(".cairn/skills")).unwrap();

        let skill = fetched_markdown_skill(
            "https://github.com/test/repo",
            "test-skill",
            "Test",
            "Test",
            "Test.",
        );

        let result = install_fetched_skill(&skill, &config_dir, Some(&project_path)).unwrap();
        assert!(result.starts_with(
            project_path
                .join(".cairn/skills")
                .as_os_str()
                .to_str()
                .unwrap()
        ));
        assert!(result.join("SKILL.md").exists());
    }

    #[test]
    fn test_is_binary() {
        assert!(!is_binary_content(b"hello world"));
        assert!(is_binary_content(b"hello\x00world"));
        assert!(!is_binary_content(b""));
    }

    // -----------------------------------------------------------------------
    // URL parsing edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_url_trims_whitespace() {
        assert_github_url(
            "  https://github.com/owner/repo/tree/main/skill  ",
            "owner",
            "repo",
            "main/skill",
            None,
        );
    }

    #[test]
    fn test_parse_shorthand_two_segments() {
        assert_github_url("anthropics/skills", "anthropics", "skills", "main", None);
    }

    #[test]
    fn test_parse_http_github_url() {
        assert_github_url(
            "http://github.com/owner/repo/tree/main/skill",
            "owner",
            "repo",
            "main/skill",
            None,
        );
    }

    #[test]
    fn test_parse_blob_root_skill_md() {
        assert_github_url(
            "https://github.com/owner/repo/blob/main/SKILL.md",
            "owner",
            "repo",
            "main/SKILL.md",
            Some(true),
        );
    }

    #[test]
    fn test_parse_github_non_tree_non_blob_path() {
        // A /commits/ path should not parse as GitHub
        assert!(parse_skill_url("https://github.com/owner/repo/commits/main").is_err());
    }

    // -----------------------------------------------------------------------
    // fetch_skill logic (with synthetic file data)
    // -----------------------------------------------------------------------

    #[test]
    fn test_fetch_skill_no_skill_md() {
        let files = vec![fetched_file("README.md", b"# Hello")];
        let err = build_fetched_skill(files, "https://example.com").unwrap_err();
        assert!(err.contains("No SKILL.md found"));
    }

    #[test]
    fn test_fetch_skill_invalid_utf8() {
        let files = vec![fetched_file("SKILL.md", vec![0xFF, 0xFE, 0x00, 0x01])];
        let err = build_fetched_skill(files, "https://example.com").unwrap_err();
        assert!(err.contains("not valid UTF-8"));
    }

    #[test]
    fn test_fetch_skill_has_scripts_detection() {
        let skill_md_content =
            b"---\nname: test\ndescription: A test\n---\n\nDo things.\n".to_vec();

        // No scripts directory
        let files = vec![fetched_file("SKILL.md", skill_md_content.clone())];
        let result = build_fetched_skill(files, "url").unwrap();
        assert!(!result.has_scripts);

        // With scripts directory
        let files = vec![
            fetched_file("SKILL.md", skill_md_content),
            fetched_file("scripts/run.sh", b"#!/bin/bash"),
        ];
        let result = build_fetched_skill(files, "url").unwrap();
        assert!(result.has_scripts);
    }

    // -----------------------------------------------------------------------
    // truncate_str
    // -----------------------------------------------------------------------

    #[test]
    fn test_truncate_str_exact_length() {
        assert_eq!(truncate_str("abcde", 5), "abcde");
    }

    #[test]
    fn test_truncate_str_over_length() {
        assert_eq!(truncate_str("abcdef", 3), "abc...");
    }

    #[test]
    fn test_truncate_str_under_length() {
        assert_eq!(truncate_str("ab", 5), "ab");
    }

    // -----------------------------------------------------------------------
    // install edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_install_creates_nested_directories() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path();

        let skill = fetched_skill(
            "https://example.com",
            "nested",
            "Nested",
            "Has deep paths",
            "Do stuff.",
            vec![
                fetched_file(
                    "SKILL.md",
                    skill_md_content("nested", "Has deep paths", "Do stuff."),
                ),
                fetched_file("scripts/tools/deep/run.py", b"print('deep')"),
            ],
        );

        let result = install_fetched_skill(&skill, config_dir, None).unwrap();
        assert!(result.join("scripts/tools/deep/run.py").exists());
    }

    #[test]
    fn test_install_multi_increment_on_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path();
        let skills_dir = config_dir.join("skills");

        // Create existing skill and skill-1
        for name in &["my-skill", "my-skill-1"] {
            let dir = skills_dir.join(name);
            write_skill_md(
                &dir,
                "---\nname: my-skill\ndescription: Existing\n---\n\nOld.",
            );
        }

        let skill = fetched_markdown_skill(
            "https://example.com",
            "my-skill",
            "My Skill",
            "New",
            "Do stuff.",
        );

        let result = install_fetched_skill(&skill, config_dir, None).unwrap();
        // Should skip my-skill and my-skill-1, land on my-skill-2
        assert!(result.ends_with("my-skill-2"));
        assert!(result.join("SKILL.md").exists());
    }
}
