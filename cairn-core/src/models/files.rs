//! File browsing types for the repository file viewer.

use serde::{Deserialize, Serialize};

/// Information about a git branch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchInfo {
    pub name: String,
    pub is_remote: bool,
    pub is_current: bool,
}

/// Represents a file or directory in the repository
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepoFile {
    pub path: String,
    pub name: String,
    pub is_directory: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<RepoFile>>,
}

/// Content of a file at a specific git ref
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileContent {
    pub path: String,
    pub content: String,
    pub language: Option<String>,
}

/// Map file extensions to language identifiers for syntax highlighting
pub fn detect_language(path: &str) -> Option<String> {
    let ext = path.rsplit('.').next()?.to_lowercase();
    let lang = match ext.as_str() {
        // Rust
        "rs" => "rust",
        // TypeScript/JavaScript
        "ts" => "typescript",
        "tsx" => "tsx",
        "js" => "javascript",
        "jsx" => "jsx",
        "mjs" | "cjs" => "javascript",
        // Web
        "html" | "htm" => "html",
        "css" => "css",
        "scss" => "scss",
        "less" => "less",
        // Data formats
        "json" => "json",
        "yaml" | "yml" => "yaml",
        "toml" => "toml",
        "xml" => "xml",
        // Markdown
        "md" | "mdx" => "markdown",
        // Shell
        "sh" | "bash" | "zsh" => "bash",
        "fish" => "fish",
        "ps1" | "psm1" => "powershell",
        // Python
        "py" | "pyi" => "python",
        // Go
        "go" => "go",
        // C/C++
        "c" | "h" => "c",
        "cpp" | "hpp" | "cc" | "cxx" => "cpp",
        // Java/Kotlin
        "java" => "java",
        "kt" | "kts" => "kotlin",
        // Swift
        "swift" => "swift",
        // Ruby
        "rb" => "ruby",
        // PHP
        "php" => "php",
        // SQL
        "sql" => "sql",
        // Docker
        "dockerfile" => "dockerfile",
        // Config
        "ini" | "conf" | "cfg" => "ini",
        "env" => "dotenv",
        // Other
        "graphql" | "gql" => "graphql",
        "vue" => "vue",
        "svelte" => "svelte",
        "astro" => "astro",
        "prisma" => "prisma",
        _ => return None,
    };
    Some(lang.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_language() {
        assert_eq!(detect_language("src/main.rs"), Some("rust".to_string()));
        assert_eq!(detect_language("app.tsx"), Some("tsx".to_string()));
        assert_eq!(detect_language("config.yaml"), Some("yaml".to_string()));
        assert_eq!(detect_language("README.md"), Some("markdown".to_string()));
        assert_eq!(
            detect_language("Dockerfile"),
            Some("dockerfile".to_string())
        );
        assert_eq!(detect_language("unknown.xyz"), None);
    }
}
