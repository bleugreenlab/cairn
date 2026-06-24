/// Split markdown with YAML frontmatter into the raw frontmatter body and prompt.
///
/// Markdown frontmatter delimiters are line-oriented, so accept both LF and CRLF
/// files. Windows installers and editors can materialize bundled Markdown with
/// CRLF endings; requiring the literal `---\n` byte sequence makes valid files
/// look delimiter-free.
pub(crate) fn split_yaml_frontmatter(content: &str) -> Result<(&str, String), String> {
    let content = content.strip_prefix('\u{feff}').unwrap_or(content);
    let after_start = content
        .strip_prefix("---\n")
        .or_else(|| content.strip_prefix("---\r\n"))
        .ok_or_else(|| "Missing frontmatter start delimiter".to_string())?;

    let (end_idx, delimiter_len) = match after_start.find("\n---\n") {
        Some(idx) => (idx, "\n---\n".len()),
        None => match after_start.find("\r\n---\r\n") {
            Some(idx) => (idx, "\r\n---\r\n".len()),
            None => return Err("Missing frontmatter end delimiter".to_string()),
        },
    };

    let frontmatter = &after_start[..end_idx];
    let prompt = after_start[end_idx + delimiter_len..].trim().to_string();
    Ok((frontmatter, prompt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_lf_frontmatter() {
        let (frontmatter, prompt) =
            split_yaml_frontmatter("---\nname: test\n---\n\nPrompt").unwrap();
        assert_eq!(frontmatter, "name: test");
        assert_eq!(prompt, "Prompt");
    }

    #[test]
    fn splits_crlf_frontmatter() {
        let (frontmatter, prompt) =
            split_yaml_frontmatter("---\r\nname: test\r\n---\r\n\r\nPrompt").unwrap();
        assert_eq!(frontmatter, "name: test");
        assert_eq!(prompt, "Prompt");
    }

    #[test]
    fn tolerates_utf8_bom_before_frontmatter() {
        let (frontmatter, prompt) =
            split_yaml_frontmatter("\u{feff}---\r\nname: test\r\n---\r\n\r\nPrompt").unwrap();
        assert_eq!(frontmatter, "name: test");
        assert_eq!(prompt, "Prompt");
    }
}
