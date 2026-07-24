const MIN_CODEX_VERSION: (u32, u32, u32) = (0, 37, 0);

/// Parse a version string like "codex 0.37.1" or "0.37.1" into (major, minor, patch).
fn parse_codex_version(version_str: &str) -> Option<(u32, u32, u32)> {
    // Find the version number portion — skip any leading text like "codex "
    let version_part = version_str
        .split_whitespace()
        .find(|s| s.chars().next().is_some_and(|c| c.is_ascii_digit()))?;
    let mut parts = version_part.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    // Patch may contain pre-release suffix like "1-beta", take only digits
    let patch_str = parts.next().unwrap_or("0");
    let patch = patch_str
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse::<u32>()
        .unwrap_or(0);
    Some((major, minor, patch))
}

pub(super) fn check_codex_version(codex_path: &str) -> Result<(), String> {
    let output = std::process::Command::new(codex_path)
        .arg("--version")
        .output()
        .map_err(|e| format!("Failed to check codex version: {}", e))?;

    let version_str = String::from_utf8_lossy(&output.stdout);
    let version_str = version_str.trim();

    // If we can't parse the version, log a warning but don't block
    let Some(version) = parse_codex_version(version_str) else {
        log::warn!("Could not parse Codex version from: {:?}", version_str);
        return Ok(());
    };

    let (min_major, min_minor, min_patch) = MIN_CODEX_VERSION;
    if version < (min_major, min_minor, min_patch) {
        return Err(format!(
            "Codex CLI version {}.{}.{} is too old. Minimum required: {}.{}.{}. Run: npm install -g @openai/codex",
            version.0, version.1, version.2, min_major, min_minor, min_patch
        ));
    }

    log::debug!(
        "Codex CLI version: {}.{}.{}",
        version.0,
        version.1,
        version.2
    );
    Ok(())
}
