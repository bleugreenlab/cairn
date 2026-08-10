//! Stable project-level thread-name validation.

/// Project-level resource segments that cannot be used as a thread's stable name.
///
/// This is the canonical list shared by creation and URI parsing.
pub const RESERVED_PROJECT_SEGMENTS: &[&str] = &[
    "actions",
    "agents",
    "browser",
    "chat",
    "check-observations",
    "check-results",
    "comments",
    "images",
    "issues",
    "messages",
    "memories",
    "recipes",
    "references",
    "repl",
    "responses",
    "routes",
    "settings",
    "skills",
    "symbols",
    "t",
    "terminal",
    "threads",
    "wakes",
    "workflows",
];

pub fn validate_thread_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("thread name must not be empty".into());
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(
            "thread name must contain only lowercase ASCII letters, digits, and '-'".into(),
        );
    }
    if name.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("thread name must not be purely numeric".into());
    }
    if !name.bytes().any(|byte| byte.is_ascii_alphanumeric()) {
        return Err("thread name must contain at least one letter or digit".into());
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("thread name must not start or end with '-'".into());
    }
    if RESERVED_PROJECT_SEGMENTS.contains(&name) {
        return Err(format!("thread name is reserved: {name}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_thread_names() {
        for valid in ["roadmap", "roadmap-2", "2-roadmap"] {
            assert!(validate_thread_name(valid).is_ok(), "{valid}");
        }
        for invalid in [
            "", "Roadmap", "road_map", "123", "-", "-roadmap", "roadmap-",
        ] {
            assert!(validate_thread_name(invalid).is_err(), "{invalid}");
        }
        for reserved in RESERVED_PROJECT_SEGMENTS {
            assert!(validate_thread_name(reserved).is_err(), "{reserved}");
        }
    }
}
