use super::*;

/// Scan `message` for `/skill-id` tokens, prepend matched skill content,
/// and pass through unknown slash commands unchanged.
pub fn resolve_skill_slash_command(
    orch: &Orchestrator,
    message: &str,
    project_path: Option<&std::path::Path>,
) -> String {
    let mut skill_blocks: Vec<String> = Vec::new();

    for word in message.split_whitespace() {
        if !word.starts_with('/') {
            continue;
        }
        let id = &word[1..];
        if id.is_empty() || id == "compact" {
            continue;
        }
        if let Ok(Some(skill)) =
            crate::config::skills::get_skill(&orch.config_dir, id, project_path)
        {
            skill_blocks.push(format!(
                "<skill name=\"{}\">\n{}\n</skill>",
                skill.name, skill.prompt
            ));
        }
    }

    if skill_blocks.is_empty() {
        return message.to_string();
    }

    format!("{}\n\n{}", skill_blocks.join("\n\n"), message)
}
