pub use cairn_session::agent_process::stdin::*;
pub(crate) async fn resolve_stable_images(
    db: &crate::db::DbState,
    authorized_project_id: &str,
    authorized_project_key: &str,
    text: impl Into<String>,
) -> Result<MessageContent, String> {
    let text = text.into();
    let mut seen = std::collections::HashSet::new();
    let mut images = Vec::new();
    for found in cairn_common::uri::scan_markdown_stored_images(&text) {
        let uri = found.uri;
        if !seen.insert(uri.clone()) {
            continue;
        }
        let cairn_common::uri::CairnResource::ProjectImage { project, reference } = found.resource
        else {
            continue;
        };
        if !project.eq_ignore_ascii_case(authorized_project_key) {
            return Err(format!(
                "durable image {uri} is outside the authorized project"
            ));
        }
        let owning_db = crate::projects::crud::owning_db(db, authorized_project_id).await?;
        let authorized_project = crate::projects::crud::get_db(&owning_db, authorized_project_id)
            .await?
            .ok_or_else(|| format!("authorized project not found: {authorized_project_id}"))?;
        if !authorized_project
            .key
            .eq_ignore_ascii_case(authorized_project_key)
        {
            return Err("durable image authority no longer matches the current project".into());
        }
        let image = match crate::attachments::fetch_image(
            &owning_db,
            authorized_project_id,
            authorized_project_key,
            &reference,
        )
        .await
        {
            Ok(image) => image,
            Err(error) => {
                log::warn!(
                    "failed to resolve durable image {uri}; leaving reference as text: {error}"
                );
                continue;
            }
        };
        images.push(MessageImage {
            mime_type: image.media_type,
            bytes: image.bytes,
        });
    }
    Ok(MessageContent { text, images })
}
