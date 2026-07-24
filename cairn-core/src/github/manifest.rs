use serde::{Deserialize, Serialize};

/// GitHub App manifest for the manifest flow.
#[derive(Debug, Serialize)]
pub struct AppManifest {
    name: String,
    url: String,
    hook_attributes: HookAttributes,
    redirect_url: String,
    callback_urls: Vec<String>,
    public: bool,
    default_permissions: Permissions,
    default_events: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct HookAttributes {
    url: String,
    active: bool,
}

#[derive(Debug, Serialize)]
pub struct Permissions {
    pull_requests: String,
    checks: String,
    contents: String,
    metadata: String,
    actions: String,
}

/// Response from GitHub's manifest flow completion endpoint.
#[derive(Debug, Deserialize)]
pub struct ManifestResponse {
    pub id: i64,
    pub slug: String,
    pub name: String,
    pub webhook_secret: String,
    pub pem: String,
}

/// Create a GitHub App manifest configured for webhook events.
///
/// `name_hint` is used as the human-readable part of the app name (for example
/// `Cairn - Alice`). When absent or empty, falls back to a hex suffix derived
/// from the webhook channel ID.
pub fn create_manifest(
    webhook_url: &str,
    redirect_url: &str,
    name_hint: Option<&str>,
) -> AppManifest {
    let name = match name_hint.filter(|s| !s.is_empty()) {
        Some(hint) => format!("Cairn - {hint}"),
        None => {
            let channel_suffix = webhook_url
                .rsplit('/')
                .next()
                .map(|s| &s[s.len().saturating_sub(8)..])
                .unwrap_or("dev");
            format!("Cairn {channel_suffix}")
        }
    };

    AppManifest {
        name,
        url: "https://github.com/bleugreen/cairn".to_string(),
        hook_attributes: HookAttributes {
            url: webhook_url.to_string(),
            active: true,
        },
        redirect_url: redirect_url.to_string(),
        callback_urls: vec![redirect_url.to_string()],
        public: true,
        default_permissions: Permissions {
            pull_requests: "write".to_string(),
            checks: "read".to_string(),
            contents: "write".to_string(),
            metadata: "read".to_string(),
            actions: "read".to_string(),
        },
        default_events: vec![
            "pull_request".to_string(),
            "pull_request_review".to_string(),
            "check_run".to_string(),
            "check_suite".to_string(),
            "push".to_string(),
        ],
    }
}

/// Complete the manifest flow by exchanging the temporary code for credentials.
pub async fn complete_manifest_flow(code: &str) -> Result<ManifestResponse, String> {
    log::info!("Completing GitHub App manifest flow...");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "https://api.github.com/app-manifests/{code}/conversions"
        ))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Cairn")
        .send()
        .await
        .map_err(|e| format!("Failed to complete manifest flow: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("GitHub API error: {status} - {body}"));
    }

    resp.json::<ManifestResponse>()
        .await
        .map_err(|e| format!("Failed to parse manifest response: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_hint_produces_human_readable_name() {
        let m = create_manifest(
            "https://example.com/hooks/abcdef1234567890",
            "http://redir",
            Some("Alice"),
        );
        assert_eq!(m.name, "Cairn - Alice");
    }

    #[test]
    fn none_hint_falls_back_to_hex_suffix() {
        let m = create_manifest(
            "https://example.com/hooks/abcdef1234567890",
            "http://redir",
            None,
        );
        assert_eq!(m.name, "Cairn 34567890");
    }

    #[test]
    fn empty_hint_falls_back_to_hex_suffix() {
        let m = create_manifest(
            "https://example.com/hooks/abcdef1234567890",
            "http://redir",
            Some(""),
        );
        assert_eq!(m.name, "Cairn 34567890");
    }

    #[test]
    fn short_channel_id_uses_whole_segment() {
        let m = create_manifest("https://example.com/hooks/abc", "http://redir", None);
        assert_eq!(m.name, "Cairn abc");
    }

    #[test]
    fn manifest_fields_are_correct() {
        let m = create_manifest(
            "https://example.com/hooks/ch123",
            "http://redir",
            Some("Bob"),
        );
        assert_eq!(m.hook_attributes.url, "https://example.com/hooks/ch123");
        assert_eq!(m.redirect_url, "http://redir");
        assert!(m.hook_attributes.active);
        assert!(m.public);
        assert_eq!(m.default_permissions.pull_requests, "write");
        assert_eq!(
            m.default_events,
            vec![
                "pull_request",
                "pull_request_review",
                "check_run",
                "check_suite",
                "push"
            ]
        );
    }
}
