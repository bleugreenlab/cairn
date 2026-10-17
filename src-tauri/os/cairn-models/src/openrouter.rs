//! How a request identifies itself to OpenRouter: its key, and the app it is
//! attributed to on OpenRouter's rankings. Every OpenRouter call the facet or
//! its hosts make (chat completions, images) sends these headers.

/// The app a request is attributed to. A host that is not Cairn names
/// itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribution {
    /// Sent as `http-referer`.
    pub referer: String,
    /// Sent as `x-openrouter-title`.
    pub title: String,
}

impl Attribution {
    pub fn cairn() -> Self {
        Self {
            referer: "https://cairn.computer".into(),
            title: "Cairn".into(),
        }
    }
}

/// The headers an OpenRouter request carries.
pub fn headers(api_key: &str, attribution: &Attribution) -> Vec<(String, String)> {
    vec![
        ("authorization".to_string(), format!("Bearer {api_key}")),
        ("content-type".to_string(), "application/json".to_string()),
        ("http-referer".to_string(), attribution.referer.clone()),
        ("x-openrouter-title".to_string(), attribution.title.clone()),
    ]
}
