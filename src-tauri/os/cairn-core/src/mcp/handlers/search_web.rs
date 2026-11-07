//! Web search behind `cairn://websearch?q=`.
//!
//! The `web-search` slot's contract lives here: guidance when no provider can
//! answer, the error text for a failed request, and the one ranked rendering
//! every provider's results share. Which provider answers, and how its request
//! and response look, is a pack declaration resolved by
//! [`crate::web_providers`] and run by the declarative HTTP engine
//! ([`cairn_packs::provider::http`]). There is no per-provider code here.
//!
//! Guidance for a configuration gap (unconfigured, not installed, no key) is
//! `Ok` text the read surfaces as the body. A provider failure (a non-2xx
//! response, a body of the wrong shape) is an `Err`, so the read renders as
//! `[error]` and an agent reads it as the failed read it is.

use cairn_packs::provider::http::{self, Inputs, ResponseMapping, SearchHit};
use cairn_packs::provider::Slot;

use crate::orchestrator::Orchestrator;
use crate::web_providers::{self, Resolved, LIBRARY_LOCATION};

/// Run a web search for `query` through the selected provider and return a
/// rendered ranked list.
pub(crate) async fn search_web(orch: &Orchestrator, query: &str) -> Result<String, String> {
    let active = match web_providers::resolve(&orch.config_dir, Slot::WebSearch) {
        Resolved::Unconfigured => {
            return Ok(no_provider_message(&web_providers::offered_labels(
                &orch.config_dir,
                Slot::WebSearch,
            )))
        }
        Resolved::NotInstalled { name } => return Ok(not_installed_message(&name)),
        Resolved::Invalid { name, error } => return Ok(invalid_message(&name, &error)),
        Resolved::Provider(active) => active,
    };
    let provider = &active.provider;
    let label = provider.provider.label.as_str();
    let (Some(adapter), Some(credential)) = (provider.provider.http(), &provider.credential) else {
        return Err(format!(
            "The {label} web-search provider is not an HTTP provider"
        ));
    };
    let ResponseMapping::Search(mapping) = &adapter.response else {
        return Err(format!("The {label} web-search provider maps no results"));
    };
    // Before the credential is read: a request with a blank required value
    // would go out credentialed and fail at the provider.
    if let Some(option) =
        cairn_packs::provider_options::missing_required(&provider.provider.options, &active.options)
    {
        return Ok(needs_option_message(label, &option.label));
    }

    // Through the broker, so the key is a scrub target before it is put in a
    // header or a request body: a provider that quotes a rejected key in its
    // error body would otherwise put it straight into the agent's result.
    let Some(api_key) =
        web_providers::credential_value(&provider.pack, credential, "web search request").await?
    else {
        return Ok(missing_key_message(label));
    };

    let prepared = http::prepare(
        &provider.provider,
        adapter,
        &Inputs {
            query: Some(query),
            target: None,
            credential: api_key.expose(),
            options: &active.options,
        },
    );
    let resp = web_providers::send(&provider.pack, credential, &prepared)
        .await
        .map_err(|e| format!("Web search via {label} failed: {e}"))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("Web search via {label} failed to read response: {e}"))?;
    if !status.is_success() {
        return Err(http_error_message(label, status.as_u16(), &body));
    }
    let results = http::map_search(mapping, &body).map_err(|e| shape_error(label, &e, &body))?;
    Ok(render(label, query, &results))
}

fn shape_error(label: &str, error: &str, body: &str) -> String {
    format!(
        "Web search via {label} returned an unexpected response shape ({error}). Raw response:\n\n{}",
        truncate(body, 1000)
    )
}

fn truncate(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max).collect();
        format!("{head}\u{2026}")
    }
}

/// Render results as a ranked `title · url · snippet` markdown list.
fn render(label: &str, query: &str, results: &[SearchHit]) -> String {
    if results.is_empty() {
        return format!("Web search for \"{query}\" via {label} returned no results.");
    }
    let mut out = format!(
        "Web search · \"{query}\" · via {label} · {} result(s)\n\n",
        results.len()
    );
    for (i, r) in results.iter().enumerate() {
        out.push_str(&format!("{}. {}\n   {}\n", i + 1, r.title, r.url));
        let snippet = truncate(&r.snippet.replace('\n', " "), 300);
        if !snippet.is_empty() {
            out.push_str(&format!("   {snippet}\n"));
        }
        out.push('\n');
    }
    out.trim_end().to_string()
}

const READ_HINT: &str = "then read `cairn://websearch?q=<your query>`";

/// Guidance when no web-search provider is selected. `offered` is the labels of
/// the providers the library ships for the slot — data, not code.
fn no_provider_message(offered: &[String]) -> String {
    let offered = if offered.is_empty() {
        String::new()
    } else {
        format!(" Offered there: {}.", offered.join(", "))
    };
    format!(
        "No web-search provider is configured. Install a search provider from {LIBRARY_LOCATION} \
         and select it for web search, {READ_HINT}.{offered}"
    )
}

fn needs_option_message(label: &str, option: &str) -> String {
    format!("{label} web search needs its {option}. Set it in {LIBRARY_LOCATION}, {READ_HINT}.")
}

fn not_installed_message(name: &str) -> String {
    format!(
        "The web-search provider `{name}` is selected but not installed. Install it from \
         {LIBRARY_LOCATION}, {READ_HINT}."
    )
}

fn invalid_message(name: &str, error: &str) -> String {
    format!(
        "The web-search provider `{name}` is installed, but its declaration does not load: \
         {error}. Update or reinstall it from {LIBRARY_LOCATION}."
    )
}

/// Guidance when the selected provider has no API key stored.
fn missing_key_message(label: &str) -> String {
    format!(
        "No API key is set for the {label} web-search provider. Add it in {LIBRARY_LOCATION}, \
         {READ_HINT}."
    )
}

/// The failure text for a non-2xx provider response.
fn http_error_message(label: &str, status: u16, body: &str) -> String {
    if status == 401 || status == 403 {
        format!("The API key for {label} is missing or invalid — set it in {LIBRARY_LOCATION}.")
    } else {
        format!(
            "Web search via {label} failed (HTTP {status}): {}",
            truncate(body, 300)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_packs::provider::{load_pack_provider, PackProvider};
    use serde_json::Value;
    use std::path::Path;

    fn bundled(pack: &str) -> PackProvider {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../packs")
            .join(pack);
        load_pack_provider(&root, pack, Slot::WebSearch)
            .unwrap()
            .unwrap()
    }

    fn goldens() -> Value {
        serde_json::from_str(include_str!(
            "../../../../cairn-packs/src/provider/testdata/search_goldens.json"
        ))
        .unwrap()
    }

    /// What an agent read from each captured fixture before the port is what it
    /// reads after it, byte for byte.
    #[test]
    fn every_captured_rendering_is_byte_identical() {
        for case in goldens()["renders"].as_array().unwrap() {
            let pack = case["provider"].as_str().unwrap();
            let provider = bundled(pack);
            let ResponseMapping::Search(mapping) = &provider.provider.http().unwrap().response
            else {
                panic!("{pack} maps no results");
            };
            let hits = http::map_search(mapping, case["raw"].as_str().unwrap()).unwrap();
            let query = case["query"].as_str().unwrap();
            assert_eq!(
                render(&provider.provider.label, query, &hits),
                case["render"].as_str().unwrap(),
                "{pack}"
            );
        }
    }

    #[test]
    fn error_texts_are_byte_identical() {
        let golden = &goldens()["errors"];
        let provider = bundled("tavily");
        let ResponseMapping::Search(mapping) = &provider.provider.http().unwrap().response else {
            unreachable!()
        };
        let error = http::map_search(mapping, "not json").unwrap_err();
        assert_eq!(
            shape_error(&provider.provider.label, &error, "not json"),
            golden["non_json"].as_str().unwrap()
        );
        assert_eq!(
            http_error_message(&provider.provider.label, 500, "oops"),
            golden["http_500"].as_str().unwrap()
        );
        assert!(http_error_message("X", 401, "").contains("invalid"));
        assert!(http_error_message("X", 403, "").contains(LIBRARY_LOCATION));
    }

    /// The guidance an agent reads names the Library and no provider: the app
    /// knows no provider by name.
    #[test]
    fn guidance_names_the_library_and_no_provider() {
        for message in [
            no_provider_message(&[]),
            not_installed_message("ghost"),
            missing_key_message("Ghost"),
            invalid_message("ghost", "bad"),
        ] {
            assert!(message.contains(LIBRARY_LOCATION), "{message}");
            assert!(!message.contains("Settings"), "{message}");
        }
        assert!(no_provider_message(&[]).contains("cairn://websearch"));
        assert!(no_provider_message(&["A".into(), "B".into()]).ends_with("Offered there: A, B."));
    }

    #[test]
    fn render_empty_is_clean() {
        let out = render("Exa", "q", &[]);
        assert!(out.contains("no results"), "{out}");
    }
}
