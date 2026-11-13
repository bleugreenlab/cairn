//! Canonical model identity: one id per model, whichever provider offers it
//! and whichever leaderboard published a result for it.
//!
//! The data lives in `data/identity/`. `models.json` lists every canonical id
//! with the lab that trained it. `aliases/<source>.json` maps each public
//! benchmark source's published names (`<scaffold>:<published name>`) onto
//! those ids; the bench's loaders read the same files, so a benchmark result
//! and a ladder rung name a model by one id. `offers.json` maps the provider
//! offers whose native id the catalog's rule cannot reach. `lines.json` lists
//! the releases of each product line in release order, which the benchmark
//! placements keep ([`crate::benchmarks`]).
//!
//! An offer resolves by equality only ([`Identity::for_offer`]): nothing is
//! matched by stripping a date or by rewriting hyphens as dots, because two
//! names that differ that way can be two releases. The Claude CLI's aliases
//! (`opus`, `sonnet`) resolve to nothing: the CLI, not Cairn, decides which
//! version they mean.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// One canonical model.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CanonicalModel {
    /// Lower case, no vendor prefix, dots for versions: `claude-opus-4.7`.
    pub id: String,
    /// The lab that trained it, spelled as OpenRouter spells labs. `None` for a
    /// fine-tune, merge or private checkpoint, whose lab is not its base
    /// model's, and for a lab Cairn has no spelling for.
    pub maker: Option<String>,
}

/// What a published leaderboard name stands for.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AliasTarget {
    /// The canonical id.
    pub model: String,
    /// The reasoning setting the name fixes (`high`, `reasoner`), when it
    /// fixes one.
    #[serde(default)]
    pub configuration: Option<String>,
}

/// The parsed identity tables.
#[derive(Debug)]
pub struct Identity {
    models: BTreeMap<String, CanonicalModel>,
    aliases: BTreeMap<String, AliasTarget>,
    offers: BTreeMap<String, String>,
    lines: BTreeMap<String, Vec<String>>,
    /// Each listed release's line and its place in it, oldest at 0.
    release: BTreeMap<String, (String, usize)>,
}

const MODELS: &str = include_str!("../data/identity/models.json");
const OFFERS: &str = include_str!("../data/identity/offers.json");
const LINES: &str = include_str!("../data/identity/lines.json");

/// Every alias table, by source. A file added to `data/identity/aliases/`
/// must be listed here; a test holds the two together.
const ALIASES: &[(&str, &str)] = &[
    ("aims", include_str!("../data/identity/aliases/aims.json")),
    (
        "arc_agi",
        include_str!("../data/identity/aliases/arc_agi.json"),
    ),
    ("bfcl", include_str!("../data/identity/aliases/bfcl.json")),
    (
        "bigcodebench",
        include_str!("../data/identity/aliases/bigcodebench.json"),
    ),
    (
        "cruxeval",
        include_str!("../data/identity/aliases/cruxeval.json"),
    ),
    (
        "epoch_swebench",
        include_str!("../data/identity/aliases/epoch_swebench.json"),
    ),
    (
        "hal_matrix",
        include_str!("../data/identity/aliases/hal_matrix.json"),
    ),
    (
        "helm_capabilities",
        include_str!("../data/identity/aliases/helm_capabilities.json"),
    ),
    (
        "helm_safety",
        include_str!("../data/identity/aliases/helm_safety.json"),
    ),
    (
        "livebench",
        include_str!("../data/identity/aliases/livebench.json"),
    ),
    (
        "livecodebench",
        include_str!("../data/identity/aliases/livecodebench.json"),
    ),
    (
        "matharena",
        include_str!("../data/identity/aliases/matharena.json"),
    ),
    (
        "multi_swebench",
        include_str!("../data/identity/aliases/multi_swebench.json"),
    ),
    ("oll", include_str!("../data/identity/aliases/oll.json")),
    (
        "swe_rebench",
        include_str!("../data/identity/aliases/swe_rebench.json"),
    ),
    (
        "swe_rebench_site",
        include_str!("../data/identity/aliases/swe_rebench_site.json"),
    ),
    (
        "swebench_experiments",
        include_str!("../data/identity/aliases/swebench_experiments.json"),
    ),
    (
        "tau2_bench",
        include_str!("../data/identity/aliases/tau2_bench.json"),
    ),
    (
        "terminal_bench",
        include_str!("../data/identity/aliases/terminal_bench.json"),
    ),
    ("wild", include_str!("../data/identity/aliases/wild.json")),
];

#[derive(Deserialize)]
struct ModelsFile {
    models: Vec<CanonicalModel>,
}

#[derive(Deserialize)]
struct AliasFile {
    aliases: BTreeMap<String, AliasTarget>,
}

#[derive(Deserialize)]
struct OffersFile {
    offers: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct LinesFile {
    lines: BTreeMap<String, Vec<String>>,
}

impl Identity {
    /// Parses the shipped tables, refusing an id listed twice, an alias or
    /// offer naming an id `models.json` does not list, an alias two sources
    /// map to different models, and a line that is not at least two releases
    /// of one maker, none of them in another line.
    pub fn load() -> Result<Identity, String> {
        let parsed: ModelsFile =
            serde_json::from_str(MODELS).map_err(|e| format!("identity/models.json: {e}"))?;
        let mut models = BTreeMap::new();
        for model in parsed.models {
            let id = model.id.clone();
            if models.insert(id.clone(), model).is_some() {
                return Err(format!("identity/models.json lists {id} twice"));
            }
        }
        let mut aliases: BTreeMap<String, AliasTarget> = BTreeMap::new();
        for (source, text) in ALIASES {
            let file: AliasFile = serde_json::from_str(text)
                .map_err(|e| format!("identity/aliases/{source}.json: {e}"))?;
            for (alias, target) in file.aliases {
                if !models.contains_key(&target.model) {
                    return Err(format!(
                        "identity/aliases/{source}.json: {alias} names {}, which models.json does not list",
                        target.model
                    ));
                }
                if let Some(existing) = aliases.get(&alias) {
                    if *existing != target {
                        return Err(format!(
                            "identity/aliases/{source}.json: {alias} conflicts with another source's entry"
                        ));
                    }
                }
                aliases.insert(alias, target);
            }
        }
        let offers: OffersFile =
            serde_json::from_str(OFFERS).map_err(|e| format!("identity/offers.json: {e}"))?;
        for (offer, id) in &offers.offers {
            if !models.contains_key(id) {
                return Err(format!(
                    "identity/offers.json: {offer} names {id}, which models.json does not list"
                ));
            }
        }
        let lines: LinesFile =
            serde_json::from_str(LINES).map_err(|e| format!("identity/lines.json: {e}"))?;
        let mut release = BTreeMap::new();
        for (line, members) in &lines.lines {
            if members.len() < 2 {
                return Err(format!(
                    "identity/lines.json: {line} lists fewer than two releases"
                ));
            }
            let mut maker: Option<&str> = None;
            for id in members {
                let model = models.get(id).ok_or_else(|| {
                    format!(
                        "identity/lines.json: {line} names {id}, which models.json does not list"
                    )
                })?;
                match (model.maker.as_deref(), maker) {
                    (None, _) => {
                        return Err(format!(
                            "identity/lines.json: {line} names {id}, which has no maker"
                        ))
                    }
                    (Some(this), Some(first)) if this != first => {
                        return Err(format!("identity/lines.json: {line} mixes makers"))
                    }
                    (Some(this), _) => maker = Some(this),
                }
            }
            for (at, id) in members.iter().enumerate() {
                if release.insert(id.clone(), (line.clone(), at)).is_some() {
                    return Err(format!("identity/lines.json lists {id} in two places"));
                }
            }
        }
        Ok(Identity {
            models,
            aliases,
            offers: offers.offers,
            lines: lines.lines,
            release,
        })
    }

    /// Every product line by name, each listing its releases oldest first.
    pub fn lines(&self) -> impl Iterator<Item = (&str, &[String])> {
        self.lines
            .iter()
            .map(|(name, members)| (name.as_str(), members.as_slice()))
    }

    /// The line the canonical model `id` is a release of, and its place in
    /// that line, oldest at 0.
    pub fn release(&self, id: &str) -> Option<(&str, usize)> {
        self.release.get(id).map(|(line, at)| (line.as_str(), *at))
    }

    /// The releases of `line`, oldest first.
    pub fn line(&self, line: &str) -> Option<&[String]> {
        self.lines.get(line).map(Vec::as_slice)
    }

    /// The canonical model `id`, when it is one.
    pub fn canonical(&self, id: &str) -> Option<&CanonicalModel> {
        self.models.get(id)
    }

    /// Every canonical model, in id order.
    pub fn models(&self) -> impl Iterator<Item = &CanonicalModel> {
        self.models.values()
    }

    /// What a leaderboard's published name stands for: `scaffold` is the
    /// harness the source ran (`terminus-2`, `epoch-bash-agent`), `published`
    /// the name exactly as the source spells it.
    pub fn resolve(&self, scaffold: &str, published: &str) -> Option<&AliasTarget> {
        self.aliases.get(&format!("{scaffold}:{published}"))
    }

    /// The canonical id of `native` on `provider`, whose discovery reported
    /// `canonical_slug`: an explicit `offers.json` entry first; otherwise the
    /// slug, then the native id, when (with a leading `~` and a `maker/`
    /// prefix removed) it equals a canonical id whose maker is that prefix.
    pub fn for_offer(
        &self,
        provider: &str,
        native: &str,
        canonical_slug: Option<&str>,
    ) -> Option<&str> {
        if let Some(id) = self.offers.get(&format!("{provider}/{native}")) {
            return Some(id);
        }
        canonical_slug
            .filter(|slug| !slug.is_empty())
            .into_iter()
            .chain(std::iter::once(native))
            .find_map(|name| self.by_rule(name))
    }

    fn by_rule(&self, name: &str) -> Option<&str> {
        let name = name.trim_start_matches('~');
        let (prefix, id) = match name.split_once('/') {
            Some((prefix, id)) => (Some(prefix), id),
            None => (None, name),
        };
        let model = self.models.get(id)?;
        match prefix {
            None => Some(&model.id),
            Some(prefix) => model
                .maker
                .as_deref()
                .is_some_and(|maker| maker.eq_ignore_ascii_case(prefix))
                .then_some(model.id.as_str()),
        }
    }
}

/// The shipped identity tables, parsed once.
pub fn identity() -> &'static Identity {
    static IDENTITY: OnceLock<Identity> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        // The tables are compiled in and checked by this module's tests, so a
        // failure here is a build that skipped them.
        Identity::load().expect("the shipped identity tables parse")
    })
}

/// [`Identity::canonical`] on the shipped tables.
pub fn canonical(id: &str) -> Option<&'static CanonicalModel> {
    identity().canonical(id)
}

/// [`Identity::resolve`] on the shipped tables.
pub fn resolve(scaffold: &str, published: &str) -> Option<&'static AliasTarget> {
    identity().resolve(scaffold, published)
}

/// [`Identity::for_offer`] on the shipped tables.
pub fn for_offer(
    provider: &str,
    native: &str,
    canonical_slug: Option<&str>,
) -> Option<&'static str> {
    identity().for_offer(provider, native, canonical_slug)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{openrouter_maker, PROVIDER_CATALOG};
    use std::collections::BTreeSet;

    #[test]
    fn the_shipped_tables_parse_with_every_target_listed_and_no_conflict() {
        let identity = Identity::load().expect("parses");
        assert!(identity.models.len() > 500, "{}", identity.models.len());
        assert!(identity.aliases.len() > 1_000);
        assert!(identity.lines().count() >= 10);
    }

    #[test]
    fn a_line_lists_its_releases_oldest_first_each_once() {
        let identity = identity();
        assert_eq!(
            identity.release("claude-opus-4.7"),
            Some(("claude-opus", 5))
        );
        assert_eq!(
            identity.line("claude-opus").unwrap()[6..],
            ["claude-opus-4.8", "claude-opus-5", "claude-opus-5.5"]
        );
        // A tier is a line of its own, and a release of uncertain place is in none.
        assert_eq!(identity.release("gpt-5.3-codex"), Some(("gpt-codex", 2)));
        assert_eq!(identity.release("gpt-5.5-pre-release"), None);
        for (line, members) in identity.lines() {
            for (at, id) in members.iter().enumerate() {
                assert_eq!(identity.release(id), Some((line, at)));
            }
        }
    }

    #[test]
    fn every_alias_file_on_disk_is_compiled_in() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("data/identity/aliases");
        let on_disk: BTreeSet<String> = std::fs::read_dir(dir)
            .expect("aliases directory")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .into_string()
                    .expect("utf-8")
            })
            .filter_map(|name| name.strip_suffix(".json").map(str::to_string))
            .collect();
        let listed: BTreeSet<String> = ALIASES.iter().map(|(s, _)| s.to_string()).collect();
        assert_eq!(on_disk, listed);
    }

    #[test]
    fn ids_are_lower_case_without_a_vendor_prefix_and_makers_are_spelled_as_openrouter_does() {
        for model in identity().models() {
            assert_eq!(model.id, model.id.to_ascii_lowercase(), "{}", model.id);
            assert!(!model.id.contains('/'), "{}", model.id);
            if let Some(maker) = &model.maker {
                assert_eq!(
                    openrouter_maker(&format!("{maker}/{}", model.id)).as_deref(),
                    Some(maker.as_str())
                );
            }
        }
    }

    #[test]
    fn the_identity_makers_agree_with_the_providers_own() {
        // A provider that serves one lab's models names that lab; its models
        // in the table must carry the same spelling.
        for (provider, model) in [("claude", "claude-opus-4.7"), ("codex", "gpt-5.5")] {
            let descriptor = PROVIDER_CATALOG
                .iter()
                .find(|d| d.key == provider)
                .expect("shipped");
            assert_eq!(
                canonical(model).and_then(|m| m.maker.as_deref()),
                descriptor.maker.as_deref(),
                "{provider}"
            );
        }
        for (slug, model) in [
            ("anthropic/claude-opus-4.6", "claude-opus-4.6"),
            ("moonshotai/kimi-k2.5", "kimi-k2.5"),
            ("z-ai/glm-5", "glm-5"),
            ("deepseek/deepseek-v3.2", "deepseek-v3.2"),
            ("minimax/minimax-m2.5", "minimax-m2.5"),
            ("qwen/qwen3.6-plus", "qwen3.6-plus"),
            ("x-ai/grok-4", "grok-4"),
            ("google/gemini-3.1-pro", "gemini-3.1-pro"),
        ] {
            assert_eq!(
                canonical(model).and_then(|m| m.maker.clone()),
                openrouter_maker(slug),
                "{slug}"
            );
        }
    }

    #[test]
    fn leaderboard_names_resolve_to_canonical_models() {
        let at = |scaffold, name| resolve(scaffold, name).map(|t| t.model.as_str());
        assert_eq!(at("terminus-2", "claude-opus-4-6"), Some("claude-opus-4.6"));
        assert_eq!(
            at("epoch-bash-agent", "claude-opus-4-7_max"),
            Some("claude-opus-4.7")
        );
        assert_eq!(
            resolve("mini-swe-agent", "kimi-k2-5-high").and_then(|t| t.configuration.as_deref()),
            Some("high")
        );
        // A private checkpoint is its own model, never its base's.
        assert_eq!(at("terminus-2", "glm-5"), Some("glm-5-private-checkpoint"));
        assert_eq!(at("terminus-2", "no-such-model"), None);
    }

    #[test]
    fn claude_api_ids_resolve_only_through_listed_spellings_and_snapshots() {
        for (native, expected) in [
            ("claude-opus-4-6", "claude-opus-4.6"),
            ("claude-opus-4-7", "claude-opus-4.7"),
            ("claude-sonnet-4-6", "claude-sonnet-4.6"),
            ("claude-fable-5-1", "claude-fable-5.1"),
            ("claude-fable-5-2", "claude-fable-5.2"),
            ("claude-haiku-4-5-20251001", "claude-haiku-4.5"),
            ("claude-opus-4-20250514", "claude-opus-4"),
            ("claude-opus-4-1-20250805", "claude-opus-4.1"),
            ("claude-opus-4-5-20251101", "claude-opus-4.5"),
            ("claude-sonnet-4-5-20250929", "claude-sonnet-4.5"),
            ("claude-3-5-sonnet-20240620", "claude-sonnet-3.5-20240620"),
            ("claude-3-5-sonnet-20241022", "claude-sonnet-3.5-v2"),
        ] {
            assert_eq!(
                for_offer("claude", native, None),
                Some(expected),
                "{native}"
            );
        }
        for native in [
            "opus",
            "sonnet",
            "haiku",
            "fable",
            "claude-opus-99-7",
            "claude-opus-4-6-20990101",
            "claude-opus-4-7-20260416",
            "claude-sonnet-4-5-20250514",
        ] {
            assert_eq!(for_offer("claude", native, None), None, "{native}");
        }
    }

    #[test]
    fn batch_offers_share_base_identity_only_with_an_exact_slug_or_approved_mapping() {
        for id in [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-oss-20b",
            "gpt-oss-120b",
            "gpt-4-turbo",
        ] {
            let base = format!("openai/{id}");
            let batch = format!("{base}:batch");
            assert_eq!(for_offer("openrouter", &batch, Some(&base)), Some(id));
            assert_eq!(for_offer("openrouter", &batch, None), None);
        }
        assert_eq!(
            for_offer(
                "openrouter",
                "openai/o4-mini:batch",
                Some("openai/o4-mini-2025-04-16")
            ),
            Some("o4-mini")
        );
        assert_eq!(
            for_offer("openrouter", "openai/o4-mini:batch", None),
            Some("o4-mini")
        );
        assert_eq!(
            for_offer("openrouter", "openai/o4-mini:unknown", None),
            None
        );
    }

    #[test]
    fn approved_openrouter_variants_resolve_to_the_benchmarked_weights() {
        for (native, expected) in [
            ("cohere/command-r7b-12-2024", "command-r7b"),
            ("meta-llama/llama-3.1-70b-instruct", "llama-3.1-70b"),
            ("meta-llama/llama-3.1-8b-instruct", "llama-3.1-8b"),
            ("meta-llama/llama-3.2-1b-instruct", "llama-3.2-1b"),
            ("meta-llama/llama-3.2-3b-instruct", "llama-3.2-3b"),
            ("meta-llama/llama-3.3-70b-instruct", "llama-3.3-70b"),
            ("nvidia/nemotron-3-super-120b-a12b", "nemotron-3-super"),
            ("amazon/nova-pro-v1", "nova-pro"),
            ("qwen/qwen-2.5-72b-instruct", "qwen2.5-72b"),
        ] {
            assert_eq!(
                for_offer("openrouter", native, None),
                Some(expected),
                "{native}"
            );
            // An approved native-id mapping takes precedence over a dated discovery slug.
            assert_eq!(
                for_offer("openrouter", native, Some("unrecognized/snapshot")),
                Some(expected),
                "{native}"
            );
            assert!(
                crate::benchmarks::shipped().model(expected).is_some(),
                "{expected}"
            );
            assert_eq!(for_offer("other-provider", native, None), None, "{native}");
        }
        for native in [
            "cohere/command-r7b-01-2025",
            "meta-llama/llama-3.1-70b-instruct:free",
            "nvidia/nemotron-3-super-49b-v1",
            "amazon/nova-pro-v2",
            "google/gemini-3-pro-image-preview",
        ] {
            assert_eq!(for_offer("openrouter", native, None), None, "{native}");
        }
    }

    #[test]
    fn offers_resolve_by_equality_with_the_makers_prefix_and_never_by_guess() {
        let offer = |provider, native, slug| for_offer(provider, native, slug);
        assert_eq!(
            offer(
                "openrouter",
                "anthropic/claude-opus-4.6",
                Some("anthropic/claude-4.6-opus-20260205")
            ),
            Some("claude-opus-4.6")
        );
        assert_eq!(
            offer("openrouter", "~anthropic/claude-opus-4.6", None),
            Some("claude-opus-4.6")
        );
        assert_eq!(offer("opencode-go", "kimi-k2.5", None), Some("kimi-k2.5"));
        assert_eq!(offer("codex", "gpt-5.5", None), Some("gpt-5.5"));
        for (native, canonical) in [
            ("claude-haiku-5-5", "claude-haiku-5.5"),
            ("claude-sonnet-5-5", "claude-sonnet-5.5"),
            ("claude-fable-5", "claude-fable-5"),
            ("claude-opus-5", "claude-opus-5"),
        ] {
            assert_eq!(offer("claude", native, None), Some(canonical));
        }
        assert_eq!(
            offer("openrouter", "deepseek/deepseek-v4-flash-vision-exp", None),
            Some("deepseek-v4-flash-vision-exp")
        );
        // The CLI decides which version an alias means.
        assert_eq!(offer("claude", "opus", None), None);
        // A prefix that is not the model's maker is not that model.
        assert_eq!(offer("openrouter", "someone/claude-opus-4.6", None), None);
        // Dates and hyphens are never rewritten.
        assert_eq!(offer("openrouter", "anthropic/claude-opus-4-6", None), None);
        assert_eq!(
            offer("openrouter", "anthropic/claude-opus-4.6-20260205", None),
            None
        );
        // An explicit entry reaches what the rule cannot.
        assert_eq!(
            offer("openrouter", "google/gemini-3-pro-preview", None),
            Some("gemini-3-pro")
        );
        // A model with no known maker never matches a prefixed slug.
        assert_eq!(
            offer("openrouter", "z-ai/glm-5-private-checkpoint", None),
            None
        );
    }
}
