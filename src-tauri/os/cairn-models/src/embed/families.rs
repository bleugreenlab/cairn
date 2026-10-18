//! What an embedding model family expects, wherever it is served.
//!
//! The prefix a model wants on a query is a property of the model, not of
//! the provider serving it: nomic wants `search_query: ` on Ollama and on
//! OpenRouter alike. Providers take purpose handling (and, where they can
//! honour it, Matryoshka widths) from here, and native widths from their own
//! source when they have one. A model this table does not know embeds with no
//! prefix, which is honest; every entry is cited to its model card, and a
//! family whose facts could not be confirmed is left out rather than guessed.

use cairn_models_types::embedding::{
    BatchLimits, EmbeddingCapabilities, EmbeddingWidths, PurposeHandling,
};

/// What one model family expects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Family {
    /// The width the family's card states, when it states one per model.
    pub native_dims: Option<u32>,
    /// The widths the model can be shortened to (Matryoshka), when it can.
    pub widths: EmbeddingWidths,
    pub purpose: PurposeHandling,
}

fn prefix(document: &str, query: &str) -> PurposeHandling {
    PurposeHandling::Prefix {
        document: document.to_string(),
        query: query.to_string(),
    }
}

fn family_of(
    native_dims: Option<u32>,
    widths: EmbeddingWidths,
    purpose: PurposeHandling,
) -> Family {
    Family {
        native_dims,
        widths,
        purpose,
    }
}

/// The name a model id is matched by: its last path segment, lowercased, with
/// an Ollama tag joined by `-` (`qwen3-embedding:4b` is `qwen3-embedding-4b`)
/// and a bare `latest` tag dropped.
fn normalize(model_id: &str) -> String {
    let segment = model_id.rsplit('/').next().unwrap_or(model_id);
    let name = segment.to_ascii_lowercase().replace(':', "-");
    name.strip_suffix("-latest")
        .map(str::to_string)
        .unwrap_or(name)
}

/// The family `model_id` belongs to, when this table knows it.
pub fn family(model_id: &str) -> Option<Family> {
    let name = normalize(model_id);
    let starts = |prefix: &str| name.starts_with(prefix);

    // OpenAI: "By default, the length of the embedding vector is 1536 for
    // text-embedding-3-small or 3072 for text-embedding-3-large", shortened
    // with `dimensions`, which is "only supported in text-embedding-3 and later
    // models"; no minimum is documented. No prefixes.
    // https://developers.openai.com/api/docs/guides/embeddings
    if starts("text-embedding-3-small") {
        return Some(family_of(
            Some(1536),
            EmbeddingWidths::UpTo { min: 1 },
            PurposeHandling::Ignored,
        ));
    }
    if starts("text-embedding-3-large") {
        return Some(family_of(
            Some(3072),
            EmbeddingWidths::UpTo { min: 1 },
            PurposeHandling::Ignored,
        ));
    }
    // ada-002 is 1536 (https://openai.com/index/new-embedding-models-and-api-updates/).
    if starts("text-embedding-ada-002") {
        return Some(family_of(
            Some(1536),
            EmbeddingWidths::NativeOnly,
            PurposeHandling::Ignored,
        ));
    }

    // Qwen3-Embedding: "Up to 1024 [2560, 4096], supports user-defined output
    // dimensions ranging from 32"; queries carry `Instruct: {task}\nQuery: `
    // with the card's default retrieval task, and "No need to add instruction
    // for retrieval documents".
    // https://huggingface.co/Qwen/Qwen3-Embedding-4B
    let qwen = |native| {
        family_of(
            Some(native),
            EmbeddingWidths::UpTo { min: 32 },
            prefix(
                "",
                "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: ",
            ),
        )
    };
    if starts("qwen3-embedding-0.6b") {
        return Some(qwen(1024));
    }
    if starts("qwen3-embedding-4b") {
        return Some(qwen(2560));
    }
    if starts("qwen3-embedding-8b") {
        return Some(qwen(4096));
    }

    // nomic-embed-text v1.5 (Ollama's `nomic-embed-text` is v1.5): 768 wide,
    // Matryoshka to 64, and "the text prompt must include a task instruction
    // prefix": `search_document: ` / `search_query: `.
    // https://huggingface.co/nomic-ai/nomic-embed-text-v1.5
    if name == "nomic-embed-text" || starts("nomic-embed-text-v1.5") {
        return Some(family_of(
            Some(768),
            EmbeddingWidths::UpTo { min: 64 },
            prefix("search_document: ", "search_query: "),
        ));
    }

    // E5: "Each input text should start with "query: " or "passage: "". Widths
    // differ by size, so they come from the provider.
    // https://huggingface.co/intfloat/e5-base-v2
    if starts("e5-") || starts("multilingual-e5-") {
        return Some(family_of(
            None,
            EmbeddingWidths::NativeOnly,
            prefix("passage: ", "query: "),
        ));
    }

    // BGE v1.5 (English): the retrieval query instruction is "Represent this
    // sentence for searching relevant passages:", and "no instruction needs to
    // be added to passages". Widths differ by size.
    // https://huggingface.co/BAAI/bge-base-en-v1.5
    if starts("bge-") && name.contains("-en-v1.5") {
        return Some(family_of(
            None,
            EmbeddingWidths::NativeOnly,
            prefix(
                "",
                "Represent this sentence for searching relevant passages: ",
            ),
        ));
    }

    // EmbeddingGemma: 768, "with smaller options available (512, 256, or 128)
    // via Matryoshka"; retrieval prompts `task: search result | query: ` and
    // `title: none | text: `.
    // https://huggingface.co/google/embeddinggemma-300m
    if starts("embeddinggemma") {
        return Some(family_of(
            Some(768),
            EmbeddingWidths::Choices(vec![512, 256, 128]),
            prefix("title: none | text: ", "task: search result | query: "),
        ));
    }

    None
}

/// What an offer of `model_id` declares, given what its provider knows.
///
/// `native_dims` is the provider's own width when it has one (Ollama reports
/// it), else the family's. Shorter widths are declared only when `matryoshka`
/// says the provider honours `dimensions` and the provider's width is the
/// family's: a server that ignores the field is caught by the facet anyway,
/// but declaring what it cannot do would only route requests into refusals.
pub fn capabilities(
    model_id: &str,
    native_dims: Option<u32>,
    matryoshka: bool,
    limits: BatchLimits,
) -> EmbeddingCapabilities {
    let family = family(model_id);
    let family_native = family.as_ref().and_then(|family| family.native_dims);
    let native_dims = native_dims.or(family_native);
    let widths = match &family {
        Some(family) if matryoshka && family_native.is_some() && native_dims == family_native => {
            family.widths.clone()
        }
        _ => EmbeddingWidths::NativeOnly,
    };
    EmbeddingCapabilities {
        native_dims,
        widths,
        purpose: family
            .map(|family| family.purpose)
            .unwrap_or(PurposeHandling::Ignored),
        limits,
    }
}
