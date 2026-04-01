use std::fmt;
use std::path::Path;

use fastembed::{
    EmbeddingModel, InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding,
    TextInitOptions, TokenizerFiles, UserDefinedEmbeddingModel,
};

/// Errors from embedding operations.
#[derive(Debug)]
pub enum EmbeddingError {
    /// Model failed to initialize
    InitError(String),
    /// Embedding computation failed
    ComputeError(String),
}

impl fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitError(msg) => write!(f, "embedding init error: {msg}"),
            Self::ComputeError(msg) => write!(f, "embedding compute error: {msg}"),
        }
    }
}

impl std::error::Error for EmbeddingError {}

/// Local embedding engine using fastembed.
///
/// Wraps a fastembed TextEmbedding model. The model is downloaded
/// on first use to the fastembed cache directory.
///
/// Note: `embed` requires `&mut self` per fastembed's API. The background
/// worker should own this exclusively. If shared access is needed,
/// wrap in `Arc<Mutex<EmbeddingEngine>>`.
pub struct EmbeddingEngine {
    model: TextEmbedding,
    model_name: String,
    dimensions: usize,
}

impl EmbeddingEngine {
    /// Initialize with the default model (all-MiniLM-L6-v2, 384 dims).
    pub fn new() -> Result<Self, EmbeddingError> {
        Self::with_model(EmbeddingModel::AllMiniLML6V2)
    }

    /// Load model from a local directory (e.g. bundled app resources).
    ///
    /// Reads model.onnx + tokenizer JSON files from disk. No HuggingFace download needed.
    pub fn with_local_model(model_dir: &Path) -> Result<Self, EmbeddingError> {
        let read = |name: &str| -> Result<Vec<u8>, EmbeddingError> {
            std::fs::read(model_dir.join(name))
                .map_err(|e| EmbeddingError::InitError(format!("Failed to read {}: {}", name, e)))
        };

        let model = UserDefinedEmbeddingModel::new(
            read("model.onnx")?,
            TokenizerFiles {
                tokenizer_file: read("tokenizer.json")?,
                config_file: read("config.json")?,
                special_tokens_map_file: read("special_tokens_map.json")?,
                tokenizer_config_file: read("tokenizer_config.json")?,
            },
        )
        .with_quantization(QuantizationMode::None)
        .with_pooling(Pooling::Mean);

        let te = TextEmbedding::try_new_from_user_defined(model, InitOptionsUserDefined::default())
            .map_err(|e| EmbeddingError::InitError(e.to_string()))?;

        Ok(Self {
            model: te,
            model_name: "all-MiniLM-L6-v2".into(),
            dimensions: 384,
        })
    }

    /// Initialize with a specific fastembed model.
    pub fn with_model(model_id: EmbeddingModel) -> Result<Self, EmbeddingError> {
        let options = TextInitOptions::new(model_id.clone()).with_show_download_progress(true);

        // Get model info for dimensions before consuming the model_id
        let model_name = model_name(&model_id);
        let model = TextEmbedding::try_new(options)
            .map_err(|e| EmbeddingError::InitError(e.to_string()))?;

        let dimensions = TextEmbedding::get_model_info(&model_id)
            .map(|info| info.dim)
            .unwrap_or(384);

        Ok(Self {
            model,
            model_name,
            dimensions,
        })
    }

    /// The model name (e.g. "all-MiniLM-L6-v2").
    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    /// The embedding dimension count (e.g. 384).
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Compute embeddings for one or more texts.
    ///
    /// Returns one `Vec<f32>` per input text. Batch processing is
    /// more efficient than calling one at a time.
    pub fn embed(&mut self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(vec![]);
        }
        self.model
            .embed(texts, None)
            .map_err(|e| EmbeddingError::ComputeError(e.to_string()))
    }

    /// Compute embedding for a single text.
    pub fn embed_one(&mut self, text: &str) -> Result<Vec<f32>, EmbeddingError> {
        let mut results = self.embed(vec![text.to_string()])?;
        results
            .pop()
            .ok_or_else(|| EmbeddingError::ComputeError("empty result".into()))
    }

    /// Serialize an f32 embedding vector to bytes (little-endian) for BLOB storage.
    pub fn to_bytes(embedding: &[f32]) -> Vec<u8> {
        embedding.iter().flat_map(|f| f.to_le_bytes()).collect()
    }

    /// Deserialize bytes (little-endian) back to f32 vector.
    pub fn from_bytes(bytes: &[u8]) -> Vec<f32> {
        bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect()
    }

    /// Compute cosine similarity between two embedding vectors.
    /// Returns a value between -1.0 and 1.0 (1.0 = identical).
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "vectors must have same dimensions");
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }
        dot / (norm_a * norm_b)
    }
}

/// Get a human-readable model name for storage.
fn model_name(model: &EmbeddingModel) -> String {
    match model {
        EmbeddingModel::AllMiniLML6V2 => "all-MiniLM-L6-v2".into(),
        EmbeddingModel::AllMiniLML6V2Q => "all-MiniLM-L6-v2-quantized".into(),
        EmbeddingModel::AllMiniLML12V2 => "all-MiniLM-L12-v2".into(),
        EmbeddingModel::BGESmallENV15 => "bge-small-en-v1.5".into(),
        EmbeddingModel::BGEBaseENV15 => "bge-base-en-v1.5".into(),
        EmbeddingModel::NomicEmbedTextV15 => "nomic-embed-text-v1.5".into(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_to_bytes_roundtrip() {
        let original = vec![1.0_f32, -2.5, 0.0, 3.14159];
        let bytes = EmbeddingEngine::to_bytes(&original);
        let recovered = EmbeddingEngine::from_bytes(&bytes);
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_to_bytes_empty() {
        let empty: Vec<f32> = vec![];
        let bytes = EmbeddingEngine::to_bytes(&empty);
        assert!(bytes.is_empty());
        let recovered = EmbeddingEngine::from_bytes(&bytes);
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let v = vec![1.0, 2.0, 3.0];
        let sim = EmbeddingEngine::cosine_similarity(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = EmbeddingEngine::cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![-1.0, -2.0, -3.0];
        let sim = EmbeddingEngine::cosine_similarity(&a, &b);
        assert!((sim - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_zero_vector() {
        let a = vec![1.0, 2.0, 3.0];
        let zero = vec![0.0, 0.0, 0.0];
        let sim = EmbeddingEngine::cosine_similarity(&a, &zero);
        assert_eq!(sim, 0.0);
    }

    // --- with_local_model tests ---

    #[test]
    fn with_local_model_nonexistent_dir_returns_init_error() {
        let result = EmbeddingEngine::with_local_model(Path::new("/nonexistent/path/to/model"));
        match result {
            Err(EmbeddingError::InitError(_)) => {} // expected
            Err(e) => panic!("expected InitError, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn with_local_model_missing_file_error_names_the_file() {
        let dir = tempfile::tempdir().unwrap();
        // Write only model.onnx — the other 4 files are missing
        std::fs::write(dir.path().join("model.onnx"), b"fake").unwrap();

        let result = EmbeddingEngine::with_local_model(dir.path());
        let msg = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for missing tokenizer files"),
        };
        // Should mention the first missing tokenizer file
        assert!(
            msg.contains("tokenizer.json"),
            "error should name the missing file, got: {msg}"
        );
    }

    #[test]
    fn with_local_model_empty_dir_errors_on_model_onnx() {
        let dir = tempfile::tempdir().unwrap();
        let msg = match EmbeddingEngine::with_local_model(dir.path()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for empty dir"),
        };
        assert!(
            msg.contains("model.onnx"),
            "error should mention model.onnx, got: {msg}"
        );
    }

    /// Integration test: loads the real bundled model and verifies it produces embeddings.
    /// Requires the model files to exist at src-tauri/resources/models/embeddings/.
    #[test]
    fn with_local_model_loads_bundled_model_and_embeds() {
        let model_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../resources/models/embeddings");

        if !model_dir.join("model.onnx").exists() {
            eprintln!("Skipping: model.onnx not present (run scripts/download-embedding-model.sh)");
            return;
        }

        let mut engine =
            EmbeddingEngine::with_local_model(&model_dir).expect("should load from bundled files");

        assert_eq!(engine.model_name(), "all-MiniLM-L6-v2");
        assert_eq!(engine.dimensions(), 384);

        let embedding = engine
            .embed_one("test sentence for embedding")
            .expect("should produce embedding");
        assert_eq!(embedding.len(), 384, "embedding should have 384 dimensions");

        // Verify non-trivial output (not all zeros)
        let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            magnitude > 0.1,
            "embedding should be non-trivial, got magnitude {magnitude}"
        );
    }

    /// Verify bundled model produces semantically reasonable embeddings:
    /// similar texts should be more similar than dissimilar ones.
    #[test]
    fn with_local_model_semantic_similarity() {
        let model_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../resources/models/embeddings");

        if !model_dir.join("model.onnx").exists() {
            eprintln!("Skipping: model.onnx not present");
            return;
        }

        let mut engine = EmbeddingEngine::with_local_model(&model_dir).expect("should load model");

        let embeddings = engine
            .embed(vec![
                "The cat sat on the mat".into(),
                "A kitten rested on the rug".into(),
                "Quantum chromodynamics describes the strong force".into(),
            ])
            .expect("should embed batch");

        let similar = EmbeddingEngine::cosine_similarity(&embeddings[0], &embeddings[1]);
        let dissimilar = EmbeddingEngine::cosine_similarity(&embeddings[0], &embeddings[2]);

        assert!(
            similar > dissimilar,
            "similar texts ({similar:.3}) should score higher than dissimilar ({dissimilar:.3})"
        );
    }
}
