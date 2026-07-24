//! Pure vector helpers shared across embedding code.
//!
//! No model dependency — these survive the fastembed removal and back both the
//! event-vibe path and the corpus recall path.

/// Serialize an f32 embedding vector to little-endian bytes for BLOB storage.
pub fn to_bytes(embedding: &[f32]) -> Vec<u8> {
    embedding.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Deserialize little-endian bytes back to an f32 vector.
pub fn from_bytes(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

/// Cosine similarity between two equal-length vectors.
/// Returns a value in [-1.0, 1.0]; 0.0 if either vector is all zeros.
pub(crate) fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vectors must have same dimensions");
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_bytes_roundtrip() {
        let original = vec![1.0_f32, -2.5, 0.0, 3.25];
        let recovered = from_bytes(&to_bytes(&original));
        assert_eq!(original, recovered);
    }

    #[test]
    fn to_bytes_empty() {
        let empty: Vec<f32> = vec![];
        assert!(to_bytes(&empty).is_empty());
        assert!(from_bytes(&[]).is_empty());
    }

    #[test]
    fn cosine_identical() {
        let v = vec![1.0, 2.0, 3.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal() {
        assert!(cosine_similarity(&[1.0, 0.0, 0.0], &[0.0, 1.0, 0.0]).abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite() {
        let sim = cosine_similarity(&[1.0, 2.0, 3.0], &[-1.0, -2.0, -3.0]);
        assert!((sim - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_vector() {
        assert_eq!(cosine_similarity(&[1.0, 2.0, 3.0], &[0.0, 0.0, 0.0]), 0.0);
    }
}
