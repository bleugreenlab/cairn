//! Tagged compression for archived event payloads.
//!
//! Each compressed payload is stored next to a per-row codec marker so that
//! decompression stays stable forever: adding a future codec never changes how
//! existing rows are read.

/// zstd compression level for archived payloads. Level 3 is zstd's default and a
/// good ratio/speed balance for the small text payloads events carry.
const ZSTD_LEVEL: i32 = 3;

/// Codec marker for an uncompressed payload.
pub const CODEC_NONE: &str = "none";
/// Codec marker for a zstd-compressed payload (version 1).
pub const CODEC_ZSTD_V1: &str = "zstd_v1";

/// Compress `data` with zstd, to be stored alongside the [`CODEC_ZSTD_V1`] marker.
pub fn compress(data: &[u8]) -> Result<Vec<u8>, String> {
    zstd::encode_all(data, ZSTD_LEVEL).map_err(|e| format!("zstd compression failed: {e}"))
}

/// Decompress `blob` according to its stored `codec` marker.
///
/// The marker — not the call site — decides how bytes are read, so rows written
/// under one codec keep decoding correctly after newer codecs are introduced.
pub fn decompress(codec: &str, blob: &[u8]) -> Result<Vec<u8>, String> {
    match codec {
        CODEC_ZSTD_V1 => {
            zstd::decode_all(blob).map_err(|e| format!("zstd decompression failed: {e}"))
        }
        CODEC_NONE => Ok(blob.to_vec()),
        other => Err(format!("unknown codec marker: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_text_payload() {
        let original = "the quick brown fox jumps over the lazy dog\n".repeat(64);
        let blob = compress(original.as_bytes()).unwrap();
        assert!(
            blob.len() < original.len(),
            "repetitive text should compress"
        );
        assert_eq!(
            decompress(CODEC_ZSTD_V1, &blob).unwrap(),
            original.as_bytes()
        );
    }

    #[test]
    fn round_trips_binary_payload() {
        let original: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let blob = compress(&original).unwrap();
        assert_eq!(decompress(CODEC_ZSTD_V1, &blob).unwrap(), original);
    }

    #[test]
    fn round_trips_empty_payload() {
        let blob = compress(&[]).unwrap();
        assert_eq!(decompress(CODEC_ZSTD_V1, &blob).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn none_marker_passes_bytes_through() {
        assert_eq!(decompress(CODEC_NONE, b"raw bytes").unwrap(), b"raw bytes");
    }

    #[test]
    fn unknown_marker_is_rejected() {
        assert!(decompress("zstd_v2", b"anything").is_err());
    }
}
