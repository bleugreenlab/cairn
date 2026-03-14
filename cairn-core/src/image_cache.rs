//! Cache for pre-computed base64 image data.
//!
//! When images are pasted in the frontend, they're saved to disk and the base64
//! is cached here. When the message is sent to Claude, stdin.rs can retrieve
//! the cached base64 instantly instead of re-reading and encoding the file.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Cache for pre-computed base64 image data.
/// Images are cached when saved and consumed when sent to Claude.
#[derive(Default)]
pub struct ImageBase64Cache {
    cache: Mutex<HashMap<PathBuf, String>>,
}

impl ImageBase64Cache {
    /// Store base64 data for a path (already have it from frontend).
    pub fn insert(&self, path: String, base64_data: String) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(PathBuf::from(path), base64_data);
        }
    }

    /// Get cached base64, removing from cache (one-time use).
    /// Returns None if not cached, caller should fall back to reading the file.
    pub fn take(&self, path: &Path) -> Option<String> {
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(path)
        } else {
            None
        }
    }

    /// Remove entry for a path (e.g., when image is deleted).
    #[allow(dead_code)]
    pub fn remove(&self, path: &Path) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(path);
        }
    }

    /// Evict entries for paths that no longer exist (periodic cleanup).
    #[allow(dead_code)]
    pub fn cleanup(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.retain(|path, _| path.exists());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_insert_and_take() {
        let cache = ImageBase64Cache::default();
        let path = "/tmp/test-image.png".to_string();
        let base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==".to_string();

        cache.insert(path.clone(), base64.clone());

        // First take should return the data
        let result = cache.take(Path::new(&path));
        assert_eq!(result, Some(base64));

        // Second take should return None (consumed)
        let result = cache.take(Path::new(&path));
        assert_eq!(result, None);
    }

    #[test]
    fn test_take_nonexistent() {
        let cache = ImageBase64Cache::default();
        let result = cache.take(Path::new("/nonexistent/path.png"));
        assert_eq!(result, None);
    }

    #[test]
    fn test_remove() {
        let cache = ImageBase64Cache::default();
        let path = "/tmp/test-image.png".to_string();

        cache.insert(path.clone(), "base64data".to_string());
        cache.remove(Path::new(&path));

        let result = cache.take(Path::new(&path));
        assert_eq!(result, None);
    }

    #[test]
    fn test_cleanup_removes_nonexistent() {
        let dir = tempdir().unwrap();
        let existing_path = dir.path().join("exists.png");
        let missing_path = dir.path().join("missing.png");

        // Create the existing file
        fs::write(&existing_path, b"test").unwrap();

        let cache = ImageBase64Cache::default();
        cache.insert(
            existing_path.to_string_lossy().to_string(),
            "data1".to_string(),
        );
        cache.insert(
            missing_path.to_string_lossy().to_string(),
            "data2".to_string(),
        );

        cache.cleanup();

        // Existing file should still be cached
        assert!(cache.take(&existing_path).is_some());

        // Missing file should have been cleaned up
        assert!(cache.take(&missing_path).is_none());
    }
}
