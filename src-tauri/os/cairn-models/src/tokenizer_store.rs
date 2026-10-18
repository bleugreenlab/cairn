//! Fetching and caching the tokenizer files Cairn pins
//! (`types::tokenizers`), verified at every step.
//!
//! Files are fetched from Hugging Face at the pin's exact revision, each body
//! capped at its pinned size and hashed as it streams, then renamed into
//! `<root>/<digest>/<path>` so a reader never sees a partial file. Whatever
//! the cache holds is hashed again immediately before it is handed on: for a
//! single-file pin the bytes returned are the bytes hashed, so nothing can
//! change between verifying and parsing; for a directory pin (Kimi), the
//! verified buffers are written into a private [`Snapshot`] holding exactly
//! the pin's files, which is what the caller loads. Nothing here ever executes
//! a fetched file; Kimi's `.py` files are read by toks as text.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::types::tokenizers::{PinnedFile, TokenizerPin};

/// A pinned tokenizer, verified and ready for toks.
#[derive(Debug)]
pub enum PinnedSource {
    /// A single `tokenizer.json`'s bytes, hashed as returned.
    Bytes(Vec<u8>),
    /// A private directory holding exactly the pin's files (a tiktoken model).
    Dir(Snapshot),
}

/// A private copy of a directory pin, holding the verified bytes of exactly
/// its files and nothing else, removed when dropped. Keep it alive until
/// toks has finished loading from [`Snapshot::path`].
///
/// toks' directory loader chooses which file to parse by what the directory
/// contains (a `tokenizer.json` wins over `tiktoken.model`), so loading from
/// the cache directory itself would let any extra file there be parsed in
/// place of the verified ones. The snapshot is created owner-only and written
/// from the very buffers that were hashed.
#[derive(Debug)]
pub struct Snapshot {
    dir: tempfile::TempDir,
}

impl Snapshot {
    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The download failed; worth retrying later.
    Fetch(String),
    /// Bytes that are not the pinned bytes, downloaded or cached. Upstream
    /// serving something else at a pinned revision is not transient.
    Mismatch {
        path: String,
        expected: String,
        actual: String,
    },
    /// The cache directory could not be read or written.
    Io(String),
}

impl StoreError {
    /// Whether a later attempt could succeed without a new pin.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, StoreError::Mismatch { .. })
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Fetch(reason) => write!(f, "tokenizer download failed: {reason}"),
            StoreError::Mismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "{path} is not the pinned file: expected {expected}, got {actual}"
            ),
            StoreError::Io(reason) => write!(f, "tokenizer cache: {reason}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub const HUGGING_FACE: &str = "https://huggingface.co";

pub struct TokenizerStore {
    root: PathBuf,
    base_url: String,
}

impl TokenizerStore {
    /// A store caching under `root` (conventionally
    /// `<cairn home>/models/tokenizers`) and fetching from Hugging Face.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_base_url(root, HUGGING_FACE)
    }

    pub fn with_base_url(root: impl Into<PathBuf>, base_url: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    /// The pin's files, fetched if absent or wrong, and verified.
    pub fn ensure(&self, pin: &TokenizerPin) -> Result<PinnedSource, StoreError> {
        let dir = self.root.join(pin.digest);
        for file in pin.files {
            let path = dir.join(file.path);
            if read_verified(&path, file).is_err() {
                self.fetch(pin, file, &path)?;
            }
        }
        if pin.is_directory() {
            let snapshot = tempfile::Builder::new()
                .prefix("cairn-tokenizer-")
                .tempdir()
                .map_err(|error| StoreError::Io(format!("tokenizer snapshot: {error}")))?;
            for file in pin.files {
                let bytes = read_verified(&dir.join(file.path), file)?;
                let path = snapshot.path().join(file.path);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
                }
                fs::write(&path, &bytes).map_err(|error| io_error(&path, error))?;
            }
            Ok(PinnedSource::Dir(Snapshot { dir: snapshot }))
        } else {
            read_verified(&dir.join(pin.files[0].path), &pin.files[0]).map(PinnedSource::Bytes)
        }
    }

    fn fetch(&self, pin: &TokenizerPin, file: &PinnedFile, path: &Path) -> Result<(), StoreError> {
        let url = format!(
            "{}/{}/resolve/{}/{}",
            self.base_url, pin.repo, pin.revision, file.path
        );
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(600))
            .build()
            .map_err(|error| StoreError::Fetch(error.to_string()))?;
        let response = client
            .get(&url)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| StoreError::Fetch(format!("{url}: {error}")))?;
        // One byte past the pinned size is enough to know the body is wrong
        // without reading an unbounded one.
        let mut bytes = Vec::with_capacity(file.size as usize);
        response
            .take(file.size + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| StoreError::Fetch(format!("{url}: {error}")))?;
        verify(file, &bytes)?;

        let parent = path.parent().expect("a cache path has a parent");
        fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        let staging = parent.join(format!(
            ".{}.{}.partial",
            file.path.replace('/', "_"),
            std::process::id()
        ));
        fs::write(&staging, &bytes).map_err(|error| io_error(&staging, error))?;
        fs::rename(&staging, path).map_err(|error| io_error(path, error))
    }
}

fn read_verified(path: &Path, file: &PinnedFile) -> Result<Vec<u8>, StoreError> {
    let bytes = fs::read(path).map_err(|error| io_error(path, error))?;
    verify(file, &bytes)?;
    Ok(bytes)
}

fn verify(file: &PinnedFile, bytes: &[u8]) -> Result<(), StoreError> {
    let actual = if bytes.len() as u64 == file.size {
        hex(&Sha256::digest(bytes))
    } else {
        format!("{} bytes", bytes.len())
    };
    if actual == file.sha256 {
        Ok(())
    } else {
        Err(StoreError::Mismatch {
            path: file.path.to_string(),
            expected: file.sha256.to_string(),
            actual,
        })
    }
}

fn io_error(path: &Path, error: std::io::Error) -> StoreError {
    StoreError::Io(format!("{}: {error}", path.display()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Serves `body` for every request and counts the requests.
    fn serve(body: Vec<u8>) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                counter.fetch_add(1, Ordering::SeqCst);
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                while reader.read_line(&mut line).is_ok_and(|n| n > 2) {
                    line.clear();
                }
                let mut stream = stream;
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(&body);
            }
        });
        (url, hits)
    }

    fn pin_for(contents: &[(&'static str, &[u8])]) -> TokenizerPin {
        let files: Vec<PinnedFile> = contents
            .iter()
            .map(|(path, bytes)| PinnedFile {
                path,
                sha256: Box::leak(hex(&Sha256::digest(bytes)).into_boxed_str()),
                size: bytes.len() as u64,
            })
            .collect();
        TokenizerPin {
            family: "fixture",
            repo: "example/fixture",
            revision: "0123456789012345678901234567890123456789",
            digest: Box::leak(hex(&Sha256::digest(contents[0].1)).into_boxed_str()),
            files: Box::leak(files.into_boxed_slice()),
            upstreams: &[],
            offers: &[],
        }
    }

    #[test]
    fn fetches_once_then_answers_from_the_cache() {
        let body = b"{\"model\":{}}".to_vec();
        let pin = pin_for(&[("tokenizer.json", &body)]);
        let (url, hits) = serve(body.clone());
        let root = tempfile::tempdir().unwrap();
        let store = TokenizerStore::with_base_url(root.path(), url);
        for _ in 0..3 {
            match store.ensure(&pin).unwrap() {
                PinnedSource::Bytes(bytes) => assert_eq!(bytes, body),
                PinnedSource::Dir(_) => panic!("a single-file pin returns its bytes"),
            }
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert!(root
            .path()
            .join(pin.digest)
            .join("tokenizer.json")
            .is_file());
    }

    #[test]
    fn a_corrupt_cache_entry_is_refetched() {
        let body = b"pinned bytes".to_vec();
        let pin = pin_for(&[("tokenizer.json", &body)]);
        let (url, hits) = serve(body.clone());
        let root = tempfile::tempdir().unwrap();
        let cached = root.path().join(pin.digest).join("tokenizer.json");
        fs::create_dir_all(cached.parent().unwrap()).unwrap();
        fs::write(&cached, b"pinned bytez").unwrap();
        let store = TokenizerStore::with_base_url(root.path(), url);
        assert!(matches!(store.ensure(&pin).unwrap(), PinnedSource::Bytes(bytes) if bytes == body));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(fs::read(&cached).unwrap(), body);
    }

    #[test]
    fn served_bytes_that_are_not_the_pin_are_a_terminal_mismatch() {
        let pin = pin_for(&[("tokenizer.json", b"pinned bytes")]);
        let (url, _) = serve(b"other  bytes".to_vec());
        let root = tempfile::tempdir().unwrap();
        let error = TokenizerStore::with_base_url(root.path(), url)
            .ensure(&pin)
            .unwrap_err();
        assert!(matches!(error, StoreError::Mismatch { .. }), "{error}");
        assert!(!error.is_retryable());
        assert!(!root.path().join(pin.digest).join("tokenizer.json").exists());
    }

    #[test]
    fn an_oversized_body_is_cut_off_and_refused() {
        let pin = pin_for(&[("tokenizer.json", b"short")]);
        let (url, _) = serve(vec![b'x'; 1 << 20]);
        let root = tempfile::tempdir().unwrap();
        let error = TokenizerStore::with_base_url(root.path(), url)
            .ensure(&pin)
            .unwrap_err();
        assert_eq!(
            error,
            StoreError::Mismatch {
                path: "tokenizer.json".into(),
                expected: pin.files[0].sha256.into(),
                actual: "6 bytes".into(),
            }
        );
    }

    #[test]
    fn an_unreachable_host_is_a_retryable_fetch_failure() {
        let pin = pin_for(&[("tokenizer.json", b"pinned")]);
        let unused = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", unused.local_addr().unwrap());
        drop(unused);
        let root = tempfile::tempdir().unwrap();
        let error = TokenizerStore::with_base_url(root.path(), url)
            .ensure(&pin)
            .unwrap_err();
        assert!(matches!(error, StoreError::Fetch(_)), "{error}");
        assert!(error.is_retryable());
    }

    #[test]
    fn a_directory_pin_returns_its_verified_directory() {
        // Every request gets the same body, so both files carry it.
        let body = b"same".to_vec();
        let pin = pin_for(&[("tiktoken.model", &body), ("tokenizer_config.json", &body)]);
        let (url, hits) = serve(body.clone());
        let root = tempfile::tempdir().unwrap();
        let store = TokenizerStore::with_base_url(root.path(), url);
        let PinnedSource::Dir(snapshot) = store.ensure(&pin).unwrap() else {
            panic!("a multi-file pin returns a directory");
        };
        assert_eq!(
            fs::read(snapshot.path().join("tiktoken.model")).unwrap(),
            body
        );
        assert_eq!(
            fs::read(snapshot.path().join("tokenizer_config.json")).unwrap(),
            body
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        let snapshot_path = snapshot.path().to_path_buf();
        drop(snapshot);
        assert!(
            !snapshot_path.exists(),
            "a snapshot is removed once dropped"
        );

        // A cached file altered after caching is refetched before any snapshot.
        let cached = root.path().join(pin.digest);
        fs::write(cached.join("tokenizer_config.json"), b"evil").unwrap();
        let PinnedSource::Dir(snapshot) = store.ensure(&pin).unwrap() else {
            panic!("a multi-file pin returns a directory");
        };
        assert_eq!(
            fs::read(snapshot.path().join("tokenizer_config.json")).unwrap(),
            body
        );
        assert_eq!(
            fs::read(cached.join("tokenizer_config.json")).unwrap(),
            body
        );
    }

    /// toks' directory loader parses a `tokenizer.json` in preference to
    /// `tiktoken.model`, so a foreign file planted beside a verified Kimi pin
    /// must never be in the directory toks is given.
    #[test]
    fn files_beside_a_directory_pin_never_reach_its_snapshot() {
        let body = b"same".to_vec();
        let pin = pin_for(&[("tiktoken.model", &body), ("tokenizer_config.json", &body)]);
        let (url, _) = serve(body.clone());
        let root = tempfile::tempdir().unwrap();
        let store = TokenizerStore::with_base_url(root.path(), url);
        drop(store.ensure(&pin).unwrap());

        let cached = root.path().join(pin.digest);
        fs::write(
            cached.join("tokenizer.json"),
            br#"{"model":{"type":"BPE"}}"#,
        )
        .unwrap();
        fs::write(cached.join("tokenization_kimi.py"), b"print('never run')").unwrap();

        let PinnedSource::Dir(snapshot) = store.ensure(&pin).unwrap() else {
            panic!("a multi-file pin returns a directory");
        };
        let mut names: Vec<String> = fs::read_dir(snapshot.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["tiktoken.model", "tokenizer_config.json"]);
        assert_ne!(snapshot.path(), cached.as_path());
    }
}
