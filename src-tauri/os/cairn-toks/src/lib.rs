//! Exact token counts for open-weight models through toks.
//!
//! [toks](https://github.com/actual-computer/toks) loads a model's own
//! `tokenizer.json` (or a Kimi `tiktoken.model` directory) and returns exactly
//! the ids Hugging Face tokenizers 0.23.2 returns, from a small C library with
//! caller-owned buffers, no threads and no allocation after load. It refuses at
//! load, naming the feature, when it cannot be exact.
//!
//! This crate links the library's pinned release (see `build.rs`) on macOS
//! arm64 and Linux x86-64/arm64 (glibc). On every other target it compiles a
//! stub: [`NATIVE`] is false and every load returns
//! [`LoadError::UnsupportedTarget`], so dependents build everywhere and fall
//! back to their own meters.
//!
//! Counts are content tokens: no BOS, EOS or template ids, and none of the
//! file's whole-document truncation or padding. Added tokens (`<|im_end|>`
//! and the like) are recognized as the serving engine recognizes them.
//!
//! The parser is young and unfuzzed, so callers must hand it only bytes they
//! pinned by digest; [`Tokenizer::source_sha256`] lets them confirm what toks
//! parsed.

use std::fmt;
use std::path::Path;

#[cfg(toks_native)]
mod native;
#[cfg(toks_native)]
mod sys;
#[cfg(toks_native)]
use native as imp;

#[cfg(not(toks_native))]
mod stub;
#[cfg(not(toks_native))]
use stub as imp;

/// Whether this build links toks. False on targets without a release bundle.
pub const NATIVE: bool = cfg!(toks_native);

/// The longest text toks encodes in one call (2^29 bytes).
pub const MAX_TEXT: usize = 1 << 29;

/// A prefix of a text that fits a token budget.
///
/// `byte_offset` is a char boundary, `tokens <= budget`, and `tokens` is
/// exactly the count of `&text[..byte_offset]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cut {
    pub byte_offset: usize,
    pub tokens: u32,
}

/// How [`Tokenizer::cut_with`] finds the prefix. [`Tokenizer::cut`] picks for
/// itself; naming one is for tests that must exercise both.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CutStrategy {
    /// Sum exact counts over toks' certified cut points, then bisect inside the
    /// one part that crosses the budget. Needs a tokenizer with certified cuts;
    /// on one without, this behaves as [`CutStrategy::TokenBytes`].
    Certified,
    /// Encode once, find the bytes the first `budget` tokens span, and recount
    /// the prefixes around that offset.
    TokenBytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoadError {
    /// This target has no toks bundle; nothing was parsed.
    UnsupportedTarget,
    /// toks refused the tokenizer. `what` is its diagnostic: the feature it
    /// cannot reproduce exactly, or why the file is invalid.
    Refused { code: i64, what: String },
    /// The linked library is not the pinned release, or speaks a different
    /// ABI than this crate mirrors.
    Library {
        version: String,
        abi_major: u32,
        abi_minor: u32,
    },
    /// The directory path cannot be passed to C.
    Path(String),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::UnsupportedTarget => write!(f, "toks has no build for this target"),
            LoadError::Refused { code, what } => write!(f, "toks refused the tokenizer ({code}): {what}"),
            LoadError::Library {
                version,
                abi_major,
                abi_minor,
            } => write!(
                f,
                "linked toks {version} (ABI {abi_major}.{abi_minor}) is not the pinned 0.3.2 (ABI 0.4)"
            ),
            LoadError::Path(path) => write!(f, "tokenizer path cannot be passed to toks: {path}"),
        }
    }
}

impl std::error::Error for LoadError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EncodeError {
    /// The text exceeds [`MAX_TEXT`].
    TooLong { len: usize },
    /// toks returned an error code (`TOKS_E_*`).
    Toks { code: i64 },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::TooLong { len } => {
                write!(f, "text of {len} bytes exceeds toks' {MAX_TEXT}-byte limit")
            }
            EncodeError::Toks { code } => write!(f, "toks encode failed with code {code}"),
        }
    }
}

impl std::error::Error for EncodeError {}

/// A loaded tokenizer. Read-only after load and shareable across threads;
/// each call borrows a pooled scratch buffer, so concurrent calls never share
/// one.
pub struct Tokenizer {
    inner: imp::Tokenizer,
}

impl Tokenizer {
    /// Load a `tokenizer.json` from its bytes (toks copies them).
    pub fn from_json(bytes: &[u8]) -> Result<Self, LoadError> {
        imp::Tokenizer::from_json(bytes).map(|inner| Self { inner })
    }

    /// Load a model directory: the only way toks reads a tiktoken model
    /// (Kimi's `tiktoken.model` beside its wrapper and config files).
    pub fn from_dir(dir: &Path) -> Result<Self, LoadError> {
        imp::Tokenizer::from_dir(dir).map(|inner| Self { inner })
    }

    /// The sha256 toks reports for the source it parsed.
    pub fn source_sha256(&self) -> [u8; 32] {
        self.inner.source_sha256()
    }

    /// The tokenizer's name as toks reports it.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Whether toks certifies cut points for this tokenizer (every family but
    /// Kimi's tiktoken wrapper, among those Cairn pins).
    pub fn has_certified_cuts(&self) -> bool {
        self.inner.has_certified_cuts()
    }

    /// The number of content tokens in `text`.
    pub fn count(&self, text: &str) -> Result<u32, EncodeError> {
        self.inner.count(text)
    }

    /// The longest prefix of `text` this meter can place within `budget`
    /// (best effort on maximality), with its exact count.
    pub fn cut(&self, text: &str, budget: u32) -> Result<Cut, EncodeError> {
        self.inner.cut(text, budget, None)
    }

    /// [`Tokenizer::cut`] with the strategy named.
    pub fn cut_with(
        &self,
        text: &str,
        budget: u32,
        strategy: CutStrategy,
    ) -> Result<Cut, EncodeError> {
        self.inner.cut(text, budget, Some(strategy))
    }

    /// The content token ids of `text`.
    pub fn encode(&self, text: &str) -> Result<Vec<u32>, EncodeError> {
        self.inner.encode(text)
    }
}
