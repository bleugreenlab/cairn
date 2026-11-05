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
//!
//! One tokenizer ships inside the crate: OpenAI's o200k encoding, as gpt-oss
//! publishes it, behind [`o200k`]. Every other tokenizer is the caller's to
//! fetch and pin.

use std::fmt;
use std::path::Path;
use std::sync::OnceLock;

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

/// Whether a tokenizer recognizes its added tokens (`<|endoftext|>`,
/// `<|im_end|>` and the like) in the text it encodes. Fixed at load, because
/// the mode changes where toks can certify cuts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AddedTokens {
    /// Recognized as the serving engine recognizes them: hf's default.
    #[default]
    Matched,
    /// Never recognized: an added token's text is encoded as ordinary text,
    /// as tiktoken's `encode_ordinary` encodes it.
    Ignored,
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
    /// The tokenizer embedded in this crate did not unpack to its pinned
    /// bytes; nothing was handed to toks, or toks parsed something else.
    Embedded(String),
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
            LoadError::Embedded(what) => write!(f, "the embedded tokenizer is not its pinned file: {what}"),
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
        Self::from_json_with(bytes, AddedTokens::Matched)
    }

    /// [`Tokenizer::from_json`] with the added-token mode named.
    pub fn from_json_with(bytes: &[u8], added: AddedTokens) -> Result<Self, LoadError> {
        imp::Tokenizer::from_json(bytes, added).map(|inner| Self { inner })
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

    /// The content token ids of `text`. A long text is encoded in certified
    /// parts, so memory stays bounded; the ids are exactly the whole text's.
    pub fn encode(&self, text: &str) -> Result<Vec<u32>, EncodeError> {
        self.inner.encode(text)
    }

    /// [`Tokenizer::encode`] in one toks call, never split: the oracle the
    /// part sums are tested against. Its scratch grows with the text (tens of
    /// bytes per byte), so it is no path for production text.
    #[doc(hidden)]
    pub fn encode_whole(&self, text: &str) -> Result<Vec<u32>, EncodeError> {
        self.inner.encode_whole(text)
    }

    /// The raw bytes `id` stands for (not hf's vocabulary spelling: a
    /// byte-level `"Ġthe"` is `b" the"`), or `None` for an id with no string.
    pub fn token_bytes(&self, id: u32) -> Option<&[u8]> {
        self.inner.token_bytes(id)
    }
}

/// Where the embedded o200k tokenizer comes from: gpt-oss's `tokenizer.json`,
/// which carries o200k_base's vocabulary and its exact pre-tokenizer pattern.
/// `data/gpt-oss-tokenizer.json.zst` is that file compressed with
/// `zstd -19`, and `data/LICENSE-gpt-oss` its license, both fetched from this
/// repository at this revision.
pub const O200K_REPO: &str = "openai/gpt-oss-20b";
pub const O200K_REVISION: &str = "6cee5e81ee83917806bbde320786a8fb61efebee";
/// The sha256 of the uncompressed file, which is what toks parses and reports.
pub const O200K_SHA256: &str = "0614fe83cadab421296e664e1f48f4261fa8fef6e03e63bb75c20f38e37d07d3";
pub const O200K_SIZE: usize = 27_868_174;

/// OpenAI's o200k_base encoding, loaded once from the embedded gpt-oss file
/// with added tokens [ignored](AddedTokens::Ignored), so its ids are
/// tiktoken's `encode_ordinary` ids: `<|endoftext|>` in a file being read is
/// seven ordinary tokens, not a control token.
///
/// [`LoadError::UnsupportedTarget`] where toks is not linked; the embedded
/// bytes are not even compiled in there. A load on a linked target that
/// fails is a defect, never a property of the input, so the answer is cached
/// for the life of the process either way.
pub fn o200k() -> Result<&'static Tokenizer, LoadError> {
    static O200K: OnceLock<Result<Tokenizer, LoadError>> = OnceLock::new();
    O200K
        .get_or_init(|| {
            let json = imp::o200k_json()?;
            let tokenizer = Tokenizer::from_json_with(&json, AddedTokens::Ignored)?;
            let parsed = hex(&tokenizer.source_sha256());
            if parsed != O200K_SHA256 {
                return Err(LoadError::Embedded(format!(
                    "toks parsed sha256 {parsed}, the pin is {O200K_SHA256}"
                )));
            }
            Ok(tokenizer)
        })
        .as_ref()
        .map_err(Clone::clone)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
