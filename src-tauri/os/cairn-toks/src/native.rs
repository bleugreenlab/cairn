//! The toks-backed implementation, compiled where `build.rs` linked toks.
//!
//! # Exactness of the part sums
//!
//! `count` of a long text and every step of the certified `cut` sum the counts
//! of parts split at toks' certified cut points. Two documented properties of
//! those cuts (toks.h `toks_split_points`, docs/split.md) make the sums exact:
//!
//! 1. At a certified cut `c` of `x`, the ids of `x` are the ids of `x[..c]`
//!    followed by the ids of `x[c..]` encoded with `TOKS_CONTINUATION`.
//! 2. A cut is decided from a window of bytes around it only (the longest
//!    added token + 16 on each side), reading the end of input only when the
//!    window reaches it.
//!
//! By (2), a cut `c` certified in `x` is also certified in any prefix
//! `x[..p]` with `c + window <= p`: the predicate reads the same bytes and
//! never reaches the end. So with bounds spaced at least a window apart, the
//! count of each prefix `x[..bounds[i]]` is the previous prefix's count plus
//! the continuation count of the part between them, by induction from
//! `bounds[0] = 0`; and the last bound is certified in `x` itself, which
//! closes the sum at `x.len()`.

use std::ffi::{c_void, CStr, CString};
use std::mem::size_of;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::{Mutex, PoisonError};

use crate::sys;
use crate::{Cut, CutStrategy, EncodeError, LoadError};

/// Content tokens only: no template ids, no whole-document truncation or
/// padding. The added-token mode is toks' default, hf's.
const FLAGS: u32 = sys::TOKS_NO_POSTPROCESS | sys::TOKS_NO_TRUNCATE | sys::TOKS_NO_PAD;

/// The text length a pooled scratch is sized for. toks' scratch is about
/// 6 MiB plus 36 B per byte of this (111 B under NFC), so 64 KiB keeps a
/// pooled scratch near 8 MiB; longer parts get a one-off scratch.
const PART_MAX: usize = 64 * 1024;

/// The length of the coarse parts the certified `cut` sums over. The part that
/// crosses the budget is then bisected, encoding at most about two of these
/// per probe.
const CUT_PART: usize = 1024;

/// Scratches kept for reuse. Each keeps toks' segment memo, which answers a
/// recount of a body it has seen (the read-budget loop counts one body many
/// times) at memory speed.
const POOL_KEEP: usize = 4;

/// Char boundaries probed past the bisection's first over-budget offset,
/// because a BPE prefix count is not strictly monotone in prefix length.
const FORWARD_PROBES: usize = 8;

/// toks' cut window beyond the longest added token (docs/split.md).
const WINDOW_SLACK: usize = 16;

struct Ctx(NonNull<sys::toks_ctx>);

// SAFETY: toks.h's contract for every function: "a context is read-only after
// load and may be shared by any number of threads; ... no global is written;
// after load there is no allocation, syscall, lock, recursion or callback."
unsafe impl Send for Ctx {}
unsafe impl Sync for Ctx {}

impl Ctx {
    fn ptr(&self) -> *const sys::toks_ctx {
        self.0.as_ptr()
    }
}

impl Drop for Ctx {
    fn drop(&mut self) {
        // SAFETY: the context came from a successful toks_load*, and no scratch
        // or call outlives the Tokenizer that owns it.
        unsafe { sys::toks_unload(self.0.as_ptr()) }
    }
}

pub(crate) struct Tokenizer {
    ctx: Ctx,
    source_sha256: [u8; 32],
    name: String,
    /// toks' cut window for this tokenizer: no certified cut is decided by a
    /// byte farther than this from it.
    window: usize,
    certified_cuts: bool,
    scratch_bytes: u64,
    pool: Mutex<Vec<Vec<u8>>>,
}

impl Tokenizer {
    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self, LoadError> {
        load(|out, opts| {
            // SAFETY: `bytes` is valid for its length; toks copies it.
            unsafe { sys::toks_load_mem_copy(out, bytes.as_ptr().cast(), bytes.len() as u64, opts) }
        })
    }

    pub(crate) fn from_dir(dir: &Path) -> Result<Self, LoadError> {
        let path = CString::new(dir.as_os_str().as_bytes())
            .map_err(|_| LoadError::Path(dir.display().to_string()))?;
        // SAFETY: `path` is NUL-terminated and outlives the call.
        load(|out, opts| unsafe { sys::toks_load(out, path.as_ptr(), opts) })
    }

    pub(crate) fn source_sha256(&self) -> [u8; 32] {
        self.source_sha256
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn has_certified_cuts(&self) -> bool {
        self.certified_cuts
    }

    pub(crate) fn count(&self, text: &str) -> Result<u32, EncodeError> {
        check_len(text)?;
        let bytes = text.as_bytes();
        let mut scratch = self.checkout()?;
        if bytes.len() <= PART_MAX {
            return scratch.count(bytes, false).map(saturate);
        }
        let bounds = self.bounds(text, PART_MAX / 2)?;
        let mut total = 0u64;
        for (index, part) in bounds.windows(2).enumerate() {
            total += scratch.count(&bytes[part[0]..part[1]], index > 0)?;
        }
        Ok(saturate(total))
    }

    pub(crate) fn cut(
        &self,
        text: &str,
        budget: u32,
        strategy: Option<CutStrategy>,
    ) -> Result<Cut, EncodeError> {
        check_len(text)?;
        let certified = self.certified_cuts && strategy != Some(CutStrategy::TokenBytes);
        if certified {
            self.cut_certified(text, u64::from(budget))
        } else {
            self.cut_by_token_bytes(text, u64::from(budget))
        }
    }

    pub(crate) fn encode(&self, text: &str) -> Result<Vec<u32>, EncodeError> {
        check_len(text)?;
        if text.is_empty() {
            return Ok(Vec::new());
        }
        let mut scratch = self.checkout()?;
        let n = scratch.encode(text.as_bytes(), FLAGS, &mut [])?;
        let mut ids = vec![0u32; n as usize];
        let written = scratch.encode(text.as_bytes(), FLAGS, &mut ids)?;
        ids.truncate(written as usize);
        Ok(ids)
    }

    /// One pass over coarse certified parts gives the exact count at every
    /// bound; the budget falls inside one part, which is then bisected with
    /// each probe counted from the latest bound a full window behind it.
    fn cut_certified(&self, text: &str, budget: u64) -> Result<Cut, EncodeError> {
        let bytes = text.as_bytes();
        let bounds = self.bounds(text, CUT_PART)?;
        let mut scratch = self.checkout()?;
        let mut cumulative = Vec::with_capacity(bounds.len());
        cumulative.push(0u64);
        for (index, part) in bounds.windows(2).enumerate() {
            let tokens = scratch.count(&bytes[part[0]..part[1]], index > 0)?;
            cumulative.push(cumulative[index] + tokens);
        }
        let total = cumulative[cumulative.len() - 1];
        if total <= budget {
            return Ok(Cut {
                byte_offset: text.len(),
                tokens: saturate(total),
            });
        }

        // cumulative[0] = 0 fits, and the total does not, so 0 <= k < last.
        let k = cumulative.partition_point(|&tokens| tokens <= budget) - 1;
        let window = self.window;
        let count_at = |p: usize| -> Result<u64, EncodeError> {
            let base = (0..=k)
                .rev()
                .find(|&j| bounds[j] == 0 || bounds[j] + window <= p)
                .unwrap_or(0);
            let from = bounds[base];
            Ok(cumulative[base] + scratch.count(&bytes[from..p], from > 0)?)
        };
        bisect(
            text,
            bounds[k],
            cumulative[k],
            bounds[k + 1],
            budget,
            count_at,
        )
    }

    /// For tokenizers without certified cuts (Kimi's tiktoken wrapper): the
    /// token byte lengths place the budget's offset, and the prefixes around it
    /// are recounted whole. A tiktoken file has no normalizer, so its token
    /// bytes sum to the input's; if a normalizer ever made the estimate miss,
    /// the whole text is bisected instead.
    fn cut_by_token_bytes(&self, text: &str, budget: u64) -> Result<Cut, EncodeError> {
        let ids = self.encode(text)?;
        if ids.len() as u64 <= budget {
            return Ok(Cut {
                byte_offset: text.len(),
                tokens: saturate(ids.len() as u64),
            });
        }
        let spans = |n: u64| -> usize {
            ids.iter()
                .take(usize::try_from(n).unwrap_or(usize::MAX))
                .map(|&id| self.token_len(id))
                .sum::<usize>()
                .min(text.len())
        };
        let start = spans(budget.saturating_sub(1));
        let end = spans(budget + 1);
        let mut best = None;
        for offset in (start..=end).filter(|&offset| text.is_char_boundary(offset)) {
            let tokens = u64::from(self.count(&text[..offset])?);
            if tokens <= budget {
                best = Some(Cut {
                    byte_offset: offset,
                    tokens: saturate(tokens),
                });
            }
        }
        if let Some(cut) = best {
            return Ok(cut);
        }
        let mut scratch = self.checkout()?;
        let bytes = text.as_bytes();
        bisect(text, 0, 0, text.len(), budget, |p| {
            scratch.count(&bytes[..p], false)
        })
    }

    /// `[0, spaced certified cuts..., len]`. Cuts are requested about `target`
    /// bytes apart and kept only a full window past the previous kept cut, which
    /// is what makes the part sums exact (see the module docs).
    fn bounds(&self, text: &str, target: usize) -> Result<Vec<usize>, EncodeError> {
        let len = text.len();
        let mut bounds = vec![0];
        if self.certified_cuts && len >= 2 * target {
            let n_want = u32::try_from(len / target).unwrap_or(u32::MAX);
            for cut in self.split_points(text.as_bytes(), n_want)? {
                let last = bounds[bounds.len() - 1];
                let spaced = last == 0 || cut >= last + self.window;
                if cut > last && cut < len && spaced && text.is_char_boundary(cut) {
                    bounds.push(cut);
                }
            }
        }
        bounds.push(len);
        Ok(bounds)
    }

    fn split_points(&self, text: &[u8], n_want: u32) -> Result<Vec<usize>, EncodeError> {
        if n_want <= 1 {
            return Ok(Vec::new());
        }
        let mut offsets = vec![0u64; n_want as usize - 1];
        // SAFETY: text and offsets are valid for the lengths passed; the scratch
        // argument is reserved and may be NULL.
        let found = unsafe {
            sys::toks_split_points(
                self.ctx.ptr(),
                text.as_ptr().cast(),
                text.len() as u64,
                FLAGS,
                n_want,
                offsets.as_mut_ptr(),
                offsets.len() as u64,
                ptr::null_mut(),
            )
        };
        if found < 0 {
            return Err(EncodeError::Toks { code: found });
        }
        offsets.truncate(found as usize);
        Ok(offsets.into_iter().map(|offset| offset as usize).collect())
    }

    fn token_len(&self, id: u32) -> usize {
        let mut len = 0u64;
        // SAFETY: toks_token returns NULL for an id with no string and leaves
        // len meaningful only otherwise.
        let bytes = unsafe { sys::toks_token(self.ctx.ptr(), id, &mut len) };
        if bytes.is_null() {
            0
        } else {
            len as usize
        }
    }

    fn checkout(&self) -> Result<Scratch<'_>, EncodeError> {
        let pooled = self
            .pool
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let buffer = match pooled {
            Some(buffer) => buffer,
            None => self.new_scratch(self.scratch_bytes)?,
        };
        Ok(Scratch {
            tokenizer: self,
            buffer: Some(buffer),
        })
    }

    fn new_scratch(&self, bytes: u64) -> Result<Vec<u8>, EncodeError> {
        let mut buffer = vec![0u8; bytes as usize];
        // SAFETY: the buffer is `bytes` long; toks binds it to this context.
        // Moving the Vec later never moves its heap allocation.
        let code =
            unsafe { sys::toks_scratch_init(self.ctx.ptr(), buffer.as_mut_ptr().cast(), bytes, 0) };
        if code != 0 {
            return Err(EncodeError::Toks { code });
        }
        Ok(buffer)
    }

    fn probe_certified_cuts(&self) -> bool {
        let probe = "The quick brown fox jumps over the lazy dog. 0123456789\n".repeat(40);
        self.split_points(probe.as_bytes(), 8)
            .map(|cuts| !cuts.is_empty())
            .unwrap_or(false)
    }
}

fn load(
    call: impl FnOnce(*mut *mut sys::toks_ctx, *const sys::toks_load_opts) -> i64,
) -> Result<Tokenizer, LoadError> {
    let mut diag = sys::toks_diag {
        code: 0,
        what: [0; 248],
    };
    let opts = sys::toks_load_opts {
        size: size_of::<sys::toks_load_opts>() as u32,
        tier: sys::TOKS_TIER_AUTO,
        flags: 0,
        rsv: 0,
        diag: &mut diag,
    };
    let mut out = ptr::null_mut();
    let code = call(&mut out, &opts);
    let ctx = match NonNull::new(out) {
        Some(ctx) if code == 0 => Ctx(ctx),
        other => {
            if let Some(ctx) = other {
                drop(Ctx(ctx));
            }
            // SAFETY: toks NUL-terminates `what`; the buffer was zeroed first.
            let what = unsafe { CStr::from_ptr(diag.what.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            return Err(LoadError::Refused { code, what });
        }
    };

    // SAFETY: toks_info is plain old data; toks reads `size` and fills the rest.
    let mut info: sys::toks_info = unsafe { std::mem::zeroed() };
    info.size = size_of::<sys::toks_info>() as u32;
    // SAFETY: ctx is live and `info` is sized as declared.
    let code = unsafe { sys::toks_get_info(ctx.ptr(), &mut info) };
    if code != 0 {
        return Err(LoadError::Refused {
            code,
            what: "toks_get_info failed".to_string(),
        });
    }
    // SAFETY: toks_version returns a static NUL-terminated string.
    let version = unsafe { CStr::from_ptr(sys::toks_version()) }
        .to_string_lossy()
        .into_owned();
    if version != sys::VERSION
        || (info.abi_major, info.abi_minor) != (sys::ABI_MAJOR, sys::ABI_MINOR)
    {
        return Err(LoadError::Library {
            version,
            abi_major: info.abi_major,
            abi_minor: info.abi_minor,
        });
    }
    // SAFETY: toks NUL-terminates `name`.
    let name = unsafe { CStr::from_ptr(info.name.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let window = longest_added_token(&ctx) + WINDOW_SLACK;
    // SAFETY: ctx is live; the call is pure.
    let scratch_bytes = unsafe { sys::toks_scratch_bytes(ctx.ptr(), PART_MAX as u64, 0) };
    let mut tokenizer = Tokenizer {
        ctx,
        source_sha256: info.source_sha256,
        name,
        window,
        certified_cuts: false,
        scratch_bytes,
        pool: Mutex::new(Vec::new()),
    };
    tokenizer.certified_cuts = tokenizer.probe_certified_cuts();
    Ok(tokenizer)
}

fn longest_added_token(ctx: &Ctx) -> usize {
    let mut longest = 0usize;
    for index in 0u32.. {
        let mut len = 0u64;
        // SAFETY: content and id may be NULL; toks returns TOKS_E_ARG past the
        // last added token.
        let found = unsafe {
            sys::toks_added(ctx.ptr(), index, ptr::null_mut(), &mut len, ptr::null_mut())
        };
        if found < 0 {
            break;
        }
        longest = longest.max(len as usize);
    }
    longest
}

/// A scratch checked out of the pool, returned on drop.
struct Scratch<'t> {
    tokenizer: &'t Tokenizer,
    buffer: Option<Vec<u8>>,
}

impl Scratch<'_> {
    fn count(&mut self, text: &[u8], continuation: bool) -> Result<u64, EncodeError> {
        if text.is_empty() {
            return Ok(0);
        }
        let flags = if continuation {
            FLAGS | sys::TOKS_CONTINUATION
        } else {
            FLAGS
        };
        self.encode(text, flags, &mut [])
    }

    /// Encode with this scratch, or with a one-off scratch sized for a text
    /// longer than the pooled ones hold. Returns the full id count; `out`
    /// receives its prefix.
    fn encode(&mut self, text: &[u8], flags: u32, out: &mut [u32]) -> Result<u64, EncodeError> {
        if text.len() as u64 > sys::TOKS_MAX_TEXT {
            return Err(EncodeError::TooLong { len: text.len() });
        }
        let tokenizer = self.tokenizer;
        let mut oversized;
        let scratch = if text.len() <= PART_MAX {
            self.buffer
                .as_mut()
                .expect("a checked-out scratch holds its buffer")
        } else {
            // SAFETY: ctx is live; the call is pure.
            let bytes =
                unsafe { sys::toks_scratch_bytes(tokenizer.ctx.ptr(), text.len() as u64, 0) };
            oversized = tokenizer.new_scratch(bytes)?;
            &mut oversized
        };
        let out_ptr = if out.is_empty() {
            ptr::null_mut()
        } else {
            out.as_mut_ptr()
        };
        // SAFETY: text, out and the scratch are valid for the lengths passed and
        // do not overlap; the scratch is bound to this context and sized for
        // this text.
        let found = unsafe {
            sys::toks_encode(
                tokenizer.ctx.ptr(),
                text.as_ptr().cast::<c_void>(),
                text.len() as u64,
                flags,
                out_ptr,
                out.len() as u64,
                scratch.as_mut_ptr().cast(),
            )
        };
        if found < 0 {
            return Err(EncodeError::Toks { code: found });
        }
        Ok(found as u64)
    }
}

impl Drop for Scratch<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let mut pool = self
                .tokenizer
                .pool
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if pool.len() < POOL_KEEP {
                pool.push(buffer);
            }
        }
    }
}

/// The largest char boundary in `[lo, hi)` whose prefix fits `budget`, by
/// bisection between `lo` (known to fit, at `lo_tokens`) and `hi` (known not
/// to), then a few probes past the first over-budget offset.
fn bisect(
    text: &str,
    lo: usize,
    lo_tokens: u64,
    hi: usize,
    budget: u64,
    mut count_at: impl FnMut(usize) -> Result<u64, EncodeError>,
) -> Result<Cut, EncodeError> {
    let (mut good, mut good_tokens, mut bad) = (lo, lo_tokens, hi);
    while let Some(mid) = boundary_between(text, good, bad) {
        let tokens = count_at(mid)?;
        if tokens <= budget {
            good = mid;
            good_tokens = tokens;
        } else {
            bad = mid;
        }
    }
    let mut probe = bad;
    for _ in 0..FORWARD_PROBES {
        match next_boundary(text, probe) {
            Some(next) if next < hi => probe = next,
            _ => break,
        }
        let tokens = count_at(probe)?;
        if tokens <= budget {
            good = probe;
            good_tokens = tokens;
        }
    }
    Ok(Cut {
        byte_offset: good,
        tokens: saturate(good_tokens),
    })
}

/// A char boundary strictly between `lo` and `hi`, near their midpoint.
fn boundary_between(text: &str, lo: usize, hi: usize) -> Option<usize> {
    let mid = lo + (hi - lo) / 2;
    let below = (lo + 1..=mid).rev().find(|&at| text.is_char_boundary(at));
    below.or_else(|| (mid + 1..hi).find(|&at| text.is_char_boundary(at)))
}

fn next_boundary(text: &str, at: usize) -> Option<usize> {
    (at + 1..=text.len()).find(|&next| text.is_char_boundary(next))
}

fn check_len(text: &str) -> Result<(), EncodeError> {
    if text.len() as u64 > sys::TOKS_MAX_TEXT {
        return Err(EncodeError::TooLong { len: text.len() });
    }
    Ok(())
}

fn saturate(tokens: u64) -> u32 {
    u32::try_from(tokens).unwrap_or(u32::MAX)
}
