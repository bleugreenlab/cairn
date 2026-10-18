//! The slice of toks' C ABI (`include/toks.h`, ABI 0.4) this crate calls.
//!
//! Hand-written rather than generated: the surface is ten functions and three
//! structs, the header is pinned with the library, and the layout tests below
//! hold the mirror to the sizes clang reports for the 0.3.2 header.

use std::ffi::{c_char, c_void};
use std::marker::{PhantomData, PhantomPinned};

pub const ABI_MAJOR: u32 = 0;
pub const ABI_MINOR: u32 = 4;
pub const VERSION: &str = "0.3.2";

pub const TOKS_TIER_AUTO: u32 = 0;

pub const TOKS_NO_POSTPROCESS: u32 = 4;
pub const TOKS_CONTINUATION: u32 = 8;
pub const TOKS_NO_TRUNCATE: u32 = 16;
pub const TOKS_NO_PAD: u32 = 32;

/// Bytes per encode call (SPEC §8.3).
pub const TOKS_MAX_TEXT: u64 = 1 << 29;

#[repr(C)]
pub struct toks_ctx {
    _opaque: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

#[repr(C)]
pub struct toks_diag {
    pub code: i64,
    pub what: [c_char; 248],
}

#[repr(C)]
pub struct toks_load_opts {
    pub size: u32,
    pub tier: u32,
    pub flags: u32,
    pub rsv: u32,
    pub diag: *mut toks_diag,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct toks_info {
    pub size: u32,
    pub abi_major: u32,
    pub abi_minor: u32,
    pub algorithm: u32,
    pub tier: u32,
    pub n_ids: u32,
    pub n_added: u32,
    pub paths: u32,
    pub cpu_features: u64,
    pub max_text: u64,
    pub control_isolation: u32,
    pub rsv: u32,
    pub source_sha256: [u8; 32],
    pub image_sha256: [u8; 32],
    pub name: [c_char; 64],
    pub trunc_on: u32,
    pub trunc_max: u32,
    pub trunc_stride: u32,
    pub pad_on: u32,
    pub pad_fixed: u32,
    pub pad_id: u32,
    pub pad_type_id: u32,
    pub pad_len: u32,
    pub pad_multiple: u32,
    pub pad_left: u32,
    pub n_template_prefix: u32,
    pub n_template_suffix: u32,
    pub seq_type_id: u32,
    pub rsv2: u32,
}

extern "C" {
    pub fn toks_load(
        out: *mut *mut toks_ctx,
        path: *const c_char,
        opts: *const toks_load_opts,
    ) -> i64;
    pub fn toks_load_mem_copy(
        out: *mut *mut toks_ctx,
        data: *const c_void,
        len: u64,
        opts: *const toks_load_opts,
    ) -> i64;
    pub fn toks_unload(ctx: *mut toks_ctx);
    pub fn toks_scratch_bytes(ctx: *const toks_ctx, max_len: u64, flags: u32) -> u64;
    pub fn toks_scratch_init(ctx: *const toks_ctx, scr: *mut c_void, bytes: u64, flags: u32)
        -> i64;
    pub fn toks_encode(
        ctx: *const toks_ctx,
        text: *const c_void,
        len: u64,
        flags: u32,
        out: *mut u32,
        cap: u64,
        scr: *mut c_void,
    ) -> i64;
    pub fn toks_split_points(
        ctx: *const toks_ctx,
        text: *const c_void,
        len: u64,
        flags: u32,
        n_want: u32,
        offs: *mut u64,
        cap: u64,
        scr: *mut c_void,
    ) -> i64;
    pub fn toks_token(ctx: *const toks_ctx, id: u32, len: *mut u64) -> *const u8;
    pub fn toks_added(
        ctx: *const toks_ctx,
        i: u32,
        content: *mut *const c_void,
        len: *mut u64,
        id: *mut u32,
    ) -> i64;
    pub fn toks_get_info(ctx: *const toks_ctx, out: *mut toks_info) -> i64;
    pub fn toks_version() -> *const c_char;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{offset_of, size_of};

    /// The sizes and offsets clang reports for toks 0.3.2's header on LP64.
    #[test]
    fn struct_layout_matches_the_pinned_header() {
        assert_eq!(size_of::<toks_diag>(), 256);
        assert_eq!(size_of::<toks_load_opts>(), 24);
        assert_eq!(size_of::<toks_info>(), 240);
        assert_eq!(offset_of!(toks_info, source_sha256), 56);
        assert_eq!(offset_of!(toks_info, name), 120);
        assert_eq!(offset_of!(toks_info, trunc_on), 184);
    }

    #[test]
    fn linked_library_is_the_pinned_release() {
        let version = unsafe { std::ffi::CStr::from_ptr(toks_version()) };
        assert_eq!(version.to_str().unwrap(), VERSION);
    }
}
