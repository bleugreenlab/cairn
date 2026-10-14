//! Code outline model, vendored from ast-grep-outline 0.43.0 (MIT).
//!
//! Only the serializable [`model`] is vendored: it is self-contained (serde +
//! std) and serves as the stable `?outline` output contract. Upstream's
//! rule-loading and index layers are intentionally not vendored — they depend on
//! an `ast-grep-config` API newer than the published 0.43.0 release, and Cairn
//! drives extraction from the per-language grammar tables in cairn-core's
//! `symbols` module rather than from outline rule YAML.

pub mod model;
