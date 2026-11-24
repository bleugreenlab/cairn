//! In-place judgment: places where Cairn decides something automatically
//! instead of asking a person or following a fixed rule. Each entry is owned by
//! the judgment thread, measured before it is built, and reached by its
//! consumers through a trait so the answerer behind it can be swapped without
//! changing what they receive.

pub mod token_forecast;
