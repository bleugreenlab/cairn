//! The tokenizer interface that read budgets, transcript counts and run
//! accounting measure text through. Implementations live with their
//! tokenizers (`cairn-session::token_meters`); this crate only names the
//! contract, so the renderer and storage layers can take any meter.

/// A prefix of a text that fits a token budget, as [`TokenMeter::cut`]
/// returns it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cut {
    pub byte_offset: usize,
    pub tokens: u32,
}

/// Counts text in one tokenizer's tokens.
///
/// # Contract
///
/// `count(text)` is the number of tokens the model sees for `text` as
/// content: no BOS, EOS or chat-template ids.
///
/// `cut(text, budget)` returns a prefix `&text[..byte_offset]` where
/// `byte_offset` is a char boundary, `tokens <= budget`, and
/// `tokens == count(&text[..byte_offset])`. It is the longest such prefix on
/// a best-effort basis: a BPE prefix count is not monotone in prefix length,
/// so an implementation may settle a few characters short.
///
/// One meter departs from the equality: the Claude estimator's `count` adds a
/// fixed per-message framing overhead (6 tokens for its current family) that
/// its `cut` deliberately does not charge, so for it
/// `cut.tokens == count(prefix) - overhead`.
pub trait TokenMeter: Send + Sync {
    fn count(&self, text: &str) -> u32;
    fn cut(&self, text: &str, budget: u32) -> Cut;
}

/// Assert [`TokenMeter`]'s contract for one text and budget, including the
/// equality the Claude estimator is exempt from. For tests of exact meters.
#[cfg(any(test, feature = "test-utils"))]
pub fn assert_meter_contract(meter: &dyn TokenMeter, text: &str, budget: u32) {
    let cut = meter.cut(text, budget);
    assert!(
        text.is_char_boundary(cut.byte_offset),
        "cut at {} is not a char boundary of {text:?}",
        cut.byte_offset
    );
    assert!(
        cut.tokens <= budget,
        "cut reports {} tokens, over the budget of {budget}, for {text:?}",
        cut.tokens
    );
    assert_eq!(
        cut.tokens,
        meter.count(&text[..cut.byte_offset]),
        "cut's tokens disagree with count of its prefix (budget {budget}, text {text:?})"
    );
    if meter.count(text) <= budget {
        assert_eq!(
            cut.byte_offset,
            text.len(),
            "a text within budget is cut whole"
        );
    }
}
