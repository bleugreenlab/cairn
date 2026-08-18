//! The completion rule every protocol family shares.
//!
//! Each family has its own streaming reader and its own terminal event, but the
//! judgement they make with it is one rule, so it lives once here rather than
//! three times beside three readers.

/// Refuse a stream that ended without the provider saying it finished.
///
/// A clean end of stream is not protocol completion. A connection severed
/// mid-generation closes exactly as cleanly as one that finished, so a reader
/// that trusts EOF would treat partial text as a whole turn and -- worse -- hand
/// the turn loop a tool call that merely happened to parse, carrying no stop
/// reason for the loop's truncation guard to judge it by. The arguments of a
/// severed call are a valid JSON *prefix* of the real ones, which is precisely
/// why nothing downstream can catch this.
///
/// `terminal_events` names what completion looks like in this family, so the
/// failure tells the reader which signal never arrived rather than that
/// something generic went wrong.
///
/// Cancellation is the one legitimate early ending: the user asked for it, the
/// caller already knows, and the run lands idle rather than failed. Read the
/// cancel flag freshly at the call site rather than from something the read loop
/// observed — a cancellation requested while the reader was blocked never
/// reaches the loop at all.
pub(crate) fn require_terminal_event(
    provider_name: &str,
    terminal_events: &str,
    saw_terminal: bool,
    cancelled: bool,
) -> Result<(), String> {
    if cancelled || saw_terminal {
        return Ok(());
    }
    Err(format!(
        "{provider_name} stream ended before the generation completed (no {terminal_events}). \
         The connection was closed mid-generation, so this turn's output is incomplete and is \
         not being recorded as a result."
    ))
}
