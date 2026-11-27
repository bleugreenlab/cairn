//! One way to report that Cairn did the nearest correct version of what an
//! agent asked for.
//!
//! Tenet 1463: a request is refused only when the caller lacks authority, the
//! target does not exist or the request is not one definite act, or carrying it
//! out would corrupt state no later act can repair. Every other mismatch is
//! carried out, and the reply names what was changed on the way in. Every verb
//! reports those changes through [`Adjustment`] so the wording is the same
//! wherever an agent meets it: one `Adjusted:` line per change, at the top of
//! the reply surface it reads (a read's target header, a write item's applied
//! summary, a run's result notes).

use std::fmt;

/// One change Cairn made to a request before carrying it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adjustment {
    /// Where in the request the change was made: `payload`, `commands[0]`,
    /// `?lines`.
    pub location: String,
    /// What the request said there, as the agent wrote it.
    pub received: String,
    /// What was applied instead.
    pub applied: String,
    /// Why the received form could not be taken as written, and, when the
    /// agent might want something else, how to get it. A fixed phrase per
    /// kind of adjustment, so the same change always reads the same way.
    pub reason: &'static str,
}

impl Adjustment {
    pub fn new(
        location: impl Into<String>,
        received: impl Into<String>,
        applied: impl Into<String>,
        reason: &'static str,
    ) -> Self {
        Self {
            location: location.into(),
            received: received.into(),
            applied: applied.into(),
            reason,
        }
    }
}

impl fmt::Display for Adjustment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Adjusted: `{}` {} \u{2192} {} ({}).",
            self.location, self.received, self.applied, self.reason
        )
    }
}

/// Every adjustment on its own line, in the order they were made; empty when
/// there were none.
pub fn render(adjustments: &[Adjustment]) -> String {
    adjustments
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

/// `summary` with the adjustments that produced it on the lines after it, or
/// `summary` unchanged when there were none.
pub fn with_summary(summary: String, adjustments: &[Adjustment]) -> String {
    if adjustments.is_empty() {
        summary
    } else {
        format!("{summary}\n{}", render(adjustments))
    }
}

/// The valid key the caller most plausibly meant: the same key in another case
/// convention (`commitMsg` for `commit_msg`), or one within two edits of a key
/// of at least four characters. A suggestion further away than that teaches
/// the wrong key, so none is made. On a tie the earlier candidate is named;
/// use [`unique_nearest`] when the answer is applied rather than suggested.
pub fn nearest<'a>(key: &str, valid: &[&'a str]) -> Option<&'a str> {
    ranked(key, valid).first().map(|(_, candidate)| *candidate)
}

/// [`nearest`], but only when exactly one candidate is closest: the form a key
/// may be mapped by, since a tie could mean either.
pub fn unique_nearest<'a>(key: &str, valid: &[&'a str]) -> Option<&'a str> {
    let ranked = ranked(key, valid);
    match ranked.as_slice() {
        [(best, candidate), rest @ ..] if rest.first().is_none_or(|(next, _)| next > best) => {
            Some(*candidate)
        }
        _ => None,
    }
}

/// Every close candidate with its distance, nearest first, stable on ties.
fn ranked<'a>(key: &str, valid: &[&'a str]) -> Vec<(usize, &'a str)> {
    let wanted = fold(key);
    let mut close: Vec<(usize, &'a str)> = valid
        .iter()
        .filter_map(|&candidate| {
            let distance = edit_distance(&wanted, &fold(candidate));
            (distance == 0 || (distance <= 2 && wanted.chars().count() >= 4))
                .then_some((distance, candidate))
        })
        .collect();
    close.sort_by_key(|(distance, _)| *distance);
    close
}

/// A key with case and `_`/`-` separators folded away, so `commitMsg`,
/// `commit_msg` and `COMMIT-MSG` compare equal.
pub fn fold(key: &str) -> String {
    key.chars()
        .filter(|c| *c != '_' && *c != '-')
        .flat_map(char::to_lowercase)
        .collect()
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    for (i, l) in left.chars().enumerate() {
        let mut current = vec![i + 1; right.len() + 1];
        for (j, r) in right.iter().enumerate() {
            let substitution = previous[j] + usize::from(l != *r);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        previous = current;
    }
    previous[right.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_adjustment_renders_as_one_line_naming_where_what_and_why() {
        let moved = Adjustment::new(
            "commands[0].executor",
            "inside an item",
            "beside `commands`",
            "one batch runs on one machine",
        );
        assert_eq!(
            moved.to_string(),
            "Adjusted: `commands[0].executor` inside an item \u{2192} beside `commands` (one batch runs on one machine)."
        );
        let second = Adjustment::new("payload", "`body`", "`note`", "r");
        assert_eq!(render(&[moved.clone(), second.clone()]).lines().count(), 2);
        assert_eq!(with_summary("Inserted x".into(), &[]), "Inserted x");
        assert_eq!(
            with_summary("Inserted x".into(), &[second]),
            "Inserted x\nAdjusted: `payload` `body` \u{2192} `note` (r)."
        );
    }

    #[test]
    fn nearest_folds_case_and_separators_and_stays_within_two_edits() {
        let keys = ["commit_msg", "stop_on_error", "waitFor"];
        assert_eq!(nearest("commitMsg", &keys), Some("commit_msg"));
        assert_eq!(nearest("stopOnError", &keys), Some("stop_on_error"));
        assert_eq!(nearest("wait_for", &keys), Some("waitFor"));
        assert_eq!(nearest("comit_msg", &keys), Some("commit_msg"));
        assert_eq!(nearest("cmd", &keys), None);
        assert_eq!(nearest("verbose", &keys), None);
    }

    #[test]
    fn unique_nearest_declines_a_tie() {
        assert_eq!(unique_nearest("note", &["notes", "nite"]), None);
        assert_eq!(unique_nearest("note", &["notes", "kind"]), Some("notes"));
        assert_eq!(unique_nearest("NOTE", &["notes", "note"]), Some("note"));
    }
}
