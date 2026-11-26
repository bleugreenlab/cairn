//! Pure admission over the stored queue order and what each entry is doing.

use std::collections::HashMap;

/// What an entry is doing, as the runner's stored facts say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Activity {
    /// Agent work is in flight: a turn running or due, or a wait on something
    /// Cairn is doing now. It holds its seat however far down the list it is.
    Active,
    /// Work is due but not started: it takes a free seat by its place.
    Wanting,
    /// Nothing it waits on is moving. It keeps its place and holds no seat.
    Dormant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
    pub execution_id: String,
    pub thread_id: Option<String>,
    pub activity: Activity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueCaps {
    pub global: usize,
    pub per_thread: usize,
}

/// Positions are one-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueDecision {
    pub position: usize,
    /// Holds a seat: active, or wanting and given a free one.
    pub admitted: bool,
    /// For a wanting entry given no seat, its place among those waiting for
    /// one (one-based).
    pub in_line: Option<usize>,
}

/// Seat the list. Every active entry holds a seat and counts against both
/// caps, even past them: running work is never cut to make room. The seats
/// left, globally and in each thread, go to wanting entries in list order. A
/// dormant entry takes none. Zero caps honestly seat no wanting entry.
///
/// Returns one decision per input entry in the same order, in expected linear
/// time and space bounded by the number of entries and distinct threads.
pub fn selection(entries: &[QueueEntry], caps: QueueCaps) -> Vec<QueueDecision> {
    let mut per_thread = HashMap::<&str, usize>::new();
    let mut global = 0;
    for entry in entries
        .iter()
        .filter(|entry| entry.activity == Activity::Active)
    {
        global += 1;
        if let Some(thread) = entry.thread_id.as_deref() {
            *per_thread.entry(thread).or_default() += 1;
        }
    }
    let mut in_line = 0;
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let (admitted, line) = match entry.activity {
                Activity::Active => (true, None),
                Activity::Dormant => (false, None),
                Activity::Wanting => {
                    let thread_room = entry.thread_id.as_deref().is_none_or(|thread| {
                        per_thread.get(thread).copied().unwrap_or(0) < caps.per_thread
                    });
                    if thread_room && global < caps.global {
                        global += 1;
                        if let Some(thread) = entry.thread_id.as_deref() {
                            *per_thread.entry(thread).or_default() += 1;
                        }
                        (true, None)
                    } else {
                        in_line += 1;
                        (false, Some(in_line))
                    }
                }
            };
            QueueDecision {
                position: index + 1,
                admitted,
                in_line: line,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use Activity::{Active, Dormant, Wanting};

    fn entries(spec: &[(&str, Option<&str>, Activity)]) -> Vec<QueueEntry> {
        spec.iter()
            .map(|(id, thread, activity)| QueueEntry {
                execution_id: (*id).into(),
                thread_id: thread.map(str::to_owned),
                activity: *activity,
            })
            .collect()
    }

    fn admitted(entries: &[QueueEntry], caps: QueueCaps) -> Vec<&str> {
        entries
            .iter()
            .zip(selection(entries, caps))
            .filter(|(_, decision)| decision.admitted)
            .map(|(entry, _)| entry.execution_id.as_str())
            .collect()
    }

    const CAPS: QueueCaps = QueueCaps {
        global: 2,
        per_thread: 1,
    };

    #[test]
    fn a_dormant_entry_holds_no_seat_and_keeps_its_place() {
        let queue = entries(&[
            ("old-review", None, Dormant),
            ("old-check", None, Dormant),
            ("a", Some("A"), Wanting),
            ("b", Some("B"), Wanting),
            ("c", Some("C"), Wanting),
        ]);
        assert_eq!(admitted(&queue, CAPS), ["a", "b"]);
        let decisions = selection(&queue, CAPS);
        assert_eq!(decisions[0].position, 1, "its place is kept");
        assert_eq!(decisions[4].in_line, Some(1));
    }

    #[test]
    fn an_active_entry_keeps_its_seat_and_is_never_cut_for_one_above_it() {
        // `waiting` was dormant and a person just acted on it: it wants a
        // seat, sits above both active entries, and still waits for a free one.
        let queue = entries(&[
            ("waiting", None, Wanting),
            ("on-its-check", None, Active),
            ("working", None, Active),
        ]);
        assert_eq!(admitted(&queue, CAPS), ["on-its-check", "working"]);
        assert_eq!(selection(&queue, CAPS)[0].in_line, Some(1));
        // Active work past a lowered cap keeps running; nothing new starts.
        let lowered = QueueCaps {
            global: 1,
            per_thread: 1,
        };
        assert_eq!(admitted(&queue, lowered), ["on-its-check", "working"]);
    }

    #[test]
    fn a_woken_entry_takes_the_next_free_seat_by_its_place() {
        let queue = entries(&[
            ("resumed", None, Wanting),
            ("running", None, Active),
            ("filed-later", None, Wanting),
        ]);
        assert_eq!(admitted(&queue, CAPS), ["resumed", "running"]);
        assert_eq!(selection(&queue, CAPS)[2].in_line, Some(1));
    }

    #[test]
    fn active_entries_fill_their_thread_before_a_wanting_one_is_seated() {
        let caps = QueueCaps {
            global: 3,
            per_thread: 1,
        };
        let queue = entries(&[
            ("a-wants", Some("A"), Wanting),
            ("a-runs", Some("A"), Active),
            ("b-wants", Some("B"), Wanting),
            ("u", None, Wanting),
        ]);
        assert_eq!(admitted(&queue, caps), ["a-runs", "b-wants", "u"]);
        assert_eq!(selection(&queue, caps)[0].in_line, Some(1));
    }

    #[test]
    fn the_same_list_and_activity_decide_the_same_seats() {
        let stored = entries(&[
            ("a", Some("A"), Wanting),
            ("b", Some("A"), Active),
            ("u", None, Dormant),
        ]);
        assert_eq!(selection(&stored, CAPS), selection(&stored.clone(), CAPS));
        assert!(selection(&[], CAPS).is_empty());
    }

    #[test]
    fn zero_caps_seat_no_wanting_entry() {
        let queue = entries(&[("a", Some("A"), Wanting), ("u", None, Wanting)]);
        let none = QueueCaps {
            global: 0,
            per_thread: 1,
        };
        assert!(admitted(&queue, none).is_empty());
        let no_thread_room = QueueCaps {
            global: 2,
            per_thread: 0,
        };
        assert_eq!(admitted(&queue, no_thread_room), ["u"]);
    }
}
