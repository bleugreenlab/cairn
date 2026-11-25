//! Pure admission over the stored queue order, independent of execution state.

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
    pub execution_id: String,
    pub thread_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueCaps {
    pub global: usize,
    pub per_thread: usize,
}

/// Positions are one-based. Global eligible position counts only entries within
/// their thread cap (or without a thread), even beyond the global cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueDecision {
    pub position: usize,
    pub thread_position: Option<usize>,
    pub global_eligible_position: Option<usize>,
    pub admitted: bool,
}

/// Take the first per-thread entries, then the first global entries of that
/// eligible subsequence. Every member consumes its position regardless of
/// execution state; only changing membership or order changes the result.
/// Zero caps honestly admit no entries in the corresponding scope.
///
/// Returns one decision per input entry in the same order, in expected linear
/// time and space bounded by the number of entries and distinct threads.
pub fn selection(entries: &[QueueEntry], caps: QueueCaps) -> Vec<QueueDecision> {
    let mut thread_positions = HashMap::<&str, usize>::new();
    let mut global_position = 0;
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let thread_position = entry.thread_id.as_deref().map(|thread| {
                let position = thread_positions.entry(thread).or_default();
                *position += 1;
                *position
            });
            let thread_eligible = thread_position.is_none_or(|p| p <= caps.per_thread);
            let global_eligible_position = thread_eligible.then(|| {
                global_position += 1;
                global_position
            });
            QueueDecision {
                position: index + 1,
                thread_position,
                global_eligible_position,
                admitted: global_eligible_position.is_some_and(|p| p <= caps.global),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(spec: &[(&str, Option<&str>)]) -> Vec<QueueEntry> {
        spec.iter()
            .map(|(id, thread)| QueueEntry {
                execution_id: (*id).into(),
                thread_id: thread.map(str::to_owned),
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
    fn thread_filter_precedes_global_cap() {
        let queue = entries(&[
            ("A1", Some("A")),
            ("A2", Some("A")),
            ("B1", Some("B")),
            ("C1", Some("C")),
        ]);
        assert_eq!(admitted(&queue, CAPS), ["A1", "B1"]);
        let decisions = selection(&queue, CAPS);
        assert_eq!(
            decisions,
            vec![
                QueueDecision {
                    position: 1,
                    thread_position: Some(1),
                    global_eligible_position: Some(1),
                    admitted: true,
                },
                QueueDecision {
                    position: 2,
                    thread_position: Some(2),
                    global_eligible_position: None,
                    admitted: false,
                },
                QueueDecision {
                    position: 3,
                    thread_position: Some(1),
                    global_eligible_position: Some(2),
                    admitted: true,
                },
                QueueDecision {
                    position: 4,
                    thread_position: Some(1),
                    global_eligible_position: Some(3),
                    admitted: false,
                },
            ]
        );
    }

    #[test]
    fn unthreaded_entries_use_only_global_capacity() {
        let queue = entries(&[("U1", None), ("U2", None), ("A1", Some("A"))]);
        let decisions = selection(&queue, CAPS);
        assert_eq!(admitted(&queue, CAPS), ["U1", "U2"]);
        assert_eq!(decisions[0].thread_position, None);
        assert_eq!(decisions[1].thread_position, None);
        assert_eq!(decisions[2].global_eligible_position, Some(3));
    }

    #[test]
    fn membership_has_no_state_exemptions() {
        let queue = entries(&[
            ("waiting", Some("A")),
            ("completed", Some("A")),
            ("provider-limited", Some("B")),
            ("running", Some("C")),
        ]);
        assert_eq!(admitted(&queue, CAPS), ["waiting", "provider-limited"]);
        let decisions = selection(&queue, CAPS);
        assert_eq!(decisions[1].thread_position, Some(2));
        assert_eq!(decisions[3].global_eligible_position, Some(3));
    }

    #[test]
    fn reorder_displaces_and_raises_entries() {
        let queue = entries(&[("A1", Some("A")), ("A2", Some("A")), ("B1", Some("B"))]);
        assert_eq!(admitted(&queue, CAPS), ["A1", "B1"]);
        let reordered = vec![queue[1].clone(), queue[2].clone(), queue[0].clone()];
        assert_eq!(admitted(&reordered, CAPS), ["A2", "B1"]);
        let global_reorder = entries(&[("C1", Some("C")), ("A2", Some("A")), ("B1", Some("B"))]);
        assert_eq!(admitted(&global_reorder, CAPS), ["C1", "A2"]);
    }

    #[test]
    fn park_removes_and_unpark_appends() {
        let queue = entries(&[("A1", Some("A")), ("A2", Some("A")), ("B1", Some("B"))]);
        let mut parked = queue[1..].to_vec();
        assert_eq!(admitted(&parked, CAPS), ["A2", "B1"]);
        parked.push(queue[0].clone());
        assert_eq!(admitted(&parked, CAPS), ["A2", "B1"]);
        assert_eq!(selection(&parked, CAPS)[2].thread_position, Some(2));
    }

    #[test]
    fn repeated_stored_order_has_identical_decisions() {
        let stored = entries(&[("A1", Some("A")), ("A2", Some("A")), ("U1", None)]);
        assert_eq!(selection(&stored, CAPS), selection(&stored.clone(), CAPS));
        assert!(selection(&[], CAPS).is_empty());
    }

    #[test]
    fn zero_caps_are_not_silently_raised() {
        let queue = entries(&[("A1", Some("A")), ("U1", None)]);
        assert!(admitted(
            &queue,
            QueueCaps {
                global: 0,
                per_thread: 1
            }
        )
        .is_empty());
        assert_eq!(
            admitted(
                &queue,
                QueueCaps {
                    global: 2,
                    per_thread: 0
                }
            ),
            ["U1"]
        );
    }
}
