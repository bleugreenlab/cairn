//! SQL text builders shared by every crate that binds a variable-length list.

/// `?, ?, ?` for an `IN (...)` list of `n` bound values.
///
/// Bind a list this way rather than as one JSON array read back through
/// `IN (SELECT value FROM json_each(?))` or a join against `json_each(?)`.
/// Turso plans a bound list as an index SEARCH on the column, but treats a
/// `json_each` source as opaque and scans the outer table instead: on 5.5k
/// executions that was 5.4 s per issue lookup against 81 ms (cairn/5206).
pub fn placeholders(n: usize) -> String {
    (0..n).map(|_| "?").collect::<Vec<_>>().join(", ")
}

/// The most values one bound `IN` list carries. A caller with more splits its
/// input into chunks of this size, which keeps every statement well under the
/// engine's bind-variable limit however large a listing gets.
pub const IN_LIST_CHUNK: usize = 500;

#[cfg(test)]
mod tests {
    use super::placeholders;

    #[test]
    fn placeholders_join_one_marker_per_value() {
        assert_eq!(placeholders(0), "");
        assert_eq!(placeholders(1), "?");
        assert_eq!(placeholders(3), "?, ?, ?");
    }
}
