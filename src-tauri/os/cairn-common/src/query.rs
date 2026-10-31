//! The `?key=value&...` grammar shared by every read and write target.
//!
//! A `&` separates params only when the token after it is a query key, so a
//! literal `&` inside a value (`grep=&mut self`) stays whole. The split set is
//! derived: every key a resource contract declares in `read_projections`, plus
//! [`STATIC_QUERY_KEYS`] for the filesystem and universal read-view modifiers no
//! contract owns. Adding a resource query key therefore means declaring it on
//! that resource's contract; nothing here changes.
//!
//! The set is global, so a value containing `&` followed by any key's exact
//! name (e.g. `grep=a&state`) splits there. Every target rejects keys it does
//! not accept by name, so such a collision fails loudly as an unsupported
//! parameter instead of silently corrupting the preceding value. The escape for
//! a literal `&` immediately before a key token is `%26`.

use crate::contract::RESOURCE_CONTRACTS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParam {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitTargetQuery {
    pub identity: String,
    pub raw_query: Option<String>,
    pub params: Vec<QueryParam>,
}

pub fn split_target_query(target: &str) -> Result<SplitTargetQuery, String> {
    let (identity, raw_query) = match target.split_once('?') {
        Some((identity, query)) => (identity, Some(query)),
        None => (target, None),
    };

    let params = match raw_query {
        Some(query) => parse_query_params(query)?,
        None => Vec::new(),
    };

    Ok(SplitTargetQuery {
        identity: identity.to_string(),
        raw_query: raw_query.map(|query| query.to_string()),
        params,
    })
}

/// Query keys no resource contract can own: the filesystem-target projections
/// and the universal read-view modifiers every target shares. Every other split
/// key comes from a contract's `read_projections` (see [`is_query_key`]), so a
/// resource's new query key is declared there, never here.
/// `static_keys_are_a_true_remainder` keeps this list from regrowing into a
/// mirror of the contracts.
const STATIC_QUERY_KEYS: &[&str] = &[
    // Universal body grep (file targets, and any rendered target via the batch
    // view layer) and its ripgrep-style modifiers.
    "grep",
    "type",
    "context",
    "-A",
    "-B",
    "-C",
    "-i",
    "-n",
    "head_limit",
    "multiline",
    // Shared read-view continuation for a single line larger than the batch
    // budget, consumed together with `offset` to resume within a line.
    "char_offset",
    // File targets: issues that touched the file, a read at another revision,
    // and the structural projections.
    "issue_history",
    "branch",
    "ast",
    "outline",
    // Blocking long-poll on a call/task-artifact URI: the workflow harness
    // `agent()` await. A machine protocol whose body the harness parses, so the
    // task-artifact contract deliberately does not advertise it to agents.
    "wait",
    // Accepted and ignored by terminal reads (`full`) and by terminal and file
    // reads (`annotations`); kept splitting so old URIs still parse, but not
    // advertised because neither changes what a read returns.
    "full",
    "annotations",
    // Retired project-issues filter: the reader refuses it with a message
    // naming its replacement, which only reaches the caller if it splits.
    "ready",
];

/// Keys whose value is a free-text payload taken verbatim: the value is NOT
/// percent-decoded, so a literal `%` — a SQL `LIKE '%x%'` wildcard, a
/// `%`-carrying search query — rides through as-is instead of tripping the
/// percent-escape validation (`Invalid percent escape in query component`).
/// Segment splitting on `&`-before-a-query-key still applies to these values, so
/// `?sql=...&limit=N` continues to separate in either order; only the decode
/// step is skipped. These keys are unique to `cairn://db` / `cairn://dev/db`
/// (`sql`) and `cairn://websearch` (`q`), so treating them as raw wherever they
/// appear is unambiguous.
const RAW_VALUE_KEYS: &[&str] = &["sql", "q"];

/// Whether `token` is a query key the grammar splits on: a key some resource
/// contract declares in its `read_projections`, or one of
/// [`STATIC_QUERY_KEYS`]. The set is global rather than scoped to the target's
/// kind, so a `cairn:~/` or `cairn:@/` target parses before it resolves.
fn is_query_key(token: &str) -> bool {
    STATIC_QUERY_KEYS.contains(&token)
        || RESOURCE_CONTRACTS
            .iter()
            .flat_map(|contract| contract.read_projections)
            .any(|projection| projection.key == token)
}

/// Return true when the start of `rest` (the text immediately after a `&`) is a
/// query key: the chars up to the next `=`, `&`, or end of string are exactly a
/// key. This decides whether a `&` separates params or is literal value
/// content, so `&mut self`, `&new(` and `&state.x)` stay inside a grep pattern
/// while `&limit=5`, `&-i&` and a trailing `&full` split.
fn next_token_is_query_key(rest: &str) -> bool {
    let token = rest.split(['=', '&']).next().unwrap_or("");
    is_query_key(token)
}

pub fn parse_query_params(query: &str) -> Result<Vec<QueryParam>, String> {
    if query.is_empty() {
        return Ok(Vec::new());
    }

    // Split into segments left-to-right. A `&` is a separator only when the
    // token that follows it is a recognized key; otherwise the `&` is literal
    // value content and stays in the current segment.
    let mut segments: Vec<&str> = Vec::new();
    let mut segment_start = 0;
    let bytes = query.as_bytes();
    for index in 0..bytes.len() {
        if bytes[index] == b'&' && next_token_is_query_key(&query[index + 1..]) {
            segments.push(&query[segment_start..index]);
            segment_start = index + 1;
        }
    }
    segments.push(&query[segment_start..]);

    segments
        .into_iter()
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_query_component(raw_key)?;
            // Free-text payloads (sql/q) are taken verbatim so a literal `%`
            // survives; every other value is percent-decoded as usual.
            let value = if RAW_VALUE_KEYS.contains(&key.as_str()) {
                raw_value.to_string()
            } else {
                decode_query_component(raw_value)?
            };
            Ok(QueryParam { key, value })
        })
        .collect()
}

pub fn encode_query_params(params: &[QueryParam]) -> String {
    params
        .iter()
        .map(|param| {
            let value = if RAW_VALUE_KEYS.contains(&param.key.as_str()) {
                param.value.clone()
            } else {
                encode_query_component(&param.value)
            };
            format!("{}={value}", encode_query_component(&param.key))
        })
        .collect::<Vec<_>>()
        .join("&")
}

pub(crate) fn encode_query_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

pub(crate) fn decode_query_component(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err(format!(
                        "Invalid percent escape in query component: {value}"
                    ));
                }
                let hi = decode_hex(bytes[index + 1])
                    .ok_or_else(|| format!("Invalid percent escape in query component: {value}"))?;
                let lo = decode_hex(bytes[index + 2])
                    .ok_or_else(|| format!("Invalid percent escape in query component: {value}"))?;
                decoded.push((hi << 4) | lo);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|_| format!("Query component is not valid UTF-8: {value}"))
}

fn decode_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_target_query_without_query() {
        let split = split_target_query("src/lib.rs").unwrap();
        assert_eq!(split.identity, "src/lib.rs");
        assert_eq!(split.raw_query, None);
        assert!(split.params.is_empty());
    }

    #[test]
    fn split_target_query_decodes_params() {
        // `+` is literal now (not form-decoded to a space); a literal space rides
        // through unchanged. `&limit=` still separates because `limit` is a known key.
        let split = split_target_query("cairn://p/CAIRN?search=memory leak&limit=5").unwrap();
        assert_eq!(split.identity, "cairn://p/CAIRN");
        assert_eq!(
            split.raw_query.as_deref(),
            Some("search=memory leak&limit=5")
        );
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "search".to_string(),
                    value: "memory leak".to_string(),
                },
                QueryParam {
                    key: "limit".to_string(),
                    value: "5".to_string(),
                },
            ]
        );
    }

    #[test]
    fn known_key_splits_when_not_leading() {
        // A recognized key that appears after the first param must still split.
        // Regression guard: `label` and `content_types` are live resource keys;
        // if either stopped being a split key, the `&` before it would be
        // treated as literal content and corrupt the preceding value.
        let split = split_target_query("cairn://p/CAIRN/issues?status=active&label=bug").unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "status".to_string(),
                    value: "active".to_string(),
                },
                QueryParam {
                    key: "label".to_string(),
                    value: "bug".to_string(),
                },
            ]
        );

        let split =
            split_target_query("cairn://p/CAIRN?search=auth&content_types=issue&limit=10").unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "search".to_string(),
                    value: "auth".to_string(),
                },
                QueryParam {
                    key: "content_types".to_string(),
                    value: "issue".to_string(),
                },
                QueryParam {
                    key: "limit".to_string(),
                    value: "10".to_string(),
                },
            ]
        );
    }

    #[test]
    fn an_event_explain_key_splits_in_either_order() {
        let expected = |first: (&str, &str), second: (&str, &str)| {
            vec![
                QueryParam {
                    key: first.0.to_string(),
                    value: first.1.to_string(),
                },
                QueryParam {
                    key: second.0.to_string(),
                    value: second.1.to_string(),
                },
            ]
        };
        let thread = "cairn://p/cairn/ui";
        assert_eq!(
            split_target_query("cairn://p/cairn/events/12?grep=scope&explain=cairn://p/cairn/ui")
                .unwrap()
                .params,
            expected(("grep", "scope"), ("explain", thread))
        );
        assert_eq!(
            split_target_query("cairn://p/cairn/events/12?explain=cairn://p/cairn/ui&grep=scope")
                .unwrap()
                .params,
            expected(("explain", thread), ("grep", "scope"))
        );
    }

    #[test]
    fn literal_ampersand_in_value_stays_whole() {
        // `&mut` and `&-C=` differ: `mut` is not a known key (literal), `-C` is.
        let split = split_target_query("file:lib.rs?grep=&mut&-C=2").unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "grep".to_string(),
                    value: "&mut".to_string(),
                },
                QueryParam {
                    key: "-C".to_string(),
                    value: "2".to_string(),
                },
            ]
        );
    }

    #[test]
    fn bare_known_key_splits_as_empty_value() {
        let split = split_target_query("file:lib.rs?grep=skill&-i&-C=3").unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "grep".to_string(),
                    value: "skill".to_string(),
                },
                QueryParam {
                    key: "-i".to_string(),
                    value: "".to_string(),
                },
                QueryParam {
                    key: "-C".to_string(),
                    value: "3".to_string(),
                },
            ]
        );
    }

    #[test]
    fn double_ampersand_and_amp_str_are_literal() {
        // Neither an empty token (`&&`) nor `str` is a known key, so both `&`s
        // stay inside the grep value.
        let split = split_target_query("file:lib.rs?grep=a&&b&str").unwrap();
        assert_eq!(
            split.params,
            vec![QueryParam {
                key: "grep".to_string(),
                value: "a&&b&str".to_string(),
            }]
        );
    }

    #[test]
    fn literal_plus_is_preserved() {
        // A regex `\d+` must survive intact rather than becoming `\d ` (space).
        let split = split_target_query("file:lib.rs?grep=\\d+&output_mode=count").unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "grep".to_string(),
                    value: "\\d+".to_string(),
                },
                QueryParam {
                    key: "output_mode".to_string(),
                    value: "count".to_string(),
                },
            ]
        );
    }

    #[test]
    fn percent_escaped_ampersand_before_known_key_is_literal() {
        // `%26` is the escape for a literal `&` immediately preceding a key token.
        let split = split_target_query("file:lib.rs?grep=a%26limit=5").unwrap();
        assert_eq!(
            split.params,
            vec![QueryParam {
                key: "grep".to_string(),
                value: "a&limit=5".to_string(),
            }]
        );
    }

    #[test]
    fn encode_decode_round_trip_for_reserved_bytes() {
        // Values containing `&`, `+`, and a space must re-parse identically after
        // a canonical re-encode (every reserved byte percent-encodes).
        let params = vec![QueryParam {
            key: "grep".to_string(),
            value: "a & b + c d".to_string(),
        }];
        let encoded = encode_query_params(&params);
        let reparsed = parse_query_params(&encoded).unwrap();
        assert_eq!(reparsed, params);
    }

    #[test]
    fn repeated_where_keys_split_and_survive_in_order() {
        let split = split_target_query(
            "cairn:~/db/attempts?where=problem=two-sum&where=lang=rust&order=-created_at&limit=5",
        )
        .unwrap();
        let pairs: Vec<(&str, &str)> = split
            .params
            .iter()
            .map(|p| (p.key.as_str(), p.value.as_str()))
            .collect();
        assert_eq!(
            pairs,
            [
                ("where", "problem=two-sum"),
                ("where", "lang=rust"),
                ("order", "-created_at"),
                ("limit", "5"),
            ]
        );
    }

    #[test]
    fn split_target_query_rejects_invalid_percent_encoding() {
        let err = split_target_query("src?grep=%ZZ").unwrap_err();
        assert!(err.contains("Invalid percent escape"));
    }

    #[test]
    fn raw_value_keys_skip_percent_decoding() {
        // A SQL LIKE wildcard carries a literal `%` that is NOT a valid percent
        // escape. For `sql` (a raw-value key) it rides through verbatim rather
        // than failing with "Invalid percent escape", and `&limit=` still splits.
        let split =
            split_target_query("cairn://db?sql=SELECT * FROM issues WHERE t LIKE '%bug%'&limit=5")
                .unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "sql".to_string(),
                    value: "SELECT * FROM issues WHERE t LIKE '%bug%'".to_string(),
                },
                QueryParam {
                    key: "limit".to_string(),
                    value: "5".to_string(),
                },
            ]
        );

        let split = split_target_query("cairn://db?sql=SELECT 1&format=json").unwrap();
        assert_eq!(
            split.params,
            vec![
                QueryParam {
                    key: "sql".to_string(),
                    value: "SELECT 1".to_string(),
                },
                QueryParam {
                    key: "format".to_string(),
                    value: "json".to_string(),
                },
            ]
        );

        // Web search `q` is a raw-value key too: `100%` no longer breaks parsing.
        let split = split_target_query("cairn://websearch?q=100% uptime claims").unwrap();
        assert_eq!(
            split.params,
            vec![QueryParam {
                key: "q".to_string(),
                value: "100% uptime claims".to_string(),
            }]
        );
    }

    #[test]
    fn non_raw_value_keys_still_reject_bad_percent_escapes() {
        // Only sql/q are raw; a stray `%` in an ordinary value (grep) still fails
        // fast rather than corrupting the pattern.
        assert!(split_target_query("file:lib.rs?grep=100%25done")
            .unwrap()
            .params
            .iter()
            .any(|p| p.key == "grep" && p.value == "100%done"));
        assert!(split_target_query("file:lib.rs?grep=100%done").is_err());
    }

    #[test]
    fn encode_query_params_preserves_raw_values_for_parser_symmetry() {
        let params = vec![
            QueryParam {
                key: "sql".to_string(),
                value: "SELECT * FROM issues WHERE title LIKE '%bug%'".to_string(),
            },
            QueryParam {
                key: "limit".to_string(),
                value: "5".to_string(),
            },
        ];
        let encoded = encode_query_params(&params);
        assert_eq!(
            encoded,
            "sql=SELECT * FROM issues WHERE title LIKE '%bug%'&limit=5"
        );
        assert_eq!(parse_query_params(&encoded).unwrap(), params);

        let websearch = vec![QueryParam {
            key: "q".to_string(),
            value: "100% uptime claims".to_string(),
        }];
        let encoded = encode_query_params(&websearch);
        assert_eq!(encoded, "q=100% uptime claims");
        assert_eq!(parse_query_params(&encoded).unwrap(), websearch);
    }

    fn pairs(target: &str) -> Vec<(String, String)> {
        split_target_query(target)
            .unwrap_or_else(|error| panic!("{target}: {error}"))
            .params
            .into_iter()
            .map(|param| (param.key, param.value))
            .collect()
    }

    fn owned(expected: &[(&str, &str)]) -> Vec<(String, String)> {
        expected
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    /// A concrete target for a contract's URI template: every `{placeholder}`
    /// becomes `x`, so the grammar sees the resource's real path shape.
    fn instantiate(template: &str) -> String {
        let mut out = String::new();
        let mut in_placeholder = false;
        for c in template.split('?').next().unwrap_or(template).chars() {
            match c {
                '{' => in_placeholder = true,
                '}' => {
                    in_placeholder = false;
                    out.push('x');
                }
                _ if !in_placeholder => out.push(c),
                _ => {}
            }
        }
        out
    }

    fn neighbour(key: &str) -> &'static str {
        if key == "limit" {
            "offset"
        } else {
            "limit"
        }
    }

    #[test]
    fn every_contract_projection_key_splits_when_not_leading() {
        for contract in RESOURCE_CONTRACTS {
            let base = instantiate(contract.uri_template);
            for projection in contract.read_projections {
                let key = projection.key;
                let other = neighbour(key);
                assert_eq!(
                    pairs(&format!("{base}?{other}=a&{key}=b")),
                    owned(&[(other, "a"), (key, "b")]),
                    "{:?} declares `{key}`, which must split after another param",
                    contract.kind
                );
            }
        }
    }

    #[test]
    fn every_static_key_splits_when_not_leading() {
        for key in STATIC_QUERY_KEYS {
            let other = neighbour(key);
            assert_eq!(
                pairs(&format!("file:src/lib.rs?{other}=a&{key}=b")),
                owned(&[(other, "a"), (key, "b")]),
                "static key `{key}` must split after another param"
            );
        }
    }

    #[test]
    fn static_keys_are_a_true_remainder() {
        for key in STATIC_QUERY_KEYS {
            let owner = RESOURCE_CONTRACTS.iter().find(|contract| {
                contract
                    .read_projections
                    .iter()
                    .any(|projection| projection.key == *key)
            });
            assert!(
                owner.is_none(),
                "`{key}` is declared by the {:?} contract; drop it from STATIC_QUERY_KEYS",
                owner.map(|contract| contract.kind)
            );
        }
    }

    #[test]
    fn split_keys_are_plain_tokens() {
        let plain = |key: &str| {
            !key.is_empty()
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        };
        let contract_keys = RESOURCE_CONTRACTS
            .iter()
            .flat_map(|contract| contract.read_projections)
            .map(|projection| projection.key);
        for key in STATIC_QUERY_KEYS.iter().copied().chain(contract_keys) {
            assert!(
                plain(key),
                "split key `{key}` must be [A-Za-z0-9_-]+ or it breaks the tokenizer"
            );
        }
        for key in RAW_VALUE_KEYS {
            assert!(is_query_key(key), "raw-value key `{key}` must also split");
        }
    }

    #[test]
    fn check_results_suite_and_environment_split_in_either_order() {
        let forward = owned(&[("suite", "rust-tests"), ("environment", "current")]);
        let swapped = owned(&[("environment", "current"), ("suite", "rust-tests")]);
        for base in [
            "cairn://p/cairn/check-results/main",
            "cairn:@/check-results/main",
        ] {
            assert_eq!(
                pairs(&format!("{base}?suite=rust-tests&environment=current")),
                forward
            );
            assert_eq!(
                pairs(&format!("{base}?environment=current&suite=rust-tests")),
                swapped
            );
        }
    }

    #[test]
    fn literal_ampersand_before_a_space_stays_in_the_value() {
        assert_eq!(
            pairs("file:lib.rs?grep=&mut self&limit=5"),
            owned(&[("grep", "&mut self"), ("limit", "5")])
        );
    }

    /// Several contract keys are everyday identifiers (`new`, `state`, `table`,
    /// `match`, `diff`, `process`). A token after `&` is a key only when the key
    /// is followed by `=`, `&`, or the end of the query, so code-shaped text
    /// that merely starts with one stays inside the grep value.
    #[test]
    fn code_shaped_ampersands_do_not_split() {
        for pattern in [
            "&new(",
            "Foo::new(&state.x)",
            "(&table)",
            "&match_arm",
            "&diff[0]",
            "&process_id",
        ] {
            assert_eq!(
                pairs(&format!("file:src/lib.rs?grep={pattern}")),
                owned(&[("grep", pattern)]),
                "`{pattern}` must stay one grep value"
            );
        }
    }

    /// The one ambiguous shape: a key name ending the query. It splits as a
    /// bare flag, which the file target then rejects by name (`%26full` keeps
    /// it literal).
    #[test]
    fn a_trailing_bare_key_splits_as_a_flag() {
        assert_eq!(
            pairs("file:src/lib.rs?grep=x&full"),
            owned(&[("grep", "x"), ("full", "")])
        );
        assert_eq!(
            pairs("file:src/lib.rs?grep=x%26full"),
            owned(&[("grep", "x&full")])
        );
    }

    #[test]
    fn encode_query_params_uses_canonical_escaping() {
        assert_eq!(
            encode_query_params(&[
                QueryParam {
                    key: "search".to_string(),
                    value: "memory leak".to_string(),
                },
                QueryParam {
                    key: "label".to_string(),
                    value: "needs/review".to_string(),
                },
            ]),
            "search=memory%20leak&label=needs%2Freview"
        );
    }
}
