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

/// Every query key any read/change target accepts. The query grammar splits a
/// segment on `&` only when the token immediately following the `&` is one of
/// these — so a literal `&` inside a value (e.g. `grep=&mut self`) stays whole
/// while `&limit=5` still separates. Keep this exhaustive: a key omitted here
/// folds into the preceding value, which fails visibly (a bad pattern or an
/// "unsupported parameter" error) rather than silently. The escape for a literal
/// `&` that immediately precedes a recognized key token is `%26`.
pub const KNOWN_QUERY_KEYS: &[&str] = &[
    "grep",
    "glob",
    "type",
    "output_mode",
    "context",
    "-A",
    "-B",
    "-C",
    "-i",
    "-n",
    "head_limit",
    "offset",
    "limit",
    "multiline",
    "issue_history",
    "search",
    "path",
    "status",
    "sort",
    "ready",
    "before",
    "after",
    "since",
    "full",
    "label",
    "content_types",
    // Live-database (cairn://db) read-only SQL projection key.
    "sql",
    // Dev-instance selector (cairn://dev/db?at=..., cairn://dev/pid?at=...).
    "at",
    // Symbol-resource keys (op/in) + structural file-projection keys (ast/outline).
    "op",
    "in",
    "ast",
    "outline",
];

/// Return true when the start of `rest` (the text immediately after a `&`) is a
/// recognized query key — i.e. the chars up to the next `=`, `&`, or end form a
/// member of `KNOWN_QUERY_KEYS`. This is the test that decides whether a `&`
/// separates params or is literal content within a value.
fn next_token_is_known_key(rest: &str) -> bool {
    let token = rest.split(['=', '&']).next().unwrap_or("");
    KNOWN_QUERY_KEYS.contains(&token)
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
        if bytes[index] == b'&' && next_token_is_known_key(&query[index + 1..]) {
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
            Ok(QueryParam {
                key: decode_query_component(raw_key)?,
                value: decode_query_component(raw_value)?,
            })
        })
        .collect()
}

pub fn encode_query_params(params: &[QueryParam]) -> String {
    params
        .iter()
        .map(|param| {
            format!(
                "{}={}",
                encode_query_component(&param.key),
                encode_query_component(&param.value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn encode_query_component(value: &str) -> String {
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

fn decode_query_component(value: &str) -> Result<String, String> {
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
        // if either is missing from KNOWN_QUERY_KEYS, the `&` before it would be
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
    fn split_target_query_rejects_invalid_percent_encoding() {
        let err = split_target_query("src?grep=%ZZ").unwrap_err();
        assert!(err.contains("Invalid percent escape"));
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
