//! The RFC 3986 unreserved-set percent codec, shared by path segments that
//! carry caller data (`encode_path_segment`) and by the `?key=value` query
//! grammar in `cairn-common`.

/// Escape every byte outside the RFC 3986 unreserved set.
pub fn encode_component(value: &str) -> String {
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

/// Exact inverse of [`encode_component`]. A malformed escape or a non-UTF-8
/// result is an error naming the value.
pub fn decode_component(value: &str) -> Result<String, String> {
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
