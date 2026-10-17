//! The blocking transport every image wire shares: headers, one request, and
//! the status-to-error mapping.

use std::time::Duration;

use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::de::DeserializeOwned;

use crate::{ImageWireError, ImagesEndpoint};

pub(crate) fn client(timeout: Duration) -> Result<Client, ImageWireError> {
    Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| ImageWireError::Transport(error.to_string()))
}

pub(crate) fn header_map(endpoint: &ImagesEndpoint) -> Result<HeaderMap, ImageWireError> {
    let mut map = HeaderMap::new();
    for (name, value) in &endpoint.headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            ImageWireError::InvalidRequest(format!(
                "invalid {} header name: {error}",
                endpoint.provider_name
            ))
        })?;
        if name == CONTENT_TYPE {
            continue;
        }
        let value = HeaderValue::from_str(value).map_err(|error| {
            ImageWireError::InvalidRequest(format!(
                "invalid {} header value: {error}",
                endpoint.provider_name
            ))
        })?;
        map.insert(name, value);
    }
    Ok(map)
}

/// Send `request` and return the success body, or the error it answered.
pub(crate) fn send(request: RequestBuilder, provider: &str) -> Result<String, ImageWireError> {
    let response = request.send().map_err(|error| {
        if error.is_timeout() {
            ImageWireError::Timeout
        } else {
            ImageWireError::Transport(format!("{provider} image request failed: {error}"))
        }
    })?;
    let status = response.status();
    let text = response.text().map_err(|error| {
        if error.is_timeout() {
            ImageWireError::Timeout
        } else {
            ImageWireError::InvalidResponse(error.to_string())
        }
    })?;
    if !status.is_success() {
        return Err(ImageWireError::from_status(status.as_u16(), &text));
    }
    Ok(text)
}

/// Decode a reply body, naming what it was meant to be.
pub(crate) fn read<T: DeserializeOwned>(text: &str, what: &str) -> Result<T, ImageWireError> {
    serde_json::from_str(text).map_err(|error| {
        let shown: String = text.chars().take(400).collect();
        ImageWireError::InvalidResponse(format!("unreadable {what}: {error}: {shown}"))
    })
}

#[cfg(test)]
pub(crate) mod loopback {
    //! One-request HTTP servers for the wire tests.

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::Receiver;

    /// One request as it crossed the socket.
    pub struct Captured {
        pub head: String,
        pub body: Vec<u8>,
    }

    impl Captured {
        pub fn header(&self, name: &str) -> Option<String> {
            self.head.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_string())
            })
        }

        pub fn json(&self) -> serde_json::Value {
            serde_json::from_slice(&self.body).expect("a JSON body")
        }
    }

    /// Serve one request with `status` and `reply`; returns the base URL and
    /// the captured request.
    pub fn serve_once(status: &'static str, reply: String) -> (String, Receiver<Captured>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut raw = Vec::new();
            let mut chunk = [0u8; 16384];
            let captured = loop {
                let read = stream.read(&mut chunk).unwrap();
                raw.extend_from_slice(&chunk[..read]);
                let Some(head_end) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
                    if read == 0 {
                        panic!("connection closed before the request head");
                    }
                    continue;
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let body = &raw[head_end + 4..];
                let length = head.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                });
                let chunked = head
                    .to_ascii_lowercase()
                    .contains("transfer-encoding: chunked");
                let complete = match length {
                    Some(length) => body.len() >= length,
                    None if chunked => body.ends_with(b"0\r\n\r\n"),
                    None => true,
                };
                if complete || read == 0 {
                    let body = if chunked {
                        dechunk(body)
                    } else {
                        body.to_vec()
                    };
                    break Captured { head, body };
                }
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            );
            let _ = sender.send(captured);
        });
        (format!("http://{address}/v1"), received)
    }

    fn dechunk(mut body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let Some(line_end) = body.windows(2).position(|w| w == b"\r\n") else {
                return out;
            };
            let size = usize::from_str_radix(String::from_utf8_lossy(&body[..line_end]).trim(), 16)
                .unwrap_or(0);
            if size == 0 {
                return out;
            }
            let start = line_end + 2;
            out.extend_from_slice(&body[start..start + size]);
            body = &body[start + size + 2..];
        }
    }
}
