//! A client for bginf's control service that can only read.

use std::fmt;
use std::time::Duration;

/// Reads bginf ctl's `GET /profiles` and `GET /state`, authenticated with
/// the `X-Bginf-Token` header.
///
/// GET-only by construction: these two methods are the only requests it can
/// make, so nothing built on it can pin, power or reconfigure bginf.
pub struct BginfClient {
    url: String,
    token: String,
    http: reqwest::blocking::Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    Build(String),
    Request {
        path: &'static str,
        error: String,
    },
    Status {
        path: &'static str,
        status: u16,
        body: String,
    },
    Body {
        path: &'static str,
        error: String,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Build(error) => write!(f, "build the bginf client: {error}"),
            Self::Request { path, error } => write!(f, "GET {path}: {error}"),
            Self::Status { path, status, body } => {
                write!(f, "GET {path}: HTTP {status}")?;
                if !body.is_empty() {
                    write!(f, ": {body}")?;
                }
                Ok(())
            }
            Self::Body { path, error } => write!(f, "GET {path}: response is not JSON: {error}"),
        }
    }
}

impl std::error::Error for ClientError {}

const TIMEOUT: Duration = Duration::from_secs(5);

impl BginfClient {
    pub fn new(
        url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<BginfClient, ClientError> {
        let http = reqwest::blocking::Client::builder()
            .timeout(TIMEOUT)
            .build()
            .map_err(|err| ClientError::Build(err.to_string()))?;
        Ok(BginfClient {
            url: url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            http,
        })
    }

    /// `GET /profiles`, as JSON.
    pub fn get_profiles(&self) -> Result<serde_json::Value, ClientError> {
        self.get("/profiles")
    }

    /// `GET /state`, as JSON.
    pub fn get_state(&self) -> Result<serde_json::Value, ClientError> {
        self.get("/state")
    }

    fn get(&self, path: &'static str) -> Result<serde_json::Value, ClientError> {
        let response = self
            .http
            .get(format!("{}{path}", self.url))
            .header("X-Bginf-Token", &self.token)
            .send()
            .map_err(|err| ClientError::Request {
                path,
                error: err.to_string(),
            })?;
        let status = response.status();
        if !status.is_success() {
            let body: String = response
                .text()
                .unwrap_or_default()
                .chars()
                .take(200)
                .collect();
            return Err(ClientError::Status {
                path,
                status: status.as_u16(),
                body: body.trim().to_string(),
            });
        }
        response.json().map_err(|err| ClientError::Body {
            path,
            error: err.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    /// Each request's line and its `X-Bginf-Token` header, if sent.
    type Seen = Vec<(String, Option<String>)>;

    /// A one-shot server that records each request's line and token header.
    fn serve(requests: usize) -> (String, std::thread::JoinHandle<Seen>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for _ in 0..requests {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut token = None;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = header.split_once(':') {
                        if name.eq_ignore_ascii_case("x-bginf-token") {
                            token = Some(value.trim().to_string());
                        }
                    }
                }
                let mut stream = stream;
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                    .unwrap();
                seen.push((request_line.trim_end().to_string(), token));
            }
            seen
        });
        (url, handle)
    }

    #[test]
    fn the_client_sends_only_its_two_gets_with_the_token() {
        let (url, server) = serve(2);
        let client = BginfClient::new(format!("{url}/"), "secret").unwrap();
        assert_eq!(client.get_profiles().unwrap(), serde_json::json!({}));
        assert_eq!(client.get_state().unwrap(), serde_json::json!({}));
        assert_eq!(
            server.join().unwrap(),
            vec![
                (
                    "GET /profiles HTTP/1.1".to_string(),
                    Some("secret".to_string())
                ),
                (
                    "GET /state HTTP/1.1".to_string(),
                    Some("secret".to_string())
                ),
            ]
        );
    }
}
