//! # `robust-ntrip-client` - Robust NTRIP Client
//!
//! This crate provides a client to connect to a Network Transport of RTCM via
//! Internet Protocol (NTRIP) server. RTCM stands for Radio Technical Commission
//! for Maritime services and is the message type carrying GNSS correction
//! signals to enable centimeter-resolution GNSS position finding.
//!
//! The implementation in this crate attempts to be robust against network
//! interruptions and other transient errors. The [`RobustNtripClient`] handles
//! the low-level interaction with the NTRIP server and would allow plugging an
//! RTCM parsing library. The [`ParsingNtripClient`] wraps this low-level client
//! and parses and validates the RTCM messages.
//!
//! See also the [`ntrip-client` crate](https://crates.io/crates/ntrip-client).
//! I was unaware of this other crate at the time I began writing
//! `robust-ntrip-client`.
//!
//! ## Example usage
//! ```rust,no_run
//! #[tokio::main]
//! async fn main() -> eyre::Result<()> {
//!    let raw_client = robust_ntrip_client::RobustNtripClient::new(
//!        "ntrip://username:password@example-ntrip-server.com/mountpoint",
//!        Default::default()
//!    ).await?;
//!    let mut ntrip = robust_ntrip_client::ParsingNtripClient::new(raw_client);
//!
//!    loop {
//!        let msg = ntrip.next().await?;
//!        println!(
//!            "message {}: {} bytes",
//!            msg.message_number(),
//!            msg.frame_data().len()
//!        );
//!    }
//!}
//! ```
use eyre::{Context, Result};
use std::str::FromStr;

/// Options for connecting to the NTRIP server.
pub struct RobustNtripClientOptions {
    /// Maximal interval to retry the NTRIP connection.
    pub max_backoff_duration: std::time::Duration,

    /// Reset the NTRIP connection after this duration of not receiving updates.
    pub timeout: Option<std::time::Duration>,
}

impl std::default::Default for RobustNtripClientOptions {
    fn default() -> Self {
        Self {
            max_backoff_duration: std::time::Duration::from_secs(30),
            timeout: Some(std::time::Duration::from_secs(10)),
        }
    }
}

/// A client which automatically reconnects to an NTRIP server in case of
/// interruption.
pub struct RobustNtripClient {
    request_url: String,
    user_pass: Option<(String, String)>,
    client: reqwest::Client,
    timeout: Option<std::time::Duration>,
    max_backoff_duration: std::time::Duration,

    response: reqwest::Response,
}

impl RobustNtripClient {
    /// Create a new connection to an NTRIP server.
    pub async fn new(url: &str, opts: RobustNtripClientOptions) -> Result<Self> {
        let uri: http::Uri = url
            .parse()
            .with_context(|| format!("While parsing NTRIP URL \"{url}\"."))?;

        let (need_tls, default_port) = if let Some(scheme) = uri.scheme() {
            let ntrip = http::uri::Scheme::from_str("ntrip").unwrap();
            let http = http::uri::Scheme::from_str("http").unwrap();
            let https = http::uri::Scheme::from_str("https").unwrap();
            if scheme == &ntrip {
                (false, Some(2101))
            } else if scheme == &http {
                (false, None)
            } else if scheme == &https {
                (true, None)
            } else {
                eyre::bail!("Unexpected URI scheme (found \"{scheme}\").");
            }
        } else {
            // I'm not sure how this would be possible. I think parsing above would
            // fail.
            eyre::bail!("No URI scheme.");
        };

        let parts = uri.into_parts();
        let (host_port, user_pass) = if let Some(auth) = &parts.authority {
            parse_authority(auth)?
        } else {
            eyre::bail!("No authority section of URL");
        };
        let auth = http::uri::Authority::from_maybe_shared(host_port)?;

        let host = auth.host();
        let port = auth.port_u16().or(default_port);
        let mountpoint = if let Some(pq) = parts.path_and_query {
            pq.path().to_string()
        } else {
            "/".to_string()
        };

        let scheme = if need_tls { "https" } else { "http" };
        let port = if let Some(port) = port {
            format!(":{port}")
        } else {
            "".to_string()
        };
        let request_url = format!("{scheme}://{host}{port}{mountpoint}");

        let mut headers = reqwest::header::HeaderMap::new();
        // See https://support.pointonenav.com/polaris-ntrip-api-docs
        headers.insert(
            "Ntrip-Version",
            reqwest::header::HeaderValue::from_static("ntrip/2.0"),
        );

        let client = reqwest::ClientBuilder::new()
            .tcp_keepalive(std::time::Duration::from_secs(5))
            .default_headers(headers)
            .user_agent(format!(
                "NTRIP {}/{}",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            ))
            .build()?;

        let max_backoff_duration = opts.max_backoff_duration;
        let timeout = opts.timeout;
        let response = establish_connection(
            &client,
            &request_url,
            user_pass.as_ref(),
            max_backoff_duration,
        )
        .await?;

        Ok(Self {
            request_url,
            user_pass,
            client,
            timeout,
            max_backoff_duration,

            response,
        })
    }

    /// Get the next chunk of raw bytes from the NTRIP server.
    pub async fn chunk(&mut self) -> Result<bytes::Bytes> {
        if let Some(duration) = self.timeout {
            self.next_chunk_with_timeout(duration).await
        } else {
            self.next_chunk_infinite_wait().await
        }
    }

    async fn next_chunk_with_timeout(
        &mut self,
        duration: std::time::Duration,
    ) -> Result<bytes::Bytes> {
        match tokio::time::timeout(duration, self.next_chunk_infinite_wait()).await {
            Ok(next) => next, // normal case: new data before timeout
            Err(_) => {
                tracing::warn!("Reconnecting due to timeout elapsed.");
                self.reconnect_and_get_first_chunk().await
            }
        }
    }

    async fn next_chunk_infinite_wait(&mut self) -> Result<bytes::Bytes> {
        match self.response.chunk().await {
            Ok(Some(next)) => Ok(next), // normal case: new data
            Ok(None) => {
                tracing::warn!("Reconnecting due to end of HTTP stream.");
                self.reconnect_and_get_first_chunk().await
            }
            Err(_) => {
                tracing::warn!("Reconnecting due to error with HTTP stream.");
                self.reconnect_and_get_first_chunk().await
            }
        }
    }

    async fn reconnect_and_get_first_chunk(&mut self) -> Result<bytes::Bytes> {
        let mut backoff = min_dur(std::time::Duration::from_secs(1), self.max_backoff_duration);
        loop {
            self.response = establish_connection(
                &self.client,
                &self.request_url,
                self.user_pass.as_ref(),
                self.max_backoff_duration,
            )
            .await?;

            match self.response.chunk().await {
                Ok(Some(chunk)) => return Ok(chunk),
                Ok(None) => {
                    tracing::warn!(
                        "NTRIP stream ended before yielding data after reconnect; retrying."
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "Error reading NTRIP stream after reconnect; retrying."
                    );
                }
            }

            tokio::time::sleep(backoff).await;
            backoff = min_dur(backoff * 2, self.max_backoff_duration);
        }
    }
}

async fn establish_connection(
    client: &reqwest::Client,
    request_url: &str,
    user_pass: Option<&(String, String)>,
    max_backoff_duration: std::time::Duration,
) -> Result<reqwest::Response> {
    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        tracing::info!("Establishing connection to {request_url}.");
        let mut req_builder = client.get(request_url);
        if let Some((username, password)) = &user_pass {
            req_builder = req_builder.basic_auth(username, Some(password));
        }
        let result_response = req_builder.send().await;
        match result_response {
            Ok(response) => {
                tracing::debug!("Sent request");

                if !response.status().is_success() {
                    eyre::bail!("Error getting NTRIP URL: HTTP status {}", response.status());
                }

                return Ok(response);
            }
            Err(e) => {
                let error = eyre::Report::from(e);
                let mut err_msg = format!("Could not open NTRIP URL: {error}");
                for cause in error.chain() {
                    err_msg = format!("{err_msg}\n   cause: {cause}");
                }
                tracing::warn!("{err_msg}");

                tokio::time::sleep(backoff).await;
                backoff = min_dur(backoff * 2, max_backoff_duration);
            }
        }
    }
}

fn min_dur(a: std::time::Duration, b: std::time::Duration) -> std::time::Duration {
    if a < b { a } else { b }
}

fn parse_authority(auth: &http::uri::Authority) -> Result<(String, Option<(String, String)>)> {
    // Replace when https://github.com/hyperium/http/pull/399 is merged.
    let auth_vec = auth.as_str().split("@").collect::<Vec<_>>();
    match auth_vec.len() {
        1 => {
            // Only "host:port"
            let host_port = auth_vec[0].to_string();
            Ok((host_port, None))
        }
        2 => {
            // "username:password@host:port"
            let user_pass = auth_vec[0];
            let host_port = auth_vec[1].to_string();
            let up = user_pass.split(":").collect::<Vec<_>>();
            if up.len() != 2 {
                eyre::bail!("Could not parse username and password from URL");
            }
            let username = up[0].to_string();
            let password = up[1].to_string();
            Ok((host_port, Some((username, password))))
        }
        _ => {
            eyre::bail!("Expected zero or one '@' symbols in authority");
        }
    }
}

#[test]
fn test_parse_example_url() {
    let uri: http::Uri = "ntrip://hostname.com:2101/mountpoint".parse().unwrap();
    let my_scheme = http::uri::Scheme::from_str("ntrip").unwrap();
    assert_eq!(uri.scheme(), Some(&my_scheme));
    let authority = uri.authority().unwrap();
    assert_eq!(authority.host(), "hostname.com");
    assert_eq!(authority.port_u16(), Some(2101));
    let (host_port, user_pass) = parse_authority(authority).unwrap();
    assert!(user_pass.is_none());
    assert_eq!(host_port, "hostname.com:2101");
    let path_and_query = uri.path_and_query().unwrap();
    assert_eq!(path_and_query.path(), "/mountpoint");
}

#[test]
fn test_parse_example_url_with_user_pass() {
    let uri: http::Uri = "ntrip://username:password@hostname.com:2101/mountpoint"
        .parse()
        .unwrap();
    let my_scheme = http::uri::Scheme::from_str("ntrip").unwrap();
    assert_eq!(uri.scheme(), Some(&my_scheme));
    let authority = uri.authority().unwrap();
    assert_eq!(authority.host(), "hostname.com");
    assert_eq!(authority.port_u16(), Some(2101));
    let (host_port, user_pass) = parse_authority(authority).unwrap();
    let (username, password) = user_pass.unwrap();
    assert_eq!(username, "username");
    assert_eq!(password, "password");
    assert_eq!(host_port, "hostname.com:2101");
    let path_and_query = uri.path_and_query().unwrap();
    assert_eq!(path_and_query.path(), "/mountpoint");
}

#[tokio::test]
async fn reconnect_retries_a_truncated_first_chunk() {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let responses: [&[u8]; 3] = [
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\ninvalid\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n",
        ];

        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut byte = [0_u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            stream.write_all(response).unwrap();
        }
    });

    let mut client = RobustNtripClient::new(
        &format!("http://{address}/mountpoint"),
        RobustNtripClientOptions {
            max_backoff_duration: std::time::Duration::ZERO,
            timeout: None,
        },
    )
    .await
    .unwrap();

    let chunk = client.reconnect_and_get_first_chunk().await.unwrap();
    assert_eq!(chunk, "test");
    server.join().unwrap();
}

/// One valid frame of RTCM data from the NTRIP server.
pub struct FrameData {
    frame_data: bytes::BytesMut,
    message_number: u16,
}

impl FrameData {
    /// Get the RTCM data.
    pub fn frame_data(&self) -> &[u8] {
        &self.frame_data
    }
    /// Get the RTCM message number.
    pub fn message_number(&self) -> u16 {
        self.message_number
    }
}

impl From<FrameData> for Vec<u8> {
    fn from(val: FrameData) -> Self {
        val.frame_data.into()
    }
}

/// A client which parses RTCM messages from the NTRIP stream.
pub struct ParsingNtripClient {
    client: RobustNtripClient,
    buf: bytes::BytesMut,
}

impl ParsingNtripClient {
    /// Create a parsing NTRIP client by wrapping a low-level NTRIP client.
    pub fn new(client: RobustNtripClient) -> Self {
        let buf = bytes::BytesMut::new();
        Self { client, buf }
    }

    /// Get the next RTCM message from the NTRIP server.
    pub async fn next(&mut self) -> Result<FrameData> {
        loop {
            let mut advance_info = None;
            for (i, start_byte) in (&self.buf).into_iter().enumerate() {
                if *start_byte == 0xd3 {
                    match rtcm_rs::MessageFrame::new(&self.buf[i..]) {
                        Ok(m) => {
                            tracing::debug!(
                                "Found RTCM message {} frame with length {}",
                                m.message_number().unwrap(),
                                m.frame_len()
                            );
                            advance_info = Some((
                                i,
                                false,
                                Some((m.frame_len(), m.message_number().unwrap())),
                            ));
                            break;
                        }
                        Err(rtcm_rs::rtcm_error::RtcmError::Incomplete) => {
                            advance_info = Some((i, true, None)); // discard data prior to the start byte.
                            break;
                        }
                        Err(rtcm_rs::rtcm_error::RtcmError::NotValid) => {
                            advance_info = Some((i + 1, false, None)); // advance past the invalid "start byte".
                            break;
                        }
                        _ => unreachable!(),
                    }
                }
            }

            let (n_discard, do_read_more, msg_info) = if let Some(x) = advance_info {
                x
            } else {
                // no start byte found, so we need to read more data.
                (self.buf.len(), true, None)
            };

            let _discard_bytes = self.buf.split_to(n_discard);
            if let Some((frame_len, message_number)) = msg_info {
                assert!(!do_read_more);
                let frame_data = self.buf.split_to(frame_len);
                return Ok(FrameData {
                    frame_data,
                    message_number,
                });
            }

            if do_read_more {
                // Fetch more data.
                let this_buf = self.client.chunk().await?;
                self.buf.extend_from_slice(&this_buf);
            }
        }
    }
}
