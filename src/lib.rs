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
//! ## Reconnection policy: we follow QGroundControl
//!
//! RTCM 10410.1, the NTRIP 2.0 standard, is paywalled, and the free RTCM
//! guidance paper for client developers ("NTRIP Client Devices / Best
//! Practices", 2023-SC104-1344) stops short of saying which failures to retry.
//! Surveying shipping clients found no agreement either: pygnssutils treats
//! every HTTP error as final, gpsd fails the attempt and leaves reconnection to
//! its caller, and QGroundControl retries all but one status.
//!
//! Rather than invent a fifth policy, this crate copies QGroundControl's, on the
//! grounds that QGC does the same job -- streaming RTCM corrections to a drone
//! autopilot -- and that matching one real implementation exactly is easier to
//! defend and to re-check. Specifically, from
//! **QGroundControl v5.0.3-1265-gf1883f93a**
//! (`f1883f93a5ba0b5e9c1ec88b3815b290e2608697`, 2026-08-12):
//!
//! | Behaviour | Ours | QGC |
//! | --- | --- | --- |
//! | HTTP 401 | returned to the caller, never retried | `NTRIPHttpTransport.cc:388-392` maps it to `AuthFailed`, which `isRetryable()` refuses (`NTRIPManager.cc:82-91`) |
//! | Any other non-2xx status, 404 included | retried | `NTRIPHttpTransport.cc:406` maps it to `HttpError`, which is retryable |
//! | Transport errors (DNS, refused, reset, timeout) | retried | retryable by `isRetryable()`'s default arm |
//! | Backoff | 1, 2, 4, 8, 16, 30, 30, ... seconds | `kMinReconnectMs * (1 << qMin(attempts, 5))` capped at `kMaxReconnectMs`, `NTRIPManager.cc:367-370` and `NTRIPManager.h:159-161` |
//! | Giving up | after 100 attempts **that reached the caster** -- see below | `kMaxReconnectAttempts`, `NTRIPManager.h:161` |
//! | `Ntrip-Version` | `Ntrip/2.0` | `NTRIPHttpTransport.cc:77` |
//! | The caster's error reply body | captured on [`CasterHttpError`], tidied the same way for display | `NTRIPHttpTransport.cc:394-406` |
//! | Resetting the ladder | **on data arriving, not on handshake** -- see below | `NTRIPManager.cc:326` |
//!
//! [`RobustNtripClientOptions::max_backoff_duration`] plays QGC's
//! `kMaxReconnectMs` and defaults to the same 30 seconds; the other two values
//! are constants, as they are in QGC. The revision and every file:line above are
//! repeated in comments next to the code that mirrors them, so the values can be
//! cross-checked against a QGC checkout.
//!
//! Two deliberate departures, both because QGC's caller is an operator watching
//! a GUI and ours is unattended software.
//!
//! Failures that never reached the caster -- no route, DNS down, connection
//! refused: what an uplink that is not up yet looks like -- are retried
//! indefinitely rather than counting towards the limit. QGC can afford to stop,
//! because stopping puts a message in front of someone who is sitting there. A
//! rig that boots before its uplink comes up should be streaming corrections an
//! hour later, not holding an error nobody read. The limit still applies to
//! attempts the caster answered, which are the ones where something may need
//! correcting and where RTCM warns about hammering.
//!
//! Second, QGC resets its attempt counter when the HTTP
//! handshake completes (`NTRIPManager.cc:326`) rather than when data arrives, so
//! a caster which accepts a connection and then stays silent is retried at the
//! minimum interval forever -- the hammering RTCM's guidance warns gets a client
//! banned. We reset on data instead, so a silent caster climbs the same ladder as
//! a refused one and eventually exhausts [`MAX_CONNECT_ATTEMPTS`]. Where the
//! reset sits looks like an artefact of QGC's state machine rather than a
//! considered choice.
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

    /// Consecutive connection attempts that have not yet produced data.
    ///
    /// This is where we depart from QGC deliberately: see [`QGC_REVISION`].
    connect_attempts: u32,

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
            // Capitalised exactly as every NTRIP Version 2 client we surveyed
            // sends it, and as RTCM's guidance paper prints it. HTTP header
            // *values* are case sensitive, so a caster which compares this
            // exactly would read a lowercase value as a Version 1 request.
            // QGC: src/GPS/NTRIP/NTRIPHttpTransport.cc:77 and
            // src/GPS/NTRIP/NTRIPSourceTableController.cc:107 (see QGC_REVISION).
            reqwest::header::HeaderValue::from_static("Ntrip/2.0"),
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
        let mut connect_attempts = 0;
        let response = establish_connection(
            &client,
            &request_url,
            user_pass.as_ref(),
            max_backoff_duration,
            &mut connect_attempts,
        )
        .await?;

        Ok(Self {
            request_url,
            user_pass,
            client,
            timeout,
            max_backoff_duration,
            // Not zeroed: the handshake succeeding is not data arriving.
            connect_attempts,

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
            Ok(Some(next)) => {
                // Data arrived, so the connection is working: the backoff ladder
                // starts from the bottom again next time.
                self.connect_attempts = 0;
                Ok(next)
            }
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
        loop {
            self.response = establish_connection(
                &self.client,
                &self.request_url,
                self.user_pass.as_ref(),
                self.max_backoff_duration,
                &mut self.connect_attempts,
            )
            .await?;

            match self.response.chunk().await {
                Ok(Some(chunk)) => {
                    self.connect_attempts = 0;
                    return Ok(chunk);
                }
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

            // A caster that accepts the connection and then says nothing is a
            // failed attempt like any other, so it advances the same ladder.
            backoff_or_give_up(
                &mut self.connect_attempts,
                self.max_backoff_duration,
                "connected to the NTRIP caster but received no data",
                true,
            )
            .await?;
        }
    }
}

/// The QGroundControl revision whose reconnection policy this crate mirrors.
///
/// QGC is the closest analogue to how this crate is used -- streaming RTCM
/// corrections to a drone autopilot -- and RTCM 10410.1, which would say what
/// the protocol actually requires, is paywalled and unavailable to us. Matching
/// one real implementation exactly is easier to defend, and to re-check, than
/// inventing a policy of our own. Every citation below is against this revision:
///
/// - `src/GPS/NTRIP/NTRIPManager.h:159-161` -- the three constants mirrored here.
/// - `src/GPS/NTRIP/NTRIPManager.cc:367-370` -- `_reconnectBackoffMs()`, the shift.
/// - `src/GPS/NTRIP/NTRIPManager.cc:372-384` -- `_scheduleReconnect()`, which waits
///   on the pre-increment attempt count, then increments and checks the ceiling.
/// - `src/GPS/NTRIP/NTRIPManager.cc:82-91` -- `isRetryable()`.
/// - `src/GPS/NTRIP/NTRIPManager.cc:326` -- **the one place we deliberately
///   differ.** QGC resets its attempt counter once the transport reports
///   `Connected`, which is the HTTP handshake completing rather than data
///   arriving (`NTRIPHttpTransport.cc:371`). A caster that accepts the
///   connection and then stays silent therefore puts QGC back at
///   `kMinReconnectMs` on every cycle, forever, which is the hammering RTCM's
///   guidance warns leads to a ban. We reset on data instead, so a silent caster
///   climbs the same ladder as a refused one and eventually exhausts
///   [`MAX_CONNECT_ATTEMPTS`]. This looks like an artefact of where the reset sits
///   in QGC's state machine rather than a considered choice, so it is the one
///   point where copying seemed worse than not.
/// - `src/GPS/NTRIP/NTRIPHttpTransport.cc:388-392` -- 401 becomes `AuthFailed`.
/// - `src/GPS/NTRIP/NTRIPHttpTransport.cc:406` -- every other status becomes
///   `HttpError`, which `isRetryable()` accepts.
pub const QGC_REVISION: &str = "QGroundControl v5.0.3-1265-gf1883f93a \
    (f1883f93a5ba0b5e9c1ec88b3815b290e2608697, 2026-08-12)";

/// Shortest wait before reconnecting. QGC `kMinReconnectMs`.
///
/// Also the floor RTCM's client guidance sets: "An NTRIP Client should NEVER
/// reconnect to an NTRIP Caster more often than once a second."
const MIN_BACKOFF_DURATION: std::time::Duration = std::time::Duration::from_secs(1);

/// Attempts after which we stop trying and return an error. QGC
/// `kMaxReconnectAttempts`.
pub const MAX_CONNECT_ATTEMPTS: u32 = 100;

/// The wait doubles per attempt only up to this shift. QGC's
/// `qMin(_reconnectAttempts, 5)`, which makes the ladder 1, 2, 4, 8, 16, 30, 30,
/// ... seconds against the default ceiling.
const MAX_BACKOFF_SHIFT: u32 = 5;

/// How long to wait before connection attempt number `attempts_so_far` + 1.
///
/// QGC `_reconnectBackoffMs()`:
/// `qMin(kMinReconnectMs * (1 << qMin(_reconnectAttempts, 5)), kMaxReconnectMs)`,
/// with `max_backoff_duration` playing `kMaxReconnectMs`.
fn reconnect_backoff(
    attempts_so_far: u32,
    max_backoff_duration: std::time::Duration,
) -> std::time::Duration {
    let doubled = MIN_BACKOFF_DURATION * 2_u32.pow(attempts_so_far.min(MAX_BACKOFF_SHIFT));
    min_dur(doubled, max_backoff_duration)
}

/// An error reply from the NTRIP caster.
///
/// Returned inside the [`eyre::Report`] from [`RobustNtripClient::new`] when the
/// caster rejects the request with a status this crate does not retry, so a
/// caller can recover the caster's own explanation rather than only a status
/// code:
///
/// ```no_run
/// # async fn f() {
/// # let opts = robust_ntrip_client::RobustNtripClientOptions::default();
/// if let Err(report) = robust_ntrip_client::RobustNtripClient::new("ntrip://host/mp", opts).await
///     && let Some(caster) = report.downcast_ref::<robust_ntrip_client::CasterHttpError>()
/// {
///     eprintln!("caster said {}: {}", caster.status, caster.body);
/// }
/// # }
/// ```
///
/// RTCM's client guidance (2023-SC104-1344, p13) asks for exactly this: the
/// reply is "generally followed by a single line providing further details" and
/// that text "should be shown to the device user if at all possible". The same
/// value is also attached as the source of the give-up error after
/// [`MAX_CONNECT_ATTEMPTS`] failures, so the last thing the caster said survives
/// there too.
#[derive(Debug, Clone)]
pub struct CasterHttpError {
    /// The status the caster replied with.
    pub status: reqwest::StatusCode,

    /// The body of the reply, as received, truncated to
    /// [`ERROR_BODY_MAX_CHARS`]. Usually one line of plain ASCII, but some
    /// casters send HTML; [`CasterHttpError`]'s `Display` tidies it, this field
    /// does not. Empty if the caster sent no body or it could not be read.
    pub body: String,
}

impl std::fmt::Display for CasterHttpError {
    /// Mirrors QGC's user-visible message (`NTRIPHttpTransport.cc:394-406`):
    /// status, then the body with HTML stripped, whitespace collapsed and a
    /// length limit.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NTRIP caster answered HTTP {}", self.status)?;
        let tidied = tidy_error_body(&self.body);
        if !tidied.is_empty() {
            write!(f, " -- {tidied}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CasterHttpError {}

/// How much of an error reply body is kept. QGC's `body.left(500)`.
pub const ERROR_BODY_MAX_CHARS: usize = 500;

/// How much of the tidied body [`CasterHttpError`] shows. QGC's
/// `cleanBody.left(200)`.
const ERROR_BODY_DISPLAY_CHARS: usize = 200;

/// Bound on reading an error reply body.
///
/// Not from QGC, which reads whatever already arrived with the header on its own
/// socket. We are one `await` away from a caster that could send headers and
/// then stall, and reconnecting matters more than the explanation does.
const ERROR_BODY_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Read an error reply's body, giving up rather than blocking the reconnect.
async fn read_error_body(response: reqwest::Response) -> String {
    // `bytes()` rather than `text()`: this crate builds reqwest without default
    // features, which is where `text()`'s charset handling lives. Error bodies
    // are "plain text (in standard ASCII)" per RTCM's paper (p13).
    match tokio::time::timeout(ERROR_BODY_READ_TIMEOUT, response.bytes()).await {
        Ok(Ok(bytes)) => String::from_utf8_lossy(&bytes)
            .chars()
            .take(ERROR_BODY_MAX_CHARS)
            .collect(),
        Ok(Err(error)) => {
            tracing::debug!(%error, "Could not read the body of the NTRIP error reply.");
            String::new()
        }
        Err(_elapsed) => {
            tracing::debug!(
                ?ERROR_BODY_READ_TIMEOUT,
                "Timed out reading the body of the NTRIP error reply."
            );
            String::new()
        }
    }
}

/// Strip HTML tags, collapse whitespace and truncate, as QGC does before showing
/// a body to the user (`NTRIPHttpTransport.cc:394-406` removes `<[^>]*>`, then
/// `simplified()`, then `left(200)`).
fn tidy_error_body(body: &str) -> String {
    let mut without_tags = String::with_capacity(body.len());
    let mut in_tag = false;
    for c in body.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => without_tags.push(c),
            _ => {}
        }
    }
    let mut simplified = String::with_capacity(without_tags.len());
    for word in without_tags.split_whitespace() {
        if !simplified.is_empty() {
            simplified.push(' ');
        }
        simplified.push_str(word);
    }
    simplified.chars().take(ERROR_BODY_DISPLAY_CHARS).collect()
}

/// Wait out the backoff for the next attempt, or refuse to make one.
///
/// QGC `_scheduleReconnect()`: the wait is computed from the attempt count
/// *before* incrementing, then the ceiling is checked.
///
/// `reached_caster` says whether this failure came back *from* the caster -- a
/// status we do not accept, or a connection it accepted and then sent no data
/// on. Only those count towards [`MAX_CONNECT_ATTEMPTS`]. A failure that never
/// reached the caster at all, which is what no network looks like, backs off the
/// same way but is retried indefinitely: there is nothing for a caller to
/// correct and nothing to report, and a rig that boots before its uplink comes
/// up should still be streaming corrections an hour later rather than holding an
/// error nobody was there to read.
///
/// QGC counts both kinds against `kMaxReconnectAttempts` and stops. It can
/// afford to: giving up puts a message in front of an operator who is sitting
/// there. Ours is called by unattended software.
async fn backoff_or_give_up(
    attempts: &mut u32,
    max_backoff_duration: std::time::Duration,
    what_failed: &str,
    reached_caster: bool,
) -> Result<()> {
    let backoff = reconnect_backoff(*attempts, max_backoff_duration);
    *attempts = attempts.saturating_add(1);
    if reached_caster && *attempts >= MAX_CONNECT_ATTEMPTS {
        eyre::bail!(
            "Gave up after {MAX_CONNECT_ATTEMPTS} attempts: {what_failed} \
             (limit mirrored from {QGC_REVISION})"
        );
    }
    tracing::info!("Reconnecting to NTRIP caster in {backoff:?} (attempt {attempts}).");
    tokio::time::sleep(backoff).await;
    Ok(())
}

async fn establish_connection(
    client: &reqwest::Client,
    request_url: &str,
    user_pass: Option<&(String, String)>,
    max_backoff_duration: std::time::Duration,
    attempts: &mut u32,
) -> Result<reqwest::Response> {
    loop {
        tracing::info!("Establishing connection to {request_url}.");
        let mut req_builder = client.get(request_url);
        if let Some((username, password)) = &user_pass {
            req_builder = req_builder.basic_auth(username, Some(password));
        }
        let result_response = req_builder.send().await;
        // Whatever went wrong this time round, kept so the give-up error below
        // can name what the caster last said and not only how often we asked.
        // Whether the caster answered at all decides whether this attempt counts
        // towards the give-up limit: see `backoff_or_give_up`.
        let mut reached_caster = false;
        let attempt_error: eyre::Report = match result_response {
            Ok(response) => {
                reached_caster = true;
                tracing::debug!("Sent request");

                let status = response.status();
                if status.is_success() {
                    return Ok(response);
                }

                let caster_error = CasterHttpError {
                    status,
                    body: read_error_body(response).await,
                };
                if status == reqwest::StatusCode::UNAUTHORIZED {
                    // The one status QGC will not retry: `isRetryable()` refuses
                    // `AuthFailed`, and only a 401 produces it. Credentials the
                    // caster has rejected do not start working, and repeating a
                    // request a caster rejected as wrong is what gets a client
                    // banned. Every other status -- including 404, which casters
                    // also return for a mountpoint that is merely absent right
                    // now -- becomes `HttpError` there and is retried.
                    return Err(eyre::Report::new(caster_error));
                }
                tracing::warn!("{caster_error}");
                eyre::Report::new(caster_error)
            }
            Err(e) => {
                // Transport errors are retryable for QGC too: `SocketError`,
                // `ConnectionTimeout` and friends all fall through
                // `isRetryable()`'s default arm.
                let error = eyre::Report::from(e);
                let mut err_msg = format!("Could not open NTRIP URL: {error}");
                for cause in error.chain() {
                    err_msg = format!("{err_msg}\n   cause: {cause}");
                }
                tracing::warn!("{err_msg}");
                error
            }
        };

        // Keep whatever the caster or the network last said as the cause, so a
        // give-up error explains itself rather than only counting.
        backoff_or_give_up(
            attempts,
            max_backoff_duration,
            "the NTRIP caster kept refusing the request",
            reached_caster,
        )
        .await
        .map_err(|gave_up| attempt_error.wrap_err(format!("{gave_up}")))?;
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

/// Answer each connection in turn with the next canned HTTP response.
///
/// Returns the URL to point a client at, and the server thread: joining it
/// asserts that every canned response was actually served, so a client which
/// makes fewer attempts than expected fails the test rather than passing it
/// quietly.
#[cfg(test)]
fn stub_caster(responses: &'static [&'static [u8]]) -> (String, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
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
    (format!("http://{address}/mountpoint"), server)
}

#[cfg(test)]
fn no_backoff() -> RobustNtripClientOptions {
    RobustNtripClientOptions {
        max_backoff_duration: std::time::Duration::ZERO,
        timeout: None,
    }
}

#[cfg(test)]
#[tokio::test]
async fn reconnect_retries_a_truncated_first_chunk() {
    let (url, server) = stub_caster(&[
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\ninvalid\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n",
    ]);

    let mut client = RobustNtripClient::new(&url, no_backoff()).await.unwrap();

    let chunk = client.reconnect_and_get_first_chunk().await.unwrap();
    assert_eq!(chunk, "test");
    server.join().unwrap();
}

/// The 404 that took FLO down: a mountpoint which is normally present but is
/// absent "at this time", in RTCM's phrasing. QGC retries it -- it is an
/// ordinary `HttpError` there, not `AuthFailed`.
#[cfg(test)]
#[tokio::test]
async fn not_found_is_retried() {
    let (url, server) = stub_caster(&[
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ]);

    RobustNtripClient::new(&url, no_backoff())
        .await
        .expect("a 404 should be retried, not returned to the caller");
    // Joining is the assertion: both responses were served, so the 404 really
    // was followed by a second attempt.
    server.join().unwrap();
}

#[cfg(test)]
#[tokio::test]
async fn server_error_is_retried() {
    let (url, server) = stub_caster(&[
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ]);

    RobustNtripClient::new(&url, no_backoff())
        .await
        .expect("a 5xx should be retried, not returned to the caller");
    server.join().unwrap();
}

/// The single exception, mirroring QGC's `AuthFailed`.
#[cfg(test)]
#[tokio::test]
async fn unauthorized_is_returned_to_the_caller() {
    // The second response is what makes this a test rather than a tautology: a
    // client which retried the 401 would connect again and succeed, so getting
    // an error back is proof that it did not. The server thread is left holding
    // that unserved response and is not joined.
    let (url, _server) = stub_caster(&[
        b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    ]);

    // `let ... else` rather than `expect_err`: the success type is not `Debug`,
    // and making it so is not this test's business.
    let Err(error) = RobustNtripClient::new(&url, no_backoff()).await else {
        panic!("a 401 should be returned to the caller, not retried");
    };
    assert!(format!("{error}").contains("401"), "{error}");
}

/// A rig with no uplink keeps trying instead of erroring out.
///
/// Nothing is listening on the port, so every attempt fails before reaching a
/// caster. Those do not count towards [`MAX_CONNECT_ATTEMPTS`], so `new` should
/// still be trying when we stop waiting -- with the backoff ceiling at zero here
/// it will have made far more than `MAX_CONNECT_ATTEMPTS` attempts by then.
#[cfg(test)]
#[tokio::test]
async fn unreachable_casters_are_retried_without_limit() {
    // Bind, learn the port, then drop the listener: connections are refused.
    let url = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}/mountpoint", listener.local_addr().unwrap())
    };

    let still_trying = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        RobustNtripClient::new(&url, no_backoff()),
    )
    .await;
    assert!(
        still_trying.is_err(),
        "should still have been retrying, not returned"
    );
}

/// ... whereas a caster which is answering and rejecting us does stop.
#[cfg(test)]
#[tokio::test]
async fn attempts_that_reach_the_caster_are_limited() {
    const NO_WAIT: std::time::Duration = std::time::Duration::ZERO;
    let mut attempts = MAX_CONNECT_ATTEMPTS - 2;

    backoff_or_give_up(&mut attempts, NO_WAIT, "test", true)
        .await
        .unwrap();
    assert_eq!(attempts, MAX_CONNECT_ATTEMPTS - 1);

    let gave_up = backoff_or_give_up(&mut attempts, NO_WAIT, "test", true).await;
    let Err(error) = gave_up else {
        panic!("the limit should have been reached");
    };
    assert!(format!("{error}").contains("Gave up"), "{error}");

    // Past the limit, a failure that never reached the caster still does not
    // give up.
    backoff_or_give_up(&mut attempts, NO_WAIT, "test", false)
        .await
        .unwrap();
}

/// The attempt ladder is reset by data arriving, not by the handshake.
///
/// This is the deliberate departure from QGC described on [`QGC_REVISION`]: QGC
/// would be back at zero as soon as the 200 arrived.
#[cfg(test)]
#[tokio::test]
async fn only_data_resets_the_ladder() {
    let (url, server) = stub_caster(&[
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n",
    ]);

    let mut client = RobustNtripClient::new(&url, no_backoff()).await.unwrap();
    assert_eq!(
        client.connect_attempts, 1,
        "the 503 cost an attempt and the handshake did not refund it"
    );

    assert_eq!(client.chunk().await.unwrap(), "test");
    assert_eq!(client.connect_attempts, 0, "data resets the ladder");
    server.join().unwrap();
}

/// The caster's own explanation has to reach the caller, not just the log.
#[cfg(test)]
#[tokio::test]
async fn caster_error_body_reaches_the_caller() {
    // Content-Length must match the body exactly, or reqwest waits for bytes
    // that never come and the body read times out instead.
    let (url, _server) = stub_caster(&[
        b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 20\r\nConnection: close\r\n\r\nERROR - Bad Password",
    ]);

    let Err(report) = RobustNtripClient::new(&url, no_backoff()).await else {
        panic!("a 401 should be returned to the caller");
    };

    let caster = report
        .downcast_ref::<CasterHttpError>()
        .expect("the report should carry the caster's reply");
    assert_eq!(caster.status, reqwest::StatusCode::UNAUTHORIZED);
    assert!(caster.body.contains("Bad Password"), "{:?}", caster.body);
    // And it is in the message an operator sees.
    assert!(format!("{report}").contains("Bad Password"), "{report:#}");
}

#[cfg(test)]
#[test]
fn error_bodies_are_tidied_for_display() {
    let html =
        "<html>\n <head><title>Oops</title></head>\n <body>Mount point taken</body>\n</html>";
    assert_eq!(tidy_error_body(html), "Oops Mount point taken");

    assert_eq!(
        tidy_error_body("  ERROR - Bad Password  \r\n"),
        "ERROR - Bad Password"
    );
    assert_eq!(tidy_error_body(""), "");

    let long = "x".repeat(ERROR_BODY_MAX_CHARS);
    assert_eq!(
        tidy_error_body(&long).chars().count(),
        ERROR_BODY_DISPLAY_CHARS
    );
}

/// Pin the mirrored ladder so a future edit cannot quietly diverge from
/// `_reconnectBackoffMs()`. See [`QGC_REVISION`].
#[cfg(test)]
#[test]
fn backoff_ladder_matches_qgc() {
    let cap = RobustNtripClientOptions::default().max_backoff_duration;
    assert_eq!(
        cap,
        std::time::Duration::from_secs(30),
        "QGC kMaxReconnectMs"
    );

    let secs: Vec<u64> = (0..9)
        .map(|attempts| reconnect_backoff(attempts, cap).as_secs())
        .collect();
    assert_eq!(secs, vec![1, 2, 4, 8, 16, 30, 30, 30, 30]);
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
