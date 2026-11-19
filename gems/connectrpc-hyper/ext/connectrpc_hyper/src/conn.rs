//! Connection setup: resolution (getaddrinfo on tokio's blocking pool), TCP
//! with Go-style dual-stack fallback, an optional proxy CONNECT tunnel,
//! rustls, then a hyper HTTP/2 or HTTP/1.1 handshake. Failures are classified
//! by type and errno, never by text.
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioIo, TokioTimer};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::body::CallBody;
use crate::failure::Failure;
use crate::gvl::Signal;
use crate::runtime::{TrackedExec, Tracker};
use proxy::{Mode, Route};

/// Go's net.Dialer FallbackDelay for RFC 6555 dual-stack dialing.
const FALLBACK_DELAY: Duration = Duration::from_millis(300);
/// grpc-go's default maximum header list size; h2 only marks larger blocks,
/// so it must sit far above core's 64 KiB gRPC field limit.
const MAX_HEADER_LIST_SIZE: u32 = 16 << 20;
/// Bounds what h2 buffers for one stream beyond its send window.
const MAX_SEND_BUF_SIZE: usize = 1 << 20;
/// Go net/http's default MaxResponseHeaderBytes, for HTTP/1.1 response heads
/// (status line included) and chunked trailers. hyper's read buffer must be
/// able to hold a whole head, so it may grow to this size on a busy
/// connection; hyper's default (about 400 KiB) is below Go's bound.
const MAX_H1_HEAD: usize = 10 << 20;
/// Response header fields per HTTP/1.1 head. Go counts only bytes; hyper
/// also caps fields (default 100) and allocates this many slots per response.
const MAX_H1_HEADERS: usize = 1024;
/// Go's net.Dialer minimum per-address share of the setup budget
/// (saneMinimum in partialDeadline).
const MIN_ATTEMPT: Duration = Duration::from_secs(2);

/// A request's destination and the proxy it goes through, if any: the pool
/// key, so direct and proxied connections never mix.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Origin {
    pub https: bool,
    pub host: String,
    pub port: u16,
    pub proxy: Option<Route>,
}

impl Origin {
    pub fn parse(uri: &http::Uri) -> Result<Self, Failure> {
        let https = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err(Failure::new("request", "URL must be HTTP(S)")),
        };
        let host = uri
            .host()
            .ok_or_else(|| Failure::new("request", "URL has no host"))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        Ok(Self {
            https,
            host,
            port,
            proxy: None,
        })
    }

    /// True when requests go to a forwarding proxy in absolute form.
    pub fn forwarded(&self) -> bool {
        self.proxy.as_ref().is_some_and(|p| p.mode == Mode::Forward)
    }

    /// host:port as a CONNECT request names it, IPv6 literals bracketed.
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// A connected TCP socket listed in the fork-safe registry while it is open.
pub struct Socket {
    stream: TcpStream,
    #[cfg(unix)]
    slot: Option<usize>,
}

impl Socket {
    fn new(stream: TcpStream) -> Self {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let slot = crate::runtime::sockets::register(stream.as_raw_fd());
            Self { stream, slot }
        }
        #[cfg(not(unix))]
        Self { stream }
    }

    /// Copies bytes waiting to be read without consuming them.
    pub async fn peek(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.stream.peek(buffer).await
    }

    /// Reads exactly `buffer.len()` bytes.
    pub async fn consume(&mut self, buffer: &mut [u8]) -> io::Result<()> {
        self.stream.read_exact(buffer).await.map(|_| ())
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // Deregister before the stream field drops and closes the fd.
        #[cfg(unix)]
        if let Some(slot) = self.slot.take() {
            crate::runtime::sockets::deregister(slot);
        }
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub struct Settings {
    pub http2: bool,
    pub keepalive: Option<(Duration, Duration, bool)>,
    pub tunnel: proxy::Limits,
}

pub enum Handshake {
    H2 {
        sender: http2::SendRequest<CallBody>,
        /// Fires when hyper's dispatcher ends: the connection admits no more
        /// requests (GOAWAY, error or close).
        closed: Arc<Signal>,
    },
    H1 {
        sender: http1::SendRequest<CallBody>,
        task: AbortHandle,
    },
}

pub struct Connected {
    pub handshake: Handshake,
    pub local_port: u16,
}

async fn resolve(host: &str, port: u16) -> Result<Vec<SocketAddr>, Failure> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| Failure::io("dns", &e))?
        .collect();
    if addresses.is_empty() {
        return Err(Failure::new("dns", "no addresses"));
    }
    Ok(addresses)
}

fn connect_failure(error: &io::Error) -> Failure {
    let kind = match error.kind() {
        io::ErrorKind::ConnectionRefused => "refused",
        io::ErrorKind::HostUnreachable | io::ErrorKind::NetworkUnreachable => "unreachable",
        _ => "connect",
    };
    Failure::io(kind, error)
}

/// Go's partialDeadline: an equal share of the setup budget left for each
/// remaining address, at least MIN_ATTEMPT (or all that is left).
fn attempt_budget(deadline: Instant, remaining_addresses: usize) -> Duration {
    let left = deadline.saturating_duration_since(Instant::now());
    let share = left / u32::try_from(remaining_addresses.max(1)).unwrap_or(u32::MAX);
    if share < MIN_ATTEMPT {
        left.min(MIN_ATTEMPT)
    } else {
        share
    }
}

/// Tries each address in turn, each within its share of the setup budget, so
/// a blackholed address does not consume all of it; returns the first error
/// if all fail. Without a budget each attempt runs until the OS gives up.
async fn dial_serial(
    addresses: &[SocketAddr],
    deadline: Option<Instant>,
) -> Result<TcpStream, Failure> {
    let mut first = None;
    for (index, address) in addresses.iter().enumerate() {
        let attempt = TcpStream::connect(address);
        let result = match deadline {
            Some(deadline) => {
                let budget = attempt_budget(deadline, addresses.len() - index);
                match tokio::time::timeout(budget, attempt).await {
                    Ok(result) => result,
                    Err(_) => {
                        first.get_or_insert_with(|| {
                            Failure::new("connect_timeout", "connection attempt timed out")
                        });
                        continue;
                    }
                }
            }
            None => attempt.await,
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                first.get_or_insert_with(|| connect_failure(&e));
            }
        }
    }
    Err(first.unwrap_or_else(|| Failure::new("connect", "no addresses")))
}

/// RFC 6555 as Go dials: the first address's family is primary; the other
/// family starts after FALLBACK_DELAY, or at once if the primary fails, and
/// the first connection wins. The primary's error is reported if both fail.
async fn dial(host: &str, port: u16, deadline: Option<Instant>) -> Result<TcpStream, Failure> {
    let addresses = resolve(host, port).await?;
    let primary_v4 = addresses[0].is_ipv4();
    let (primaries, fallbacks): (Vec<SocketAddr>, Vec<SocketAddr>) = addresses
        .into_iter()
        .partition(|a| a.is_ipv4() == primary_v4);
    if fallbacks.is_empty() {
        return dial_serial(&primaries, deadline).await;
    }
    let primary = dial_serial(&primaries, deadline);
    tokio::pin!(primary);
    let delay = tokio::time::sleep(FALLBACK_DELAY);
    tokio::pin!(delay);
    let mut primary_failure = None;
    tokio::select! {
        result = &mut primary => match result {
            Ok(stream) => return Ok(stream),
            Err(failure) => primary_failure = Some(failure),
        },
        _ = &mut delay => {}
    }
    let fallback = dial_serial(&fallbacks, deadline);
    tokio::pin!(fallback);
    if let Some(failure) = primary_failure {
        return fallback.await.map_err(|_| failure);
    }
    let mut fallback_failure = None;
    loop {
        tokio::select! {
            result = &mut primary, if primary_failure.is_none() => match result {
                Ok(stream) => return Ok(stream),
                Err(failure) => {
                    if fallback_failure.is_some() {
                        return Err(failure);
                    }
                    primary_failure = Some(failure);
                }
            },
            result = &mut fallback, if fallback_failure.is_none() => match result {
                Ok(stream) => return Ok(stream),
                Err(failure) => {
                    if let Some(primary) = primary_failure {
                        return Err(primary);
                    }
                    fallback_failure = Some(failure);
                }
            },
        }
    }
}

fn tls_failure(error: &io::Error) -> Failure {
    let verify = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        .is_some_and(|e| {
            matches!(
                e,
                rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented
            )
        });
    if verify {
        Failure::new("tls_verify", "certificate verification failed")
    } else if error
        .get_ref()
        .is_some_and(|inner| inner.is::<rustls::Error>())
    {
        Failure::new("tls", "TLS handshake failed")
    } else {
        Failure::io("closed", error)
    }
}

/// Connects and completes the handshake hyper performs (for HTTP/2, the
/// preface is written; hyper exposes no peer-SETTINGS boundary). `deadline`
/// ends the setup budget that TCP attempts share; the caller enforces it.
pub async fn connect(
    settings: &Settings,
    tls: Option<Arc<rustls::ClientConfig>>,
    origin: &Origin,
    tracker: &Arc<Tracker>,
    deadline: Option<Instant>,
) -> Result<Connected, Failure> {
    let stream = match &origin.proxy {
        Some(route) => dial(&route.host, route.port, deadline).await?,
        None => dial(&origin.host, origin.port, deadline).await?,
    };
    let _ = stream.set_nodelay(true);
    let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
    let mut socket = Socket::new(stream);
    if let Some(route) = origin.proxy.as_ref().filter(|r| r.mode == Mode::Tunnel) {
        // Only an h2c server may send its SETTINGS before the client speaks.
        let strict = origin.https || !settings.http2;
        proxy::tunnel(
            &mut socket,
            &origin.authority(),
            route.authorization.as_ref(),
            settings.tunnel,
            strict,
        )
        .await?;
    }
    let (io, h2): (Box<dyn Io>, bool) = match tls {
        Some(config) => {
            // An IP literal becomes ServerName::IpAddress: no SNI (RFC 6066).
            let name = ServerName::try_from(origin.host.clone())
                .map_err(|_| Failure::new("tls", "invalid server name"))?;
            let stream = tokio_rustls::TlsConnector::from(config)
                .connect(name, socket)
                .await
                .map_err(|e| tls_failure(&e))?;
            let h2 = stream.get_ref().1.alpn_protocol() == Some(b"h2");
            (Box::new(stream), h2)
        }
        // Cleartext HTTP/2 is prior knowledge (h2c), as gRPC clients use.
        None => (Box::new(socket), settings.http2),
    };
    let io = TokioIo::new(io);
    let exec = TrackedExec(tracker.clone());
    if h2 {
        let mut builder = http2::Builder::new(exec);
        builder
            .timer(TokioTimer::new())
            .adaptive_window(true)
            .max_header_list_size(MAX_HEADER_LIST_SIZE)
            .max_send_buf_size(MAX_SEND_BUF_SIZE);
        if let Some((interval, timeout, while_idle)) = settings.keepalive {
            builder
                .keep_alive_interval(interval)
                .keep_alive_timeout(timeout)
                .keep_alive_while_idle(while_idle);
        }
        let (sender, connection) = builder
            .handshake(io)
            .await
            .map_err(|e| Failure::from_hyper(&e))?;
        let closed = Signal::new();
        let dispatcher_closed = closed.clone();
        tracker.spawn(async move {
            let _ = connection.await;
            dispatcher_closed.fire();
        });
        Ok(Connected {
            handshake: Handshake::H2 { sender, closed },
            local_port,
        })
    } else {
        let (sender, connection) = http1::Builder::new()
            .max_buf_size(MAX_H1_HEAD)
            .max_header_size(MAX_H1_HEAD)
            .max_headers(MAX_H1_HEADERS)
            .handshake(io)
            .await
            .map_err(|e| Failure::from_hyper(&e))?;
        let task = tracker.spawn(async move {
            let _ = connection.await;
        });
        Ok(Connected {
            handshake: Handshake::H1 { sender, task },
            local_port,
        })
    }
}

/// HTTP proxy routes. Ruby resolves the proxy (core's ConnectRPC::Proxy) and
/// passes only the endpoint; nothing here reads proxy environment variables.
/// A CONNECT exchange has Go net/http's bounds: the response head is capped,
/// the exchange is capped in time even without a call deadline, and the status
/// must be exactly 200. Only fixed phrases leave this module, so proxy reply
/// bytes never reach errors.
pub mod proxy {
    use std::fmt;
    use std::time::{Duration, Instant};

    use tokio::io::AsyncWriteExt;

    use super::Socket;
    use crate::failure::Failure;

    /// How a request reaches its origin through a proxy.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum Mode {
        /// A CONNECT tunnel; the origin's protocol runs inside it.
        Tunnel,
        /// Absolute-form HTTP/1.1 requests with Proxy-Authorization.
        Forward,
    }

    /// The only place that chooses the mode. TLS and h2c targets tunnel, as
    /// grpc-go does; plaintext HTTP/1.1 is forwarded in absolute form, as Go
    /// net/http and core NetHTTP do.
    pub fn mode(https: bool, http2: bool) -> Mode {
        if https || http2 {
            Mode::Tunnel
        } else {
            Mode::Forward
        }
    }

    /// A resolved proxy endpoint. Part of the pool key, so direct connections,
    /// different proxies and different credentials never share a connection.
    #[derive(Clone, PartialEq, Eq, Hash)]
    pub struct Route {
        pub host: String,
        pub port: u16,
        /// The Proxy-Authorization value (Basic), marked sensitive.
        pub authorization: Option<http::HeaderValue>,
        pub mode: Mode,
    }

    impl fmt::Debug for Route {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Route(proxy)")
        }
    }

    /// Bounds for one CONNECT exchange.
    #[derive(Clone, Copy)]
    pub struct Limits {
        pub timeout: Duration,
        pub max_head: usize,
    }

    fn failed() -> Failure {
        Failure::new("proxy_failed", "proxy tunnel failed")
    }

    /// Opens a tunnel to `authority` over a connected proxy socket. Nothing past
    /// the response head is consumed. `strict` rejects bytes that arrive with the
    /// head: only an h2c server may speak first (its SETTINGS preface).
    pub async fn tunnel(
        socket: &mut Socket,
        authority: &str,
        authorization: Option<&http::HeaderValue>,
        limits: Limits,
        strict: bool,
    ) -> Result<(), Failure> {
        let started = Instant::now();
        let mut request =
            format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n").into_bytes();
        if let Some(value) = authorization {
            request.extend_from_slice(b"Proxy-Authorization: ");
            request.extend_from_slice(value.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(b"\r\n");
        let exchange = async {
            socket
                .write_all(&request)
                .await
                .map_err(|e| Failure::io("closed", &e))?;
            read_head(socket, limits.max_head).await
        };
        let timed_out = || Failure::new("proxy_timeout", "proxy tunnel timed out");
        let (head, extra) = tokio::time::timeout(limits.timeout, exchange)
            .await
            .map_err(|_| timed_out())??;
        // A reply read after the cap passed is never accepted.
        if started.elapsed() >= limits.timeout {
            return Err(timed_out());
        }
        check_status(&head)?;
        if strict && extra {
            return Err(failed());
        }
        Ok(())
    }

    /// The end of the head at `index`: `\r?\n\r?\n`, returning the index after it.
    fn head_end_at(bytes: &[u8], index: usize) -> Option<usize> {
        let mut at = index;
        if bytes.get(at) == Some(&b'\r') {
            at += 1;
        }
        if bytes.get(at) != Some(&b'\n') {
            return None;
        }
        at += 1;
        match (bytes.get(at), bytes.get(at + 1)) {
            (Some(b'\r'), Some(b'\n')) => Some(at + 2),
            (Some(b'\n'), _) => Some(at + 1),
            _ => None,
        }
    }

    /// Reads the head without its terminator, peeking so that no byte after it is
    /// consumed, and reports whether more bytes were already buffered.
    async fn read_head(socket: &mut Socket, max: usize) -> Result<(Vec<u8>, bool), Failure> {
        let mut head = Vec::new();
        let mut buffer = vec![0u8; 16_384];
        loop {
            let room = max.saturating_sub(head.len());
            if room == 0 {
                return Err(failed());
            }
            let window = room.min(buffer.len());
            let peeked = socket
                .peek(&mut buffer[..window])
                .await
                .map_err(|e| Failure::io("closed", &e))?;
            if peeked == 0 {
                return Err(failed());
            }
            // A terminator may start in the last three bytes already read.
            let carried = head.len().min(3);
            let mut scan = head[head.len() - carried..].to_vec();
            scan.extend_from_slice(&buffer[..peeked]);
            let found =
                (0..scan.len()).find_map(|index| head_end_at(&scan, index).map(|end| (index, end)));
            let take = match found {
                Some((_, end)) => end - carried,
                None => peeked,
            };
            socket
                .consume(&mut buffer[..take])
                .await
                .map_err(|e| Failure::io("closed", &e))?;
            head.extend_from_slice(&buffer[..take]);
            if let Some((start, end)) = found {
                let head_len = head.len() - (end - start);
                head.truncate(head_len);
                return Ok((head, peeked > take));
            }
        }
    }

    fn token(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
    }

    /// Core NetHTTP's status and field checks: `HTTP/1.[01] NNN[ reason]` and
    /// `token:` fields; 407 and any other non-200 status have fixed phrases.
    fn check_status(head: &[u8]) -> Result<(), Failure> {
        let mut lines = head
            .split(|&byte| byte == b'\n')
            .map(|line| line.strip_suffix(b"\r").unwrap_or(line));
        let status = lines.next().unwrap_or_default();
        let code = status
            .strip_prefix(b"HTTP/1.")
            .filter(|rest| rest.len() >= 5 && matches!(rest[0], b'0' | b'1') && rest[1] == b' ')
            .map(|rest| &rest[2..])
            .filter(|rest| rest[..3].iter().all(u8::is_ascii_digit))
            .filter(|rest| {
                let reason = &rest[3..];
                reason.is_empty()
                    || (reason[0] == b' ' && !reason.iter().any(|&b| b == b'\r' || b == b'\n'))
            })
            .map(|rest| &rest[..3]);
        let fields = lines.all(|line| {
            let name = line.iter().take_while(|&&byte| token(byte)).count();
            name > 0 && line.get(name) == Some(&b':')
        });
        let (Some(code), true) = (code, fields) else {
            return Err(failed());
        };
        match code {
            b"200" => Ok(()),
            b"407" => Err(Failure::new("proxy_auth", "proxy authentication failed")),
            _ => Err(Failure::new("proxy_refused", "proxy tunnel refused")),
        }
    }
}
