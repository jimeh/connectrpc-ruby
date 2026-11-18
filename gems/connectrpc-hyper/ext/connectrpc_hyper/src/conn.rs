//! Connection setup: resolution (getaddrinfo on tokio's blocking pool), TCP
//! with Go-style dual-stack fallback, rustls, then a hyper HTTP/2 or HTTP/1.1
//! handshake. Failures are classified by type and errno, never by text.
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioIo, TokioTimer};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::task::AbortHandle;

use crate::body::CallBody;
use crate::failure::Failure;
use crate::gvl::Signal;
use crate::runtime::{TrackedExec, Tracker};

/// Go's net.Dialer FallbackDelay for RFC 6555 dual-stack dialing.
const FALLBACK_DELAY: Duration = Duration::from_millis(300);
/// grpc-go's default maximum header list size; h2 only marks larger blocks,
/// so it must sit far above core's 64 KiB gRPC field limit.
const MAX_HEADER_LIST_SIZE: u32 = 16 << 20;
/// Bounds what h2 buffers for one stream beyond its send window.
const MAX_SEND_BUF_SIZE: usize = 1 << 20;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Origin {
    pub https: bool,
    pub host: String,
    pub port: u16,
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
        Ok(Self { https, host, port })
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

/// Tries each address in turn; returns the first error if all fail.
async fn dial_serial(addresses: &[SocketAddr]) -> Result<TcpStream, Failure> {
    let mut first = None;
    for address in addresses {
        match TcpStream::connect(address).await {
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
async fn dial(host: &str, port: u16) -> Result<TcpStream, Failure> {
    let addresses = resolve(host, port).await?;
    let primary_v4 = addresses[0].is_ipv4();
    let (primaries, fallbacks): (Vec<SocketAddr>, Vec<SocketAddr>) = addresses
        .into_iter()
        .partition(|a| a.is_ipv4() == primary_v4);
    if fallbacks.is_empty() {
        return dial_serial(&primaries).await;
    }
    let primary = dial_serial(&primaries);
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
    let fallback = dial_serial(&fallbacks);
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
/// preface is written; hyper exposes no peer-SETTINGS boundary).
pub async fn connect(
    settings: &Settings,
    tls: Option<Arc<rustls::ClientConfig>>,
    origin: &Origin,
    tracker: &Arc<Tracker>,
) -> Result<Connected, Failure> {
    let stream = dial(&origin.host, origin.port).await?;
    let _ = stream.set_nodelay(true);
    let local_port = stream.local_addr().map(|a| a.port()).unwrap_or(0);
    let socket = Socket::new(stream);
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
