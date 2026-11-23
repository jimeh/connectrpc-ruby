//! Native failures. Ruby maps each `kind` to core's fixed phrases; the detail
//! is diagnostic only and never carries paths or key material.
use std::error::Error as _;
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct Failure {
    pub kind: &'static str,
    pub detail: String,
    /// HTTP/2 error code of a received RST_STREAM or GOAWAY.
    pub reset: Option<u32>,
    pub errno: Option<i32>,
}

impl Failure {
    pub fn new(kind: &'static str, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            reset: None,
            errno: None,
        }
    }

    pub fn io(kind: &'static str, error: &std::io::Error) -> Self {
        Self {
            kind,
            detail: error.kind().to_string(),
            reset: None,
            errno: error.raw_os_error().map(ruby_errno),
        }
    }

    /// An I/O failure on an established connection. TLS 1.3 servers reject a
    /// client certificate after the handshake, so their alert arrives here.
    /// hyper's HTTP/1.1 body decoder reports bytes it rejects (a malformed
    /// chunk, or trailers past the field or byte cap) as `InvalidData` or
    /// `InvalidInput`, kinds a socket failure never has.
    pub fn transport_io(error: &std::io::Error) -> Self {
        match Self::rustls(error) {
            Some(failure) => failure,
            None if matches!(
                error.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
            ) =>
            {
                Self::new("protocol", "malformed response")
            }
            None => Self::io("closed", error),
        }
    }

    /// The TLS failure an I/O error carries, when rustls raised it.
    fn rustls(error: &std::io::Error) -> Option<Self> {
        match error.get_ref()?.downcast_ref::<rustls::Error>()? {
            rustls::Error::InvalidCertificate(_) => {
                Some(Self::new("tls_verify", "certificate verification failed"))
            }
            _ => Some(Self::new("tls", "TLS failure")),
        }
    }

    pub fn admission() -> Self {
        Self::new("admission", "transport admission is closed")
    }

    pub fn shutdown() -> Self {
        Self::new("shutdown", "transport closed")
    }

    /// Classifies by type, HTTP/2 code and errno, never by message text.
    pub fn from_hyper(error: &hyper::Error) -> Self {
        let mut source = error.source();
        while let Some(inner) = source {
            if let Some(h2e) = inner.downcast_ref::<h2::Error>() {
                if let Some(reason) = h2e.reason() {
                    let kind = if h2e.is_go_away() { "goaway" } else { "reset" };
                    return Self {
                        kind,
                        detail: format!("HTTP/2 {kind} {}", u32::from(reason)),
                        reset: Some(u32::from(reason)),
                        errno: None,
                    };
                }
                if let Some(io) = h2e.get_io() {
                    return Self::transport_io(io);
                }
            }
            if let Some(io) = inner.downcast_ref::<std::io::Error>() {
                return Self::transport_io(io);
            }
            source = inner.source();
        }
        if error.is_parse() || error.is_parse_status() {
            Self::new("protocol", "malformed response")
        } else {
            Self::new("closed", "connection closed")
        }
    }

    /// A stream the server refused before processing it (RFC 9113 8.7):
    /// RST_STREAM(REFUSED_STREAM), not a connection-level GOAWAY.
    pub fn refused_stream(error: &hyper::Error) -> bool {
        let mut source = error.source();
        while let Some(inner) = source {
            if let Some(h2e) = inner.downcast_ref::<h2::Error>() {
                return h2e.is_reset()
                    && h2e.is_remote()
                    && h2e.reason() == Some(h2::Reason::REFUSED_STREAM);
            }
            source = inner.source();
        }
        false
    }

    /// h2 failed the stream for a GOAWAY the peer sent: h2 read that frame,
    /// whatever else the tap saw on the socket.
    pub fn remote_go_away(error: &hyper::Error) -> bool {
        let mut source = error.source();
        while let Some(inner) = source {
            if let Some(h2e) = inner.downcast_ref::<h2::Error>() {
                return h2e.is_go_away() && h2e.is_remote();
            }
            source = inner.source();
        }
        false
    }
}

/// The first TLS failure rustls reported on one connection, whatever call was
/// running. hyper's HTTP/1.1 dispatcher returns an error that arrives with no
/// request queued (such as a TLS 1.3 server's `certificate_required` alert on
/// an idle connection) as its own output, so the next request only learns the
/// connection closed; h2 keeps only an I/O error's kind and text, losing the
/// rustls error. A call that then fails with a bare connection loss reports
/// this failure instead.
#[derive(Default)]
pub struct TlsFault(OnceLock<Failure>);

impl TlsFault {
    /// Keeps `error`'s TLS failure if it is the first on the connection.
    pub fn record(&self, error: &std::io::Error) {
        if self.0.get().is_none() {
            if let Some(failure) = Failure::rustls(error) {
                let _ = self.0.set(failure);
            }
        }
    }

    /// A call's failure on this connection: a connection loss (closed, or a
    /// protocol error without a reset code) becomes the recorded TLS failure.
    /// Resets, GOAWAYs, limits and timeouts pass through.
    pub fn resolve(&self, failure: Failure) -> Failure {
        let lost =
            failure.kind == "closed" || (failure.kind == "protocol" && failure.reset.is_none());
        match self.0.get() {
            Some(tls) if lost => tls.clone(),
            _ => failure,
        }
    }

    pub fn failure(&self, error: &hyper::Error) -> Failure {
        self.resolve(Failure::from_hyper(error))
    }
}

/// Ruby's `Errno` number for an OS error code. Windows reports socket errors
/// as WSA codes, which Ruby maps to the C runtime's errno values when it
/// raises (win32.c's `errmap`), so `Errno::ECONNREFUSED::Errno` is 107 there,
/// not 10061. Codes without a C runtime value keep their WSA number, as Ruby
/// defines those constants.
fn ruby_errno(code: i32) -> i32 {
    if cfg!(windows) {
        WSA_ERRNO
            .iter()
            .find(|(wsa, _)| *wsa == code)
            .map_or(code, |(_, errno)| *errno)
    } else {
        code
    }
}

/// (WSA code, UCRT errno) for the socket errors Ruby's `errmap` maps.
const WSA_ERRNO: [(i32, i32); 32] = [
    (10004, 4),   // EINTR
    (10009, 9),   // EBADF
    (10013, 13),  // EACCES
    (10014, 14),  // EFAULT
    (10022, 22),  // EINVAL
    (10024, 24),  // EMFILE
    (10035, 140), // EWOULDBLOCK
    (10036, 112), // EINPROGRESS
    (10037, 103), // EALREADY
    (10038, 128), // ENOTSOCK
    (10039, 109), // EDESTADDRREQ
    (10040, 115), // EMSGSIZE
    (10041, 136), // EPROTOTYPE
    (10042, 123), // ENOPROTOOPT
    (10043, 135), // EPROTONOSUPPORT
    (10045, 130), // EOPNOTSUPP
    (10047, 102), // EAFNOSUPPORT
    (10048, 100), // EADDRINUSE
    (10049, 101), // EADDRNOTAVAIL
    (10050, 116), // ENETDOWN
    (10051, 118), // ENETUNREACH
    (10052, 117), // ENETRESET
    (10053, 106), // ECONNABORTED
    (10054, 108), // ECONNRESET
    (10055, 119), // ENOBUFS
    (10056, 113), // EISCONN
    (10057, 126), // ENOTCONN
    (10060, 138), // ETIMEDOUT
    (10061, 107), // ECONNREFUSED
    (10062, 114), // ELOOP
    (10063, 38),  // ENAMETOOLONG
    (10065, 110), // EHOSTUNREACH
];
