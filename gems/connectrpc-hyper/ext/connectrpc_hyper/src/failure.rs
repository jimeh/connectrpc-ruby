//! Native failures. Ruby maps each `kind` to core's fixed phrases; the detail
//! is diagnostic only and never carries paths or key material.
use std::error::Error as _;

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
            errno: error.raw_os_error(),
        }
    }

    /// An I/O failure on an established connection. TLS 1.3 servers reject a
    /// client certificate after the handshake, so their alert arrives here.
    pub fn transport_io(error: &std::io::Error) -> Self {
        match error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        {
            Some(rustls::Error::InvalidCertificate(_)) => {
                Self::new("tls_verify", "certificate verification failed")
            }
            Some(_) => Self::new("tls", "TLS failure"),
            None => Self::io("closed", error),
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
}
