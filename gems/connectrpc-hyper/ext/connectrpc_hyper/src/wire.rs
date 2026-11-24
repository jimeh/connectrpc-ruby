//! What one HTTP/2 connection's frames say about its streams, for the GOAWAY
//! retry (plan D12). h2 exposes neither a GOAWAY's last stream id nor, through
//! hyper, a request's stream id, so the transport observes both itself:
//!
//! - `Tap` sits between hyper and the socket (above TLS, so it sees plaintext
//!   frames) and reads only the 9-byte frame headers and a GOAWAY's last
//!   stream id. It records the lowest last stream id the peer's GOAWAYs name
//!   and the highest client stream id whose HEADERS frame header h2 offered
//!   to the socket, written or not. h2 0.4.20's `FramedWrite` encodes frames
//!   into one buffer and offers all of it (DATA payloads aside) on each write
//!   attempt, which follows every encode in the same poll, so a HEADERS frame
//!   is offered before it can be written.
//! - A request body's first `size_hint` call takes the connection's next
//!   stream index. In hyper 1.13.0 (`proto/h2/client.rs`,
//!   `ClientTask::poll_dispatch`) the dispatch task pops requests in order,
//!   skips one whose caller has gone, then calls `size_hint` exactly once
//!   and, in the same synchronous poll, h2's `send_request`. That consumes
//!   the next client stream id (1, 3, 5, ...) unless the connection already
//!   failed, and that failure is permanent, so no later request gets an id
//!   either. Index n is therefore stream 2n + 1. h2 can also skip an id
//!   while resetting a stream the peer should not have opened; that only
//!   makes an index lower than its stream, which never authorises a replay.
//!   A CONNECT request may be dropped after `size_hint` without an id, so
//!   one disables the record for its connection.
//!
//! A stream above a GOAWAY's last stream id was never processed (RFC 9113
//! 6.8): the peer's GOAWAY is the authority on that, as its REFUSED_STREAM
//! is. The tap follows frame lengths as h2 does, so each GOAWAY it reads is
//! one the peer sent, whatever h2 makes of the frames around it; it does not
//! repeat h2's validation. A peer that lies about what it processed can make
//! any client replay a request. A sent request is also replayed only when h2
//! itself failed its stream for a GOAWAY; any other replay needs proof that
//! the request's HEADERS never reached the socket. Anything the record cannot
//! vouch for answers "no".
//!
//! The tap also records that the server started HTTP/2 (its first non-ACK
//! SETTINGS frame, RFC 9113 3.4), for the pool dial's waiters (pool.rs).
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::holds::Holds;

const PREFACE: usize = 24;
const HEAD: usize = 9;
const NONE: u32 = u32::MAX;

const HEADERS: u8 = 0x1;
const SETTINGS: u8 = 0x4;
const ACK: u8 = 0x1;
const GOAWAY: u8 = 0x7;
const CONTINUATION: u8 = 0x9;

pub struct Wire {
    next: AtomicU64,
    /// Set when the record cannot be trusted: a CONNECT request, a GOAWAY
    /// that is malformed or raises the last stream id, or client frames the
    /// reader cannot follow.
    disabled: AtomicBool,
    /// The lowest last stream id of the GOAWAYs read, or NONE.
    goaway: AtomicU32,
    /// The highest client stream id whose HEADERS frame header h2 offered to
    /// the socket. It only grows, so one read answers "may it be sent".
    offered: AtomicU32,
    /// The server started HTTP/2: the tap read a non-ACK SETTINGS frame from
    /// it, its connection preface. Also set when the server-frame reader lost
    /// its place first, so the record never claims a preface was missing
    /// when it cannot tell. `disabled` does not touch it: CONNECT and the
    /// client-frame reader leave server frames readable.
    started: AtomicBool,
    holds: Arc<Holds>,
}

impl Wire {
    pub fn new(holds: Arc<Holds>) -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
            goaway: AtomicU32::new(NONE),
            offered: AtomicU32::new(0),
            started: AtomicBool::new(false),
            holds,
        })
    }

    pub fn disable(&self) {
        self.disabled.store(true, Ordering::SeqCst);
    }

    /// A GOAWAY arrived: the connection admits nothing more.
    pub fn going_away(&self) -> bool {
        self.goaway.load(Ordering::SeqCst) != NONE
    }

    /// The server's preface arrived, or the reader can no longer tell (see
    /// `started`). A connection that ends before it never was an HTTP/2
    /// connection: a TLS 1.3 server rejecting the client's certificate sends
    /// its alert where its SETTINGS would be.
    pub fn server_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn record_started(&self) {
        if !self.started.swap(true, Ordering::SeqCst) {
            // Spec hold point: armed with a zero bound, it counts the
            // connections whose server preface the tap read.
            self.holds.block("wire:settings");
        }
    }

    fn record_goaway(&self, last: u32) -> bool {
        let current = self.goaway.load(Ordering::SeqCst);
        // RFC 9113 6.8 forbids raising it; h2 fails the connection.
        if current != NONE && last > current {
            return false;
        }
        self.goaway.store(last, Ordering::SeqCst);
        true
    }

    fn record_headers(&self, stream: u32) {
        self.offered.fetch_max(stream, Ordering::SeqCst);
    }

    /// The stream id of `mark`'s request if the peer's GOAWAY says it never
    /// processed it.
    fn unprocessed(&self, mark: &Mark) -> Option<u32> {
        if self.disabled.load(Ordering::SeqCst) {
            return None;
        }
        let index = *mark.index.get()?;
        let stream = u32::try_from(2 * index + 1).ok()?;
        let last = self.goaway.load(Ordering::SeqCst);
        (last != NONE && stream > last).then_some(stream)
    }

    /// Whether a HEADERS frame for `stream` reached the socket or still
    /// waits in h2's write buffer.
    fn sent(&self, stream: u32) -> bool {
        self.offered.load(Ordering::SeqCst) >= stream
    }
}

/// A request's claim on its connection's stream order.
pub struct Mark {
    wire: Arc<Wire>,
    index: OnceLock<u64>,
}

impl Mark {
    pub fn new(wire: Arc<Wire>) -> Arc<Self> {
        Arc::new(Self {
            wire,
            index: OnceLock::new(),
        })
    }

    /// Called from the body's `size_hint`, in hyper's dispatch task.
    pub fn take_index(&self) {
        self.index.get_or_init(|| {
            // Spec hold points: armed with a zero bound, they count the
            // requests hyper's dispatch task passed to h2, and each stream id
            // derived for one, so a spec can compare them with the ids h2 put
            // on the wire.
            let holds = &self.wire.holds;
            holds.block("request:indexed");
            let index = self.wire.next.fetch_add(1, Ordering::SeqCst);
            if holds.armed() {
                holds.block(&format!("request:stream:{}", 2 * index + 1));
            }
            index
        });
    }

    /// hyper's dispatch task passed the request to h2 (`take_index`), so h2
    /// and the stream's response task, not the dispatcher, answer it. Read
    /// directly: a disabled record still says whether hyper took it.
    pub fn indexed(&self) -> bool {
        self.index.get().is_some()
    }

    /// A GOAWAY names a last stream id below this request's stream.
    pub fn unprocessed(&self) -> bool {
        self.wire.unprocessed(self).is_some()
    }

    /// Above the cutoff as above, and h2 never offered its HEADERS to the
    /// socket. Asked after the request failed. Once h2 has failed the stream
    /// for that GOAWAY, or for a connection error, it has dropped the
    /// stream's queued frames, so the HEADERS never go out. A request hyper
    /// dropped after an earlier GOAWAY that disowned nothing may still be
    /// opened before h2 reads the GOAWAY the tap already saw; then RFC 9113
    /// 6.8 keeps the peer from processing it.
    pub fn unsent(&self) -> bool {
        self.wire
            .unprocessed(self)
            .is_some_and(|stream| !self.wire.sent(stream))
    }
}

/// Incremental frame-header reader for one direction.
#[derive(Clone)]
struct Frames {
    /// Bytes of the client preface still to pass (write side only).
    preface: usize,
    header: [u8; HEAD],
    have: usize,
    /// Payload bytes of the current frame still to pass.
    remaining: usize,
    /// The current frame is a GOAWAY: its payload bytes seen so far.
    goaway: Option<([u8; 4], usize)>,
    /// The current frame is a non-ACK SETTINGS (server side): it counts as
    /// the server's preface only once its whole payload has passed, as
    /// grpc-go reads the full frame before accepting the preface.
    settings: bool,
    broken: bool,
}

impl Frames {
    fn new(preface: usize) -> Self {
        Self {
            preface,
            header: [0; HEAD],
            have: 0,
            remaining: 0,
            goaway: None,
            settings: false,
            broken: false,
        }
    }

    /// Server frames: records each GOAWAY, and the first non-ACK SETTINGS,
    /// once its payload passed.
    fn read(&mut self, wire: &Wire, mut bytes: &[u8]) {
        while !self.broken && !bytes.is_empty() {
            if self.remaining > 0 {
                let take = self.remaining.min(bytes.len());
                if let Some((last, seen)) = self.goaway.as_mut() {
                    let want = 4usize.saturating_sub(*seen).min(take);
                    last[*seen..*seen + want].copy_from_slice(&bytes[..want]);
                    *seen += want;
                }
                self.remaining -= take;
                bytes = &bytes[take..];
                if self.remaining == 0 {
                    self.end_frame(wire);
                }
                continue;
            }
            let take = (HEAD - self.have).min(bytes.len());
            self.header[self.have..self.have + take].copy_from_slice(&bytes[..take]);
            self.have += take;
            bytes = &bytes[take..];
            if self.have < HEAD {
                break;
            }
            self.have = 0;
            self.server_header();
            if self.remaining == 0 {
                self.end_frame(wire);
            }
        }
        if self.broken {
            wire.disable();
            // The reader stops here: it can no longer vouch that no SETTINGS
            // follows.
            wire.started.store(true, Ordering::SeqCst);
        }
    }

    fn server_header(&mut self) {
        let (length, kind, flags, stream) = parse(&self.header);
        self.settings = kind == SETTINGS && flags & ACK == 0;
        if kind == GOAWAY && (stream != 0 || length < 8) {
            // Not a GOAWAY anyone can read a last stream id from.
            self.broken = true;
            return;
        }
        self.goaway = (kind == GOAWAY).then_some(([0; 4], 0));
        self.remaining = length;
    }

    fn end_frame(&mut self, wire: &Wire) {
        if std::mem::take(&mut self.settings) {
            wire.record_started();
        }
        if let Some((last, _)) = self.goaway.take() {
            let last = u32::from_be_bytes(last) & 0x7fff_ffff;
            if !wire.record_goaway(last) {
                self.broken = true;
            }
        }
    }

    /// Client frames: records the HEADERS frame headers written.
    fn write(&mut self, wire: &Wire, bytes: &[u8]) {
        self.walk(bytes, |stream| wire.record_headers(stream));
        if self.broken {
            wire.disable();
        }
    }

    /// The highest stream id with a HEADERS frame header in `chunks`, which
    /// follow what was written, without consuming them.
    fn scan<'a>(&self, chunks: impl Iterator<Item = &'a [u8]>) -> u32 {
        let mut ahead = self.clone();
        let mut highest = 0;
        for chunk in chunks {
            ahead.walk(chunk, |stream| highest = highest.max(stream));
        }
        highest
    }

    fn walk(&mut self, mut bytes: &[u8], mut headers: impl FnMut(u32)) {
        while !self.broken && !bytes.is_empty() {
            if self.preface > 0 {
                let take = self.preface.min(bytes.len());
                self.preface -= take;
                bytes = &bytes[take..];
                continue;
            }
            if self.remaining > 0 {
                let take = self.remaining.min(bytes.len());
                self.remaining -= take;
                bytes = &bytes[take..];
                continue;
            }
            let take = (HEAD - self.have).min(bytes.len());
            self.header[self.have..self.have + take].copy_from_slice(&bytes[..take]);
            self.have += take;
            bytes = &bytes[take..];
            if self.have < HEAD {
                break;
            }
            self.have = 0;
            let (length, kind, _, stream) = parse(&self.header);
            if kind > CONTINUATION {
                // h2 sends no extension frames: the reader lost its place.
                self.broken = true;
                break;
            }
            if kind == HEADERS {
                headers(stream);
            }
            self.remaining = length;
        }
    }
}

fn parse(header: &[u8; HEAD]) -> (usize, u8, u8, u32) {
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    (length, header[3], header[4], stream)
}

/// The connection's IO as hyper sees it, observed by `Wire`.
pub struct Tap<T> {
    inner: T,
    wire: Arc<Wire>,
    read: Frames,
    write: Frames,
}

impl<T> Tap<T> {
    pub fn new(inner: T, wire: Arc<Wire>) -> Self {
        Self {
            inner,
            wire,
            read: Frames::new(0),
            write: Frames::new(PREFACE),
        }
    }

    /// Commits the bytes the socket took; HEADERS in the rest count as
    /// offered too.
    fn wrote(&mut self, result: &Poll<io::Result<usize>>, bufs: &[io::IoSlice<'_>]) {
        let mut taken = match result {
            Poll::Ready(Ok(n)) => *n,
            // A failed TLS write may still have sent part of the offer.
            Poll::Pending | Poll::Ready(Err(_)) => 0,
        };
        let mut split = (bufs.len(), 0);
        for (index, buf) in bufs.iter().enumerate() {
            let take = taken.min(buf.len());
            self.write.write(&self.wire, &buf[..take]);
            taken -= take;
            if take < buf.len() {
                split = (index, take);
                break;
            }
        }
        let (index, offset) = split;
        let rest = bufs
            .get(index)
            .map(|buf| &buf[offset..])
            .into_iter()
            .chain(bufs.iter().skip(index + 1).map(|buf| &buf[..]));
        let ahead = self.write.scan(rest);
        self.wire.record_headers(ahead);
        if ahead > 0 {
            // Spec hold point: armed with a zero bound, it counts writes that
            // left a HEADERS frame offered but not yet taken.
            self.wire.holds.block("wire:headers-queued");
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Tap<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            this.read.read(&this.wire, &buf.filled()[before..]);
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Tap<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.wrote(&result, &[io::IoSlice::new(buf)]);
        result
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.wrote(&result, bufs);
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let length = u32::try_from(payload.len())
            .expect("small payload")
            .to_be_bytes();
        let mut bytes = vec![length[1], length[2], length[3], kind, flags];
        bytes.extend_from_slice(&stream.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn read(frames: &[Vec<u8>]) -> Arc<Wire> {
        let wire = Wire::new(Arc::default());
        let mut reader = Frames::new(0);
        for bytes in frames {
            // One byte at a time: headers split across reads still count.
            for byte in bytes {
                reader.read(&wire, std::slice::from_ref(byte));
            }
        }
        wire
    }

    #[test]
    fn a_server_settings_frame_starts_http2_and_its_ack_does_not() {
        assert!(!read(&[frame(SETTINGS, ACK, 0, &[])]).server_started());
        assert!(read(&[frame(SETTINGS, 0, 0, &[0, 3, 0, 0, 0, 100])]).server_started());
    }

    #[test]
    fn a_settings_frame_cut_short_is_no_preface() {
        let whole = frame(SETTINGS, 0, 0, &[0, 3, 0, 0, 0, 100]);
        assert!(!read(&[whole[..whole.len() - 1].to_vec()]).server_started());
    }

    #[test]
    fn a_reader_that_lost_its_place_cannot_vouch_for_a_missing_preface() {
        let wire = read(&[frame(GOAWAY, 0, 1, &[0; 8])]);
        assert!(wire.server_started());
        assert!(wire.disabled.load(Ordering::SeqCst));
    }
}
