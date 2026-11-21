//! What one HTTP/2 connection's frames say about its streams, for the GOAWAY
//! retry (plan D12). h2 exposes neither a GOAWAY's last stream id nor, through
//! hyper, a request's stream id, so the transport observes both itself:
//!
//! - `Tap` sits between hyper and the socket (above TLS, so it sees plaintext
//!   frames) and reads only the 9-byte frame headers and a GOAWAY's last
//!   stream id. It records the lowest last stream id of each GOAWAY that h2
//!   accepts and the highest client stream id whose HEADERS frame header the
//!   socket took.
//! - A request body's first `size_hint` call takes the connection's next
//!   stream index. In hyper 1.12.0 (`proto/h2/client.rs`, `ClientTask::poll`)
//!   the dispatch task pops requests in order, skips one whose caller has
//!   gone, then calls `size_hint` exactly once and, in the same synchronous
//!   poll, h2's `send_request`. That consumes the next client stream id
//!   (1, 3, 5, ...) unless the connection already failed, and that failure is
//!   permanent, so no later request gets an id either. Index n is therefore
//!   stream 2n + 1. h2 can also skip an id while resetting a stream the peer
//!   should not have opened; that only makes an index lower than its stream,
//!   which never authorises a replay. A CONNECT request may be dropped after
//!   `size_hint` without an id, so one disables the record for its connection.
//!
//! A stream above an accepted GOAWAY's last stream id was never processed
//! (RFC 9113 6.8). Anything the record cannot vouch for answers "no".
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Notify;

use crate::holds::Holds;

const PREFACE: usize = 24;
const HEAD: usize = 9;
/// hyper's local SETTINGS_MAX_FRAME_SIZE: h2 rejects larger frames.
const MAX_FRAME: usize = 16_384;
const NONE: u32 = u32::MAX;

const HEADERS: u8 = 0x1;
const SETTINGS: u8 = 0x4;
const PUSH_PROMISE: u8 = 0x5;
const GOAWAY: u8 = 0x7;
const CONTINUATION: u8 = 0x9;
const END_HEADERS: u8 = 0x4;

pub struct Wire {
    next: AtomicU64,
    /// Set when the record cannot be trusted: a CONNECT request, or frames
    /// h2 would reject (it then fails the connection itself).
    disabled: AtomicBool,
    /// The lowest last stream id of the GOAWAYs read, or NONE.
    goaway: AtomicU32,
    /// The highest client stream id whose HEADERS frame header was written.
    written: AtomicU32,
    /// A socket write returned Pending since h2 last flushed. h2 buffers
    /// encoded frames and calls `poll_flush` only once its buffer is empty,
    /// so frames can wait in that buffer, unseen, only while this is set.
    backlog: AtomicBool,
    flushes: AtomicU64,
    /// Woken by each flush and when the tap drops.
    progress: Notify,
    closed: AtomicBool,
    holds: Arc<Holds>,
}

impl Wire {
    pub fn new(holds: Arc<Holds>) -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
            goaway: AtomicU32::new(NONE),
            written: AtomicU32::new(0),
            backlog: AtomicBool::new(false),
            flushes: AtomicU64::new(0),
            progress: Notify::new(),
            closed: AtomicBool::new(false),
            holds,
        })
    }

    pub fn disable(&self) {
        self.disabled.store(true, Ordering::SeqCst);
    }

    /// An accepted GOAWAY: the connection admits nothing more.
    pub fn going_away(&self) -> bool {
        self.goaway.load(Ordering::SeqCst) != NONE
    }

    fn record_goaway(&self, last: u32) -> bool {
        let current = self.goaway.load(Ordering::SeqCst);
        // h2 rejects a GOAWAY that raises the last stream id.
        if current != NONE && last > current {
            return false;
        }
        self.goaway.store(last, Ordering::SeqCst);
        true
    }

    fn record_headers(&self, stream: u32) {
        self.written.fetch_max(stream, Ordering::SeqCst);
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

    /// Whether a HEADERS frame for `stream` may have reached the socket.
    /// Waits only while h2 holds frames that a pending write left buffered.
    async fn sent(&self, stream: u32) -> bool {
        let flushes = self.flushes.load(Ordering::SeqCst);
        loop {
            let progress = self.progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            if !self.backlog.load(Ordering::SeqCst)
                || self.flushes.load(Ordering::SeqCst) != flushes
                || self.closed.load(Ordering::SeqCst)
            {
                break;
            }
            progress.await;
        }
        self.written.load(Ordering::SeqCst) >= stream
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
            // Spec hold point: armed with a zero bound, it counts the requests
            // hyper's dispatch task passed to h2.
            self.wire.holds.block("request:indexed");
            self.wire.next.fetch_add(1, Ordering::SeqCst)
        });
    }

    /// The peer never processed this request's stream (RFC 9113 6.8), so it
    /// may be replayed where D12 allows a refused stream to be.
    pub fn unprocessed(&self) -> bool {
        self.wire.unprocessed(self).is_some()
    }

    /// Never processed, and its HEADERS never reached the socket: a request
    /// that may be replayed on any call shape.
    pub async fn unsent(&self) -> bool {
        match self.wire.unprocessed(self) {
            Some(stream) => !self.wire.sent(stream).await,
            None => false,
        }
    }
}

/// Incremental frame-header reader for one direction.
struct Frames {
    /// Bytes of the client preface still to pass (write side only).
    preface: usize,
    header: [u8; HEAD],
    have: usize,
    /// Payload bytes of the current frame still to pass.
    remaining: usize,
    /// The current frame is a GOAWAY: its payload bytes seen so far.
    goaway: Option<([u8; 4], usize)>,
    first: bool,
    in_block: bool,
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
            first: true,
            in_block: false,
            broken: false,
        }
    }

    /// Server frames: records each GOAWAY h2 accepts once its payload passed.
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
                    self.end_goaway(wire);
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
                self.end_goaway(wire);
            }
        }
        if self.broken {
            wire.disable();
        }
    }

    fn server_header(&mut self) {
        let (length, kind, flags, stream) = parse(&self.header);
        // h2 fails the connection on each of these; the record stops there.
        let valid = length <= MAX_FRAME
            && (!self.first || kind == SETTINGS)
            && (!self.in_block || kind == CONTINUATION)
            && (kind != GOAWAY || (stream == 0 && length >= 8));
        if !valid {
            self.broken = true;
            return;
        }
        self.first = false;
        if matches!(kind, HEADERS | PUSH_PROMISE | CONTINUATION) {
            self.in_block = flags & END_HEADERS == 0;
        }
        self.goaway = (kind == GOAWAY).then_some(([0; 4], 0));
        self.remaining = length;
    }

    fn end_goaway(&mut self, wire: &Wire) {
        if let Some((last, _)) = self.goaway.take() {
            let last = u32::from_be_bytes(last) & 0x7fff_ffff;
            if !wire.record_goaway(last) {
                self.broken = true;
            }
        }
    }

    /// Client frames: records each new stream's HEADERS once its frame
    /// header was written.
    fn write(&mut self, wire: &Wire, mut bytes: &[u8]) {
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
                wire.record_headers(stream);
            }
            self.remaining = length;
        }
        if self.broken {
            wire.disable();
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

    fn wrote(&mut self, result: &Poll<io::Result<usize>>, bufs: &[io::IoSlice<'_>]) {
        match result {
            Poll::Ready(Ok(n)) => {
                let mut n = *n;
                for buf in bufs {
                    let take = n.min(buf.len());
                    self.write.write(&self.wire, &buf[..take]);
                    n -= take;
                    if n == 0 {
                        break;
                    }
                }
            }
            Poll::Pending => self.wire.backlog.store(true, Ordering::SeqCst),
            Poll::Ready(Err(_)) => {}
        }
    }
}

impl<T> Drop for Tap<T> {
    fn drop(&mut self) {
        self.wire.closed.store(true, Ordering::SeqCst);
        self.wire.progress.notify_waiters();
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
        let this = self.get_mut();
        // h2 flushes only with its own buffer empty: every frame it encoded
        // has passed `poll_write` above.
        this.wire.backlog.store(false, Ordering::SeqCst);
        this.wire.flushes.fetch_add(1, Ordering::SeqCst);
        this.wire.progress.notify_waiters();
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
