//! Request bodies: nothing, a fixed buffer, or an incrementally written
//! stream. An aborted stream fails with CANCEL instead of ending, so an
//! interrupted upload reaches the peer as RST_STREAM(CANCEL) (HTTP/2) or a
//! broken connection (HTTP/1.1), never as a clean end of the request. An
//! HTTP/2 stream whose response ended while the upload was open fails with
//! NO_ERROR (`Upload`).
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tokio::sync::{mpsc, oneshot};

use crate::gvl::Signal;
use crate::wire::Mark;

type AbortWait = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

const OPEN: u8 = 0;
const CLOSED: u8 = 1;
const RESET: u8 = 2;

/// How an HTTP/2 upload ends, shared by a streaming call and its request
/// bodies. Whichever comes first decides: the writer closing its side
/// (`close`) commits a clean END_STREAM after the queued bytes; the
/// transport observing the response's end (`end_response`) while the upload
/// is open resets the stream with NO_ERROR and drops the unsent bytes, as
/// grpc-go's client resets a stream it still sends on once the server ended
/// it. hyper's pipe resets the stream with the reason of the body's error.
#[derive(Default)]
pub struct Upload {
    state: AtomicU8,
    response_ended: Signal,
}

impl Upload {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The writer closed its side.
    pub fn close(&self) {
        let _ = self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// The transport observed the response's end.
    pub fn end_response(&self) {
        let _ = self
            .state
            .compare_exchange(OPEN, RESET, Ordering::SeqCst, Ordering::SeqCst);
        self.response_ended.fire();
    }

    pub fn response_ended(&self) -> bool {
        self.response_ended.fired()
    }

    pub async fn wait(&self) {
        self.response_ended.wait().await;
    }

    fn is_reset(&self) -> bool {
        self.state.load(Ordering::SeqCst) == RESET
    }
}

/// A body's hold on its call's `Upload`, with a wait that wakes hyper's pipe
/// once the response ended.
struct UploadEnd {
    upload: Arc<Upload>,
    wait: Option<AbortWait>,
}

pub struct CallBody {
    kind: Kind,
    /// The request's place in its HTTP/2 connection's stream order.
    mark: Option<Arc<Mark>>,
}

enum Kind {
    Empty,
    Full(Option<Bytes>),
    Stream {
        rx: Option<mpsc::Receiver<Bytes>>,
        /// Set once the body ended cleanly (the writer closed its side).
        ended: Arc<AtomicBool>,
        abort: AbortWait,
        /// Takes the receiver back when hyper drops a body that never
        /// yielded a frame, so a replay can carry the writer's bytes.
        home: Option<oneshot::Sender<mpsc::Receiver<Bytes>>>,
        /// HTTP/2 only.
        upload: Option<UploadEnd>,
        yielded: bool,
    },
}

impl CallBody {
    pub fn empty() -> Self {
        Self::from(Kind::Empty)
    }

    pub fn full(bytes: Bytes) -> Self {
        Self::from(Kind::Full(Some(bytes)))
    }

    pub fn stream(
        rx: mpsc::Receiver<Bytes>,
        ended: Arc<AtomicBool>,
        abort: Arc<Signal>,
        home: oneshot::Sender<mpsc::Receiver<Bytes>>,
        upload: Option<Arc<Upload>>,
    ) -> Self {
        let upload = upload.map(|upload| UploadEnd {
            wait: Some(Box::pin({
                let upload = upload.clone();
                async move { upload.wait().await }
            })),
            upload,
        });
        Self::from(Kind::Stream {
            rx: Some(rx),
            ended,
            abort: Box::pin(async move { abort.wait().await }),
            home: Some(home),
            upload,
            yielded: false,
        })
    }

    fn from(kind: Kind) -> Self {
        Self { kind, mark: None }
    }

    pub fn set_mark(&mut self, mark: Option<Arc<Mark>>) {
        self.mark = mark;
    }
}

impl Drop for CallBody {
    fn drop(&mut self) {
        if let Kind::Stream {
            rx, home, yielded, ..
        } = &mut self.kind
        {
            if let (false, Some(rx), Some(home)) = (*yielded, rx.take(), home.take()) {
                // Nobody waiting closes the channel, as dropping it would.
                let _ = home.send(rx);
            }
        }
    }
}

impl Body for CallBody {
    type Data = Bytes;
    type Error = h2::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, h2::Error>>> {
        match &mut self.get_mut().kind {
            Kind::Empty => Poll::Ready(None),
            Kind::Full(data) => Poll::Ready(data.take().map(|bytes| Ok(Frame::data(bytes)))),
            Kind::Stream {
                rx,
                ended,
                abort,
                upload,
                yielded,
                ..
            } => {
                // Abort wins over queued bytes and over the writer's close.
                if abort.as_mut().poll(cx).is_ready() {
                    *yielded = true;
                    return Poll::Ready(Some(Err(h2::Error::from(h2::Reason::CANCEL))));
                }
                // A response that ended the open upload wins over its queued
                // bytes; the reset is never a clean end (`ended`).
                if let Some(end) = upload {
                    if end
                        .wait
                        .as_mut()
                        .is_some_and(|wait| wait.as_mut().poll(cx).is_ready())
                    {
                        end.wait = None;
                    }
                    if end.upload.is_reset() {
                        *yielded = true;
                        return Poll::Ready(Some(Err(h2::Error::from(h2::Reason::NO_ERROR))));
                    }
                }
                let rx = rx.as_mut().expect("receiver until drop");
                let polled = match rx.poll_recv(cx) {
                    Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                    Poll::Ready(None) => {
                        ended.store(true, Ordering::SeqCst);
                        Poll::Ready(None)
                    }
                    Poll::Pending => return Poll::Pending,
                };
                *yielded = true;
                polled
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self.kind, Kind::Empty | Kind::Full(None))
    }

    fn size_hint(&self) -> SizeHint {
        // hyper's HTTP/2 dispatch calls this once per request, right before
        // h2 assigns its stream id (see wire.rs).
        if let Some(mark) = &self.mark {
            mark.take_index();
        }
        match &self.kind {
            Kind::Empty | Kind::Full(None) => SizeHint::with_exact(0),
            Kind::Full(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            Kind::Stream { .. } => SizeHint::default(),
        }
    }
}
