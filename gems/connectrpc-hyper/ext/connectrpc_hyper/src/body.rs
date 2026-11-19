//! Request bodies: nothing, a fixed buffer, or an incrementally written
//! stream. An aborted stream fails with CANCEL instead of ending, so an
//! interrupted upload reaches the peer as RST_STREAM(CANCEL) (HTTP/2) or a
//! broken connection (HTTP/1.1), never as a clean end of the request.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

use crate::gvl::Signal;

type AbortWait = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

pub enum CallBody {
    Empty,
    Full(Option<Bytes>),
    Stream {
        rx: mpsc::Receiver<Bytes>,
        /// Set once the body ended cleanly (the writer closed its side).
        ended: Arc<AtomicBool>,
        abort: AbortWait,
    },
}

impl CallBody {
    pub fn stream(rx: mpsc::Receiver<Bytes>, ended: Arc<AtomicBool>, abort: Arc<Signal>) -> Self {
        CallBody::Stream {
            rx,
            ended,
            abort: Box::pin(async move { abort.wait().await }),
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
        match self.get_mut() {
            CallBody::Empty => Poll::Ready(None),
            CallBody::Full(data) => Poll::Ready(data.take().map(|bytes| Ok(Frame::data(bytes)))),
            CallBody::Stream { rx, ended, abort } => {
                // Abort wins over queued bytes and over the writer's close.
                if abort.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Some(Err(h2::Error::from(h2::Reason::CANCEL))));
                }
                match rx.poll_recv(cx) {
                    Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                    Poll::Ready(None) => {
                        ended.store(true, Ordering::SeqCst);
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(self, CallBody::Empty | CallBody::Full(None))
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            CallBody::Empty | CallBody::Full(None) => SizeHint::with_exact(0),
            CallBody::Full(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            CallBody::Stream { .. } => SizeHint::default(),
        }
    }
}
