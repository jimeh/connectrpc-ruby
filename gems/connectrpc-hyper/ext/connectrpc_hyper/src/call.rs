//! One streaming exchange. Opening waits for nothing: the first `write` or
//! `response_head` connects and dispatches. The send and receive halves are
//! independent (core allows one reader and one writer at once), each taken
//! out of its lock for the duration of an operation. `abort` fires a signal
//! that every wait and the request body observe, so it wins over busy
//! operations instead of waiting for them.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::client::conn::TrySendError;
use tokio::sync::{mpsc, oneshot};

use crate::body::CallBody;
use crate::failure::Failure;
use crate::gvl::Signal;
use crate::pool::{DriverCore, Inflight, Lease, RequestSpec};

pub type Sent = Pin<
    Box<
        dyn Future<
                Output = Result<hyper::Response<Incoming>, TrySendError<hyper::Request<CallBody>>>,
            > + Send,
    >,
>;

/// Enqueues the request on the leased connection now; the returned future
/// resolves with the response head or hands back an unsent request.
pub fn send(lease: &mut Lease, request: hyper::Request<CallBody>) -> Sent {
    match lease {
        Lease::H2 { sender, .. } => Box::pin(sender.try_send_request(request)),
        Lease::H1 { conn, .. } => {
            let conn = conn.as_mut().expect("HTTP/1.1 lease holds its connection");
            Box::pin(conn.sender.try_send_request(request))
        }
    }
}

type Delivered = Result<(hyper::Response<Incoming>, Lease), Failure>;

enum Dispatch {
    Pending,
    Dispatched(Option<oneshot::Receiver<Delivered>>),
    Failed(Failure),
}

pub struct Head {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub end_stream: bool,
}

#[derive(Default)]
pub struct RecvHalf {
    pending: Option<oneshot::Receiver<Delivered>>,
    pub head: Option<Head>,
    body: Option<Incoming>,
    buffer: Bytes,
    pub trailers: Option<http::HeaderMap>,
    done: bool,
}

pub struct SendHalf {
    tx: Option<mpsc::Sender<Bytes>>,
}

pub struct CallState {
    core: Arc<DriverCore>,
    spec: Arc<RequestSpec>,
    require_h2: bool,
    pub deadline: Option<Instant>,
    pub cancel: Arc<Signal>,
    pub abort: Arc<Signal>,
    aborted: AtomicBool,
    finished: AtomicBool,
    failed: AtomicBool,
    /// True while a request is queued (see `Failure::forced`); a retry of a
    /// request handed back unsent clears it until it queues again.
    pub dispatched: Arc<AtomicBool>,
    ended: Arc<AtomicBool>,
    body: Mutex<Option<mpsc::Receiver<Bytes>>>,
    dispatch: tokio::sync::Mutex<Dispatch>,
    pub send: Mutex<Option<SendHalf>>,
    pub recv: Mutex<Option<RecvHalf>>,
    lease: Mutex<Option<Lease>>,
    inflight: Mutex<Option<Inflight>>,
    local_port: AtomicU16,
    h2: AtomicBool,
}

impl CallState {
    pub fn new(
        core: Arc<DriverCore>,
        spec: RequestSpec,
        require_h2: bool,
        deadline: Option<Instant>,
        cancel: Arc<Signal>,
        inflight: Inflight,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(1);
        Arc::new(Self {
            core,
            spec: Arc::new(spec),
            require_h2,
            deadline,
            cancel,
            abort: Signal::new(),
            aborted: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            dispatched: Arc::new(AtomicBool::new(false)),
            ended: Arc::new(AtomicBool::new(false)),
            body: Mutex::new(Some(rx)),
            dispatch: tokio::sync::Mutex::new(Dispatch::Pending),
            send: Mutex::new(Some(SendHalf { tx: Some(tx) })),
            recv: Mutex::new(Some(RecvHalf::default())),
            lease: Mutex::new(None),
            inflight: Mutex::new(Some(inflight)),
            local_port: AtomicU16::new(0),
            h2: AtomicBool::new(false),
        })
    }

    pub fn shutdown(&self) -> &Arc<Signal> {
        &self.core.shutdown
    }

    pub fn local_port(&self) -> u16 {
        self.local_port.load(Ordering::SeqCst)
    }

    pub fn is_h2(&self) -> bool {
        self.h2.load(Ordering::SeqCst)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst) || self.aborted.load(Ordering::SeqCst)
    }

    /// Returns a half after an operation, unless the call was aborted
    /// meanwhile; then the half is dropped here instead.
    pub fn restore<T>(&self, slot: &Mutex<Option<T>>, half: T) {
        let mut guard = slot.lock().unwrap_or_else(|e| e.into_inner());
        if !self.aborted.load(Ordering::SeqCst) {
            *guard = Some(half);
        }
    }

    pub fn mark_failed(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }

    async fn dispatch(self: &Arc<Self>) -> Result<(), Failure> {
        let mut state = self.dispatch.lock().await;
        match &*state {
            Dispatch::Pending => {}
            Dispatch::Dispatched(_) => return Ok(()),
            Dispatch::Failed(failure) => return Err(failure.clone()),
        }
        let mut lease = match self
            .core
            .acquire(&self.spec.origin, self.require_h2, None, self.deadline)
            .await
        {
            Ok(lease) => lease,
            Err(failure) => {
                *state = Dispatch::Failed(failure.clone());
                return Err(failure);
            }
        };
        if self.aborted.load(Ordering::SeqCst) {
            return Err(Failure::new("aborted", "exchange aborted"));
        }
        // From here to the end nothing awaits: the request is queued exactly
        // once, even if this operation is woken and resumed.
        let rx = self
            .body
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("request body is dispatched once");
        let body = CallBody::stream(rx, self.ended.clone(), self.abort.clone());
        let request = match self.spec.build(body, self.deadline, lease.is_h2()) {
            Ok(request) => request,
            Err(failure) => {
                *state = Dispatch::Failed(failure.clone());
                return Err(failure);
            }
        };
        self.local_port.store(lease.local_port(), Ordering::SeqCst);
        self.h2.store(lease.is_h2(), Ordering::SeqCst);
        self.dispatched.store(true, Ordering::SeqCst);
        let sent = send(&mut lease, request);
        let (deliver, delivered) = oneshot::channel();
        self.core.tracker.spawn(respond(
            self.core.clone(),
            self.spec.clone(),
            self.require_h2,
            self.deadline,
            lease,
            sent,
            self.abort.clone(),
            self.dispatched.clone(),
            deliver,
        ));
        *state = Dispatch::Dispatched(Some(delivered));
        Ok(())
    }

    pub async fn head(self: &Arc<Self>, recv: &mut RecvHalf) -> Result<(), Failure> {
        if recv.head.is_some() {
            return Ok(());
        }
        if recv.pending.is_none() {
            self.dispatch().await?;
            let mut state = self.dispatch.lock().await;
            if let Dispatch::Dispatched(delivered) = &mut *state {
                recv.pending = delivered.take();
            }
        }
        let pending = recv
            .pending
            .as_mut()
            .ok_or_else(|| Failure::new("state", "response head already taken"))?;
        let delivered = pending.await;
        recv.pending = None;
        let (response, lease) =
            delivered.map_err(|_| Failure::new("aborted", "exchange aborted"))??;
        {
            // abort sets `aborted` before it takes the lease, so checking under
            // the lease lock means an abort that already ran drops the lease
            // here instead of leaving it held until the call is collected.
            let mut slot = self.lease.lock().unwrap_or_else(|e| e.into_inner());
            if self.aborted.load(Ordering::SeqCst) {
                drop(lease);
                return Err(Failure::new("aborted", "exchange aborted"));
            }
            *slot = Some(lease);
        }
        let (parts, body) = response.into_parts();
        // HEADERS carried END_STREAM: a gRPC trailers-only response. h2 keeps
        // trailers that arrived right behind the head in its queue, so this is
        // false when the status came in a separate trailing HEADERS frame.
        let end_stream = http_body::Body::is_end_stream(&body);
        recv.done = end_stream;
        recv.head = Some(Head {
            status: parts.status.as_u16(),
            headers: parts.headers,
            end_stream,
        });
        recv.body = Some(body);
        Ok(())
    }

    pub async fn read(
        self: &Arc<Self>,
        recv: &mut RecvHalf,
        max: usize,
    ) -> Result<Option<Bytes>, Failure> {
        self.head(recv).await?;
        loop {
            if !recv.buffer.is_empty() {
                let take = max.max(1).min(recv.buffer.len());
                return Ok(Some(recv.buffer.split_to(take)));
            }
            if recv.done {
                return Ok(None);
            }
            let body = recv.body.as_mut().expect("body after head");
            match body.frame().await {
                None => recv.done = true,
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => recv.buffer = data,
                    Err(frame) => {
                        if let Ok(trailers) = frame.into_trailers() {
                            recv.trailers = Some(trailers);
                        }
                    }
                },
                Some(Err(error)) => return Err(Failure::from_hyper(&error)),
            }
        }
    }

    /// Queues `bytes` once, then returns when hyper has pulled them, which it
    /// does only while the stream has send window: a peer that does not read
    /// blocks the writer.
    pub async fn write(self: &Arc<Self>, send: &mut SendHalf, bytes: Bytes) -> Result<(), Failure> {
        self.dispatch().await?;
        let Some(tx) = send.tx.as_ref() else {
            return Err(Failure::new("state", "request body already closed"));
        };
        // A closed channel means hyper dropped the request body: the stream
        // ended or was reset, and the response side reports why.
        match tx.reserve().await {
            Ok(permit) => permit.send(bytes),
            Err(_) => return Ok(()),
        }
        let _ = tx.reserve().await;
        Ok(())
    }

    pub fn close_send(&self) {
        if let Some(half) = self.send.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            half.tx = None;
        }
    }

    pub async fn drain(self: &Arc<Self>, recv: &mut RecvHalf) -> Result<(), Failure> {
        while self.read(recv, 64 * 1024).await?.is_some() {}
        Ok(())
    }

    /// After a clean drain: an HTTP/1.1 socket returns to the pool only when
    /// the request body also ended cleanly.
    pub fn complete(&self) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }
        let lease = self.lease.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut lease) = lease {
            if self.ended.load(Ordering::SeqCst) && !self.failed.load(Ordering::SeqCst) {
                self.core.release_h1(&mut lease);
            }
        }
        drop(self.send.lock().unwrap_or_else(|e| e.into_inner()).take());
        // The head and trailers stay readable; the h2 stream handle goes, so
        // nothing holds the connection open after the call.
        if let Some(recv) = self.recv.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            recv.body = None;
            recv.pending = None;
        }
        drop(
            self.inflight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );
    }

    /// Drops the stream: RST_STREAM(CANCEL) on HTTP/2, a closed socket on
    /// HTTP/1.1. Never waits, so it is safe in a masked ensure.
    pub fn abort(&self) {
        if self.finished.load(Ordering::SeqCst) || self.aborted.swap(true, Ordering::SeqCst) {
            return;
        }
        // Signal first: the request body then fails with CANCEL instead of
        // ending cleanly when its sender drops below.
        self.abort.fire();
        drop(self.send.lock().unwrap_or_else(|e| e.into_inner()).take());
        drop(self.recv.lock().unwrap_or_else(|e| e.into_inner()).take());
        drop(self.lease.lock().unwrap_or_else(|e| e.into_inner()).take());
        drop(
            self.inflight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );
    }
}

/// Owns the response future so an unsent request can be retried once on a
/// fresh connection even while the caller is blocked in a write.
#[allow(clippy::too_many_arguments)]
async fn respond(
    core: Arc<DriverCore>,
    spec: Arc<RequestSpec>,
    require_h2: bool,
    deadline: Option<Instant>,
    lease: Lease,
    sent: Sent,
    abort: Arc<Signal>,
    dispatched: Arc<AtomicBool>,
    deliver: oneshot::Sender<Delivered>,
) {
    let work = async {
        match sent.await {
            Ok(response) => Ok((response, lease)),
            Err(mut error) => {
                let Some(request) = error.take_message() else {
                    return Err(Failure::from_hyper(error.error()));
                };
                // Handed back unsent: the peer never saw it.
                let exclude = lease.id();
                drop(lease);
                dispatched.store(false, Ordering::SeqCst);
                let mut lease = core
                    .acquire(&spec.origin, require_h2, Some(exclude), deadline)
                    .await?;
                let request = spec.build(request.into_body(), deadline, lease.is_h2())?;
                dispatched.store(true, Ordering::SeqCst);
                match send(&mut lease, request).await {
                    Ok(response) => Ok((response, lease)),
                    Err(error) => Err(Failure::from_hyper(error.error())),
                }
            }
        }
    };
    let delivered = tokio::select! {
        result = work => result,
        _ = abort.wait() => return,
        _ = core.shutdown.wait() => Err(Failure::forced(dispatched.load(Ordering::SeqCst))),
    };
    let _ = deliver.send(delivered);
}
