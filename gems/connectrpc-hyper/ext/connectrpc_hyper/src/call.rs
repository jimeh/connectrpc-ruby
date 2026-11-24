//! One streaming exchange. Opening waits for nothing: the first `write` or
//! `response_head` connects and dispatches. The send and receive halves are
//! independent (core allows one reader and one writer at once), each taken
//! out of its lock for the duration of an operation. `abort` fires a signal
//! that every wait and the request body observe, so it wins over busy
//! operations instead of waiting for them.
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::client::conn::TrySendError;
use tokio::sync::{mpsc, oneshot};

use crate::body::CallBody;
use crate::failure::{Failure, TlsFault};
use crate::gvl::Signal;
use crate::holds::Holds;
use crate::pool::{DriverCore, Inflight, Lease, RequestSpec};
use crate::wire::Mark;

/// Why a request got no response head.
pub enum Unanswered<E> {
    /// hyper's failure, which carries the request if hyper handed it back.
    Failed(E),
    /// The connection's dispatcher ended without ever taking the request.
    Stranded,
}

type Response = hyper::Response<Incoming>;
type HyperError = TrySendError<hyper::Request<CallBody>>;
type Promise = Pin<Box<dyn Future<Output = Result<Response, HyperError>> + Send>>;
pub type Sent = Pin<Box<dyn Future<Output = Result<Response, Unanswered<HyperError>>> + Send>>;

/// Enqueues the request on the leased connection now; the returned future
/// resolves with the response head, hands back an unsent request, or reports
/// it stranded. On HTTP/2 the mark learns the request's stream id (see
/// wire.rs).
///
/// Stranded: tokio's unbounded `send` checks that the channel is open, then
/// pushes, while the receiver's drop closes the channel, then drains what it
/// can read (tokio-rs/tokio#7714). A request pushed between the two is
/// neither drained nor ever received, and its envelope, whose drop would
/// answer the caller, lives until the connection's last sender drops: the
/// lease, the pool and sibling leases hold those. Since hyper 1.13.0
/// (hyperium/hyper#4222, #4242) a dispatcher that shuts down closes its
/// channel and waits for such a push before its future completes, so this
/// guards a future dropped unfinished (an aborted task), an HTTP/1.1 upgrade,
/// which completes without that drain, and any other path to that race. The
/// answer races the connection's `closed` signal, which fires only after
/// hyper's dispatcher was dropped and so answered every request it held (see
/// `conn::Dispatcher`).
pub fn send(
    lease: &mut Lease,
    request: hyper::Request<CallBody>,
    holds: &Holds,
) -> (Sent, Option<Arc<Mark>>) {
    if request.method() == http::Method::CONNECT {
        // hyper may drop a CONNECT request after taking its index.
        lease.disable_wire();
    }
    let mark = lease.mark();
    let closed = lease
        .closed()
        .expect("a lease holds its connection")
        .clone();
    let http1 = !lease.is_h2();
    // Spec hold point: armed, it counts the request and keeps it, body
    // included, until the point is released, never handing it to hyper, as a
    // stranded envelope stays in the channel while a sender lives. The kept
    // request carries no mark: nothing indexes it, and a mark would tie the
    // driver's holds to themselves (mark, wire, holds) if the driver were
    // dropped while the point is armed.
    let promise: Promise = match holds.keep("request:strand", request) {
        None => Box::pin(std::future::pending()),
        Some(mut request) => {
            request.body_mut().set_mark(mark.clone());
            match lease {
                Lease::H2 { sender, .. } => Box::pin(sender.try_send_request(request)),
                Lease::H1 { conn, .. } => {
                    let conn = conn.as_mut().expect("HTTP/1.1 lease holds its connection");
                    Box::pin(conn.sender.try_send_request(request))
                }
            }
        }
    };
    let taken = mark.clone();
    // hyper's HTTP/1.1 dispatcher answers every request it took when it is
    // dropped. hyper's HTTP/2 dispatcher indexes each request it takes before
    // passing it to h2 (wire.rs), and an indexed request belongs to h2 and its
    // response task, which may still answer it (a graceful GOAWAY drains).
    let stranded = move || http1 || !taken.as_ref().is_some_and(|mark| mark.indexed());
    (Box::pin(answered(promise, closed, stranded)), mark)
}

/// `promise`'s answer, or `Stranded` when `closed` has fired, `promise` is
/// still pending and `stranded` says nobody else can answer it.
async fn answered<T, E>(
    promise: impl Future<Output = Result<T, E>>,
    closed: Arc<Signal>,
    stranded: impl FnOnce() -> bool,
) -> Result<T, Unanswered<E>> {
    let mut promise = pin!(promise);
    tokio::select! {
        biased;
        answer = promise.as_mut() => return answer.map_err(Unanswered::Failed),
        () = closed.wait() => {}
    }
    // The dispatcher answered every request it held before `closed` fired
    // (SeqCst), so such an answer is ready now. Polled again because the first
    // poll may have come before it, and unconstrained because tokio's oneshot
    // checks the task's cooperative budget before its state: an exhausted
    // budget must not read as pending.
    let again = tokio::task::unconstrained(std::future::poll_fn(|cx| {
        Poll::Ready(promise.as_mut().poll(cx))
    }))
    .await;
    if let Poll::Ready(answer) = again {
        return answer.map_err(Unanswered::Failed);
    }
    if stranded() {
        return Err(Unanswered::Stranded);
    }
    promise.await.map_err(Unanswered::Failed)
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
    /// The response's connection's TLS failure record, for body errors.
    fault: Arc<TlsFault>,
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
    /// Fires when the request was stranded: its body, which owns the upload
    /// channel's receiver, may stay alive, so a blocked `write` returns.
    stranded: Arc<Signal>,
    aborted: AtomicBool,
    finished: AtomicBool,
    failed: AtomicBool,
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
            stranded: Signal::new(),
            aborted: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            failed: AtomicBool::new(false),
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
        let (home, homed) = oneshot::channel();
        let body = CallBody::stream(rx, self.ended.clone(), self.abort.clone(), home);
        let request = match self.spec.build(body, self.deadline, lease.is_h2()) {
            Ok(request) => request,
            Err(failure) => {
                *state = Dispatch::Failed(failure.clone());
                return Err(failure);
            }
        };
        self.local_port.store(lease.local_port(), Ordering::SeqCst);
        self.h2.store(lease.is_h2(), Ordering::SeqCst);
        let (sent, mark) = send(&mut lease, request, &self.core.holds);
        // Spec hold point: armed with a zero bound, it counts the requests
        // handed to hyper. `block` keeps this section free of awaits.
        self.core.holds.block("request:queued");
        let (deliver, delivered) = oneshot::channel();
        self.core.tracker.spawn(respond(
            self.core.clone(),
            self.spec.clone(),
            Attempt {
                require_h2: self.require_h2,
                deadline: self.deadline,
                ended: self.ended.clone(),
                lease,
                sent,
                mark,
                homed,
            },
            self.abort.clone(),
            self.cancel.clone(),
            self.stranded.clone(),
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
        // `respond` ends without delivering only on this call's abort, or
        // when forced teardown aborted it or a sealed tracker refused it.
        let (response, lease) = delivered.map_err(|_| {
            if self.abort.fired() {
                Failure::new("aborted", "exchange aborted")
            } else {
                Failure::shutdown()
            }
        })??;
        {
            // abort sets `aborted` before it takes the lease, so checking under
            // the lease lock means an abort that already ran drops the lease
            // here instead of leaving it held until the call is collected.
            let mut slot = self.lease.lock().unwrap_or_else(|e| e.into_inner());
            if self.aborted.load(Ordering::SeqCst) {
                drop(lease);
                return Err(Failure::new("aborted", "exchange aborted"));
            }
            if let Some(fault) = lease.fault() {
                recv.fault = fault.clone();
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
                Some(Err(error)) => return Err(recv.fault.failure(&error)),
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
        // ended or was reset, and the response side reports why. So does a
        // stranded request, whose body may never be dropped or read.
        let reserved = tokio::select! {
            reserved = tx.reserve() => reserved,
            () = self.stranded.wait() => return Ok(()),
        };
        match reserved {
            Ok(permit) => permit.send(bytes),
            Err(_) => return Ok(()),
        }
        tokio::select! {
            _ = tx.reserve() => {}
            () = self.stranded.wait() => {}
        }
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

/// The first dispatch of a streaming call, as `respond` retries it.
struct Attempt {
    require_h2: bool,
    deadline: Option<Instant>,
    ended: Arc<AtomicBool>,
    lease: Lease,
    sent: Sent,
    mark: Option<Arc<Mark>>,
    /// The request body's receiver, back if hyper dropped the body unread.
    homed: oneshot::Receiver<mpsc::Receiver<Bytes>>,
}

/// Owns the response future so an unsent request can be retried once on a
/// fresh connection even while the caller is blocked in a write: one hyper
/// handed back, or one the peer's GOAWAY says it never processed whose
/// HEADERS never reached the socket and whose body yielded nothing. A
/// stranded request (see `send`) fails with its connection's failure: its
/// body cannot be recovered, and `stranded` releases a blocked writer.
async fn respond(
    core: Arc<DriverCore>,
    spec: Arc<RequestSpec>,
    attempt: Attempt,
    abort: Arc<Signal>,
    cancel: Arc<Signal>,
    stranded: Arc<Signal>,
    deliver: oneshot::Sender<Delivered>,
) {
    let Attempt {
        require_h2,
        deadline,
        ended,
        lease,
        sent,
        mark,
        homed,
    } = attempt;
    let strand = |lease: &Lease| {
        stranded.fire();
        lease.closed_failure()
    };
    let work = async {
        let mut error = match sent.await {
            Ok(response) => return Ok((response, lease)),
            Err(Unanswered::Failed(error)) => error,
            Err(Unanswered::Stranded) => return Err(strand(&lease)),
        };
        // A call canceled before its replacement is queued never sends it.
        let retry = async {
            let body = match error.take_message() {
                // Handed back unsent: the peer never saw it. Spec hold point:
                // it parks, or with a zero bound counts, these retries.
                Some(request) => {
                    core.holds.reach("retry:unsent").await;
                    request.into_body()
                }
                None => {
                    let failure = lease.failure(error.error());
                    let unsent = mark.as_ref().is_some_and(|mark| mark.unsent());
                    // hyper drops the body around the failure it reports; a
                    // body that yielded a frame never comes back.
                    let rx = match unsent {
                        true => until(deadline, homed).await.and_then(Result::ok),
                        false => None,
                    };
                    let Some(rx) = rx else {
                        return Err(failure);
                    };
                    // Spec hold point: it parks, or with a zero bound counts,
                    // these retries.
                    core.holds.reach("retry:unprocessed").await;
                    // Retried at most once: this body has no receiver to return.
                    CallBody::stream(rx, ended, abort.clone(), oneshot::channel().0)
                }
            };
            let exclude = lease.id();
            drop(lease);
            let mut lease = core
                .acquire(&spec.origin, require_h2, Some(exclude), deadline)
                .await?;
            let request = spec.build(body, deadline, lease.is_h2())?;
            let (sent, _) = send(&mut lease, request, &core.holds);
            Ok((lease, sent))
        };
        let (lease, sent) = tokio::select! {
            biased;
            _ = cancel.wait() => return Err(Failure::new("canceled", "call canceled")),
            retried = retry => retried?,
        };
        match sent.await {
            Ok(response) => Ok((response, lease)),
            Err(Unanswered::Failed(error)) => Err(lease.failure(error.error())),
            Err(Unanswered::Stranded) => Err(strand(&lease)),
        }
    };
    let delivered = tokio::select! {
        result = work => result,
        _ = abort.wait() => return,
        _ = core.shutdown.wait() => Err(Failure::shutdown()),
    };
    let _ = deliver.send(delivered);
}

/// `future`'s output, or None once `deadline` passes.
async fn until<F: Future>(deadline: Option<Instant>, future: F) -> Option<F::Output> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline.into(), future).await.ok(),
        None => Some(future.await),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pending at its first poll, which spends the task's whole cooperative
    /// budget; then a tokio oneshot that already holds its value, which
    /// checks the budget before its state, as hyper's promise does.
    struct Exhausting {
        first: bool,
        rx: oneshot::Receiver<u8>,
    }

    impl Future for Exhausting {
        type Output = Result<u8, ()>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
            if std::mem::take(&mut self.first) {
                while pin!(tokio::task::consume_budget()).poll(cx).is_ready() {}
                return Poll::Pending;
            }
            Pin::new(&mut self.rx)
                .poll(cx)
                .map(|answer| answer.map_err(|_| ()))
        }
    }

    fn fired() -> Arc<Signal> {
        let signal = Signal::new();
        signal.fire();
        signal
    }

    #[test]
    fn an_answer_ready_when_closed_fires_wins_under_an_exhausted_budget() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let (tx, rx) = oneshot::channel();
        tx.send(7).expect("receiver open");
        let promise = Exhausting { first: true, rx };
        let answer = runtime.block_on(answered(promise, fired(), || true));
        assert!(matches!(answer, Ok(7)));
    }

    #[test]
    fn a_pending_answer_is_stranded_only_when_nobody_else_can_answer() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let stranded = runtime.block_on(answered(
            std::future::pending::<Result<u8, ()>>(),
            fired(),
            || true,
        ));
        assert!(matches!(stranded, Err(Unanswered::Stranded)));
        let (tx, rx) = oneshot::channel();
        let answer = answered(async { rx.await.map_err(|_| ()) }, fired(), || false);
        let mut answer = pin!(answer);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        runtime.block_on(async {
            assert!(answer.as_mut().poll(&mut cx).is_pending());
            tx.send(9).expect("receiver open");
            assert!(matches!(answer.as_mut().poll(&mut cx), Poll::Ready(Ok(9))));
        });
    }
}
