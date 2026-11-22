//! Blocking waits with the GVL released and an unblocking function (UBF).
//!
//! Ruby calls the UBF from another thread when it wants this thread back
//! (Thread#raise, Thread#kill, signals, VM shutdown, a fiber scheduler's
//! cancellation). The UBF only fires a signal; the wait returns, and pending
//! interrupts are delivered by `rb_thread_check_ints` under `rb_protect`, so
//! no longjmp crosses Rust frames. Rust never calls Ruby inside the body.
//! A call's body may run on a fiber scheduler's worker thread
//! (RB_NOGVL_OFFLOAD_SAFE, on Ruby 4.0 and later; see `FLAGS`), so
//! everything it touches is `Send`; the close wait always runs on the
//! calling thread (see `wait_signal`).
use std::cell::Cell;
use std::ffi::c_void;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use magnus::{Error, Module, Ruby};
use tokio::sync::Notify;

use crate::failure::Failure;
use crate::{holds, runtime};

thread_local! {
    /// Native operations driven by this Ruby thread, and the GVL releases
    /// among them; specs read both to prove the unary fast path is one
    /// operation that releases the GVL at most once. A ready result is taken
    /// without a release, so releases alone cannot count operations.
    static OPERATIONS: Cell<u64> = const { Cell::new(0) };
    static WAITS: Cell<u64> = const { Cell::new(0) };
}

pub fn wait_count() -> u64 {
    WAITS.with(Cell::get)
}

pub fn operation_count() -> u64 {
    OPERATIONS.with(Cell::get)
}

/// Waits with the GVL released right now, across threads and fibers; specs
/// poll it to know a call is parked even when a fiber scheduler offloaded it.
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

pub fn active_waits() -> usize {
    ACTIVE.load(Ordering::SeqCst)
}

/// Waits that resumed the same operation after a wake without a pending
/// Ruby interrupt, across threads; specs read it to see one blocked
/// operation re-enter its wait.
static RESUMED: AtomicUsize = AtomicUsize::new(0);

pub fn resumed_waits() -> usize {
    RESUMED.load(Ordering::SeqCst)
}

/// Wait bodies begun, and running now, across threads. A running body has
/// the GVL released and its unblocking function registered, so specs read
/// these to wake a wait that is really blocked rather than one between
/// bodies.
static ENTERED: AtomicUsize = AtomicUsize::new(0);
static RUNNING: AtomicUsize = AtomicUsize::new(0);

pub fn wait_bodies() -> (usize, usize) {
    (
        ENTERED.load(Ordering::SeqCst),
        RUNNING.load(Ordering::SeqCst),
    )
}

/// The `rb_nogvl` flags for a call's waits (`drive`), set once by
/// `init_flags`. The close wait never offloads (`DEFERRED_FLAGS`).
///
/// RB_NOGVL_INTR_FAIL: with an interrupt already pending, rb_nogvl returns
/// NULL without running the body or raising.
///
/// RB_NOGVL_OFFLOAD_SAFE lets a fiber scheduler with a worker pool run the
/// body on a worker, which `Data` and the pinned future on the waiting
/// thread's stack must outlive. Ruby 4.0's blocking-operation API (extract,
/// execute, cancel) lets io-event's worker pool finish a running body before
/// it re-raises a cancellation. Ruby 3.4 hands the body to the scheduler's
/// `blocking_operation_wait` as a plain proc (scheduler.c,
/// `rb_fiber_scheduler_blocking_operation_wait`), so a scheduler cancelled
/// while that body still runs could unwind the waiting frame under it. The
/// flag is passed only on Ruby 4.0 and later.
static FLAGS: AtomicI32 = AtomicI32::new(rb_sys::RB_NOGVL_INTR_FAIL as i32);

/// The `rb_nogvl` flags for the close wait: never RB_NOGVL_OFFLOAD_SAFE, so
/// its body runs on the calling Ruby thread. From a non-blocking fiber under
/// a fiber scheduler Ruby calls close from a helper thread
/// (`Transport#close`), so the wait takes neither the reactor thread nor one
/// of a worker pool's workers, which a sibling fiber's draining call may need.
const DEFERRED_FLAGS: i32 = rb_sys::RB_NOGVL_INTR_FAIL as i32;

/// Sets `FLAGS` from the running Ruby's version, not the headers the
/// extension was built against.
pub fn init_flags(ruby: &Ruby) -> Result<(), Error> {
    let version: String = ruby.class_object().const_get("RUBY_VERSION")?;
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .unwrap_or(0);
    let mut flags = rb_sys::RB_NOGVL_INTR_FAIL;
    if major >= 4 {
        flags |= rb_sys::RB_NOGVL_OFFLOAD_SAFE;
    }
    FLAGS.store(flags as i32, Ordering::SeqCst);
    Ok(())
}

struct Active;

impl Active {
    fn enter() -> Self {
        ACTIVE.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A one-shot wake-up signal that a UBF or another thread may fire.
#[derive(Default)]
pub struct Signal {
    fired: AtomicBool,
    notify: Notify,
}

impl Signal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn fire(&self) {
        self.fired.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    pub async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();
            if self.fired() {
                return;
            }
            notified.await;
        }
    }
}

struct Data<F, R> {
    func: Option<F>,
    out: Option<std::thread::Result<R>>,
}

unsafe extern "C" fn call<F: FnOnce() -> R, R>(data: *mut c_void) -> *mut c_void {
    // SAFETY: `data` points at the caller's `Data`, which outlives rb_nogvl;
    // only this body touches it while the wait runs.
    let data = unsafe { &mut *(data as *mut Data<F, R>) };
    let func = data.func.take().expect("nogvl body runs once");
    ENTERED.fetch_add(1, Ordering::SeqCst);
    RUNNING.fetch_add(1, Ordering::SeqCst);
    data.out = Some(catch_unwind(AssertUnwindSafe(func)));
    RUNNING.fetch_sub(1, Ordering::SeqCst);
    // Any non-null value tells the caller the body ran.
    std::ptr::dangling_mut::<c_void>()
}

unsafe extern "C" fn unblock(data: *mut c_void) {
    // SAFETY: `data` is the `Signal` the caller keeps alive across rb_nogvl.
    let signal = unsafe { &*(data as *const Signal) };
    signal.fire();
}

/// Runs `func` without the GVL. `interrupt` fires when Ruby wants the thread
/// back, and `func` must return promptly once it does. Pending Ruby
/// interrupts are raised (as a magnus Error) after `func` returns.
fn without_gvl<F, R>(ruby: &Ruby, interrupt: &Arc<Signal>, func: F) -> Result<R, Error>
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    WAITS.with(|waits| waits.set(waits.get() + 1));
    let _active = Active::enter();
    let mut data = Data {
        func: Some(func),
        out: None,
    };
    loop {
        let data_ptr = &mut data as *mut Data<F, R> as *mut c_void;
        let signal_ptr = Arc::as_ptr(interrupt) as *mut c_void;
        // See FLAGS. An offloading scheduler's cancellation re-raises through
        // rb_nogvl after draining the body, so rb_protect keeps that raise
        // from unwinding Rust frames.
        let flags = FLAGS.load(Ordering::SeqCst);
        let ran = magnus::rb_sys::protect(|| unsafe {
            rb_sys::rb_nogvl(
                Some(call::<F, R>),
                data_ptr,
                Some(unblock),
                signal_ptr,
                flags,
            ) as rb_sys::VALUE
        })?;
        if ran != 0 {
            break;
        }
        // Deliver the pending interrupt. A masked one (Thread.handle_interrupt)
        // clears the flag without raising, and the body runs next time.
        ruby.thread_check_ints()?;
    }
    let out = data.out.take().expect("nogvl body ran");
    ruby.thread_check_ints()?;
    match out {
        Ok(value) => Ok(value),
        Err(panic) => resume_unwind(panic),
    }
}

/// What ends a wait besides the operation itself.
pub struct Waits<'a> {
    /// The call's cancellation token, fired from Ruby's on_cancel.
    pub cancel: Option<&'a Arc<Signal>>,
    /// The exchange's abort.
    pub abort: Option<&'a Arc<Signal>>,
    /// The driver's forced teardown.
    pub shutdown: &'a Arc<Signal>,
    pub deadline: Option<Instant>,
}

enum Wake<T> {
    Done(T),
    Interrupted,
    Canceled,
    Aborted,
    Shutdown,
    Timeout,
}

/// Drives `future` to completion with the GVL released. The future lives in
/// this frame, so a wake without a pending Ruby interrupt resumes the same
/// operation state and never repeats a side effect. A ready result is taken
/// first without releasing the GVL, except while a spec hold point is armed:
/// one that parks the thread must not hold the GVL (see `holds`).
pub fn drive<T, F>(ruby: &Ruby, waits: &Waits<'_>, future: F) -> Result<Result<T, Failure>, Error>
where
    F: Future<Output = Result<T, Failure>> + Send,
    T: Send,
{
    OPERATIONS.with(|operations| operations.set(operations.get() + 1));
    let handle = runtime::handle();
    let mut future = pin!(future);
    if let Some(failure) = settled(waits) {
        return Ok(Err(failure));
    }
    if !holds::any_armed() {
        let _context = handle.enter();
        let mut cx = Context::from_waker(Waker::noop());
        if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
            return Ok(result);
        }
    }
    let never = Signal::new();
    let cancel = waits.cancel.unwrap_or(&never);
    let abort = waits.abort.unwrap_or(&never);
    let shutdown = waits.shutdown;
    let deadline = waits.deadline.map(tokio::time::Instant::from_std);
    loop {
        let interrupt = Signal::new();
        let wake = without_gvl(ruby, &interrupt, || {
            handle.block_on(async {
                let expiry = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = interrupt.wait() => Wake::Interrupted,
                    _ = abort.wait() => Wake::Aborted,
                    _ = shutdown.wait() => Wake::Shutdown,
                    _ = cancel.wait() => Wake::Canceled,
                    result = future.as_mut() => Wake::Done(result),
                    _ = expiry => Wake::Timeout,
                }
            })
        })?;
        return Ok(match wake {
            Wake::Interrupted => {
                RESUMED.fetch_add(1, Ordering::SeqCst);
                continue;
            }
            Wake::Done(result) => result,
            Wake::Canceled => Err(Failure::new("canceled", "call canceled")),
            Wake::Aborted => Err(Failure::new("aborted", "exchange aborted")),
            Wake::Shutdown => Err(Failure::shutdown()),
            Wake::Timeout => Err(Failure::new("timeout", "wait timed out")),
        });
    }
}

fn settled(waits: &Waits<'_>) -> Option<Failure> {
    if waits.abort.is_some_and(|abort| abort.fired()) {
        return Some(Failure::new("aborted", "exchange aborted"));
    }
    if waits.shutdown.fired() {
        return Some(Failure::shutdown());
    }
    if waits.cancel.is_some_and(|cancel| cancel.fired()) {
        return Some(Failure::new("canceled", "call canceled"));
    }
    if waits.deadline.is_some_and(|at| at <= Instant::now()) {
        return Some(Failure::new("timeout", "wait timed out"));
    }
    None
}

/// Waits with the GVL released until `signal` fires, for an outcome that a
/// runtime task publishes, such as a transport's teardown. A Ruby exception
/// raised meanwhile (a thread interrupt) does not end the wait: the first
/// one is held and returned once `signal` fired, for the caller to raise.
/// Every entry, re-entry after an interrupt included, waits on the calling
/// thread (`DEFERRED_FLAGS`); `Transport#close` holds a fiber scheduler's
/// cancellation, such as async's `Task#stop`, itself.
pub fn wait_signal(ruby: &Ruby, signal: &Arc<Signal>) -> Option<Error> {
    let handle = runtime::handle();
    let mut raised: Option<Error> = None;
    while !signal.fired() {
        let interrupt = Signal::new();
        let (_, error) = without_gvl_deferred(ruby, &interrupt, || {
            handle.block_on(async {
                tokio::select! {
                    biased;
                    _ = interrupt.wait() => {}
                    _ = signal.wait() => {}
                }
            })
        });
        if let Some(error) = error {
            raised.get_or_insert(error);
        }
    }
    raised
}

/// Like `without_gvl`, but never raises and never offloads: it returns the
/// body's result, if the body ran, and any Ruby exception raised around it,
/// for the caller to raise later. The body runs on the calling thread.
fn without_gvl_deferred<F, R>(
    ruby: &Ruby,
    interrupt: &Arc<Signal>,
    func: F,
) -> (Option<R>, Option<Error>)
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    let _active = Active::enter();
    let mut data = Data {
        func: Some(func),
        out: None,
    };
    loop {
        let data_ptr = &mut data as *mut Data<F, R> as *mut c_void;
        let signal_ptr = Arc::as_ptr(interrupt) as *mut c_void;
        // With RB_NOGVL_INTR_FAIL and no offload, rb_nogvl returns without
        // raising; rb_protect still keeps any raise from unwinding Rust
        // frames.
        let ran = magnus::rb_sys::protect(|| unsafe {
            rb_sys::rb_nogvl(
                Some(call::<F, R>),
                data_ptr,
                Some(unblock),
                signal_ptr,
                DEFERRED_FLAGS,
            ) as rb_sys::VALUE
        });
        match ran {
            Ok(0) => {}
            Ok(_) => return (ran_output(&mut data), None),
            Err(error) => return (ran_output(&mut data), Some(error)),
        }
        // An interrupt is pending. The caller masks thread interrupts around
        // close, so this usually only clears the flag; one that raises is
        // returned for the caller to hold.
        if let Err(error) = ruby.thread_check_ints() {
            return (None, Some(error));
        }
    }
}

/// The body's result, if it ran; a panic in the body resumes here.
fn ran_output<F, R>(data: &mut Data<F, R>) -> Option<R> {
    match data.out.take()? {
        Ok(value) => Some(value),
        Err(panic) => resume_unwind(panic),
    }
}
