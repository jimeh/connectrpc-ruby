//! The process runtime, fork guards, the fork-safe socket registry and the
//! task tracker every driver spawns through.
//!
//! Fork: a child has none of the parent's runtime threads, and any lock or
//! waker may have been mid-update at fork time. Nothing created by the parent
//! runtime is ever dropped or used in a child: `ForkGuard` leaks it, the Ruby
//! transport builds a new driver, and the runtime slot is replaced. No state
//! here is guarded by a lock a fork can inherit.
use std::collections::HashMap;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

pub fn pid() -> u32 {
    std::process::id()
}

struct Slot {
    pid: u32,
    runtime: Runtime,
}

/// The current process's runtime, or a leaked parent's, or null.
static RUNTIME: AtomicPtr<Slot> = AtomicPtr::new(ptr::null_mut());

/// The process runtime, created on first use (never when a transport is
/// built) and recreated in a forked child. Concurrent cold callers each build
/// one; the compare-and-swap elects a winner and losers shut theirs down.
pub fn handle() -> Handle {
    let me = pid();
    let mut current = RUNTIME.load(Ordering::Acquire);
    loop {
        // SAFETY: a published slot is never freed: slots are leaked for the
        // life of the process, including inherited ones in a child.
        if let Some(slot) = unsafe { current.as_ref() } {
            if slot.pid == me {
                return slot.runtime.handle().clone();
            }
        }
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("hyper-runtime")
            .enable_all()
            .build()
            .expect("tokio runtime");
        let fresh = Box::into_raw(Box::new(Slot { pid: me, runtime }));
        match RUNTIME.compare_exchange(current, fresh, Ordering::AcqRel, Ordering::Acquire) {
            // An inherited slot, if any, is leaked: its threads do not exist here.
            Ok(_) => {
                // SAFETY: `fresh` was just published and is never freed.
                return unsafe { &*fresh }.runtime.handle().clone();
            }
            Err(actual) => {
                // SAFETY: `fresh` was never published, so this thread owns it.
                let lost = unsafe { Box::from_raw(fresh) };
                lost.runtime.shutdown_background();
                current = actual;
            }
        }
    }
}

/// A process-scoped value in a lock-free slot: the parent's value is leaked
/// in a child, and concurrent initialisers elect one value.
pub struct PidCell<T: 'static> {
    slot: AtomicPtr<(u32, T)>,
}

impl<T: 'static> PidCell<T> {
    pub const fn new() -> Self {
        Self {
            slot: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// `init` may run more than once when callers race; one result wins.
    pub fn get_or_init(&'static self, init: impl Fn() -> T) -> &'static T {
        let me = pid();
        let mut current = self.slot.load(Ordering::Acquire);
        loop {
            // SAFETY: published values are leaked, never freed.
            if let Some((owner, value)) = unsafe { current.as_ref() } {
                if *owner == me {
                    return value;
                }
            }
            let fresh = Box::into_raw(Box::new((me, init())));
            match self
                .slot
                .compare_exchange(current, fresh, Ordering::AcqRel, Ordering::Acquire)
            {
                // SAFETY: published and leaked.
                Ok(_) => return &unsafe { &*fresh }.1,
                Err(actual) => {
                    // SAFETY: never published; this thread owns it.
                    drop(unsafe { Box::from_raw(fresh) });
                    current = actual;
                }
            }
        }
    }
}

/// Wraps values owned by the parent process's runtime and leaks them when
/// dropped in a forked child.
pub struct ForkGuard<T> {
    pid: u32,
    value: ManuallyDrop<T>,
}

impl<T> ForkGuard<T> {
    pub fn new(value: T) -> Self {
        Self {
            pid: pid(),
            value: ManuallyDrop::new(value),
        }
    }

    pub fn inherited(&self) -> bool {
        self.pid != pid()
    }
}

impl<T> std::ops::Deref for ForkGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> Drop for ForkGuard<T> {
    fn drop(&mut self) {
        if !self.inherited() {
            // SAFETY: dropped exactly once, here, in the owning process.
            unsafe { ManuallyDrop::drop(&mut self.value) }
        }
    }
}

/// Fork-safe registry of connection sockets. A slot holds an fd only while
/// that fd is open as one of our sockets: a socket registers after it is
/// connected and deregisters before it closes. A forked child closes (never
/// shuts down, which would affect the parent's shared socket) every
/// registered fd in a `pthread_atfork` child handler, before any child code
/// runs. Sockets still connecting at fork time are not registered and stay
/// open in the child until it exits.
#[cfg(unix)]
pub mod sockets {
    use std::sync::Once;
    use std::sync::atomic::{AtomicI32, Ordering};

    const SLOTS: usize = 4096;
    static FDS: [AtomicI32; SLOTS] = [const { AtomicI32::new(-1) }; SLOTS];
    static INSTALL: Once = Once::new();

    extern "C" fn close_inherited() {
        for slot in &FDS {
            let fd = slot.swap(-1, Ordering::AcqRel);
            if fd >= 0 {
                // SAFETY: async-signal-safe; the fd is an inherited copy of a
                // registered socket (see the module invariant).
                unsafe { libc::close(fd) };
            }
        }
    }

    pub fn install() {
        INSTALL.call_once(|| {
            // SAFETY: registers a handler that only touches atomics and close(2).
            unsafe { libc::pthread_atfork(None, None, Some(close_inherited)) };
        });
    }

    /// Returns the slot index, or None when the registry is full.
    pub fn register(fd: i32) -> Option<usize> {
        FDS.iter().position(|slot| {
            slot.compare_exchange(-1, fd, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })
    }

    pub fn deregister(index: usize) {
        FDS[index].store(-1, Ordering::Release);
    }
}

#[cfg(not(unix))]
pub mod sockets {
    pub fn install() {}
}

/// Every task a driver starts, including hyper's connection, body and
/// response tasks, so a closing transport can abort them and wait for them
/// to end: no detached tasks outlive `close`. Once sealed it refuses new
/// tasks, so a call still mid-poll when teardown starts cannot register one
/// after `close` observed the set empty.
pub struct Tracker {
    tasks: Mutex<TrackerState>,
    emptied: Notify,
}

#[derive(Default)]
struct TrackerState {
    next: u64,
    sealed: bool,
    running: HashMap<u64, AbortHandle>,
}

/// A tracked task's handle; empty when a sealed tracker refused the task.
pub struct Task(Option<AbortHandle>);

impl Task {
    pub fn abort(&self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

struct Finished {
    tracker: Arc<Tracker>,
    id: u64,
}

/// A tracked future and its registration. Fields drop in declaration order,
/// so the future, and every socket or task it owns, is gone before the
/// registration leaves the set: whether the task completed, panicked, or was
/// aborted before or during a poll. An `async` block holding both would drop
/// its captures in capture order when aborted before its first poll, removing
/// the registration first.
struct Tracked {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    _finished: Finished,
}

impl Future for Tracked {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

impl Drop for Finished {
    fn drop(&mut self) {
        let mut state = self.tracker.tasks.lock().unwrap_or_else(|e| e.into_inner());
        state.running.remove(&self.id);
        if state.running.is_empty() {
            self.tracker.emptied.notify_waiters();
        }
    }
}

impl Tracker {
    /// Binds the process runtime at the first spawn, not at construction.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tasks: Mutex::new(TrackerState::default()),
            emptied: Notify::new(),
        })
    }

    pub fn spawn<F>(self: &Arc<Self>, future: F) -> Task
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // The lock spans the spawn, so a task that ends at once removes an
        // entry that already exists, and `seal` waits for a registration in
        // progress, then aborts it.
        let mut state = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        if state.sealed {
            drop(state);
            // Dropped outside the lock: dropping it may release the last
            // driver reference, whose teardown takes this lock.
            drop(future);
            return Task(None);
        }
        state.next += 1;
        let id = state.next;
        let finished = Finished {
            tracker: self.clone(),
            id,
        };
        let join = handle().spawn(Tracked {
            future: Box::pin(future),
            _finished: finished,
        });
        let abort = join.abort_handle();
        state.running.insert(id, abort.clone());
        Task(Some(abort))
    }

    /// Refuses every later spawn and aborts every registered task.
    pub fn seal(&self) {
        let mut state = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        state.sealed = true;
        for task in state.running.values() {
            task.abort();
        }
    }

    pub fn abort_all(&self) {
        let state = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        for task in state.running.values() {
            task.abort();
        }
    }

    pub fn len(&self) -> usize {
        let state = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        state.running.len()
    }

    pub fn is_empty(&self) -> bool {
        let state = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        state.running.is_empty()
    }

    pub async fn wait_empty(&self) {
        loop {
            let emptied = self.emptied.notified();
            tokio::pin!(emptied);
            emptied.as_mut().enable();
            if self.is_empty() {
                return;
            }
            emptied.await;
        }
    }
}

/// hyper's executor: every connection, body pipe and response task it
/// spawns is tracked by the driver.
#[derive(Clone)]
pub struct TrackedExec(pub Arc<Tracker>);

impl<F> hyper::rt::Executor<F> for TrackedExec
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        self.0.spawn(future);
    }
}
