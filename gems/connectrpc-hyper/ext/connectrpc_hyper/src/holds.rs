//! Spec hold points. A spec arms a named point on one driver; whatever
//! reaches an armed point parks there until the spec releases it or the
//! point's bound passes, so a spec can hold one step of an interleaving that
//! a race would otherwise decide. A driver with no armed point pays one
//! atomic load per point.
//!
//! A point that parks its thread must never park a Ruby thread holding the
//! GVL: the whole VM would stall for the point's bound, and the spec waiting
//! to observe the hold could not run. `drive` therefore skips its GVL-held
//! first poll while any point is armed in the process (`any_armed`).
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::Notify;

/// Points armed across every driver in the process.
static ARMED: AtomicUsize = AtomicUsize::new(0);

pub fn any_armed() -> bool {
    ARMED.load(Ordering::Acquire) > 0
}

#[derive(Default)]
pub struct Holds {
    armed: AtomicBool,
    points: Mutex<HashMap<String, Arc<Point>>>,
}

struct Point {
    bound: Duration,
    reached: AtomicUsize,
    released: Mutex<bool>,
    changed: Condvar,
    notify: Notify,
    /// Polls stalled at the point (`poll_stall`), woken on release.
    wakers: Mutex<Vec<Waker>>,
}

impl Point {
    fn is_released(&self) -> bool {
        *self.released.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn release(&self) {
        *self.released.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
        self.notify.notify_waiters();
        self.wake_stalled();
    }

    /// Wakes the polls stalled here, so each looks at its point again.
    fn wake_stalled(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap_or_else(|e| e.into_inner()));
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Holds {
    fn point(&self, name: &str) -> Option<Arc<Point>> {
        if !self.armed.load(Ordering::Acquire) {
            return None;
        }
        let points = self.points.lock().unwrap_or_else(|e| e.into_inner());
        points.get(name).cloned()
    }

    /// Parks the current task at `name` while it is armed. A zero bound only
    /// counts the arrival, without yielding: even a zero timeout waits for a
    /// timer tick, which a worker parked at another point may be holding up.
    pub async fn reach(&self, name: &str) {
        let Some(point) = self.point(name) else {
            return;
        };
        point.reached.fetch_add(1, Ordering::SeqCst);
        if point.bound.is_zero() {
            return;
        }
        let released = async {
            loop {
                let notified = point.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if point.is_released() {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(point.bound, released).await;
    }

    /// Parks the calling thread itself at `name` while it is armed: for an
    /// ordering inside one poll or under a lock, where yielding would let
    /// another branch of the same poll decide it. A zero bound only counts.
    pub fn block(&self, name: &str) {
        let Some(point) = self.point(name) else {
            return;
        };
        point.reached.fetch_add(1, Ordering::SeqCst);
        if point.bound.is_zero() {
            return;
        }
        let released = point.released.lock().unwrap_or_else(|e| e.into_inner());
        let _ = point
            .changed
            .wait_timeout_while(released, point.bound, |released| !*released);
    }

    /// Whether any point is armed on this driver: callers that build a
    /// point's name check this first.
    pub fn armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    /// Stalls a poll at `name` while it is armed and not released, for an
    /// IO operation a spec holds back; the bound does not apply. Each
    /// stalled poll counts an arrival.
    pub fn poll_stall(&self, name: &str, cx: &mut Context<'_>) -> Poll<()> {
        let Some(point) = self.point(name) else {
            return Poll::Ready(());
        };
        let mut wakers = point.wakers.lock().unwrap_or_else(|e| e.into_inner());
        if point.is_released() {
            return Poll::Ready(());
        }
        point.reached.fetch_add(1, Ordering::SeqCst);
        if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
            wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }

    /// Arms `name`; each arrival parks for at most `bound`.
    pub fn arm(&self, name: String, bound: Duration) {
        let point = Arc::new(Point {
            bound,
            reached: AtomicUsize::new(0),
            released: Mutex::new(false),
            changed: Condvar::new(),
            notify: Notify::new(),
            wakers: Mutex::new(Vec::new()),
        });
        let mut points = self.points.lock().unwrap_or_else(|e| e.into_inner());
        match points.insert(name, point) {
            // Polls stalled on the old point have no bound: they look again
            // and stall on this one. Arrivals parked there keep their bound.
            Some(replaced) => replaced.wake_stalled(),
            None => {
                ARMED.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.armed.store(true, Ordering::Release);
    }

    /// Releases every arrival at `name` and disarms it. A driver with no
    /// armed point goes back to the one-load check.
    pub fn release(&self, name: &str) {
        let point = {
            let mut points = self.points.lock().unwrap_or_else(|e| e.into_inner());
            let point = points.remove(name);
            if points.is_empty() {
                self.armed.store(false, Ordering::Release);
            }
            point
        };
        if let Some(point) = point {
            ARMED.fetch_sub(1, Ordering::AcqRel);
            point.release();
        }
    }

    /// Arrivals at an armed `name`.
    pub fn reached(&self, name: &str) -> usize {
        let points = self.points.lock().unwrap_or_else(|e| e.into_inner());
        points
            .get(name)
            .map_or(0, |point| point.reached.load(Ordering::SeqCst))
    }
}

impl Drop for Holds {
    fn drop(&mut self) {
        let points = self.points.get_mut().unwrap_or_else(|e| e.into_inner());
        ARMED.fetch_sub(points.len(), Ordering::AcqRel);
    }
}
