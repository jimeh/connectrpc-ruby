//! Driver state and connection admission.
//!
//! Pooled HTTP/2: one admitting connection per origin. A connection whose
//! dispatcher ended (GOAWAY, error, close) stops admitting and drains its
//! streams on its own while the next caller dials a new one.
//!
//! Pool dials, as grpc-go's channel dials: an origin has at most one shared
//! HTTP/2 dial at a time, a task the driver owns. Its outcome is decided only
//! by the dial and by `close`, never by a caller, and every caller that waited
//! for it gets the same outcome: a pooled connection to check out, HTTP/1.1
//! (the clean connection is parked idle; each waiter then uses a usable
//! pooled or idle connection, else dials its own), or the failure. A caller
//! decides to use a connection, or else to join or start the dial, under one
//! lock. It waits under its own deadline, cancellation and interrupts, and
//! leaving changes nothing about the dial, whose per-address budget splits
//! `connect_timeout` alone. A dial still running when its transport is
//! garbage collected ends within `connect_timeout`.
//!
//! Unshared dials run inside one call, bounded by the earlier of
//! `connect_timeout` and the call deadline: per-call mode, `http2: false`
//! (one HTTP/1.1 connection serves one call at a time, so connections dial
//! in parallel, as Go net/http dials them), callers after an HTTP/1.1
//! outcome and a retry from a pooled connection that still admits. A second
//! HTTP/2 connection never replaces a usable pooled one: it serves only the
//! call that dialed it. Idle connections close after
//! `idle_timeout`; the reaper claims the exact checkout generation it
//! observed idle, read under the lock checkouts count with. At most two idle
//! HTTP/1.1 keep-alive connections per origin; only a connection whose request
//! and response both completed cleanly returns, and each idle connection has
//! one idle timer, cancelled when a call takes it.
//!
//! Closing: waiters of a pool dial fail with the closed phrase and the dial
//! ends. An admitted call's own unshared dial still proceeds within the
//! grace, but a connection dialed after admission closed is never pooled.
//! Teardown runs as a runtime task, so it ends however closers are
//! interrupted, and every closer gets its published outcome.
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use hyper::client::conn::{http1, http2};
use tokio::sync::Notify;

use crate::body::CallBody;
use crate::conn::{self, Connected, Handshake, Origin, Settings};
use crate::failure::Failure;
use crate::gvl::Signal;
use crate::holds::Holds;
use crate::runtime::{self, Task, Tracker};
use crate::tls::{TlsInputs, native_roots};

/// Go's DefaultMaxIdleConnsPerHost.
const MAX_IDLE_H1: usize = 2;
/// core's GRPCProtocol::FIELD_LIMIT.
const GRPC_FIELD_LIMIT: usize = 65_536;

type TlsConfigs = (Arc<rustls::ClientConfig>, Arc<rustls::ClientConfig>);

pub struct Config {
    pub pooled: bool,
    pub idle_timeout: Option<Duration>,
    pub connect_timeout: Duration,
    pub settings: Settings,
    pub tls: TlsInputs,
}

pub struct DriverCore {
    pub config: Config,
    tls_configs: tokio::sync::OnceCell<TlsConfigs>,
    pub tracker: Arc<Tracker>,
    origins: Mutex<HashMap<Origin, Arc<OriginEntry>>>,
    closing: AtomicBool,
    /// Fires when admission closes: wakes the waiters of a pool dial.
    pub closed: Arc<Signal>,
    /// Fires on forced teardown: ends every wait of every call.
    pub shutdown: Arc<Signal>,
    teardown: AtomicBool,
    /// The teardown result: unknown, false or true (0, 1, 2).
    outcome: AtomicU8,
    /// Fires once `outcome` is published.
    torn_down: Arc<Signal>,
    inflight: AtomicUsize,
    drained: Notify,
    opened: AtomicUsize,
    next_id: AtomicU64,
    pub holds: Arc<Holds>,
}

#[derive(Default)]
pub struct OriginEntry {
    state: Mutex<OriginState>,
}

#[derive(Default)]
struct OriginState {
    h2: Option<Arc<H2Conn>>,
    /// The pool dial in progress.
    dialing: Option<Arc<Dial>>,
    idle_h1: Vec<IdleH1>,
}

/// A pool dial's outcome, shared by everyone who waited for it.
#[derive(Default)]
struct Dial {
    outcome: Mutex<Option<Outcome>>,
    done: Signal,
}

#[derive(Clone)]
enum Outcome {
    /// The pool has a usable HTTP/2 connection: check it out.
    Pooled,
    /// TLS negotiated HTTP/1.1. The connection is parked idle, if it fits;
    /// waiters claim again without sharing a dial.
    Http1,
    /// The dial failed: every waiter fails the same way.
    Failed(Failure),
    /// Close ended the dial, or its task ended without an outcome.
    Ended,
}

impl Dial {
    async fn wait(&self) -> Outcome {
        self.done.wait().await;
        self.outcome
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or(Outcome::Ended)
    }
}

/// Publishes a pool dial's outcome when dropped, so every exit of its task
/// (an outcome, close, abort, refusal by a sealed tracker or a panic) ends
/// the dial for its waiters. Clearing `dialing` comes first, so a caller
/// woken by the outcome finds either the pooled connection or no dial.
struct Publish {
    entry: Arc<OriginEntry>,
    dial: Arc<Dial>,
    outcome: Option<Outcome>,
}

impl Publish {
    fn finish(mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for Publish {
    fn drop(&mut self) {
        {
            let mut state = self.entry.state.lock().unwrap_or_else(|e| e.into_inner());
            if state
                .dialing
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &self.dial))
            {
                state.dialing = None;
            }
        }
        *self.dial.outcome.lock().unwrap_or_else(|e| e.into_inner()) = self.outcome.take();
        self.dial.done.fire();
    }
}

/// What one look at an origin's state gives a caller.
enum Claim {
    /// A pooled connection; a parked HTTP/1.1 one still needs `ready`.
    Lease(Lease),
    /// Dial a connection for this call only.
    Dial,
    /// Wait for the pool dial; `start` when this caller created it.
    Wait { dial: Arc<Dial>, start: bool },
}

struct H2Conn {
    sender: http2::SendRequest<CallBody>,
    shared: Arc<ConnShared>,
    local_port: u16,
}

impl H2Conn {
    fn usable(&self) -> bool {
        !self.sender.is_closed() && !self.shared.closed.fired()
    }
}

struct ConnShared {
    id: u64,
    active: AtomicUsize,
    generation: AtomicU64,
    idle: Notify,
    closed: Arc<Signal>,
}

struct IdleH1 {
    /// Unique per park, so a timer only ever claims the park it started for.
    park: u64,
    conn: H1Conn,
    _timer: Option<IdleTimer>,
}

/// An idle connection's timer task, aborted when the connection leaves the
/// idle list (checkout, close or expiry).
struct IdleTimer(Task);

impl Drop for IdleTimer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub struct H1Conn {
    pub sender: http1::SendRequest<CallBody>,
    task: Task,
    id: u64,
    pub local_port: u16,
}

impl H1Conn {
    /// Closes the socket now, whatever was in flight.
    fn abort(self) {
        self.task.abort();
    }
}

/// One call's hold on a connection.
pub enum Lease {
    H2 {
        sender: http2::SendRequest<CallBody>,
        local_port: u16,
        id: u64,
        _active: Option<Active>,
    },
    H1 {
        conn: Option<H1Conn>,
        entry: Option<Arc<OriginEntry>>,
    },
}

impl Lease {
    pub fn id(&self) -> u64 {
        match self {
            Lease::H2 { id, .. } => *id,
            Lease::H1 { conn, .. } => conn.as_ref().map_or(0, |c| c.id),
        }
    }

    pub fn local_port(&self) -> u16 {
        match self {
            Lease::H2 { local_port, .. } => *local_port,
            Lease::H1 { conn, .. } => conn.as_ref().map_or(0, |c| c.local_port),
        }
    }

    pub fn is_h2(&self) -> bool {
        matches!(self, Lease::H2 { .. })
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // A lease dropped without a clean release closes an HTTP/1.1 socket:
        // its request or response may be incomplete.
        if let Lease::H1 { conn, .. } = self {
            if let Some(conn) = conn.take() {
                conn.abort();
            }
        }
    }
}

/// Counts a call on a pooled HTTP/2 connection; dropping it may start the
/// idle clock.
pub struct Active {
    shared: Arc<ConnShared>,
}

impl Drop for Active {
    fn drop(&mut self) {
        if self.shared.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.idle.notify_waiters();
        }
    }
}

/// Holds a call in the driver's in-flight count until released.
pub struct Inflight {
    core: Weak<DriverCore>,
}

impl Drop for Inflight {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            if core.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
                core.drained.notify_waiters();
            }
        }
    }
}

/// The parts of a request that survive a retry.
pub struct RequestSpec {
    pub method: http::Method,
    pub uri: http::Uri,
    pub origin: Origin,
    pub headers: Vec<(http::HeaderName, http::HeaderValue)>,
    pub timeout_header: Option<TimeoutHeader>,
    pub grpc: bool,
}

#[derive(Clone, Copy)]
pub enum TimeoutHeader {
    Grpc,
    Connect,
}

/// core's GRPCProtocol.timeout.
fn grpc_timeout(seconds: f64) -> String {
    const UNITS: [(&str, f64); 6] = [
        ("n", 1e-9),
        ("u", 1e-6),
        ("m", 0.001),
        ("S", 1.0),
        ("M", 60.0),
        ("H", 3600.0),
    ];
    for (unit, scale) in UNITS {
        let scaled = seconds / scale;
        if scaled.is_finite() && scaled <= 99_999_999.0 {
            let amount = scaled.ceil();
            if amount <= 99_999_999.0 {
                return format!("{}{unit}", amount.max(1.0) as u64);
            }
        }
    }
    "99999999H".to_string()
}

/// core's Deadline#timeout_ms.
fn connect_timeout_ms(seconds: f64) -> String {
    let millis = (seconds * 1000.0).min(9_999_999_999.0).ceil();
    (millis as u64).clamp(1, 9_999_999_999).to_string()
}

impl RequestSpec {
    /// Builds the request at dispatch: the wire timeout reflects the deadline
    /// remaining now, after connection setup.
    pub fn build(
        &self,
        body: CallBody,
        deadline: Option<Instant>,
        h2: bool,
    ) -> Result<hyper::Request<CallBody>, Failure> {
        let mut builder = hyper::Request::builder().method(self.method.clone());
        builder = if h2 {
            builder.uri(self.uri.clone())
        } else {
            let authority = self
                .uri
                .authority()
                .map_or(self.origin.host.as_str(), |a| a.as_str());
            // A forwarding proxy takes the absolute form and its credentials;
            // an origin, direct or through a tunnel, takes the origin form.
            let builder = match self
                .origin
                .proxy
                .as_ref()
                .filter(|_| self.origin.forwarded())
            {
                Some(route) => {
                    let builder = builder.uri(self.uri.clone());
                    match &route.authorization {
                        Some(value) => {
                            builder.header(http::header::PROXY_AUTHORIZATION, value.clone())
                        }
                        None => builder,
                    }
                }
                None => builder.uri(self.uri.path_and_query().map_or("/", |p| p.as_str())),
            };
            builder.header(http::header::HOST, authority)
        };
        let mut request = builder
            .body(body)
            .map_err(|_| Failure::new("request", "invalid request"))?;
        let headers = request.headers_mut();
        for (name, value) in &self.headers {
            headers.append(name.clone(), value.clone());
        }
        if let (Some(kind), Some(deadline)) = (self.timeout_header, deadline) {
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .as_secs_f64();
            if remaining <= 0.0 {
                return Err(Failure::new("timeout", "deadline expired before dispatch"));
            }
            let (name, value) = match kind {
                TimeoutHeader::Grpc => ("grpc-timeout", grpc_timeout(remaining)),
                TimeoutHeader::Connect => ("connect-timeout-ms", connect_timeout_ms(remaining)),
            };
            let value = http::HeaderValue::from_str(&value).expect("timeout header is ASCII");
            headers.insert(http::HeaderName::from_static(name), value);
        }
        if self.grpc {
            // core's GRPCProtocol.check_fields!, on the fields actually sent.
            let size: usize = headers
                .iter()
                .filter(|(name, _)| *name != http::header::HOST)
                .map(|(name, value)| name.as_str().len() + value.len() + 32)
                .sum();
            if size > GRPC_FIELD_LIMIT {
                return Err(Failure::new(
                    "field_limit",
                    "gRPC field block exceeds byte limit",
                ));
            }
        }
        Ok(request)
    }
}

impl DriverCore {
    pub fn new(config: Config, tracker: Arc<Tracker>) -> Arc<Self> {
        Arc::new(Self {
            config,
            tls_configs: tokio::sync::OnceCell::new(),
            tracker,
            origins: Mutex::new(HashMap::new()),
            closing: AtomicBool::new(false),
            closed: Signal::new(),
            shutdown: Signal::new(),
            teardown: AtomicBool::new(false),
            outcome: AtomicU8::new(0),
            torn_down: Signal::new(),
            inflight: AtomicUsize::new(0),
            drained: Notify::new(),
            opened: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            holds: Arc::default(),
        })
    }

    pub fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }

    /// Idle pooled HTTP/1.1 connections across origins.
    pub fn idle_h1(&self) -> usize {
        let origins = self.origins.lock().unwrap_or_else(|e| e.into_inner());
        origins
            .values()
            .map(|entry| {
                entry
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .idle_h1
                    .len()
            })
            .sum()
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    pub fn admit(self: &Arc<Self>) -> Result<Inflight, Failure> {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        let guard = Inflight {
            core: Arc::downgrade(self),
        };
        if self.is_closing() {
            return Err(Failure::admission());
        }
        Ok(guard)
    }

    fn entry(&self, origin: &Origin) -> Arc<OriginEntry> {
        let mut origins = self.origins.lock().unwrap_or_else(|e| e.into_inner());
        origins.entry(origin.clone()).or_default().clone()
    }

    async fn tls_config(&self, h2: bool) -> Result<Arc<rustls::ClientConfig>, Failure> {
        let (with_h2, h1_only) = self
            .tls_configs
            .get_or_try_init(|| async {
                let native = tokio::task::spawn_blocking(native_roots)
                    .await
                    .map_err(|_| Failure::new("tls", "trust store load failed"))?;
                self.config.tls.configs(&native)
            })
            .await?;
        Ok(if h2 { with_h2.clone() } else { h1_only.clone() })
    }

    /// Dials within `connect_timeout`. As Go's dialer does, each address
    /// attempt gets a share of what is left before the earlier of
    /// `connect_timeout` and `deadline`: the call deadline for an unshared
    /// dial, none for a pool dial. The call deadline itself still ends the
    /// call as the caller's expiry.
    async fn dial(
        self: &Arc<Self>,
        origin: &Origin,
        deadline: Option<Instant>,
    ) -> Result<Connected, Failure> {
        let limit = self.config.connect_timeout;
        let mut budget = tokio::time::Instant::now() + limit;
        if let Some(deadline) = deadline {
            budget = budget.min(tokio::time::Instant::from_std(deadline));
        }
        let work = async {
            let tls = if origin.https {
                Some(self.tls_config(self.config.settings.http2).await?)
            } else {
                None
            };
            conn::connect(
                &self.config.settings,
                tls,
                origin,
                &self.tracker,
                &self.holds,
                budget,
            )
            .await
        };
        let connected = tokio::time::timeout(limit, work)
            .await
            .map_err(|_| Failure::new("connect_timeout", "connection setup timed out"))??;
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(connected)
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// A connection for one call ending at `deadline`. `exclude` names a
    /// connection that just refused or handed back a request: a retry never
    /// reuses it, and never evicts it while it still admits other calls. A
    /// call admitted before close keeps its own unshared dial; new calls and
    /// waiters of a pool dial fail with the closed phrase.
    pub async fn acquire(
        self: &Arc<Self>,
        origin: &Origin,
        require_h2: bool,
        exclude: Option<u64>,
        deadline: Option<Instant>,
    ) -> Result<Lease, Failure> {
        if self.is_closing() {
            return Err(Failure::admission());
        }
        if !self.config.pooled {
            let connected = self.dial(origin, deadline).await?;
            return self.fresh_lease(connected, None, require_h2);
        }
        let entry = self.entry(origin);
        // HTTP/1.1 connections cannot be shared, so neither are their dials,
        // nor dials after a pool dial negotiated HTTP/1.1.
        let mut share = self.config.settings.http2;
        loop {
            let dial = match self.claim(&entry, require_h2, exclude, share)? {
                Claim::Lease(lease) => match self.ready(lease).await? {
                    Some(lease) => return Ok(lease),
                    None => continue,
                },
                Claim::Dial => {
                    let connected = self.dial(origin, deadline).await?;
                    return self.fresh_lease(connected, Some(&entry), require_h2);
                }
                Claim::Wait { dial, start } => {
                    if start {
                        self.tracker.spawn(self.clone().pool_dial(
                            entry.clone(),
                            origin.clone(),
                            dial.clone(),
                        ));
                        // Spec hold point: armed with a zero bound, it counts
                        // the pool dials started.
                        self.holds.reach("dial:started").await;
                    }
                    // Spec hold point: armed with a zero bound, it counts the
                    // callers that will get a pool dial's outcome. A thread
                    // parked in a native wait may not have claimed yet.
                    self.holds.reach("dial:joined").await;
                    dial
                }
            };
            let outcome = tokio::select! {
                outcome = dial.wait() => outcome,
                _ = self.closed.wait() => return Err(Failure::admission()),
            };
            if self.is_closing() {
                return Err(Failure::admission());
            }
            match outcome {
                Outcome::Pooled => {}
                // The origin speaks HTTP/1.1; dialing again would not change it.
                Outcome::Http1 if require_h2 => {
                    return Err(Failure::new("alpn", "HTTP/2 was not negotiated"));
                }
                Outcome::Http1 => share = false,
                Outcome::Failed(failure) => return Err(failure),
                Outcome::Ended => return Err(Failure::new("closed", "connection setup ended")),
            }
        }
    }

    /// A pool dial: its own task, bounded by `connect_timeout` alone and
    /// ended by close. `Publish` exists before the task does, so dropping the
    /// task unpolled (refused by a sealed tracker or aborted) publishes too.
    fn pool_dial(
        self: Arc<Self>,
        entry: Arc<OriginEntry>,
        origin: Origin,
        dial: Arc<Dial>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let publish = Publish {
            entry,
            dial,
            outcome: None,
        };
        async move {
            let connected = tokio::select! {
                connected = self.dial(&origin, None) => connected,
                _ = self.closed.wait() => return,
            };
            let outcome = match connected {
                Err(failure) => Outcome::Failed(failure),
                Ok(connected) => match connected.handshake {
                    Handshake::H2 { sender, closed } => {
                        let id = self.next_id();
                        let port = connected.local_port;
                        let pooled = self.pool_h2(&publish.entry, sender, closed, port, id, 0);
                        if pooled.is_none() && self.is_closing() {
                            return;
                        }
                        // Pooled, or an unshared dial pooled a usable
                        // connection first and this one closes unused.
                        self.holds.reach("dial:pooled").await;
                        Outcome::Pooled
                    }
                    Handshake::H1 { sender, task } => {
                        let conn = H1Conn {
                            sender,
                            task,
                            id: self.next_id(),
                            local_port: connected.local_port,
                        };
                        self.park_h1(publish.entry.clone(), conn);
                        if self.is_closing() {
                            return;
                        }
                        Outcome::Http1
                    }
                },
            };
            publish.finish(outcome);
        }
    }

    /// A parked HTTP/1.1 connection once hyper reports it ready, or None if
    /// it closed meanwhile; any other lease as it is.
    async fn ready(self: &Arc<Self>, mut lease: Lease) -> Result<Option<Lease>, Failure> {
        let ready = match &mut lease {
            Lease::H1 {
                conn: Some(conn), ..
            } => tokio::select! {
                ready = conn.sender.ready() => ready.is_ok(),
                _ = self.closed.wait() => return Err(Failure::admission()),
            },
            _ => true,
        };
        Ok(ready.then_some(lease))
    }

    /// One look at the origin under its lock: a usable connection, or else
    /// the pool dial to join or start. Deciding both under one lock means a
    /// caller never misses a connection pooled between its look and its join.
    /// The caller spawns a dial it started after the lock is released: a
    /// sealed tracker drops the task at once, and its `Publish` takes this
    /// lock.
    fn claim(
        self: &Arc<Self>,
        entry: &Arc<OriginEntry>,
        require_h2: bool,
        exclude: Option<u64>,
        share: bool,
    ) -> Result<Claim, Failure> {
        let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(conn) = state.h2.as_ref() {
            let usable = conn.usable();
            if usable && exclude == Some(conn.shared.id) {
                // A retry from a connection that still admits: it stays pooled
                // for its other calls, and the retry gets a connection of its
                // own, which pool_h2 then leaves unpooled.
                return Ok(Claim::Dial);
            }
            if usable {
                // Under the state lock, so the reaper's claim sees it.
                conn.shared.active.fetch_add(1, Ordering::AcqRel);
                conn.shared.generation.fetch_add(1, Ordering::AcqRel);
                return Ok(Claim::Lease(Lease::H2 {
                    sender: conn.sender.clone(),
                    local_port: conn.local_port,
                    id: conn.shared.id,
                    _active: Some(Active {
                        shared: conn.shared.clone(),
                    }),
                }));
            }
            // Stops admitting; its in-flight streams drain on their own.
            state.h2 = None;
        }
        // Taking a connection out of the idle list drops its timer.
        while let Some(idle) = state.idle_h1.pop() {
            if idle.conn.sender.is_closed() {
                continue;
            }
            if require_h2 {
                // The origin negotiated HTTP/1.1; keep the clean socket.
                state.idle_h1.push(idle);
                return Err(Failure::new("alpn", "HTTP/2 was not negotiated"));
            }
            return Ok(Claim::Lease(Lease::H1 {
                conn: Some(idle.conn),
                entry: Some(entry.clone()),
            }));
        }
        if !share {
            return Ok(Claim::Dial);
        }
        if let Some(dial) = &state.dialing {
            return Ok(Claim::Wait {
                dial: dial.clone(),
                start: false,
            });
        }
        let dial = Arc::new(Dial::default());
        state.dialing = Some(dial.clone());
        Ok(Claim::Wait { dial, start: true })
    }

    fn fresh_lease(
        self: &Arc<Self>,
        connected: Connected,
        entry: Option<&Arc<OriginEntry>>,
        require_h2: bool,
    ) -> Result<Lease, Failure> {
        let id = self.next_id();
        let local_port = connected.local_port;
        match connected.handshake {
            // Without an entry (per-call mode) it serves only this call.
            Handshake::H2 { sender, closed } => {
                let active = entry
                    .and_then(|entry| {
                        self.pool_h2(entry, sender.clone(), closed, local_port, id, 1)
                    })
                    .map(|shared| Active { shared });
                Ok(Lease::H2 {
                    sender,
                    local_port,
                    id,
                    _active: active,
                })
            }
            Handshake::H1 { sender, task } => {
                let conn = H1Conn {
                    sender,
                    task,
                    id,
                    local_port,
                };
                if require_h2 {
                    // ALPN chose HTTP/1.1: fail before any request is queued.
                    // The untouched socket stays available to unary calls.
                    if let Some(entry) = entry {
                        self.park_h1(entry.clone(), conn);
                    }
                    return Err(Failure::new("alpn", "HTTP/2 was not negotiated"));
                }
                Ok(Lease::H1 {
                    conn: Some(conn),
                    entry: entry.cloned(),
                })
            }
        }
    }

    /// Pools an HTTP/2 connection already held by `active` calls and starts
    /// its reaper; returns its shared state, or None when it was not pooled.
    /// Checked under the lock begin_close clears the pool with: a connection
    /// dialed for a call admitted before close serves only that call, then
    /// closes with it. So does one dialed while a usable connection is pooled
    /// (an unshared dial after an HTTP/1.1 outcome may still get h2 from TLS,
    /// and a retry dials past a connection that still admits): replacing it
    /// would retire a working connection with streams in flight.
    fn pool_h2(
        self: &Arc<Self>,
        entry: &Arc<OriginEntry>,
        sender: http2::SendRequest<CallBody>,
        closed: Arc<Signal>,
        local_port: u16,
        id: u64,
        active: usize,
    ) -> Option<Arc<ConnShared>> {
        let shared = Arc::new(ConnShared {
            id,
            active: AtomicUsize::new(active),
            generation: AtomicU64::new(1),
            idle: Notify::new(),
            closed,
        });
        {
            let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
            let pooled = state.h2.as_ref().is_some_and(|current| current.usable());
            if self.is_closing() || pooled {
                return None;
            }
            state.h2 = Some(Arc::new(H2Conn {
                sender,
                shared: shared.clone(),
                local_port,
            }));
        }
        if let Some(timeout) = self.config.idle_timeout {
            self.tracker.spawn(reap(
                Arc::downgrade(entry),
                shared.clone(),
                timeout,
                self.closed.clone(),
                self.holds.clone(),
            ));
        }
        Some(shared)
    }

    /// Returns an HTTP/1.1 connection once hyper reports it ready for the
    /// next request, within the idle cap. Called only after a clean exchange.
    pub fn release_h1(self: &Arc<Self>, lease: &mut Lease) {
        if let Lease::H1 { conn, entry } = lease {
            match (conn.take(), entry.take()) {
                (Some(conn), Some(entry)) if !self.is_closing() => self.park_h1(entry, conn),
                // Per-call or closing: dropping the sender closes it gracefully.
                (Some(conn), _) => drop(conn.sender),
                _ => {}
            }
        }
    }

    /// Parks a clean HTTP/1.1 connection at once; the next checkout waits for
    /// hyper to report it ready before reusing it. Beyond the cap, dropping
    /// the sender closes it gracefully. Each park starts one idle timer,
    /// which the checkout that takes the connection aborts, so the
    /// connection closes only after a full `idle_timeout` of idleness.
    fn park_h1(self: &Arc<Self>, entry: Arc<OriginEntry>, conn: H1Conn) {
        let park = self.next_id();
        let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_closing() || state.idle_h1.len() >= MAX_IDLE_H1 {
            return;
        }
        let timer = self.config.idle_timeout.map(|timeout| {
            let closed = self.closed.clone();
            let entry = Arc::downgrade(&entry);
            IdleTimer(self.tracker.spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(timeout) => {}
                    _ = closed.wait() => {}
                }
                // Claims this exact park; a connection checked out and parked
                // again has a new park id and its own timer.
                let Some(entry) = entry.upgrade() else { return };
                let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
                state.idle_h1.retain(|idle| idle.park != park);
            }))
        });
        state.idle_h1.push(IdleH1 {
            park,
            conn,
            _timer: timer,
        });
    }

    /// Stops admission: later calls, dial waiters and lazy exchanges fail
    /// with the closed phrase. Idle connections close now.
    pub fn begin_close(&self) {
        self.closing.store(true, Ordering::SeqCst);
        self.closed.fire();
        let origins = self.origins.lock().unwrap_or_else(|e| e.into_inner());
        for entry in origins.values() {
            let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
            state.h2 = None;
            state.idle_h1.clear();
        }
    }

    pub async fn wait_drained(&self) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.inflight.load(Ordering::SeqCst) == 0 {
                return;
            }
            drained.await;
        }
    }

    /// Starts teardown once: stops admission, lets in-flight calls finish
    /// within `grace`, then forces the rest and waits for every owned task.
    /// It is a runtime task outside the tracker, which `force` seals and waits
    /// for, so no closer's interruption ends it part way. Every call returns
    /// the signal that fires once the outcome is published.
    pub fn start_teardown(self: &Arc<Self>, grace: Duration) -> Arc<Signal> {
        if !self.teardown.swap(true, Ordering::SeqCst) {
            self.begin_close();
            let core = self.clone();
            runtime::handle().spawn(async move {
                let expires = tokio::time::Instant::now() + grace;
                let drained = tokio::time::timeout_at(expires, core.wait_drained())
                    .await
                    .is_ok();
                let quiet = drained
                    && tokio::time::timeout_at(expires, core.tracker.wait_empty())
                        .await
                        .is_ok();
                core.force().await;
                core.outcome
                    .store(if quiet { 2 } else { 1 }, Ordering::SeqCst);
                core.torn_down.fire();
            });
        }
        self.torn_down.clone()
    }

    /// The published teardown result: true when everything drained within
    /// the grace, false after forced teardown, None before it ended.
    pub fn close_outcome(&self) -> Option<bool> {
        match self.outcome.load(Ordering::SeqCst) {
            0 => None,
            outcome => Some(outcome == 2),
        }
    }

    /// Ends every call's waits and every owned task, then waits for the
    /// tasks. Sealing first means a call mid-poll registers no task later.
    /// The order does not choose a call's phrase: a call already mid-poll
    /// sees a task's abort before `shutdown` in either order, so each site
    /// that observes one maps it to the shutdown or closed phrase itself.
    pub async fn force(&self) {
        self.tracker.seal();
        self.shutdown.fire();
        // Spec hold point: a spec armed with a zero bound observes the seal.
        self.holds.reach("force:sealed").await;
        self.tracker.wait_empty().await;
    }
}

impl Drop for DriverCore {
    fn drop(&mut self) {
        // No call holds the driver any more; stop its owned tasks.
        self.closed.fire();
        self.tracker.abort_all();
    }
}

/// Closes a pooled HTTP/2 connection after `timeout` without calls, claiming
/// the checkout generation it observed idle so a racing checkout wins.
async fn reap(
    entry: Weak<OriginEntry>,
    shared: Arc<ConnShared>,
    timeout: Duration,
    closed: Arc<Signal>,
    holds: Arc<Holds>,
) {
    loop {
        let generation = loop {
            let idle = shared.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if shared.closed.fired() || closed.fired() {
                return;
            }
            let Some(origin) = entry.upgrade() else {
                return;
            };
            let observed = {
                // One snapshot under the lock checkouts count calls with, so
                // a checkout between the two reads cannot pass for idleness.
                let _state = origin.state.lock().unwrap_or_else(|e| e.into_inner());
                (shared.active.load(Ordering::Acquire) == 0).then(|| {
                    holds.block("reap:idle");
                    shared.generation.load(Ordering::Acquire)
                })
            };
            drop(origin);
            if let Some(generation) = observed {
                break generation;
            }
            tokio::select! {
                _ = idle => {}
                _ = shared.closed.wait() => return,
                _ = closed.wait() => return,
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(timeout) => {}
            _ = shared.closed.wait() => return,
            _ = closed.wait() => return,
        }
        let Some(entry) = entry.upgrade() else { return };
        let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
        let current = state
            .h2
            .as_ref()
            .is_some_and(|conn| conn.shared.id == shared.id);
        if !current {
            return;
        }
        if shared.active.load(Ordering::Acquire) == 0
            && shared.generation.load(Ordering::Acquire) == generation
        {
            // Dropping the pool's sender lets hyper close the connection.
            state.h2 = None;
            return;
        }
    }
}
