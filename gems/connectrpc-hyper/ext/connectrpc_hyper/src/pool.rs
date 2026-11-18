//! Driver state and connection admission.
//!
//! Pooled HTTP/2: one admitting connection per origin. A connection whose
//! dispatcher ended (GOAWAY, error, close) stops admitting and drains its
//! streams on its own while the next caller dials a new one; concurrent first
//! callers share one dial. Idle connections close after `idle_timeout`; the
//! reaper claims the exact checkout generation it observed idle.
//! Pooled HTTP/1.1: at most two idle keep-alive connections per origin; only
//! a connection whose request and response both completed cleanly returns.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use hyper::client::conn::{http1, http2};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::body::CallBody;
use crate::conn::{self, Connected, Handshake, Origin, Settings};
use crate::failure::Failure;
use crate::gvl::Signal;
use crate::runtime::Tracker;
use crate::tls::{TlsInputs, native_roots};

/// Go's DefaultMaxIdleConnsPerHost.
const MAX_IDLE_H1: usize = 2;
/// core's GRPCProtocol::FIELD_LIMIT.
const GRPC_FIELD_LIMIT: usize = 65_536;

type TlsConfigs = (Arc<rustls::ClientConfig>, Arc<rustls::ClientConfig>);

pub struct Config {
    pub pooled: bool,
    pub idle_timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub settings: Settings,
    pub tls: TlsInputs,
}

pub struct DriverCore {
    pub config: Config,
    tls_configs: tokio::sync::OnceCell<TlsConfigs>,
    pub tracker: Arc<Tracker>,
    origins: Mutex<HashMap<Origin, Arc<OriginEntry>>>,
    closing: AtomicBool,
    /// Fires when admission closes: wakes callers waiting behind a dial.
    pub closed: Arc<Signal>,
    /// Fires on forced teardown: ends every wait of every call.
    pub shutdown: Arc<Signal>,
    inflight: AtomicUsize,
    drained: Notify,
    opened: AtomicUsize,
    next_id: AtomicU64,
}

#[derive(Default)]
pub struct OriginEntry {
    dial: tokio::sync::Mutex<()>,
    state: Mutex<OriginState>,
}

#[derive(Default)]
struct OriginState {
    h2: Option<Arc<H2Conn>>,
    idle_h1: Vec<IdleH1>,
}

struct H2Conn {
    sender: http2::SendRequest<CallBody>,
    shared: Arc<ConnShared>,
    local_port: u16,
}

struct ConnShared {
    id: u64,
    active: AtomicUsize,
    generation: AtomicU64,
    idle: Notify,
    closed: Arc<Signal>,
}

struct IdleH1 {
    id: u64,
    conn: H1Conn,
}

pub struct H1Conn {
    pub sender: http1::SendRequest<CallBody>,
    task: AbortHandle,
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
            inflight: AtomicUsize::new(0),
            drained: Notify::new(),
            opened: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
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

    async fn dial(self: &Arc<Self>, origin: &Origin) -> Result<Connected, Failure> {
        let work = async {
            let tls = if origin.https {
                Some(self.tls_config(self.config.settings.http2).await?)
            } else {
                None
            };
            conn::connect(&self.config.settings, tls, origin, &self.tracker).await
        };
        let connected = match self.config.connect_timeout {
            Some(limit) => tokio::time::timeout(limit, work)
                .await
                .map_err(|_| Failure::new("connect_timeout", "connection setup timed out"))??,
            None => work.await?,
        };
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(connected)
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// A connection for one call. `exclude` names a connection that just
    /// refused or handed back a request: a retry never reuses it.
    pub async fn acquire(
        self: &Arc<Self>,
        origin: &Origin,
        require_h2: bool,
        exclude: Option<u64>,
    ) -> Result<Lease, Failure> {
        if self.is_closing() {
            return Err(Failure::admission());
        }
        if !self.config.pooled {
            let connected = self.dial(origin).await?;
            return self.fresh_lease(connected, None, require_h2);
        }
        let entry = self.entry(origin);
        if let Some(lease) = self.checkout_ready(&entry, require_h2, exclude).await? {
            return Ok(lease);
        }
        // Concurrent first callers wait here for one dial, then share it.
        let _dialing = tokio::select! {
            guard = entry.dial.lock() => guard,
            _ = self.closed.wait() => return Err(Failure::admission()),
        };
        if self.is_closing() {
            return Err(Failure::admission());
        }
        if let Some(lease) = self.checkout_ready(&entry, require_h2, exclude).await? {
            return Ok(lease);
        }
        let connected = self.dial(origin).await?;
        if self.is_closing() {
            return Err(Failure::admission());
        }
        self.fresh_lease(connected, Some(&entry), require_h2)
    }

    /// A checked-out connection ready for a request. A parked HTTP/1.1
    /// connection that closed meanwhile is dropped, and the next one tried.
    async fn checkout_ready(
        self: &Arc<Self>,
        entry: &Arc<OriginEntry>,
        require_h2: bool,
        exclude: Option<u64>,
    ) -> Result<Option<Lease>, Failure> {
        loop {
            let Some(mut lease) = self.checkout(entry, require_h2, exclude)? else {
                return Ok(None);
            };
            let ready = match &mut lease {
                Lease::H1 {
                    conn: Some(conn), ..
                } => tokio::select! {
                    ready = conn.sender.ready() => ready.is_ok(),
                    _ = self.closed.wait() => return Err(Failure::admission()),
                },
                _ => true,
            };
            if ready {
                return Ok(Some(lease));
            }
        }
    }

    fn checkout(
        self: &Arc<Self>,
        entry: &Arc<OriginEntry>,
        require_h2: bool,
        exclude: Option<u64>,
    ) -> Result<Option<Lease>, Failure> {
        let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(conn) = state.h2.as_ref() {
            let usable = !conn.sender.is_closed() && !conn.shared.closed.fired();
            if usable && exclude != Some(conn.shared.id) {
                // Under the state lock, so the reaper's claim sees it.
                conn.shared.active.fetch_add(1, Ordering::AcqRel);
                conn.shared.generation.fetch_add(1, Ordering::AcqRel);
                return Ok(Some(Lease::H2 {
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
        while let Some(idle) = state.idle_h1.pop() {
            if idle.conn.sender.is_closed() {
                continue;
            }
            if require_h2 {
                // The origin negotiated HTTP/1.1; keep the clean socket.
                state.idle_h1.push(idle);
                return Err(Failure::new("alpn", "HTTP/2 was not negotiated"));
            }
            return Ok(Some(Lease::H1 {
                conn: Some(idle.conn),
                entry: Some(entry.clone()),
            }));
        }
        Ok(None)
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
            Handshake::H2 { sender, closed } => {
                let Some(entry) = entry else {
                    return Ok(Lease::H2 {
                        sender,
                        local_port,
                        id,
                        _active: None,
                    });
                };
                let shared = Arc::new(ConnShared {
                    id,
                    active: AtomicUsize::new(1),
                    generation: AtomicU64::new(1),
                    idle: Notify::new(),
                    closed,
                });
                let conn = Arc::new(H2Conn {
                    sender: sender.clone(),
                    shared: shared.clone(),
                    local_port,
                });
                entry.state.lock().unwrap_or_else(|e| e.into_inner()).h2 = Some(conn);
                if let Some(timeout) = self.config.idle_timeout {
                    self.tracker.spawn(reap(
                        Arc::downgrade(entry),
                        shared.clone(),
                        timeout,
                        self.closed.clone(),
                    ));
                }
                Ok(Lease::H2 {
                    sender,
                    local_port,
                    id,
                    _active: Some(Active { shared }),
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
    /// the sender closes it gracefully.
    fn park_h1(self: &Arc<Self>, entry: Arc<OriginEntry>, conn: H1Conn) {
        let id = conn.id;
        {
            let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
            if self.is_closing() || state.idle_h1.len() >= MAX_IDLE_H1 {
                return;
            }
            state.idle_h1.push(IdleH1 { id, conn });
        }
        let Some(timeout) = self.config.idle_timeout else {
            return;
        };
        let closed = self.closed.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(timeout) => {}
                _ = closed.wait() => {}
            }
            // Claims this exact idle connection, if no call took it meanwhile.
            let mut state = entry.state.lock().unwrap_or_else(|e| e.into_inner());
            state.idle_h1.retain(|idle| idle.id != id);
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

    /// Ends every call's waits and every owned task, then waits for the tasks.
    pub async fn force(&self) {
        self.shutdown.fire();
        self.tracker.abort_all();
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
) {
    loop {
        let generation = loop {
            let idle = shared.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if shared.closed.fired() || closed.fired() {
                return;
            }
            if shared.active.load(Ordering::Acquire) == 0 {
                break shared.generation.load(Ordering::Acquire);
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
