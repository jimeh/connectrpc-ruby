//! Native core of ConnectRPC::Hyper::Transport. Ruby owns protocol logic,
//! deadlines, cancellation and error phrases; this crate owns connections,
//! HTTP/2 streams and blocking waits. Every wait releases the GVL with an
//! unblocking function, and Rust never calls Ruby while the GVL is released.
mod body;
mod call;
mod conn;
mod failure;
mod gvl;
mod pool;
mod runtime;
mod tls;
mod unary;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use magnus::value::{Lazy, ReprValue};
use magnus::{
    Class, Error, ExceptionClass, RArray, RClass, RHash, RModule, RString, Ruby, Value, function,
    method, prelude::*,
};

use crate::call::CallState;
use crate::conn::{Origin, Settings, proxy};
use crate::failure::Failure;
use crate::gvl::{Signal, Waits, drive, drive_uninterruptible};
use crate::pool::{Config, DriverCore, RequestSpec, TimeoutHeader};
use crate::runtime::{ForkGuard, Tracker};
use crate::tls::TlsInputs;

static FAILURE: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    ruby.class_object()
        .const_get::<_, RModule>("ConnectRPC")
        .and_then(|m| m.const_get::<_, RModule>("Hyper"))
        .and_then(|m| m.const_get::<_, RModule>("Native"))
        .and_then(|m| m.const_get("Failure"))
        .expect("ConnectRPC::Hyper::Native::Failure is defined")
});

fn failure_error(ruby: &Ruby, failure: Failure) -> Error {
    let class = ruby.get_inner(&FAILURE);
    let args = (
        failure.detail,
        ruby.to_symbol(failure.kind),
        failure.reset,
        failure.errno,
    );
    match class.new_instance(args) {
        Ok(exception) => exception.into(),
        Err(error) => error,
    }
}

fn arg_error(ruby: &Ruby, message: &'static str) -> Error {
    Error::new(ruby.exception_arg_error(), message)
}

fn settle<T>(ruby: &Ruby, result: Result<Result<T, Failure>, Error>) -> Result<T, Error> {
    result?.map_err(|failure| failure_error(ruby, failure))
}

/// The longest duration native code accepts from Ruby (about 115 days, the
/// largest `connect-timeout-ms` value). Ruby accepts any finite positive
/// number, and `Duration::from_secs_f64` or `Instant + Duration` panic for
/// values above about 1.8e19 s, so every Ruby-supplied duration saturates
/// here. The cap also keeps hyper's own `Instant + interval` keepalive
/// arithmetic far from overflow.
const MAX_SECONDS: f64 = 9_999_999.999;

/// A Ruby-supplied number of seconds as a duration, saturated to
/// `[0, MAX_SECONDS]`; never panics (NaN becomes zero).
fn seconds(value: f64) -> Duration {
    Duration::try_from_secs_f64(value.clamp(0.0, MAX_SECONDS)).unwrap_or(Duration::ZERO)
}

fn instant(seconds: Option<f64>) -> Option<Instant> {
    seconds.map(|s| Instant::now() + self::seconds(s))
}

fn bytes(value: RString) -> Bytes {
    // SAFETY: the slice is copied before any Ruby code can run.
    Bytes::copy_from_slice(unsafe { value.as_slice() })
}

fn headers(
    ruby: &Ruby,
    pairs: RArray,
) -> Result<Vec<(http::HeaderName, http::HeaderValue)>, Error> {
    let mut headers = Vec::with_capacity(pairs.len());
    for pair in pairs.into_iter() {
        let pair = RArray::try_convert(pair)?;
        let name: RString = pair.entry(0)?;
        let value: RString = pair.entry(1)?;
        // SAFETY: both slices are copied before any Ruby code can run.
        let name = http::HeaderName::from_bytes(unsafe { name.as_slice() })
            .map_err(|_| arg_error(ruby, "Invalid request header name"))?;
        let value = http::HeaderValue::from_bytes(unsafe { value.as_slice() })
            .map_err(|_| arg_error(ruby, "Invalid request header value"))?;
        headers.push((name, value));
    }
    Ok(headers)
}

fn header_hash(ruby: &Ruby, map: &http::HeaderMap) -> Result<RHash, Error> {
    let hash = ruby.hash_new_capa(map.keys_len());
    for name in map.keys() {
        let values = ruby.ary_new();
        for value in map.get_all(name) {
            values.push(ruby.str_from_slice(value.as_bytes()))?;
        }
        hash.aset(ruby.str_new(name.as_str()), values)?;
    }
    Ok(hash)
}

fn timeout_header(kind: Option<magnus::Symbol>) -> Result<Option<TimeoutHeader>, Error> {
    let Some(kind) = kind else { return Ok(None) };
    Ok(match kind.name()?.as_ref() {
        "grpc" => Some(TimeoutHeader::Grpc),
        _ => Some(TimeoutHeader::Connect),
    })
}

/// A proxy endpoint Ruby resolved: [host, port, Proxy-Authorization or nil].
type ProxyArg = Option<(String, u16, Option<RString>)>;

fn proxy_route(
    ruby: &Ruby,
    proxy: ProxyArg,
    https: bool,
    http2: bool,
) -> Result<Option<proxy::Route>, Error> {
    let Some((host, port, authorization)) = proxy else {
        return Ok(None);
    };
    let authorization = authorization
        .map(|value| {
            // SAFETY: the slice is copied before any Ruby code can run.
            http::HeaderValue::from_bytes(unsafe { value.as_slice() })
                .map(|mut value| {
                    value.set_sensitive(true);
                    value
                })
                .map_err(|_| arg_error(ruby, "Invalid proxy configuration"))
        })
        .transpose()?;
    Ok(Some(proxy::Route {
        host: host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase(),
        port,
        authorization,
        mode: proxy::mode(https, http2),
    }))
}

#[allow(clippy::too_many_arguments)]
fn request_spec(
    ruby: &Ruby,
    method: &str,
    url: &str,
    pairs: RArray,
    kind: Option<magnus::Symbol>,
    grpc: bool,
    proxy: ProxyArg,
    http2: bool,
) -> Result<RequestSpec, Error> {
    let uri: http::Uri = url
        .parse()
        .map_err(|_| arg_error(ruby, "Invalid request URL"))?;
    let mut origin = Origin::parse(&uri).map_err(|_| arg_error(ruby, "Invalid request URL"))?;
    origin.proxy = proxy_route(ruby, proxy, origin.https, http2)?;
    let method = http::Method::from_bytes(method.as_bytes())
        .map_err(|_| arg_error(ruby, "Invalid request method"))?;
    Ok(RequestSpec {
        method,
        uri,
        origin,
        headers: headers(ruby, pairs)?,
        timeout_header: timeout_header(kind)?,
        grpc,
    })
}

/// A cancellation handle registered with the call's token before the first
/// native wait. `fire` is safe from any Ruby thread. A token created before
/// fork and cancelled in the child does nothing: its Notify lock may have
/// been held by a parent runtime thread at fork time.
#[magnus::wrap(class = "ConnectRPC::Hyper::Native::Cancel", free_immediately, size)]
struct Cancel {
    signal: ForkGuard<Arc<Signal>>,
}

impl Cancel {
    fn new() -> Self {
        Self {
            signal: ForkGuard::new(Signal::new()),
        }
    }

    fn fire(&self) {
        if !self.signal.inherited() {
            self.signal.fire();
        }
    }

    /// For specs: whether this process fired the token.
    fn is_fired(&self) -> bool {
        !self.signal.inherited() && self.signal.fired()
    }
}

#[magnus::wrap(class = "ConnectRPC::Hyper::Native::Driver", free_immediately, size)]
struct Driver {
    core: ForkGuard<Arc<DriverCore>>,
}

impl Driver {
    #[allow(clippy::too_many_arguments)]
    fn new(
        ruby: &Ruby,
        pooled: bool,
        http2: bool,
        idle_timeout: Option<f64>,
        connect_timeout: Option<f64>,
        keepalive: Option<(f64, f64, bool)>,
        roots: RArray,
        chain: Option<RArray>,
        key: Option<RString>,
        tunnel: (f64, usize),
    ) -> Result<Self, Error> {
        let ders = |list: RArray| -> Result<Vec<Vec<u8>>, Error> {
            (0..list.len())
                .map(|index| {
                    list.entry::<RString>(index as isize)
                        .map(|der| bytes(der).to_vec())
                })
                .collect()
        };
        let chain = chain.map(ders).transpose()?;
        let tls = TlsInputs::new(ders(roots)?, chain, key.map(|der| bytes(der).to_vec()))
            .map_err(|input| failure_error(ruby, Failure::new("tls_config", input)))?;
        runtime::sockets::install();
        let config = Config {
            pooled,
            idle_timeout: idle_timeout.map(seconds),
            connect_timeout: connect_timeout.map(seconds),
            settings: Settings {
                http2,
                keepalive: keepalive.map(|(time, timeout, while_idle)| {
                    (seconds(time), seconds(timeout), while_idle)
                }),
                tunnel: proxy::Limits {
                    timeout: seconds(tunnel.0),
                    max_head: tunnel.1,
                },
            },
            tls,
        };
        // The tracker binds the runtime lazily: building a driver starts no
        // thread and opens no socket.
        Ok(Self {
            core: ForkGuard::new(DriverCore::new(config, Tracker::new())),
        })
    }

    fn owned(&self, ruby: &Ruby) -> Result<&Arc<DriverCore>, Error> {
        if self.core.inherited() {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "driver was created before fork",
            ));
        }
        Ok(&self.core)
    }

    /// Connect unary in one native operation: [status, headers, body].
    #[allow(clippy::too_many_arguments)]
    fn unary(
        ruby: &Ruby,
        rb_self: &Self,
        method: String,
        url: String,
        pairs: RArray,
        payload: Option<RString>,
        limit: usize,
        kind: Option<magnus::Symbol>,
        timeout: Option<f64>,
        cancel: &Cancel,
        proxy: ProxyArg,
    ) -> Result<RArray, Error> {
        let core = rb_self.owned(ruby)?.clone();
        let http2 = core.config.settings.http2;
        let spec = request_spec(ruby, &method, &url, pairs, kind, false, proxy, http2)?;
        let payload = payload.map(bytes);
        let deadline = instant(timeout);
        let inflight = core
            .admit()
            .map_err(|failure| failure_error(ruby, failure))?;
        let waits = Waits {
            cancel: Some(&cancel.signal),
            abort: None,
            shutdown: &core.shutdown,
            deadline,
        };
        let outcome = drive(
            ruby,
            &waits,
            unary::unary(core.clone(), spec, payload, limit, deadline),
        );
        drop(inflight);
        let response = settle(ruby, outcome)?;
        Ok(ruby.ary_new_from_values(&[
            ruby.into_value(response.status),
            header_hash(ruby, &response.headers)?.as_value(),
            ruby.str_from_slice(&response.body).as_value(),
        ]))
    }

    /// Opens a streaming exchange without waiting; its first write or
    /// response head connects and dispatches.
    #[allow(clippy::too_many_arguments)]
    fn open(
        ruby: &Ruby,
        rb_self: &Self,
        url: String,
        pairs: RArray,
        kind: Option<magnus::Symbol>,
        timeout: Option<f64>,
        grpc: bool,
        require_h2: bool,
        cancel: &Cancel,
        proxy: ProxyArg,
    ) -> Result<Call, Error> {
        let core = rb_self.owned(ruby)?.clone();
        let http2 = core.config.settings.http2;
        let spec = request_spec(ruby, "POST", &url, pairs, kind, grpc, proxy, http2)?;
        let inflight = core
            .admit()
            .map_err(|failure| failure_error(ruby, failure))?;
        let state = CallState::new(
            core,
            spec,
            require_h2,
            instant(timeout),
            cancel.signal.clone(),
            inflight,
        );
        Ok(Call {
            state: ForkGuard::new(state),
        })
    }

    /// Stops admission, lets in-flight calls finish within `grace`, then
    /// aborts the rest and waits for every owned task to end. True when
    /// everything drained in time.
    fn close(ruby: &Ruby, rb_self: &Self, grace: f64) -> Result<bool, Error> {
        if rb_self.core.inherited() {
            return Ok(true);
        }
        let core = rb_self.core.clone();
        core.begin_close();
        let expires = tokio::time::Instant::now() + seconds(grace);
        // Runs to the end even if the caller is interrupted or a fiber
        // scheduler cancels the wait, so force always follows the grace.
        drive_uninterruptible(ruby, async move {
            let drained = tokio::time::timeout_at(expires, core.wait_drained())
                .await
                .is_ok();
            let quiet = drained
                && tokio::time::timeout_at(expires, core.tracker.wait_empty())
                    .await
                    .is_ok();
            core.force().await;
            quiet
        })
    }

    fn opened(&self) -> usize {
        if self.core.inherited() {
            0
        } else {
            self.core.opened()
        }
    }

    /// [connections dialed, calls in flight, owned tasks running, idle
    /// HTTP/1.1 connections], for specs.
    fn stats(&self) -> (usize, usize, usize, usize) {
        if self.core.inherited() {
            return (0, 0, 0, 0);
        }
        (
            self.core.opened(),
            self.core.inflight(),
            self.core.tracker.len(),
            self.core.idle_h1(),
        )
    }
}

#[magnus::wrap(class = "ConnectRPC::Hyper::Native::Call", free_immediately, size)]
struct Call {
    state: ForkGuard<Arc<CallState>>,
}

impl Drop for Call {
    fn drop(&mut self) {
        if !self.state.inherited() {
            self.state.abort();
        }
    }
}

impl Call {
    fn state(&self, ruby: &Ruby) -> Result<&Arc<CallState>, Error> {
        if self.state.inherited() {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "call inherited across fork",
            ));
        }
        Ok(&self.state)
    }

    fn overlap(ruby: &Ruby) -> Error {
        Error::new(
            ruby.exception_runtime_error(),
            "overlapping or finished stream operation",
        )
    }

    fn waits<'a>(state: &'a Arc<CallState>, deadline: Option<Instant>) -> Waits<'a> {
        Waits {
            cancel: Some(&state.cancel),
            abort: Some(&state.abort),
            shutdown: state.shutdown(),
            deadline,
        }
    }

    fn take_recv(ruby: &Ruby, state: &Arc<CallState>) -> Result<call::RecvHalf, Error> {
        state
            .recv
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| Self::overlap(ruby))
    }

    fn head(ruby: &Ruby, rb_self: &Self, timeout: Option<f64>) -> Result<RArray, Error> {
        let state = rb_self.state(ruby)?;
        let mut recv = Self::take_recv(ruby, state)?;
        let waits = Self::waits(state, instant(timeout));
        let outcome = drive(ruby, &waits, state.head(&mut recv));
        let head = recv.head.as_ref().map(|head| {
            (
                head.status,
                header_hash(ruby, &head.headers),
                head.end_stream,
            )
        });
        state.restore(&state.recv, recv);
        settle(ruby, outcome).inspect_err(|_| state.mark_failed())?;
        let (status, headers, end_stream) = head.expect("head after success");
        Ok(ruby.ary_new_from_values(&[
            ruby.into_value(status),
            headers?.as_value(),
            ruby.into_value(end_stream),
        ]))
    }

    fn read(
        ruby: &Ruby,
        rb_self: &Self,
        max: usize,
        timeout: Option<f64>,
    ) -> Result<Option<RString>, Error> {
        let state = rb_self.state(ruby)?;
        let mut recv = Self::take_recv(ruby, state)?;
        let waits = Self::waits(state, instant(timeout));
        let outcome = drive(ruby, &waits, state.read(&mut recv, max));
        state.restore(&state.recv, recv);
        let chunk = settle(ruby, outcome).inspect_err(|_| state.mark_failed())?;
        Ok(chunk.map(|bytes| ruby.str_from_slice(&bytes)))
    }

    fn write(
        ruby: &Ruby,
        rb_self: &Self,
        data: RString,
        timeout: Option<f64>,
    ) -> Result<Value, Error> {
        let state = rb_self.state(ruby)?;
        let mut send = state
            .send
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| Self::overlap(ruby))?;
        let waits = Self::waits(state, instant(timeout));
        let outcome = drive(ruby, &waits, state.write(&mut send, bytes(data)));
        state.restore(&state.send, send);
        settle(ruby, outcome).inspect_err(|_| state.mark_failed())?;
        Ok(ruby.qnil().as_value())
    }

    fn close_send(ruby: &Ruby, rb_self: &Self) -> Result<Value, Error> {
        rb_self.state(ruby)?.close_send();
        Ok(ruby.qnil().as_value())
    }

    fn trailers(ruby: &Ruby, rb_self: &Self) -> Result<Option<RHash>, Error> {
        let state = rb_self.state(ruby)?;
        let guard = state.recv.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref().and_then(|recv| recv.trailers.as_ref()) {
            Some(trailers) => Ok(Some(header_hash(ruby, trailers)?)),
            None => Ok(None),
        }
    }

    /// Reads the rest of the response (discarding data), then releases the
    /// call and its connection.
    fn finish(ruby: &Ruby, rb_self: &Self, timeout: Option<f64>) -> Result<Value, Error> {
        let state = rb_self.state(ruby)?;
        if state.is_finished() {
            return Ok(ruby.qnil().as_value());
        }
        let mut recv = Self::take_recv(ruby, state)?;
        let waits = Self::waits(state, instant(timeout));
        let outcome = drive(ruby, &waits, state.drain(&mut recv));
        state.restore(&state.recv, recv);
        settle(ruby, outcome).inspect_err(|_| state.mark_failed())?;
        state.complete();
        Ok(ruby.qnil().as_value())
    }

    fn abort(&self) {
        if !self.state.inherited() {
            self.state.abort();
        }
    }

    fn local_port(&self) -> u16 {
        if self.state.inherited() {
            0
        } else {
            self.state.local_port()
        }
    }

    fn is_h2(&self) -> bool {
        !self.state.inherited() && self.state.is_h2()
    }

    fn is_finished(&self) -> bool {
        self.state.inherited() || self.state.is_finished()
    }
}

fn wait_count() -> u64 {
    gvl::wait_count()
}

fn active_waits() -> usize {
    gvl::active_waits()
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let connectrpc = ruby.define_module("ConnectRPC")?;
    let hyper = connectrpc.define_module("Hyper")?;
    let native = hyper.define_module("Native")?;
    native.define_singleton_method("wait_count", function!(wait_count, 0))?;
    native.define_singleton_method("active_waits", function!(active_waits, 0))?;

    let cancel: RClass = native.define_class("Cancel", ruby.class_object())?;
    cancel.define_singleton_method("new", function!(Cancel::new, 0))?;
    cancel.define_method("fire", method!(Cancel::fire, 0))?;
    cancel.define_method("fired?", method!(Cancel::is_fired, 0))?;

    let driver: RClass = native.define_class("Driver", ruby.class_object())?;
    driver.define_singleton_method("new", function!(Driver::new, 9))?;
    driver.define_method("unary", method!(Driver::unary, 9))?;
    driver.define_method("open", method!(Driver::open, 8))?;
    driver.define_method("close", method!(Driver::close, 1))?;
    driver.define_method("opened", method!(Driver::opened, 0))?;
    driver.define_method("stats", method!(Driver::stats, 0))?;

    let call: RClass = native.define_class("Call", ruby.class_object())?;
    call.define_method("head", method!(Call::head, 1))?;
    call.define_method("read", method!(Call::read, 2))?;
    call.define_method("write", method!(Call::write, 2))?;
    call.define_method("close_send", method!(Call::close_send, 0))?;
    call.define_method("trailers", method!(Call::trailers, 0))?;
    call.define_method("finish", method!(Call::finish, 1))?;
    call.define_method("abort", method!(Call::abort, 0))?;
    call.define_method("local_port", method!(Call::local_port, 0))?;
    call.define_method("h2?", method!(Call::is_h2, 0))?;
    call.define_method("finished?", method!(Call::is_finished, 0))?;
    Ok(())
}
