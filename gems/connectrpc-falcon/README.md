<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-falcon

**Falcon worker hosting for ConnectRPC with Rack and Rails fallback.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-falcon?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-falcon)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-falcon` serves ConnectRPC services from real
[Falcon](https://github.com/socketry/falcon) process workers. RPCs dispatch
natively through Protocol::HTTP, and every other request goes to an explicit,
ordinary Rack or Rails fallback app on the same listener.

Use it when you want one Falcon deployment to serve both your Rails or Rack
application and its RPCs, or when you need Connect streaming over HTTP/1.1. It
supports:

- Connect unary and streaming: server and client streaming over HTTP/1.1, and
  all four shapes over HTTP/2
- native gRPC and server gRPC-Web
- plaintext, TLS and mTLS, with TLS negotiating `h2` or `http/1.1`
- worker-local, versioned TLS providers with live refresh and drain

For a standalone HTTP/2 host without Falcon, use
[connectrpc-async](../connectrpc-async/README.md), which this package builds on.

The API is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-falcon", "~> 0.3.0"
gem "google-protobuf", ">= 4.36.2", "< 5"
```

Require `connectrpc/falcon`; core and Rails bundles do not load this package.
Falcon's local HTTP/1 resource and the shared Async session use source-coupled
private operations, so use matched `connectrpc-async` and `connectrpc-falcon`
versions.

## Run a service

The [runnable configuration](examples/falcon.rb) uses Falcon's normal service
DSL and Google protobuf's standard `StringValue` message:

```ruby
require "connectrpc/falcon"
require "connectrpc/bindings/google_protobuf"
require "google/protobuf/wrappers_pb"

service "connectrpc-echo" do
  include ConnectRPC::Falcon::Environment

  url { ENV.fetch("CONNECTRPC_URL", "http://127.0.0.1:9292") }
  count 2

  # Application initialization runs in each Falcon worker before TLS loads.
  prepare! do |_instance|
    # Initialize application-owned dependencies here, after fork.
  end

  connectrpc_server do
    method = ConnectRPC::Method.new(service: "demo.EchoService", name: "Echo",
                                    request_type: Google::Protobuf::StringValue,
                                    response_type: Google::Protobuf::StringValue, idempotency: :no_side_effects)
    service = ConnectRPC::Service.new(name: "demo.EchoService", rpcs: [method])
    ConnectRPC::Server.new(services: [service], handlers: { method.path => ->(message, _context) { message } })
  end

  rack_app do
    ->(_env) { [200, { "content-type" => "text/plain" }, ["Ordinary Rack fallback\n"]] }
  end
end
```

In an application bundle containing `connectrpc-falcon` and `google-protobuf`,
run:

```sh
bundle exec falcon host examples/falcon.rb
```

The example binds localhost, starts two workers and serves
`/demo.EchoService/Echo`. In this repository, `mise run falcon:example` runs an
ephemeral version, checks both codecs and the Rack fallback, then stops all
workers.

### Service environment

Include `ConnectRPC::Falcon::Environment` in each service:

- `connectrpc_server` (required) constructs a `ConnectRPC::Server` in the worker.
- `rack_app` supplies the optional fallback.
- `tls` or `tls_provider` supplies the TLS source; see [TLS lifecycle](#tls-lifecycle).
- `server_options` supplies shared limits and host options such as `cache:`,
  `keepalive:`, and since 0.3.0 `max_concurrent_streams:`,
  `request_body_timeout:`, `response_body_timeout:`, `frame_write_timeout:`,
  `stream_receive_window:`, `connection_receive_window:`,
  `dynamic_receive_windows:` and `max_receive_window:`.
- Since 0.3.0, `connectrpc_close_grace` sets the managed worker stop grace in
  seconds (default 5); see [Shutdown](#shutdown).
- `url` describes the listener.

When combining Falcon environments, include them first and
`ConnectRPC::Falcon::Environment` last. With `Falcon::Environment::Rack`
included first, its `config.ru` application becomes the fallback unless the
service defines `rack_app`. Including `ConnectRPC::Falcon::Environment` first
lets Falcon's application environment replace the listener endpoint, so
starting the service raises `ArgumentError` before binding.

Application `prepare!` runs after fork, before static TLS compilation or the
first provider call. Initialize application-owned database clients there; the
library never opens a database connection. Falcon binds raw TCP in the parent
and owns worker count, readiness, health checks and its default restart policy.

### Listener and connection capacity

`server_options` `max_connections:` caps each worker's connections, HTTP/1.1
and HTTP/2, from raw accept until retirement. Since 0.3.0 the default is 1024,
up from 128, and a worker at its cap stops taking peers from the shared listen
backlog, so a sibling worker accepts them, or the peer waits until a slot frees.
Before 0.3.0 a full worker accepted the peer and closed it. Idle connections
hold their slot until `idle_timeout` (default 60 seconds); the
[Async guide](../connectrpc-async/README.md#host) explains that trade-off for
long-lived gRPC clients. The cap bounds resources and is not denial-of-service
protection; limit connections per client in a proxy.

Since 0.3.0, accept errors follow the
[Async Host policy](../connectrpc-async/README.md#accept-errors): `EMFILE`,
`ENFILE`, `ENOBUFS` and `ENOMEM` retry with a warning and a delay from 5 ms
doubling to 1 second, and `ECONNABORTED` and `EINTR` retry at once, instead of
ending the worker's accept loop. `ConnectRPC::Falcon::Endpoint#bound` binds
close-on-exec by default since 0.3.0, so both the parent-bound listener and a
directly bound one stay out of exec'd child processes. Forked workers still
inherit it. Pass `close_on_exec: false` to keep the descriptor across exec.

### Run inside an application-owned Async task

For direct use inside an application-owned Async task:

```ruby
endpoint = ConnectRPC::Falcon::Endpoint.parse("https://127.0.0.1:9292")
host = ConnectRPC::Falcon::Server.new(
  server: connect_server,
  endpoint: endpoint,
  rack_app: ordinary_rack_app,
  tls_provider: -> { application_tls_snapshot }
)
begin
  host.run
  # Run the rest of the application in this reactor.
ensure
  host.close(grace: 5)
end
```

### Interrupts

Handlers and the Rack fallback app run as fibers in the worker's reactor. Stop
their work with RPC deadlines, Async task cancellation, `with_timeout`, or
`Timeout.timeout` from inside the request's task. `Thread#raise` from a
watchdog thread, as Rack::Timeout uses, is not supported. See
[Interrupts inside a reactor](../connectrpc-async/README.md#interrupts-inside-a-reactor).

## Routing and protocols

The native application owns each registered service prefix, including unknown
methods. It separates the raw query from the path for core dispatch. Safe unary
GET uses the same application over HTTP/1 and HTTP/2; the app must route
intended GET calls and own cache/security policy. Native RPC retains the core
opaque identity/gzip bytes, wire/inflated caps, deadlines, metadata and errors.

Falcon's generic content encoding/cache middleware wraps only `rack_app`. RPC
compression and byte limits stay outside it. Native handlers do not inherit
Rails callbacks/authentication. They also bypass the Rails executor and
reloader; see [Rails on Falcon](#rails-on-falcon). Ordinary Rails fallback
preserves its controller, CSRF and session policy.

TLS negotiates `h2` or `http/1.1`; no ALPN selects HTTP/1.1. Plaintext supports
HTTP/2 prior knowledge and HTTP/1. The standalone Async Host remains strict h2.

### Unsupported on this listener

This listener never switches protocols. It ignores HTTP/1.1 `Upgrade` offers,
such as the h2c offer from `curl --http2`, and serves those requests as
ordinary HTTP/1.1 RPC or Rack fallback requests. Their bodies use normal
Content-Length or chunked framing. The connection closes after the response, so
no bytes that follow an offer are parsed as a request, even behind a proxy that
starts tunnelling on the client's offer alone. The Rack fallback still sees the
offer in `rack.protocol` and `HTTP_UPGRADE`, as with plain Falcon. Accepting it
(a 101 status or interim response, a `rack.protocol` or `upgrade` response
header, or full/partial Rack hijack) gets 501 before any upgrade bytes or socket
transfer; other interim responses, such as 103 Early Hints, are sent. The
HTTP/1 connection's response writers refuse a 101 status too, even when code
reaches them through `request.connection`. Writing raw bytes to the connection
stream bypasses HTTP framing and is unsupported. CONNECT, HTTP/2 extended CONNECT and HTTP/2
requests carrying `:protocol` or an `upgrade` field also get 501. WebSockets
and ActionCable are unsupported here. Ordinary enumerable, chunked, streamed
and callable HTTP bodies are supported. Callable bodies must close their
supplied stream according to the Protocol::Rack contract. Plain Falcon and the
existing Rack/Rails integration APIs are unchanged.

### Request input

Native H2 uses the shared Async private bounded ingress for both RPC requests
and ordinary Rack fallback input. Slow or nonreading Rack consumers withhold
stream credit while healthy same-connection streams can advance. Connect
HTTP/1.1 early responses use the bounded
[input retirement policy](#http11-input-retirement). See the
[Async ingress bounds](../connectrpc-async/README.md#backend-boundaries-and-verification)
for padding, cleanup, allocation layers and the limits of this guarantee.

## Rails on Falcon

With a Rails application as the fallback, through `rack_app` or a `config.ru`
application with `Falcon::Environment::Rack`, the native application dispatches
registered RPCs before the fallback: it checks whether the core Server handles
the path and calls the fallback only when it does not. Native handlers
therefore never pass through the Rails middleware stack, so
`ActionDispatch::Executor` and, with reloading enabled,
`ActionDispatch::Reloader` do not run around them. As observed with Rails 8.1
and ActiveRecord, that skips the following:

- Rails does not turn on ActiveRecord's query cache for the RPC.
- A connection that a handler leases for the rest of the request, for example
  with `ActiveRecord::Base.connection` or `lease_connection`, is not returned to
  the pool when the RPC ends. With `config.active_support.isolation_level =
  :fiber`, it stays leased to the finished request fiber until ActiveRecord
  reclaims it. With the default `:thread` isolation, the worker thread keeps it
  checked out between RPCs. Ordinary model queries, such as `Widget.count`,
  check out a connection for each query and return it, with or without the
  executor.
- `ActiveSupport::CurrentAttributes` are not reset after an RPC. With the
  default `:thread` isolation, a value that one handler sets is still visible to
  later RPCs on the same worker thread. With `:fiber` isolation, each RPC runs
  in its own fiber and starts with empty attributes.
- In development, RPCs do not reload changed code. Code that a handler looks up
  during the call picks up a change only after an ordinary Rails request has
  run the reloader. Handler objects that `connectrpc_server` builds, including
  the bound methods that `handlers_for` captures, are created once per worker.
  Even with the interceptor below, a change to their own class needs a worker
  restart; only code they look up per call, such as models, is reloaded.

With the default `:thread` isolation, Rails keeps this state per worker thread,
not per request. A handler that runs while an ordinary Rails request on the same
thread is inside the executor therefore sees that request's executor state,
including its query cache and `CurrentAttributes`.

To run native RPCs the way Rails runs a request, do two things. First, use
fiber isolation:

```ruby
# config/application.rb
config.active_support.isolation_level = :fiber
```

With `:thread` isolation, concurrent RPCs on one worker thread share executor
state and `CurrentAttributes`. When another RPC on the thread already holds the
executor, Rails' wrap runs the block at once without its own executor run, so
the interceptor below cannot isolate those RPCs from each other.

Second, register a server interceptor that runs each RPC inside
`Rails.application.reloader.wrap`. The wrap runs the executor and, when
reloading is enabled, checks for changed code and reloads it as the
application's reloading configuration says:

```ruby
# Runs each native RPC the way a Rails request runs: inside the Rails executor,
# and, when reloading is enabled, after reloading changed code.
class RailsExecution
  def server_unary(_call, &) = wrap(&)
  def server_stream(_call, &) = wrap(&)

  private

  # The executor reports every exception that escapes the wrapped block to
  # Rails.error. A ConnectRPC::Error is a deliberate response, so it leaves
  # after the wrap.
  def wrap
    error = nil
    result = Rails.application.reloader.wrap do
      yield
    rescue ConnectRPC::Error => e
      error = e
    end
    raise error if error

    result
  end
end
```

Pass it to the core Server that `connectrpc_server` builds:

```ruby
connectrpc_server do
  ConnectRPC::Server.new(services: [MyService::SERVICE], handlers: MyService.handlers_for(MyHandlers.new),
                         interceptors: [RailsExecution.new])
end
```

The `ConnectRPC::Error` rescue keeps the executor from reporting deliberate RPC
errors, such as `not_found`, to `Rails.error` as unhandled exceptions. An
unexpected exception that a handler raises is still reported, once. Like any core
[interceptor](../../docs/unary.md#interceptors) or
[stream interceptor](../../docs/streaming.md#interceptors), it covers every RPC
the host serves: unary and all stream kinds on Connect and native gRPC, plus
unary and server-stream gRPC-Web, which has no client or bidi streaming.

It does not cover RPCs that a
[connectrpc-on-rails](../connectrpc-on-rails/README.md#rails-on-falcon)
controller serves. Those go through the Rails application's own middleware
stack, like any other Rails request on this listener.
[Rack fallback completion](#rack-fallback-completion) describes how the listener
completes those requests on their own request fiber, which Rails' executor
needs under `:fiber` isolation.

## Rack fallback caching

Rack fallback caching is disabled by default (`cache: false`). Falcon's fallback
content encoding remains active, and native RPC bypasses both middleware layers.
To enable Falcon's existing shared in-memory cache within each worker, pass
`cache: true` to `ConnectRPC::Falcon::Server.new`. In a service configuration,
use the supported `server_options` path:

```ruby
server_options { { cache: true } }
```

The inherited Falcon `cache` DSL setting does not configure this host. `cache:`
must be `true` or `false`; with no `rack_app`, either value has no effect.

Opt-in uses Falcon's async-http-cache semantics, described here for Falcon
0.57.0 / async-http-cache 0.4.6; newer allowed releases may differ.

- Cache keys use authority, method and path, plus response `Vary` headers.
  Custom authorization headers are not automatically cache keys.
- GET/HEAD requests without a body, `Authorization` or `Cookie` can have 200,
  203, 300, 301, 302, 404 and 410 responses cached without an explicit
  `Cache-Control` header.
- Entries without `max-age` can persist for the worker's lifetime.

Applications must set `Cache-Control` and `Vary` so responses are safe to share.
Use `private` or `no-store` for unshared content; response `no-cache` alone is
insufficient. Use appropriate `Vary` headers for deliberately shared content
that depends on request headers. These are upstream cache semantics, not a
promise of full RFC cache policy support.

## Rack fallback completion

Each Rack fallback request runs in its own request task, a child of the
session's handler task. That task calls `rack_app`, then waits until the
transport closes the response body, and runs the `rack.response_finished`
callbacks and the Rack body's `close` itself, in that order. The app's `call`,
its callbacks and its body close therefore run on one fiber over HTTP/1.1 and
HTTP/2. Body chunks are still read where plain Falcon reads them: by the
connection's writer over HTTP/1.1 and by the stream's output task over HTTP/2.

protocol-rack's body conversion closes some bodies before the callbacks, on the
same fiber, as it does in plain Falcon. It calls `to_ary` on a body that
responds to it, and Rack 3 requires a body that also responds to `close` to
close itself there, as `Rack::BodyProxy` does. It closes the body of a 204, 205
or 304 response that does not respond to `to_ary`. It also closes the body of a
200 response that responds to `to_path`, which is then served from that file.

Rails needs this under `config.active_support.isolation_level = :fiber`.
`ActionDispatch::Executor` completes the request from a `rack.response_finished`
callback, and that completion returns the fiber's ActiveRecord connection,
clears `CurrentAttributes`, pops the execution context and tears down the query
cache for the fiber it runs on. Run elsewhere, it leaves the request fiber's
connection checked out after the fiber ends, which the pool then counts as
dead. Over HTTP/2 this is stricter than plain Falcon, which runs the callbacks
on async-http's output task, because fiber isolation is a Ruby-specific hazard.

The close that starts completion returns only after the callbacks and closes
have finished, so request metrics and HTTP/1.1 admission are released
afterwards, and an HTTP/1.1 keep-alive connection reads its next request only
then. Over HTTP/2, a peer reset releases the stream's admission slot when the
stream closes, which can be before the callbacks finish, as before the request
task existed. A client can still finish reading the response before completion
finishes, as with plain Falcon, which closes the body after the final flush.

Before the request task existed, the closing fiber ran the callbacks itself, so
a cancellation of that fiber and a callback's `Async::Task#defer_cancel` met on
one task. The closer keeps that behavior across the two tasks. It passes each
interruption it receives on to the request task with the force it arrived with.
One that was forced through the closer's own deferred cancellation, as
retirement forces the HTTP/1.1 connection's writer, is delivered even if the
callback defers it, and only once: a callback that rescues it inside its
deferral finishes its cleanup there before the deferral exits. Any other is
delivered once, and the callback defers it or not. When the request task defers
a cancellation the closer passed on, the closer defers one too, so a host that
forces deferring tasks also forces the closer, which passes that force on.
Forced retirement, `abort` and owner shutdown do this, so a callback that defers
cancellation is still interrupted within `close(grace:)`. An HTTP/2 peer reset
cancels the stream's output task only once, so a callback that defers it
finishes its deferred section and is cancelled as it leaves it, as before. A
callback that does not defer sees each passed-on cancellation once. When a
transport task is the closer, retirement also cancels the session's handler
task, which has already returned and so cancels the request task directly. A
callback that rescues a cancellation and keeps waiting can then be cancelled
again. The closer returns, or raises its first interruption, only once the
request task has settled, so request metrics and HTTP/1.1 admission stay held
until completion finishes. An error that a body's `close` raises, an
`Async::Stop` included, reaches the closer as before. A cancellation of the
request task itself does not, unless it arrives while a body's `close` is
blocked: that close raises it, and the closer receives it as the close's error.

If the request task is cancelled or fails while it waits, for example during
forced retirement, `abort` or owner shutdown, it does not close the body or run
callbacks then, because the writer may still be writing the body and closing
it would truncate a response that could look complete. The later close, on
whichever fiber closes the body, then runs completion as before. In that
case completion runs on the closing fiber, so Rails' executor completes there,
with the connection leak this section describes for that request.
`peer_certificate` returns nil in those callbacks when a transport closer, the
HTTP/1.1 connection's writer or the HTTP/2 stream's output task, runs them.
When the session's handler task is the closer, for a HEAD or no-content
response or an abandoned body, the callbacks run on that handler, which keeps
its own client identity.

Completion runs at most once, including for a cached response's shared copies.
It does not run for a response lost inside the session's dispatch after the
Falcon server returned it: the request task waits until retirement cancels it
and then exits without completion, as before the request task existed. It also
does not run for an app-raised `Async::Stop`, described below.

An app that raises a `StandardError` has its registered callbacks run on its
own fiber with a nil status and headers and the error, as protocol-rack does,
and the client gets the usual failure response. An app-raised `Async::Stop`
runs no callbacks. A rejected hijack, protocol switch or 101 response still
gets 501, with the app's body closed first and its callbacks run when the 501
body closes. A response that Falcon's response middleware fails on, such as
content encoding, is closed with that error, so its callbacks run too. A
response the app returns just as or after the session's handler task was
interrupted is completed on the request task with that interruption, for
example an error page that an app renders after rescuing the cancellation, as
Rails' `ActionDispatch::ShowExceptions` does. The client does not receive that
response.

`peer_certificate` also returns the client leaf in the request task, including
inside its `rack.response_finished` callbacks.

One limitation remains over HTTP/2. If the peer resets a stream while its
response HEADERS are being written, the stream closes the body while it holds
the connection's frame-writing guard. A completion callback that then writes a
frame to that same connection waits for that guard, which stalls the
connection's other streams until retirement forces the closer and that force
reaches the request task, within `close(grace:)`. Ordinary Rack
callbacks do not write frames to their own connection.

## TLS lifecycle

### Provide TLS material

A TLS provider returns a complete Hash with exactly these symbol keys:

```ruby
{
  version: "application-owned-opaque-version",
  certificate_chain: [leaf_pem, intermediate_pem],
  private_key: private_key_pem,
  client_ca: [explicit_client_ca_pem]
}
```

Use explicit `client_ca: nil` for server-only TLS. A missing/nil provider result,
missing field, invalid key/chain/trust or source exception fails closed; it never
selects plaintext. Listener scheme and TLS settings must agree. The same
complete configuration without `version` can be supplied as static `tls:`.

TLS keeps the shared Async policy: explicit-only trust anchors, scoped partial
chains, TLS 1.2+, fresh session context per generation, disabled session cache
and `OP_NO_TICKET`. This does not claim that OpenSSL emits zero TLS 1.3 tickets.

### Snapshots and handshakes

The worker caches immutable compiled snapshots. Handshakes perform no provider
or database work. Every raw accept captures one generation before scheduling,
including no-SNI/IP clients and sockets waiting to send their ClientHello.
Pending handshakes keep that captured identity even if a refresh completes.

### Refresh

Provider defaults are `refresh_interval: 60`, `refresh_jitter: 0.1` and
`refresh_timeout: 10`. Scheduling uses monotonic time after completed source
checks, with uniform positive/negative jitter. Concurrent `refresh_tls` callers
join one worker-local attempt. Cancelling one waiter does not cancel that
attempt. Recursive provider refresh/close calls fail safely. These cooperative
timeouts and shutdown bounds cannot preempt arbitrary native or CPU-bound code.
There is no fixed database-outage propagation guarantee or fleet-atomic update.

Every result is validated. Equal versions with equal canonical identity/trust
content preserve generation and context; PEM whitespace and trust-anchor order
do not cause churn. Equal versions with changed content fail. A different
version can deliberately restore previously used material with a new generation.
Failed refreshes retain the last good snapshot and source-check timestamp.
There is no maximum-staleness policy. A candidate whose chain has already
expired fails validation as `:invalid`. Since 0.3.0, an identity that expires
while it is published keeps being served to new and pending handshakes, so a
verifying client fails with a certificate-expired error instead of an EOF, and
each worker reports `:expired` once per generation when the wall clock reaches
the earliest identity-chain `not_after`. It re-reads the wall clock at least
every 60 seconds. Before 0.3.0 expiry rejected new accepts and pending
handshake starts. Established connections retain their negotiated
identity/trust.

### Operations and status

- `refresh_tls` requests an immediate provider check.
- `reload_tls` accepts the three static TLS fields and conflicts with provider
  ownership.
- `tls_generation` is initially 1 for TLS.
- `tls_status` returns safe state, generation, category, `source_check_age` and
  `publication_age` in monotonic seconds; it never exposes the source version,
  certificates, keys or fingerprints. `category` is the last reported event.
  Since 0.3.0, `identity_expires_at` is the served identity chain's earliest
  `not_after` as a frozen `Time`, so `identity_expires_at <= Time.now` tests
  expiry whatever `category` says.
- `on_tls_error:` receives an immutable safe category/status event, including
  `:expired` since 0.3.0. Default diagnostics are fixed safe text; reporter
  exceptions are isolated. Calling `close` or `refresh_tls` from the reporter
  raises `LifecycleError`; `reload_tls` and `drain_tls` work there.

Initial failure prevents successful TLS/application traffic before Falcon
reports startup failure. TCP backlog can still queue connections on the
parent-bound listener.

### Drain and client identity

`drain_tls(grace: 5)` captures a generation cutoff and retires older connections.
Pending older handshakes close immediately. Already admitted responses can
finish. Older HTTP/2 connections get the two-phase GOAWAY drain with debug data
`tls_rotation`, so streams opened before its cutoff are still served and later
ones are refused with `REFUSED_STREAM`. Later HTTP/1 requests get a bare 503.

`peer_certificate` returns a detached authenticated client leaf only in the
exact session handler task and in the exact
[Rack fallback request task](#rack-fallback-completion). Child tasks, other
servers, plaintext and
server-only TLS have no client identity. It does not claim that a
peer-presented chain is a verified chain.

## Shutdown

`close(grace: 5)` and a normal managed worker stop both end accepts and TLS
refresh, then perform bounded connection drain. HTTP/2 connections get the
two-phase GOAWAY drain with debug data `graceful_stop` and keep serving streams
opened before its cutoff; HTTP/1 requests that arrive during the drain get a
bare 503. HTTP/1 completion includes fixed/chunked wire bytes and
response-finished callbacks. On grace expiry, teardown shuts raw TCP down
before cancelling owned work, then performs buffered/TLS cleanup and closes the
descriptor once that work has joined. Callback close is guarded against reentry.
Fatal teardown or owner cancellation outside the managed stop
boundary uses raw-first abort and offers no response-delivery guarantee.
HTTP response completion is separate from TLS `close_notify`; orderly TLS
shutdown is not promised.

Since 0.3.0 the worker cancels and joins every accept task before it closes
each owned listening socket once, so no accept task is still waiting on a
socket as it closes, and concurrent shutdown steps run one at a time. Before
0.3.0 it stopped the accept tasks and closed the sockets without waiting for
the tasks to finish.

Since 0.3.0 a failed teardown step never skips a later one and is never
raised. The worker stops every accept task and closes every owned listening
socket even when one of them fails. An accept task whose cancel fails is not
joined, and its socket stays open. A failed listener step skips neither TLS
shutdown nor the drain, so close still returns the drain's result. Each failure is written as a warning that names
only its class, such as `ConnectRPC teardown error: Errno::EBADF`. A managed
worker that failed keeps its own failure when its `abort` or `close` then
meets a teardown failure. See
[Close and cleanup](../connectrpc-async/README.md#close-and-cleanup).

Since 0.3.0, managed worker stop uses the service's `connectrpc_close_grace`
(default 5 seconds). Keep it below the Falcon container's graceful stop
timeout, after which the container kills workers. `falcon host` uses
async-container's default of 10 seconds, which `ASYNC_CONTAINER_GRACEFUL_STOP`
overrides.

### Close results

`Server#close` returns `bool?`:

- External callers join actual cleanup and receive the final boolean, true for
  drained or false for forced teardown, unless they are interrupted first.
- A resource-owned handler initiating positive-grace close starts the ordinary
  asynchronous drain and returns nil while pending. Admitted siblings keep their
  grace and can continue processing.
- Owned repeated requests and cleanup/close coordinator reentry also return nil
  while pending, without joining cleanup that must join them. Nil neither
  decides the outcome nor publishes completion.
- Repeated requests with a positive grace retain the first closer and expiry
  even when passed a different grace. Since 0.3.0 a repeated zero-grace
  request forces the drain unless its outcome is decided or every connection
  has retired.
- Once closed, all calls return that same boolean, including coordinator
  reentry.
- Zero grace still forces teardown; explicit abort and already-decided outcomes
  are preserved.
- Since 0.3.0 a failed listener step or connection retirement is written as a
  warning and leaves the drain's boolean, and `Server#abort` returns as usual.
- Since 0.3.0 a connection counts as drained once its streams or HTTP/1
  exchange finished within the grace, however the peer leaves afterwards. An
  HTTP/1 response that has been delivered counts as finished during its
  post-response wait, described in
  [HTTP/1.1 input retirement](#http11-input-retirement). Close still waits for
  the peer's EOF, or for that wait, until the grace ends and then forces
  retirement, but neither a missing EOF nor that forced retirement makes the
  result false. `drain_tls` reports its connections the same way. Before 0.3.0
  such a close returned false.

### Connection retirement

External abort and close callers wait for actual connection retirement,
including owned processors, output tasks, cleanup descendants and final
disposal. Owned tasks initiating or reentering abort request retirement without
waiting on cleanup that must join them. A session accepted handle's existing
`wait_closed` operation uses this same boundary, so the active connection metric
and server connection slot remain retained until cleanup completes. Each
external private connection abort joiner retains its own first interruption
through the join. A failed retirement step is reported by the connection's own
cleanup task, never by its joiners.

Since 0.3.0, a `Server#close` or `Server#abort` caller that is cancelled, times
out or is otherwise interrupted gets its exception at once. The drain keeps its
original grace, or the forced retirement keeps running, in a task the worker's
session owner holds. Before 0.3.0 an interrupted close forced retirement and
joined it before raising. A later `close` joins the drain, `close(grace: 0)`
forces it, and cancelling the session owner still forces it. A managed worker
stop that is cancelled while it closes therefore forces the drain when the
worker's task ends. See
[Interrupted server close](../connectrpc-async/README.md#interrupted-server-close).

Private task joins wait until a task and its non-transient descendants have
settled: each task's block, `ensure` included, has returned. A join keeps every
child it has seen joined, through reparenting or a transient flag, and walks
the tree again after each wake, so it also joins a task created while the tree
unwinds. Async may still be stopping a settled task's children or clearing its
fiber, which runs no application code.
A scheduling opportunity after raw failure establishes no completion boundary;
all retained task joins still precede disposal and retirement publication.

### Limits of the grace period

The grace period bounds cooperative drain before forced teardown; it is not a
hard cleanup timer. Non-yielding application/native work can delay retirement
indefinitely. Ordinary owner cancellation leaves independent cleanup running
while an external waiter or lifetime guard keeps the tree alive; terminating the
reactor cannot guarantee completion. H2 client pool cleanup can also outlast an
RPC deadline, as described in the [Async guide](../connectrpc-async/README.md).

Falcon's local HTTP/1 resource and the shared Async session use source-coupled
private completion and lifetime-anchor operations. Use matched Async/Falcon
package versions. These operations add no public API or SPI and change neither
HTTP/1 response timing nor the input retirement limits below.

## HTTP/2 keepalive and connection age

`Server.new(keepalive:)` takes the Async package's `ServerKeepalive` (default
`ServerKeepalive.new`, nil turns it off) and applies it to native HTTP/2
connections exactly as the Async `Host` does: server PINGs, grpc-go PING
enforcement with `too_many_pings`, and the graceful age drain. `idle_timeout`
drains idle HTTP/2 connections gracefully. HTTP/1 connections have no keepalive
and are still closed once idle. See
[Server keepalive](../connectrpc-async/README.md#server-keepalive).

Since 0.3.0, `max_concurrent_streams:` (default 100) and `request_body_timeout:`
(default 60 seconds) also apply to native HTTP/2 connections as on the Async
`Host`: a stream over the connection's limit or over full `max_requests`
admission is refused with `REFUSED_STREAM` while its HEADERS are read, before
any of its DATA, and a stalled request body other than a client or bidi stream
is reset with `CANCEL`. On HTTP/1.1, `request_body_timeout` bounds only the
[read of unread input](#http11-input-retirement) before a Connect or rejected
gRPC-Web response. A
refusal for full admission counts once in `requests_total`. See [Stream
admission](../connectrpc-async/README.md#stream-admission). HTTP/1 requests keep
the bare 503 when admission is full.

Since 0.3.0, `stream_receive_window:` (default 65,536) and
`connection_receive_window:` (default 16 MiB) size native HTTP/2 request
uploads as on the Async `Host`, with the same memory and throughput trade-off.
`dynamic_receive_windows:` (default true) grows them at run time from BDP PINGs
up to `max_receive_window:` (default 1 MiB, below grpc-go's 16 MiB to bound
unread request payload), as on the `Host`.
See [Flow-control windows](../connectrpc-async/README.md#flow-control-windows).
They do not apply to HTTP/1.

Since 0.3.0, native HTTP/2 responses are bounded in the same way as on the Async
`Host`. `response_body_timeout:` (default 60 seconds, nil turns it off) resets
with `CANCEL` a unary, client-streaming or Rack fallback response that waits
that long for flow-control credit; server-streaming and bidi responses are
exempt. Each frame write must finish within `frame_write_timeout:` (default 60
seconds, nil turns it off), or the connection is aborted. On Linux 4.6 and
later, a blocked write is renewed for another bound while the peer keeps
acknowledging data, so a slow reader whose TCP acknowledgements keep advancing
stays connected and one that stops is aborted one to two bounds after the later
of its last progress and the blocked write's start, much like grpc-go's
`TCP_USER_TIMEOUT`. On other platforms and older Linux kernels each write keeps
a fixed deadline, which a slow reader can miss because a blocked write waits for
the peer to drain about 950 KB; raise `frame_write_timeout:` there. Neither
grpc-go nor Go's net/http has such a bound by default. See [Stream
admission](../connectrpc-async/README.md#stream-admission). Neither bound
applies to HTTP/1 connections.

`falcon:interop:grpc` also runs a grpc-go bidi stream through an age drain on
two real workers over TLS. On a second pair of workers with the default
keepalive, the grpc-go default client's large uploads and downloads check that
its BDP PINGs are acknowledged and that the workers send their own and grow
their stream windows without a GOAWAY.

```ruby
server_options do
  { keepalive: ConnectRPC::Async::ServerKeepalive.new(max_connection_age: 1800, max_connection_age_grace: 30) }
end
```

## Native streaming

The core [streaming guide](../../docs/streaming.md) documents all three scoped
Connect streaming shapes, typed generated clients/handlers, early headers,
per-message limits and bounded ownership. Native Async/Falcon HTTP/2 supports
both codecs and identity/gzip over h2c/TLS/mTLS. Falcon additionally serves
Connect server and client streaming over HTTP/1.1, with protobuf/ProtoJSON and
identity/gzip over TCP/TLS/mTLS. The standalone Async Host and original Async
Transport remain H2-only. The explicit `ConnectRPC::Async::HTTP1Transport`
supports Connect unary, safe GET, server streaming and client streaming in an
application-owned reactor.

The streaming guide defines how Falcon reads unread HTTP/1.1 input before its
response and its cooperative 500 ms final-close budget. Native Connect streaming
clients have a separate, provisional one-second absolute HTTP tail policy. It
applies even with a nil RPC deadline and can reject otherwise valid late EOF or
slow intermediaries.

### HTTP/1.1 input retirement

Incomplete H1 uploads disable reuse at response-head resolution. Since 0.3.0,
Falcon then reads the unread body before it writes the response head, as Go's
`net/http` server does, so a client that sends its whole upload before reading
neither deadlocks nor loses the response to a reset on its late write:

- Falcon reads at most 256 KiB, and leaves a declared `Content-Length`
  remainder of 256 KiB or more unread. A chunked read returns at most 64 KiB,
  so the last one can pass the limit by less than that. An input adapter's
  readahead remainder is released, not counted.
- `request_body_timeout` (default 60 seconds) bounds each read wait, and the
  Connect deadline bounds the whole read. With neither, a stalled client holds
  the connection until owner shutdown, as `net/http` without `ReadTimeout` does.
- A stalled, expired or malformed remainder still receives the response.
- While the read runs, Falcon holds the original processor's remaining output
  (its response message and terminal) in memory, so processing never waits on
  the read or delivers a truncated message at its deadline.
- A head published before any output, such as client-stream headers sent
  before the handler reads its input, is written at once, and the remainder is
  read before the first output chunk.
- A bidi 505 and an unanswered `Expect: 100-continue` request skip the read.
  Falcon never sends `100 Continue`.

One absolute final-close expiry is anchored at the actual final HTTP flush plus
500 ms, before body callbacks or lifecycle observation. After those, Falcon
half-closes raw TCP writes and waits without reading until that expiry, like
`net/http`'s `rstAvoidanceDelay`.
Since 0.3.0 a close or `drain_tls` whose grace ends during that wait still
reports the connection drained, and forces raw-first retirement at once.
Following pipelined requests are never parsed or dispatched. Admission/socket
ownership lasts until retirement and original processing join. Earlier owner
shutdown can shorten either phase; failed delivery receives no success grace.

Since 0.3.1, a Connect unary request whose response is ready before its body
has been read gets the same read before the head, with the same limits and
bounds, once processing finishes. As on `net/http`, the connection stays open
for the next request when the read reaches EOF. Otherwise the response carries
`Connection: close`, and the connection closes after the same 500 ms wait. An
unanswered `Expect: 100-continue` request skips the read and closes. Admission
ends with delivery; the socket is held through the wait. A close or `drain_tls`
waits for a stalled unary read until its grace, as it does for streaming input.
Native gRPC unary over HTTP/1, unhandled paths and Rack fallback keep
async-http's handling of unread input. Before 0.3.1 async-http
answered first and then read a unary remainder with no byte cap, bounded only
by `idle_timeout`, or, after a partial chunked read, never read it, so the next
request on that connection waited until `idle_timeout` closed it.

Timers are cooperative and may overshoot under scheduler delay. Holding
admission during the read and the remaining grace increases resource retention.
This is best effort reset avoidance, with no arbitrary network-delivery
guarantee. The [support
contract](../../docs/support.md#falcon-connect-h1-retirement) promises this
behavior and treats the 256 KiB and 500 ms values as provisional tuning values.

### Streaming conformance

CI requires the separate `falcon:conformance:http1` production profiles to pass
all 174 identity and 160 gzip cases on Ruby 3.4.12 and 4.0.9, with unchanged
assertions and inventories. Web rejection-head ordering is repaired separately,
with its gate excluded from Connect delivery. Historical Web
failures and the baseline repair remain recorded in the
[conformance status](../../conformance/README.md#connect-http11-server-profiles).

`falcon:conformance` runs actual server profiles. `falcon:interop:streaming`
exercises real workers with the pinned Go streaming client. Both belong to
`falcon:verify`; duplicated native-client runs are not Falcon server evidence.

## Native gRPC

Native HTTP/2 supports unary, server streaming, client streaming and bidi gRPC
with protobuf, identity/gzip, h2c, verified TLS and mTLS. Configure the core
Client with `protocol: :grpc` and the Server with `:grpc` or `:auto`. Generated
clients and handlers reuse the existing declarations. Portable HTTP/1 execution
retains its early rejection guard. See the [gRPC guide](../../docs/grpc.md) for
status codecs, actual final HEADERS, independent status bounds and native field
retention limits. Connect's one-second terminal tail policy remains Connect-only.

Use `falcon:conformance:grpc` for exact official inventories and
`falcon:interop:grpc` for generated official grpc-go peer checks. Both belong
to the owning verify task. `falcon:interop:grpc` first reproduces the official Go
and Ruby peer outputs through the Falcon bundle, the same check root
`grpc:generation:check` runs through the core bundle. `grpc:status:check`
reproduces private Any/Status descriptors.

## gRPC-Web

Native H1 and H2 support explicitly enabled binary/text unary and server-stream
gRPC-Web. H1 retains original processor identity and admission through
wire/callback completion. Browser incremental delivery uses text. See the
[Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion
rules.

### Rejected HTTP/1 Web input

Since 0.3.1, rejected native Web HTTP/1 input is read as
[Connect input](#http11-input-retirement) is: before the response head, up to
256 KiB, with `request_body_timeout` bounding each read wait and the valid
grpc-timeout captured at original application entry bounding the whole read.
Progress does not extend that deadline. The original processor reads, before it
releases the staged terminal and rejection head, and its reads use
protocol-http1's framed body, which returns at most 64 KiB per read, without
aggregating the upload or consuming the next request. An existing input-adapter
remainder can coexist with one read, each at most 64 KiB; this is not a total
allocation or RSS bound. Successful calls already consume their input and do
not enter this policy.

Without a valid grpc-timeout, only each read wait and the 256 KiB limit bound
the read. A client that sends a byte within every `request_body_timeout` keeps
the processor, its admission slot and the connection until the limit, or until
drain grace or owner shutdown, as `net/http` without `ReadTimeout` does. With
`request_body_timeout: nil` as well, a stalled client holds them until drain
grace or owner shutdown, as for [Connect input](#http11-input-retirement).
Core's own HTTP/1 read before the rejection has no timeout either. The read is
part of original processing, so processing completion, lifecycle observation
and processing-duration metrics for a rejected Web call include it.

When the read reaches EOF, the connection stays open for the next request.
When the remainder is above the limit, stalls, expires or is malformed, the
rejection is still delivered with `Connection: close`, and the connection then
closes after the 500 ms wait without reading. An unanswered
`Expect: 100-continue` request skips the read. Admission ends with delivery.
Cancellation, host drain and owner abort retain control: they retire only the
affected connection, closing raw TCP before body/TLS cleanup, without delivering
the response. The original RPC processing outcome remains separate from aborted
delivery. Over TLS, a client that ends its TCP stream mid-upload without
`close_notify` cannot receive the response: OpenSSL treats that EOF as fatal and
the response write fails, where Go's `crypto/tls` would still write it.

Before 0.3.1 the read had one absolute 100 ms allowance with no byte limit, and
expiry or malformed framing reset the connection, so a client that paused its
upload for longer saw a reset instead of the error. The support contract
classifies this read as
[provisional](../../docs/support.md#falcon-connect-h1-retirement).

## Explicit unary cancellation

Pass `cancellation: source.token` to core or generated client calls, where
`source = ConnectRPC::CancellationSource.new`. The source is permanent and
shareable; only `source.cancel` initiates cancellation. `timeout: nil` without
`deadline:` keeps no RPC deadline or wire timeout. Context offers cooperative `cancelled?`,
`cancellation_reason` and `check!`. See the
[unary guide](../../docs/unary.md#cancellation) for listener, arbitration and
registration lifetime rules.

The Async transport accepts foreign-thread cancellation through a Promise;
only the owner reactor stops private SDK tasks and performs stream cleanup.
Healthy multiplexed siblings survive. A stuck frame retains the one 1-second
raw-first whole-connection cleanup fallback. Native HTTP/2 Context observes
only an actual peer RST_STREAM(CANCEL), before input teardown. Other resets,
normal half-close, connection loss and host shutdown do not fabricate explicit
cancellation. Processing completion detaches the source independently of delivery.

Falcon HTTP/1 and portable Rack/Rails do not add peer detection or application
token env injection. Application handlers remain cooperative.

## Interceptors

Server interceptors configured on the core Server from `connectrpc_server` run
for every native RPC, on HTTP/2 and HTTP/1.1:
[unary](../../docs/unary.md#interceptors) calls through `server_unary`, and
[streams](../../docs/streaming.md#interceptors) through `server_stream`. They
run after core decodes a unary or server-streaming request, around the handler,
with the first interceptor listed outermost. They run in the session's
processing task, so `peer_certificate` works inside them as it does in the
handler.

The native Server's interceptors do not apply to the explicit Rack fallback. A
Rack mount behind it configures its own Server, and a Rails application uses
its controllers'
[`connect_interceptors`](../connectrpc-on-rails/README.md#interceptors) hook.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional
[OpenTelemetry adapter](../connectrpc-otel/README.md). Native hosts create
explicit handles before a request's handler starts, activate the actual handler
tasks, and finish after full-write/reset/cleanup evidence. An HTTP/2 stream
refused at HEADERS because admission is full is observed once after the refusal,
as on the [Async Host](../connectrpc-async/README.md#rpc-observation).
Connection-level setup/admission before an RPC exists is outside this
lifecycle.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`,
`library "connectrpc-async"` and `library "connectrpc-falcon"` to a strict Steep
target with this gem installed. The manifest lists only standard libraries, so
name each connectrpc library explicitly; this also works with `rbs collection`.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

The finite upstream compatibility signatures in `rbs/async_service_0_25` are
explicit opt-ins, separate from normal gem discovery. See the
[RBS guide](../../docs/rbs.md#opt-in-framework-compatibility) for tested
versions, supported framework forms and how to avoid duplicate
application-owned declarations.

## Development

The independent bundle resolves the newest allowed dependencies into
`vendor/falcon` and reports full optional library coverage under
`coverage/falcon/<Ruby version>`. `floors:falcon` repeats specs and package
checks with every runtime dependency at its gemspec floor. `canary:falcon` is the
non-gating pins-lifted lane: it runs the upstream contract spec and the suite with
connectrpc-async's async-http and protocol-http2 patch pins lifted:

```sh
mise run falcon:setup
mise run falcon:test
mise run falcon:check
mise run falcon:verify
```

`falcon:tools`, `falcon:build`, `falcon:package`, `falcon:example` and
`falcon:interop` are separate discoverable leaves. Both Ruby lanes exercise real
localhost protocols and workers. The Go peer uses the repository's independently
pinned Connect/Go graph. This is unary subset evidence, not full conformance.
`falcon:soak` runs the non-gating long-running lifecycle campaign; see
[development](../../docs/development.md#lifecycle-campaign).

## License

This package is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg. It is
released with the whole [release train](../../docs/releasing.md).
