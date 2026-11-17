<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-async

**Async HTTP/2 host and HTTP/1.1 and HTTP/2 clients for ConnectRPC.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-async?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-async)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-async` provides optional HTTP/2 hosting and explicit HTTP/1.1 and
HTTP/2 client transports built on [Async](https://github.com/socketry/async).
It supports Connect unary and streaming, native gRPC and server gRPC-Web,
identity and opt-in core gzip, protobuf and ProtoJSON, h2c prior knowledge, and
TLS with verified peers and ALPN `h2`. Core, Rails and Rack remain independent;
require `connectrpc/async` explicitly.

Pick the component that matches what you need:

| Component | Use it to |
| --- | --- |
| [`Host`](#host) | Serve RPCs from a standalone HTTP/2 listener (h2c, TLS or mTLS) inside your Async reactor |
| [`Transport`](#transport) | Call HTTP/2 services from inside an Async reactor, with pooled, multiplexed connections, streaming and gRPC |
| [`ThreadedTransport`](#ordinary-threads) | Call Connect unary and server-streaming services, or native gRPC unary and server-streaming services, over HTTP/2 from ordinary threads, such as Puma or Sidekiq workers, without running a reactor yourself |
| [`HTTP1Transport`](#explicit-http11-client) | Call Connect services that only speak HTTP/1.1, including unary, server streaming and client streaming |
| [`Application`, `ServerSession`](#native-application-and-managed-tls) | Build another host on the native application and session, as [connectrpc-falcon](../connectrpc-falcon/README.md) does |

To serve HTTP/1.1 and HTTP/2 from Falcon workers next to a Rack or Rails app, use
[connectrpc-falcon](../connectrpc-falcon/README.md).

This package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

```ruby
gem "connectrpc-async", "~> 0.2.0"
```

## Quick start

The example assumes existing core `server`, `binding`, `method` and `message`
objects, as shown in the repository unary examples. It starts a host on an
ephemeral port, calls it over h2c and closes both ends:

```ruby
require "async"
require "connectrpc/async"

Async do
  host = ConnectRPC::Async::Host.new(server: server).start
  origin = "http://127.0.0.1:#{host.port}"
  transport = ConnectRPC::Async::Transport.new(origin: origin)
  client = ConnectRPC::Client.new(base_url: origin, binding: binding, transport: transport)
  begin
    client.call(method, message)
  ensure
    transport.close
    host.close
  end
end.wait
```

In this repository, `mise run async:example` runs the generated localhost
example. The [runnable examples](../../examples/README.md) include Async
servers and HTTP/1.1 and HTTP/2 clients, with mutual TLS.

## Reactor ownership

The application owns a long-lived reactor task. Construction, networking and
lifecycle operations must use its reactor and thread. There is no hidden
synchronous reactor thread; [`ThreadedTransport`](#ordinary-threads) is the
explicit, separate object for ordinary threads.

Scheduler-compatible I/O yields; CPU, native codecs and native DNS may block.
Ensure graceful close before the owner exits. Owner cancellation aborts only
owned sockets/tasks and makes no delivery promise. A client transport that is
never closed is aborted, without a graceful drain, once its owner task's block
and every other task under that owner have finished, so the owner can finish.
A live task under the owner keeps the transport open. Applications own the
Async reactor, authorization and lifecycle. Using an object from the wrong
reactor, thread or state raises `ConnectRPC::Async::LifecycleError`. Since
0.3.0, that constant is public, so applications can rescue it by name.

## Host

`Host.new(server:, host: "127.0.0.1", port: 0, tls: nil, tls_provider: nil,
max_connections: 1024, max_requests: 128, max_concurrent_streams: 100,
request_body_timeout: 60, response_body_timeout: 60,
frame_write_timeout: 60, handshake_timeout: 10, idle_timeout: 60,
refresh_interval: 60, refresh_jitter: 0.1, refresh_timeout: 10,
on_tls_error: nil, keepalive: ServerKeepalive.new, fallback: nil,
stream_receive_window: 65_536, connection_receive_window: 16_777_216)` returns
a host. `fallback:`, `max_concurrent_streams:`, `request_body_timeout:`,
`response_body_timeout:`, `frame_write_timeout:`, `stream_receive_window:` and
`connection_receive_window:` are available since 0.3.0. The receive windows size request uploads; see
[Flow-control windows](#flow-control-windows).

- `start` binds/listens before returning self; `port` and `addresses` expose the
  actual binding. Since 0.3.0, every address `host` resolves to listens on one
  port. With `port: 0` the kernel picks it for the first address; if a later
  address already has that port in use, the Host closes what it bound and
  retries the whole bind up to 5 times before raising. Listeners are
  close-on-exec, so exec'd child processes do not keep them open.
- States are new/running/closing/closed; hosts cannot restart.
- Connection admission happens before TLS/preface; see
  [Connection capacity](#connection-capacity). Request admission is
  described in [Stream admission](#stream-admission).
- Setup has one combined cooperative timeout.
- `idle_timeout` counts from when a connection's last stream and handler
  finished, or from its start, and then drains the connection gracefully; see
  [Server keepalive](#server-keepalive). PINGs do not extend it. Nil disables it.
  The 60-second default frees the connection slots that idle clients hold, in
  the range of nginx's `keepalive_timeout` (75 seconds). TLS session resumption
  is off, so an idle gRPC client reconnects with a full TLS handshake after 60
  seconds. grpc-go's `MaxConnectionIdle` defaults to infinity; for long-lived
  gRPC clients, raise `idle_timeout` or set it to nil and rely on
  [server keepalive](#server-keepalive) PINGs to find dead peers.

The Host serves HTTP/2 only. There is no HTTP/1 listener or automatic replay.
For TLS options, see [TLS and client authentication](#tls-and-client-authentication).

### Connection capacity

`max_connections` caps the connections the Host holds, from raw accept until
the connection's retirement completes, including pending handshakes and
draining connections. Since 0.3.0, a listener stops accepting while the Host is
at the cap, as Go's `netutil.LimitListener` does: a new peer waits in the
kernel's listen backlog and is accepted on the same TCP connection as soon as a
slot frees, on whichever listener it reached. Before 0.3.0 the Host accepted and
immediately closed it. The default is 1024 since 0.3.0, up from 128. Go's
`net/http` and grpc-go have no connection cap, and nginx's common
`worker_connections` setting is 1024, which also matches the usual soft limit on
open files.

`max_connections` bounds resources; it is not denial-of-service protection. A
peer that never sends its ClientHello or connection preface holds a slot for
`handshake_timeout`, and idle connections hold theirs until `idle_timeout`.
Limit connections per client in a proxy in front of the Host.

### Stream admission

`max_requests` caps the requests admitted across the whole host. Since 0.3.0,
each HTTP/2 connection advertises the smaller of `max_concurrent_streams`, an
integer from 1 to 2^32-1, and `max_requests` as its
`SETTINGS_MAX_CONCURRENT_STREAMS`, and enforces it whether or not the client has
acknowledged its SETTINGS. Keep `max_concurrent_streams` below `max_requests`,
as the defaults do, so that one connection cannot take every request slot. When
`max_requests` is 100 or less, the default per-connection limit equals it, so
set `max_concurrent_streams` lower explicitly.

- A stream over its connection's limit, or one that arrives while host
  admission is full or the host is aborting, is reset with
  `RST_STREAM(REFUSED_STREAM)`. That tells the client the request was not
  processed and is safe to retry. The handler is not called. The server
  decides while it reads the stream's HEADERS, before it reads any of the
  stream's DATA, as grpc-go and Go's `golang.org/x/net/http2` refuse streams
  over their limits. An admitted stream takes its request slot at that point,
  so streams whose HEADERS arrive together take free slots in order and one
  free slot admits exactly one of them. A slot taken this way is released if
  the peer resets the stream before its handler starts; that stream is then
  neither processed nor counted as a refusal. The server still
  decodes a refused stream's header block and ignores its later frames, and
  the connection returns credit for any DATA the client already sent, so the
  connection and its other streams see no error. Metrics and traces record a
  refusal for full host admission or an abort once as `admission_rejected`,
  without an HTTP status; its delivery completes once the reset is written. A
  stream over the connection's limit is refused the same way and is not
  observed. A stream refused for full host admission counts toward that limit
  until its reset is written, so a client that ignores the limit cannot keep
  more observed refusals waiting on one connection than the limit allows.
- Since 0.3.0, `request_body_timeout` bounds how long a handler waits for
  request body bytes on an admitted HTTP/2 stream, like nginx's
  `client_body_timeout`. Each wait starts again when body bytes arrive; empty or
  padding-only DATA frames do not count. When a wait exceeds it, the server
  resets the stream with `RST_STREAM(CANCEL)`, the handler's read fails, and the
  stream's admission slot is released. Client-streaming and bidi RPCs are
  exempt, since they may idle between messages; keepalive and deadlines bound
  them. The value is finite positive seconds; nil turns the bound off.
- Since 0.3.0, `response_body_timeout` bounds each wait of a response body on an
  admitted HTTP/2 stream for flow-control credit, like nginx's `send_timeout`.
  A wait starts when the stream's or the connection's window is used up and
  ends when credit for this stream arrives; a WINDOW_UPDATE that leaves the
  stream without credit does not restart it. When a wait exceeds the bound, the
  server resets the stream with `RST_STREAM(CANCEL)`, the call finishes as not
  delivered, and the stream's admission slot is released. The connection and
  its other streams continue. Unary and client-streaming RPCs and fallback
  responses are bounded. Server-streaming and bidi RPCs are exempt, since their
  consumers may apply backpressure for as long as they need. The value is
  finite positive seconds; nil turns the bound off.
- Since 0.3.0, `frame_write_timeout` bounds each frame write on an HTTP/2
  connection, once it holds the connection's frame guard. The default is 60
  seconds, independent of keepalive. This covers response HEADERS, DATA,
  trailers and END_STREAM, resets and control frames, for every RPC shape and
  whatever `response_body_timeout` is. A peer that stops reading its socket
  fills the server's send buffer and stalls the write; the connection is then
  aborted raw socket first, which cancels its handlers and releases all of its
  slots. The bound applies to each frame or header block, not to a whole
  response, and steady progress within one write does not extend it. A write
  finishes when the kernel accepts its bytes. Once the socket's send buffer is
  full, the kernel accepts more only after the peer has drained part of it,
  which on Linux can be a third of the buffer, up to about 1 MiB with the
  default 4 MiB maximum. So a peer that keeps reading, but too slowly to drain
  that much within the bound, is also aborted. Unlike grpc-go's
  `TCP_USER_TIMEOUT`, which fires when sent data stays unacknowledged, or unsent
  behind a zero window, for that long, so that any progress resets it, this
  bound is not reset by partial progress. Control frames queued behind a stalled
  write, such as the ACKs of a client's BDP or keepalive PINGs or a drain's
  GOAWAY and PING, wait for that write's bound instead of failing the connection
  at the keepalive `timeout` first, and their deadlines count again once it
  finishes. A control frame can therefore wait for several frame writes in turn,
  each within its own bound, while the peer keeps reading. Raise
  `frame_write_timeout` for clients that read large responses over slow links.
  nil turns the bound off; a control frame queued behind a stalled write then
  still ends the connection after the keepalive `timeout`, or 20 seconds without
  keepalive. Otherwise the value is finite positive seconds. Application work
  under the guard, such as closing a HEAD response's body, does not count as a
  frame write.

These two response bounds go beyond grpc-go and Go's `golang.org/x/net/http2`,
which have no equivalent. grpc-go waits for flow-control credit without a bound
and leaves dead peers to keepalive and `TCP_USER_TIMEOUT`; x/net/http2 also has
no credit bound, and its `WriteByteTimeout` is off by default. This package has
them because it owns its HTTP/2 server layer on top of async-http, where a frame
write that defers cancellation can only be ended by failing its connection, and
because a stuck connection costs more in a Ruby process. One process serves all
of a worker's connections within a fixed `max_requests`, so slots, fibers and
buffered bodies that a non-reading peer holds come out of the capacity every
other client shares. Both defaults are a generous 60 seconds so that slow but
live peers are rarely cut off, and nil turns either bound off.

These limits do not bound every way to hold a slot:

- Idle client-streaming and bidi calls have no idle bound by design, as in
  grpc-go and connect-go, because a bound would end legitimate long-lived
  streams such as subscriptions. A call deadline, `max_connection_age` with
  `max_connection_age_grace` (both off by default) or `close` ends them.
  Keepalive only proves that the peer still answers, and
  `max_concurrent_streams` limits how many streams a connection has, not how
  long they last, so by default a responsive peer can hold these slots
  indefinitely.
- Server-streaming and bidi responses may wait for flow-control credit
  indefinitely. A peer that keeps reading frames such as PINGs but never
  grants the stream window holds its slot until a call deadline,
  `max_connection_age` with `max_connection_age_grace`, or `close` ends the
  call. Keepalive does not, because the peer keeps answering PINGs. A peer that
  stops reading its socket altogether hits the frame write bound instead.
- A peer that grants a little credit just within each `response_body_timeout`
  keeps a bounded stream, since each grant starts a new wait, as with nginx's
  `send_timeout`. A call deadline, `max_connection_age` with
  `max_connection_age_grace`, or `close` ends it.

### Fallback application

Since 0.3.0, the Host accepts a fallback application.

`fallback:` takes a native Protocol::HTTP callable, `call(request)` returning a
`Protocol::HTTP::Response`, like [Application](#application)'s fallback. It is
not a Rack app. Core keeps whole registered service prefixes, including unknown
methods under them, so RPC traffic never reaches the fallback; every other
request goes to it with its method, path and query intact. Without `fallback:`,
unrelated paths get a bare 404. A non-callable fallback raises `ArgumentError`.

Fallback requests share the Host's [stream admission](#stream-admission),
including the request body bound, and connection lifetime. An exception from the
fallback fails only its own stream. The Host owns no tunnels or socket handoff,
so no `CONNECT` request reaches the fallback. An extended CONNECT (`:protocol`)
request gets a bare 501. A plain CONNECT has no `:path`, so the Host resets its
stream, with or without a fallback. A fallback response with status 101 or a
`protocol` is closed and replaced by 501.

The fallback inherits the Host's HTTP/2-only listener, so it serves only
HTTP/2 clients: h2c with prior knowledge, or TLS with ALPN `h2`. For example,
one Host can serve RPCs and Prometheus metrics:

```ruby
require "prometheus/client/formats/text"

metrics_app = lambda do |request|
  next Protocol::HTTP::Response[404, {}, nil] unless request.path == "/metrics"

  Protocol::HTTP::Response[200, { "content-type" => Prometheus::Client::Formats::Text::CONTENT_TYPE },
                           [Prometheus::Client::Formats::Text.marshal(Prometheus::Client.registry)]]
end
host = ConnectRPC::Async::Host.new(server: server, tls: tls, fallback: metrics_app).start
```

Prometheus scrapes a TLS Host over h2. A cleartext h2c Host is not reachable by
HTTP/1.1 clients, so those still need a separate listener or the
[Falcon package](../connectrpc-falcon/README.md), which serves HTTP/1.1 and
HTTP/2.

### Supervise with `wait`

`Host#wait` supports application supervision:

```ruby
Async do
  host = ConnectRPC::Async::Host.new(server: server).start
  begin
    host.wait # Ordinary listener stop returns nil; terminal failure raises.
  ensure
    host.close # Await session drain separately from ordinary listener stop.
  end
end.wait
```

Before start it raises `LifecycleError`, unless the Host was already closed.
Ordinary shutdown returns nil after all listener tasks stop; session retirement
may still be running. External `close(grace:)` callers await the session's
boolean drain outcome; resource-owned calls can return nil while shutdown is
pending, as described in [Close results](#close-results). Terminal listener
failure instead makes `wait` raise the identical original exception after
cooperative session abort, listener joins and actual finalization of its stored
private exit task. Repeated/concurrent waits share that outcome; cancelling a
waiter does not close the Host. Like other lifecycle operations, `wait`
requires the owning reactor and thread.

### Accept errors

Since 0.3.0, accept errors that come from exhausted resources never stop a
listener on their own, as in Go's `net/http` `Server.Serve` and grpc-go:

- `EMFILE`, `ENFILE`, `ENOBUFS` and `ENOMEM` retry indefinitely with a
  cooperative delay that starts at 5 ms, doubles and is capped at 1 second.
  Each delay writes one warning, such as
  `ConnectRPC accept error: Errno::EMFILE; retrying in 0.005s`, so a sustained
  failure writes about one line per second per listener. A successful raw
  accept resets that listener's delay.
- `ECONNABORTED` and `EINTR` retry at once, after one turn of the scheduler so
  that a stream of them cannot starve established connections or `close`. They
  do not affect the delay.

Admitted RPCs and other listeners keep running during a delay, and `close`
ends it. Other accept errors and unexpected listener errors are terminal, as
described below. No RPC is replayed and no retry policy knobs are exposed.

### Terminal failure and startup

The first terminal listener cause closes every listener and forces raw-first
session abort with zero drain time, then cooperative cancellation and joins.
Server abort has no cleanup deadline and cannot forcibly interrupt nonyielding
or cancellation-resistant application code. The stored exit task runs outside
the session lifetime under the application owner and survives ordinary owner
cancellation; `wait` joins its actual finalization before reporting the cause.
Ordinary owner cancellation stops accepting and resolves ordinary waits after
listener finalization, while session retirement may still be running. `close`
retains its drain result and does not raise the stored listener cause.

A terminal cause already observed during startup is raised by `start` after
cleanup; bind/initial setup errors remain synchronous and subsequent `wait`
returns nil. The Host cannot restart after close or failure.

- Caller-context validation, including the TLS provider and `on_tls_error`
  recursion guard, runs in the calling task before startup cleanup: a recursive
  initial `start` raises `LifecycleError` without creating the exit task or
  aborting the outer startup.
- An overlapping `start` from another task while startup is still in progress
  raises `LifecycleError` before binding, so it cannot leak a second listener
  set, reset startup state or abort the first start.
- Startup failure uses that same exit task and preserves its first caller/setup
  error, or an already recorded terminal cause, through repeated caller
  interruption.
- When an independent ordinary close already owns shutdown, startup exit stops
  only listeners and late acquisitions. It preserves that close's admitted
  RPCs, original grace and outcome without awaiting its peer drain.

When binding one of several resolved addresses fails, `start` closes the
listeners it already bound and raises the bind error.

### Server keepalive

`keepalive:` takes a `ServerKeepalive` and follows grpc-go's server keepalive.
The default is `ServerKeepalive.new`; nil turns off server PINGs, PING
enforcement and the age limit:

```ruby
keepalive = ConnectRPC::Async::ServerKeepalive.new(
  time: 7200, timeout: 20, min_time: 5, permit_without_calls: true,
  max_connection_age: 1800, max_connection_age_grace: 30
)
host = ConnectRPC::Async::Host.new(server: server, keepalive: keepalive)
```

All values are finite seconds. `time`, `min_time` and `max_connection_age` are
positive or nil, `timeout` is positive, and `max_connection_age_grace` is
nonnegative or nil and requires `max_connection_age`.

- **Server PINGs.** After `time` seconds without reading a frame, the server
  sends a PING. Without the matching ACK within `timeout`, the connection is
  aborted raw socket first and its handlers are cancelled. `time: nil` turns
  them off.
- **PING enforcement.** A client PING less than `min_time` after the previous
  one is a strike. Without open streams and unless `permit_without_calls`, any
  PING within 2 hours of the previous one is. A server HEADERS or DATA write
  forgives the next PING and clears the strikes, and the first PING never
  strikes. The third strike gets GOAWAY `ENHANCE_YOUR_CALM` with debug data
  `too_many_pings`, naming the highest stream already received. The connection
  then half-closes, waits up to 1 second for EOF and closes; streams in progress
  are not waited for. `min_time: nil` turns enforcement off. The defaults accept
  every grpc-go client configuration and still stop floods. grpc-go clients ping
  at most every 10 seconds, and the 5-second `min_time` leaves room for network
  jitter, which an idle connection would otherwise accumulate as strikes. This
  matches the server in grpc-go's keepalive example. grpc-go's own server
  default, 5 minutes and no PINGs without calls, is stricter.
- **Connection age.** After `max_connection_age` seconds, +/-10% jitter, the
  connection drains gracefully. `max_connection_age_grace` seconds after age
  expiry, whatever the drain's progress, it is aborted; nil waits indefinitely.
- **Control frames.** Every HTTP/2 connection writes its GOAWAYs, drain PINGs
  and stream resets, and with keepalive its PING ACKs too, through a
  per-connection control writer, so the reader never blocks writing its own
  replies. The reader can still stall at its next buffer refill, which flushes
  pending output, behind an outbound write that does not finish. Each control
  frame must be written within `timeout` seconds of being queued, including any
  wait behind another stream's stuck frame, and its own write within the 100 ms
  cleanup allowance; otherwise the connection is aborted. Since 0.3.0, a stuck
  frame write that `frame_write_timeout` bounds is the exception: control frames
  waiting behind it wait for its bound, and their deadlines count again from
  when it finishes. At most 1024 control frames wait; a peer that outruns the
  writer, such as a PING flood with `min_time: nil`, fails the connection.
  Each refused stream queues one reset there. A compliant client counts a
  stream as open until its reset arrives, so it can reach that cap with
  refused streams only when the connection's stream limit is above about
  1,000.
  Server PINGs, age grace and close also bound a stuck connection. The
  `too_many_pings` GOAWAY is stricter: it must be written within 100 ms of the
  third strike, or the connection is aborted. Without keepalive, the reader
  answers client PINGs itself, and control frames use the 20-second default
  `timeout`.
- **Frame writes.** Since 0.3.0, frame writes have their own bound,
  `frame_write_timeout:` on the host, independent of `timeout`; see [Stream
  admission](#stream-admission).

Idle and age expiry, session `close` and `drain_tls` drain a connection in
three steps, as grpc-go does:

1. A GOAWAY with last stream 2^31-1 and debug data `max_idle`, `max_age`,
   `graceful_stop` (close) or `tls_rotation` (`drain_tls`), followed by a PING.
   Requests keep dispatching normally.
2. When that PING's ACK arrives, or after 5 seconds, the highest stream received
   becomes the cutoff and a second GOAWAY names it. A stream the client opens
   after the cutoff is still decoded, including HPACK, CONTINUATION and
   connection flow control, but is reset with `REFUSED_STREAM` and never
   dispatched. Its siblings see no connection error.
3. When the accepted streams and their handlers finish, the server half-closes
   and discards input until EOF, for up to 1 second, then closes.

Session `close` and `drain_tls` start this drain at once on every ready HTTP/2
connection, or take over one in progress, keeping its debug data and cutoff and
never raising the cutoff. Once the cutoff GOAWAY is written, they wait for the
accepted streams, send a final GOAWAY naming the cutoff, half-close and wait for
EOF, all within their own grace. A peer that closes or resets the connection
after its streams finished, as the GOAWAY asks, counts as drained. A
`too_many_pings` decision keeps its own 1-second EOF bound during a takeover.
Every write in these steps follows the control frame rules above.

Rails and Rack hosts cannot offer any of this; see
[support](../../docs/support.md#http2-keepalive-and-connection-age).

`async:interop:keepalive` checks keepalive in both directions against the
pinned grpc-go peer over h2c, TLS and mTLS. The Ruby client is checked against
grpc-go PING enforcement, both compliant and over-eager with `too_many_pings`.
grpc-go's documented client keepalive and its BDP PINGs are checked against
the default `ServerKeepalive`, along with a raw PING flood and an age drain
with a bidi stream in flight. It belongs to `async:verify`. grpc-go clients
PING at most every 10 seconds, so the run takes about 45 seconds.

## Transport

`Transport.new(origin:, ca_file: nil, certificate_chain: nil, private_key: nil,
connection_limit: 1, max_requests: 128, connect_timeout: 10,
connection_policy: ConnectionPolicy.new, connection_observer: nil,
stream_receive_window: 4_194_304, connection_receive_window: 16_777_216)`
implements core's transport seam for HTTP/2. Since 0.3.0, the receive windows
size response downloads; see [Flow-control windows](#flow-control-windows).

- Origins contain only scheme/host/port; a bare trailing `/` is accepted.
  Request paths, including core's base prefix, remain intact.
- One connection multiplexes requests. Admission fails with unavailable:
  `HTTP/2 transport admission is full` at `max_requests` or
  `max_pending_bytes`, and `HTTP/2 transport admission is closed` once closing.
  Acquisition waits honor finite RPC deadlines.
- Nil RPC deadlines remain unbounded, while each connection attempt has its own
  `connect_timeout`.
- An enclosing Async `with_timeout` around a call or stream operation ends it
  with that same `Async::TimeoutError`, not a ConnectRPC error. The stream is
  reset and its admission released as for any interrupted call. An already
  expired RPC deadline or cancellation still wins.
- Connection and transport failures raise unavailable
  `HTTP/2 transport failed: <reason>`, keeping the original exception as
  `cause`. The reason is one of `connection refused`, `name resolution
  failed`, `host unreachable`, `timed out`, `TLS verification failed`,
  `ALPN negotiation failed`, `TLS error`, `connection closed` or `HTTP/2
  protocol error`; an unclassified failure has no reason. Messages never
  include the origin, addresses or certificate details. `HTTP1Transport` uses
  the same reasons with `HTTP/1`, plus `malformed response`.
- No redirects, proxy environment routing or HTTP/1 fallback.
- The transport carries opaque encoded bytes; core owns compression and
  negotiation.

[Client connection management](#client-connection-management) describes
reconnection, backoff, GOAWAY replacement, admission, rerouting of unsent calls
and the single resend of a refused Connect unary call. For TLS options, see
[TLS and client authentication](#tls-and-client-authentication).

### Safe unary GET

Safe unary GET uses core `Client.new(use_get: true)` and declared
`:no_side_effects` methods. Native HTTP/2 requests carry HEADERS with END_STREAM
and a nil body; core owns query encoding, parsing and independent URL/payload
limits. The transport never falls back to POST; a refused GET is resent only
under the refused-stream rule in
[Client connection management](#client-connection-management).

## Client connection management

Each H2 `Transport` owns a private connection manager on its reactor. Its policy
is on by default; these are the defaults:

```ruby
policy = ConnectRPC::Async::ConnectionPolicy.new(
  backoff: ConnectRPC::Async::Backoff.new(base: 1.0, multiplier: 1.6, jitter: 0.2, max: 30.0),
  wait_for_ready: false,
  max_pending_bytes: 16 * 1024 * 1024,
  retry_refused: true,
  max_draining: 1,
  drain_timeout: nil,
  keepalive: nil
)
transport = ConnectRPC::Async::Transport.new(origin: origin, connection_policy: policy,
                                             connection_observer: ->(event) { record(event) })
transport.connection_state # => :idle, :connecting, :ready, :transient_failure, :closing or :closed
```

Invalid values raise `ArgumentError` at construction. `wait_for_ready: true`
requires a backoff, `max_draining` is a nonnegative integer, `drain_timeout` is
nil or finite positive seconds, `keepalive` is nil or a `Keepalive`, and
`connection_policy: nil` is rejected. Each feature can be turned off:
`backoff: nil`, `max_pending_bytes: nil`, `retry_refused: false`,
`max_draining: 0`, `drain_timeout: nil` and `keepalive: nil` (the default).

### Connection attempts

- No socket opens before the first call. A connection is ready after TCP, TLS
  with hostname and ALPN `h2` verification, the client preface and the server's
  first SETTINGS frame. TCP connect alone is not readiness.
- Attempts are shared, one at a time, and run under the transport rather than
  under any RPC task or lifecycle. A call whose deadline or cancellation fires
  stops waiting without cancelling the attempt; the attempt continues until
  `connect_timeout` or `close`, and a connection that becomes ready stays for
  the next call. No attempt starts without a waiting call.

### Backoff and waiting calls

- After a failed attempt the manager waits
  `min(base * multiplier ** (failures - 1), max) * (1 - rand * jitter)` seconds
  before the next one. Jitter only shortens a delay, so `max` is a hard cap.
  Readiness resets `failures`; bursts of calls and idle periods do not. A timer
  that expires with no waiting call starts nothing and the state returns to
  `idle`. GOAWAY is not a failure.
- Calls are fail-fast by default. A call fails with `unavailable` when an
  attempt it joined fails and no usable connection exists; otherwise it keeps
  waiting for capacity. A call whose attempt succeeded can still join a later
  one, for example after another call took the new capacity. Calls arriving
  while the timer runs fail at once unless a usable connection exists. With
  `wait_for_ready: true`, calls keep waiting through backoff and later attempts
  until their own deadline, cancellation or `close`.
- `backoff: nil` removes the timer and the `transient_failure` state. A failed
  attempt fails every call that joined it, and the next call dials at once.

### Connections and admission

- `connection_limit` counts open connections outside the draining set, reusable
  or not, plus the attempt in progress and any connection removed from
  accounting whose socket is not yet closed. A call reuses the first connection
  with spare stream capacity, which is the peer's
  `SETTINGS_MAX_CONCURRENT_STREAMS`, and opens another only when none has
  capacity and a slot is free. A peer raising that limit wakes waiting calls;
  lowering it never revokes a stream already acquired. While any connection is
  usable the state stays `ready`, even if an extra attempt failed and is backing
  off. Extra connections add capacity, not load spreading.
- Every call and stream takes one of `max_requests`. `Transport#call` (Connect
  unary) also reserves its encoded request bytes from `max_pending_bytes`: the
  POST body, or the request target for GET. Native gRPC unary and all streams
  reserve no bytes. A full count or byte budget, or a single call larger than
  the budget, fails immediately with `unavailable`. `max_pending_bytes: nil`
  admits by count only. Reservations last until the call's terminal outcome.

### GOAWAY and draining connections

- A connection that receives GOAWAY or dies takes no new streams and counts as
  `draining` in observer events. After a graceful GOAWAY (`NO_ERROR`) with
  streams still open, the connection joins the draining set while it has fewer
  than `max_draining` members. It then leaves the `connection_limit` cap, so a
  waiting call can open a replacement while it drains; GOAWAY alone never
  dials, and replacement is not a failure for backoff. Otherwise it keeps its
  slot until its streams finish, and joins the set when a member retires. The
  client never has more than `connection_limit + max_draining` sockets open.
- `drain_timeout` closes any graceful GOAWAY connection, in the set or not,
  that still has streams after that many seconds; those streams fail with
  `unavailable` and are not resent. `close` cancels drain timers, and its own
  grace bounds the drain.
- An error GOAWAY or a dead connection fails its remaining streams at once. A
  connection that dies or receives GOAWAY while idle closes and republishes the
  state immediately.

### Reroutes and the refused-stream retry

- A call whose connection became unusable after acquisition but before any
  stream ID was allocated or request byte written returns to acquisition with
  its existing admission. This reroute is not a replay.
- There are no other retries, with one exception: with `retry_refused: true`, a
  Connect unary call through `Transport#call` (POST or GET) is sent once more
  when waiting for its response headers raises
  `Protocol::HTTP::RefusedError` and no response HEADERS, not even 1xx, arrived.
  The pinned protocol-http2 raises it only for streams the server provably never
  processed: `RST_STREAM(REFUSED_STREAM)` and streams above a GOAWAY's last
  stream id (RFC 9113 sections 6.8 and 8.7). The resend first retires the
  refused stream's output within the 100ms cleanup allowance, then goes back
  through ordinary acquisition with the same admission and deadline, so it may
  reuse a healthy connection or wait for capacity. A second refusal, or a
  refused output frame that stays stuck, fails the call with `unavailable`.
- Native gRPC unary calls and all streams are never resent. Otherwise, once
  request HEADERS start, a failed call is never resent; only a separate later
  call reconnects.

### Keepalive

Keepalive is off by default. Turn it on to detect half-open connections, for
example behind a NAT or L4 load balancer that silently drops idle flows:

```ruby
keepalive = ConnectRPC::Async::Keepalive.new(time: 300, timeout: 20, permit_without_calls: false)
policy = ConnectRPC::Async::ConnectionPolicy.new(keepalive: keepalive)
```

`time` and `timeout` are finite positive seconds; `timeout` defaults to 20 and
`permit_without_calls` to false. `time: 300` respects grpc-go's default server
enforcement (PINGs at least 5 minutes apart, none without calls), so grpc-go
servers do not answer it with `too_many_pings`.

- After `time` seconds without reading any frame, the connection sends a PING.
  Only the ACK carrying that PING's payload proves the peer alive; other frames
  only postpone the next PING, so a peer that writes but never reads cannot
  pass. Without the matching ACK within `timeout`, counted from before the PING
  competes for the connection's write lock, the connection retires raw socket
  first, exactly like a dead connection. Its calls and streams fail with
  `unavailable` and are never resent; the next call reconnects.
- A connection is monitored while it has open streams, frames being written or
  waiting to be written, or stream output still finishing a frame. Otherwise it
  stays quiet until a stream opens, unless `permit_without_calls: true`, which
  also pings idle connections. A connection draining after GOAWAY keeps being
  monitored while work remains.
- A peer GOAWAY with `ENHANCE_YOUR_CALM` and debug data `too_many_pings` sets
  the interval for later connections to at least twice that connection's
  `time`, as grpc-go does. The existing connection is not changed.
- Keepalive runs on the owner reactor, also for `ThreadedTransport`, so it keeps
  running after the calling thread's call returns. `close` stops it before
  waiting for calls; it never delays a graceful drain.

### Observing connections

- `connection_observer` receives frozen `ConnectionEvent` values with `type`
  (`:state_changed`, `:attempt_finished`, `:admission_rejected`,
  `:connection_closed`, or `:counts_changed` since 0.3.0), `state`,
  `previous_state`, `outcome`, `duration` in monotonic seconds, and the counts
  `active`, `draining`, `pending_calls` and `pending_bytes`. An
  `:attempt_finished` outcome is `:ready`, `:refused`, `:timeout`, `:tls`,
  `:protocol`, `:closed` or `:other`. Observers that switch on `type` should
  ignore types they do not know.
- Every event carries the counts at the time it was published. `active` counts
  accounted connections that accept new calls. `draining` counts accounted
  connections that no longer accept new calls (after a GOAWAY, or failed or
  aborted while their calls unwind) until their last call ends. Neither counts
  a removed socket that is still closing. `pending_calls` counts calls waiting
  to acquire a connection, and `pending_bytes` the request bytes reserved by
  admitted calls.
- Since 0.3.0, `:counts_changed` is emitted when `active`, `draining` or
  `pending_calls` differs from the last published event's counts without another
  event reporting it: a call starts or stops waiting, or a connection drains or
  is removed. A call that finds spare capacity emits nothing. `pending_bytes`
  changes with every call and never triggers it. After transport close and its
  cleanup, the last event carries zero `active`, `draining` and `pending_calls`;
  a connection whose call is still unwinding at close publishes its removal when
  the call ends. A `ThreadedTransport` owner publishes from its reactor, so a
  stalled owner publishes nothing until it runs again; a replacement owner
  starts from zero counts.
- `:connection_closed` is emitted exactly once when a ready connection leaves
  the manager, which for a connection with calls in progress is when its last
  call ends. Its `outcome` is the first recorded reason: `:keepalive_timeout`,
  `:drain_timeout`, `:goaway` (the peer's GOAWAY ended it, gracefully or with an
  error) or `:closed` (EOF, a protocol or I/O error, or a local abort). Its
  `duration` is nil. Transport `close` emits none for the connections it
  closes. Metrics observers read only its counts. Events carry
  no addresses, certificates, payloads, URLs or exception text. The observer
  runs synchronously on the owner reactor after the manager's bookkeeping and
  must return quickly. Its `StandardError`s are ignored. Calling `call`,
  `open_stream`, `open_grpc_stream` or `close` from the observer raises;
  `connection_state` and `inspect` remain available.
- `connection_state` is a published observation, not a promise that the next
  call succeeds. `close` additionally stops admission, fails waiting calls with
  `unavailable`, and cancels the backoff timer and any attempt, raw socket
  first.

## Ordinary threads

`ThreadedTransport` lets threaded programs such as Puma, Sidekiq or plain
threads share one client without an application reactor. It supports Connect
unary (POST and safe GET) and server streaming, and native gRPC unary and server
streaming. `streaming_capabilities` is `[:server_stream]` and
`grpc_capabilities` is `[:unary, :server_stream]`, so core rejects client and
bidi streaming before any I/O. Client and bidi streaming from ordinary threads
are tracked in [issue #29](https://github.com/jimeh/connectrpc-ruby/issues/29).

```ruby
require "connectrpc/async"

# Boot, for example in an initializer. No thread or socket starts here.
RPC = ConnectRPC::Async::ThreadedTransport.new(origin: "https://rpc.internal:8443")
client = ConnectRPC::Client.new(base_url: "https://rpc.internal:8443", binding: binding, transport: RPC)

client.call(method, message, timeout: 2)                             # Any thread.
client.server_stream(watch, message) { |reader| reader.each { ... } } # The block runs on this thread.
RPC.close(grace: 5)                                                  # Shutdown, for example in on_worker_shutdown.
```

It takes the same options as `Transport`, and they behave the same, including
the [connection policy](#client-connection-management). The constructor
validates them and compiles the TLS snapshot on the calling thread, so
configuration errors raise at boot. `call`, `open_stream`, `open_grpc_stream`,
`close(grace: 5)`, `connection_state` and `inspect` work from any thread.

### The owner thread

- The first call in a process starts one owner thread, named
  `connectrpc-async HOST:PORT`, that runs an Async reactor and an H2
  `Transport`. Calls from every thread multiplex over its connections. Core
  encodes and decodes on the caller thread; only frozen bytes and headers cross
  to the owner. The owner thread does not keep the process alive and there is no
  `at_exit` hook: process exit aborts its sockets raw-first, so call `close` from
  your server's shutdown hook for a graceful drain.
- After fork, the child's first call builds its own owner thread and
  connection; it never writes to or closes the parent's. The compiled TLS
  snapshot is reused. A transport closed before fork stays closed; `close` in a
  child closes only the child's owner.
- If the owner thread dies, pending callers get `unavailable`, the observer sees
  the inner transport close, and the next call starts a new owner.
- `call`, the stream methods, exchange operations and `close` from the owner
  thread, such as from a `connection_observer`, raise `LifecycleError`.

### Admission, deadlines and interrupts

- `max_requests` and `max_pending_bytes` count a call until both the owner has
  its outcome and the caller has taken it or left. Interrupted callers whose
  streams are still cleaning up, and completed replies not yet taken, stay
  within the bound. Full admission fails with `unavailable` and emits
  `:admission_rejected` on the owner thread.
- A finite deadline also bounds the caller's wait: if the owner is stalled, for
  example by native code, the caller fails with `deadline_exceeded` after the
  deadline plus the 100 ms cleanup allowance and the call is cancelled once the
  owner resumes. A nil deadline waits for the reply, its cancellation token or
  an interruption. `Timeout` and `Thread#raise` cancel the call's stream (the
  server sees `RST_STREAM(CANCEL)`) and their exception reaches the caller
  unchanged. `Thread#kill` cancels the stream the same way and raises nothing.
  Either way the call's admission is released once the owner finishes the
  stream, and siblings and the connection survive. Core may still report an
  already accepted cancellation or deadline instead, as with every transport.
- The waits in `call` and `close` accept `Timeout`, `Thread#raise` and
  `Thread#kill` even inside an enclosing
  `Thread.handle_interrupt(... => :never)` region, because Ruby cannot read the
  caller's mask. Core NetHTTP defers those interrupts inside such a region.
  The reactor-owned `Transport` does not reliably defer them either, because
  masks are per fiber and the reactor delivers them to other fibers.

### Streams from ordinary threads

A stream's block runs on the caller's thread, so core's stream session keeps
its thread affinity and no application code moves to the owner thread. The
owner runs the real H2 exchange. The block receives a private exchange proxy;
each `response_head`, `read`, `write`, `close_send`, `finish` and, for gRPC,
`response_trailers` posts one command to the owner and waits for its result.

- `open_stream` and `open_grpc_stream` wait until the owner has dispatched the
  stream, so connection and dispatch failures raise before the block, as with
  `Transport`. Each stream holds one admission slot and no request bytes until
  both the owner has retired it and the caller has left.
- One operation runs at a time per stream; a concurrent one raises
  `StreamStateError`. `read(n)` returns at most `n` bytes and trailers stay
  behind response EOF. Header maps reach the caller as their own frozen copies,
  and each chunk is a new string the caller owns.
- An operation's wait is bounded by the deadline and by its own timeout, each
  plus the 100 ms allowance. If the owner is stalled past that, the operation
  fails with `deadline_exceeded`, or with `unavailable` for its own timeout, and
  the stream is reset when the owner resumes.
- A normal block exit waits for the owner's actual outcome, bounded only by the
  deadline as unary calls are. A gRPC cleanup failure after the block returns
  reaches the caller as `unavailable`; the caller never reports success
  without the owner's outcome.
- An exception, `Timeout`, `Thread#raise`, `Thread#kill`, `break` or `throw`
  out of the block, or interrupting an operation, fires the stream's
  cancellation before the owner is told to stop. A read or write blocked on the
  wire therefore wakes and resets only that stream (`RST_STREAM(CANCEL)`);
  siblings and the connection keep working. The original exception reaches the
  caller without waiting for cleanup, and admission is released once the owner
  retires the stream. If application code rescues such an interrupt and the
  block then returns, the stream fails with `canceled`.
- Core raises a remote error, such as `not_found`, from inside the block after
  the exchange's `finish` returned. So when a `ConnectRPC::Error` leaves the
  block after a successful `finish`, the owner finishes the stream normally and the
  caller waits for its outcome, for at most the 100 ms allowance and never past
  the deadline or an accepted cancellation plus the allowance. Admission is
  released before the error returns, and a late gRPC cleanup failure is
  reported to instrumentation as an aborted delivery, as with `Transport`. The
  original exception reaches the caller unchanged, even when cleanup failed.
  If the owner is stalled past the allowance, the caller fires the stream's
  cancellation and returns the error without the owner's outcome; admission is
  then released once the owner retires the stream, and its late delivery facts
  are not reported. An interrupt during the wait propagates as above. Other
  exceptions, including `Timeout::Error`, `throw`, `break` and errors before
  `finish` never wait. Core raises some errors before `finish`, such as an HTTP
  status error from a proxy or a rejected gRPC content type; their admission is
  released once the owner retires the stream, so back-to-back failures near
  `max_requests` can briefly see `unavailable`.
- `abort`, which core calls when a reader abandons the stream, only queues the
  request and never waits, so unwinding cannot block on a stalled owner. An
  explicit abandon followed by a normal return keeps its normal outcome.
- A retained exchange used after its block finished raises
  `unavailable` (`HTTP/2 exchange is retired`), including a cached
  `response_head`. An operation still waiting on another thread when the block
  finishes wakes with the stream's outcome or the same error.
- A forked child's inherited exchange raises the same error. A child that
  leaves an inherited block gets that error for a normal return, or its own
  exception, at once; it never waits on the parent's owner or touches the
  parent's stream or admission.
- The block runs with interrupts enabled even inside an enclosing
  `Thread.handle_interrupt(... => :never)` region, extending the same rule for
  waits above to application code in the block. This is deliberate: the
  transport's own bookkeeping stays masked, so an interrupt can never skip the
  stream's cancellation or leak its admission.

### Closing

`close` stops admission at once and drains with the grace counted from the
call. Concurrent callers share the outcome; a transport that never started
returns true. If the owner is stalled past the grace plus the allowance,
`close` returns false, wakes pending callers with `unavailable` and does not
join the thread; the owner aborts when it resumes. An interrupted closer
re-raises its own exception while the others still get the outcome.

### Latency under CPU load

MRI hands the GVL to a thread waking from I/O only after the running thread's
time slice, 100 ms by default, so CPU-bound threads delay the owner thread.
Lowering `RUBY_THREAD_TIMESLICE`, for example to 10, reduces that latency.

## Explicit HTTP/1.1 client

```ruby
Async do
  transport = ConnectRPC::Async::HTTP1Transport.new(origin: "https://rpc.example.com")
  client = ConnectRPC::Client.new(base_url: "https://rpc.example.com", binding: binding,
                                  transport: transport, use_get: true)
  begin
    client.call(method, message)
    # Existing generated clients and core server_stream/client_stream use it too.
  ensure
    transport.close
  end
end.wait
```

`HTTP1Transport.new(origin:, ca_file: nil, certificate_chain: nil, private_key:
nil, connection_limit: 4, max_requests: 128, connect_timeout: 10)` is an explicit
Connect H1 client. Pass it through the existing core/generated client's
`transport:` option inside an application-owned Async reactor. It supports safe
unary GET, both codecs and identity/gzip through core, and SS/CS through the
existing streaming SPI. It has no gRPC or bidi capability. Standalone Async Host
remains H2-only; Falcon owns Ruby H1 hosting. For ordinary threads, see
[`ThreadedTransport`](#ordinary-threads).

### Leases and capacity

Each connection has one exclusive lease through setup, upload, full HTTP EOF
and cleanup. Pending dials and retiring connections count toward the hard cap;
lease waits count toward admission. Four long streams can occupy every default
slot. Configure capacity or give waiters cancellation/deadlines; fairness is not
promised. `close(grace: 5)` stops admission and drains admitted operations,
including waiters, returning false on forced teardown. Owner exit aborts owned
resources. Admission fails with `HTTP/1 transport admission is full` at
`max_requests` and `HTTP/1 transport admission is closed` once closing. There
is no hidden thread, pipelining, replay or fallback, and one resend: a server
may close an idle keep-alive connection while the next request crosses it, so
a unary GET whose reused connection closed before any response head is sent
once more on another connection, unless it was cancelled, its deadline expired
or the transport started closing. Call observers see no failure from the
attempt that was resent. POST and streams are never resent.

### Deadlines and reuse

Finite RPC deadlines cover every wait including cleanup. Nil RPC deadlines add
no watchdog; setup still has its own combined connect timeout. Foreign-thread
cancellation only signals the reactor. Full native EOF, a successful joined
writer and successful cleanup permit reuse. Any abandoned/failed exchange
retires its connection. Clean close-delimited unary EOF is supported, but
cannot detect an otherwise valid semantic truncation without Content-Length.
TLS unexpected EOF without close_notify remains a transport error.

### TLS

TLS offers only `http/1.1`, accepts absent or H1 ALPN, and rejects every other
negotiation before request bytes. Peer/hostname verification and mTLS remain
enabled. TCP rejects TLS options. Replace the transport to change TLS identity.

### Upload failures

Response reads are independent of a blocked upload. A complete unary non-200
response stops pending output destructively so core can validate its error;
HTTP 200 requires writer success. For streaming, `write` alone reports a writer
failure. Core may then recover a remote error through its bounded terminal-only
policy. An unreported upload-tail failure returns unavailable even if a remote
error terminal arrived, masking that error. `close_send` never raises writer
failure; `finish` requires full HTTP EOF and joined cleanup under the supplied
completion budget. A write-only caller without a deadline can still block when
a peer neither consumes nor closes. See
[streaming](../../docs/streaming.md#explicit-http11-client).

### HTTP/1.1 evidence

`async:conformance:http1` runs separate strict official 1.0.6 native H1 CLIENT
inventories: 386 identity and 352 gzip cases, including safe GET, both codecs,
TCP/TLS and client certificates. `async:interop:http1` runs independent pinned
connect-go H1-only TCP/TLS/mTLS listeners, with actual HTTP/1.1 and ALPN evidence.
Existing H2, NetHTTP and Falcon server inventories remain separate.

## Close and cleanup

Both `Host` and `Transport` provide `close(grace: 5)`. `Host` stops accepting
connections and drains each HTTP/2 connection with the two-phase GOAWAY in
[Server keepalive](#server-keepalive): streams opened before the cutoff are
still served, then handlers and wire streams complete, a final GOAWAY follows,
and the connection half-closes and awaits EOF. `Transport` stops admission,
awaits calls and complete wire streams, sends GOAWAY when empty, then
half-closes and awaits EOF. External callers receive true when drained, false
after forced teardown. Grace is finite and nonnegative; zero aborts.
Repeated/concurrent calls share the first outcome.

Server connections also end on their own: graceful idle and age drains,
unanswered server PINGs and `too_many_pings`, as described in
[Server keepalive](#server-keepalive). A session close during a drain keeps the
drain's cutoff.

Finite deadlines begin in core before encoding and cover acquisition, headers
and body. A shared connection attempt is bounded by `connect_timeout` instead.
Timers use at most 86400-second waits without shortening logical deadlines.

### Cleanup allowance

Client cleanup uses one total internal 100ms allowance. Ordinary cancellation
resets its stream. A frame that cannot finish within cleanup deliberately fails
the whole connection and siblings. One exception: when the peer has already
reset a Connect or gRPC stream (anything except Connect unary `call`), cleanup
returns the lease without waiting for a request frame that is still being
written. That frame always finishes before the connection writes anything
else, so later calls on the connection wait behind it until it finishes or
their own deadline or cancellation ends them. With `keepalive` set, a frame
that never finishes also blocks the keepalive PING, so the connection retires
within `time + timeout` and the waiting calls, which sent nothing, move to a
new connection. Forced teardown closes raw TCP
before cancelling and joining writers/readers, then performs buffered/TLS
cleanup.
These are cooperative bounds, not arbitrary native/application interruption
guarantees.

H2 connection release, acquisition or close can therefore wait beyond an RPC
deadline for actual retirement. The 100 ms fallback allowance triggers
destructive cleanup; it is not a hard deadline for that cleanup.

### Forced teardown and retirement

Forced teardown invalidates the connection immediately. External callers of
private connection abort and server session abort/close then wait for owned
tasks, their cleanup descendants and final buffered/TLS disposal. An owned task
initiating or reentering abort requests retirement without waiting on cleanup
that must join that same task. Each external abort/close joiner retains its own
first interruption until cleanup completes.

An external server close caller interrupted during a nonzero graceful drain
requests forced session retirement, joins actual cleanup, then raises its
identical first exception. Other callers retain their own result or exception,
and an already-decided close outcome remains unchanged.

### Close results

Server `Host#close` and `ServerSession#close` return `bool?`:

- A resource-owned handler initiating positive-grace close starts the ordinary
  asynchronous drain and returns nil while shutdown is pending. Admitted
  siblings retain their grace and can continue processing.
- Owned repeated requests and cleanup/close coordinator reentry also return nil
  while pending, without joining cleanup that must join them. Nil neither
  decides the outcome nor publishes completion.
- Repeated requests share the original closer and expiry, even with a different
  grace argument.
- External callers join actual cleanup and receive its final boolean. Once
  closed, every call returns that same boolean, including coordinator reentry.
- Zero grace still forces teardown; explicit abort and already-decided outcomes
  are preserved.

The client `Transport` and `HTTP1Transport` close return types remain `bool`.

### Task joins

Private task joins distinguish a resolved result from actual finalization. They
block on required children and task-local progress/finalization notifications,
retaining captured child identities through reparenting or transient changes.
A scheduling opportunity after raw close establishes no completion boundary;
all retained task joins still precede disposal and retirement publication.

These joins require cooperative tasks and a live application reactor. A
non-yielding handler or native operation can delay them indefinitely. Cleanup
survives ordinary owner cancellation while an external waiter or lifetime guard
keeps the task tree alive; reactor termination cannot guarantee completion.

## TLS and client authentication

Applications load their own files. Host TLS is a complete hash:

```ruby
tls = {
  certificate_chain: [File.read("leaf.pem"), File.read("intermediate.pem")],
  private_key: File.read("key.pem"),
  client_ca: nil # Or a nonempty array of explicit trust-anchor PEM certificates.
}
```

### Certificates and verification

Each certificate element contains exactly one PEM certificate, leaf first.
Keys are unencrypted PEM private keys. Inputs are copied and parsed without
password prompts; key possession, leaf validity and adjacent chain signatures
are checked. Snapshots retain private parsed objects, redact inspection/errors,
and compile fresh contexts before publication. Explicit intermediates, including
an empty array, prevent auto-chaining through the client-auth trust store.

Both roles require TLS >=1.2 and post-check ALPN `h2`. Clients always verify
peer/hostname, including localhost, using system roots plus optional `ca_file`.
Client chain/key must appear together; TLS options are invalid for h2c. IP
literal origins send no server name (RFC 6066 section 3), and the certificate
must still match the address.

Client TLS inputs differ in kind: `ca_file` is a filesystem path to a PEM CA
bundle, `certificate_chain` is an array of PEM certificate strings, leaf first,
and `private_key` is a PEM string. An invalid input raises `ArgumentError`
naming the input, such as `Invalid TLS client configuration: ca_file`, without
its value or a cause.

### Client authentication

A nonempty `client_ca` requires verified client certificates using only those
anchors, with partial-chain trust enabled deliberately. A scoped intermediate
can be the boundary; sibling issuers under its root are not trusted. Nil means
server-only TLS and no client authentication. There is no optional-client mode.

`host.peer_certificate` returns a detached authenticated client leaf only within
that host's exact handler task; outside/child/other-host/plain/server-only TLS
returns nil. It does not expose or claim a verified chain and does not change
core Context. Applications own authorization based on that identity.

### Reload and drain

`host.reload_tls(certificate_chain:, private_key:, client_ca:)` replaces the
complete snapshot for newly accepted sockets on the same listener. Generation
starts at 1 and increments only after successful validation; invalid candidates
leave the last good snapshot intact. Existing connections retain their prior
identity and authentication. Every generation has a fresh context/session ID,
OP_NO_TICKET and SESSION_CACHE_OFF. Reuse is disabled; this does not claim that
OpenSSL emits zero TLS 1.3 tickets. Replace a transport to change client identity.

`host.drain_tls(grace: 5)` captures the current generation cutoff and retires
only older accepted connections, including pending handshakes. Old connections
get the two-phase GOAWAY drain with debug data `tls_rotation`: streams opened
before its cutoff are served, later ones are refused with `REFUSED_STREAM`, and
admitted streams finish. Pending handshakes abort immediately.
A persistent retirement floor prevents old pending sockets from becoming live.
The listener and current/newer generations survive concurrent reloads/drains.
Close supersedes drains. Renewal alone does not instantly revoke existing peers.
There are no file watchers, ACME, secret services or authentication policy DSLs.

## Native application and managed TLS

### Application

`Application.new(server:, fallback: nil)` is a reusable Protocol::HTTP callable.
It routes whole registered service prefixes to core, splits the request target
once, forwards the raw query to core, and returns bare 404 or calls the native
fallback for unrelated paths. HTTP/1 requests do not need a stream object. Keep
generic compression/cache middleware on fallback so core owns RPC encoded bytes
and limits.

### Managed TLS providers

Host accepts `tls_provider:`, `refresh_interval: 60`, `refresh_jitter: 0.1`,
`refresh_timeout: 10` and `on_tls_error:`. `refresh_tls` coalesces immediate
source checks; `tls_status` exposes safe state/generation, the last event
`category`, separate monotonic source-check/publication ages and, since 0.3.0,
`identity_expires_at`. Managed `reload_tls` conflicts with provider
ownership. `TLSConfiguration` also exposes `start`, `refresh`, complete static
`reload`, `close`, `generation` and `status`. Construction is reactor/thread
bound; initial static/provider compilation happens on `start`, before accepts.

Providers return exactly `version`, `certificate_chain`, `private_key` and
`client_ca` symbol keys. A nonempty opaque String version identifies validated
canonical content. Same version/content preserves generation; changed content
under that version fails. A different previous version can deliberately roll
back through a fresh generation. Missing/nil data never disables TLS. Background
errors retain the last good snapshot and emit only safe immutable diagnostics.
Concurrent callers join one owned attempt, and waiter cancellation does not
cancel it. Timer intervals start after each completed attempt. Timeout and
shutdown remain cooperative for arbitrary native/CPU application code.

Candidates are validated when they are published, so a chain that has already
expired fails `start`, `reload_tls` or a provider refresh as `:invalid`. Since
0.3.0, an identity that expires while it is published keeps being served, as Go's
`crypto/tls`, nginx and Envoy serve whatever certificate is configured: a client
that verifies certificates then fails its handshake with a certificate-expired
error instead of an unexplained EOF. Before 0.3.0 the Host closed new peers
instead. Since 0.3.0, `tls_status[:identity_expires_at]` is the earliest
`not_after` in the served identity chain, as a frozen `Time` (nil before the
first publication or without TLS), and the session reports category `:expired`
once per generation when the wall clock reaches it, through `on_tls_error` or a
warning. The check re-reads the wall clock at least every 60 seconds, so a clock
adjustment delays the event by at most that. `category` holds the last reported
event, so a later refresh failure replaces `:expired`; compare
`identity_expires_at` with the current time to test expiry. A refresh that
publishes a new identity clears it. As during a provider refresh report, calling
`close` or `refresh_tls` from the reporter raises `LifecycleError`;
`reload_tls` and `drain_tls` work there, so a static-TLS reporter can publish a
replacement identity. There
is no maximum staleness setting; established connections retain negotiated
state until drain/close.
The [Falcon guide](../connectrpc-falcon/README.md#tls-lifecycle) explains complete
provider semantics and safe diagnostics shared by both hosts.

### ServerSession hosting SPI

`ServerSession` is the small hosting SPI: start the worker session, synchronously
`accept(raw)` before scheduling, then call the opaque accepted handle's `serve`.
`schedule` attaches accepted work to the independent session lifetime. The
handle also supports `close`, `wait_closed`, `timeout=` and `remote_address`,
and exposes no SSLContext.

`wait_closed` waits for actual connection retirement, including owned task
unwind and final disposal, rather than merely observing invalidation. Retiring
server connections retain their admission slot until this boundary. The H2
client's private connection manager removes a retiring connection's slot and
wakes waiting calls before joining its connection close.

The session forwards TLS lifecycle/identity operations, bounded `close` and
raw-first `abort` for listener/setup failure. Its `keepalive:` (default
`ServerKeepalive.new`), graceful `idle_timeout`, `max_concurrent_streams:`,
`request_body_timeout:`, `response_body_timeout:` and `frame_write_timeout:`
(all since 0.3.0) and the [receive windows](#flow-control-windows) apply to
HTTP/2 resources only; see [Stream admission](#stream-admission). An application other
than `Application` gets the request and response body bounds on every HTTP/2
request. Falcon's HTTP/1 resource keeps
closing idle connections, has no keepalive, and answers a request with a bare
503 when admission is full or the session is closing. The optional
`stop_accepting:` callback runs after affinity/grace validation, before refresh
stops and connection drain begins. Its one HTTP/1 construction hook is for the
optional Falcon-local resource; HTTP/2 resources do not pretend to implement
HTTP/1. The standalone Host keeps its strict h2 listener and composes the shared
session.

Since 0.3.0 the Host and Falcon take each peer through the session's private
accept step, which accepts only while the session is under `max_connections`.
A hosting caller that calls `accept(raw)` above the cap still has that peer
closed.

The shared session's private resource completion and lifetime-anchor operations
are source-coupled to Falcon's local HTTP/1 resource. Use matched Async/Falcon
package versions; these internals are not an additional public API or SPI.

## Backend boundaries and verification

### Compression and limits

Enable gzip with `encoding: :gzip` on the core client and server. Client
`accept_encoding:` controls response negotiation independently. Core's
`request_limit:` and `response_limit:` bound serialized/decompressed messages;
the separate wire limits default to those same caps. The transport's
`TransportRequest.response_limit` remains the encoded wire read cap. Defaults
remain identity, and neither host nor transport performs transparent inflation.

### Header and length handling

Core owns codecs, errors, metadata and serialized byte limits. The backend
reads bounded chunks, reconstructs declared length from backend body.length,
preserves raw repeated leading fields and leading `trailer-*`, and ignores real
HTTP/2 trailing HEADERS for core metadata. The stack normalizes lengths using
Ruby Integer: `+10`, hexadecimal and underscores can normalize before core;
malformed/duplicate lengths may reset/close connections, and singleton header
policies can produce bare errors before core. These outcomes are tested and
are not a full protocol conformance claim. Settings use 16KiB frames, the
configured stream window and bounded stream admission; they do not bound total
process memory or HPACK/header-continuation buffering.

### Flow-control windows

Since 0.3.0, `stream_receive_window:` and `connection_receive_window:` size the
HTTP/2 receive windows of `Transport`, `ThreadedTransport`, `Host`,
`ServerSession` and the Falcon server. They limit how much the peer may send
before this side returns credit: response bodies for a client, request bodies
for a server. Each is an integer from 65,535, the HTTP/2 initial window, to
2,147,483,647.

- `stream_receive_window` (default 4 MiB, 4,194,304, on `Transport` and
  `ThreadedTransport`; 65,536 on `Host`, `ServerSession` and the Falcon server)
  is advertised as `SETTINGS_INITIAL_WINDOW_SIZE` and is also each stream's
  [ingress bound](#ingress-bounds): stream credit returns only as the
  application consumes or discards bytes, so a stream retains at most this many
  unread payload bytes. A stream receives at most one window per round trip, so
  its download or upload rate is at most about the window divided by the
  round-trip time: 64 KiB at 50 ms RTT is under 1.3 MiB/s, 4 MiB is about
  80 MiB/s.
- `connection_receive_window` (default 16 MiB) caps bytes in flight across all
  streams of a connection. Each side grows its connection window from 65,535
  with one `WINDOW_UPDATE` on stream 0 right after its SETTINGS, as Go's
  net/http2 does. Connection credit returns as DATA arrives rather than when it
  is read, so unread bytes never exceed the stream windows. The connection
  window instead limits how fast those windows can fill: before 0.3.0, one
  round trip delivered at most 64 KiB per connection. Keep it at least the
  stream window times the streams expected to transfer at once. The client
  default lets four streams each fill a 4 MiB window in one round trip.

Memory grows with the stream window. Each open stream retains at most one
stream window of unread payload, so a server connection retains at most
`stream_receive_window` times its stream limit (the smaller of
`max_concurrent_streams` and `max_requests`), as before 0.3.0: 6.25 MiB with
the defaults, 100 MiB with 1 MiB windows. Admitted requests hold at most
`stream_receive_window` times `max_requests` (default 128) across the host:
8 MiB with the defaults. A stream that arrives while host admission is full is
refused before the server reads its DATA, so it retains nothing.

A client call retains at most one stream window of unread response payload,
4 MiB by default, for example while a streaming call's application stops
reading. A client transport therefore retains at most `stream_receive_window`
times its own `max_requests`: 512 MiB with the defaults, against 8 MiB before
0.3.0. A Connect unary call reads its response as it arrives into one buffer
bounded by core's `response_wire_limit` (default `response_limit`, 4 MiB), so it
holds at most that buffer plus one window of unread bytes, and an oversized
response still fails with `resource_exhausted`. These add to core's per-message
`request_limit` and `response_limit` buffering and the other allocations listed
under [Ingress bounds](#ingress-bounds). Pass `stream_receive_window: 65_536` to
restore the 64 KiB client bound, for example on a transport that holds many
concurrent streams whose readers fall behind.

Since 0.3.0, the client default follows Go's net/http2 client; before, client
streams used 64 KiB. A client receives only the responses it requested, so a
larger window costs memory only for calls the application started, and a Connect
unary response within the default 4 MiB `response_wire_limit` fits in one
window, so it never waits for stream credit. The server default keeps the 64 KiB
per-stream bound, because a server receives whatever its peers send. Raise the
host's `stream_receive_window` for faster uploads when the round-trip time
matters. `mise run async:bench:windows` measured these with 3 MiB unary bodies
through a local proxy that adds 50 ms of round-trip time; `--stream` or
`--connection` in bytes sets a window on both the client and the host:

| Windows (client stream / host stream / connection) | Download | 4 concurrent downloads | Upload |
| --- | --- | --- | --- |
| 64 KiB / 64 KiB / 64 KiB (the windows before 0.3.0) | 7.0 s | 17.7 s | 5.1 s |
| 64 KiB / 64 KiB / 16 MiB (`--stream 65536`) | 5.0 s | 5.0 s | 5.0 s |
| 4 MiB / 64 KiB / 16 MiB (default) | 0.13 s | 0.18 s | 5.0 s |
| 1 MiB / 1 MiB / 16 MiB (`--stream 1048576`) | 0.35 s | 0.43 s | 0.31 s |
| 4 MiB / 4 MiB / 16 MiB (`--stream 4194304`) | 0.12 s | 0.17 s | 0.12 s |

grpc-go starts both windows at 64 KiB and grows them up to 16 MiB by estimating
the bandwidth-delay product, unless the application sets them. Go's net/http2
uses 4 MiB stream and 1 GiB connection windows on clients and 1 MiB for both on
servers. This package does not size windows dynamically. The client connection
window stays at 16 MiB, grpc-go's ceiling, instead of net/http2's 1 GiB, because
the two windows mean different things. net/http2 returns connection credit as
the application reads, so its 1 GiB also caps unread bytes. This package returns
it as DATA arrives, so the connection window bounds only bytes in flight, and
the stream windows bound memory.

### Ingress flow control

Native HTTP/2 request and response inputs use a private coalescing queue. Stream
WINDOW_UPDATE replenishes only application-consumed or deliberately discarded
DATA bytes. Padding, including its length octet, uses wire credit and is
discarded without entering body bytes or Content-Length. Connection credit is
independent, so unread bodies do not prevent healthy streams or control frames
from advancing. The shared reader never waits for application consumption;
serialized socket writes can still block. Stream over-credit resets that stream
with FLOW_CONTROL_ERROR; connection over-credit fails the connection and its
siblings. Upstream parsing and stream-state errors remain authoritative. This
follows [HTTP/2 flow control](https://www.rfc-editor.org/rfc/rfc9113.html#section-5.2).

### Ingress bounds

With a 65,536-byte stream window, the server default, each queue retains at
most 65,536 payload bytes in at most five data entries, independent of frame
count. A larger `stream_receive_window`, such as the 4 MiB client default,
raises the byte bound to that window, in 16 KiB entries. Each returned native chunk is
at most 16,384 bytes. The core input adapter can retain one such chunk's remainder;
the shared reader can hold one decoded DATA frame of at most 16,384 bytes.
Coalescing/copying can temporarily retain old and new bounded chunks. String byte
length does not bound Ruby backing allocations or RSS. Caller-retained strings,
buffered unary bodies, codecs/compression, socket/TLS buffers and HPACK/headers
are separate allocations. These are ingress payload bounds, not a total memory
or general denial-of-service guarantee. Streaming adds bounded output ownership
without changing these ingress limits.

Normal producer EOF drains preceding payload. Closing the consumer releases its
queue once; later DATA is discarded while local output remains live, following
the pinned backend's finish-input and NO_ERROR policy. Reset/error/owned teardown
wakes readers and retires input references. Only peer CANCEL signals explicit
RPC cancellation; credit accounting adds no lifecycle instrumentation.

### Upstream contract guard

This package replaces or calls a few async-http and protocol-http2 hooks, and
its retirement joins rely on how Async finalizes tasks. `require
"connectrpc/async"` checks both once per process, in about a millisecond:

- Each hook's owner, visibility and parameter shape.
- In a separate thread with its own reactor: Async must call a per-instance
  `#consume` after it clears a finished task's fiber (on normal return, failure
  and cancellation). It must also call the parent's `#consume` when the last
  child leaves, and `Fiber.scheduler.push` must run a pushed object on a later
  iteration.

If any check fails, require raises one error that names each broken contract,
the installed gem version and the versions the package was verified with.
That replaces a hang or misbehavior at runtime. There is no switch to skip the
check. The finalization probe has a 5 s budget, including the time to stop it.
If it runs out, the error lists the failures already found and the step that
stalled. When the broken contract also stops Async from tearing down the
probe's reactor, the probe thread stays asleep for the rest of the process.
`connectrpc/falcon` checks its async-http `Endpoint` hooks the same way. The
guards cover the patch-pinned async-http and protocol-http2 hooks and Async's
finalization contract. Async itself is no longer patch-pinned: the finalization
probe is what lets the gemspec allow `async >= 2.46.0, < 3` (the guarded-coupling
tier in the [dependency policy](../../docs/development.md#gem-dependency-constraints)).
async-http and protocol-http2 stay patch-pinned because the semantics their
replaced hooks depend on cannot be proven at boot; the daily
[pins-lifted canary](../../docs/development.md#pins-lifted-canary) runs these
contracts against their newest releases. Falcon's overrides of the `falcon` gem
are not checked, because its `>= 0.57.0, < 1` range allows harmless changes a
shape check would reject.
Each gem's `spec/upstream_contract_spec.rb` pins the behaviors these checks
depend on. Its "worked around" examples fail when upstream fixes a behavior
this package works around, and each one names the workaround to delete. See
[issue #68](https://github.com/jimeh/connectrpc-ruby/issues/68).

### Verification evidence

Independent connect-go tests exercise both directions/codecs over h2c/TLS/mTLS,
with observed HTTP/2/ALPN and large typed wire-tail drains. Original core fixture
and official conformance harnesses remain separate. Installed package checks use
normal offline dependencies and a fresh GEM_HOME outside the checkout. The check
asserts the graph activates the OpenSSL and JSON releases the development bundle
resolved; installed core NetHTTP TLS/JSON with identity and gzip is also
exercised under that activation. Focused combined
specs cover actual gzip bytes over h2c/TLS in both codecs and independent host
wire/inflated and transport wire/client inflated caps. Nothing is published.

## Explicit unary cancellation

Pass `cancellation: source.token` to core or generated client calls, where
`source = ConnectRPC::CancellationSource.new`. The source is permanent and
shareable; only `source.cancel` initiates cancellation. `timeout: nil` keeps no
RPC deadline or wire timeout. Context offers cooperative `cancelled?`,
`cancellation_reason` and `check!`. See the
[unary guide](../../docs/unary.md#cancellation) for listener, arbitration and
registration lifetime rules.

The Async transport accepts foreign-thread cancellation through a Promise;
only the owner reactor stops private SDK tasks and performs stream cleanup.
Cancellation, an expired deadline or a stopped caller task resets the call's
stream with `RST_STREAM(CANCEL)`, including while its upload waits for
flow-control credit. Healthy multiplexed siblings survive. A stuck frame retains the one 100 ms
raw-first whole-connection cleanup fallback. Native HTTP/2 Context observes
only an actual peer RST_STREAM(CANCEL), before input teardown. Other resets,
normal half-close, connection loss and host shutdown do not fabricate explicit
cancellation. Processing completion detaches the source independently of delivery.

## Native streaming

The core [streaming guide](../../docs/streaming.md) documents all three scoped
Connect streaming shapes, typed generated clients/handlers, early headers,
per-message limits and bounded ownership. Native Async/Falcon HTTP/2 supports
both codecs and identity/gzip over h2c/TLS/mTLS. Falcon additionally serves
Connect server and client streaming over HTTP/1.1, with protobuf/ProtoJSON and
identity/gzip over TCP/TLS/mTLS. The standalone Async Host and original Async
Transport remain H2-only. Use the independent
[`HTTP1Transport`](#explicit-http11-client) for Connect unary, server streaming
and client streaming. See the streaming guide for Falcon's cooperative 500 ms
total final-close budget and unchanged 100 ms / 256 KiB discard limits. The
client's provisional one-second absolute HTTP tail policy applies even with a
nil RPC deadline and can reject otherwise valid late EOF or slow intermediaries.
See the [support contract](../../docs/support.md#native-client-completion).

Use `async:conformance` and `async:interop:streaming` for the exact official
native client/server inventories and independent pinned Go streaming matrix.
Both belong to `async:verify`; original unary interoperability remains separate.

## Native gRPC

Native HTTP/2 supports unary, server streaming, client streaming and bidi gRPC
with protobuf, identity/gzip, h2c, verified TLS and mTLS. Configure the core
Client with `protocol: :grpc` and the Server with `:grpc` or `:auto`. Generated
clients and handlers reuse the existing declarations. Portable HTTP/1 execution
retains its early rejection guard. See the [gRPC guide](../../docs/grpc.md) for
status codecs, actual final HEADERS, independent status bounds and native field
retention limits. Connect's one-second terminal tail policy remains Connect-only.

Use `async:conformance:grpc` for exact official inventories and
`async:interop:grpc` for generated official grpc-go peer checks. Both belong
to the owning verify task. `async:interop:grpc` first reproduces the official Go
and Ruby peer outputs through the Async bundle, the same check root
`grpc:generation:check` runs through the core bundle. `grpc:status:check`
reproduces private Any/Status descriptors.

## gRPC-Web

The native H2 server supports explicitly enabled binary/text unary and
server-stream gRPC-Web. Browser incremental delivery uses text. See the
[Web guide](../../docs/grpc-web.md) for protocol, CORS, bounds and completion
rules.

## Interceptors

Server interceptors configured on the core Server cover every RPC the native
host serves: [unary](../../docs/unary.md#interceptors) calls through
`server_unary`, and server, client and bidi
[streams](../../docs/streaming.md#interceptors) through `server_stream`, on
Connect and native gRPC, plus unary and server-stream gRPC-Web. They run after
core decodes a unary or server-streaming request, around the handler, with the
first interceptor listed outermost. They run in the per-request processing
task, so `host.peer_certificate` returns the authenticated client leaf inside an
interceptor, the same as in the handler. Stream wrapper methods see it only when
called from that task; a child task gets nil, so read the certificate in the
interceptor before starting child work. A continuation called from a child task
fails with `InterceptorStateError`.

Client interceptors run on the calling task or thread for `Transport`,
`HTTP1Transport` and `ThreadedTransport`, before core encodes the request.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional
[OpenTelemetry adapter](../connectrpc-otel/README.md). Native hosts create
explicit handles before a request's handler starts, activate the actual handler
tasks, and finish after full-write/reset/cleanup evidence. An HTTP/2 stream
refused while its HEADERS are read, because host admission is full, gets its
handle in a separate task after the refusal. That handle is never activated and
finishes once the reset is written, or as not delivered if the connection ends
first. Shared H2 client connection attempts run outside every RPC lifecycle, so
tracing never parents connection setup to the call that triggered it. Use the
transport's `connection_observer` for connection events.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`
and `library "connectrpc-async"` to a strict Steep target with this gem
installed. The manifest lists only standard libraries, so name each connectrpc
library explicitly; this also works with `rbs collection`. See the
[RBS guide](../../docs/rbs.md) for supported APIs, extension contracts, partial
records, runtime validation and framework-signature limits. `mise run rbs:check`
checks application callers for this package; strict core source checking is a
separate target from backend implementation checking.

## Development

Discover `async:setup`, `async:test`, `async:check`, `async:build`,
`async:package`, `async:example`, `async:interop`, and `async:verify` with
`mise tasks`. The separate bundle resolves the newest allowed dependencies into
`vendor/async`, and coverage uses `coverage/async`. `floors:async` repeats specs
and package checks with every runtime dependency at its gemspec floor.
`canary:async` is the non-gating pins-lifted lane: it runs the upstream contract
spec and the suite with the async-http and protocol-http2 patch pins lifted to
their newest releases. `async:verify` checks the independent bundle, installed
packages and connect-go interoperability. Core require and core/Rails
dependencies stay independent. The [backend evidence](#verification-evidence)
above describes what these checks exercise.
`async:soak` runs the non-gating long-running lifecycle campaign; see
[development](../../docs/development.md#lifecycle-campaign).

## License

Original project work is [MIT licensed](LICENSE), copyright 2026 Jim Myhrberg.
Both archives include the complete license and pin `allowed_push_host` to
`https://rubygems.org`; only the [release workflow](../../docs/releasing.md)
publishes them.
