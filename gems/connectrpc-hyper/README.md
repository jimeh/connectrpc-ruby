<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-hyper

**A reactor-free ConnectRPC client transport on Rust's hyper, h2 and rustls.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-hyper?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-hyper)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-hyper` is an optional client transport for
[connectrpc](https://github.com/jimeh/connectrpc-ruby). Since 0.3.0,
`ConnectRPC::Hyper::Transport` lets any Ruby thread make Connect and native
gRPC calls of all four shapes over HTTP/2, and Connect calls over HTTP/1.1,
without an Async reactor. A native extension built on hyper, h2, rustls with
the aws-lc-rs provider and tokio owns connections, streams and blocking waits.
Ruby keeps the protocol logic, deadlines, cancellation and error phrases.

The package is experimental and follows the release train's version. See the
[supported 0.x contract](../../docs/support.md) for the support profile and the
platforms that were built, installed and exercised.

## Installation

```ruby
gem "connectrpc-hyper", "~> 0.3.0"
```

The runtime dependency is `connectrpc ~> 0.3.0`. The transport never loads
connectrpc-async, Rack or Google protobuf.

### Prebuilt platform gems

Platform gems carry the extension for Ruby 3.4 and 4.0 and install without
Rust or a compiler:

| Platform | Requirement |
| --- | --- |
| `x86_64-linux`, `aarch64-linux` | glibc 2.29 or later |
| `x86_64-linux-musl`, `aarch64-linux-musl` | musl with `libgcc_s` (Alpine: the `libgcc` package) |
| `arm64-darwin` | macOS 11 or later |
| `x86_64-darwin` | Intel macOS; the binaries declare macOS 10.13, and CI smoke-tests them on macOS 15 |
| `x64-mingw-ucrt` | Windows x64, provisional (see [support](../../docs/support.md)) |
| `aarch64-mingw-ucrt` | Windows arm64, provisional (see [support](../../docs/support.md)) |

### Source gem

Elsewhere RubyGems installs the source gem, which compiles the extension at
install time with your `cargo`. It needs Rust 1.85 or later (the declared
`rust-version`), libclang for rb-sys's bindgen and a C compiler. The build uses
the packaged `Cargo.lock` with `--locked`, and set `CARGO_NET_OFFLINE=true` to
build from a pre-fetched crate cache. The source gem also depends on `rb_sys`
(`>= 0.9.130, < 1`); platform gems drop it. The extension is about 6 MB per
Ruby minor.

## Usage

```ruby
require "connectrpc/hyper"

transport = ConnectRPC::Hyper::Transport.new
client = ConnectRPC::Client.new(base_url: "https://api.example.com", transport: transport, protocol: :grpc)
# Generated clients take the same core client. Share one transport between threads.
transport.close(grace: 5) # => true when every in-flight call drained
```

Construction validates every option, raises `ArgumentError` for invalid ones
and starts no thread or socket. `inspect` shows the connection mode, `http2`,
whether TLS options are set and the state, never paths, key material or proxy
URLs.

| Option | Default | Meaning |
| --- | --- | --- |
| `connections:` | `:pooled` | `:pooled` shares one admitting connection per origin; `:per_call` opens one per call. |
| `idle_timeout:` | `1800` | Seconds before a pooled connection with no calls in flight closes; nil keeps it. |
| `keepalive:` | `nil` | A `ConnectRPC::Keepalive`; see [keepalive](#keepalive). |
| `http2:` | `true` | `false` sends Connect over HTTP/1.1 only. |
| `connect_timeout:` | `10` | Seconds for DNS, TCP, proxy `CONNECT`, TLS and the HTTP/2 handshake. Must be finite, as connectrpc-async requires, so every dial ends on its own. Each address of a host gets a share; see [connections](#connections-and-server-restarts). |
| `ca_file:` | `nil` | PEM file appended to the trusted roots. |
| `certificate_chain:` | `nil` | Client certificate chain, an Array of PEM strings, leaf first. |
| `private_key:` | `nil` | Client key as a PEM string; required with `certificate_chain:`. |
| `proxy:` | `:environment` | See [proxies](#proxies). |

Durations accept any finite positive number. Native code caps each one (and
`close(grace:)`) at about 115 days (9,999,999.999 seconds, the largest
`connect-timeout-ms`), so a larger value behaves as that cap.

`streaming_capabilities` is `[:server_stream, :client_stream, :bidi_stream]`
with HTTP/2 and `[:server_stream, :client_stream]` with `http2: false`.
`grpc_capabilities` is all four shapes with HTTP/2 and empty with
`http2: false`, so core rejects those calls before they reach the transport.

## Which transport

- Core `ConnectRPC::Transports::NetHTTP` is enough for simple Connect unary
  calls. It has no dependencies and opens one connection per call.
- `ConnectRPC::Hyper::Transport` serves any thread (Puma, Sidekiq, scripts)
  with pooled HTTP/2, every call shape and native gRPC.
- connectrpc-async's `Transport` and `ThreadedTransport` suit applications
  already on Async, or ones that need its connection policy (backoff,
  `wait_for_ready`, admission budgets) and connection observers.
- connectrpc-async's `PerCallTransport` makes calls from inside an Async
  reactor. A hyper call there blocks the reactor thread, unless Ruby 4.0 and
  async's opt-in worker pool offload it; see
  [fiber schedulers](#fiber-schedulers).

## Connections and server restarts

One HTTP/2 connection per origin and transport admits new calls. When there
is none, the transport starts one dial for the origin, owned by the transport
rather than by any call, and every caller that finds no connection waits for
that dial. As in a grpc-go channel, its outcome is the same for every waiter:
the new connection, or the failure. A pooled connection that ends before the
server's HTTP/2 preface (its SETTINGS frame) fails the dial's waiters with
that connection's failure instead of dialing again, as grpc-go treats a
transport that never received the server preface as a failed connection
attempt. So a server that ends every connection right after its handshake,
such as a TLS 1.3 server rejecting a missing client certificate, cannot keep
a caller dialing until its deadline. A connection that closes or drains
after the preface, such as one that received a graceful GOAWAY, was working:
its waiters dial again, as grpc-go re-picks a transport, and so do the
waiters of a connection the idle timeout closed. A waiter's own deadline,
cancellation or interrupt ends only its wait. A call with a short deadline
fails `deadline_exceeded` while the dial carries on and pools its connection
for later calls. A second HTTP/2 connection never replaces a usable pooled
one: it serves only the call that dialed it, then closes. hyper multiplexes calls
up to the peer's `SETTINGS_MAX_CONCURRENT_STREAMS`. Behaviour compared with a
grpc-go channel:

- An idle connection answers server PINGs and processes GOAWAY without a call
  in flight.
- After a graceful GOAWAY (a server restart or `MaxConnectionAge`), the
  connection stops admitting and drains its open streams up to the GOAWAY's
  last stream id, which finish normally. The next call dials a new
  connection. A stream above the last stream id was never processed
  (RFC 9113 section 6.8), so it is retried within the limits below, as
  grpc-go retries it. A server that drains in two steps first names the
  highest possible stream id, which disowns nothing; its eligible streams
  are retried once its final GOAWAY names a lower one.
- After a hard kill, calls in flight fail `unavailable` with
  `HTTP transport failed: connection closed`. Once the transport has read
  the reset, the next call reconnects; a call that starts before then can
  still go to the dead connection. If hyper had not taken its request yet,
  it hands it back and the call is retried (below); once hyper took it, the
  call fails the same way. grpc-go also retries a call whose headers its
  writer had not yet attempted; hyper does not report that, so this
  transport does not.
- A peer that goes silent without closing the socket is only detected by
  [keepalive](#keepalive) or the call deadline.
- Reconnection is lazy. There is no `wait_for_ready`, no background reconnect
  and no backoff: a call that cannot connect fails at once with the connect
  phrase, and the next call dials again. connectrpc-async's connection policy
  offers backoff and `wait_for_ready`.
- `idle_timeout` (grpc-go's 30 minutes) closes a pooled connection with no
  calls in flight. The timer claims the exact connection it saw idle, so it
  never closes a connection a caller has just checked out, and a connection
  closes only after a full `idle_timeout` without calls.
- A request waiting inside hyper for a `SETTINGS_MAX_CONCURRENT_STREAMS`
  slot already has a stream id but never reached the wire. When a GOAWAY's
  last stream id falls below that id, it is retried on any call shape, as
  are requests still in hyper's own queue (below). hyper drops such a request
  on a server's first GOAWAY, which may name the highest possible stream id.
  Unless the final GOAWAY's lower last stream id has already arrived by then,
  it fails `unavailable` (`HTTP transport failed: connection closed`).

`connections: :per_call` dials a fresh connection for every call and closes it
afterwards. With `http2: false`, each call dials its own connection, in
parallel, as Go's `net/http` dials them, since one HTTP/1.1 connection serves
one call at a time. When TLS negotiates `http/1.1` for an `http2: true`
transport, each caller that waited for that dial uses a usable pooled or idle
connection if there is one, such as the clean connection that dial parked,
and otherwise dials its own; a streaming call, which needs HTTP/2, fails with
the ALPN phrase instead. A later burst that finds no idle connection waits for one
shared dial again. Pooling keeps at most two idle keep-alive connections per
origin (Go's `DefaultMaxIdleConnsPerHost`); only a connection whose request
and response both completed returns to the pool.

Requests the server never processed are retried once on a fresh connection,
within the original deadline and with the wire timeout recomputed:

- on any call shape, a request that never reached the wire: one hyper hands
  back unsent, or one whose stream id is above a GOAWAY's last stream id and
  whose HEADERS never reached the socket (a streaming request also needs its
  body unread);
- a Connect unary call, before response headers, that the server refused with
  `REFUSED_STREAM` or whose stream id is above a GOAWAY's last stream id.

tokio's request channel can keep a request sent just as hyper's dispatcher
ends: never received and never answered. Since hyper 1.13.0 a dispatcher that
shuts down waits for such a request and hands it back. A request can still be
stranded when a forced close stops the dispatcher before it finishes, or when
an HTTP/1.1 connection ends in an upgrade. The transport detects one once the
dispatcher is gone, as Go's `net/http` watches a connection's close while it
waits for a response. A Connect unary call treats it as handed back unsent,
within the retry above. A streaming call, gRPC unary included, fails with its
connection's failure, since its body cannot be recovered. A `write` blocked
on that body returns, and a later `write` raises that failure.

Nothing else is replayed, so a handler that started never runs twice. After
a GOAWAY, a stream at or below its last stream id is never replayed; a
`REFUSED_STREAM` refusal still is, since RFC 9113 section 8.7 says that
stream was not processed. Nor is a gRPC call whose HEADERS were sent, since
`open_grpc_stream` cannot tell unary from streaming. hyper and h2 report
neither stream ids nor the last stream id, so the transport derives each
request's stream id from the order hyper hands requests to h2 and reads the
last stream id off the connection's HTTP/2 frame headers. The server's GOAWAY
is the authority on what it processed, as its `REFUSED_STREAM` is; a
malformed GOAWAY, or one that raises the last stream id, ends GOAWAY retries
on that connection. A Connect unary call whose HEADERS were sent is replayed
only when h2 itself failed its stream for a GOAWAY. A call canceled before
its retry is queued is not retried. When the pooled connection that refused
or handed back the request still admits calls, it stays pooled for them and
the retry dials a connection that serves only that call. A connection that
received GOAWAY admits nothing more, so its retries share the next pool dial.

Hostnames resolve through tokio's blocking pool (`getaddrinfo`), capped at
512 threads, close to Go's 500 cgo lookup threads. A stalled lookup keeps its
thread after the caller leaves. Addresses of both families are tried with Go's
300 ms fallback delay. Within one family, addresses are tried in turn, each
within an equal share of what is left of the setup budget, but at least 2
seconds (Go's `partialDeadline`, as NetHTTP's dialer does), so a blackholed
first address does not use up the whole budget. The shared HTTP/2 dial's
budget is `connect_timeout`; a dial that serves one call (per-call mode,
`http2: false`, a call after an `http/1.1` outcome, or a retry that leaves a
pooled connection in place) gets the earlier of `connect_timeout` and its
call's deadline. A call whose deadline ends during setup still fails
`deadline_exceeded`. A shared dial still running when its transport is
garbage collected ends within `connect_timeout`.

## Keepalive

```ruby
keepalive = ConnectRPC::Keepalive.new(time: 60, timeout: 20, permit_without_calls: false)
transport = ConnectRPC::Hyper::Transport.new(keepalive: keepalive)
```

`ConnectRPC::Keepalive` is core's value (Since 0.3.0); connectrpc-async's
`ConnectRPC::Async::Keepalive` is the same class. It maps to hyper's
`keep_alive_interval`, `keep_alive_timeout` and `keep_alive_while_idle`. After
`time` seconds without a read, the transport sends a PING. Without its ACK
within `timeout`, the connection retires, its calls fail `unavailable` and the
next call reconnects. hyper's clock differs from grpc-go's:

- It resets on response HEADERS, on DATA and trailers as Ruby consumes them,
  and on PING ACKs. Server PINGs, SETTINGS and WINDOW_UPDATE do not reset it,
  so it may ping earlier than grpc-go.
- DATA that arrived while the peer went silent moves the clock only once Ruby
  reads it, so a stalled reader can see detection later than grpc-go would.
- Without `permit_without_calls`, a connection with no calls is never probed.
- Keepalive and BDP flow control share one outstanding PING.

A server's keepalive enforcement (grpc-go's `MinTime`, 5 minutes by default)
can close connections that ping more often; choose `time` to match the server.

## Interrupts, deadlines and cancellation

Waits release the GVL. `Thread#raise`, `Thread#kill`, `Timeout.timeout`, the
call deadline and a cancellation token stop a call during name resolution,
connect, TLS, a blocked write and a blocked read. The stream is reset with
`CANCEL`, so the peer sees `RST_STREAM(CANCEL)`, and the shared connection
keeps serving other calls. A token cancels name resolution, connect and TLS,
including a wait for the shared dial (which carries on for other callers). The wire timeout headers
(`grpc-timeout`, `connect-timeout-ms`) carry the deadline remaining at dispatch,
after connection setup and on a retry. The one wait that holds the GVL is a
process's first use waiting for the native runtime to start: normally under a
millisecond, and at most 5 seconds when the start fails (see [errors](#errors)).

## Fiber schedulers

Inside an Async reactor, a call blocks the reactor thread, and with it every
sibling task, until it finishes. On Ruby 4.0 with async's opt-in worker pool
(`ASYNC_SCHEDULER_WORKER_POOL=true` or
`Async::Scheduler.new(worker_pool: true)`), the wait moves to a worker thread:
sibling tasks keep running, and `Task#stop`, cancellation, deadlines,
`Thread#raise` and `Thread#kill` still land. async creates io-event's worker
pool with one worker by default, so parked waits from different tasks run one
at a time: a second task's wait queues until the first one's ends. Pass
`Async::Scheduler.new(worker_pool: IO::Event::WorkerPool.new(maximum_worker_count: N))`
for more workers. Waits offer a fiber scheduler's `blocking_operation_wait`
hook only on Ruby 4.0 and later, whose blocking-operation API lets io-event
finish a running wait before it raises a cancellation. Ruby 3.4 hands the hook
a plain proc, so a cancelled scheduler could unwind the call while its wait
still runs on a worker; there calls always block the reactor, whatever the
scheduler. Use connectrpc-async's transports for calls from inside a reactor.

`close` is the exception. Called from a non-blocking fiber under a fiber
scheduler, on Ruby 3.4 and 4.0 alike, it waits for teardown on a helper
thread, taking neither the reactor thread nor a worker of the pool, so sibling
tasks keep running and their in-flight calls can drain within the grace. A
closer inside `Fiber.blocking {}`, or one whose helper thread cannot start
(`ThreadError`), waits on its own thread and holds the reactor until teardown
ends.

## Fork

The tokio runtime has two `hyper-runtime` worker threads plus tokio's blocking
pool, created lazily on the first call in each process. A short-lived
`hyper-runtime-start` thread builds it, and exists only while that first use
waits for the start. A forked child never touches its parent's runtime or
connections: it builds its own on first use. The child closes, never shuts
down, the inherited connection sockets it can identify, so the parent's
connections keep working and a long-lived child does not hold them open. A
socket still connecting at the moment of fork is not identified and stays open
in the child until it exits. A transport closed before fork stays closed in the
child.

## TLS

Native roots come from the platform store through rustls-native-certs, loaded
once per process. `SSL_CERT_FILE` or `SSL_CERT_DIR` replaces the platform store
rather than adding to it. `ca_file` appends its certificates to whichever roots
were selected. TLS 1.2 is the minimum, session resumption is off, ALPN offers
`h2, http/1.1` (`http/1.1` with `http2: false`) and IP literals get no SNI.

Inputs are validated in Ruby with OpenSSL, as core NetHTTP validates them. An
invalid input raises `ArgumentError` naming only the input, for example
`Invalid TLS client configuration: private_key`, with no `cause`. A TLS option
on an `http://` request raises `ArgumentError, "TLS options require an HTTPS
origin"`. rustls is stricter than OpenSSL:

- CN-only server certificates (without a matching SAN), X.509 v1
  certificates, a wildcard over a single label and a CA certificate used as the
  server certificate fail with `HTTP transport failed: TLS verification failed`.
- Encrypted private keys are not accepted.
- A key OpenSSL accepts but aws-lc-rs cannot sign with, such as Ed448, fails at
  construction with the `private_key` error.
- `ca_file` is read with OpenSSL's store semantics, as NetHTTP reads it: a
  CRL-only file adds no roots, and trust settings in `TRUSTED CERTIFICATE`
  blocks are dropped. A `ca_file` certificate that rustls cannot use as a
  trust anchor is rejected at construction.

## HTTP/1.1 and ALPN

`http2: false` sends Connect unary, server-streaming and client-streaming calls
over HTTP/1.1, cleartext or TLS. With `http2: true`, every streaming call needs
h2: when TLS negotiates `http/1.1`, `open_stream` and `open_grpc_stream` fail
`unavailable` (`HTTP transport failed: ALPN negotiation failed`) before any
request is sent, so the server sees no application request. While the origin's
pool holds an idle HTTP/1.1 connection from such a negotiation, a streaming call
fails with the same phrase without dialing. Connect unary calls proceed over
HTTP/1.1. Cleartext `http://` with HTTP/2 uses prior knowledge (h2c).

## Proxies

`proxy:` follows core NetHTTP's rules (Since 0.3.0); the
[NetHTTP proxy guide](../../docs/unary.md#nethttp-tls-and-proxies) has the full
environment and `NO_PROXY` semantics.

- `proxy: :environment`, the default, snapshots `HTTP_PROXY`, `HTTPS_PROXY`,
  `NO_PROXY` and their lowercase forms when the transport is constructed and
  applies Go 1.27.3's rules through core's resolver. `localhost` and loopback
  addresses never use the environment proxy, and in a CGI process
  (`REQUEST_METHOD` set) `HTTP_PROXY` fails `http://` calls instead of being
  trusted.
- A String or URI proxies every request, including loopback and `NO_PROXY`
  hosts. `proxy: nil` always connects directly.
- Only `http://` proxies are supported, with Basic `Proxy-Authorization` from
  the URL's percent-decoded userinfo. An explicit invalid or unsupported proxy
  raises `ArgumentError` (`Invalid proxy configuration` or
  `Unsupported proxy scheme`) with no cause. A problem with an environment
  proxy fails the affected calls with `unavailable` and NetHTTP's fixed phrases
  (`invalid proxy address`, `unsupported proxy scheme`,
  `proxy refused in CGI environment`) before any socket opens, and never falls
  back to a direct connection.
- h2c and TLS targets tunnel through `CONNECT`, as grpc-go does; TLS verifies
  the target host, not the proxy. Plaintext HTTP/1.1 (`http2: false`) requests
  to `http://` targets go to the proxy in absolute form, as NetHTTP and Go's
  `net/http` send them, so proxies that allow `CONNECT` only to port 443 still
  work. A forwarding proxy handles each request as an HTTP intermediary:
  streaming calls over this path depend on it passing chunked bodies through
  incrementally. TLS targets always tunnel, end to end.
- The environment proxy is chosen by the target URL's scheme, as Go's
  `net/http` and NetHTTP choose it: an h2c target is `http://`, so it takes
  `HTTP_PROXY`. grpc-go instead looks proxies up with an `https` scheme for
  every target, so it would use `HTTPS_PROXY` for the same h2c target.
- A `CONNECT` exchange reads at most a 10 MiB reply head within 60 seconds, as
  in Go, and needs exactly status 200. For TLS and HTTP/1.1 targets no bytes
  may follow the reply head, as in NetHTTP; for h2c they may, because an h2c
  server can send its SETTINGS right behind the proxy's reply, and grpc-go
  keeps such bytes. Failures are `unavailable` with
  `proxy authentication failed` (407), `proxy tunnel refused`,
  `proxy tunnel failed` or `proxy tunnel timed out`, and proxy reply bytes
  never reach errors. The call deadline, cancellation, interrupts and
  `connect_timeout` also cover the proxy hop; an earlier deadline still ends
  the call with `deadline_exceeded`.
- Pooled connections are kept per origin and proxy route (proxy address and
  credentials), so direct and proxied calls never share a connection.
  `inspect` shows only `proxy=environment`, `explicit` or `direct`, never the
  proxy URL or credentials.
- The native code never reads proxy variables itself; Ruby resolves the proxy
  for each request, and `cargo deny` rejects hyper-util's proxy features.
- Compared with Go: `CONNECT` sends no `User-Agent` (Go sends
  `Go-http-client/1.1`), forwarded HTTP/1.1 connections are not shared across
  targets, and HTTPS and SOCKS proxies are not supported.

Core's default `User-Agent` reaches the wire unchanged, and caller metadata
that sets `user-agent` wins.

## Limits

- A Connect unary response larger than the call's response limit fails
  `resource_exhausted` (`Response exceeds byte limit`), whether the
  `content-length` advertises it or the body grows past it.
- gRPC response headers and trailers are limited to core's 65,536 bytes of
  regular fields (`gRPC field block exceeds byte limit`), and outbound gRPC
  metadata over the limit fails before dispatch. h2 itself accepts header
  blocks up to 16 MiB, grpc-go's default, so a hostile block above that can
  still truncate trailers.
- An HTTP/1.1 response head (status line and fields) may be up to 10 MiB, the
  bound Go's `MaxResponseHeaderBytes` default puts on the response head, in
  bytes only. This transport applies the same 10 MiB cap to chunked trailers,
  its own choice. hyper also counts fields, so this transport accepts up to
  1,024 of them in a head or in trailers; hyper's own defaults (100 fields,
  about 400 KiB) are lower. A larger head or trailer block fails with
  `malformed response`.
- Streaming messages follow core's message limits. Flow control uses BDP
  adaptive windows capped at 16 MiB like grpc-go, and each stream buffers at
  most 1 MiB of unsent request data, so `write` blocks while the peer does not
  read, until the deadline.

## Errors

Transport failures are `unavailable` with a fixed message and the native
failure as `cause`:

| Message | Failure |
| --- | --- |
| `HTTP transport failed: name resolution failed` | DNS |
| `HTTP transport failed: connection refused` | TCP refused |
| `HTTP transport failed: host unreachable` / `connection failed` | Other connect errors |
| `HTTP transport failed: timed out` | `connect_timeout` |
| `HTTP transport failed: TLS verification failed` / `TLS error` | Certificate or handshake failure, or a TLS rejection the server sends after the handshake (such as for a missing client certificate) when the connection that fails the call has read it. A connection whose write fails first reports `connection closed`, as grpc-go reports a write error. A call retried after its first connection read the rejection reports its retry connection's failure |
| `HTTP transport failed: ALPN negotiation failed` | Streaming call on an HTTP/1.1 TLS connection |
| `HTTP transport failed: connection closed` | The connection closed, retired or received GOAWAY |
| `HTTP transport failed: malformed response` | Protocol error |
| `HTTP transport failed: stream reset` | A Connect stream reset by the server |
| `HTTP transport failed: stream closed` | A streaming write after an HTTP/2 stream ended, when the transport cannot tell why (below) |
| `HTTP transport admission is closed` | A call after `close`, or one forced teardown ended after its own setup saw `close`, with no `cause` |

A server `RST_STREAM` on a gRPC call maps as connectrpc-async does: codes 3
and 11 to `resource_exhausted`, 7 to `unavailable`, 8 to `canceled`, 12 to
`permission_denied` and anything else to `internal`. Deadlines and cancellation
raise core's `deadline_exceeded` and `canceled` errors first.

A streaming `write` that starts after the server reset the stream, or ended
its response and the stream, raises instead of discarding the message, as
connectrpc-async and grpc-go do. The write in flight when the stream ends may
still return, since its bytes were already queued. The write raises the
stream's own failure when the response side already knows it: a reset before
the response head, or after the head with no unread response data, raises the
reset as mapped above, and an HTTP/1.1 response failure the client already
received raises that failure. Otherwise the write raises `unavailable` with
`HTTP transport failed: stream closed` on HTTP/2, and with
`HTTP transport failed: connection closed` on HTTP/1.1, where the connection
ending is what usually ends the upload. Core then stops writing and reports the
server's status from the response, or raises the write's failure when the
response carries no error status. The response side still reads the head,
every response byte, and the trailers or the reset.

A single streaming `write` of 2 GiB or more raises `ArgumentError` before
anything is queued: h2 refuses a chunk that large, which would end the upload
while the response waits for it. Core writes at most 16 KiB at a time.

hyper does not tell the request body why the stream ended, which limits this:

- A gRPC reset behind unread response data, or one that arrives while another
  thread reads the response after its head, makes the write raise
  `stream closed`. Core then reports `unavailable` where connectrpc-async
  reports the reset's mapped code. The reader still receives the reset.
- Over HTTP/1.1 the write never waits for the response, since hyper also ends
  an upload its own framing finished while the response is pending: a request
  that declares a `Content-Length` once the body reached it (at once for a
  length of 0), and any upload on a reused HTTP/1.0 connection. Such a write
  also raises `connection closed`, although the connection may still be open.
  Core never sends `Content-Length` on a stream.
- A response head followed by `RST_STREAM(NO_ERROR)` without a valid status
  also raises `stream closed`, where connectrpc-async raises `internal`.
- A server that ends its response without resetting the stream leaves the
  stream half-open, and writes keep succeeding until the call ends, as they
  do with connectrpc-async.

`close(grace: 5)` stops admission, lets in-flight calls finish within `grace`,
then aborts the rest and waits for every connection task. It returns true when
everything drained and false after forced teardown; repeated and concurrent
calls share the first call's grace and outcome. Callers waiting for the shared
HTTP/2 dial fail with `HTTP transport admission is closed`, and the dial ends.
A call admitted before `close` that is dialing its own connection (per-call
mode, HTTP/1.1, a dial after an HTTP/1.1 outcome, or a retry's own dial) is in
flight: it keeps dialing and runs within the grace, but its new connection is
never pooled for other calls. Lazy exchanges that have not dispatched and
pending retries fail with `HTTP transport admission is closed`. Forced teardown
fails each call it ends `unavailable` with `HTTP transport admission is closed`
if the call's own setup saw `close` first, and otherwise with
`HTTP transport failed: connection closed`. Which of the two a call gets is
not guaranteed, and it never gets another phrase. Teardown runs in the
background once started, so it completes even if the closing thread is interrupted or a
fiber scheduler stops the closing task (async's `Task#stop`). Every closer, a
concurrent one included, keeps waiting for the outcome and raises the
exception after teardown. On a non-blocking fiber under a fiber scheduler that
wait runs on a helper thread, so sibling tasks still run and their calls can
drain, and threads that sibling tasks start meanwhile keep their usual
interrupt handling (see [fiber schedulers](#fiber-schedulers)). Such a closer
stops waiting if it is killed (`Fiber#kill` or `Thread#kill`); teardown still
completes, and later closers wait for and return the published outcome.
Without a fiber scheduler, or on a blocking fiber, a closer defers
`Thread#kill` until `close` returns.

The first close in a process can be the first use of the native runtime. If
the OS refuses the runtime's first thread then, `close` raises Ruby's `fatal`
without starting teardown, and the transport stays closing: later calls still
fail with `HTTP transport admission is closed`, and a later `close` starts
teardown and returns its outcome. A closer whose helper thread could not start
either retries once, and if that starts teardown, it raises the `fatal` after
the outcome is published, as it does with an interrupt. The first use waits
for the runtime's start for at most 5 seconds, holding the GVL; a start that
fails or takes longer raises `fatal` the same way, and a later use starts the
runtime afresh. A closer whose helper thread could not start, and that retries
once itself, can wait for the start twice. If the OS refuses only the runtime's
second worker thread, tokio 1.54 never finishes that start
([tokio-rs/tokio#8408](https://github.com/tokio-rs/tokio/pull/8408); the
unreleased
[tokio-rs/tokio#8752](https://github.com/tokio-rs/tokio/pull/8752) makes it
fail instead), so the first close or call raises after the 5 seconds, and the
stuck start keeps its start thread and first worker thread for the life of the
process.

## License

MIT. The extension links third-party Rust crates under their own licenses,
listed with their license texts in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
