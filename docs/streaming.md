# Native RPC streaming

The [supported 0.x contract](support.md) defines the target compatibility
boundary and the remaining adoption gates.

Connect server streaming, client streaming and bidirectional streaming use the
same dependency-free core engine. The optional Async transport and Async/Falcon
native HTTP/2 hosts support protobuf and ProtoJSON, identity/gzip, h2c, verified
TLS and mTLS. Falcon also serves Connect server and client streaming over
HTTP/1.1 with both codecs and identity/gzip over TCP/TLS/mTLS. The explicit
`ConnectRPC::Async::HTTP1Transport` provides unary, SS and CS on the owning
reactor. The standalone Async Host and original Async Transport remain H2-only. NetHTTP, Rack, Rails and Testing remain unary.
Unsupported clients reject a streaming operation before encoding or I/O. Native
listeners explicitly select their actual protocol capabilities. Portable adapters reject streaming
media before reading input or invoking handlers, including Rails callbacks.

Native gRPC reuses the declarations and session engine below, including unary
calls through the exchange SPI. Its protobuf-only negotiation, final HTTP/2
HEADERS, status codec and independent bounds are described in [gRPC](grpc.md).
The Connect terminal envelope and one-second tail policy below apply to Connect.

## Falcon HTTP/1.1 server

Custom hosts opt in with `Server#call_stream(capabilities: [:server_stream,
:client_stream], ...)`. The default `capabilities: nil` retains portable
rejection; legacy `http2: true` grants all three shapes. Explicit capabilities
must be a nonempty, unique Array of supported shape symbols and cannot be
combined with `http2: true`. Omitted SS/CS returns bounded `unimplemented`;
omitted known bidi returns host-neutral bare 505 before input or codecs.
Falcon turns every native H1 bidi 505 into a nonpersistent response with exactly
one `Connection: close`. Web negotiation and CORS retain precedence. Native
gRPC remains H2-only, and HTTP/1.0 Connect streaming is rejected.

The original processor retains handlers, codecs, authenticated peer identity,
trace activation and application cleanup. Response consumers transmit prepared
bytes. Headers can publish during input reads, but successful HTTP terminal
chunk delivery waits for original application cleanup. Leading metadata and
propagation exclude actual chunked trailers, including `connect-timeout-ms`.
H1 peer EOF does not synthesize Context cancellation.

When original framed input is incomplete at response-head resolution, Falcon
disables reuse and, since 0.3.0, reads the unread body before it writes the
response head, as Go's `net/http` server does (`maxPostHandlerReadBytes`). It
reads at most 256 KiB, though the last chunked read can pass that by less than
64 KiB, and leaves a declared remainder of 256 KiB or more unread.
`request_body_timeout` bounds each read wait and the Connect deadline bounds the
whole read; progress never extends the deadline, and `grpc-timeout` does not
apply. A stalled, expired or malformed remainder still receives the response.
Only the connection task reads, once the original processor has stopped reading
input: it has finished, or it has produced response output, which core does
only after a handler's last input read. During the read a separate task drains
that output into memory, so processing never blocks on it; the drain never
waits for processing completion or lifecycle observers. A head published before
any output, such as headers sent during CS input reads, is written at once, and
the remainder is read before the first output chunk. A bidi 505 and an
unanswered `Expect: 100-continue` request skip the read.

A successfully delivered response on a retiring connection uses one absolute
final-close expiry at the actual final HTTP flush plus 500 ms. On a successful
write, protocol-http1 closes the body only after its final flush (for HTTP/1.0
close-delimited bodies, also after the write half-close). A private body wrapper
therefore anchors the expiry when the writer closes the body without an error
after EOF or a HEAD response, before the body's close callbacks or lifecycle
observation. A response without a body anchors when the writer returns. Payload
bytes cannot trigger it. Cancellation can also close the body without an error, but
the response is then undelivered and ignores the expiry. Delayed callbacks
consume the same total budget. After completed delivery, Falcon half-closes raw
TCP writes and waits for the rest of that budget without reading, like
`net/http`'s `rstAvoidanceDelay`. Earlier owner abort/shutdown can shorten
either phase. Failed or incomplete delivery gets no success grace. Late
hygiene/grace failures preserve completed processing and delivery facts.

Admission and socket ownership remain held until retirement and original
processing join. Retirement never parses or dispatches a pipelined request,
even if the original request finishes. Fully consumed nonretiring input retains
persistence. Raw socket abort precedes framed/Finishable close on forced teardown.
Both timers are cooperative and can overshoot under scheduler delay. Remaining
grace retains resources and is best effort reset avoidance, not a guarantee of
response delivery under arbitrary network or peer failure. The
[support contract](support.md#falcon-connect-h1-retirement) promises this
behavior and treats its budgets as provisional tuning values.

The production official H1 profiles pass 174/174 identity and 160/160 gzip cases
on both pinned Rubies, including the original oversized CS cases over TCP/TLS.
No official assertion or inventory changed. Unlike `net/http`, Falcon never
reuses a connection after incomplete input, and it also reads when the client
sent `Connection: close`. Native H1 Web retains its separate policy: original
input cleanup gates the staged Web terminal, and an incomplete upload also gates
a rejection head to avoid stopping the client's upload, with its own 100 ms
allowance. The historical Web TCP/TLS failures, diagnosis and separate baseline
repair are recorded in the [conformance
status](../conformance/README.md#connect-http11-server-profiles).

## Completion policy

After a terminal envelope, the client requires HTTP EOF and transport completion
within one **absolute second**, with at most one extra byte read. Any extra byte
is a protocol error. A stalled tail is `unavailable`, including with `timeout: nil`.
A shorter real RPC deadline or explicit cancellation can win first. Otherwise
valid responses with late EOF or slow intermediaries can be rejected. This is
this library's interoperability policy, not a Connect requirement, RPC deadline,
wire timeout header or general stream watchdog. The
[support contract](support.md#native-client-completion) classifies it as
provisional, and [#102](https://github.com/jimeh/connectrpc-ruby/issues/102)
evaluates relaxing it.

An incidental send I/O failure permits terminal-only recovery of at most 262,144
additional wire body bytes in one absolute 100 ms, including response headers,
buffered remainders, prefixes, skipped data, terminal tail and transport finish.
Recovery skips data incrementally without decoding or inflating it. It cannot
steal a live reader or resume normal reading after discarding data. Only a valid
remote error with verified HTTP tail can replace the generic write failure.
Remote success cannot erase an unhandled send failure; an application may handle
a send failure and complete its normal response path. Recovery and the existing
100 ms destructive cleanup stage are separate, up to 200 ms together. Native or
application code that blocks the reactor remains outside hard timing guarantees.

## Clients and declarations

`ServerStreamMethod[Q,R]`, `ClientStreamMethod[Q,R]` and `BidiStreamMethod[Q,R]`
are invariant siblings of unary `Method[Q,R]`. A mixed `Service` can contain all
four. Each operation requires a block:

```ruby
client.server_stream(watch, request, timeout: nil) do |reader|
  reader.headers # Waits for leading metadata without consuming a message.
  reader.each { |message| consume(message) }
  reader.trailers
end # => nil

response = client.client_stream(collect) do |writer|
  inputs.each { |message| writer.send_message(message) }
end # Auto close_send, exactly one response, terminal and tail, then Response[R].

client.bidi_stream(chat, cancellation: source.token) do |duplex|
  duplex.send_message(request)
  reply = duplex.receive
  duplex.close_send
  duplex.each { |message| consume(message) }
end # => nil
```

Generated mixed-service wrappers preserve these blocks, all options, actual nil
timeouts and cancellation tokens. Since 0.3.0, each operation also accepts
`deadline:`, and a stream uses the earlier of it and `timeout:`, as a
[unary call](unary.md#deadlines) does. A handler passes `deadline:
context.deadline` to bound a downstream stream by its own deadline. The same configured `Client(use_get: true)`
uses GET for eligible unary calls and POST for streaming, without mode changes,
retry, fallback or resend. `mode:` defaults to `:unary`, its only accepted
value, so streaming needs no mode change.

`send_message` returns nil after bounded local ownership transfer, not wire
flush or peer receipt. Nil is forbidden as a streaming message. `receive` returns
nil only after successful validated EOF; an empty/default protobuf is a message.
`each` is block-only. `close_send` is idempotent and sends HTTP END_STREAM, never
a request terminal. Sending afterward fails locally before codecs or I/O.

One reader and one writer may run concurrently on the owning reactor/thread.
Overlapping operations in the same direction fail locally. Applications own
explicit Async concurrency; no hidden producer thread or stream Enumerator is
created. SS/bidi ordinary exit without EOF raises `StreamNotConsumedError` and
aborts. Explicit `abandon` allows intentional SS/bidi exit without claiming
success; CS abandonment raises because no Response exists. Nonlocal break/return
keeps Ruby semantics and runs bounded abandonment. No abandonment drain occurs.
Escaped I/O fails locally; already captured metadata remains readable. Independent
user exceptions retain identity/backtrace over cancellation or cleanup. SDK
operation failures retain call arbitration. A call never completes its shared
application cancellation source.

## Handlers and publication

Handlers receive `(Q, ResponseWriter[R], StreamingContext)` for SS,
`(RequestReader[Q], StreamingContext) -> R` for CS, and
`(ServerDuplex[Q,R], StreamingContext)` for bidi. SS/bidi return values are ignored;
only `send_message` emits a response. SS validates exactly one input and request
EOF before invocation. CS success requires exactly one response. Cardinality
violations are `unimplemented`. `context.request_metadata` stays mutable; reader
and duplex `headers` return a frozen snapshot taken at the first read, which
later Context writes do not change.

Since 0.3.0, a generated service's `handlers_for(impl)` registers an object's
public methods with these same arguments, such as `def watch(request, writer,
context)`, beside unary methods. RPCs the object lacks are left out, so they
answer `unimplemented`:

```ruby
server = ConnectRPC::Server.new(services: [service::SERVICE],
                                handlers: service.handlers_for(MixedService.new, strict: true))
```

`context.send_headers(metadata: {})`, also delegated by response writers and
server duplexes, publishes once. It can precede a handler delay or CS input.
First payload/terminal implicitly publishes. Leading metadata is deeply frozen
at publication; trailers remain mutable until terminal serialization, then freeze.
There is no synthetic nil message for publishing headers. Core invokes the host
publisher at most once per call. If publication fails, the call raises and the
host aborts the response; core never finishes a stream with a terminal but no head.

The original processor task owns authenticated peer identity, cancellation and
instrumentation through handlers, codecs and cleanup. A separate publication
promise exposes response headers while processing continues. Body tasks consume
prepared bytes. Successful HTTP EOF waits for original processor completion;
late cleanup failure aborts/reset. Admission and graceful drain include processing,
wire tail and cleanup. Only actual peer RST_STREAM(CANCEL) signals explicit server
Context cancellation. EOF, half-close, other reset codes and host shutdown do not.
A long-lived handler that blocks on application state can register
`context.on_cancel { ... }` to wake itself on that signal; the
[health service](../gems/connectrpc-health/README.md) Watch uses it this way.

## Interceptors

Client and Server [interceptors](unary.md#interceptors) also cover streams. An
interceptor's `client_stream(call)` or `server_stream(call)` method wraps the
whole stream scope for server, client and bidi streaming on Connect and native
gRPC, and for gRPC-Web server streaming. One object may define unary and stream
methods; core runs only the method for each call's role and kind. The unary
continuation rules apply, except that every stream layer must yield on both
roles: one that returns without yielding raises `InterceptorStateError`.

```ruby
class ValidateMessages
  def initialize(validator) = @validator = validator

  def server_stream(call)
    case call
    when ConnectRPC::ServerStreamCall::ServerStreaming then @validator.check!(call.request)
    end
    call.wrap_stream { |stream| ValidatingStream.new(stream, @validator) }
    yield
  end
end

class ValidatingStream < SimpleDelegator
  def initialize(stream, validator)
    super(stream)
    @validator = validator
  end

  def send_message(message)
    @validator.check!(message)
    __getobj__.send_message(message)
  end
end
```

This example validates the server-streaming request and every message the
handler sends. It does not validate incoming messages; see the delegation rule
below.

`ClientStreamCall` exposes `method`, `protocol` (`:connect` or `:grpc`),
`request_metadata`, `deadline`, `cancellation` and `wrap_stream`.
`ServerStreamCall` exposes `method`, `protocol` (`:connect`, `:grpc` or
`:grpc_web`), `context`, the handler's `StreamingContext`, and `wrap_stream`.
Server streaming calls are the `ClientStreamCall::ServerStreaming` and
`ServerStreamCall::ServerStreaming` subclasses, which add `request` and
`request=`; narrow with `case`/`when` before replacing the request.
`method.cardinality` names the shape. `request=`, `wrap_stream` and client
request metadata writes work until the innermost continuation starts dispatch.
Afterwards `request=` and `wrap_stream` raise `InterceptorStateError` and
metadata writes raise `FrozenError`. `wrap_stream` without a block raises
`ArgumentError`.

### Order and wrappers

The first interceptor listed is the outermost on both roles. On the client the
chain runs inside the call's deadline and cancellation scope, before the stream
opens. The innermost continuation opens the stream, applies the wrappers, sends
the server-streaming request, runs the application block and completes the
scope, so code after `yield` sees the scope's result or exception. On the
server the chain runs after the stream session exists; for server streaming,
core first reads and checks the single request, as it does without
interceptors. The innermost continuation calls the handler.

`call.wrap_stream { |stream| ... }` registers a wrapper. The block receives the
inner endpoint and returns the object that outer layers, and finally the
application block or handler, receive instead:

| Shape | Client endpoint | Server endpoint |
| --- | --- | --- |
| Server streaming | `ResponseReader` | `ResponseWriter` |
| Client streaming | `RequestWriter` | `RequestReader` |
| Bidi | `ClientDuplex` | `ServerDuplex` |

- Core applies wrappers innermost first. The application receives
  `outer(inner(raw))`, and its `send_message` passes through the outermost
  wrapper first. Each block runs once per call, so wrapper state stays per
  stream.
- After each block returns, core checks that its result responds to every
  public method of the endpoint it replaces, inherited ones included. A
  wrapper that drops a method, such as `each` on a duplex, raises
  `InterceptorStateError` naming it before the application block runs.
- A wrapper should delegate every method it does not change. Delegation does
  not route reads through the wrapper: an endpoint's `each` calls its own
  `receive`, so a wrapper that overrides only `receive` is bypassed by every
  read made through `each`. A read-side wrapper must override both, for example
  by implementing `each` as a loop over its own `receive`.
- Wrappers are duck-typed; application blocks and handlers keep their nominal
  endpoint types.

### Cleanup and results

Core runs abandonment, `close_send`, session close, terminal rendering and
lifecycle completion once, on the raw session and context, whichever
interceptor, wrapper or application block raised. A wrapper's explicit
`close_send` followed by stream completion reaches the exchange once. A wrapper
retained after its scope delegates to a closed session and raises
`StreamStateError`.

`yield` returns the scope result. For client streaming that is the frozen
`Response` on the client and the handler's response message on the server;
for server streaming and bidi it is nil, and core ignores interceptor return
values. The value the outermost client-streaming interceptor returns is the
result: the client returns it, and it must be a `Response` whose `metadata`
and `trailers` are `Metadata`; the server sends it as the single response,
and nil fails the call with `internal`. Every client-streaming `Response` is
frozen, with or without interceptors: the struct, its metadata and its
trailers.

No layer can turn a failed scope into success. A server layer that rescues a
handler exception and returns renders the continuation's exception, which is
an inner layer's translation when there is one; messages already sent stay
sent. On the client, an exception from the application block, a wrapper
method or a `wrap_stream` block reaches the caller with its identity unless an
interceptor translates it, and then the translation reaches the caller.
Interceptors see the application's exception even when transport cleanup
fails afterward. An abandoned scope, explicit or from an unconsumed stream,
reports `canceled` unless an interceptor exception escaped the outermost
interceptor; that exception then sets the outcome code. Delivery is
`:unobserved` when the chain failed before the innermost continuation ran,
`:completed` only when the exchange completed, and `:aborted` otherwise,
including a failure after dispatch started but before the stream opened.

### Metadata, diagnostics and cancellation

- Server response metadata is writable until headers are published by
  `send_headers`, the first message or the terminal. Later writes raise
  `FrozenError`, so add headers before `yield` or from a wrapper before the
  first `send_message`. Trailers stay writable until the terminal, so trailers
  written after `yield`, including after a handler error, reach it.
- A handler error's `metadata` joins the head while the head is unpublished.
  Once the head is published, the terminal carries it instead: Connect
  end-stream metadata, or gRPC and gRPC-Web trailers. There it follows the
  context trailers and precedes the error's own `trailers`, as in connect-go
  1.21.1. Each value appears in exactly one place. In the terminal it counts
  toward the terminal byte limits, so large error metadata after the head is
  published can turn the terminal into the bounded `resource_exhausted`
  replacement.
- Client server-streaming and bidi interceptors read response `headers` and
  `trailers` through the endpoint. A client-streaming interceptor reads them
  from the `Response` that `yield` returns.
- A session operation failure that the application or a wrapper catches, such
  as a rejected `send_message`, is recorded when it happens and can sit on an
  `OK` outcome, as it does without interceptors. Only a failure that escapes a
  continuation is guaranteed to fail the call.
- Interceptors run inside the instrumentation activation, and core records the
  exception escaping each continuation, so observers keep a handler's or
  application's original exception after an interceptor translates it.
- Cancellation follows the unary rules. The client commits once after the
  outermost interceptor; an accepted reason replaces a core-owned failure,
  while an exception an interceptor raises, including a translation, keeps its
  identity and reports its own code. Error-mapping interceptors should check
  `call.cancellation.cancelled?` before translating. On the server,
  `context.cancelled?`, `check!` and `on_cancel` work inside interceptors, and
  an accepted reason is the terminal error. Core cannot interrupt an
  interceptor; one that blocks should observe cancellation itself, and its
  frame lasts as long as a long-lived stream such as a health Watch.

## Bounds and protocol

Each direction retains at most one queued 16,384-byte output fragment and one
writer-owned in-flight fragment. One current encoded and decoded message,
5-byte prefix and at most one 16 KiB input remainder are separate allocations.
Native ingress retains at most one stream receive window, with chunks at most
16,384 bytes: by default 65,536 bytes in at most five entries on a server and
4 MiB on an Async HTTP/2 client; see
[flow-control windows](../gems/connectrpc-async/README.md#flow-control-windows). Stream credit follows consumption/discard; connection credit
allows healthy siblings/control traffic to advance. These are payload accounting
bounds, not RSS, CPU, HPACK, socket/TLS, caller retention or native codec guarantees.

`request_limit`, `response_limit` and their independent wire limits apply per
message (default 4 MiB). `terminal_wire_limit` and `terminal_limit` independently
default to 65,536 bytes, excluding prefix; JSON nesting is limited to 64. Lengths
are checked before payload allocation. Oversized terminals become a bounded
`resource_exhausted` without details/trailers, or reset if the minimal error cannot
fit. Observation retains original and actual replacement failures separately.

Flags 0/1 carry data, flag 2 an uncompressed response terminal. Compressed terminal
3 is accepted only with negotiated gzip. Request terminal and reserved bits are
`invalid_argument` before compression checks. The chosen pinned-reference policy
maps request DATA flag 1 under identity/absent coding to `internal`. Other malformed
request framing/truncation/gzip is `invalid_argument`; malformed responses are
`internal`. Bounds remain `resource_exhausted`. Per-message gzip state is independent.

Bounded tolerant terminal input accepts `error: null`, and maps absent/null/unknown
string codes to `unknown`, preserving valid message/details. Terminals and unary
error bodies share one normalization that follows connect-go 1.21.1: a null
message reads as empty, null details as none, an absent or null detail value as
empty bytes, and a detail type URL keeps the name after its last `/`. Numeric
codes, non-string messages, nonobjects, non-object details, invalid base64 and
metadata containers fail. Unlike connect-go, a detail type that is missing, null,
empty or not a valid protobuf name after that step also fails the whole error,
because `ErrorDetail` requires a valid type name. A failed terminal is
`internal`; a failed unary body uses the HTTP status fallback. Invalid individual
metadata fields are omitted. Output always uses valid Connect terminal errors.
A terminal is only a candidate until its HTTP tail and transport cleanup succeed.

## Custom transport SPI

Unary `_Transport#call`, `TransportRequest`, `TransportResponse` and `Client#call`
remain compatible. Streaming requires explicit `streaming_capabilities` and
`open_stream(StreamRequest) { |exchange| ... }`. The exchange is writable after
request-head dispatch **before** waiting for response headers. It provides
`response_head(timeout: nil)`, bounded `read(n, timeout: nil)`, `write(bytes)`,
`close_send`, `finish(timeout: nil)` and `abort`. Timeouts cap each requested wait;
the core retains the absolute budget across calls. Core calls an exchange's
`close_send` at most once, so transports need not make it idempotent. `finish`
attests owned transport completion and cleanup, never peer receipt. See
[RBS](rbs.md) for the typed SPI, records, normal package discovery and explicit
framework compatibility bridges.

Explicit gRPC-Web server streaming uses the same declarations/session ownership
on native Async H2 and Falcon H1/H2, with a staged body terminal. Browser
incrementality uses text; binary XHR buffers. Portable adapters remain unary.
See the [Web guide](grpc-web.md) for its capability SPI and identity-only bounds.

## Explicit HTTP/1.1 client

Use `ConnectRPC::Async::HTTP1Transport` from the existing Async package through
the core or generated client's `transport:` option. It advertises only SS/CS;
core rejects bidi and gRPC before encoding or I/O. Construction and calls stay
on the application's owning reactor/thread. No hidden thread is created.

The default four exclusive connections include pending setup and retirement.
Four long streams can occupy every slot. Waiters count toward `max_requests:
128`; use deadlines/cancellation or configure more connections to avoid
starvation. There is no fairness promise, pipelining, retry or protocol fallback.
A stream's request head is flushed before yielding the writable exchange, and
response reads progress independently of a blocked upload. A write-only caller
without a deadline can still block if a peer neither consumes nor closes.

Writer success, full native HTTP EOF and owned cleanup all precede successful
`finish`. `close_send` is idempotent and never reports writer failure. A writer
failure surfaced through `write` permits core's bounded terminal-only remote
error recovery, but always retires the connection. If failure first appears
after the last accepted write, `finish` returns unavailable even when the
terminal contains a remote error. This existing-SPI limit can mask that remote
error; the transport does not decode terminals or invent early success.
Unhandled send failures still win over remote success under core's policy.

The supplied completion timeout and original RPC deadline also cover cleanup;
cleanup has at most its existing 100 ms allowance. Destructive retirement closes
raw TCP before canceling/joining writers/readers and buffered/TLS cleanup. Abort
after successful finish is harmless; abandonment otherwise retires the lease.
TLS offers only `http/1.1` and accepts absent ALPN as legacy H1, with peer and
hostname verification enabled. Unary close-delimited responses accept clean EOF;
without Content-Length, EOF cannot detect a semantically truncated payload that
is otherwise valid. Core still validates protobuf/JSON; unexpected TLS EOF
without close_notify remains a transport failure.
