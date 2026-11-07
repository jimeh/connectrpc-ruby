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
disables reuse and delivers the complete response before best-effort input
hygiene. Hygiene has one absolute 100 ms allowance and a 256 KiB entity-byte cap,
including the adapter remainder. Every additional framed read reserves a full
64 KiB, so retirement can stop below the cap. Progress never restarts the timer;
Connect does not use `grpc-timeout`.

A successfully delivered response on a retiring connection uses one absolute
final-close expiry at the actual final HTTP flush plus 500 ms. A private body EOF
marker anchors the pinned writer's next successful final flush before body-close
callbacks or lifecycle observation; payload bytes cannot trigger it. Delayed
callbacks consume the same total budget. After completed delivery, Falcon
half-closes raw TCP writes and performs bounded discard. When discard stops,
Falcon waits only the remaining part of that original 500 ms budget, without
reading more. It does not add another 500 ms after discard. Earlier owner
abort/shutdown can shorten either phase. Failed or incomplete delivery gets no
success grace. Late hygiene/grace failures preserve completed processing and
delivery facts.

Admission and socket ownership remain held until retirement and original
processing join. Retirement never parses or dispatches a pipelined request,
even if the original request finishes. Fully consumed nonretiring input retains
persistence. Raw socket abort precedes framed/Finishable close on forced teardown.
Both timers are cooperative and can overshoot under scheduler delay. Remaining
grace retains resources and is best effort reset avoidance, not a guarantee of
response delivery under arbitrary network or peer failure.

The production official H1 profiles pass 174/174 identity and 160/160 gzip cases
on both pinned Rubies, including the original oversized CS cases over TCP/TLS.
No official assertion or inventory changed. This is a bounded, response-first variant of
Go's conditional reset avoidance: connect-go may consume the full oversized
declared message, and net/http may drain before flushing or reuse a connection.
Literal Go parity is not claimed. Native H1 Web retains its separate policy:
original input cleanup gates the staged Web terminal, and an incomplete upload
also gates a rejection head to avoid stopping the client's upload. This Web-only
gate never delays Connect response delivery. The historical Web TCP/TLS failures,
diagnosis and separate baseline repair are recorded in the
[conformance status](../conformance/README.md#connect-http11-server-profiles).

## Completion policy

After a terminal envelope, the client requires HTTP EOF and transport completion
within one **absolute second**, with at most one extra byte read. Any extra byte
is a protocol error. A stalled tail is `unavailable`, including with `timeout: nil`.
A shorter real RPC deadline or explicit cancellation can win first. Otherwise
valid responses with late EOF or slow intermediaries can be rejected. This is
this library's interoperability policy, not a Connect requirement, RPC deadline,
wire timeout header or general stream watchdog.

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
timeouts and cancellation tokens. The same configured `Client(use_get: true)`
uses GET for eligible unary calls and POST for streaming, without mode changes,
retry, fallback or resend. Legacy `mode: :unary` remains accepted.

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
violations are `unimplemented`.

`context.send_headers(metadata: {})`, also delegated by response writers and
server duplexes, publishes once. It can precede a handler delay or CS input.
First payload/terminal implicitly publishes. Leading metadata is deeply frozen
at publication; trailers remain mutable until terminal serialization, then freeze.
There is no synthetic nil message for publishing headers.

The original processor task owns authenticated peer identity, cancellation and
instrumentation through handlers, codecs and cleanup. A separate publication
promise exposes response headers while processing continues. Body tasks consume
prepared bytes. Successful HTTP EOF waits for original processor completion;
late cleanup failure aborts/reset. Admission and graceful drain include processing,
wire tail and cleanup. Only actual peer RST_STREAM(CANCEL) signals explicit server
Context cancellation. EOF, half-close, other reset codes and host shutdown do not.

## Bounds and protocol

Each direction retains at most one queued 16,384-byte output fragment and one
writer-owned in-flight fragment. One current encoded and decoded message,
5-byte prefix and at most one 16 KiB input remainder are separate allocations.
Native ingress retains at most 65,536 bytes in at most five entries, with chunks
at most 16,384 bytes. Stream credit follows consumption/discard; connection credit
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
string codes to `unknown`, preserving valid message/details. Numeric codes,
nonobjects, malformed details and metadata containers fail. Invalid individual
metadata fields are omitted. Output always uses valid Connect terminal errors.
A terminal is only a candidate until its HTTP tail and transport cleanup succeed.

## Custom transport SPI

Unary `_Transport#call`, `TransportRequest`, `TransportResponse` and `Client#call`
remain compatible. Streaming requires explicit `streaming_capabilities` and
`open_stream(StreamRequest) { |exchange| ... }`. The exchange is writable after
request-head dispatch **before** waiting for response headers. It provides
`response_head(timeout: nil)`, bounded `read(n, timeout: nil)`, `write(bytes)`,
`close_send`, `finish(timeout: nil)` and `abort`. Timeouts cap each requested wait;
the core retains the absolute budget across calls. `finish` attests owned transport
completion and cleanup, never peer receipt. See [RBS](rbs.md) for the typed SPI,
records, normal package discovery and explicit framework compatibility bridges.

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
