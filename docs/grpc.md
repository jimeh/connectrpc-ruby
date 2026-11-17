# Native gRPC

The [supported 0.x contract](support.md) defines the target compatibility
boundary and the remaining adoption gates.

## Quick start

This serves and calls the `Greet::V1::GreetService` from the
[README quick start](../README.md#quick-start) over native gRPC. Generate
`lib/greet/v1/greet_pb.rb` and `lib/greet/v1/greet_connect.rb` as step 3 there
shows. The Gemfile needs these gems:

```ruby
gem "connectrpc"
gem "connectrpc-async"
gem "google-protobuf", ">= 4.36.2", "< 5"
```

Give `connectrpc` and `connectrpc-async` the same version requirement, as the
[package policy](../README.md#packages) requires.

Save the server as `server.rb` and run `bundle exec ruby server.rb`. It serves
Connect and native gRPC on one HTTP/2 port, h2c here, inside a reactor the
application owns. On Ctrl-C, `ensure` closes the host gracefully and Ruby then
reports the `Interrupt`:

```ruby
$LOAD_PATH.unshift(File.expand_path("lib", __dir__))
require "async"
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "greet/v1/greet_connect"

service = Greet::V1::GreetService
greet = lambda do |request, _context|
  Greet::V1::GreetResponse.new(greeting: "Hello, #{request.name}!")
end
server = ConnectRPC::Server.new(
  services: [service::SERVICE],
  binding: ConnectRPC::Bindings::GoogleProtobuf.new,
  handlers: service.handlers(greet: greet),
  protocol: :auto
)

Sync do
  host = ConnectRPC::Async::Host.new(server: server, port: 8080).start
  puts "Serving Connect and gRPC on http://127.0.0.1:#{host.port}"
  host.wait # Returns once host.close stops the listeners.
ensure
  host&.close(grace: 5)
end
```

Save the client as `client.rb` and run `bundle exec ruby client.rb` from another
terminal. `ThreadedTransport` lets a plain thread, such as a Puma or Sidekiq
worker, call without a reactor:

```ruby
$LOAD_PATH.unshift(File.expand_path("lib", __dir__))
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
require "greet/v1/greet_connect"

transport = ConnectRPC::Async::ThreadedTransport.new(origin: "http://127.0.0.1:8080")
core = ConnectRPC::Client.new(
  base_url: "http://127.0.0.1:8080",
  binding: ConnectRPC::Bindings::GoogleProtobuf.new,
  transport: transport,
  protocol: :grpc
)
client = Greet::V1::GreetService::Client.new(client: core)
begin
  response = client.greet(Greet::V1::GreetRequest.new(name: "gRPC"), timeout: 2)
  puts response.message.greeting # => Hello, gRPC!
ensure
  transport.close
end
```

Code that already runs in a reactor task can use `ConnectRPC::Async::Transport`
with the same options instead. Other gRPC clients, such as grpc-go, can call
the same server. The [Async guide](../gems/connectrpc-async/README.md) covers
TLS, mTLS, keepalive and connection management.

## Protocol and configuration

Configure `Client(protocol: :grpc)` with an Async transport and a protobuf
binding. Unary, server streaming, client streaming and bidi use the existing
method declarations, generated clients and scoped stream endpoints. Configure
`Server(protocol: :grpc)` for gRPC alone, or `protocol: :auto` for exactly Connect
and gRPC. Async and Falcon HTTP/2 hosts share the native execution engine.
h2c, verified TLS and mTLS use the existing backend configuration.

The default remains Connect. gRPC accepts `application/grpc` and
`application/grpc+proto`, and emits the latter. It supports protobuf, identity and
opt-in per-message gzip. JSON and GET are rejected eagerly. NetHTTP, Rack, Rails,
Testing and Falcon HTTP/1 reject gRPC before message encoding or handler input.
Native handlers are plain handlers; native execution provides no Rails callback
or authentication execution contract.

Enabling gRPC requires a capable status codec at construction. A binding may
implement `_GRPCStatusCodec`, or callers can pass a separate
`status_codec:` with `encode_status(Error) -> String` and
`decode_status(String, code: Integer) -> Error`. The optional Google binding
provides it using canonical Any and Status descriptors in a private pool.
Opaque detail types and bytes survive without registering their descriptors.
Core has no protobuf runtime dependency.

`grpc_status_wire_limit` bounds the complete terminal field block, counting
name bytes, value bytes and 32 bytes per occurrence. `grpc_status_limit` bounds
decoded rich status bytes before protobuf parsing. Both default to 65,536.
Base64 expansion means a decoded status may exceed the independent wire limit.
Native gRPC always limits each complete decoded HEADERS block to 65,536 bytes,
including custom and informational fields, even with larger core configuration.
These are retention bounds applied before application copies, excluding HPACK,
CONTINUATION, parser temporary allocation, CPU and RSS. Message and independent
wire limits remain 4 MiB per message. Ingress and output bounds are unchanged.

Unary and server-streaming handlers receive exactly one request message, then
end of input. A second request envelope fails with `unimplemented` once its
5-byte header passes the normal envelope checks, before its payload is read,
inflated or decoded. An oversized second envelope still fails with
`resource_exhausted`. A client that receives a non-gRPC response content type
reads a body of up to 262,144 bytes, within the call's normal waits, to reach
the trailers. `grpc-status` in the head or trailers maps to `internal`;
otherwise the HTTP status fallback applies. For a larger body, reading stops at
the first byte past that limit, the error is classified from the head alone and
the normal call cleanup abandons the stream. During send-I/O terminal recovery,
its separate budget, described below, applies instead.

Clients require actual final HTTP/2 HEADERS and stream EOF. Trailers-only means
initial final-response HEADERS with END_STREAM. Ordinary initial grpc-status is
not terminal. Server terminal fields are staged behind processor completion;
late application cleanup failure prevents successful delivery. Success includes
transport-owned cleanup, never peer receipt or TLS shutdown.

Status accepts a nonempty ASCII digit sequence with leading zeros, bounded by
the field block budget. Numeric values above uint32 map to unknown; there is no
10-character spelling limit or unbounded integer parse. Output is canonical.
Rich status codes compare numerically before unknown-code normalization, and
rich messages take precedence over grpc-message. Invalid percent escapes remain
literal; decoded invalid UTF8 is scrubbed. Details on OK, malformed or
duplicate details, and mismatched rich codes produce safe internal instead of
silently dropping rich data. grpc-go 1.85.0 also rejects a mismatch and invalid
base64, but ignores details that repeat or do not parse as a `google.rpc.Status`,
and accepts details on OK when their code is also OK. The
[support contract](support.md#native-grpc-rich-status) classifies this strict
policy as provisional.

Missing server timeout means no logical expiry. gRPC timeout accepts all six
case-sensitive units and zero as immediately expired. Clients default to 30
seconds; nil omits the timeout and logical watchdog. Dispatch recomputes remaining
budget without extending monotonic expiry. gRPC per-operation waits have a
9,999,999.999-second ceiling, further shortened by real deadlines. Connect Async
nil waits retain their existing behavior. There is no one-second gRPC tail timer.
Send-I/O terminal recovery has one absolute 100ms budget and allows at most
262,144 additional body bytes, with separate finite header limits. It skips
messages without decoding or inflating them and cannot take over an active
reader. Only a validated remote error can replace an unhandled send failure;
remote success cannot. The existing destructive cleanup has a separate 100ms
budget. A stuck frame can fail its connection and siblings during that cleanup.

Only actual peer CANCEL resets signal server Context cancellation. Half-close,
EOF, other resets, connection loss and shutdown do not. No retry or replay occurs.
Client reset mapping follows grpc-go 1.85.0: FLOW_CONTROL_ERROR and
ENHANCE_YOUR_CALM map to resource_exhausted, REFUSED_STREAM to unavailable,
CANCEL to canceled, and INADEQUATE_SECURITY to permission_denied. Other resets
map to internal. The FLOW_CONTROL_ERROR choice differs from the older protocol
document's mapping. NO_ERROR can preserve queued data and a valid final status;
it cannot establish success without that status and owned transport completion.
Native unary executes the configured invoker with a normal Context, including
custom invokers that have no handler-map entry. As on every server Context,
`context.request_metadata` is mutable per-call metadata; writes do not change
what instrumentation recorded for the request.

## Interceptors

[Unary interceptors](unary.md#interceptors) cover native gRPC unary calls on
both roles, with `call.protocol == :grpc`.
[Stream interceptors](streaming.md#interceptors) cover native gRPC server,
client and bidi streaming the same way.

On the client, the chain runs inside the call's deadline and cancellation scope
and before the stream opens. Request metadata is final when the innermost
continuation opens the stream, and a cancellation accepted before then fails
the call with `canceled` without opening a stream. The returned `Response`, its
metadata and its trailers are frozen, with or without interceptors. Delivery
comes from the exchange, not from the code that raised: a short-circuit or a
rejection before `yield` reports `:unobserved`; an interceptor exception after a
completed exchange, or after a remote error the peer delivered, reports
`:completed`; an interceptor that translates a transport or cleanup failure
leaves it `:aborted`.

On the server, interceptors run after the single request message is read and
decoded, around the configured invoker. The head is published after the chain
returns, so response metadata written before or after `yield` reaches the
client, and trailers stay writable until the terminal status.
