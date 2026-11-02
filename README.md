# connectrpc-ruby

A private, unofficial, experimental Ruby implementation of typed
[Connect](https://connectrpc.com/docs/protocol/) unary RPCs. It provides a
configurable client, a Net::HTTP transport, and Rack middleware for reusable
Ruby handlers. It is not production ready.

The implemented profile is POST, identity encoding, and `application/proto` or
protobuf `application/json`. Streaming, GET, compression, gRPC, gRPC-Web, Rails,
Faraday, a production server, and code generation are outside this slice.
Unsupported profiles and streaming declarations fail explicitly.

## Try the localhost example

Install [Mise](https://mise.jdx.dev/) 2026.9.14 or newer and the
[Ruby source-build prerequisites](https://github.com/rbenv/ruby-build/wiki#suggested-build-environment),
then run:

```sh
mise run setup
mise run example
mise run verify
```

[The example](examples/unary.rb) starts Rack on an ephemeral loopback port,
calls a typed handler through real Net::HTTP in both codecs, prints the results,
and closes the host and its workers. Its schema comes from the shared
[descriptor fixture](spec/fixtures/echo.proto). Go is needed only for interop
and final verification, and those tasks install its project pin separately.

## Declare a unary method

Core `require "connectrpc"` needs no Google or Rack gems. Load optional
integrations explicitly. Development uses exact pins; the integration entry
points support google-protobuf `>= 4.36.2, < 5` and Rack `>= 3.2.7, < 4`.
Applications that use them must supply those gems themselves.

```ruby
require "connectrpc/bindings/google_protobuf"
require "connectrpc/rack"
require "google/protobuf/wrappers_pb"

type = Google::Protobuf::StringValue
method = ConnectRPC::Method.new(
  service: "example.v1.EchoService", name: "Echo",
  request_type: type, response_type: type
)
service = ConnectRPC::Service.new(name: method.service, methods: [method])
binding = ConnectRPC::Bindings::GoogleProtobuf.new
handler = lambda do |request, context|
  context.check!
  context.response_metadata.add("x-worker", "ruby")
  context.response_trailers.add("x-finished", "yes")
  type.new(value: "Hello, #{request.value}!")
end
server = ConnectRPC::Server.new(
  services: [service], binding: binding, handlers: { method.path => handler }
)
app = ConnectRPC::Rack.new(server: server) # Supply this to your Rack host.
```

`ConnectRPC::Rack.new(downstream_app, server: server)` falls through for unrelated
services. An unknown method within a registered service returns `unimplemented`.
Rack routes `PATH_INFO`, so mounting under a prefix works. Handlers receive a
typed request and a fresh `Context` for each call. A custom
`invoker: ->(method, request, context) { ... }` can own execution instead.
Reusable handlers must synchronize any application state they share.

With that app hosted at a base URL, a client call is:

```ruby
client = ConnectRPC::Client.new(
  base_url: "http://127.0.0.1:8080", binding: binding, codec: :json
)
response = client.call(
  method, type.new(value: "Ruby"), metadata: { "x-request" => ["example"] }, timeout: 2
)
puts response.message.value
p response.metadata.to_h
p response.trailers.to_h
```

The default codec is `:proto`; `:json` uses the native protobuf JSON mapping,
including int64 strings, enum names, presence, and well-known types. Decoding
ignores unknown JSON fields. The Google binding handles scalar wrapper roots
through the native Any decoder because protobuf 4.36 cannot parse those roots
directly. This does not change Connect error details: each `ErrorDetail` has a
protobuf type name and binary bytes, with a base64 representation on the wire.

Bindings implement `encode(message, type:, codec:)` and
`decode(bytes, type:, codec:)`, raising `ConnectRPC::CodecError` for codec
failures. Transports implement `call(TransportRequest)` and return a
`TransportResponse`. Their fields and responsibilities are documented in
[design](docs/design.md). Neither seam requires Google or Rack types.

## Metadata and failures

Metadata keys are normalized to lower case and values are arrays of printable
ASCII header strings. Protocol and framing keys are reserved. Use
`metadata.add_binary("key-bin", bytes)` and `metadata.binary("key-bin")` for
binary values. Writers emit unpadded base64; readers accept valid padded and
unpadded base64. Only binary helpers split coalesced comma-separated values.
Other metadata values keep their received boundaries.
Incoming HTTP fields whose names or values cannot be represented as Connect
metadata are omitted, including the entire field if any repeated value is
invalid. Valid adjacent fields remain available in leading and trailing
metadata. Outbound names and values are always validated strictly.

Rack responses preserve arrays. Net::HTTP preserves repeated raw response
fields, but joins outgoing request arrays with `", "`. rackup/WEBrick also
coalesces incoming and response fields. For example, `['a', 'b']` may arrive as
`['a, b']`. Interop asserts these observed values; original outbound field
boundaries are not preserved.

Raise a deliberate `ConnectRPC::Error.new(:invalid_argument, "safe message",
details: [...], metadata: {...}, trailers: {...})` from a handler. Unexpected
application exceptions become `internal` with `"Internal error"`. Clients raise
`ConnectRPC::Error` with `code`, `details`, `metadata`, and `trailers`. Decode a
detail with `detail.decode(binding: binding, type: message_class)` after choosing
the class corresponding to `detail.type`.

## Limits and deadlines

Request and response byte limits default to 4 MiB each. Set `request_limit:` and
`response_limit:` on either `Client` or `Server` to positive integers. Inputs
are bounded before decode, including unknown-length Rack input and HTTP error
bodies. Serialization can allocate the whole message before checking its size.
If even a minimal error exceeds the response limit, the server sends an empty
body with status 429; clients then apply the protocol's HTTP fallback mapping.
Header size limits belong to the HTTP library and host.

Client `timeout:` accepts finite positive seconds, defaulting to 30. Explicit
`timeout: nil` sends no timeout header and creates no logical deadline or expiry
watchdog. Native TCP/TLS/read/write waits remain independently capped at
`9_999_999.999` seconds; a native timeout returns `unavailable`. Sustained progress
can outlast that per-operation cap. For finite values, one monotonic
budget starts before encoding and covers network I/O and checks around codec
work. NetHTTP uses one connection per call, closes it on every outcome, sends
`Accept-Encoding: identity`, ignores proxy environment variables, disables
retries and decompression, preserves TLS peer/hostname verification, and does
not follow redirects. A transport-owned watchdog closes its socket at expiry,
including during response-header trickle and blocked writes. Native DNS
resolution may outlast the budget; expiry is checked when control returns.
Codec work and request preparation are synchronous and checked before/after,
not interrupted. Dispatch rechecks expiry and refreshes the wire timeout after
connection setup; a closed socket cannot trigger an implicit reconnect. Native
operations are independently capped at `9_999_999.999` seconds, the Connect
wire maximum, to avoid platform overflow for very large budgets. A native
operation timeout before logical expiry returns `unavailable`; actual expiry
returns `deadline_exceeded`. The logical deadline stays unchanged, and the
watchdog rechecks it after each capped wait.

Server contexts expose `deadline`, `cancelled?`, and `check!`. With no inbound
timeout, there is no server expiry. Cancellation means deadline expiry in this
slice. Checks surround input, decode, and invocation; handlers must cooperate.
Synchronous Rack cannot interrupt blocked reads or arbitrary handlers, or
detect every disconnected peer. See [design](docs/design.md) for these limits.

CRuby >= 3.4 is the baseline; project verification targets source-built Ruby
3.4.12 and 4.0.9 on Linux x64. See [development](docs/development.md) for the
commands, dependency policy, isolated installation and evidence scope. The
interop fixture runs 18 cases: six Go-to-Go reference baseline cases and twelve
Ruby-to-Go/Go-to-Ruby cases. Both codecs share typed success, deliberate error
and request-limit scenarios. This covers the advertised unary subset, not full
conformance.

The pinned [official conformance harness](conformance/README.md) independently
executes 94 client and 88 server cases for the declared profile. Two client
caller-cancellation instructions currently fail because there is no cancellation
API; both still execute real RPCs and are required to fail for the exact
documented reason. Results are 92 client passes plus two expected failures, and
88 server passes. This is a bounded profile, not full Connect conformance.

## Private status and licensing

Licensing is undecided. `Nonstandard` gem metadata grants no new license.
The gem's `allowed_push_host` is `https://gems.invalid`, a reserved invalid
domain, to guard against accidental public RubyGems pushes. It is not a
publication destination or a complete access-control mechanism. Do not publish
this gem without an explicit licensing and publication decision.
