# connectrpc-ruby

An unofficial, experimental Ruby implementation of typed
[Connect](https://connectrpc.com/docs/protocol/) unary and native HTTP/2 streaming RPCs. It provides a
configurable client, a Net::HTTP transport, and host-neutral servers for reusable
Ruby handlers. The optional [connectrpc-rack gem](gems/connectrpc-rack/README.md)
provides Rack middleware. It is not production ready.

The implemented profile is POST and opt-in safe unary GET, identity or opt-in gzip encoding, and `application/proto` or
protobuf `application/json`. The optional [connectrpc-rails gem](gems/connectrpc-rails/README.md)
runs generated unary handlers through native Rails controllers. Native streaming
uses the optional Async transport and Async/Falcon HTTP/2 hosts. See the
[streaming guide](docs/streaming.md), especially its one-second HTTP tail policy.
Native [gRPC](docs/grpc.md) supports unary, server streaming, client streaming and
bidi through the same core declarations and Async/Falcon HTTP/2 engine, with
protobuf, identity/gzip, h2c, verified TLS and mTLS. Connect remains the default.
gRPC-Web and Faraday remain unsupported. The API is experimental.

## Try the localhost example

Install [Mise](https://mise.jdx.dev/) 2026.9.14 or newer and the
[Ruby source-build prerequisites](https://github.com/rbenv/ruby-build/wiki#suggested-build-environment),
then run:

```sh
mise run setup
mise run example
mise run verify
mise run rack:verify
```

[The example](examples/unary.rb) starts Rack on an ephemeral loopback port,
calls a typed handler through real Net::HTTP in both codecs, prints the results,
and closes the host and its workers. Its schema comes from the shared
[descriptor fixture](spec/fixtures/echo.proto). Go is needed only for interop
and final verification, and those tasks install its project pin separately.

## Generate services from protobuf

`protoc-gen-connectrpc-ruby` generates mixed unary/streaming declarations, thin clients
and handler maps. Supply google-protobuf `>= 4.36.2, < 5` for the plugin and
Google message integration. The core gem still has zero runtime dependencies.
Run the complete `.proto` -> generated messages/service -> ephemeral localhost
example with:

```sh
mise run example:generated
```

It compiles [greeting.proto](examples/proto/greeting.proto) with protoc 36.2,
then [calls the generated client](examples/generated.rb) in binary and JSON,
including typed deliberate error details. To use the installed gem's plugin
in your own project, with `protoc-gen-connectrpc-ruby` on PATH:

```sh
mkdir -p generated
protoc -I examples/proto --ruby_out=generated --connectrpc-ruby_out=generated greeting.proto
```

This creates official `greeting_pb.rb` and `greeting_connect.rb`. Add `generated`
to the Ruby load path. The service API is:

```ruby
require "greeting_connect"
require "connectrpc/bindings/google_protobuf"

service = Greeting::V1::GreetingService
binding = ConnectRPC::Bindings::GoogleProtobuf.new
core = ConnectRPC::Client.new(base_url: "http://127.0.0.1:8080", binding: binding, codec: :json)
client = service::Client.new(client: core)
response = client.greet(Greeting::V1::Greeting.new(name: "Ruby"), timeout: 2)

# Supply application callables accepting (request, context).
handlers = service.handlers(greet: greet_handler, fail: fail_handler)
server = ConnectRPC::Server.new(services: [service::SERVICE], handlers: handlers, binding: binding)
```

`greet_handler` and `fail_handler` above are application-defined callables; the
[runnable example](examples/generated.rb) defines both and hosts the Rack app.
`GREET` and `FAIL` are frozen `ConnectRPC::Method` constants; `SERVICE` is reusable.
Wrapper calls return the existing `Response` and forward keyword options exactly,
including omission of `timeout:` versus explicit `timeout: nil`. Binding, transport,
codec, URL and limits belong to the configured core client. `handlers` requires
all declared RPC keywords and validates callability; use the existing Server
handler map directly for partial/custom registration.

Buf 1.75.0 local generation is tested alongside official message generation:

```yaml
# buf.gen.yaml, for a Buf module containing greeting.proto
version: v2
plugins:
  - protoc_builtin: ruby
    out: generated
  - local: protoc-gen-connectrpc-ruby
    out: generated
```

Run `buf generate` in that module with pinned protoc on PATH. For this checkout,
`mise run generation:check` runs both actual tools with explicit local paths,
compares glue bytes and exercises both generated message trees.

Proto2 and proto3, including optional presence, are supported. Editions,
streaming targets and unsupported plugin parameters are rejected. Default output is
`dir/foo_connect.rb` for service-bearing `dir/foo.proto`; message generation
remains official `--ruby_out`. Set `rbs=true` to also emit `dir/foo_connect.rbs`.
The gem ships optional core signatures; the [RBS guide](docs/rbs.md) covers
official message `--rbs_out`, explicit protobuf bridge selection and strict Steep
configuration. Ruby module
placement follows official package/`ruby_package` naming; RPC methods use the
gRPC Ruby underscore rule (`GetHTTP` becomes `get_http`). Leading-underscore
RPCs, object hooks, implicit conversion methods, reserved runtime names and
collisions visible in the request fail with diagnostics. Compatible `google.*`
and `Google::...` Ruby package nesting is supported, including WKT imports;
replacing `Google`, `ConnectRPC` or concrete protobuf runtime constants is rejected.
Cross-invocation namespace conflicts cannot all be discovered; generated code
also guards its directly defined constants at load time. See [design](docs/design.md).

## Use unary GET

Declare safe queries with the standard protobuf method option:

```proto
rpc Read(Request) returns (Reply) {
  option idempotency_level = NO_SIDE_EFFECTS;
}
```

The generator maps `NO_SIDE_EFFECTS` to `idempotency: :no_side_effects`,
`IDEMPOTENT` to `:idempotent`, and an absent/unknown level to `:unknown`.
Manual `Method.new` declarations accept the same three symbols.
Enable GET on the core client; generated wrappers keep their existing API:

```ruby
core = ConnectRPC::Client.new(base_url: "https://api.example.com", binding: binding,
                             use_get: true, get_url_limit: 8192)
```

Only `:no_side_effects` methods select GET. The default remains POST.
The URL cap counts the complete absolute URI in bytes, including the base
prefix and escaped query. Excess raises `resource_exhausted` before dispatch;
there is no POST fallback, retry or replay. Binary and gzip payloads use
unpadded URL-safe base64; identity JSON uses UTF-8 form encoding.

Servers admit GET automatically for declared safe methods. Native hosts and
Testing forward the raw query; direct `Server.call` callers pass `query: "..."`
separately from `path:`. `Server.new(get_query_limit: nil)` derives its raw-query
cap as `3 * (4 * ((request_wire_limit + 2) / 3)) + 1024` after validating the
wire limit. An explicit cap must be a positive integer. Serialized/decompressed
message bytes, decoded query payload bytes before inflation, and raw query/URL
bytes have independent limits. Hosts have already allocated the incoming URL;
these caps are not a total memory guarantee.

GET URLs can appear in proxy/server logs, browser history and generic HTTP
instrumentation. Choose safe request contents and application cache/security
policy accordingly. All GET responses vary on `Accept-Encoding`, preserving
application Vary tokens and `*`. The library never adds public Cache-Control.
Authentication, cookies, sessions and CSRF remain application/framework policy.
Official Ruby protobuf serialization is repeatable for the same exercised
object within a process, including gzip. It has no deterministic flag or
cross-process canonical cache-key guarantee, including for maps.

## Declare a unary method

Core `require "connectrpc"` needs no Google or Rack gems. Load optional
integrations explicitly. Development uses exact pins; the integration entry
points support google-protobuf `>= 4.36.2, < 5` and Rack `>= 3.2.7, < 4`.
Applications using Google must supply `google-protobuf`. Rack middleware ships
in the separate `connectrpc-rack` gem, which installs compatible core and Rack
dependencies. Existing middleware users must add this Gemfile dependency:

```ruby
gem "connectrpc-rack", "~> 0.1.0"
```

Keep `require "connectrpc/rack"` and the middleware API unchanged. Core installed
alone has no middleware file, so requiring that path raises an ordinary
`LoadError`. This is an installation migration; Rack already loaded only on
explicit require.

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

## Unary gzip

Identity is the default. Configure `encoding: :gzip` on the client to compress
requests and advertise `gzip, identity` for responses. Configure it on the server
to accept gzip requests and negotiate gzip success responses in both codecs:

```ruby
client = ConnectRPC::Client.new(base_url: "http://127.0.0.1:8080", binding: binding,
                                encoding: :gzip)
server = ConnectRPC::Server.new(services: [service], binding: binding,
                                handlers: { method.path => handler }, encoding: :gzip)
```

Client `accept_encoding:` independently controls accepted responses. Its default
is derived from request encoding. An explicit nonempty array contains distinct
`:gzip`/`:identity` symbols; identity is always appended as the last fallback.
For example, `accept_encoding: [:gzip]` permits identity requests with gzip
responses; `encoding: :gzip, accept_encoding: [:identity]` permits gzip requests
with identity responses. The server selects a supported compression coding from
the ordered accept fields before identity. An absent field inherits the request
coding; an explicitly empty field selects identity. Unknown tokens and quality
values are ignored. Only one identity or gzip content coding is accepted.

Empty serialized messages selected for gzip produce valid nonempty gzip members.
Zero-length HTTP content is never inflated, after its coding header is validated.
Concatenated members are supported under one aggregate decoded limit; every
member's CRC, size and completion are validated. Truncation and trailing garbage
are rejected. Server errors remain identity; clients can decode compressed peer
errors, including typed details and metadata. Malformed compressed HTTP errors
use HTTP fallback, while malformed compressed success responses return internal.

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

Serialized/decompressed request and response limits default to 4 MiB each.
Set `request_limit:` and `response_limit:` on either endpoint to positive integers.
Separate `request_wire_limit:` and `response_wire_limit:` cap encoded HTTP bytes
and default to their corresponding message limits. Identity must satisfy both
caps. Gzip can expand small messages, including empty-message framing; explicitly
increase the wire cap if that expansion is desired. Compression never silently
falls back to identity after exceeding a cap. `TransportRequest.response_limit`
remains the wire read cap for custom transports.

Input is bounded before inflation and binding decode, including unknown-length
Rack input and HTTP errors. Native gzip output is checked before each append,
with at most one 16 KiB output callback beyond the retained limit. This is not a
total allocation bound: wire and decoded buffers coexist, and native serialization
can allocate the whole message before its size check. Server identity errors use
the smaller response cap and safe replacement; if the replacement cannot fit,
they send an empty 429 and clients apply HTTP fallback. Header size limits and
HTTP input coalescing belong to the library and host.

Client `timeout:` accepts finite positive seconds, defaulting to 30. Explicit
`timeout: nil` sends no timeout header and creates no logical deadline or expiry
watchdog. Native TCP/TLS/read/write waits remain independently capped at
`9_999_999.999` seconds; a native timeout returns `unavailable`. Sustained progress
can outlast that per-operation cap. For finite values, one monotonic
budget starts before encoding and covers network I/O and checks around codec
work. NetHTTP uses one connection per call, closes it on every outcome, sends
the configured `Accept-Encoding`, ignores proxy environment variables, disables
retries and decompression, preserves TLS peer/hostname verification, and does
not follow redirects. A transport-owned watchdog closes its socket at expiry,
including during response-header trickle and blocked writes. Native DNS
resolution may outlast the budget; expiry is checked when control returns.
Codec work and request preparation are synchronous and checked before/after;
gzip work also checks each native input/output chunk and member boundary. They are
not interrupted. Dispatch rechecks expiry and refreshes the wire timeout after
connection setup; a closed socket cannot trigger an implicit reconnect. Native
operations are independently capped at `9_999_999.999` seconds, the Connect
wire maximum, to avoid platform overflow for very large budgets. A native
operation timeout before logical expiry returns `unavailable`; actual expiry
returns `deadline_exceeded`. The logical deadline stays unchanged, and the
watchdog rechecks it after each capped wait.

Explicit unary cancellation uses a permanent, shareable token:

```ruby
source = ConnectRPC::CancellationSource.new
# Pass source.token to each call that should share cancellation.
response = client.call(method, message, timeout: nil, cancellation: source.token)
# Another thread or reactor task can call source.cancel while the call blocks.
```

`source.cancel` returns true only for its first transition. A token exposes
`cancelled?`, `reason` (`:canceled` or nil), `check!`, and
`on_cancel { ... }`. Registration returns an object with `unregister`, which
returns true only when it prevents an invocation that has not started. It never
waits for a running listener. Listeners run in the cancelling thread, must be
short and nonblocking, and isolate `StandardError`. Unregistering releases the
callback; completing a call releases its internal registrations. Sources cannot
be reset, and a call never completes or changes another call's shared token.

Each call accepts one terminal outcome. An already-expired deadline takes
precedence while the call is open, followed by observed explicit cancellation.
Accepted cancellation stays canceled through later expiry. A committed response
or error stays final after later source cancellation. Source transition time is
not acceptance time: a slow earlier listener may delay acceptance until expiry.
Final decoding/error commit checks the source directly, even if notification is
delayed. Cancellation never substitutes a timeout header for `timeout: nil`.

NetHTTP closes only its call-owned socket to interrupt exposed TLS, writes,
headers and body reads. Native DNS/TCP before socket exposure remains
cooperative; cancellation is checked on return before any later dispatch.
The optional Async transport wakes its owner reactor and resets its stream.
Healthy multiplexed siblings survive; a stuck frame retains the documented
connection-wide cleanup fallback. Neither backend retries or replays a call.

Server contexts expose `deadline`, `cancelled?`, `cancellation_reason`
(`:canceled`, `:deadline_exceeded`, or nil), and `check!`. Handlers cooperate at
application boundaries. Low-level `Server#call(cancellation:)` and the Testing
transport accept explicit cancellation. Native HTTP/2 publishes peer
`RST_STREAM(CANCEL)` before input teardown; normal END_STREAM/half-close, other
reset codes, connection loss and host shutdown do not manufacture explicit
cancellation. Rack/Rails and Falcon HTTP/1 add no peer detection or token env
injection. Synchronous adapters cannot interrupt blocked application code.
See [design](docs/design.md) for ownership and transport limits.

CRuby >= 3.4 is the baseline; project verification targets Ruby 3.4.12 and
4.0.9 on Linux x64. Local setup builds from source; CI uses official prebuilt
runtimes. See [development](docs/development.md) for the
commands, dependency policy, isolated installation and evidence scope. The
interop fixture runs 18 cases: six Go-to-Go reference baseline cases and twelve
Ruby-to-Go/Go-to-Ruby cases. Both codecs share typed success, deliberate error
and request-limit scenarios. This covers the advertised unary subset, not full
conformance. A separate gzip portion adds 40 cases with actual wire observations,
including asymmetric raw Go HTTP probes and documented reference-client quirks.

The pinned [official conformance harness](conformance/README.md) independently
executes 96 client and 90 server identity cases, including two safe GET cases
per direction. The disjoint gzip profile adds 88 client and 82 server cases.
All 184 client and 172 server cases must pass with zero expected failures.
The four caller-cancellation cases use their original 5 ms asynchronous timer,
200 ms response request and no RPC deadline. The ledger requires actual request
writes, source cancellation, owned socket cleanup and the library's canceled
result. This is a bounded profile, not full Connect conformance.

## Status and licensing

Original project work is licensed under the [MIT License](LICENSE), copyright
2026 Jim Myhrberg. The core, Rack, Rails, Async and testing gem archives each include the
complete license. Third-party dependencies and imported fixtures retain their
own licenses and notices, including the separately licensed
[conformance schemas](conformance/README.md).

The gems' `allowed_push_host` remains `https://gems.invalid`, a reserved invalid
domain, to guard against accidental public RubyGems pushes. It is not a
publication destination or a complete access-control mechanism. Project MIT
licensing is approved; publishing, releasing or deploying still requires
separate, explicit authorization.

## Optional standalone HTTP/2

The [connectrpc-async package](gems/connectrpc-async/README.md) provides explicit
`connectrpc/async` integration: `ConnectRPC::Async::Host` and a matching pooled
`Transport` for Connect unary protobuf/ProtoJSON over h2c and verified TLS `h2`.
It supports required client authentication, complete live TLS snapshot reloads
on the same listener, older-generation drain, and an authenticated client leaf
accessor scoped to the exact handler task. Applications own the Async reactor,
authorization and lifecycle. There is no HTTP/1 fallback, automatic replay or
streaming API. See the package guide for defaults, bounded cleanup and backend
header normalization. `mise run async:example` runs the generated localhost
example; `async:verify` checks the independent bundle, installed packages and
connect-go interoperability. Core require and core/Rails dependencies stay
independent.

See the optional [in-process testing package](gems/connectrpc-testing/README.md) for real encoded client/server round trips without sockets.

## Optional Falcon hosting

The [connectrpc-falcon package](gems/connectrpc-falcon/README.md) serves the native
unary application in real Falcon process workers, with explicit Rack/Rails
fallback, h2/HTTP1 TLS selection and worker-local versioned TLS providers.
RPC compression and byte limits stay outside generic Falcon middleware.
WebSockets, ActionCable, upgrades, CONNECT and hijack are rejected on this
listener. `mise run falcon:example` exercises the normal service DSL;
`falcon:verify` checks its independent bundle, protocols, workers, packages and
pinned Go interoperability. Core, Rails and Async bundles remain independent.

## RPC lifecycle instrumentation

Pass `instrumentation: ConnectRPC::Instrumentation.new(subscribers: [recorder])`
to a Client or Server for synchronous per-call observation. A subscriber's
`start(call)` returns an observer with optional `attach`, `detach(token)`,
`processing_finished(outcome)` and `finished(completion)` methods. Core owns the
application yield and isolates each StandardError from subscribers and the
optional `on_error:` reporter. Original exceptions are process-local trusted
subscriber data. Instrumentation is disabled by default and performs no lifecycle
allocation or instrumentation clock reads on that path.

The optional [connectrpc-otel package](gems/connectrpc-otel/README.md) turns this
lifecycle into real Connect CLIENT/SERVER traces using an application-owned
OpenTelemetry SDK. Core has no OpenTelemetry dependency. See
[design](docs/design.md#rpc-lifecycle-observation) for host ownership and
`mise run otel:example` for a runnable generated caller with real SDK exports.

## Native Connect streaming

Async and Falcon native HTTP/2 support server, client and bidirectional streaming.
See the [streaming guide](docs/streaming.md) for scoped APIs, mixed generated
services, bounds and the one-second HTTP tail completion policy.

Explicit server gRPC-Web support is documented in the [Web guide](docs/grpc-web.md),
including native H1/H2 streaming, portable unary, CORS and the optional official browser harness.
