# Unary RPC guide

This guide covers unary RPCs with the core `connectrpc` gem: generating
services from protobuf, declaring methods by hand, hosting and calling them,
and the GET, compression, metadata, error, limit, deadline, cancellation and
instrumentation behavior they share. For a first working service, start with
the [quick start](../README.md#quick-start).

Related guides cover the other RPC shapes and protocols:
[streaming](streaming.md), [native gRPC](grpc.md) and [gRPC-Web](grpc-web.md).
[Design](design.md) documents the internal boundaries, and the
[supported 0.x contract](support.md) defines the target compatibility profile.

## Install the core gem and its integrations

Core `require "connectrpc"` needs no Google or Rack gems. Load optional
integrations explicitly. Development pins google-protobuf 4.36.2 and tests Rack
at its newest allowed release and its floor; the integration entry points
support google-protobuf `>= 4.36.2, < 5` and Rack `>= 3.2.7, < 4`.
Applications using Google must supply `google-protobuf`.

Rack middleware ships in the separate `connectrpc-rack` gem, which installs
compatible core and Rack dependencies. Add it to your Gemfile and keep
`require "connectrpc/rack"`:

```ruby
gem "connectrpc-rack", "~> 0.2.0"
```

Core installed alone has no middleware file, so requiring `connectrpc/rack`
without `connectrpc-rack` raises an ordinary `LoadError`. Rack itself loads only
on that explicit require. See the [connectrpc-rack guide](../gems/connectrpc-rack/README.md)
for the middleware API.

## Generate services from protobuf

`protoc-gen-connectrpc-ruby` generates mixed unary/streaming declarations, thin
clients and handler maps. Supply google-protobuf `>= 4.36.2, < 5` for the plugin
and Google message integration. The core gem still has zero runtime
dependencies.

In this repository, run the complete `.proto` -> generated messages/service ->
ephemeral localhost example with:

```sh
mise run example:generated
```

It compiles [greeting.proto](../examples/proto/greeting.proto) with protoc 36.2,
then [calls the generated client](../examples/generated.rb) in binary and JSON,
including typed deliberate error details.

To use the installed gem's plugin in your own project, put
`protoc-gen-connectrpc-ruby` on PATH (for example by running protoc through
`bundle exec`) and generate official messages and Connect glue together. With
the schema at `proto/greeting.proto`:

```sh
mkdir -p generated
bundle exec protoc -I proto --ruby_out=generated --connectrpc-ruby_out=generated greeting.proto
```

This creates official `greeting_pb.rb` and `greeting_connect.rb`. Add
`generated` to the Ruby load path.

### Use the generated service API

```ruby
require "greeting_connect"

service = Greeting::V1::GreetingService
core = ConnectRPC::Client.new(base_url: "http://127.0.0.1:8080", codec: :json)
client = service::Client.new(client: core)
response = client.greet(Greeting::V1::Greeting.new(name: "Ruby"), timeout: 2)

# Supply application callables accepting (request, context).
handlers = service.handlers(greet: greet_handler, fail: fail_handler)
server = ConnectRPC::Server.new(services: [service::SERVICE], handlers: handlers)
```

`greet_handler` and `fail_handler` above are application-defined callables; the
[runnable example](../examples/generated.rb) defines both and hosts the Rack app.

Since 0.3.0, a service object can supply the handlers instead. `handlers_for`
maps each RPC to the object's public method of the same snake-case name, which
takes the arguments the callable would, `(request, context)` for unary:

```ruby
class Greeter
  def greet(request, _context) = Greeting::V1::Greeting.new(name: "Hello, #{request.name}!")
  def fail(_request, _context) = raise ConnectRPC::Error.new(:failed_precondition, "Not today")
end

server = ConnectRPC::Server.new(services: [service::SERVICE],
                                handlers: service.handlers_for(Greeter.new, strict: true))
```

- `GREET` and `FAIL` are frozen `ConnectRPC::Method` constants; `SERVICE` is
  reusable.
- Wrapper calls return the existing `Response` and forward keyword options
  exactly, including omission of `timeout:` versus explicit `timeout: nil`.
- Binding, transport, codec, URL and limits belong to the configured core
  client. Since 0.3.0, the generated file requires
  `connectrpc/bindings/google_protobuf`, so Client and Server use the Google
  binding when `binding:` is omitted.
- `handlers` requires all declared RPC keywords and validates callability. Use
  the existing Server handler map directly for partial/custom registration.
- Since 0.3.0, `handlers_for(impl, strict: false)` checks each RPC with
  `impl.respond_to?(name)`, so private methods count as missing and
  `respond_to_missing?` is honored. A method whose owner is in
  `Class.ancestors` when `handlers_for` runs also counts as missing, so methods
  `impl` only inherits from `Object` (or, for a class or module, from `Class`
  or `Module`) are never bound, including methods that frameworks such as
  ActiveSupport add there. A handler module that is also mixed into `Object`,
  `Module` or `Class` counts as a core method and is not bound. Methods from
  `impl`'s class, its other mixins and its singleton class, including a
  module's own singleton methods (`def self.greet`), count. It omits
  RPCs the object lacks, which the Server's default invoker answers with
  `unimplemented`. `strict: true` raises `ArgumentError` naming every missing
  method. It does not check arity. Server streaming methods take
  `(request, writer, context)`, client streaming `(reader, context)` and bidi
  `(duplex, context)`.

### Generate with Buf

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

### Generation rules

Proto2 and proto3, including optional presence and mixed unary/streaming
services, are supported. Editions and unsupported plugin parameters are rejected.
Default output is `dir/foo_connect.rb` for service-bearing `dir/foo.proto`.
Message generation remains official `--ruby_out`. Set `rbs=true` to also emit
`dir/foo_connect.rbs`. Since 0.3.0, `rbs=false` is accepted and equals the
default.

The gem ships optional core signatures; the [RBS guide](rbs.md) covers official
message `--rbs_out`, explicit protobuf bridge selection and strict Steep
configuration.

Ruby module placement follows official package/`ruby_package` naming. RPC
methods use the gRPC Ruby underscore rule (`GetHTTP` becomes `get_http`).
Leading-underscore RPCs, object hooks, implicit conversion methods, reserved
runtime names and collisions visible in the request fail with diagnostics.
Compatible `google.*` and `Google::...` Ruby package nesting is supported,
including WKT imports; replacing `Google`, `ConnectRPC` or concrete protobuf
runtime constants is rejected. Packages may nest under the builtin modules
`FileTest`, `GC`, `Marshal`, `Math`, `ObjectSpace`, `Process` and `Signal`
(`math.v1` becomes `Math::V1`), but not at or below their existing values and
classes, such as `Math::E` or `Process::Status`. Other reserved roots, such as
`File`, `Kernel`, `Errno` and the standard library roots core loads, fail with a
suggestion to set `ruby_package`. Cross-invocation namespace conflicts cannot all
be discovered; generated code also guards its directly defined constants at
load time. See [design](design.md).

Generated service modules use protoc's Ruby constant rule, which upcases the
first letter and keeps underscores, so service `kw_service` becomes
`Kw_service`. A name that does not start with a letter gets a `PB_` prefix, so
`_service` becomes `PB__service`. This matches the message constants in official `_pb.rb` files.
grpc-ruby's `_services_pb.rb` camelizes service names instead (`KwService`), so
code migrating from grpc-ruby may need different constants for services whose
names contain underscores.

## Declare a method without generation

Generated code composes the same public objects you can build by hand. This
example declares one method, hosts it with Rack and adds response metadata and
trailers:

```ruby
require "connectrpc/bindings/google_protobuf"
require "connectrpc/rack"
require "google/protobuf/wrappers_pb"

type = Google::Protobuf::StringValue
method = ConnectRPC::Method.new(
  service: "example.v1.EchoService", name: "Echo",
  request_type: type, response_type: type
)
service = ConnectRPC::Service.new(name: method.service, rpcs: [method])
handler = lambda do |request, context|
  context.check!
  context.response_metadata.add("x-worker", "ruby")
  context.response_trailers.add("x-finished", "yes")
  type.new(value: "Hello, #{request.value}!")
end
server = ConnectRPC::Server.new(services: [service], handlers: { method.path => handler })
app = ConnectRPC::Rack.new(server: server) # Supply this to your Rack host.
```

`ConnectRPC::Rack.new(downstream_app, server: server)` falls through for
unrelated services. An unknown method within a registered service returns
`unimplemented`. Rack routes `PATH_INFO`, so mounting under a prefix works.
Handlers receive a typed request and a fresh `Context` for each call. Its
`request_metadata` is a mutable per-call copy of the request metadata on every
protocol. A custom `invoker: ->(method, request, context) { ... }` can own
execution instead.
Reusable handlers must synchronize any application state they share.

With that app hosted at a base URL, a client call is:

```ruby
client = ConnectRPC::Client.new(base_url: "http://127.0.0.1:8080", codec: :json)
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
A zero-length JSON payload is rejected, as connect-go does. Send the message's
JSON form instead: `{}` for an empty ordinary message, or the well-known type's
own form, such as `""` for an empty `StringValue`.

### Binding and transport seams

Bindings implement `encode(message, type:, codec:)` and
`decode(bytes, type:, codec:)`, raising `ConnectRPC::CodecError` for codec
failures. Since 0.3.0, `Client.new`, `Server.new` and `ErrorDetail#decode`
accept an omitted or nil `binding:` once `require
"connectrpc/bindings/google_protobuf"` has loaded the Google binding, and raise
`ArgumentError` naming that require otherwise. Core never loads it. Pass
`binding:` explicitly for a custom binding.

Transports implement `call(TransportRequest)` and return a `TransportResponse`.
Their fields and responsibilities are documented in [design](design.md). Neither seam requires Google or Rack types. Since 0.3.0,
an in-process transport can set `TransportResponse#cause` on an error response;
the client raises its decoded error with that exception as the `cause`.

### NetHTTP TLS and proxies

Since 0.3.0, `ConnectRPC::Transports::NetHTTP.new` takes the TLS options of the
Async client transports, plus `proxy:`. NetHTTP stays a dependency-free client
that opens one connection per call; the Async transports are the pooled and
HTTP/2 options.

```ruby
transport = ConnectRPC::Transports::NetHTTP.new(
  ca_file: "/etc/ssl/private-ca.pem",              # Added to the system trust store.
  certificate_chain: [leaf_pem, intermediate_pem], # Client certificate, leaf first.
  private_key: key_pem,
  proxy: :environment                              # Default. Or a URL String or URI, or nil.
)
```

- TLS always verifies the server certificate and hostname, requires TLS 1.2 or
  later and offers ALPN `http/1.1`. `ca_file` adds a trust anchor file to the
  system default store. `certificate_chain` and `private_key` must be given
  together. An invalid input raises `ArgumentError` that names only the input,
  such as `Invalid TLS client configuration: private_key`, with no `cause`. A
  call to an `http://` URL on a transport with any TLS option raises
  `ArgumentError` before it connects.
- `proxy: :environment` reads `HTTPS_PROXY` for `https://` URLs, `HTTP_PROXY`
  for `http://` URLs and the `NO_PROXY` exceptions once, when the transport is
  constructed. As in Go's `net/http`, uppercase names win over lowercase ones,
  a value without a scheme is read as `http://value`, an unparseable value is
  ignored, and `localhost` and loopback addresses never use the environment
  proxy.
- `NO_PROXY` takes comma-separated entries: `*`, a domain (`example.com`
  matches the domain and its subdomains; `.example.com` and `*.example.com`
  match only subdomains), an IP address or a CIDR range, each with an optional
  `:port`. Internationalized names are not converted to punycode.
- A String or URI proxies every request, including loopback and `NO_PROXY`
  hosts. A String without a scheme is read as `http://value`. `proxy: nil`
  always connects directly.
- Only `http://` proxies are supported. An `https://` URL tunnels through
  `CONNECT`, and TLS verifies the target host, not the proxy. An `http://` URL
  is sent to the proxy in absolute form. Proxy URL userinfo is percent-decoded
  and sent as `Proxy-Authorization: Basic`. An explicit `https://` or
  `socks5://` proxy raises `ArgumentError`; from the environment it fails the
  call.
- In a CGI process, where `REQUEST_METHOD` is set, an `HTTP_PROXY` value fails
  every `http://` call rather than trusting a client-controlled `Proxy` request
  header, as Go does. Explicit proxies are unaffected.

Proxy failures are `unavailable`, with a fixed reason:
`proxy authentication failed` (status 407), `proxy tunnel refused` (any other
status but 200), `proxy tunnel failed` (a malformed, truncated or oversized
reply, or bytes after a 200 reply), `proxy tunnel timed out`,
`unsupported proxy scheme`, `invalid proxy address` and
`proxy refused in CGI environment`. A `CONNECT` exchange is capped at 60
seconds, as in Go, even with `timeout: nil`; an earlier deadline still ends the
call with `deadline_exceeded`. Error messages and `inspect` never include proxy
URLs, credentials, reply text or TLS material.

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
core = ConnectRPC::Client.new(base_url: "https://api.example.com", use_get: true, get_url_limit: 8192)
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

### GET privacy and caching

GET URLs can appear in proxy/server logs, browser history and generic HTTP
instrumentation. Choose safe request contents and application cache/security
policy accordingly. All GET responses vary on `Accept-Encoding`, preserving
application Vary tokens and `*`. The library never adds public Cache-Control.
Authentication, cookies, sessions and CSRF remain application/framework policy.
Official Ruby protobuf serialization is repeatable for the same exercised
object within a process, including gzip. It has no deterministic flag or
cross-process canonical cache-key guarantee, including for maps.

## Compress with gzip

Identity is the default. Configure `encoding: :gzip` on the client to compress
requests and advertise `gzip, identity` for responses. Configure it on the server
to accept gzip requests and negotiate gzip success responses in both codecs:

```ruby
client = ConnectRPC::Client.new(base_url: "http://127.0.0.1:8080", encoding: :gzip)
server = ConnectRPC::Server.new(services: [service], handlers: { method.path => handler }, encoding: :gzip)
```

Client `accept_encoding:` independently controls accepted responses. Its default
is derived from request encoding. An explicit nonempty array contains distinct
`:gzip`/`:identity` symbols; identity is always appended as the last fallback.
For example:

- `accept_encoding: [:gzip]` permits identity requests with gzip responses.
- `encoding: :gzip, accept_encoding: [:identity]` permits gzip requests with
  identity responses.

The server selects a supported compression coding from the ordered accept
fields before identity. An absent field inherits the request coding; an
explicitly empty field selects identity. Unknown tokens and parameters are
ignored, except that a zero quality value (`gzip;q=0`) refuses gzip. Only one
identity or gzip content coding is accepted.

Empty serialized messages selected for gzip produce valid nonempty gzip members.
Zero-length HTTP content is never inflated, after its coding header is validated.
Concatenated members are supported under one aggregate decoded limit; every
member's CRC, size and completion are validated. Truncation and trailing garbage
are rejected. Server errors remain identity; clients can decode compressed peer
errors, including typed details and metadata. Malformed compressed HTTP errors
use HTTP fallback, while malformed compressed success responses return internal.

## Metadata and errors

### Metadata

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

Since 0.3.0, Connect clients send
`User-Agent: connectrpc-ruby/<version> (ruby <RUBY_VERSION>)` and gRPC clients
send `grpc-ruby-connectrpc/<version> (ruby <RUBY_VERSION>)`, unless the call
metadata or an interceptor sets `user-agent`. `"user-agent" => []` sends no
User-Agent. The client adds the default after interceptors run, so interceptors
do not see it. NetHTTP sends exactly the request's `user-agent` values and
never adds Net::HTTP's `User-Agent: Ruby`.

Rack responses preserve arrays. Net::HTTP preserves repeated raw response
fields, but joins outgoing request arrays with `", "`. rackup/WEBrick also
coalesces incoming and response fields. For example, `['a', 'b']` may arrive as
`['a, b']`. Interop asserts these observed values; original outbound field
boundaries are not preserved.

### Errors

Raise a deliberate `ConnectRPC::Error.new(:invalid_argument, "safe message",
details: [...], metadata: {...}, trailers: {...})` from a handler. Unexpected
application exceptions become `internal` with `"Internal error"`. Clients raise
`ConnectRPC::Error` with `code`, `details`, `metadata`, and `trailers`. Decode a
detail with `detail.decode(type: message_class)` after choosing
the class corresponding to `detail.type`. `Error#code` is a frozen String such
as `"invalid_argument"`, even though `Error.new` accepts a Symbol, so compare it
with a String: `error.code == "invalid_argument"`.

The peer never sees an unexpected exception, so log or report it on the server.
Since 0.3.0, pass `on_error:` to the Server:

```ruby
server = ConnectRPC::Server.new(
  services: [service::SERVICE], handlers: handlers,
  on_error: ->(error) { logger.error(error.full_message(highlight: false)) }
)
```

Core calls `on_error` with the original exception when a `StandardError` that
is not a `ConnectRPC::Error` escapes a handler, an interceptor, a custom
invoker or unary response encoding, and the peer receives the generic
`internal` error. This covers Connect, gRPC and gRPC-Web, unary and streaming,
on every host. The callable runs on the request's thread or fiber, inside the
RPC's instrumentation scope, so request-scoped loggers and an active trace
still apply. Exceptions raised by `on_error` itself are ignored, and the peer
response is unchanged.

Some failures are not reported. When the deadline or a cancellation already
decides the outcome, the peer receives `deadline_exceeded` or `canceled`
instead. Failures that core maps to its own error first, such as a streaming
response message the binding cannot encode, reach
[instrumentation](#instrumentation) only.

Hosts add their own reporting:

- Since 0.3.0, [connectrpc-rack](../gems/connectrpc-rack/README.md) also writes
  each unexpected exception with its backtrace to `env["rack.errors"]`.
- Since 0.3.0, [connectrpc-testing](../gems/connectrpc-testing/README.md)
  records the original exception as `TransportResponse#cause`, and the client
  raises its decoded `internal` error with that `cause`.
- [connectrpc-on-rails](../gems/connectrpc-on-rails/README.md) builds its own
  Server per request. Handler exceptions follow Rails: `rescue_from`, the
  controller logger and `Rails.error`. Unexpected exceptions outside the action,
  such as from a server interceptor, go to `env["rack.errors"]` as on Rack.
- [connectrpc-sentry](../gems/connectrpc-sentry/README.md) already reports
  unexpected handler exceptions, so do not also send them to Sentry from
  `on_error`.

## Limits

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

## Deadlines

Client `timeout:` accepts finite real seconds, defaulting to 30. Since 0.3.0,
zero and negative timeouts are already expired, as in grpc-go: the call fails
locally with `deadline_exceeded` before the transport runs. Infinite, NaN and
non-numeric timeouts raise `ArgumentError`. Explicit `timeout: nil` removes
only the relative timeout budget. A call has no logical deadline, expiry
watchdog or timeout header only when it passes `timeout: nil` and no
`deadline:`.

Since 0.3.0, every call, including the streaming calls and generated wrappers,
also accepts `deadline:`, a `ConnectRPC::Deadline` or nil. The call uses the
earlier of `timeout:` and `deadline:`, chosen once at call setup, and reuses the
supplied object when it is strictly earlier; a tie keeps the `timeout:`
budget, which always runs on the default monotonic clock. A `deadline:` built
on a custom `clock:` is compared with the call's timeout by remaining time at
call setup, so the choice can be off by the time between those two clock
reads; deadlines on the default clock compare exactly. A handler passes its
own deadline to a downstream call so it ends no later than the request that
caused it:

```ruby
def greet(request, context)
  profile = profiles.lookup(ProfileRequest.new(id: request.name), deadline: context.deadline)
  Greeting::V1::Greeting.new(name: "Hello, #{profile.message.display_name}!")
end
```

The downstream call keeps the default 30 second cap when that is earlier;
pass `timeout: nil` to rely on `deadline:` alone. Nothing propagates
implicitly, and cancelling the upstream call does not cancel the downstream
one. Native TCP/TLS/read/write waits remain independently capped at
`9_999_999.999` seconds; a native timeout returns `unavailable`. Sustained
progress can outlast that per-operation cap.

For finite values, one monotonic budget starts before the
[interceptor chain](#interceptors) and covers interceptor time, encoding, network
I/O and checks around codec work. The wire timeout is computed at dispatch from
the remaining budget.

The default NetHTTP transport:

- uses one connection per call and closes it on every outcome
- sends the configured `Accept-Encoding`
- uses HTTP proxies from the environment by default (see
  [NetHTTP TLS and proxies](#nethttp-tls-and-proxies))
- disables retries and decompression
- preserves TLS peer/hostname verification
- does not follow redirects

A transport-owned watchdog closes the call's socket at expiry, including during
TCP connect, a proxy `CONNECT` exchange, the TLS handshake, response-header
trickle and blocked writes. Native DNS resolution may outlast
the budget; expiry is checked when control returns. Codec work and request
preparation are synchronous and checked before/after; gzip work also checks
each native input/output chunk and member boundary. They are not interrupted.
Dispatch rechecks expiry and refreshes the wire timeout after connection setup;
a closed socket cannot trigger an implicit reconnect.

Native operations are independently capped at `9_999_999.999` seconds, the
Connect wire maximum, to avoid platform overflow for very large budgets. A
native operation timeout before logical expiry returns `unavailable`; actual
expiry returns `deadline_exceeded`. The logical deadline stays unchanged, and
the watchdog rechecks it after each capped wait.

## Cancellation

Explicit unary cancellation uses a permanent, shareable token:

```ruby
source = ConnectRPC::CancellationSource.new
# Pass source.token to each call that should share cancellation.
response = client.call(method, message, timeout: nil, cancellation: source.token)
# Another thread or reactor task can call source.cancel while the call blocks.
```

### Sources, tokens and listeners

`source.cancel` returns true only for its first transition. A token exposes
`cancelled?`, `reason` (`:canceled` or nil), `check!`, and
`on_cancel { ... }`. Registration returns an object with `unregister`, which
returns true only when it prevents an invocation that has not started. It never
waits for a running listener. Listeners run in the cancelling thread, must be
short and nonblocking, and isolate `StandardError`. Unregistering releases the
callback; completing a call releases its internal registrations. Sources cannot
be reset, and a call never completes or changes another call's shared token.

### Outcome arbitration

Each call accepts one terminal outcome. An already-expired deadline takes
precedence while the call is open, followed by observed explicit cancellation.
Accepted cancellation stays canceled through later expiry. A committed response
or error stays final after later source cancellation. Source transition time is
not acceptance time: a slow earlier listener may delay acceptance until expiry.
Final decoding/error commit checks the source directly, even if notification is
delayed. Cancellation never substitutes a timeout header for `timeout: nil`.

### Transport behavior

NetHTTP owns each socket from its creation and closes only that socket to
interrupt TCP connect, a proxy `CONNECT` exchange, TLS, writes, headers and body
reads. Native DNS resolution remains cooperative; cancellation is checked on
return before any later dispatch.
The optional Async transport wakes its owner reactor and resets its stream.
Healthy multiplexed siblings survive; a stuck frame retains the documented
connection-wide cleanup fallback. Neither backend retries or replays a cancelled call.

### Server contexts

Server contexts expose `deadline`, `cancelled?`, `cancellation_reason`
(`:canceled`, `:deadline_exceeded`, or nil), and `check!`. Handlers cooperate at
application boundaries. `context.on_cancel { ... }` registers a listener with the
same semantics as a token listener and returns its registration; unregister it
when the work it guards ends. It runs when the call's cancellation is signaled,
or when a check observes an expired deadline. A blocked handler wakes on
cancellation without polling; deadline expiry runs the listener only after a
check, so a handler waiting past its deadline needs a wait bounded by
`deadline.remaining` and a check after waking. Low-level `Server#call(cancellation:)` and the Testing
transport accept explicit cancellation. Native HTTP/2 publishes peer
`RST_STREAM(CANCEL)` before input teardown; normal END_STREAM/half-close, other
reset codes, connection loss and host shutdown do not manufacture explicit
cancellation. The [support contract](support.md#delivery-cancellation-and-metadata)
classifies this signal policy as provisional. Rack/Rails and Falcon HTTP/1 add
no peer detection or token env injection. Synchronous adapters cannot interrupt
blocked application code.
See [design](design.md) for ownership and transport limits.

## Interceptors

Interceptors put one policy, such as authentication, validation, metadata
injection or error translation, around every unary call of a Client or Server.
An interceptor is any object with a `client_unary(call)` or `server_unary(call)`
method. Its block is the continuation: `yield` runs the next interceptor, or
dispatch for the innermost one.

```ruby
class BearerAuth
  def initialize(tokens) = @tokens = tokens

  def client_unary(call)
    call.request_metadata["authorization"] = "Bearer #{@tokens.current}"
    yield
  end
end

class RequireTenant
  def server_unary(call)
    tenant = call.context.request_metadata["x-tenant"]
    raise ConnectRPC::Error.new(:unauthenticated, "Missing tenant") unless tenant

    yield
  end
end

client = ConnectRPC::Client.new(base_url: url, interceptors: [BearerAuth.new(tokens)])
server = ConnectRPC::Server.new(services: services, handlers: handlers, interceptors: [RequireTenant.new])
```

`interceptors:` defaults to an empty Array. Core discovers each element's
methods once at construction and raises `ArgumentError` for an element with
neither the unary nor the stream method for that role, so a server-only
interceptor passed to a Client is rejected. An element may also define
`client_stream` or `server_stream`, which cover the
[streaming shapes](streaming.md#interceptors); unary calls run only the unary
methods. Interceptor objects are shared by concurrent calls; per-call state
belongs in the call object. With no interceptors for a kind, dispatch takes the
same path as before.

The first interceptor listed is the outermost on both roles. It sees the call
first and the response or error last. Interceptors work with generated clients,
every unary transport, custom bindings and custom invokers, on Connect POST and
GET, [native gRPC](grpc.md#interceptors) and [gRPC-Web](grpc-web.md#interceptors).

### Continuation rules

Core enforces these rules and raises `ConnectRPC::InterceptorStateError` when
one is broken:

- Call the continuation at most once, on the fiber that called the interceptor,
  and only before the interceptor returns. A second call, a call from another
  thread, fiber or `Async` task, and a captured block called later all raise
  and dispatch nothing. A second call or a call from another thread or fiber
  also fails the call when the interceptor returns, even if the interceptor
  swallows the error, and instrumentation records that misuse as the call's
  error even if the interceptor translates it. A captured block called after
  its interceptor returned only raises to its caller.
- A continuation fails when its block raises or is left by a `throw` or other
  non-local exit. If a layer catches such a `throw` inside the chain and
  returns, the call fails with the exception the continuation raised, or with
  `InterceptorStateError` if it did not raise. A `throw` that leaves the
  outermost interceptor propagates as it would without interceptors, and a
  killed thread or fiber keeps dying.
- A client interceptor may return a `ConnectRPC::Response` without yielding,
  for example from a local cache. Every other layer must yield; a server
  interceptor that returns without yielding fails the call with `internal`.
- A client interceptor's normal return must be a `Response` whose `metadata` and
  `trailers` are `ConnectRPC::Metadata`.
- No interceptor can turn a failed continuation into success. It may raise a
  different exception to translate the failure. If it returns normally after
  the continuation raised, core discards the return value and re-raises the
  continuation's exception. On the server, a successful response therefore
  always means the invoker returned successfully. To supply a client fallback,
  rescue around `client.call`.

On the client an `InterceptorStateError` reaches the caller. On the server it is
recorded and becomes `internal`, like any unexpected handler exception.

### Client calls

`ClientUnaryCall` exposes `method`, `protocol` (`:connect` or `:grpc`),
`request`, `request_metadata`, `deadline` and `cancellation`. Until the
innermost continuation starts dispatch, `request=` replaces the message and
`request_metadata` accepts validated writes. Afterwards `request=` raises
`InterceptorStateError` and metadata writes raise `FrozenError`.

- The deadline and the call's cancellation arbiter start before the outermost
  interceptor, so interceptor time counts against `timeout:`. `cancellation` is
  a read-only view with `cancelled?`, `reason`, `check!` and `on_cancel`.
  Interceptors cannot change the deadline. Core cannot interrupt an interceptor;
  one that blocks on I/O should observe `deadline` or `cancellation` itself.
- Instrumentation injects its propagation headers after the chain, merging them
  with interceptor-added metadata as caller metadata. The lifecycle records the
  caller metadata before the chain and the metadata actually sent at dispatch.
- Every client unary `Response` is frozen, with or without interceptors: the
  struct, its `metadata` and its `trailers`. Core freezes a short-circuit or
  replacement `Response` before the next outer layer receives it. The message is
  not copied or frozen, so a cached `Response` shares its message between
  callers; return a fresh `Response` with a copied message when callers may
  mutate it.
- A short-circuit records an outcome with no HTTP facts and `delivery:
  :unobserved`. A rejection raised before `yield` never reaches the transport.

Core commits the call's outcome once, after the outermost interceptor returns
or raises. A cancellation or expiry accepted before then replaces a successful
result, and replaces a failure that came from dispatch: transport, codec,
metadata and status failures. An exception an interceptor raises, including a
translation of a dispatch failure, reaches the caller with its identity, and
the outcome reports its own code. Error-mapping interceptors should check
`call.cancellation.cancelled?` before translating, so an accepted cancellation
is not hidden.

### Server calls

Server interceptors run after the bounded read and decode, and around the
invoker, so a custom invoker is the innermost layer. They do not run for 405,
415, CORS preflight, undeclared methods or header, timeout, read and decode
failures. A declared method without a handler reaches the chain and fails with
the invoker's `unimplemented` error.

In Rails, controllers return server interceptors from the private
[`connect_interceptors`](../gems/connectrpc-on-rails/README.md#interceptors)
hook. The Rails invoker is the innermost layer, so interceptors run before
callbacks, CSRF verification and `rescue_from`, and a successful call still
needs the whole controller chain to succeed.

`ServerUnaryCall` exposes `method`, `protocol` (`:connect`, `:grpc` or
`:grpc_web`), `request`, `request=` (until dispatch) and `context`, the same
`Context` the handler receives:

- `context.request_metadata` is shared with the handler, so an interceptor can
  add derived values such as a verified principal. Writes do not change the
  request metadata that instrumentation recorded.
- Response metadata and trailers are written after the chain returns, so
  writes before and after `yield` both reach the response, including the
  error response when the call fails.
- A raised `ConnectRPC::Error` becomes the wire error with its details,
  metadata and trailers. Any other exception becomes `internal`.
- After a successful continuation, the return value is the response message.
  A nil return follows the handler nil path.
- The request deadline started when headers were parsed. The context's
  `cancelled?`, `check!` and `on_cancel` work inside interceptors. Once the
  context accepts cancellation or expiry, that reason is the wire error,
  whatever the interceptor raises or returns.

### Failures and observation

Interceptors run inside the instrumentation activation, so observer spans are
current while they run and processing durations include their time. Core
records the exception escaping each continuation before the outer interceptor
sees it, and recording keeps the first failure. Observers therefore receive a
handler's or transport's original exception even after an interceptor
translated it, while the caller or wire receives the translation.

## Instrumentation

Pass `instrumentation: ConnectRPC::Instrumentation.new(subscribers: [recorder])`
to a Client or Server for synchronous per-call observation. A subscriber's
`start(call)` returns an observer with optional `attach`, `detach(token)`,
`processing_finished(outcome)`, `finished(completion)`, `message_received(bytes)`
and `message_sent(bytes)` methods. The message callbacks fire live, once per
decoded or emitted message, with its encoded payload size. Core owns the
application yield and isolates each StandardError from subscribers and the
optional `on_error:` reporter. Original exceptions are process-local trusted
subscriber data. Instrumentation is disabled by default and performs no lifecycle
allocation or instrumentation clock reads on that path.

The optional [connectrpc-otel package](../gems/connectrpc-otel/README.md) turns
this lifecycle into real Connect CLIENT/SERVER traces using an
application-owned OpenTelemetry SDK. Core has no OpenTelemetry dependency. See
[design](design.md#rpc-lifecycle-observation) for host ownership and
`mise run otel:example` for a runnable generated caller with real SDK exports.

For metrics, the optional `require "connectrpc/metrics"` layer in core records a
fixed go-grpc-prometheus compatible metric set through a small recorder SPI.
The [connectrpc-prometheus](../gems/connectrpc-prometheus/README.md) and
[connectrpc-yabeda](../gems/connectrpc-yabeda/README.md) packages are its sinks.

The optional [connectrpc-sentry package](../gems/connectrpc-sentry/README.md)
records the same lifecycle as native Sentry transactions and spans, propagates
`sentry-trace`/`baggage` and reports unexpected server handler exceptions,
using an application-owned Sentry SDK. Core has no Sentry dependency.

## Runtime baseline and evidence

CRuby >= 3.4 is the baseline; project verification targets Ruby 3.4.12 and
4.0.9 on Linux x64. Local setup builds from source; CI uses official prebuilt
runtimes. See [development](development.md) for the commands, dependency
policy, isolated installation and evidence scope.

The interop fixture runs 18 cases: six Go-to-Go reference baseline cases and
twelve Ruby-to-Go/Go-to-Ruby cases. Both codecs share typed success, deliberate
error and request-limit scenarios. This covers the advertised unary subset, not
full conformance. A separate gzip portion adds 40 cases with actual wire
observations, including asymmetric raw Go HTTP probes and documented
reference-client quirks.

The pinned [official conformance harness](../conformance/README.md)
independently executes 96 client and 90 server identity cases, including two
safe GET cases per direction. The disjoint gzip profile adds 88 client and 82
server cases. All 184 client and 172 server cases must pass with zero expected
failures. The four caller-cancellation cases use their original 5 ms
asynchronous timer, 200 ms response request and no RPC deadline. The ledger
requires actual request writes, source cancellation, owned socket cleanup and
the library's canceled result. This is a bounded profile, not full Connect
conformance.
