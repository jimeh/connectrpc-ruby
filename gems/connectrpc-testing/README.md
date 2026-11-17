<div align="center">

<img height="128px" width="128px" src="../../img/connectrpc-ruby.svg" alt="connectrpc-ruby logo">

# connectrpc-testing

**In-process transport for real ConnectRPC unary round trips in tests.**

[![RubyGems](https://img.shields.io/gem/v/connectrpc-testing?logo=rubygems&logoColor=white&label=RubyGems)](https://rubygems.org/gems/connectrpc-testing)
[![CI](https://img.shields.io/github/actions/workflow/status/jimeh/connectrpc-ruby/ci.yml?branch=main&logo=github&label=CI)](https://github.com/jimeh/connectrpc-ruby/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/Docs-guides-0969da?logo=markdown&logoColor=white)](https://github.com/jimeh/connectrpc-ruby/tree/main/docs)
[![License](https://img.shields.io/github/license/jimeh/connectrpc-ruby?label=License)](https://github.com/jimeh/connectrpc-ruby/blob/main/LICENSE)

</div>

`connectrpc-testing` provides a synchronous in-process transport for real
Connect unary round trips. The application supplies a core `Server`, its
binding, services and handlers. The ordinary core or generated client still
encodes requests and decodes responses, including protocol errors, metadata,
safe unary GET and compression. No sockets are opened.

Use it to test handlers and clients together through the actual wire encoding,
without starting a server. It is not a substitute for testing against a real
host: see [what it does not cover](#what-it-does-not-cover).

The package is experimental. See the [supported 0.x contract](../../docs/support.md)
for the target support profile, matched package versions, compatibility policy
and adoption gates.

## Installation

Add the gem to your development or test group:

```ruby
group :development, :test do
  gem "connectrpc-testing", "~> 0.2.0"
end
```

The runtime dependency is only `connectrpc ~> 0.2.0`; Google protobuf and a
test framework are application choices.

## Usage

This example uses the repository's generated `Example::V1::EchoService`
fixture. Replace it with your own generated service:

```ruby
require "connectrpc/testing"
require "connectrpc/bindings/google_protobuf" # application's optional binding
require "echo_connect" # application's generated messages and service

service = Example::V1::EchoService
binding = ConnectRPC::Bindings::GoogleProtobuf.new
server = ConnectRPC::Server.new(
  services: [service::SERVICE], binding: binding,
  handlers: service.handlers(
    echo: ->(request, context) {
      context.response_trailers.add("x-tested", "yes")
      request
    },
    fail: ->(_request, _context) {
      raise ConnectRPC::Error.new(:invalid_argument, "safe message")
    }
  )
)
transport = ConnectRPC::Testing::Transport.new(server: server)
core = ConnectRPC::Client.new(
  base_url: "https://in-process.invalid", binding: binding, transport: transport
)
client = service::Client.new(client: core)
response = client.echo(Example::V1::EchoMessage.new(name: "hello"), timeout: nil)
raise "Unexpected response" unless response.message.name == "hello"
raise "Missing trailer" unless response.trailers["x-tested"] == ["yes"]
```

Calls through `client` raise `ConnectRPC::Error` for error responses, exactly as
they would over the network. Since 0.3.0, when a handler raises an exception
that is not a `ConnectRPC::Error`, the transport still returns the generic
`internal` error response and records the original exception as its
`TransportResponse#cause`, which is never encoded. The client raises its
decoded error with that `cause`, so test failures show the handler's
backtrace, while client instrumentation and response limits behave as they
would over the network. A Server `on_error:` reporter still runs.

## Request handling

### URLs and routing

The hostname is validated but never resolved. URLs must be absolute HTTP(S)
URIs without credentials or fragments. Raw URI queries are forwarded to core,
including safe unary GET.

The literal URI path identifies the method: a base URL prefix is retained and
will normally produce core's unknown-method response. There is no mount-prefix
stripping or Rack fallthrough. The actual HTTP method and repeated leading
header arrays pass to core unchanged. Malformed protocol header values retain
core classification; structurally unsupported Ruby inputs raise `ArgumentError`
before dispatch. Frozen caller URI, headers and body are supported and preserved.

### Bodies and limits

The private byte reader uses `byteslice` over the original body under core's
bounded `MessageIO` reads, including multibyte JSON. It does not decode or
recompress payloads. `TransportRequest.response_limit` caps encoded bytes,
including error bodies and gzip framing. Core's server materializes a response
before the transport checks its cap. That check cannot undo the allocation and
does not establish a total memory bound. Server and Client message and wire
limits remain separate.

### Deadlines

A supplied original `Deadline` is checked before and after dispatch. Immediately
before dispatch, the transport copies headers and refreshes `connect-timeout-ms`
from the remaining budget. Core reconstructs its cooperative server deadline
from rounded/clamped wire milliseconds. The original deadline remains
authoritative and expiry takes precedence over the response wire cap.

The Client's omitted timeout remains 30 seconds; `timeout: nil` adds no deadline
or timeout header. A low-level request with nil deadline can still explicitly
supply a core timeout header.

### Explicit unary cancellation

Pass `cancellation: source.token` to core or generated client calls, where
`source = ConnectRPC::CancellationSource.new`. The source is permanent and
shareable; only `source.cancel` initiates cancellation. `timeout: nil` keeps no
RPC deadline or wire timeout. Context offers cooperative `cancelled?`,
`cancellation_reason` and `check!`. See the
[unary guide](../../docs/unary.md#cancellation) for listener, arbitration and
registration lifetime rules.

Testing forwards the read-only cancellation seam into a distinct server Context.
Completing server processing cannot complete its client or shared source. Checks
are cooperative; Testing does not interrupt a blocked handler.

## Interceptors

Client and Server [interceptors](../../docs/unary.md#interceptors) both run in
process on the calling thread. The client chain wraps the transport call. The
server chain runs inside `Transport#call`, after core decodes the request and
around the invoker. A call passes through the client interceptors, outermost
first, then the server interceptors, outermost first, then the handler. A
client interceptor that returns a `Response` without yielding skips the
transport, so the server interceptors and handler do not run for that call.
Streaming is not supported by this transport, so only `client_unary` and
`server_unary` run.

## What it does not cover

Execution is synchronous and cooperative. This transport cannot interrupt a
blocked handler or codec. It makes no network, DNS, TLS, fiber, backpressure,
concurrency, lifecycle, retry or cancellation compatibility promise. Use real
hosts and independent peers for those checks. These round trips are not
interoperability or official conformance evidence.

## gRPC-Web

Manual encoded TransportRequest values can exercise explicitly enabled
binary/text unary gRPC-Web. This adds no Web Client mode, serializer, network
or browser conformance claim. See the [Web guide](../../docs/grpc-web.md) for
protocol, CORS, bounds and completion rules.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional
[OpenTelemetry adapter](../connectrpc-otel/README.md). The server always
extracts serialized propagation headers, even while a client context is
ambient. Direct processing ends with delivery `unobserved`.

## Static signatures

This package owns `sig/` and its discovery manifest. Add `library "connectrpc"`
and `library "connectrpc-testing"` to a strict Steep target with this gem
installed. The manifest lists only standard libraries, so name each connectrpc
library explicitly; this also works with `rbs collection`. See the
[RBS guide](../../docs/rbs.md) for supported APIs, extension contracts, partial
records, runtime validation and framework-signature limits. `mise run rbs:check`
checks application callers for this package; strict core source checking is a
separate target from backend implementation checking.

## Development

Inside this repository, development and test bundles use
`gem "connectrpc-testing", path: "gems/connectrpc-testing"`. To try unreleased
changes from another local application, point `path:` at this package and
provide the core gem through its own local path.

From the repository root:

```sh
mise run testing:setup
mise run testing:test -- spec/transport_spec.rb
mise run testing:check
mise run testing:example
mise run testing:package
MISE_RUBY_VERSION=4.0.9 mise run testing:verify
```

Pinned Ruby and Bundler must already be installed; these tasks never install
Bundler globally. The independent bundle, caches and archives use
`vendor/testing/`; SimpleCov starts before library requires, includes every
package library file and writes `coverage/testing/<Ruby>/`. `testing:check`
parses every maintained Ruby file and uses the repository RuboCop policy.

`testing:package` builds its own core archive, installs only the testing archive
with normal offline resolution from an adjacent core archive in a fresh external
GEM_HOME, checks public require without optional bindings/hosts, then installs
the cached Google dependency closure and runs the generated example in both
codecs and encodings. Root verification remains independent.

## License

Licensed under the [MIT License](LICENSE). It is released with the whole
[release train](../../docs/releasing.md); its package tasks never publish it.
