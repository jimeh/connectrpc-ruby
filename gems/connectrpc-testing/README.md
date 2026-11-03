# connectrpc-testing

A synchronous in-process transport for real Connect unary round trips. The
application supplies a core `Server`, its binding, services and handlers. The
ordinary core or generated client still encodes requests and decodes responses,
including protocol errors, metadata, safe unary GET and compression. No sockets are opened.

Add `gem "connectrpc-testing", path: "gems/connectrpc-testing"` to a development
or test bundle in this repository. For another local application, point `path:`
at this package and provide the core gem through its own local path. Publication
is disabled. The runtime dependency is only `connectrpc ~> 0.1.0`; Google protobuf
and a test framework are application choices.

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

The hostname is validated but never resolved. URLs must be absolute HTTP(S) URIs
without credentials or fragments. Raw URI queries are forwarded to core, including safe unary GET. The literal URI path
identifies the method: a base URL prefix is retained and will normally produce
core's unknown-method response. There is no mount-prefix stripping or Rack
fallthrough. The actual HTTP method and repeated leading header arrays pass to
core unchanged. Malformed protocol header values retain core classification;
structurally unsupported Ruby inputs raise `ArgumentError` before dispatch.
Frozen caller URI, headers and body are supported and preserved.

The private byte reader uses `byteslice` over the original body under core's
bounded `MessageIO` reads, including multibyte JSON. It does not decode or
recompress payloads. `TransportRequest.response_limit` caps encoded bytes,
including error bodies and gzip framing. Core's server materializes a response
before the transport checks its cap. That check cannot undo the allocation and
does not establish a total memory bound. Server and Client message and wire
limits remain separate.

A supplied original `Deadline` is checked before and after dispatch. Immediately
before dispatch, the transport copies headers and refreshes `connect-timeout-ms`
from the remaining budget. Core reconstructs its cooperative server deadline
from rounded/clamped wire milliseconds. The original deadline remains
authoritative and expiry takes precedence over the response wire cap. The
Client's omitted timeout remains 30 seconds; `timeout: nil` adds no deadline or
timeout header. A low-level request with nil deadline can still explicitly
supply a core timeout header.

Execution is synchronous and cooperative. This transport cannot interrupt a
blocked handler or codec. It makes no network, DNS, TLS, fiber, backpressure,
concurrency, lifecycle, retry or cancellation compatibility promise. Use real
hosts and independent peers for those checks. These round trips are not
interoperability or official conformance evidence.

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
Bundler globally. The independent checksummed bundle, caches and archives use
`vendor/testing/`; SimpleCov starts before library requires, includes every
package library file and writes `coverage/testing/<Ruby>/`. `testing:check`
parses every maintained Ruby file and uses the repository RuboCop policy.
`testing:package` builds its own core archive, installs only the testing archive
with normal offline resolution from an adjacent core archive in a fresh external
GEM_HOME, checks public require without optional bindings/hosts, then installs
the cached Google dependency closure and runs the generated example in both
codecs and encodings. Root verification remains independent.

Licensed under the [MIT License](LICENSE). The invalid `allowed_push_host` guard
remains in place; no task publishes or releases this package.

## RPC observation

Configure `instrumentation:` on the core Client/Server to enable the fixed
neutral per-call lifecycle or the optional [OpenTelemetry adapter](../connectrpc-otel/README.md).
The server always extracts serialized propagation headers, even while a client
context is ambient. Direct processing ends with delivery `unobserved`.

## Static signatures

This package owns `sig/` and its discovery manifest. Add
`library "connectrpc-testing"` to a strict Steep target with this gem installed.
See the [RBS guide](../../docs/rbs.md) for supported APIs, extension contracts,
partial records, runtime validation and framework-signature limits.
`mise run rbs:check` checks application callers for this package; strict core
source checking is a separate target from backend implementation checking.

## Explicit unary cancellation

Pass `cancellation: source.token` to core or generated client calls, where
`source = ConnectRPC::CancellationSource.new`. The source is permanent and
shareable; only `source.cancel` initiates cancellation. `timeout: nil` keeps no
RPC deadline or wire timeout. Context offers cooperative `cancelled?`,
`cancellation_reason` and `check!`. See the [core guide](../../README.md) for
listener, arbitration and registration lifetime rules.

Testing forwards the read-only cancellation seam into a distinct server Context.
Completing server processing cannot complete its client or shared source. Checks
are cooperative; Testing does not interrupt a blocked handler.
