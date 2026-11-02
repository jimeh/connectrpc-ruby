# Design and implemented profile

The gem implements a first typed Connect unary slice. Shared protocol code is
independent of HTTP hosting and concrete protobuf classes. The public require
is `connectrpc`, and the Ruby namespace is `ConnectRPC`. The API is experimental.

## Shared boundaries

`Method` holds validated service/method names, request/response binding types,
`cardinality: :unary`, and a case-sensitive path. `Service` validates its method
set. Other cardinalities are rejected. Generated service glue reuses these objects.

`Client` owns request encoding, protocol headers, response validation, errors,
metadata, byte limits and the deadline. Base URL path prefixes are retained.
A binding provides `encode(message, type:, codec:)` and
`decode(bytes, type:, codec:)`. Custom bindings raise `CodecError` for invalid
messages. The optional Google binding delegates binary and
[protobuf JSON](https://protobuf.dev/programming-guides/json/) to native
protobuf APIs. It ignores unknown JSON fields. Scalar wrapper roots use a
standard Any envelope internally to work around protobuf 4.36's direct-root
decoder limitation; the backend still validates the protobuf values.

A transport provides `call(request)` with these host-neutral value objects:

| Object | Fields |
| --- | --- |
| `TransportRequest` | `url` (URI), `http_method`, `headers` (lower-case keys, string arrays), `body` (bytes), `response_limit`, `deadline` |
| `TransportResponse` | `status` (integer), `headers` (repeated string arrays), `body` (bounded bytes) |
| `Response` | `message` (binding type), `metadata`, `trailers` (Metadata objects) |

`TransportRequest.deadline` is a `Deadline` or `nil`. Custom transports must
enforce a supplied deadline during their I/O and bound the body incrementally.
An absent deadline means no aggregate RPC expiry or wire timeout. Client validation checks their output again but
cannot undo an oversized allocation or interrupt their work.

`Server` owns negotiation, bounded input, decoding, invocation, serialization,
and safe errors. `Rack` only translates request/response HTTP state. It wraps
an optional downstream app and routes `PATH_INFO`; unrelated service paths
fall through, while unknown methods within registered services fail explicitly.
Neither core nor handlers need Rack envs. Google and Rack load only through
`connectrpc/bindings/google_protobuf` and `connectrpc/rack`, with useful errors
when their optional dependency is absent or incompatible.

A default invoker calls the registered plain handler with `(request, context)`.
An explicit `invoker` callable receives `(method, request, context)` and can
supply framework execution later. Every invocation has a fresh `Context` with
request metadata, response leading/trailing metadata, and optional deadline.
Handlers are reusable; application-owned shared state needs its own locking.

## Wire behavior

The implemented [Connect profile](https://connectrpc.com/docs/protocol/) is
unary POST, identity encoding, and `application/proto` or `application/json`.
JSON also accepts `charset=utf-8`. Clients send protocol version `1`; servers
accept an absent version and reject any other supplied value. A 200 response
must match the request codec. Non-application response types produce `unknown`;
mismatched application codecs and nonidentity response encodings produce
`internal`. JSON errors are only parsed for an appropriate JSON content type.
The single identity content coding is case-insensitive on requests and
responses; duplicate or multiple codings are rejected.

Other verbs get bare 405 with `Allow: POST`; unsupported types, including
Connect streaming and gRPC variants, get bare 415 with `Accept-Post`.
Unsupported request encoding returns `unimplemented` listing identity. Invalid
version, timeout, or request decoding returns `invalid_argument`. Limits return
`resource_exhausted`. Unexpected application exceptions return generic
`internal`; deliberate `Error` messages are application-controlled safe text.
Standard Connect-to-HTTP and fallback HTTP-to-Connect tables are used. Null, absent and unrecognized wire codes use the HTTP fallback while preserving
valid message and details. Malformed bodies, code types or details use the
HTTP fallback without decoding those error fields. Error details preserve a type name and binary bytes, with
unpadded base64 on output and validated padded/unpadded input.

`Metadata` stores lower-case keys and repeated printable ASCII values and
rejects protocol/framing overrides and header injection. Binary `-bin` helpers
handle base64 and comma coalescing. Trailing metadata becomes `trailer-`
response fields. Other fields are never blindly comma-split. The middleware
emits separate arrays under `Rack::Lint`, and NetHTTP preserves raw repeated
response fields. NetHTTP joins outgoing request arrays with comma+space.
rackup/WEBrick additionally coalesces incoming and response fields; interop
asserts those observed values explicitly. Received fields outside the metadata
name/printable-ASCII contract are omitted during extraction, including an
entire field with any invalid repeated value. Valid adjacent leading/trailing
fields survive. Protocol validation is separate, and outbound metadata remains
strictly validated.

## Bounds and lifecycle

Default serialized request/response limits are 4 MiB each. Declared lengths
are validated and oversized lengths rejected before input or decode.
Unknown-length bodies are read with bounded chunks, stopping at the first
excess byte. HTTP responses and error bodies are bounded before decode.
Serialization can allocate before the output check. If a bounded error cannot
fit, the server emits an empty 429 body. There is no independent header-size
guarantee beyond the host and HTTP library.

The default timeout is 30 seconds. Explicit `timeout: nil` means no logical
client deadline, timeout header or expiry watchdog. Native operation waits still
use the exact `9_999_999.999` second cap, returning `unavailable` on timeout.
Sustained progress can outlast this per-operation cap. For finite positive
timeouts, one monotonic client deadline begins before encoding. Remaining timeout milliseconds are
rounded up and clamped to ten digits without extending actual expiry. NetHTTP
recomputes the header at dispatch and checks the budget around connection,
codec and body progress. Each call owns one connection; a finite deadline also owns one watchdog. The
watchdog closes the owned raw socket on expiry, including during header
trickle, stalled reads and blocked writes, and is joined on every exit.
NetHTTP depends on the pinned stdlib BufferedIO socket layout for that close.
Request preparation finishes before connecting, with an expiry check afterward.
The connection checks expiry and recomputes the wire timeout immediately before
dispatch, and rejects Net::HTTP's implicit reconnect of a closed socket. This
keeps every dispatched byte on the one socket owned by the watchdog.
TCP/TLS/read/write timeout arguments and watchdog waits are capped at exactly
`9_999_999.999` seconds, the Connect wire maximum, for safe native time conversion.
Individual native operations are independently bounded: an operation timeout
before logical expiry returns `unavailable`, while actual expiry returns
`deadline_exceeded`. Logical expiry remains unchanged; the watchdog re-evaluates
it after each capped wait and only closes the socket at actual expiry. Local
huge-budget calls verify conversions on the exercised plain TCP paths, not an
actual 115-day duration or runtime TLS behavior.

DNS/native resolution may outlast the budget. Net::HTTP's internal connection
implementation can itself use `Timeout.timeout` on Ruby 3.4; ConnectRPC never
calls it or asynchronously interrupts application code. TLS handshake timeout
uses the remaining budget after TCP/DNS. Peer/hostname verification stays on.
There is no pooling, proxy-env routing, retries, redirect following, automatic
decompression or compression negotiation beyond identity.

Server timeout validation creates a per-call monotonic deadline. Checks run
around input, decoding, handler execution and encoding. `cancelled?` means
expiry, and handlers can call `check!` cooperatively. No header means no server
expiry. Synchronous Rack cannot interrupt blocked input, arbitrary handlers or
codec work, or detect every disconnect. No hard server cancellation is claimed.

## Evidence and future direction

Specs exercise real custom bindings/transports/invokers, ProtoJSON mapping,
Rack::Lint success and errors, real localhost HTTP, byte limits and lifecycle.
The pinned connect-go Client and Handler verify both cross-language directions
and codecs against one shared descriptor fixture, including details,
host-visible metadata and a request limit. The same Go client scenarios also
run against the Go server as a reference baseline, with raw repeated Go fields
instead of WEBrick's coalesced values. There are 18 fixture cases: six reference
baseline cases and twelve cross-language cases. This is a unary subset, not
full conformance. The separate [official conformance harness](../conformance/README.md)
executes 94 configured client and 88 server cases. Two caller-cancellation
instructions remain executed expected failures under an exact-name limitation
manifest with strict causal result checks. There is no full-conformance claim.

Wire protocol, HTTP transport, and message binding remain separate choices.
Streaming, GET, gzip, gRPC/gRPC-Web, interceptors, a production server, Rails,
and Faraday are future work. Unsupported modes/options are rejected
rather than represented as buffered streaming. Optional Rails execution should
use the invoker boundary for callbacks, authentication, `rescue_from` and
instrumentation, not claim a mounted Rack app supplies the controller lifecycle.
See the [Rails controller guide](https://guides.rubyonrails.org/action_controller_overview.html).

A future standalone host will need verified HTTP/2 and duplex behavior. Start
with an existing stack; [Async::HTTP](https://github.com/socketry/async-http) is
a candidate, not a selection. Connect streaming carries end-stream metadata in
its body and does not require HTTP trailers; future gRPC has different
[trailer requirements](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
[Net::HTTP](https://docs.ruby-lang.org/en/3.4/Net/HTTP.html) provides HTTP/1.1
APIs, not proof of RPC streaming. Each future [Faraday adapter](https://lostisland.github.io/faraday/#/adapters/index)
needs capability verification.

The explicit optional `connectrpc/generator` entry point implements the standard
protoc request/response protocol through official `google/protobuf/plugin_pb`.
The extensionless executable reads raw protobuf to EOF, with no conformance
length framing. Malformed protobuf, missing/incompatible Google and CLI arguments
fail on stderr with nonzero status and empty stdout. Unsupported descriptors
return `CodeGeneratorResponse.error`, no partial files and exit zero. Unexpected
implementation exceptions remain visible. Only `FEATURE_PROTO3_OPTIONAL` is
advertised; editions are unsupported.

Each service-bearing target emits `_connect.rb`, requiring core, its official
`_pb.rb` and any additionally referenced message files. Imported streaming
services are not targets and do not cause streaming rejection. Generated modules
contain frozen RPC `Method` constants, `SERVICE`, a `Client` initialized with
`client:`, and `.handlers` with required callable RPC keywords. The client only
forwards `call(METHOD, request, **options)` and returns its result unchanged.
Partial handlers/custom invocation remain core Server features. No framework,
transport configuration, binding auto-require or static typing system is added.
Concrete types come from the official generated descriptor pool, preserving
nested classes, imports, proto2/proto3 presence and `ruby_package` mappings.

Naming follows protoc 36.2's
[RubifyConstant and PackageToModule](https://github.com/protocolbuffers/protobuf/blob/v36.2/src/google/protobuf/compiler/ruby/ruby_generator.cc):
message/service names preserve underscores and use `PB_` for a nonalphabetic
initial character; package underscores delimit capitalized words. An explicit
`ruby_package` containing `::` is used component-wise; otherwise dot/package
conversion applies. RPC names use the gRPC Ruby acronym and lower/digit-to-upper
underscore rule. Keyword/inherited wrapper names (including private copy/object
hooks and implicit conversions), leading-underscore RPCs,
`BEGIN`/`END`, reserved constants, unsafe paths, unresolved/non-message types,
duplicates and namespace/output collisions fail before emitting files.
All runtime constant references are absolute. Namespace checks index the request's
package/message/enum graph and target services, including Ruby aliases and
class/module conflicts. Fixed runtime protection applies to package ancestors,
official message/enum assignments and target service declarations in the loaded
import/type closure. Compatible `Google`/`Google::Protobuf` package modules may
be shared, including equivalent `ruby_package` mappings. Direct replacement of
those modules, reserved roots such as `ConnectRPC`/`File`, and concrete protobuf
runtime names such as `Google::Protobuf::DescriptorPool` are rejected. Known
official protobuf types are accepted only under their canonical file, proto name
and Ruby namespace, preserving WKT imports without allowing application types to
replace them. The fixed policy follows the pinned protobuf sources, rather than
reflecting the generator process's constants. A service cannot merge with
another package. No global registry spans generator invocations.
Generated files guard directly defined `SERVICE`, `Client` and RPC constants;
normal `require` is idempotent, while deliberate `load`/redefinition fails clearly.
Application constants outside the request still need compatible namespaces.

A Rust descriptor-driven
backend using [prost-reflect](https://docs.rs/prost-reflect/latest/prost_reflect/)
remains an option; cross-backend message-class compatibility is not promised.
No custom protobuf parser is implemented. See the
[Ruby generated-code guide](https://protobuf.dev/reference/ruby/ruby-generated/)
and [Rack specification](https://github.com/rack/rack/blob/v3.2.7/SPEC.rdoc).
