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
unary POST, identity or opt-in gzip encoding, and `application/proto` or `application/json`.
JSON also accepts `charset=utf-8`. Clients send protocol version `1`; servers
accept an absent version and reject any other supplied value. A 200 response
must match the request codec. Non-application response types produce `unknown`;
mismatched application codecs and unsupported/unadvertised response encodings produce
`internal`. JSON errors are only parsed for an appropriate JSON content type.
Single identity/gzip content codings are case-insensitive; empty, repeated,
stacked and unknown content codings are rejected. Identity-only servers continue
to reject gzip. Client response preferences use distinct symbol arrays and
normalize identity to the last fallback. Server gzip capability negotiates
supported nonidentity accept tokens before identity, ignoring unknown/quality
value tokens. Absent accept inherits request coding; empty accept selects identity.
Repeated accept fields combine in field order. Gzip-selected empty serialization
produces a valid nonempty member; zero-length HTTP content bypasses inflation
after header validation. Every concatenated member validates CRC/ISIZE/completion;
truncation and trailing data fail under an aggregate decoded cap.

Other verbs get bare 405 with `Allow: POST`; unsupported types, including
Connect streaming and gRPC variants, get bare 415 with `Accept-Post`.
Unsupported request encoding returns `unimplemented` listing supported codings. Invalid
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

Default serialized/decompressed request/response limits are 4 MiB each.
Independent wire limits default to the corresponding message limits, preserving
both bounds for existing callers. `TransportRequest.response_limit` is the
encoded response read cap. Declared lengths are validated against wire bytes and
oversized lengths rejected before reading. Unknown-length input stops at the
first excess byte. Identity satisfies both caps. Outgoing serialization is checked
before compression; compressed output, including framing/footer, must fit its
wire cap without fallback. Explicitly larger wire limits permit small-message
expansion without increasing message bounds.

Gzip uses default Zlib gzip-only streams with bounded input and block output,
checking size before every append. At most one 16 KiB native output callback can
exceed retained payload, plus native state. Every member/chunk checks cooperative
expiry; streams close on every outcome. Wire and decoded buffers coexist, and
native protobuf/JSON serialization can allocate before its output check. Custom
transports may allocate before returning; core revalidation cannot undo that.
There is no total allocation, hard CPU or independent header-size guarantee.

Server errors remain identity and use the smaller response cap, including the
safe replacement and empty-429 fallback. Valid gzip peer errors are decoded
before JSON parsing. Malformed gzip or JSON on non-200 uses HTTP fallback with
valid metadata/trailers; malformed gzip 200 is internal. Header rejection stays
internal, local size excess stays resource_exhausted, and actual expiry stays
deadline_exceeded. No generic error-body rescue changes those classifications.

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
decompression. NetHTTP constructs requests with the actual explicit accept
advertisement so the default HTTP gem never transparently inflates or rewrites
content encoding/length; transports carry opaque encoded bytes.

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
manifest with strict causal result checks. A separate gzip inventory adds
88 client and 82 server cases. Its two caller-cancellation failures remain
executed expected failures under a separate exact-name manifest with strict
causal and wire checks; all other cases pass. Separate Go gzip scenarios
exercise both directions/codecs, negotiation, errors and bounds with wire
observations. The pinned Go client rewrites accept preferences, so independent
raw Go HTTP requests additionally prove gzip-request/identity-response behavior.
The pinned Go server prefers request gzip over explicit identity acceptance;
Ruby clients reject that unadvertised response. There is no full-conformance claim.

Wire protocol, HTTP transport, and message binding remain separate choices.
Streaming, GET, gRPC/gRPC-Web, interceptors, a production server,
and Faraday are future work. Unsupported modes/options are rejected
rather than represented as buffered streaming. The optional [connectrpc-rails gem](../gems/connectrpc-rails/README.md) uses
the invoker boundary to run native controller callbacks, authentication,
`rescue_from` and instrumentation after bounded core decode. It reuses Rack
translation and keeps hosting dependencies out of core. It preserves native
CSRF/session policy, reports unexpected errors through Rails.error and keeps
generated files non-reloadable outside Zeitwerk paths. A mounted Rack app alone
does not supply the Rails controller lifecycle.
See the [Rails controller guide](https://guides.rubyonrails.org/action_controller_overview.html).

The optional [connectrpc-async package](../gems/connectrpc-async/README.md)
selects Async HTTP/2 for a standalone unary host and matching pooled transport.
It uses the existing transport/server seams, generated definitions and Google
binding without changing core Context or wire contracts. The application owns
the reactor and close lifecycle. Private, version-bounded wrappers own writes,
stream cancellation, request tasks and wire-complete drain; no frame/HPACK
engine or global monkeypatch is added. Real h2c and verified TLS `h2` are
required. Full TLS snapshots enable required client authentication, same-listener
credential reload and older-generation retirement. The exact-task client leaf
accessor is the optional authorization seam; peer-presented chains are not
claimed to be verified chains. See the package guide for session isolation,
cleanup's 100ms allowance and deliberate whole-connection failure for a stuck
frame. No streaming or production daemon claim is made.

Connect streaming carries end-stream metadata in its body; future gRPC has
different [trailer requirements](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
The Async backend preserves leading repeated fields and leading `trailer-*`
metadata while ignoring actual trailing HEADERS after backend validation.
It reconstructs lengths normalized by the backend; malformed lengths and some
singleton repeats can fail before core. Conservative frame/window settings do
not provide a hard process-memory/header-size guarantee. Independent connect-go
unary h2c/TLS/mTLS results are separate from core's identity and gzip conformance
profiles. Combined gzip specs exercise opaque bytes and wire/decompressed bounds
over the actual Async host and transport. NetHTTP remains HTTP/1.1; each future
transport needs its own verified capability scope.

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
