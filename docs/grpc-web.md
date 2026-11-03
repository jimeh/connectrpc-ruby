# gRPC-Web servers

Enable gRPC-Web explicitly on the existing server. Generated declarations,
message bindings, unary invokers and server-stream handlers remain protocol neutral.
There is no Ruby gRPC-Web client or new runtime dependency.

```ruby
binding = ConnectRPC::Bindings::GoogleProtobuf.new
cors = ConnectRPC::GRPCWebCORS.new(
  origins: ["https://app.example"], credentials: true,
  request_headers: %w[authorization x-csrf-token], expose_headers: ["x-request-id"]
)
server = ConnectRPC::Server.new(
  services: [service], handlers: handlers, binding: binding,
  protocol: %i[connect grpc grpc_web], cors: cors
)
```

`protocol: :grpc_web` selects Web alone. A protocol array is copied and frozen;
it must contain distinct `:connect`, `:grpc` or `:grpc_web` symbols. The default
is `:connect`. `:auto` remains exactly Connect and native gRPC. Both gRPC families
require `_GRPCStatusCodec`, supplied by the Google binding or a separate
`status_codec:` composed with a custom protobuf binding. Enabling Web on a server
does not change `Client(protocol: :connect | :grpc)` or its default transport.

When Connect is enabled, an actual GET selects Connect before observation,
native publication and Web CORS. Query controls override ignored POST content
headers, including a supplied Web Content-Type. Safe unary methods use the
existing Connect GET path; unsafe methods return 405 without handler dispatch.
A Web-only server still classifies Web GET as Web and rejects it with 405.

| Host | Unary binary/text | Server-stream binary/text wire | Incremental browser stream |
| --- | --- | --- | --- |
| Async native H2 | Yes | Yes | Text |
| Falcon native H2 and H1 | Yes | Yes | Text |
| Rack with rackup/WEBrick | Yes | Rejected before input | No |
| Native Rails controller | Yes | Rejected before callbacks | No |
| Testing manual encoded `TransportRequest` | Yes | Rejected before input | No |

Official grpc-web 2.1.1 browser clients support binary unary, text unary and
incremental text server streaming. Binary XHR buffers server-stream responses.
Binary server-stream wire interoperability uses independent connect-go. Client
streaming, bidi, JSON, per-message compression and whole-body compression are
outside this profile. Puma has not been verified. Browser TLS fixtures use an
explicit test-only verification exception; verified TLS/mTLS wire checks are
separate. Browser client-certificate selection is not promised.

## Media, framing and errors

Requests accept exactly `application/grpc-web[+proto]` or
`application/grpc-web-text[+proto]`, case-insensitively, without parameters.
Responses include `+proto`. POST is required. An independent bounded Accept
list supports these aliases, `application/*`, `*/*` and standard q values. The
most specific match determines each representation's quality. Highest quality
wins; the request representation wins ties. Responses vary on Accept.

Exactly one framed protobuf request and request EOF precede invocation. Empty
protobuf is a message; missing or excess requests are UNIMPLEMENTED. Excess
framing is detected without decoding a second message. Reserved flags, request
terminal, truncation, malformed protobuf/base64 and malformed Content-Length
are INVALID_ARGUMENT. Unnegotiated compressed DATA is INTERNAL. Unsupported
valid grpc-encoding tokens are UNIMPLEMENTED; malformed/repeated encoding is
INTERNAL. Web remains identity-only on a mixed server configured with gzip.

Before negotiation, 405/Allow, 415/Accept-Post and 406 are bare HTTP responses.
After negotiation, RPC errors use HTTP 200 and one 0x80 body terminal. The
terminal contains lower-case CRLF field lines, exactly one grpc-status and no
extra empty line. Repeated trailers remain separate wire lines; the official
JS runtime collapses duplicate trailer keys. Rich errors use the canonical
private Any/Status codec described in [native gRPC](grpc.md). All six grpc-timeout
units work; zero expires immediately and absence means no logical expiry.

## CORS and Rails

CORS is disabled by default. One owner controls each mount. `GRPCWebCORS` is an
immutable configuration value with exact serialized ASCII HTTP(S) origins.
It accepts no wildcard, regex, callback or null origin. At most 32 distinct
origins of 2 KiB each and 32 additional token header names of 128 bytes each are
accepted. Credential sharing defaults false. Explicit additions are required
for auth, CSRF and trace request headers.

Owned OPTIONS preflight returns empty 204 or 403 before RPC observation, body
reads, binding or application callbacks. Requested method must be POST and
requested names must be allowed. Max-Age is zero. Actual Web requests without
Origin are allowed; disallowed or ambiguous origins receive 403 before input.
Allowed-origin policy also decorates RPC errors, negotiation failures and native
admission 503. Vary merges Origin, Accept and preflight controls without replacing
application cache, cookie or security fields. Conflicting application
access-control fields fail closed before publication. Set `cors: nil` when outer
middleware already owns CORS. CORS does not authenticate or replace CSRF checks.

Native Rails overrides stay private:

```ruby
def connect_protocol = %i[connect grpc_web]
def connect_status_codec = connect_binding
def connect_cors = application_web_cors
def connect_limits = { grpc_status_wire_limit: 65_536, grpc_status_limit: 65_536 }
```

Explicitly route the intended POST and OPTIONS paths. The normal controller
invoker, callbacks, authentication, sessions, cookies, CSRF and security headers
remain active. Preflight does not bypass actual POST checks. A valid CSRF token
does not override Rails' origin policy. No CSRF exemption is installed.

## Bounds and host SPI

Message and independent message-wire limits default to 4 MiB. Framing adds five
bytes. Binary request entity cap is `5 + request_wire_limit`; text entity cap is
`4 * (5 + request_wire_limit)` because independently padded one-byte groups can
expand fourfold. Declared and actual encoded bytes, decoded framing and message
bytes are checked independently. Incremental decoding retains at most three
encoded carry characters plus a bounded decoded fragment. Whitespace, bad
alphabet, padding, pad bits and incomplete quartets are rejected.

`grpc_status_wire_limit` and `grpc_status_limit` default to 64 KiB. Terminal
serialization checks both actual CRLF bytes and name/value/32-byte field
accounting. Rich status has an independent decoded cap. Oversize status becomes
bounded RESOURCE_EXHAUSTED, or aborts if even the minimal terminal cannot fit.
Text terminal staging is separately bounded by base64 expansion. Portable unary
uses a finite aggregate response cap; native streaming never aggregates a whole
response. Native output retains one queued and one in-flight 16 KiB entity
fragment. Text uses raw slices of at most 12,288 bytes and flushes padded groups
at existing write boundaries, including the first small prefix.

Native H2 ingress remains 65,536 bytes/five entries with 16 KiB chunks and
consumption-based stream credit. H1 may retain one pinned backend chunk of up
to 64 KiB. Complete parsed field blocks are checked before application copies;
H2 includes pseudo/status fields. Parser/readahead, HPACK/CONTINUATION temporaries,
codec allocations, CPU and total RSS are outside these retention bounds.

Custom hosts call `Server#call_grpc_web(path:, http_method:, headers:, input:,
output:, publish:, capabilities: [:unary], lifecycle: nil, cancellation: nil)`.
Streaming requires explicit `:server_stream` capability. Input and publication
reuse `_StreamingInput` and `StreamHead`; `_GRPCWebOutput` provides bounded
`write(bytes, timeout:)`, `finish(terminal:)` and `abort`. Finish only stages
prepared bytes. Hosts must hold **all terminal bytes** until the original
processor and outer application cleanup succeed, then release those bytes.
Body consumers perform no handler, codec, status or base64 work. Cleanup failure
discards the candidate terminal and aborts delivery. Admission includes original
processing and wire/callback completion. H1 has explicit resources, not fake H2
streams. Only an actual H2 CANCEL reset signals Context cancellation. H1 EOF,
disconnect and host shutdown do not. There is no Connect one-second tail policy.

`Server#cors_response(path:, http_method:, headers:, response: nil)` is a pure
typed host SPI for early decisions and idempotent decoration, including admission
503. Call it before lifecycle start for preflight or denied requests.

The optional OTel adapter maps Web to the project's gRPC semconv 1.44 mapping,
with `rpc.system.name=grpc` and bounded `connectrpc.protocol=grpc_web`. Call.protocol
is fixed before subscriber start. Processing, admission, delivery and original
failures remain separate; SDK/exporter ownership and privacy are unchanged.
