# Native gRPC

The [supported 0.x contract](support.md) defines the target compatibility
boundary and the remaining adoption gates.

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
literal; decoded invalid UTF8 is scrubbed. As a deliberate stricter policy than
grpc-go, details on OK, malformed or duplicate details, and mismatched rich codes
produce safe internal instead of silently dropping rich data.

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
custom invokers that have no handler-map entry.
