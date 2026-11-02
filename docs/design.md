# Design direction

This records accepted future boundaries. The current gem is a bootstrap with
no protocol, hosting, codec, or transport implementation. Public names such as
Services, Channel, and Server remain sketches.

## Shared protocol and hosting

Shared Ruby protocol code will own dispatch, framing, negotiation, compression,
errors, metadata, limits, deadlines, and interceptors. It must remain independent
of HTTP hosting and concrete protobuf message classes. Wire selection
(Connect, gRPC, or gRPC-Web), HTTP implementation, and protobuf backend are
separate configuration choices.

Rack middleware is required. Optional Rails integration will map individual
RPCs to controller actions through a shared invocation boundary so callbacks,
authentication, `rescue_from`, and instrumentation participate. A mounted Rack
app alone does not provide that controller lifecycle. See the
[Rack specification](https://github.com/rack/rack/blob/main/SPEC.rdoc) and
[Rails controller guide](https://guides.rubyonrails.org/action_controller_overview.html).

A standalone server is required long-term for HTTP/2, HTTP trailers, and
bidirectional features. Start with an existing HTTP stack; Async::HTTP is a
candidate, not a selection. Connect streaming carries end-stream metadata in
its response body; it does not require HTTP trailers. Future gRPC support has
different trailer requirements. See the
[Connect protocol](https://connectrpc.com/docs/protocol/),
[gRPC HTTP/2 protocol](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md),
and [Async::HTTP](https://github.com/socketry/async-http).

## Messages and generated code

Codec and message binding will be configurable. The first product slice will
use google-protobuf's native binary and protobuf JSON support. Protobuf JSON
means the protobuf mapping, not arbitrary Ruby JSON serialization. A Rust,
descriptor-driven backend using prost-reflect is only an option. Changing
backends may change concrete message classes; cross-backend message-class
compatibility is not promised. Do not implement a new protobuf parser.

Eventually a protoc plugin will generate service metadata and stubs with Buf
compatibility. See the [Ruby generated-code guide](https://protobuf.dev/reference/ruby/ruby-generated/),
[protobuf JSON mapping](https://protobuf.dev/programming-guides/json/),
[prost-reflect](https://docs.rs/prost-reflect/latest/prost_reflect/), and
[Buf plugin model](https://buf.build/docs/generate/).

## Client HTTP transports

Client HTTP transports will be configurable. Net::HTTP over HTTP/1.1 can carry
unary RPCs and one-direction request or response streaming with a proper
implementation. It does not provide HTTP/2 or duplex exchange. Its body-stream
and response-chunk APIs do not by themselves prove an RPC implementation's
streaming behavior. See [Net::HTTP](https://docs.ruby-lang.org/en/master/Net/HTTP.html).

Faraday is optional; each adapter's capabilities require verification. Direct
Async::HTTP is a full-feature candidate, not a selected transport. Unsupported
protocol, transport, or streaming combinations must fail explicitly rather
than silently buffering. See [Faraday adapters](https://lostisland.github.io/faraday/#/adapters/index)
and [Async::HTTP](https://github.com/socketry/async-http).

## Proposed next PR

Implement a small end-to-end Connect unary slice: shared core, errors and
service metadata, configurable google-protobuf binary and protobuf JSON
binding, a Net::HTTP client, and Rack middleware. Verify real localhost HTTP
transport and an independent Connect interoperability fixture or peer. Cover
negotiation, errors, malformed input, unknown methods, content types, limits,
and deadlines in proportion to the actual slice.

Rails, a standalone server, streaming, other transports, and other wire
protocols are outside that slice. This bootstrap implements none of it.
