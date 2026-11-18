# connectrpc-hyper

Experimental, optional client transport for
[connectrpc](https://github.com/jimeh/connectrpc-ruby) on Rust's hyper, h2,
rustls (aws-lc-rs) and tokio. Any Ruby thread can make Connect and native gRPC
calls of all four shapes, over HTTP/2 (and Connect over HTTP/1.1), without an
Async reactor. Since 0.3.0.

```ruby
require "connectrpc/hyper"

transport = ConnectRPC::Hyper::Transport.new(
  connections: :pooled,   # or :per_call
  idle_timeout: 1800,     # pooled connections; nil disables
  keepalive: nil,         # ConnectRPC::Keepalive.new(time: 60)
  http2: true,            # false: Connect over HTTP/1.1, unary/server/client streaming
  connect_timeout: 20,
  ca_file: nil, certificate_chain: nil, private_key: nil
)
client = ConnectRPC::Client.new(base_url: "https://api.example.com", transport: transport, protocol: :grpc)
transport.close(grace: 5) # => true when in-flight calls drained
```

## Which transport

- Core `ConnectRPC::Transports::NetHTTP`: simple Connect unary, no
  dependencies.
- `ConnectRPC::Hyper::Transport`: any thread (Puma, Sidekiq, scripts), pooled
  HTTP/2, every call shape and native gRPC.
- connectrpc-async's `Transport` and `ThreadedTransport`: applications already
  on Async, or that need its connection policy and observers.
- Inside an Async reactor, a hyper call blocks the reactor thread unless Ruby
  4.0 and async's worker pool (`ASYNC_SCHEDULER_WORKER_POOL=true`) offload it.

## Behaviour

- One admitting HTTP/2 connection per origin and transport. A connection that
  received GOAWAY drains its streams while the next call dials a new one;
  concurrent first callers share one dial. HTTP/1.1 keeps at most two idle
  connections per origin, and only after a clean request and response.
- `Thread#raise`, `Thread#kill`, `Timeout`, deadlines and cancellation tokens
  stop a call at DNS, connect, TLS, a blocked write and a blocked read; the
  stream is reset with `CANCEL`.
- `connect_timeout` bounds DNS, TCP, TLS and the HTTP/2 preface. hyper exposes
  no peer-SETTINGS boundary, so a peer that accepts and stays silent is bounded
  by the call deadline (and keepalive when set). A stalled `getaddrinfo` keeps
  its resolver thread after the caller leaves.
- Keepalive maps to hyper's: its idle clock resets on response HEADERS, on DATA
  and trailers as Ruby consumes them, and on PING ACKs, so it may ping earlier
  than grpc-go; DATA buffered while the peer went silent moves it later once
  consumed. Without `permit_without_calls` an idle connection is never probed.
- A request hyper hands back unsent, and a Connect unary call refused with
  `REFUSED_STREAM` before response headers, are retried once on a fresh
  connection within the original deadline. Nothing else is replayed.
- TLS: native roots (or `SSL_CERT_FILE`/`SSL_CERT_DIR`, read once per process)
  plus appended `ca_file` certificates, TLS 1.2 minimum, no resumption.
  rustls rejects CN-only and X.509 v1 server certificates, single-label
  wildcards, a CA certificate used as the server certificate and encrypted
  private keys.
- gRPC response fields are limited to core's 65,536 bytes; h2 accepts blocks up
  to 16 MiB, so a hostile block above that can still truncate trailers.
- Fork: a child builds its own runtime and connections, closes (never shuts
  down) inherited connection sockets, and never writes to the parent's. A
  transport closed before fork stays closed.

## Platforms

Platform gems need glibc 2.29+ (x86_64/aarch64 Linux), musl with `libgcc_s`,
or macOS 11+. The source gem needs Rust 1.85+, libclang and a C compiler.

## License

MIT; see `THIRD_PARTY_NOTICES.md` for the linked Rust crates.
