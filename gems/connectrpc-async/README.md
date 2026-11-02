# ConnectRPC Async

Optional HTTP/2 unary hosting and pooled transport. Require `connectrpc/async`
explicitly. Core, Rails and Rack remain independent. This experimental package
supports Connect unary identity, protobuf and ProtoJSON, h2c prior knowledge
and TLS with verified peers and ALPN `h2`.

The example assumes existing core `server`, `binding`, `method` and `message`
objects, as shown in the repository unary examples.

```ruby
require "async"
require "connectrpc/async"

Async do
  host = ConnectRPC::Async::Host.new(server: server).start
  origin = "http://127.0.0.1:#{host.port}"
  transport = ConnectRPC::Async::Transport.new(origin: origin)
  client = ConnectRPC::Client.new(base_url: origin, binding: binding, transport: transport)
  begin
    client.call(method, message)
  ensure
    transport.close
    host.close
  end
end.wait
```

The application owns a long-lived reactor task. Construction, networking and
lifecycle operations must use its reactor and thread. There is no hidden
synchronous reactor thread. Scheduler-compatible I/O yields; CPU, native codecs
and native DNS may block. Ensure graceful close before the owner exits. Owner
cancellation aborts only owned sockets/tasks and makes no delivery promise.

`Host.new(server:, host: "127.0.0.1", port: 0, tls: nil, max_connections: 128,
max_requests: 128, handshake_timeout: 10, idle_timeout: 60)` returns a host.
`start` binds/listens before returning self; `port` and `addresses` expose the
actual binding. States are new/running/closing/closed; hosts cannot restart.
Connection admission happens before TLS/preface. Request admission fails with
bare 503. Setup has one combined cooperative timeout. Idle expiry runs only
without streams or handlers and can be disabled with nil.

`Transport.new(origin:, ca_file: nil, certificate_chain: nil, private_key: nil,
connection_limit: 1, max_requests: 128, connect_timeout: 10)` implements core's
transport seam. Origins contain only scheme/host/port. Request paths, including
core's base prefix, remain intact. One connection multiplexes requests. Full
admission fails with unavailable; pool waits honor finite RPC deadlines. Nil
RPC deadlines remain unbounded, while actual setup still has its own timeout.
No retries, redirects, proxy environment routing, compression or HTTP/1 fallback.
A future separate call can open a fresh connection after failure/GOAWAY.

Both objects provide `close(grace: 5)`: stop admission, await handlers/calls and
complete wire streams, send GOAWAY when empty, then half-close and await EOF.
Returns true when drained, false after forced teardown. Grace is finite and
nonnegative; zero aborts. Repeated/concurrent calls share the first outcome.
Finite deadlines begin in core before encoding and cover pool/setup/headers/body.
Timers use at most 86400-second waits without shortening logical deadlines.
Cleanup uses one total internal 100ms allowance. Ordinary cancellation resets
its stream. A frame that cannot finish within cleanup deliberately fails the
whole connection and siblings. Forced teardown closes raw TCP before cancelling
and joining writers/readers, then performs buffered/TLS cleanup. These are
cooperative bounds, not arbitrary native/application interruption guarantees.

## TLS and client authentication

Applications load their own files. Host TLS is a complete hash:

```ruby
tls = {
  certificate_chain: [File.read("leaf.pem"), File.read("intermediate.pem")],
  private_key: File.read("key.pem"),
  client_ca: nil # Or a nonempty array of explicit trust-anchor PEM certificates.
}
```

Each certificate element contains exactly one PEM certificate, leaf first.
Keys are unencrypted PEM private keys. Inputs are copied and parsed without
password prompts; key possession, leaf validity and adjacent chain signatures
are checked. Snapshots retain private parsed objects, redact inspection/errors,
and compile fresh contexts before publication. Explicit intermediates, including
an empty array, prevent auto-chaining through the client-auth trust store.
Both roles require TLS >=1.2 and post-check ALPN `h2`. Clients always verify
peer/hostname, including localhost, using system roots plus optional `ca_file`.
Client chain/key must appear together; TLS options are invalid for h2c.

A nonempty `client_ca` requires verified client certificates using only those
anchors, with partial-chain trust enabled deliberately. A scoped intermediate
can be the boundary; sibling issuers under its root are not trusted. Nil means
server-only TLS and no client authentication. There is no optional-client mode.
`host.peer_certificate` returns a detached authenticated client leaf only within
that host's exact handler task; outside/child/other-host/plain/server-only TLS
returns nil. It does not expose or claim a verified chain and does not change
core Context. Applications own authorization based on that identity.

`host.reload_tls(certificate_chain:, private_key:, client_ca:)` replaces the
complete snapshot for newly accepted sockets on the same listener. Generation
starts at 1 and increments only after successful validation; invalid candidates
leave the last good snapshot intact. Existing connections retain their prior
identity and authentication. Every generation has a fresh context/session ID,
OP_NO_TICKET and SESSION_CACHE_OFF. Reuse is disabled; this does not claim that
OpenSSL emits zero TLS 1.3 tickets. Replace a transport to change client identity.

`host.drain_tls(grace: 5)` captures the current generation cutoff and retires
only older accepted connections, including pending handshakes. Old later streams
receive 503 while admitted streams finish. Pending handshakes abort immediately.
A persistent retirement floor prevents old pending sockets from becoming live.
The listener and current/newer generations survive concurrent reloads/drains.
Close supersedes drains. Renewal alone does not instantly revoke existing peers.
There are no file watchers, ACME, secret services or authentication policy DSLs.

## Backend boundaries and verification

Core owns codecs, errors, metadata and serialized byte limits. The backend
reads bounded chunks, reconstructs declared length from backend body.length,
preserves raw repeated leading fields and leading `trailer-*`, and ignores real
HTTP/2 trailing HEADERS for core metadata. The stack normalizes lengths using
Ruby Integer: `+10`, hexadecimal and underscores can normalize before core;
malformed/duplicate lengths may reset/close connections, and singleton header
policies can produce bare errors before core. These outcomes are tested and
are not a full protocol conformance claim. Settings use 16KiB frames, 64KiB
stream windows and bounded stream admission; they do not bound total process
memory or HPACK/header-continuation buffering.

Discover `async:setup`, `async:test`, `async:check`, `async:build`, `async:package`,
`async:example`, `async:interop`, and `async:verify` with `mise tasks`. The separate
checksummed bundle uses `vendor/async` and coverage uses `coverage/async`.
Independent connect-go tests exercise both directions/codecs over h2c/TLS/mTLS,
with observed HTTP/2/ALPN and large typed wire-tail drains. Original core fixture
and official conformance harnesses remain separate. Installed package checks use
normal offline dependencies and a fresh GEM_HOME outside the checkout. This
graph activates OpenSSL 4.0.2/JSON 3.1.0; installed core NetHTTP TLS/JSON is also
exercised under that activation. Nothing is published. Licensing is undecided
and the invalid `https://gems.invalid` push guard remains in both archives.
