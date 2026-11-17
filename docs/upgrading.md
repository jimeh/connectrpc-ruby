# Upgrading

Each section covers one release that needs more than a version bump. Upgrade
every ConnectRPC gem to the same version, as the
[support policy](support.md#match-the-connectrpc-package-versions) requires.
The [changelog](../CHANGELOG.md) lists every change.

## 0.3.0

### Breaking change: Steepfiles name core explicitly

Only applications that type-check with Steep or RBS are affected. The optional
gems' signature manifests no longer name connectrpc gems, because
`rbs collection` locks every manifest dependency as a standard library and then
fails to load the whole environment. Without `rbs collection`, an optional
library therefore no longer loads core's signatures. Add `library "connectrpc"`
beside each optional connectrpc library, and `library "connectrpc-async"` beside
`library "connectrpc-falcon"`:

```ruby
target :app do
  library "connectrpc"
  library "connectrpc-async" # Required by connectrpc-falcon.
  library "connectrpc-falcon"
  check "app"
end
```

Projects that use `rbs collection` now install every connectrpc package as a
gem in any Gemfile order and need no other change. Runtime behavior, generated
code and signature declarations are unchanged. See
[package APIs](rbs.md#package-apis-and-extension-contracts).

### Behavior changes

These affect the Async HTTP/2 and HTTP/1 client transports and the core
`Net::HTTP` transport. Error codes are unchanged.

- An enclosing Async `with_timeout` around a client call or stream operation
  now ends it with that same `Async::TimeoutError`. Before 0.3.0 the call ran
  on under its RPC deadline, or failed as `unavailable` with a nil deadline.
  Rescue `Async::TimeoutError` where you previously rescued
  `ConnectRPC::Error` for this case. An expired RPC deadline or cancellation
  still raises `deadline_exceeded` or `canceled`.
- Client transport failures that a fixed phrase can classify now append it
  to the message, such as `HTTP/2 transport failed: connection refused` or
  `HTTP transport failed: TLS verification failed`. They also keep the
  original exception as `cause`; before 0.3.0 it was nil. Code that matches
  the exact message should match its prefix instead. Codec, compression and
  metadata errors still have no `cause`.
- Admission failures say `... transport admission is closed` once a transport
  is closing and `... transport admission is full` at its count or byte limit,
  instead of `... admission is closed or full`.
- An invalid Async client TLS input raises `ArgumentError` naming it, such as
  `Invalid TLS client configuration: ca_file`.
- An Async client transport that is never closed is aborted once its owner
  task's block and every other task under that owner have finished. Before
  0.3.0 it kept the owner, and with it `Sync`, from finishing. Close
  transports explicitly for a graceful drain.
- `HTTP1Transport` sends a unary GET once more when its reused keep-alive
  connection closed before any response head arrived.

These affect `connectrpc-otel` spans and trace propagation.

- `connectrpc-otel` uses the application's global propagator,
  `OpenTelemetry.propagation`, when `Subscriber.new` gets no `propagator:`,
  instead of a fixed W3C trace-context plus baggage propagator.
  `OpenTelemetry::SDK.configure` installs trace context plus baggage unless
  `OTEL_PROPAGATORS` says otherwise, so SDK applications keep W3C propagation
  and now also honor settings such as `b3`. An application that never sets a
  global propagator, such as one with only the API installed, now propagates
  nothing. Pass `propagator:` to keep the old behavior. See
  [what the application owns](../gems/connectrpc-otel/README.md#what-the-application-owns).
- An OpenTelemetry SERVER span whose response delivery aborted with a non-OK
  code now follows the server error policy, as a delivered response does. A
  client cancellation leaves the span UNSET with `rpc.response.status_code`
  `canceled` (`CANCELLED` for gRPC) and no `error.type`; it was ERROR with
  `error.type=delivery_aborted` (or `CANCELLED`). A Connect CLIENT span whose
  delivery aborted with a non-OK code reports that code as `error.type`
  instead of `delivery_aborted`. An abort after `OK` or without a code is
  unchanged. Update server error-rate dashboards and alerts that counted
  client cancellations.
- `error.type` for a failure from a non-Connect exception without an RPC code
  is the exception's class name, such as `RuntimeError`, instead of
  `operation_failed`. Anonymous exception classes still report
  `operation_failed`.

This affects the Google protobuf binding.

- The Google protobuf binding rejects a zero-length JSON payload, as connect-go
  does, instead of decoding it as an empty message. A server answers
  `invalid_argument` and a client raises `internal`. Send the message's JSON
  form instead: `{}` for an empty ordinary message, or a well-known type's own
  form, such as `""` for an empty `StringValue`. Binary protobuf still accepts
  an empty payload. See
  [the Google binding](unary.md#declare-a-method-without-generation).

These affect the Async `Host` and Falcon's native listener. See
[Connection capacity](../gems/connectrpc-async/README.md#connection-capacity)
and [Accept errors](../gems/connectrpc-async/README.md#accept-errors).

- `max_connections` defaults to 1024 instead of 128 on `Host`,
  `ServerSession` and `ConnectRPC::Falcon::Server`. Set it explicitly to keep
  the old cap, and check that the process's open-file limit leaves room for it.
- A host at `max_connections` no longer accepts and closes new peers. They wait
  in the kernel's listen backlog until a connection retires, and a full Falcon
  worker leaves them for its siblings. A client now sees a slow connection
  instead of a reset. The kernel completes the TCP handshake, so the wait is
  bounded by whatever covers the client's next step: the Async `Transport`'s
  `connect_timeout` covers TLS and the HTTP/2 SETTINGS exchange, while a
  plaintext HTTP/1.1 client needs a request or read timeout. A custom host
  that calls `ServerSession#accept(raw)` itself still has peers above the cap
  closed.
- `EMFILE`, `ENFILE`, `ENOBUFS` and `ENOMEM` from `accept` retry indefinitely
  with a delay from 5 ms doubling to 1 second and a warning per retry, instead
  of stopping the Host after six failures. `ECONNABORTED` and `EINTR` retry at
  once. Falcon's native listener used to end its accept loop on these errors
  and now retries the same way. Watch the warnings, or the open-file count,
  instead of relying on `Host#wait` to raise.
- A TLS identity that expires while it is published keeps being served, and
  the host reports `on_tls_error` category `:expired` once per generation (a
  warning without a reporter). New handshakes used to be closed before the
  ClientHello. Verifying clients now fail with a certificate-expired error.
  Handle `:expired` in reporters that switch on the category, and use the new
  `tls_status[:identity_expires_at]` to alert before expiry.
- Calling `close` or `refresh_tls` from `on_tls_error` during the `:expired`
  report raises `LifecycleError`, as it already did during a provider refresh
  report. `reload_tls` and `drain_tls` work there, so a static-TLS reporter can
  publish a replacement identity.
- Host listeners and listeners from `ConnectRPC::Falcon::Endpoint#bound` are
  close-on-exec, so exec'd child processes no longer inherit them. Pass
  `close_on_exec: false` to `Endpoint#bound` to keep the old behavior.
- `Host.new(port: 0)` binds every resolved address on one port, so
  `host.port` is the port of every entry in `host.addresses`.

## 0.2.0

### Breaking change: read-only transport cancellation

Only custom transports are affected. `TransportRequest#cancellation` and
`StreamRequest#cancellation` are now a read-only `CallCancellation::View`
instead of the call's mutable `ConnectRPC::CallCancellation`. The view exposes
`cancelled?`, `reason`, `check!` and `on_cancel`, which is the whole typed
`_Cancellation` SPI, so that SPI is unchanged. A transport that called
`commit!` or `close` on `request.cancellation` must stop; core completes the
call itself. `CallCancellation.with_request` passes the view through unchanged.
See [explicit cancellation ownership](design.md#explicit-cancellation-ownership).

Generated code, message encoding and protocol defaults are unchanged, and code
does not need to be regenerated.

### Behavior changes

- A server handler that returns or sends a nil response message now fails the
  call with `internal` and the message "Nil response message" on every
  protocol. Server streaming, client streaming, native gRPC and gRPC-Web unary
  previously reported `invalid_argument`, and Connect unary depended on the
  binding. A client that sends nil still gets `invalid_argument`.
- An error that core normalizes from a rescued codec, compression or header
  exception has no Ruby `cause`. The Rails controller does the same for
  exceptions that escape it. Lifecycle observers still receive the original
  failure, so read it there instead of from `error.cause`.
- A custom binding whose `decode` raises a non-codec exception, such as
  `ArgumentError`, now propagates that exception to the caller unchanged.
  Unary clients previously reported it as `internal` "Malformed response
  metadata".
- Core calls a streaming exchange's `close_send` at most once, so custom
  streaming transports no longer see a second call.
- The generator rejects a package whose root constant collides with a standard
  library constant that core loads, such as `Net`, `URI`, `OpenSSL`, `Socket`
  or `Zlib`. Output for other schemas is unchanged.
- The Async `Host` and Falcon's native HTTP/2 path enable server keepalive by
  default (`ServerKeepalive.new`). The server sends a PING after 2 hours
  without reading a frame, and sends GOAWAY `too_many_pings` to a client whose
  PINGs arrive less than 5 seconds apart three times. `idle_timeout` now drains
  an idle HTTP/2 connection with GOAWAY instead of aborting it. Pass
  `keepalive: nil` to turn off server PINGs and enforcement. See
  [Server keepalive](../gems/connectrpc-async/README.md#server-keepalive).
- The OpenTelemetry adapter writes local `error.type` classifications in lower
  case on every protocol, such as `operation_failed` and `preflight_rejected`.
  Before 0.2.0, gRPC and gRPC-Web spans reported them in upper case
  (`OPERATION_FAILED`). Only gRPC and gRPC-Web status codes are upper case.
  Update dashboards and alerts that match the old values.
- When an instrumentation subscriber injects `baggage`, the client now appends
  it after the caller's `baggage` metadata instead of replacing it, so the
  request carries both. Other injected keys still replace the caller's values.
- `require "connectrpc/async"` and `require "connectrpc/falcon"` check the
  upstream Async hooks they depend on. When one has changed, the require
  raises an error that lists each changed hook with its installed gem version
  and the versions the package was tested with, instead of failing later at
  runtime. Pin the socketry gems to tested versions until a ConnectRPC release
  supports the new ones.
