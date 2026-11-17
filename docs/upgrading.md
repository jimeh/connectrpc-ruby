# Upgrading

Each section covers one release that needs more than a version bump. Upgrade
every ConnectRPC gem to the same version, as the
[support policy](support.md#match-the-connectrpc-package-versions) requires.
The [changelog](../CHANGELOG.md) lists every change.

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
