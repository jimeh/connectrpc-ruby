# Runnable examples

Start the server of your choice in one terminal and call it from the client of
your choice in another. Each server and client is one short file that shows how
to host or call ConnectRPC that way. They all share one
[demo schema](proto/demo/v1/demo.proto), so any client can call any server that
supports the call.

## Where to read

These files show how to use ConnectRPC:

| File | Shows |
| --- | --- |
| [servers/](servers/) | One file per host: Rack, Rails, Async and Falcon |
| [clients/](clients/) | One file per transport: NetHTTP, Async HTTP/1.1 and Async HTTP/2 |
| [demo/handlers.rb](demo/handlers.rb) | The service implementation, one handler per RPC shape, and the `ConnectRPC::Server` the hosts share |
| [demo/calls.rb](demo/calls.rb) | Calling each RPC shape through the generated client |
| [demo/call_log.rb](demo/call_log.rb) | An instrumentation subscriber that logs each RPC on the server |
| [demo/mtls.rb](demo/mtls.rb) | The TLS options a host and a client need for [mutual TLS](#mutual-tls) |
| [proto/demo/v1/demo.proto](proto/demo/v1/demo.proto) | The schema; [gen/](gen/) holds its generated Ruby |

The rest is demo plumbing that you can skip: [demo/cli.rb](demo/cli.rb) parses
client arguments, [demo/support.rb](demo/support.rb) holds the support table and
state file, [demo/launch.rb](demo/launch.rb) handles the port, `--mtls` and
Ctrl-C, [demo/certs.rb](demo/certs.rb) issues the demo certificates, and
`scripts/example.rb` picks each file's bundle.

## Quick start

Each server and client runs in the bundle of the package it uses. Install the
bundles once before the first run. This pair needs the root bundle, the Falcon
bundle for the server and the Async bundle for the client:

```sh
mise run setup
mise run falcon:setup
mise run async:setup
```

Start the server in one terminal and call it from another:

```sh
# Terminal 1
mise run example:server -- falcon

# Terminal 2
mise run example:client -- async-h2 chat Ada Grace
```

The server prints its URL and serves until Ctrl-C:

```text
falcon server listening on http://127.0.0.1:8080 (Ctrl-C to stop)
```

The client sends a random `demo-request-id` as request metadata, then prints
the messages, this demo's `demo-*` response headers and trailers, or the RPC
error:

```text
async-h2 -> falcon at http://127.0.0.1:8080 (connect, proto, identity)
request: demo-request-id=5c1f9a2e
message: Hello, Ada!
message: Hello, Grace!
headers: demo-served-by=falcon
```

The server prints one line per RPC with the same ID, showing what arrived (see
[Server call log](#server-call-log)):

```text
served Chat request-id=5c1f9a2e protocol=connect http=POST codec=proto encoding=identity code=OK
```

`mise run example:client -- --list` prints every server, client, RPC and option.
If a bundle is missing, the launcher prints the setup command to run.

## Servers

Each server binds `127.0.0.1:8080`. Set `PORT` to use another port; `PORT=0`
picks a free one and the ready line shows it. The `async` and `falcon` servers
also accept `--mtls`; see [Mutual TLS](#mutual-tls).

| Name | File | Setup task | Hosting | Serves |
| --- | --- | --- | --- | --- |
| `rack` | [servers/rack.rb](servers/rack.rb) | `rack:setup` | `ConnectRPC::Rack` on WEBrick | HTTP/1.1: Connect unary |
| `rails` | [servers/rails.rb](servers/rails.rb) | `setup:rails` | Single-file Rails app, controller includes `ConnectRPC::Rails::Controller` | HTTP/1.1: Connect unary |
| `async` | [servers/async.rb](servers/async.rb) | `async:setup` | `ConnectRPC::Async::Host` in an application-owned reactor | HTTP/2: Connect and gRPC, all four shapes |
| `falcon` | [servers/falcon.rb](servers/falcon.rb) | `falcon:setup` | `ConnectRPC::Falcon::Server` on a pre-bound endpoint | HTTP/1.1: Connect unary, server and client streaming. HTTP/2: Connect and gRPC, all four shapes |

The Rails server runs in the lane selected by `RAILS_LANE` (`8.1` by default, or
`8.0`). The Falcon file runs one in-process server so it can bind port 0; real
deployments usually use `falcon host` with the service DSL, as in
[the Falcon configuration example](../gems/connectrpc-falcon/examples/falcon.rb).

## Clients

| Name | File | Setup task | Transport | Calls |
| --- | --- | --- | --- | --- |
| `nethttp` | [clients/nethttp.rb](clients/nethttp.rb) | `setup` | Core default NetHTTP | HTTP/1.1: Connect unary, POST or `--get` |
| `async-http1` | [clients/async_http1.rb](clients/async_http1.rb) | `async:setup` | `ConnectRPC::Async::HTTP1Transport` | HTTP/1.1: Connect unary, server and client streaming |
| `async-h2` | [clients/async_h2.rb](clients/async_h2.rb) | `async:setup` | `ConnectRPC::Async::Transport` (h2c prior knowledge) | HTTP/2: Connect and gRPC, all four shapes |

## Supported pairings

U is unary, SS server streaming, CS client streaming and bidi bidirectional
streaming. The [support table](demo/support.rb) mirrors the
[protocol and host matrix](../docs/support.md#protocol-and-host-matrix).

| Client | `rack` | `rails` | `async` | `falcon` |
| --- | --- | --- | --- | --- |
| `nethttp` | Connect U | Connect U | Refused: H2-only host | Connect U |
| `async-http1` | Connect U | Connect U | Refused: H2-only host | Connect U/SS/CS |
| `async-h2` | Refused: H1-only host | Refused: H1-only host | Connect and gRPC U/SS/CS/bidi | Connect and gRPC U/SS/CS/bidi |

A client checks its pairing before it opens a connection. An unsupported
pairing exits with status 2 and says why, which clients or servers would work,
and where to read more:

```text
$ mise run example:client -- async-http1 chat
Refused: async-http1 supports only unary, server streaming, client streaming Connect calls; chat is bidi streaming.
Clients that can call chat on falcon: async-h2
See examples/README.md for the supported pairings.
```

Anything the table allows reaches the library. If the library rejects it
anyway, the client prints that error rather than hiding it.

## Mutual TLS

With mutual TLS (mTLS), each side presents a certificate and verifies the
other's. Start the `async` or `falcon` server with `--mtls` to serve TLS and
require a client certificate, then call it as usual:

```sh
# Terminal 1
mise run example:server -- falcon --mtls

# Terminal 2
mise run example:client -- async-h2 greet Ada
```

The first `--mtls` server issues a throwaway demo CA, a server certificate for
`localhost` and `127.0.0.1`, and a client certificate for `demo-client`. It
writes them to `vendor/examples/tls/`, next to the state file, and later servers
reuse them until a day before they expire after 7 days. Renewing them replaces
the CA, so restart any older `--mtls` server that is still running. They exist
only for these examples.

```text
Issued demo certificates in vendor/examples/tls
falcon server listening on https://127.0.0.1:8080 (Ctrl-C to stop)
```

The client sees the `https://` URL in the state file, so it trusts the demo CA
and presents the client certificate. The handler gets the certificate that the
server verified and returns its subject in a `demo-client` header:

```text
async-h2 -> falcon at https://127.0.0.1:8080 (connect, proto, identity, mTLS)
request: demo-request-id=5c1f9a2e
headers: demo-served-by=falcon demo-client=CN=demo-client
message: Hello, Ada!
trailers: demo-greeted=Ada
```

Three pieces configure it:

- The server passes a `tls:` hash to `ConnectRPC::Async::Host` or
  `ConnectRPC::Falcon::Server`, built by `MTLS.host_tls` in
  [demo/mtls.rb](demo/mtls.rb). The hash holds its certificate chain, its
  private key, and `client_ca`, the CAs it accepts client certificates from. A
  non-empty `client_ca` makes the client certificate mandatory.
- The client passes `ca_file:`, `certificate_chain:` and `private_key:` to
  `ConnectRPC::Async::Transport` or `HTTP1Transport`, built by
  `MTLS.client_tls`. The client always verifies the server's certificate and
  hostname.
- Inside a handler, `host.peer_certificate` returns the client certificate that
  the host verified for the call. [servers/async.rb](servers/async.rb) and
  [servers/falcon.rb](servers/falcon.rb) pass it to the handlers. A real
  application would authorize the call from it.

`async-h2` can call either mTLS server, and `async-http1` can call `falcon` over
HTTP/1.1 with TLS. `nethttp` is refused because the core NetHTTP transport has no
client certificate option, and `rack` and `rails` refuse `--mtls` because they
have no qualified TLS profile.

To watch the server enforce the client certificate, call it with curl:

```sh
curl --cacert vendor/examples/tls/ca.pem \
  --cert vendor/examples/tls/client.pem --key vendor/examples/tls/client-key.pem \
  -H 'content-type: application/json' -d '{"name":"Ada"}' \
  https://127.0.0.1:8080/demo.v1.DemoService/Greet
```

This prints `{"greeting":"Hello, Ada!"}`. Without `--cert` and `--key`, the
server ends the handshake with a `certificate required` alert. Without
`--cacert`, curl cannot verify the server's certificate and refuses to connect.

Production servers load certificates from their own CA, and both hosts can
reload them without a restart. See
[TLS and client authentication](../gems/connectrpc-async/README.md#tls-and-client-authentication)
in the Async README.

## RPCs and options

```text
mise run example:client -- NAME [RPC [ARGS...]] [options]
```

| RPC | Shape | Default arguments |
| --- | --- | --- |
| `greet [NAME]` | Unary; the default RPC | `World` |
| `fail` | Unary with an empty name, returns `invalid_argument` | |
| `countdown [N]` | Server streaming, N down to 1 (1..10) | `3` |
| `collect NAMES...` | Client streaming, one summary response | `Ada Grace` |
| `chat NAMES...` | Bidi streaming, one reply per name | `Ada Grace` |

| Option | Effect |
| --- | --- |
| `--server NAME` | Name the server you are calling. See [Finding the server](#finding-the-server). |
| `--url URL` | Call this URL. Without `--server`, it must match the running server's URL. An `https://` URL makes an [mTLS](#mutual-tls) call. |
| `--codec proto\|json` | Protobuf (default) or ProtoJSON. |
| `--get` | Connect GET for unary calls (`Greet` is declared `NO_SIDE_EFFECTS`). |
| `--gzip` | Compress requests and accept gzip responses. |
| `--protocol connect\|grpc` | Connect (default) or native gRPC. gRPC needs HTTP/2 and protobuf, and has no GET. |

A successful call exits 0, an RPC error prints `error: code: message` and
exits 1, and a refusal exits 2.

## Server call log

Every example server prints one line per RPC:

```text
served Greet request-id=1f2e3d4c protocol=connect http=POST codec=json encoding=gzip code=OK
```

The line comes from a `ConnectRPC::Instrumentation` subscriber in
[demo/call_log.rb](demo/call_log.rb). The Rack, Async and Falcon servers pass it
to `ConnectRPC::Server.new(instrumentation:)`; the Rails controller returns it
from its private `connect_instrumentation` hook.

- `request-id` is the `demo-request-id` metadata the client sent.
- `protocol`, `codec` and `encoding` come from the request headers the server
  received.
- `http` is inferred from those headers. A Connect GET has no body and no
  content headers, because its codec and compression travel in the query
  string. For GET, `codec` and `encoding` describe the server's response, which
  it chose from that query and the client's Accept-Encoding.
- `code` is the RPC result. A request rejected before any handler ran, such as
  a POST without a content type, prints `http=-` and `code=-`.

## Finding the server

The running server records its name, URL and process ID in
`vendor/examples/server.json`. Set `CONNECTRPC_EXAMPLE_STATE` to use another
path; a relative path resolves from the repository root. Without `--server` or
`--url`, a client calls the recorded server, or explains how to start one when
the file is missing or its process has exited.

`--server NAME` tells the client which host it is calling, so it can call a
server that the state file does not describe. The client then calls:

1. `--url`, when given.
2. Otherwise the recorded URL, when the state file names the same server.
3. Otherwise `http://127.0.0.1:$PORT`, where `PORT` defaults to 8080 and must be
   a number from 1 to 65535.

The client refuses when the state file says a different server runs at the URL
it would call. It cannot detect a wrong `--server` at an address the state file
does not describe, so name the host that is actually running there.

`--url` must be an `http://` or `https://` URL with a host and no credentials,
query or fragment. A state file without a known server name, a URL and a numeric
process ID counts as no running server.

## gRPC-Web

The example servers leave gRPC-Web off because there is no Ruby gRPC-Web client
to call them. Each host can enable it:

- `rack`, `async` and `falcon` add `:grpc_web` to their `ConnectRPC::Server`
  protocols. Rack serves only HTTP/1.1, which has no native gRPC, so it uses
  `protocol: %i[connect grpc_web]`; `async` and `falcon` replace `:auto` with
  `protocol: %i[connect grpc grpc_web]` to keep native gRPC.
- `rails` has no `ConnectRPC::Server` of its own; its controller enables
  gRPC-Web through the private `connect_protocol`, `connect_status_codec` and
  `connect_cors` hooks, with explicit POST and OPTIONS routes.

Browsers also need CORS: pass a `cors:` policy to the server, or return one from
`connect_cors` in Rails, unless outer middleware already owns CORS. See the
[gRPC-Web guide](../docs/grpc-web.md) for per-host limits and the
[browser fixture](../browser/) for an official browser client against real Ruby
hosts.

## Other examples

- [Testing](../gems/connectrpc-testing/README.md) calls generated clients in
  process without a socket: `mise run testing:example`.
- [OpenTelemetry](../gems/connectrpc-otel/README.md) traces client and server
  calls: `mise run otel:example`.
- `mise run example` and `mise run example:generated` run self-contained Rack
  and NetHTTP examples on ephemeral ports, from [unary.rb](unary.rb) and
  [generated.rb](generated.rb).
- `mise run async:example` and `mise run falcon:example` run the generated
  unary scenarios against the Async and Falcon hosts.

## Verification

`mise run example:verify` starts each server on an ephemeral port, runs every
supported client, RPC and option combination plus representative refusals, and
then stops the server with INT or TERM. It then runs the `async` and `falcon`
servers again with `--mtls` for a representative set of calls, and checks the
`demo-client` header on each. CI runs it in the "Runnable examples" job.

For every supported call, the verifier finds the server's
[call log](#server-call-log) line with the client's `demo-request-id`. It checks
the protocol, HTTP method, codec, encoding and code against what the client
asked for. A client that ignored `--protocol`, `--codec`, `--get` or `--gzip`
therefore fails, even though its reply looks the same. For GET, the check uses
the codec and encoding of the server's response, as the call log section
explains.

The generated files in [gen/](gen/) come from `mise run generation:build`, and
`mise run generation:check` keeps them reproducible.
