# Runnable examples

Start the server of your choice in one terminal and call it from the client of
your choice in another. Every server and client is one short file built on the
same [demo schema](proto/demo/v1/demo.proto) and [shared handlers](demo/handlers.rb),
so the files differ only in how they host or call ConnectRPC.

## Quick start

```sh
mise run setup
mise run falcon:setup
mise run async:setup

# Terminal 1
mise run example:server -- falcon

# Terminal 2
mise run example:client -- async-h2 chat Ada Grace
```

The server prints its URL and serves until Ctrl-C:

```text
falcon server listening on http://127.0.0.1:8080 (Ctrl-C to stop)
```

The client prints the messages, this demo's `demo-*` response headers and
trailers, or the RPC error:

```text
async-h2 -> falcon at http://127.0.0.1:8080 (connect, proto, identity)
message: Hello, Ada!
message: Hello, Grace!
headers: demo-served-by=falcon
```

`mise run example:client -- --list` prints every server, client, RPC and option.
If a bundle is missing, the launcher prints the exact setup command to run.

## Servers

Each server binds `127.0.0.1:8080`. Set `PORT` to use another port; `PORT=0`
picks a free one and the ready line shows it.

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
| `--server NAME` | Name the server you are calling, instead of reading the state file. |
| `--url URL` | Call this URL. Alone, it must match the running server's URL. |
| `--codec proto\|json` | Protobuf (default) or ProtoJSON. |
| `--get` | Connect GET for unary calls (`Greet` is declared `NO_SIDE_EFFECTS`). |
| `--gzip` | Compress requests and accept gzip responses. |
| `--protocol connect\|grpc` | Connect (default) or native gRPC. gRPC needs HTTP/2 and protobuf, and has no GET. |

A successful call exits 0, an RPC error prints `error: code: message` and
exits 1, and a refusal exits 2.

## Finding the server

The running server records its name, URL and process ID in
`vendor/examples/server.json`. Set `CONNECTRPC_EXAMPLE_STATE` to use another
path; a relative path resolves from the repository root. Without `--server` or
`--url`, a client calls the recorded server, or explains how to start one when
the file is missing or its process has exited.

`--server NAME` tells the client which host it is talking to, so it can call a
server that the state file does not describe. Name the actual host: the client
cannot detect a mislabelled `--server`.

## gRPC-Web

Each of these hosts can also serve gRPC-Web when its `ConnectRPC::Server`
enables it, for example with `protocol: %i[connect grpc grpc_web]`. The example
servers leave it off because there is no Ruby gRPC-Web client to call them. See
the [gRPC-Web guide](../docs/grpc-web.md) for per-host limits and the
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
then stops the server with INT or TERM. CI runs it in the "Runnable examples"
job. The generated files in [gen/](gen/) come from `mise run generation:build`;
`mise run generation:check` keeps them reproducible.
