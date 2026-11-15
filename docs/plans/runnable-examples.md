# Runnable server and client examples

Base: `95ecf9ffb4fd750f7d73caa84c7eb3f0bb79a4d3`.
Delivery: one PR from `feat/runnable-examples`. No merge, publish or release authority.

## Objective

A reader starts the server of their choice in one terminal and calls it from the
client of their choice in another:

```sh
mise run example:server -- falcon
mise run example:client -- async-h2 chat Ada Grace
```

Each server and client is one short, readable file that teaches how to build that
kind of host or caller. An unsupported pairing refuses with a message that names
supported alternatives; it never appears to work by accident.

## Decisions settled with Jim

- Full matrix: servers Rack/WEBrick, single-file Rails, Async Host, Falcon;
  clients NetHTTP, Async H2 `Transport`, Async `HTTP1Transport`. Connect unary and
  streaming, plus native gRPC where supported. Testing and OTel keep their existing
  standalone examples (no socket server/client role). gRPC-Web gets a documentation
  note only, because there is no Ruby Web client.
- Launcher: two Mise tasks, `example:server -- NAME` and
  `example:client -- NAME [RPC [ARGS...]] [options]`, backed by a dispatcher that
  runs each file in its owning bundle. `--list` prints the choices.
- Mismatches: a declared support table, checked by the client before any call.
- CI runs every supported pair plus representative refused pairs on ephemeral
  ports, plaintext only, in one new job on one Ruby lane.
- Rails: add `webrick 1.9.2` as a development gem to the Rails Appraisal lanes and
  regenerate both lane gemfiles/locks with Appraisal, so the Rails server runs from
  its own lane (`RAILS_LANE` selects 8.0 or 8.1).
- Mutual TLS (added 2026-11-15 after the first review rounds): `example:server --
  async|falcon --mtls` serves TLS and requires a client certificate. The server
  issues a throwaway demo CA, server and client certificate on demand. Clients
  use mTLS automatically for an `https://` URL. See [Mutual TLS](#mutual-tls).

## Constraints

- No new public API, signatures or inventory entries. No backend dependencies in
  core or Rails bundles beyond the WEBrick development gem above.
- Each file runs in its owning bundle: NetHTTP client in root; Rack server in the
  `gems/connectrpc-rack` bundle; Rails server in the selected Rails lane; Async
  server and both Async clients in the async bundle; Falcon server in the falcon
  bundle. Reuse each existing runner's bundle environment rather than duplicating
  it (for example, an `exec` passthrough in each runner).
- Generated `_pb.rb`/`_connect.rb` come from `mise run generation:build`; never
  hand-edit. Keep `allowed_push_host` guard untouched.
- Fixed default port 8080 with `PORT` override (`PORT=0` binds ephemeral). Bind
  `127.0.0.1` only. Verification uses ephemeral ports.
- Keep edits to `mise.toml`, `README.md`, `docs/development.md` and
  `docs/support.md` narrow (open PR #51 touches them).
- Existing `example`, `example:generated`, `async:example`, `falcon:example`,
  `testing:example` and `otel:example` tasks, their scripts and CI coverage stay
  unchanged in behavior.

## Approach and scope

### Shared schema

- `examples/proto/demo/v1/demo.proto` (package `demo.v1`, service `DemoService`):
  `Greet` unary (`NO_SIDE_EFFECTS`, so GET is eligible), `Countdown` server
  stream, `Collect` client stream, `Chat` bidi. Small request/response messages.
  `Greet` with an empty name raises `invalid_argument` to show errors.
- Check in generated output under `examples/gen/` (or similar), produced and
  byte-checked by `scripts/build_generation.rb` (`generation:build` /
  `generation:check`). Extend the existing `.rubocop.yml` generated-code
  exclusions (`_pb.rb` in AllCops, and the scoped `Style/ArgumentsForwarding` /
  `Naming/ClassAndModuleCamelCase` `_connect.rb` exceptions) to the demo output
  directory so lint never demands edits to generated files.
- `examples/demo/handlers.rb`: one shared handler set used by every server, so
  servers differ only in hosting code. `examples/demo/support.rb` (name flexible):
  the declared support table and the pairing check.

### Servers (`examples/servers/*.rb`)

Each builds a `ConnectRPC::Server` from the shared handlers, binds
`127.0.0.1:$PORT`, prints one ready line with the actual URL and server name,
writes a state file (below), and shuts down cleanly on INT/TERM with a bounded wait.

| Server | Hosting | Protocols | Shapes |
| --- | --- | --- | --- |
| `rack` | `ConnectRPC::Rack` on WEBrick via `examples/localhost.rb` (add an optional `port:` keyword defaulting to 0) | Connect | U |
| `rails` | Single-file `Rails::Application` (railties + action_controller), explicit route, controller including `ConnectRPC::Rails::Controller`, served through the same WEBrick host | Connect | U |
| `async` | `ConnectRPC::Async::Host` (h2c) inside an application-owned `Async` reactor, `host.wait` | Connect + gRPC (`protocol: :auto`) | U/SS/CS/bidi |
| `falcon` | `ConnectRPC::Falcon::Server` with a pre-bound `Endpoint` inside `Async` (supports port 0; close the caller-owned bound endpoint on shutdown); comment points to the DSL form in `gems/connectrpc-falcon/examples/falcon.rb` | H1: Connect only. H2: Connect + gRPC | H1: Connect U/SS/CS. H2: Connect and gRPC U/SS/CS/bidi |

### Clients (`examples/clients/*.rb`)

| Client | Transport | HTTP | Protocols | Shapes |
| --- | --- | --- | --- | --- |
| `nethttp` | default core client | H1 | Connect (POST or `--get`) | U |
| `async-h2` | `ConnectRPC::Async::Transport` | H2 prior knowledge | Connect, gRPC (`--protocol grpc`) | U/SS/CS/bidi |
| `async-http1` | `ConnectRPC::Async::HTTP1Transport` | H1 | Connect (POST or `--get`) | U/SS/CS |

RPC commands: `greet [NAME]` (default), `countdown [N]`, `collect NAMES...`,
`chat NAMES...`, `fail`. Options: `--server NAME`, `--url URL`,
`--codec proto|json`, `--get`, `--gzip`, `--protocol connect|grpc`. Output prints
messages, response metadata/trailers and errors (`code: message`). Exit 0 on
success, nonzero on error or refusal.

### Mutual TLS

- Servers: `async` and `falcon` accept `--mtls` and pass a `tls:` hash with
  `client_ca`, so a verified client certificate is mandatory. `rack` and `rails`
  refuse `--mtls` with exit 2, since `docs/support.md` qualifies no TLS for them.
- Certificates: `examples/demo/certs.rb` issues an EC P-256 CA, a server leaf for
  `localhost`/`127.0.0.1` and a `demo-client` leaf, valid 7 days, into a `tls`
  directory next to the state file (`vendor/examples/tls` by default). Servers
  reuse a set valid for at least another day, under a lock so simultaneous starts
  do not replace each other's set. Each issue writes a new set directory and
  renames a new `tls` symlink over the old one, so readers resolve one complete
  set; a replaced set is removed a minute after replacement. Nothing is
  committed.
- Clients: `async-h2` and `async-http1` pass `ca_file`, `certificate_chain` and
  `private_key` for an `https://` URL. `nethttp` is refused, because the core
  transport has no client certificate option. A client refuses when the
  certificate files are missing.
- Evidence: handlers return the subject of `host.peer_certificate` in a
  `demo-client` response header, which shows the server verified the client.
- Rotation (added 2026-11-15 at Jim's request): both servers use
  `tls_provider:` with `refresh_interval: 5`, and the provider rereads the files
  with a content digest as `version`. `Certs.renew!` issues new leaves under the
  current CA, so peers that have not refreshed still trust each other. Comments
  and the README cover `drain_tls`, rotating a client by replacing its
  transport, and overlapping trust when a CA changes. The servers log `Read TLS
  certificates` for each version the provider returns and `Rejected TLS
  certificates` from `on_tls_error:`. `example:verify` renews under each
  running `--mtls` server, checks by TLS handshake that the server presents the
  renewed certificate, then makes a call that must show the verified client.
- `examples/demo/mtls.rb` holds the host and client TLS options in one readable
  place.

### Pairing check

- The table mirrors `docs/support.md`: server → per HTTP version, per protocol,
  the supported shapes (gRPC is H2-only everywhere); client → HTTP version, protocols, shapes; option rules (gRPC
  requires protobuf, no GET; GET only for unary). A pair is allowed only when the
  client's HTTP version is served, and the protocol and RPC shape are supported on
  that HTTP version.
- The client resolves the target from the state file written by the running
  server (default `vendor/examples/server.json`, overridable with an env var;
  `vendor/` is gitignored). `--server NAME` overrides identity; `--url` alone is
  accepted only when it matches the state record's URL, otherwise `--server` is
  required and an unknown identity is refused before any I/O. A missing or stale
  state file (dead pid) without `--server` prints how to start a server.
- Owning runners `chdir` into their gem directories, so every script, generated
  code and default state path is resolved from the repository root (absolute),
  and a relative state-path override resolves against the repository root too.
- Refusal message: what is unsupported and which servers/clients/options would
  work, with a pointer to `examples/README.md`. Library-level rejections remain
  visible for anything the table allows.

### Launcher and tasks

- `scripts/example.rb server|client NAME ...` (or `examples/run.rb`): validates the
  name, ensures the owning bundle is set up (run the owning `*:setup` task or fail
  with the exact command), then execs the file in that bundle. `--list` prints the
  servers, clients and RPCs.
- Mise: `example:server`, `example:client`, `example:verify`. `example:verify`
  depends on the setup tasks for the root, rack, selected Rails lane, async and
  falcon bundles.

### Verification script and CI

- `scripts/example_verify.rb`: for each server, start it with `PORT=0` and a
  temporary state path, wait for the ready line under a bounded deadline (report
  last output on timeout), run every allowed client × RPC × relevant option
  (codec json, `--get`, `--gzip`, gRPC), assert exit status and expected output,
  run the refused pairs listed below and assert nonzero exit plus the specific
  refusal reason, then stop the server and assert bounded exit. Every client
  subprocess also runs under an external deadline with TERM then KILL escalation
  and captured-output diagnostics; servers are cleaned up unconditionally
  (`ensure`). At least one client per server resolves the target from the state
  file alone (no `--server`/`--url`). No fixed sleeps.
- New CI job "Runnable examples" on Ruby 4.0.9 (or 3.4.12; pick one and say so),
  modeled on existing jobs: bundle caches for each bundle used, `mise run
  example:verify`, workspace-clean check. Update `docs/ci.md` and
  `spec/ci_runtime_spec.rb` (it asserts the exact job set, the 30-check total and
  two-Ruby matrices): keep existing assertions and assert the new job's single
  lane and command separately.

### Documentation

- `examples/README.md`: quick start, the server and client tables, the
  compatibility matrix, RPC commands/options, gRPC-Web note (link to
  `docs/grpc-web.md` and the browser fixture), pointers to Testing/OTel examples and
  the existing ephemeral examples.
- `README.md`: one short pointer from "Try the localhost example".
- `docs/development.md`: rows for the three new tasks.

## Observable success

- `example:server -- <each>` prints its URL and serves until Ctrl-C, exiting 0.
- Every allowed client × server × RPC prints the expected response.
- Refused pairs exit nonzero with a message naming working alternatives.
- `example:verify` passes locally and in the new CI job; existing example tasks and
  their CI steps are unchanged.

## Failure paths and risks

- Port in use: report the bind error and the `PORT` override; no retry.
- Stale state file: detect dead pid and require `--server`/start a server.
- Wrong `--server` for the actual target: out of scope to detect beyond the table;
  the README says `--server` must name the running host.
- Server crash or hang in verification: bounded readiness/shutdown, kill on
  timeout, print captured output.
- Rails lane lock regeneration: use Appraisal with `BUNDLE_COOLDOWN=7` and the root
  `vendor/bundle` cache; `generation:check:rails` must pass. Rails 8.0 lane runs on
  MRI 3.4.12 only, matching existing policy.
- WEBrick input draining: reuse `examples/localhost.rb`, whose draining wrapper
  exists for correctness.
- Interrupt handling inside Async reactors: close hosts in `ensure` with a finite
  grace.

## Refused pairs verified (representative)

- `async-h2` → `rack` and `rails` (H2 against H1-only host).
- `async-http1` → `async` (H1 against strict H2 Host).
- `nethttp countdown` → any server (streaming on a unary client).
- `async-http1 chat` → `falcon` (bidi over H1).
- `async-http1 countdown` → `rack` and `rails` (streaming on a portable host).
- `async-http1 --protocol grpc` and `nethttp --protocol grpc` → `falcon` (gRPC
  over H1).
- `--protocol grpc --codec json` and `--protocol grpc --get`.

## Non-goals

- Server-only TLS, and TLS for Rack, Rails or NetHTTP, gRPC-Web clients, Testing and
  OTel in the server/client matrix, Puma or other Rack servers, production
  deployment guidance, detecting a mislabelled `--server`.
- Changing the behavior of existing example tasks.

## Verification map

| Concern | Evidence |
| --- | --- |
| Every supported pair works | `example:verify` (local + CI) |
| Refused pairs fail clearly | `example:verify` negative cases |
| Generated demo code reproducible | `generation:check` |
| Rails lane with WEBrick | `generation:check:rails`, `test:rails` (8.1), `example:verify` Rails server |
| `localhost.rb` `port:` change keeps callers working | `example`, `test:interop`, `check:package`, `rack:package` |
| Existing example tasks unchanged | `example`, `example:generated`, `async:example`, `falcon:example`, `testing:example`, `otel:example` |
| CI job set guard | `mise run test -- spec/ci_runtime_spec.rb` |
| State-file discovery across runner `chdir` | `example:verify` state-file-only client runs |
| Lint, syntax, workflow | `mise run check` (includes `lint:workflows`), `async:check`, `falcon:check`, `rack:check` |
| Final handoff | `mise run verify` then `mise run rack:verify` |
| Certificate rotation without a restart | `example:verify` renewal check; a provider that logs a new version but serves the old certificate fails it |
| mTLS pairs work and the server verifies the client | `example:verify` `--mtls` runs check the `demo-client` header; perturbing `client_ca: nil` fails them |
| Unsupported mTLS pairs fail clearly | `example:verify`: `rack --mtls`, `nethttp` to an mTLS server, missing certificates |
| Manual | Two-terminal run of one pair per server, Ctrl-C shutdown; curl with and without a client certificate |

## Open questions

None blocking. The CI Ruby lane choice is left to implementation (recommend
4.0.9).
