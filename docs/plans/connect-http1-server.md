# Connect HTTP/1.1 streaming server contract

Base: `3d34adb`, including lifecycle ownership #39 and package staging #40.
Delivery: one server PR. No client implementation, stack, merge, or release.
Amended on 2026-11-04 with Jim's approval of the tested cooperative final-close
grace. The earlier experiment-only restriction is superseded.

## Objective and supported scope

Falcon native HTTP/1.1 serves Connect server streaming and client streaming in
protobuf/ProtoJSON with identity/gzip, over plain TCP, verified TLS and mTLS.
The standalone Async Host remains H2-only. Existing H2 Connect shapes, native
gRPC and gRPC-Web retain their protocol gates and behavior. Portable Rack,
Rails and Testing streaming, NetHTTP streaming, HTTPX, ordinary-thread client
integration (#29), and the dependent Async H1 client are outside this PR.

## Capability and routing contract

- Add explicit shape capabilities to `Server#call_stream`, following the Web
  opt-in pattern. Preserve legacy `http2: true` as all existing H2 shapes and
  default portable rejection. Reject conflicting, duplicate and unsupported
  capability options. Update signatures, supported/SPI inventory, strict
  positive/negative custom-host callers and installed package consumers.
  Use `capabilities: nil` by default and shape symbols `:server_stream`,
  `:client_stream`, `:bidi_stream`. A non-nil list must be a nonempty, unique
  Array of supported symbols. Combining it with `http2: true` is invalid.
  Explicit omitted SS/CS shapes return the existing bounded `unimplemented`
  streaming error before input/handler work. An explicitly omitted known bidi
  shape returns bare 505 before codec/input/handler work. The default nil list
  plus `http2: false` retains portable rejection.
- Generalize the private publication predicate in Async Application,
  ServerSession and Falcon delegates. Select actual H1/H2 capabilities from
  the resource/request. Never fabricate an H2 stream or pass H2 capability for
  H1 gRPC. Preserve unary GET and fallback routing.
- On a known bidi route over native H1, reject with bare 505 and
  `Connection: close` before codec/input/handler work, matching connect-go
  v1.21.1 `handler.go:ServeHTTP`. Preserve unrelated portable rejection statuses.
  Core returns host-neutral status without a Connection field. The native H1
  host sets `persistent = false` for every 505, even when input is absent,
  letting upstream write exactly one `Connection: close` field.
- Explicitly close fully consumed H1 bodies on the original processor task,
  because the pinned upstream Finishable otherwise retains the request.
  Snapshot leading metadata before the chunked parser can append trailers.

Native H1 routing precedence is explicit:

| Request | Decision |
| --- | --- |
| CORS preflight/early response | Existing CORS behavior first. |
| gRPC-Web, including a bidi route | Existing Web negotiation, capability and rejected-input policy. |
| Any other request to a known bidi route | Bare 505/close before method/media validation, including GET, native gRPC, and a server without Connect enabled. |
| Other native gRPC H1 request | Existing unsupported-protocol behavior; never H2 capability. |
| POST, handled path, Connect streaming media | Connect publication with actual H1 shape capabilities. |
| Other unary/fallback/portable traffic | Existing path and behavior. |

The private publication predicate must remain narrow. The host retirement
handoff also covers streaming-route admission 503 and Connect preflight
405/415/505 responses with unread input, preventing upstream's unbounded
implicit discard. This does not broaden streaming execution to other protocols.
HTTP/1.0 Connect streaming is rejected rather than claimed as supported.

## Processing, delivery and connection retirement

First prove on the pinned runtime, with real sockets, that the same H1
connection permits response-head writes while original framed input reads are
active. Stop and report a concrete blocker if this cannot safely meet the
approved early-response outcome. The probe precedes product implementation.

The original processor owns handlers, codecs, authenticated peer identity,
trace activation, and application cleanup. Body consumers transmit prepared
bytes. Publication exposes headers independently, while successful HTTP EOF
continues to require original processing and application cleanup to succeed.
Processing cleanup failure must abort delivery rather than falsely finish it.

At response-head publication, if request EOF is not established, disable reuse
and advertise `Connection: close`. Deliver the complete Connect response,
terminal and HTTP EOF before unread-input hygiene. Slow uploads alone cannot
delay that EOF or change valid response success/error to unavailable.
Sample framing completion once on the connection task after the head resolves
and before response serialization: no body, Fixed remaining zero, or Chunked
finished, never merely `empty?` after failure/close. Persistence never flips
back to true. The Connect terminal may already be sent on late cleanup failure;
the protected success boundary is the HTTP terminal chunk, not receipt of that
Connect terminal or a promise that every client observes failure.

After successful response delivery, best-effort discard applies only to the
original framed request input. Use one absolute 100 ms allowance, at most
256 KiB total discarded entity bytes including adapter buffered remainders,
and reads at most 64 KiB. Progress cannot reset the allowance. Do not use
grpc-timeout for Connect. Earlier owner abort/shutdown may cut hygiene short.
Retire the connection even if the remainder finishes. Never parse, dispatch,
or consume a following pipelined request. Exhaustion, malformed framing,
peer failure and hygiene errors cannot overwrite processing or delivery facts.
Raw-first abort remains the forced teardown boundary. Preserve healthy fully
consumed H1 persistence and the current gRPC-Web rejected-input policy.

For a successfully delivered response on a retiring connection, establish one
absolute final-close expiry at the actual final HTTP response flush plus
500 ms, and perform the tested raw TCP write-side half-close. Preserve the
original 100 ms discard clock and byte/read caps. Once discard stops, wait only
the remaining part of that same 500 ms total budget, without reading further.
Do not add 500 ms after discard. Owner abort/shutdown can shorten either phase.
Failure before successful HTTP completion still uses immediate raw-first
teardown. Graceful retirement must not overwrite an already observed delivery.

The actual final flush, rather than write_body return, outer callbacks or
lifecycle observation, anchors the expiry. Detect this boundary through the
pinned writer's completion state; arbitrary application payload bytes must not
be mistaken for an HTTP terminal. Include empty/preflight responses in the
owned framing paths. Preserve non-Connect and gRPC-Web behavior.

These timers are cooperative and can overshoot under scheduler delay; they are
not hard real-time ceilings. Socket/admission retention through the remaining
grace is accepted. The half-close plus bounded wait improves reset avoidance,
but does not promise delivery under arbitrary network or peer failure.
This is a deliberate response-first, bounded-discard variant of Go's conditional
HTTP/1.1 reset-avoidance handling. connect-go can consume an oversized declared
message and net/http can drain before flushing or reuse the connection; no
literal Go parity is claimed. Keep the official runner and cases unchanged.

A private per-request handoff retains the original Finishable and core input
adapter remainder. The connection task runs hygiene after successful
`HTTP1Connection#write_body` and delivery observation, before upstream
Finishable wait. It always closes the Finishable in ensure, using raw-first
teardown when forced. The processor closes consumed bodies only; it must not
close unconsumed input before handoff. No processor/input reader may still own
a read at handoff, including timed Input child tasks.

Count retained adapter remainder first. Since pinned framed body reads have
no length argument and return at most 65,536 bytes, reserve that full amount
before every further read. Stop when fewer than 65,536 bytes remain in the
discard allowance. The cap is conservative and may stop below 256 KiB. These
are framed body-API read bounds, not io-stream socket readahead bounds. No
following request bytes are logically consumed or parsed by hygiene.
Retain host resource/admission ownership until this handoff closes, while
keeping the already completed RPC delivery fact independent.

No EOF watcher or synthesized Context cancellation on H1 peer EOF/disconnect.
Actual deadlines, I/O failures and explicit application cancellation retain
their current meanings. Cooperative disconnected handlers are not promised
forced interruption. This is best effort under real peer/network failure.

## Verification map

Use observable events, queues and bounded progress conditions for ordering.
Never use fixed sleeps to assume operation progress. Record actual commands,
outcomes and revision identity. Report new failures without weakening checks
or retrying until green. Both pinned Ruby lanes are 3.4.12 and 4.0.9.

| Concern | Required evidence |
| --- | --- |
| Runtime feasibility | Real pinned H1 concurrent head write and active body read probe, with bounded completion. |
| Incrementality | Real-socket first SS message before handler completion; CS first receive before upload completion; response headers while CS input remains active. |
| Early response | Valid success/error during slow and non-finishing fixed/chunked uploads over plain TCP and TLS; HTTP terminal chunk observed before discard/retirement completes. |
| Lifecycle | Processing/application cleanup failure prevents false EOF; completed response remains completed after hygiene failure; admission held through remaining final-close grace, close/drain/abort interruption, unrelated-connection progress and real worker shutdown. |
| Bounds and isolation | Actual final HTTP flush plus one cooperative 500 ms total expiry, including delayed callbacks; no payload-based false completion; unchanged absolute discard time under continued trickle, exact 4 x 64 KiB boundary and adapter remainder plus reads, body read sizes, malformed/truncated/oversize fixed/chunked input including InvalidTrailerError, pipelined next-request isolation and no replay; both plain TCP and TLS. |
| Protocol gates | Native H1 bidi 505/close before codec/input/handler; H1 native gRPC remains rejected; portable, Web and H2 behavior remains intact. |
| Metadata and identity | Actual HTTP chunked trailers (including x-t and connect-timeout-ms) excluded from leading metadata, propagation and deadline; immutable TLS identity/trust; real SDK span activation and peer identity on original processor through codecs/cleanup. |
| Independent peers | Extend pinned connect-go peer modes explicitly. Observe actual ProtoMajor==1 for SS/CS x protobuf/JSON x identity/gzip x plain/TLS/mTLS on both Ruby lanes. Retain H2 shape checks. |
| Official conformance | Separate HTTP_VERSION_1 Connect SS/CS server identity/gzip profiles, supportsHalfDuplexBidiOverHttp1:false; preserve existing inventories and strict ledgers. Discover actual cases before recording counts. |
| Public API/packages | Owning signatures, API inventory, positive/negative custom-host callers, rbs:check and rbs:package, normal installed package discovery and host consumers on both lanes. |
| Regression and delivery | Focused specs/checks during implementation; repository verify and rack:verify at final handoff; relevant Async/Falcon/OTel checks and packages; CI owns its actual clean-environment matrix. |

Run raw-socket specs on both Ruby lanes. Include same-connection keep-alive
follow-ups after consumed SS/CS, pending timed-read cleanup before hygiene,
abort/drain during hygiene, and a blocked-writer concurrency case. Replace the
old fallback H1 SS rejection assertion deliberately. Update stale H1-unary
claims in root and owning package READMEs, design/streaming and conformance docs.
Keep the existing H2 conformance/client inventories intact; select new H1
profiles only for Falcon server execution, with explicit adapter/ledger modes.

## Delivery gates

Review this revised contract with Claude before implementation. Use a fresh
Codex implementer in the delivery checkout, with exclusive product mutation
ownership. The parent inspects code and evidence. File a draft PR, run required
Codex/Claude dual review on the pushed exact candidate, then CodeRabbit once
internally accepted. Apply convergence rules and retain draft on coverage gaps.
No UX review is needed because no UI changes are involved.

Ready requires current base in head, passing required checks/reviews, composed
review coverage of head and no valid unresolved blockers. Rebase onto refreshed
main with range-diff and force-with-lease on this branch only; re-review affected
code after conflicts or overlapping base changes. Report URL/head, decisions,
open threads/reasons, gaps and manual checks to the coordinator. Never merge.

## Open questions

No product decisions remain open. Runtime feasibility and the private 500 ms
overlay passed, but production changes must obtain their own revision-bound
verification and lifecycle/concurrency review. The earlier hosted Web H1/TLS
failure remains an unexplained disclosed residual; do not claim the new policy
fixes it without evidence. No waiver, runner upgrade or merge is authorized.
