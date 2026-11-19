# frozen_string_literal: true

backoff = ConnectRPC::Async::Backoff.new(base: 0.5, multiplier: 2, jitter: 0, max: 10)
backoff.max.positive?
policy = ConnectRPC::Async::ConnectionPolicy.new(backoff: backoff, wait_for_ready: true, max_pending_bytes: nil)
policy.max_pending_bytes&.succ
(policy == ConnectRPC::Async::ConnectionPolicy.new(backoff: nil)).to_s
ConnectRPC::Async::ConnectionPolicy.new.backoff&.jitter&.zero?
draining = ConnectRPC::Async::ConnectionPolicy.new(retry_refused: false, max_draining: 0, drain_timeout: 30)
draining.retry_refused.to_s
draining.max_draining.succ
draining.drain_timeout&.positive?
keepalive = ConnectRPC::Async::Keepalive.new(time: 300, timeout: 20, permit_without_calls: false)
(keepalive.time + keepalive.timeout).positive?
keepalive.permit_without_calls.to_s
kept = ConnectRPC::Async::ConnectionPolicy.new(keepalive: keepalive)
kept.keepalive&.time&.positive?
(ConnectRPC::Async::ConnectionPolicy.new.keepalive == keepalive).to_s
transport = ConnectRPC::Async::Transport.new(
  origin: "https://example.invalid", connection_policy: policy,
  connection_observer: lambda do |event|
    event.type.to_s
    # @type var outcome: ConnectRPC::Async::connection_outcome?
    outcome = event.outcome
    outcome == :keepalive_timeout if event.type == :connection_closed
    event.pending_calls.zero? if event.type == :counts_changed
    event.previous_state&.to_s
    event.duration&.round
    (event.active + event.draining + event.pending_calls + event.pending_bytes).succ
    nil
  end
)
# @type var state: ConnectRPC::Async::connection_state
state = transport.connection_state
state.to_s
ConnectRPC::Async::Transport.new(origin: "http://example.invalid", connection_observer: APIConnectionObserver.new)
transport.close(grace: 0)
begin
  transport.close
rescue ConnectRPC::Async::LifecycleError => e
  e.message.to_s
end
# The Async name is core's class, so either spelling satisfies the policy.
ConnectRPC::Async::ConnectionPolicy.new(keepalive: ConnectRPC::Keepalive.new(time: 60)).keepalive&.timeout&.positive?
core_keepalive = ConnectRPC::Async::Keepalive.new(time: 1) #: ConnectRPC::Keepalive
core_keepalive.time.positive?
