# frozen_string_literal: true

backoff = ConnectRPC::Async::Backoff.new(base: 0.5, multiplier: 2, jitter: 0, max: 10)
backoff.max.positive?
policy = ConnectRPC::Async::ConnectionPolicy.new(backoff: backoff, wait_for_ready: true, max_pending_bytes: nil)
policy.max_pending_bytes&.succ
(policy == ConnectRPC::Async::ConnectionPolicy.new(backoff: nil)).to_s
ConnectRPC::Async::ConnectionPolicy.new.backoff&.jitter&.zero?
transport = ConnectRPC::Async::Transport.new(
  origin: "https://example.invalid", connection_policy: policy,
  connection_observer: lambda do |event|
    event.type.to_s
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
