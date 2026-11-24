# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/retirement_helpers"

# A failing listener step never skips a later one or replaces the close result
# (docs/design.md, rule 4).
RSpec.describe ConnectRPC::Falcon::Server, "listener teardown failures" do
  it "stops every other accept task and closes every other owned socket when one of each fails" do
    reactor do |task|
      sockets = 2.times.flat_map { IO::Endpoint.tcp("127.0.0.1", 0).bound.sockets }
      endpoint = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0")
      endpoint.define_singleton_method(:bound) { |**| IO::Endpoint::BoundEndpoint.new(self, sockets.dup) }
      host = described_class.new(server: server, endpoint: endpoint)
      host.run
      wrapper = host.instance_variable_get(:@accept_wrapper)
      wait_until(task, "both accept tasks to start") { wrapper.instance_variable_get(:@listeners).size == 2 }
      stuck, accepting = wrapper.instance_variable_get(:@listeners).keys
      stuck.define_singleton_method(:stop) { |*| raise "accept stop failed" }
      close = sockets.first.method(:close)
      sockets.first.define_singleton_method(:close) { raise Errno::EBADF, "listener close failed" }
      expect do
        expect(host.close(grace: 5)).to be(true)
      end.to output(teardown_warnings(RuntimeError, Errno::EBADF)).to_stderr
      wait_until(task, "the other accept task to exit") { accepting.finished? }
      expect(sockets.last).to be_closed
      expect(host.instance_variable_get(:@session).instance_variable_get(:@state)).to eq(:closed)
    ensure
      AsyncHelpers::INTERNAL.cancel(stuck)
      close&.call
      sockets&.each { |socket| AsyncHelpers::INTERNAL.raw_close(socket) }
    end
  end

  # A worker that ends outside the managed stop aborts in its ensure. A
  # teardown failure there is reported and never replaces how it ended.
  it "keeps a worker's own cancellation when its ensure abort meets a failing teardown step" do
    reactor do |task|
      owners = Async::Queue.new
      bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      core = server
      evaluator = Object.new
      evaluator.define_singleton_method(:connectrpc_close_grace) { 5 }
      evaluator.define_singleton_method(:make_server) do |endpoint|
        owners.enqueue(Async::Task.current)
        ConnectRPC::Falcon::Server.new(server: core, endpoint: endpoint)
      end
      host = ConnectRPC::Falcon::Service.allocate.run(nil, evaluator, Struct.new(:endpoint).new(bound))
      owner = owners.dequeue
      host.instance_variable_get(:@accept_wrapper).define_singleton_method(:stop) { raise "accept stop failed" }
      fatal = RuntimeError.new("fatal worker failure")
      # Async records no terminal cause on the task, so observe the cause
      # its fiber ends with as it enters the cancelled state.
      causes = []
      ended = TracePoint.new(:call) do |event|
        causes << event.binding.local_variable_get(:cause) if event.self.equal?(owner) && event.method_id == :cancelled!
      end
      expect do
        ended.enable do
          owner.cancel(cause: fatal)
          task.with_timeout(1) { owner.wait }
        end
      end.to output(teardown_warnings(RuntimeError)).to_stderr
      expect(owner).to be_cancelled
      expect(causes.first).to equal(fatal)
      expect(host.instance_variable_get(:@session).instance_variable_get(:@state)).to eq(:closed)
    ensure
      owner&.cancel
      bound&.close
    end
  end
end

RSpec.describe "Falcon HTTP/1 retirement teardown failures" do
  include RetirementHelpers

  def retirement_host(core, tls: nil, **)
    scheme = tls ? "https" : "http"
    host = ConnectRPC::Falcon::Server.new(server: core, endpoint: ConnectRPC::Falcon::Endpoint.parse(
      "#{scheme}://127.0.0.1:0"
    ), tls: tls, **)
    host.run
    [host, "#{scheme}://127.0.0.1:#{host.instance_variable_get(:@owned_bound).sockets.first.local_address.ip_port}"]
  end

  # The stream and TLS closes fail without closing raw, so the final raw
  # close meets the last fault.
  it "reports failed disposal steps and a failed final raw close and still completes retirement" do
    reactor do |task|
      with_retirement_connection(task, mode: :tls, protocol: :h1) do |state|
        session, resource, gate, handler = state.values_at(:session, :resource, :gate, :handler)
        expect(resource).to be_a(ConnectRPC::Falcon.const_get(:HTTP1Resource))
        raw = resource.raw
        closed_after_handler = fail_first_raw_close(raw, handler)
        resource.connection.define_singleton_method(:close) { |*| raise IOError, "HTTP/1 close failed" }
        resource.socket.define_singleton_method(:close) { raise OpenSSL::SSL::SSLError, "TLS close failed" }
        expect do
          aborter = task.async(finished: false) { session.abort }
          waiter = task.async(finished: false) { state.fetch(:accepted).wait_closed }
          expect(gate.entered.dequeue).to equal(handler)
          gate.release
          join_retirement_tasks(task, aborter, waiter, handler)
          expect(aborter.result).to equal(session)
        end.to output(teardown_warnings(IOError, OpenSSL::SSL::SSLError, RuntimeError)).to_stderr
        expect(closed_after_handler).to eq([true])
        expect(retirement_complete?(resource)).to be(true)
        expect(raw).to be_closed
      end
    end
  end
end
