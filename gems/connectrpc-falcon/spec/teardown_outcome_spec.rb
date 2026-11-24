# frozen_string_literal: true

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
      expect do
        owner.cancel(cause: RuntimeError.new("fatal worker failure"))
        task.with_timeout(1) { owner.wait }
      end.to output(teardown_warnings(RuntimeError)).to_stderr
      expect(owner).to be_cancelled
      expect(host.instance_variable_get(:@session).instance_variable_get(:@state)).to eq(:closed)
    ensure
      owner&.cancel
      bound&.close
    end
  end
end
