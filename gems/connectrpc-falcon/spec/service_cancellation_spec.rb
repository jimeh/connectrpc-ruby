# frozen_string_literal: true

RSpec.describe "Falcon service owner cancellation" do
  [nil, RuntimeError.new("fatal owner failure")].each do |cause|
    it "aborts raw peers immediately for #{cause ? 'fatal' : 'direct'} cancellation outside managed stop" do
      reactor do |task|
        entered = Async::Queue.new
        owners = Async::Queue.new
        cancelled = Async::Queue.new
        rack = lambda do |_env|
          entered.enqueue(true)
          begin
            Async::Condition.new.wait
          ensure
            cancelled.enqueue(true)
          end
        end
        bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
        core = server
        evaluator = Object.new
        evaluator.define_singleton_method(:make_server) do |endpoint|
          owners.enqueue(Async::Task.current)
          ConnectRPC::Falcon::Server.new(server: core, endpoint: endpoint, rack_app: rack)
        end
        host = ConnectRPC::Falcon::Service.allocate.run(nil, evaluator, Struct.new(:endpoint).new(bound))
        owner = owners.dequeue
        raw = IO::Endpoint.tcp("127.0.0.1", bound.sockets.first.local_address.ip_port).connect
        raw.write("GET /blocked HTTP/1.1\r\nHost: localhost\r\n\r\n")
        entered.dequeue
        session = host.instance_variable_get(:@session)
        accepted_raw = session.instance_variable_get(:@resources).first.raw
        owner.cancel(cause: cause)
        expect(accepted_raw).to be_closed
        task.with_timeout(1) { owner.wait }
        expect(host.tls_status[:state]).to eq(:closed)
        task.with_timeout(1) { cancelled.dequeue }
        expect(raw.read).to eq("")
      ensure
        owner&.cancel
        owner&.wait
        AsyncHelpers::INTERNAL.raw_close(raw)
        bound&.close
      end
    end
  end
end
