# frozen_string_literal: true

RSpec.describe "Native admission and forced teardown observation" do
  def record_calls
    calls = []
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      events = []
      calls << [call, events]
      observer = Object.new
      observer.define_singleton_method(:processing_finished) { |outcome| events << [:processing, outcome] }
      observer.define_singleton_method(:finished) { |completion| events << [:finished, completion] }
      observer
    end
    [calls, subscriber]
  end

  %i[completed aborted].each do |delivery|
    it "records admission rejection with #{delivery} output and no fabricated processing callback" do
      _provider, exporter, bus = tracing
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        target = server(instrumentation: bus, echo: lambda { |message, _|
          if message.name == "admitted"
            entered.enqueue(true)
            release.dequeue
          end
          message
        })
        host = ConnectRPC::Async::Host.new(server: target, max_requests: 1).start
        origin = "http://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin)
        typed = client(transport, origin)
        second_transport = ConnectRPC::Async::Transport.new(origin: origin)
        second = client(second_transport, origin)
        second.echo(AsyncHelpers::MESSAGE.new(name: "warmup"))
        exporter.reset
        active = task.async { typed.echo(AsyncHelpers::MESSAGE.new(name: "admitted"), timeout: nil) }
        entered.dequeue
        resource = host_session(host).instance_variable_get(:@resources).first
        if delivery == :aborted
          gate = FrameGate.new(resource.socket, frame_type: 1)
          resource.connection.stream.instance_variable_set(:@io, gate)
          gate.armed = true
        end
        rejected = task.async do
          second.echo(AsyncHelpers::MESSAGE.new(name: "rejected"), timeout: nil)
        rescue ConnectRPC::Error => e
          e
        end
        if gate
          gate.started.dequeue
          # The rejection's header frame has started. Force teardown of its
          # independently owned connection before the frame can finish.
          resource.abort
          expect(rejected.wait.code).to eq("unavailable")
          gate.release.enqueue(true)
        else
          expect(rejected.wait.code).to eq("unavailable")
        end
        wait_until(task, "admission span completion") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.attributes).to include("connectrpc.processing.state" => "not_started",
                                           "connectrpc.decision" => "admission_rejected",
                                           "connectrpc.delivery" => delivery.to_s,
                                           "http.response.status_code" => 503)
        expect(span.attributes).not_to have_key("rpc.response.status_code")
        release.enqueue(true)
        expect(active.wait.message.name).to eq("admitted")
      ensure
        gate&.release&.enqueue(true)
        release&.enqueue(true)
        second_transport&.close(grace: 0)
        transport&.close(grace: 0)
        host&.close(grace: 0)
      end
    end
  end

  it "latches wire abort until running processing cleanup finishes and emits exactly once" do
    calls, recorder = record_calls
    _provider, exporter, bus = tracing(subscribers: [recorder])
    reactor do |task|
      entered = Async::Queue.new
      cleaning = Async::Queue.new
      release = Async::Queue.new
      target = server(instrumentation: bus, echo: lambda { |message, _|
        begin
          entered.enqueue(true)
          Async::Condition.new.wait
          message
        ensure
          cleaning.enqueue(true)
          release.dequeue
        end
      })
      host = ConnectRPC::Async::Host.new(server: target).start
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      typed = client(transport, origin)
      call = task.async do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "blocked"), timeout: nil)
      rescue ConnectRPC::Error
        nil
      end
      entered.dequeue
      closing = task.async { host.close(grace: 0) }
      cleaning.dequeue
      expect(calls.fetch(0).last).to be_empty
      expect(rpc_spans(exporter)).to be_empty
      release.enqueue(true)
      closing.wait
      call.wait
      events = calls.fetch(0).last
      expect(events.map(&:first)).to eq(%i[processing finished])
      expect(events.last.last).to have_attributes(delivery: :aborted, processing_state: :finished)
      expect(rpc_spans(exporter).size).to eq(1)
      expect(current_span.context).not_to be_valid
    ensure
      release&.enqueue(true)
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
  it "aborts Falcon HTTP/1 cancellation during body completion callbacks even after wire bytes arrive" do
    _provider, exporter, bus = tracing
    reactor do |task|
      callback_entered = Async::Queue.new
      callback_release = Async::Queue.new
      callback_body = Class.new(Protocol::HTTP::Body::Wrapper) do
        define_method(:close) do |error = nil|
          return if @closed

          @closed = true
          begin
            callback_entered.enqueue(true)
            callback_release.dequeue
          ensure
            super(error)
          end
        end
      end
      backend = Class.new(ConnectRPC::Falcon::Server) do
        define_method(:call) do |request, **options|
          response = super(request, **options)
          response.body = callback_body.new(response.body) if response.body
          response
        end
      end
      bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
      host = backend.new(server: server(instrumentation: bus), endpoint: bound)
      host.run
      origin = "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
      reading = task.async do
        http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", headers: { "content-type" => "application/proto" },
                                                     body: AsyncHelpers::MESSAGE.new(name: "callbacks").to_proto)
      end
      callback_entered.dequeue
      expect(reading.wait.first).to eq(200)
      expect(rpc_spans(exporter)).to be_empty
      expect(host.close(grace: 0)).to be(false)
      span = rpc_spans(exporter).fetch(0)
      expect(span.attributes).to include("rpc.response.status_code" => "OK", "connectrpc.delivery" => "aborted")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
    ensure
      callback_release&.enqueue(true)
      host&.close(grace: 0)
      bound&.close
    end
  end
  it "finishes an RPC as aborted when Falcon HTTP/1 response headers fail before write_body" do
    _provider, exporter, bus = tracing
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      target = server(instrumentation: bus, echo: lambda { |message, _|
        entered.enqueue(true)
        release.dequeue
        message
      })
      falcon(server: target) do |host, origin|
        reading = task.async do
          http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST",
                                                       headers: { "content-type" => "application/proto" }, body: AsyncHelpers::MESSAGE.new.to_proto)
        rescue IOError
          :closed
        end
        entered.dequeue
        connection = host_session(host).instance_variable_get(:@resources).first.connection
        # Fault the external write boundary after real admission and processing began.
        connection.define_singleton_method(:write_response) { |*| raise IOError, "header write failed" }
        release.enqueue(true)
        expect(reading.wait).to eq(:closed)
        wait_until(task, "header-failure completion") { rpc_spans(exporter).size == 1 }
        span = rpc_spans(exporter).first
        expect(span.attributes).to include("rpc.response.status_code" => "OK", "connectrpc.delivery" => "aborted")
      end
    ensure
      release&.enqueue(true)
    end
  end
end
