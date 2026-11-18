# frozen_string_literal: true

RSpec.describe ConnectRPC::Health::Registry do
  subject(:registry) { described_class.new }

  def list(target = registry)
    target.handlers.fetch(HealthHelpers::HEALTH::LIST.path)
          .call(Grpc::Health::V1::HealthListRequest.new, ConnectRPC::Context.new)
          .statuses.each_with_object({}) { |(name, response), result| result[name] = response.status }
  end

  def error_code
    raise_error(ConnectRPC::Error) { |error| yield error.code }
  end

  describe "status and Check" do
    it "starts the overall server entry serving and reports registered names" do
      expect(registry.status("")).to eq(:serving)
      expect(check(registry, "")).to eq(:SERVING)
      expect(registry.set("demo.v1.Demo", :not_serving)).to be(true)
      expect(registry.status("demo.v1.Demo")).to eq(:not_serving)
      expect(check(registry, "demo.v1.Demo")).to eq(:NOT_SERVING)
      registry.set("demo.v1.Demo", :unknown)
      expect(check(registry, "demo.v1.Demo")).to eq(:UNKNOWN)
    end

    it "fails Check for an unregistered name with not_found" do
      expect(registry.status("missing")).to be_nil
      expect { check(registry, "missing") }.to(error_code { |code| expect(code).to eq("not_found") })
    end

    it "stores a frozen copy of the service name" do
      name = +"demo.v1.Demo"
      registry.set(name, :serving)
      name << ".changed"
      expect(registry.status("demo.v1.Demo")).to eq(:serving)
      expect(registry.status("demo.v1.Demo.changed")).to be_nil
    end

    it "rejects invalid names, statuses and watcher limits" do
      [:demo, nil, "\xFF".b, "\xFF".dup.force_encoding(Encoding::UTF_8)].each do |name|
        expect { registry.set(name, :serving) }.to raise_error(ArgumentError)
        expect { registry.status(name) }.to raise_error(ArgumentError)
      end
      [:service_unknown, "serving", :SERVING, nil].each do |status|
        expect { registry.set("demo", status) }.to raise_error(ArgumentError)
      end
      [0, -1, 1.5, nil, "1"].each do |limit|
        expect { described_class.new(max_watchers: limit) }.to raise_error(ArgumentError)
      end
      expect(registry.status("demo")).to be_nil
    end
  end

  describe "List" do
    it "returns a snapshot of every entry" do
      registry.set("a", :not_serving)
      registry.set("b", :unknown)
      expect(list).to eq("" => :SERVING, "a" => :NOT_SERVING, "b" => :UNKNOWN)
    end

    it "lists 100 entries and fails resource_exhausted above that" do
      99.times { |index| registry.set("service-#{index}", :serving) }
      expect(list.size).to eq(100)
      registry.set("service-99", :serving)
      expect { list }.to(error_code { |code| expect(code).to eq("resource_exhausted") })
    end
  end

  describe "shutdown" do
    it "marks every entry not serving, ignores later sets and is idempotent" do
      registry.set("demo", :serving)
      expect(registry.shutdown?).to be(false)
      expect(registry.shutdown).to be_nil
      expect(registry.shutdown).to be_nil
      expect(registry.shutdown?).to be(true)
      expect(registry.set("demo", :serving)).to be(false)
      expect(registry.set("late", :serving)).to be(false)
      expect(list).to eq("" => :NOT_SERVING, "demo" => :NOT_SERVING)
      expect(registry.status("late")).to be_nil
    end
  end

  describe "Watch" do
    it "sends the current status, then each change, and ends after shutdown" do
      registry.set("demo", :serving)
      writer = HealthHelpers::RecordingWriter.new
      call = watch_call(registry, "demo", writer)
      expect(writer.next_status).to eq(:SERVING)
      expect(registry.watcher_count).to eq(1)
      registry.set("demo", :not_serving)
      expect(writer.next_status).to eq(:NOT_SERVING)
      registry.set("other", :not_serving)
      registry.set("demo", :serving)
      expect(writer.next_status).to eq(:SERVING)
      registry.shutdown
      expect(writer.next_status).to eq(:NOT_SERVING)
      expect(call.outcome).to eq(:ok)
      expect(writer.sent).to be_empty
      expect(registry.watcher_count).to eq(0)
    end

    it "sends SERVICE_UNKNOWN for an unregistered name and its status once registered" do
      writer = HealthHelpers::RecordingWriter.new
      call = watch_call(registry, "later", writer)
      expect(writer.next_status).to eq(:SERVICE_UNKNOWN)
      registry.set("later", :serving)
      expect(writer.next_status).to eq(:SERVING)
      registry.shutdown
      expect(writer.next_status).to eq(:NOT_SERVING)
      expect(call.outcome).to eq(:ok)
    end

    it "ends an unregistered watcher at shutdown without another message" do
      writer = HealthHelpers::RecordingWriter.new
      call = watch_call(registry, "missing", writer)
      expect(writer.next_status).to eq(:SERVICE_UNKNOWN)
      registry.shutdown
      expect(call.outcome).to eq(:ok)
      expect(writer.sent).to be_empty
    end

    it "does not repeat NOT_SERVING at shutdown when it was the last status sent" do
      registry.set("demo", :not_serving)
      writer = HealthHelpers::RecordingWriter.new
      call = watch_call(registry, "demo", writer)
      expect(writer.next_status).to eq(:NOT_SERVING)
      registry.shutdown
      expect(call.outcome).to eq(:ok)
      expect(writer.sent).to be_empty
    end

    it "coalesces updates made while the writer is blocked to the latest status" do
      registry.set("demo", :serving)
      gate = Thread::Queue.new
      writer = HealthHelpers::RecordingWriter.new(gate: gate)
      call = watch_call(registry, "demo", writer)
      expect(writer.next_status).to eq(:SERVING)
      # The handler is parked inside send_message until the gate opens.
      registry.set("demo", :not_serving)
      registry.set("demo", :unknown)
      gate << true
      expect(writer.next_status).to eq(:UNKNOWN)
      registry.shutdown
      gate << true
      expect(writer.next_status).to eq(:NOT_SERVING)
      gate << true
      expect(call.outcome).to eq(:ok)
    end

    it "skips a coalesced status equal to the last one sent" do
      registry.set("demo", :serving)
      now = 0.0
      context = ConnectRPC::Context.new(deadline: ConnectRPC::Deadline.new(60, clock: -> { now }))
      gate = Thread::Queue.new
      writer = HealthHelpers::RecordingWriter.new(gate: gate)
      call = watch_call(registry, "demo", writer, context)
      expect(writer.next_status).to eq(:SERVING)
      registry.set("demo", :not_serving)
      registry.set("demo", :serving)
      # The deadline ends the stream without overwriting the pending slot.
      now = 61.0
      gate << true
      expect(call.outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "deadline_exceeded"))
      expect(writer.sent).to be_empty
    end

    it "fails deadline_exceeded when the deadline passes while waiting" do
      # The injected clock keeps the deadline open until the first status is
      # sent; the waiter then observes expiry when its bounded wait returns.
      now = 0.0
      context = ConnectRPC::Context.new(deadline: ConnectRPC::Deadline.new(0.05, clock: -> { now }))
      writer = HealthHelpers::RecordingWriter.new
      call = watch_call(registry, "", writer, context)
      expect(writer.next_status).to eq(:SERVING)
      now = 1.0
      expect(call.outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "deadline_exceeded"))
      expect(registry.watcher_count).to eq(0)
    end

    it "admits up to max_watchers and admits again once a slot frees" do
      limited = described_class.new(max_watchers: 2)
      sources = Array.new(2) { ConnectRPC::CancellationSource.new }
      writers = Array.new(2) { HealthHelpers::RecordingWriter.new }
      calls = sources.zip(writers).map do |source, writer|
        watch_call(limited, "", writer, ConnectRPC::Context.new(cancellation: source.token))
      end
      writers.each { |writer| expect(writer.next_status).to eq(:SERVING) }
      rejected = HealthHelpers::RecordingWriter.new
      outcome = watch_call(limited, "", rejected).outcome
      expect(outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "resource_exhausted"))
      expect(rejected.sent).to be_empty
      sources.first.cancel
      expect(calls.first.outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "canceled"))
      expect(limited.watcher_count).to eq(1)
      readmitted = HealthHelpers::RecordingWriter.new
      watch_call(limited, "", readmitted)
      expect(readmitted.next_status).to eq(:SERVING)
      limited.shutdown
      expect(calls.last.outcome).to eq(:ok)
    end

    it "fails unavailable after shutdown before sending anything" do
      registry.shutdown
      writer = HealthHelpers::RecordingWriter.new
      outcome = watch_call(registry, "", writer).outcome
      expect(outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "unavailable"))
      expect(writer.sent).to be_empty
      expect(registry.watcher_count).to eq(0)
    end

    it "releases its slot when a send fails" do
      gate = Thread::Queue.new
      writer = HealthHelpers::RecordingWriter.new(gate: gate)
      call = watch_call(registry, "", writer)
      expect(writer.next_status).to eq(:SERVING)
      expect(registry.watcher_count).to eq(1)
      gate << IOError.new("peer gone")
      expect(call.outcome).to be_a(IOError)
      expect(registry.watcher_count).to eq(0)
    end
  end

  describe "Watch cancellation" do
    it "fails canceled without sending when the call is already cancelled" do
      source = ConnectRPC::CancellationSource.new
      source.cancel
      writer = HealthHelpers::RecordingWriter.new
      outcome = watch_call(registry, "", writer, ConnectRPC::Context.new(cancellation: source.token)).outcome
      expect(outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "canceled"))
      expect(writer.sent).to be_empty
      expect(registry.watcher_count).to eq(0)
    end

    it "does not lose a cancellation that arrives while the handler is not waiting" do
      source = ConnectRPC::CancellationSource.new
      writer = HealthHelpers::RecordingWriter.new
      writer.define_singleton_method(:send_message) do |message|
        super(message)
        # Cancels synchronously in the handler's own thread, before it waits.
        source.cancel
      end
      call = watch_call(registry, "", writer, ConnectRPC::Context.new(cancellation: source.token))
      expect(writer.next_status).to eq(:SERVING)
      expect(call.outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "canceled"))
      expect(registry.watcher_count).to eq(0)
    end

    it "does not lose a cancellation racing the handler's wait" do
      source = ConnectRPC::CancellationSource.new
      cancellers = []
      condition_class = Class.new(ConditionVariable) do
        define_method(:wait) do |mutex, timeout = nil|
          # The canceller needs the registry mutex, so it can only run once
          # this waiter is queued and has released it inside the real wait.
          cancellers << Thread.new { source.cancel } if cancellers.empty?
          super(mutex, timeout)
        end
      end
      allow(ConditionVariable).to receive(:new).and_return(condition_class.new)
      writer = HealthHelpers::RecordingWriter.new
      call = watch_call(registry, "", writer, ConnectRPC::Context.new(cancellation: source.token))
      expect(writer.next_status).to eq(:SERVING)
      expect(call.outcome).to be_a(ConnectRPC::Error).and(have_attributes(code: "canceled"))
      expect(cancellers.size).to eq(1)
      expect(cancellers.first.join(5)).to be_truthy
      expect(registry.watcher_count).to eq(0)
    end

    it "completes when its listener is still running as the stream ends" do
      source = ConnectRPC::CancellationSource.new
      gate = Thread::Queue.new
      writer = HealthHelpers::RecordingWriter.new(gate: gate)
      call = watch_call(registry, "", writer, ConnectRPC::Context.new(cancellation: source.token))
      expect(writer.next_status).to eq(:SERVING)
      lock = registry.instance_variable_get(:@mutex)
      lock.lock
      begin
        # The claimed listener blocks on the registry mutex while it runs.
        canceller = Thread.new { source.cancel }
        eventually("listener blocked on the registry") { canceller.status == "sleep" }
        # The stream now ends: unregister must not wait for the running listener.
        gate << IOError.new("peer gone")
        eventually("watch cleanup blocked on the registry") do
          gate.num_waiting.zero? && call.thread.status == "sleep"
        end
      ensure
        lock.unlock
      end
      expect(call.outcome).to be_a(IOError)
      expect(canceller.join(5)).to be_truthy
      expect(registry.watcher_count).to eq(0)
    end
  end

  describe "handlers" do
    it "maps the three generated method paths and wraps without allocating a watcher" do
      expect(registry.handlers.keys).to eq(HealthHelpers::HEALTH::SERVICE.rpcs.map(&:path))
      expect(registry.handlers).to be_frozen
      watch = registry.handlers.fetch(HealthHelpers::HEALTH::WATCH.path)
      guarded = lambda do |request, writer, context|
        unless context.request_metadata["authorization"] == ["Bearer probe"]
          raise ConnectRPC::Error.new(:permission_denied, "denied")
        end

        watch.call(request, writer, context)
      end
      writer = HealthHelpers::RecordingWriter.new
      expect { guarded.call(HealthHelpers::REQUEST.new(service: ""), writer, ConnectRPC::Context.new) }
        .to(error_code { |code| expect(code).to eq("permission_denied") })
      expect(writer.sent).to be_empty
      expect(registry.watcher_count).to eq(0)
    end

    it "exposes the generated service" do
      expect(ConnectRPC::Health::SERVICE).to equal(Grpc::Health::V1::Health::SERVICE)
      expect(ConnectRPC::Health::SERVICE.name).to eq("grpc.health.v1.Health")
    end
  end
end
