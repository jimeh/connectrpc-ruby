# frozen_string_literal: true

require "json"
require "stringio"
require_relative "../../../conformance/native_server"

# The official conformance server adapter runs in one Async reactor, and its
# fibers share the telemetry file with the runner's other adapter processes.
RSpec.describe "conformance server adapter" do
  around do |example|
    Dir.mktmpdir do |directory|
      @target = File.join(directory, "telemetry.jsonl")
      previous = ENV.fetch("CONFORMANCE_TELEMETRY", nil)
      ENV["CONFORMANCE_TELEMETRY"] = @target
      example.run
    ensure
      ENV["CONFORMANCE_TELEMETRY"] = previous
    end
  end

  def recorded = File.readlines(@target).map { |line| JSON.parse(line) }

  def raising_host
    Object.new.tap do |host|
      host.define_singleton_method(:close) { |grace:| raise Async::Cancel, "closing at #{grace}" }
    end
  end

  # Ruby 4's IO#write can yield to the fiber scheduler of a non-blocking fiber
  # while the writer holds the file lock. The stub yields the same way. A
  # second writer that could start then would block the reactor in flock, or,
  # waiting any other way, could be cancelled before recording, as an
  # unwinding observer's can be.
  it "completes a locked write before a second fiber can start or cancel its own" do
    reached = false
    allow(File).to receive(:open).and_wrap_original do |original, path, *arguments, &block|
      next original.call(path, *arguments, &block) unless path == @target

      original.call(path, *arguments) do |file|
        file.define_singleton_method(:puts) do |*lines|
          unless reached
            reached = true
            Fiber.current_scheduler&.yield
          end
          super(*lines)
        end
        block.call(file)
      end
    end

    second = nil
    worker = Thread.new do
      Sync do |task|
        first = task.async { Conformance::Telemetry.record("first") }
        second = task.async { Conformance::Telemetry.record("second") }
        second.cancel
        [first, second].each(&:wait)
      end
    end
    joined = worker.join(5)
    worker.kill unless joined

    expect(joined).to equal(worker), "a second writer blocked the reactor holding the suspended first writer"
    expect(reached).to be(true)
    expect(second).to be_completed
    expect(recorded.map { |event| event.fetch("event") }).to eq(%w[first second])
  end

  it "closes the listener and records the stop and the escape when closing the host raises" do
    listener = Object.new
    listener.define_singleton_method(:close) { @closed = true }

    expect do
      expect do
        Conformance::NativeServerAdapter.stop_native_host({ host: raising_host, bound: listener, ready: true },
                                                          Conformance::NativeStallReport.new)
      end.to raise_error(Async::Cancel)
    end.to output(/Native server close failed: .*closing at 0/m).to_stderr
    expect(listener.instance_variable_get(:@closed)).to be(true)
    expect(recorded.map { |event| event.slice("event", "error_class", "message") }).to eq(
      [{ "event" => "server_close_failed", "error_class" => "Async::Cancel", "message" => "closing at 0" },
       { "event" => "server_stopped" }]
    )
  end

  it "stops a ready adapter through the close path on SIGTERM" do
    host = raising_host
    allow(Conformance::NativeServerAdapter).to receive(:start_native_host) do |_adapter, _config, owned|
      owned[:host] = host
      Process.kill("TERM", Process.pid)
      4321
    end
    input = StringIO.new
    Conformance::Framing.write(input, Conformance::ServerCompatRequest.new(protocol: :PROTOCOL_GRPC,
                                                                           http_version: :HTTP_VERSION_2))
    input.rewind

    worker = joined = nil
    expect do
      worker = Thread.new { Conformance::NativeServerAdapter.run(input, StringIO.new) }
      joined = worker.join(10)
      worker.kill unless joined
    end.to output(/Native server close failed/).to_stderr

    expect(joined).to equal(worker), "the adapter did not stop after SIGTERM"
    expect(recorded.map { |event| event.fetch("event") }).to eq(%w[server_ready server_close_failed server_stopped])
  end
end
