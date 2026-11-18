# frozen_string_literal: true

require "json"
require_relative "../../../conformance/telemetry"
require_relative "../../../conformance/native_server_process"

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

  # Ruby 4's IO#write can yield to the fiber scheduler while a writer holds
  # the file lock, so this suspends the first writer there.
  it "lets a second fiber record telemetry while the first is suspended inside its locked write" do
    suspended = false
    allow(File).to receive(:open).and_wrap_original do |original, path, *arguments, &block|
      next original.call(path, *arguments, &block) unless path == @target

      original.call(path, *arguments) do |file|
        file.define_singleton_method(:puts) do |*lines|
          unless suspended
            suspended = true
            Async::Task.current.yield
          end
          super(*lines)
        end
        block.call(file)
      end
    end

    worker = Thread.new do
      Sync do |task|
        first = task.async { Conformance::Telemetry.record("first") }
        second = task.async { Conformance::Telemetry.record("second") }
        [first, second].each(&:wait)
      end
    end
    joined = worker.join(5)
    worker.kill unless joined

    expect(joined).to equal(worker), "a second writer blocked the reactor holding the suspended first writer"
    expect(suspended).to be(true)
    expect(recorded.map { |event| event.fetch("event") }).to eq(%w[first second])
  end

  it "records the stop and the escape when closing the host raises a cancellation" do
    host = Object.new
    host.define_singleton_method(:close) { |grace:| raise Async::Cancel, "closing at #{grace}" }

    expect do
      Conformance::NativeServerAdapter.stop_native_host(host, nil, Conformance::NativeStallReport.new)
    end.to raise_error(Async::Cancel)
    expect(recorded.map { |event| event.slice("event", "error_class", "message") }).to eq(
      [{ "event" => "server_close_failed", "error_class" => "Async::Cancel", "message" => "closing at 0" },
       { "event" => "server_stopped" }]
    )
  end
end
