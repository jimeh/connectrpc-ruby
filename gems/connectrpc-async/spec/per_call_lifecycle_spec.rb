# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/ruby", __dir__))
require "generation/streaming_connect"
require_relative "support/threaded_stream_helpers"
require_relative "support/per_call_helpers"

RSpec.describe "PerCallTransport instrumentation and fork" do
  include ThreadedStreamHelpers
  include PerCallHelpers

  # Records every callback of each observed call, keyed by call.
  def recording_bus(log)
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      entry = { call: call, received: 0, sent: 0 }
      log << entry
      observer = Object.new
      observer.define_singleton_method(:processing_finished) { |outcome| entry[:outcome] = outcome }
      observer.define_singleton_method(:finished) { |completion| entry[:completion] = completion }
      observer.define_singleton_method(:message_received) { |*| entry[:received] += 1 }
      observer.define_singleton_method(:message_sent) { |*| entry[:sent] += 1 }
      observer
    end
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  # Runs a Connect unary call, a gRPC server stream and a cancelled Connect
  # unary call, and returns each call's observed facts. The peer cancels the
  # held handler when the client resets its stream.
  def observe(transport, origin, entered)
    log = Queue.new
    source = ConnectRPC::CancellationSource.new
    typed = mixed_client(transport, origin, instrumentation: recording_bus(log))
    grpc = mixed_client(transport, origin, protocol: :grpc, instrumentation: recording_bus(log))
    canceller = Thread.new do
      entered.pop
      source.cancel
    end
    results = [typed.lookup(item("ok")).message.name, grpc.watch(item("ss")) { |reader| reader.each { nil } },
               capture_error { typed.lookup(item("held"), cancellation: source.token) }.code]
    canceller.join(10)
    entries = []
    entries << log.pop until log.empty?
    facts = entries.map do |entry|
      call = entry.fetch(:call)
      outcome = entry.fetch(:outcome)
      completion = entry.fetch(:completion)
      [call.protocol, call.url_scheme, call.url_path, call.server_address, call.server_port, outcome.code,
       outcome.http_status, entry[:sent], entry[:received], completion.delivery, completion.processing_state]
    end
    [results, entries.map { |entry| entry.fetch(:call).transport_class }, facts]
  end

  PerCallHelpers::PATHS.each do |path|
    it "reports the same RPC facts as the #{path == :inline ? 'H2 Transport' : 'ThreadedTransport'} on the " \
       "#{path} path, naming the public class" do
      release = Thread::Queue.new
      entered = Thread::Queue.new
      peer = threaded_host(server: mixed_server(
        lookup: lambda { |message, _|
          if message.name == "held"
            entered << true
            release.pop
          end
          message
        },
        watch: ->(message, writer, _) { 3.times { writer.send_message(message) } }
      ))
      origin = "http://127.0.0.1:#{peer.peer.port}"
      observed = on_path(path) { observe(per_call, origin, entered) }
      reference = on_path(path) do
        transport = path == :inline ? ConnectRPC::Async::Transport.new(origin: origin) : threaded(origin: origin)
        observe(transport, origin, entered)
      ensure
        transport&.close(grace: 0)
      end
      raise reference if reference.is_a?(Exception)

      expect(observed.first).to eq(["ok", nil, "canceled"])
      expect(observed[1]).to eq([PerCallHelpers::PER_CALL] * 3)
      expect(observed.last).to eq(reference.last)
      expect(observed.last.map { |facts| facts.values_at(0, 5, 7, 8) })
        .to eq([[:connect, "OK", 1, 1], [:grpc, "OK", 1, 3], [:connect, "canceled", 1, 0]])
    ensure
      3.times { release&.<<(true) }
      peer&.stop
    end
  end

  context "across fork" do
    before { skip "Process.fork is unavailable" unless Process.respond_to?(:fork) }

    # Runs the block in a child that exits without at_exit hooks and returns
    # its report. The parent's wait is bounded and a hung child is killed.
    def in_child
      reader, writer = IO.pipe
      pid = Process.fork do
        reader.close
        report = begin
          yield
        rescue Exception => e # rubocop:disable Lint/RescueException -- report every child failure to the parent
          "#{e.class}: #{e.message}"
        end
        writer.write(report.to_s)
        writer.close
        exit!(0)
      end
      writer.close
      status = Process.detach(pid).join(20)&.value
      Process.kill("KILL", pid) unless status
      [status&.success?, reader.read]
    ensure
      reader&.close
    end

    %i[h2c tls].each do |mode|
      it "gives a child forked during a parent call its own fresh call over #{mode}" do
        with_ca do |ca_file|
          tls, scheme, options = threaded_mode(mode, ca_file)
          entered = Thread::Queue.new
          release = Thread::Queue.new
          peer = threaded_host(tls: tls, server: server(echo: lambda { |message, _|
            entered << message.name
            release.pop if message.name == "held"
            message
          }))
          origin = "#{scheme}://127.0.0.1:#{peer.peer.port}"
          typed = client(per_call(**options), origin)
          held = Thread.new { typed.echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: 10).message.name }
          expect(pop_within(entered, "the held call in the handler")).to eq("held")
          child = in_child { typed.echo(AsyncHelpers::MESSAGE.new(name: "child"), timeout: 10).message.name }
          expect(child).to eq([true, "child"])
          release << true
          expect(value_within(held, "the held parent call")).to eq("held")
          expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "after")).message.name).to eq("after")
          expect(peer.peer.accepts.size).to eq(3)
        ensure
          release&.<<(true)
          peer&.stop
        end
      end
    end
  end
end
