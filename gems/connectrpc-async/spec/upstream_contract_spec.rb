# frozen_string_literal: true

require "stringio"
require_relative "support/perturbed_require"

# Each example pins one upstream behavior or hook. "Relied on" failures mean
# this package is broken on the installed stack. "Worked around" failures mean
# upstream fixed something; the description names the workaround to delete.
RSpec.describe "connectrpc-async upstream contract" do
  include PerturbedRequire

  def contract = AsyncHelpers::INTERNAL.const_get(:UpstreamContract)

  def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  # A per-instance #consume observer, installed like CompletionJoin's.
  def observe_consume(task, &observer)
    seen = []
    spy = Module.new do
      define_method(:consume) do
        result = super()
        seen << [fiber.nil?, finished?]
        observer&.call(self)
        result
      end
    end
    task.extend(spy)
    seen
  end

  def end_task(task, exit)
    if exit == :cancel
      expect(task).to be_alive
      task.cancel
    end
    task.wait
  rescue ArgumentError
    nil
  end

  def task_body(exit, gate)
    case exit
    when :raise then raise ArgumentError, "probe failure"
    when :cancel then gate.wait
    end
  end

  # Our client and server connections over a socket pair, with no resource.
  def connection_pair(task)
    left, right = Socket.pair(:UNIX, :STREAM, 0)
    server = AsyncHelpers::INTERNAL::ServerConnection.new(IO::Stream(left))
    client = AsyncHelpers::INTERNAL::ClientConnection.new(IO::Stream(right))
    preface = task.async(finished: false) { server.read_connection_preface(AsyncHelpers::INTERNAL.settings(16)) }
    client.send_connection_preface(AsyncHelpers::INTERNAL.settings(16))
    preface.wait
    server.read_in_background(parent: task)
    client.read_in_background(parent: task)
    yield server, client
  ensure
    client&.close
    server&.close
    [left, right].each { |socket| AsyncHelpers::INTERNAL.raw_close(socket) }
  end

  def open_request(server, client, body)
    response = client.create_response
    client.write_request(response, Protocol::HTTP::Request["POST", "/contract", {}, body, scheme: "http",
                                                                                          authority: "localhost"])
    [response, server.requests.dequeue]
  end

  def require_async(perturbation) = require_perturbed("connectrpc/async", perturbation)

  def expect_async_error(output, *lines, **)
    expect_contract_error(output, "connectrpc-async", ConnectRPC::Async::VERSION, *lines, **)
  end

  describe "relied on" do
    it "passes the boot-time hook and finalization checks on the installed stack" do
      expect(contract.hook_failures(contract::HOOKS)).to eq([])
      expect(contract.finalization_failures).to eq([])
    end

    it "names only tested versions that the gemspec allows" do
      gemspec = Gem::Specification.load(File.expand_path("../connectrpc-async.gemspec", __dir__))
      requirements = gemspec.runtime_dependencies.to_h { |dependency| [dependency.name, dependency.requirement] }
      contract::TESTED.each do |gem, version|
        expect(requirements.fetch(gem)).to be_satisfied_by(Gem::Version.new(version)), gem
      end
    end

    it "reports a changed, moved or missing hook with the upstream gem and version" do
      output = "async-http #{Async::HTTP::VERSION}: Async::HTTP::Protocol::HTTP2::Output#stream expected public " \
               "stream(_) from Async::HTTP::Protocol::HTTP2::Output, found private stream(_) from " \
               "Async::HTTP::Protocol::HTTP2::Output"
      failures = contract.hook_failures(
        [["async-http", "Async::HTTP::Protocol::HTTP2::Output", :stream, :public, "(_)",
          "Async::HTTP::Protocol::HTTP2::Output"],
         ["async-http", "Async::HTTP::Protocol::HTTP2::Output", :start, :public, "(parent: ?)", "Object"],
         ["async-http", "Async::HTTP::Protocol::HTTP2::Missing", :start, :public, "()", "Object"]]
      )
      expect(failures[0]).to eq(output)
      expect(failures[1]).to end_with("found public start(parent: ?) from Async::HTTP::Protocol::HTTP2::Output")
      expect(failures[2]).to end_with("found nothing")
    end

    it "reports a hook for a gem without a known version constant as an unknown version" do
      failures = contract.hook_failures([["unlisted-gem", "Async::HTTP::Protocol::HTTP2::Missing", :start, :public,
                                          "()", "Object"]])
      expect(failures).to eq(["unlisted-gem (unknown version): Async::HTTP::Protocol::HTTP2::Missing#start " \
                              "expected public start() from Object, found nothing"])
    end

    it "bounds a stuck finalization probe and names the step it was in" do
      probe = contract.const_get(:FinalizationProbe)
      allow_any_instance_of(probe).to receive(:call) { Thread::Queue.new.pop }
      started = now
      failures = contract.finalization_failures(0.2)
      expect(now - started).to be < 0.45
      expect(failures).to eq(["async #{Async::VERSION}: Task finalization probe did not finish within 0.2 s " \
                              "while starting its reactor"])
    end

    # A kill grace added on top of the budget would take 0.7 s here.
    it "keeps a probe thread that ignores kill within the budget and says it could not stop" do
      probe = contract.const_get(:FinalizationProbe)
      release = Thread::Queue.new
      allow_any_instance_of(probe).to receive(:call) do
        Thread::Queue.new.pop
      ensure
        release.pop
      end
      started = now
      failures = contract.finalization_failures(0.2)
      expect(now - started).to be < 0.45
      expect(failures).to eq(["async #{Async::VERSION}: Task finalization probe did not finish within 0.2 s " \
                              "while starting its reactor, and Async could not stop the probe thread"])
    ensure
      release&.push(nil)
    end

    it "reports a probe crash as a broken contract" do
      probe = contract.const_get(:FinalizationProbe)
      allow_any_instance_of(probe).to receive(:call).and_raise(NoMethodError, "undefined method 'consume'")
      expect(contract.finalization_failures).to eq(["async #{Async::VERSION}: Task finalization probe raised " \
                                                    "NoMethodError while starting its reactor: undefined method " \
                                                    "'consume'"])
    end

    %i[return raise cancel].each do |exit|
      it "calls a per-instance #consume with the fiber cleared when a task ends by #{exit}" do
        reactor do |root|
          gate = Async::Promise.new
          task = Async::Task.new(root, finished: false) { task_body(exit, gate) }
          seen = observe_consume(task)
          task.run
          end_task(task, exit)
          expect(seen).to include([true, true])
          expect(task).to be_finished
        end
      end

      it "calls a parent's per-instance #consume when its last child finishes after the parent ends by #{exit}" do
        reactor do |root|
          child_gate = Async::Promise.new
          parent_gate = Async::Promise.new
          started = Async::Promise.new
          child = nil
          parent = Async::Task.new(root, finished: false) do |task|
            child = task.async(finished: false) { |inner| inner.defer_cancel { child_gate.wait } }
            started.resolve(true)
            task_body(exit, parent_gate)
          end
          seen = observe_consume(parent)
          parent.run
          started.wait
          end_task(parent, exit)
          expect(parent.fiber).to be_nil
          expect(parent).not_to be_finished
          expect(seen.map(&:last)).not_to include(true)
          child_gate.resolve(true)
          child.wait
          expect(seen).to include([true, true])
          expect(parent).to be_finished
        end
      end
    end

    it "transfers an object pushed from #consume on a later iteration, after the task's fiber is dead" do
      reactor do |root|
        done = Async::Promise.new
        events = []
        fiber = nil
        pushed = Object.new
        pushed.define_singleton_method(:alive?) { !done.resolved? }
        pushed.define_singleton_method(:transfer) do
          events << [:transfer, fiber.alive?]
          done.resolve(true)
        end
        task = Async::Task.new(root, finished: false) { fiber = Fiber.current }
        observe_consume(task) do |target|
          next unless target.fiber.nil? && events.empty?

          Fiber.scheduler.push(pushed)
          events << :pushed
        end
        task.run
        done.wait
        expect(events).to eq([:pushed, [:transfer, false]])
      end
    end

    it "dispatches incoming streams through create_stream, so the server builds our RequestStream" do
      reactor do |task|
        connection_pair(task) do |server, client|
          response, incoming = open_request(server, client, Protocol::HTTP::Body::Buffered.wrap(["hello"]))
          expect(incoming.stream).to be_a(AsyncHelpers::INTERNAL::RequestStream)
          expect(response.stream).to be_a(AsyncHelpers::INTERNAL::ResponseStream)
        end
      end
    end

    it "stores request input in @input and the declared length in @length for upstream readers" do
      reactor do |task|
        connection_pair(task) do |server, client|
          _, incoming = open_request(server, client, Protocol::HTTP::Body::Buffered.wrap(["hello"]))
          stream = incoming.stream
          expect(stream.input).to be_a(AsyncHelpers::INTERNAL::Ingress)
          expect(stream.wait_for_input).to equal(stream.input)
          expect(stream.declared_length).to eq(5)
        end
      end
    end

    it "lets upstream Stream#closed stop the Output that send_body stores in @output" do
      reactor do |task|
        connection_pair(task) do |server, client|
          response, incoming = open_request(server, client, Protocol::HTTP::Body::Buffered.wrap(["hello"]))
          incoming.send_response(Protocol::HTTP::Response[200, {}, Protocol::HTTP::Body::Writable.new])
          output = incoming.stream.response_task
          expect(output).to be_alive
          response.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
          output.wait
          expect(output).to be_cancelled
        end
      end
    end

    it "lets upstream start_connection reuse the reader that read_in_background stores in @reader" do
      reactor do |task|
        connection_pair(task) do |_server, client|
          expect(client.reader_task).to be_alive
          expect(client.start_connection).to equal(client.reader_task)
        end
      end
    end

    it "lets upstream Output#stop stop the task that Output#start stores in @task" do
      reactor do |task|
        stream = Object.new
        stream.define_singleton_method(:finish_output) { |_error = nil| nil }
        output = AsyncHelpers::INTERNAL::Output.new(stream, Protocol::HTTP::Body::Writable.new)
        started = nil
        output.start(parent: task) { |owned| started = owned }
        expect(started).to be_alive
        output.stop(nil)
        started.wait
        expect(started).to be_cancelled
      end
    end

    it "routes every frame write through our guard and never acquires async-http's @write_frame_guard" do
      upstream = {}.compare_by_identity
      owned = {}.compare_by_identity
      acquired = []
      trace = TracePoint.new(:return, :call) do |event|
        if event.event == :return && event.method_id == :initialize
          case event.defined_class.name
          when "Async::HTTP::Protocol::HTTP2::Connection"
            upstream[event.self.instance_variable_get(:@write_frame_guard)] = true
          when "ConnectRPC::Async::Internal::Connection"
            owned[event.self.instance_variable_get(:@frame_guard)] = true
          end
        elsif event.event == :call && event.defined_class == Async::Semaphore &&
              %i[acquire async wait].include?(event.method_id)
          acquired << :upstream if upstream.key?(event.self)
          acquired << :owned if owned.key?(event.self)
        end
      end
      trace.enable do
        reactor do
          host = ConnectRPC::Async::Host.new(server: server).start
          origin = "http://127.0.0.1:#{host.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin)
          message = AsyncHelpers::MESSAGE.new(name: "frame guard")
          expect(client(transport, origin).echo(message).message).to eq(message)
        ensure
          transport&.close
          host&.close
        end
      end
      expect([upstream.size, owned.size]).to eq([2, 2])
      expect(acquired).to include(:owned)
      expect(acquired).not_to include(:upstream)
    end
  end

  describe "relied on by the owned HTTP/1 client stream" do
    # HTTP1Stream implements only the stream calls protocol-http1 makes.
    stream_calls = %i[gets read readpartial write flush close close_read close_write]

    let(:recorder) do
      Class.new(BasicObject) do
        def initialize(target, calls)
          @target = target
          @calls = calls
        end

        def method_missing(name, ...)
          @calls << name
          @target.__send__(name, ...)
        end

        def respond_to_missing?(name, include_private = false)
          @calls << :"respond_to?(#{name})"
          @target.respond_to?(name, include_private)
        end
      end
    end

    it "defines every stream call protocol-http1 makes" do
      stream_class = AsyncHelpers::INTERNAL.const_get(:HTTP1Stream)
      expect(stream_calls.reject { |name| stream_class.public_method_defined?(name) }).to eq([])
    end

    it "protocol-http1 drives chunked and fixed exchanges and close with only those stream calls" do
      left, right = Socket.pair(:UNIX, :STREAM, 0)
      left.timeout = 5
      right.write("HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n" \
                  "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nhi")
      calls = []
      stream = AsyncHelpers::INTERNAL.const_get(:HTTP1Stream).new(left)
      connection = Protocol::HTTP1::Connection.new(recorder.new(stream, calls))
      bodies = Array.new(2) do
        connection.write_request("localhost", "POST", "/contract", "HTTP/1.1", {})
        connection.write_body("HTTP/1.1", Protocol::HTTP::Body::Buffered.wrap(["x"]))
        connection.read_response("POST")[4].join
      end
      connection.close
      expect(bodies).to eq(%w[abc hi])
      expect(calls.uniq - stream_calls).to eq([])
    ensure
      [left, right].each { |socket| AsyncHelpers::INTERNAL.raw_close(socket) }
    end
  end

  describe "perturbed at require" do
    finish = <<~RUBY
      Async::Task.prepend(Module.new do
        private def finish!
          return super unless bypass?

          @fiber&.async_task = nil
          @fiber = nil
          @block = nil
          Async::Node.instance_method(:consume).bind_call(self)
        end
      end)
    RUBY

    it "fails require by name when Task finalization bypasses a per-instance #consume (P9)" do
      output = require_async("#{finish}\nAsync::Task.define_method(:bypass?) { true }")
      expect_async_error(output, "async #{Async::VERSION}: Task finalization after normal return no longer " \
                                 "calls a per-instance #consume once the task's fiber is cleared")
    end

    it "fails require by name when only cancellation bypasses a per-instance #consume" do
      output = require_async("#{finish}\nAsync::Task.define_method(:bypass?) { cancelled? }")
      expect_async_error(output, "async #{Async::VERSION}: Task finalization after cancellation no longer")
      expect(output).not_to include("after normal return")
    end

    it "fails require by name when Node#consume stops calling an ancestor's #consume" do
      output = require_async(<<~RUBY)
        Async::Node.prepend(Module.new do
          def consume
            return unless (parent = @parent) && finished?

            parent.__send__(:remove_child, self)
            if @children
              while (child = @children.shift)
                child.finished? ? child.__send__(:set_parent, nil) : parent.__send__(:add_child, child)
              end
              @children = nil
            end
            Async::Node.instance_method(:consume).bind_call(parent)
          end
        end)
      RUBY
      expect_async_error(output, "async #{Async::VERSION}: Task finalization no longer calls a parent's " \
                                 "per-instance #consume when its last child finishes")
    end

    it "fails require within its budget, keeping recorded failures, when broken consumption hangs teardown" do
      output = require_async("Async::Node.prepend(Module.new { def consume = nil })")
      seconds = contract::PROBE_SECONDS
      expect_async_error(output, "async #{Async::VERSION}: Task finalization no longer calls a parent's " \
                                 "per-instance #consume when its last child finishes after the parent ended by " \
                                 "normal return",
                         "async #{Async::VERSION}: Task finalization probe did not finish within #{seconds} s " \
                         "while closing its reactor, and Async could not stop the probe thread",
                         within: seconds + 0.5)
    end

    it "names the probe step that crashed, not the teardown after it" do
      output = require_async(<<~RUBY)
        Async::Task.prepend(Module.new { def cancel(...) = raise(NoMethodError, "undefined method 'cancel'") })
      RUBY
      expect_async_error(output, "async #{Async::VERSION}: Task finalization probe raised NoMethodError while " \
                                 "finalizing a task after cancellation: undefined method 'cancel'")
    end

    it "fails require by name when Scheduler#push transfers a pushed object inline" do
      output = require_async(<<~RUBY)
        Async::Scheduler.prepend(Module.new do
          def push(fiber)
            return super if fiber.is_a?(Fiber)

            fiber.transfer if fiber.alive?
          end
        end)
      RUBY
      expect_async_error(output, "async #{Async::VERSION}: Fiber.scheduler.push ran a pushed object inline")
    end

    it "fails require by name when a hook changes visibility or signature" do
      output = require_async(<<~RUBY)
        Async::HTTP::Protocol::HTTP2::Output.send(:public, :stream)
        Async::HTTP::Protocol::HTTP2::Output.class_eval { def start(parent) = parent }
      RUBY
      output_class = "Async::HTTP::Protocol::HTTP2::Output"
      expect_async_error(
        output,
        "async-http #{Async::HTTP::VERSION}: #{output_class}#stream expected private stream(_) from " \
        "#{output_class}, found public stream(_) from #{output_class}",
        "async-http #{Async::HTTP::VERSION}: #{output_class}#start expected public start(parent: ?) from " \
        "#{output_class}, found public start(_) from #{output_class}",
        "Verified with async #{contract::TESTED.fetch('async')}",
        "See https://github.com/jimeh/connectrpc-ruby/issues/68."
      )
    end
  end

  describe "worked around" do
    let(:framer) do
      Class.new do
        def initialize(&on_write) = @on_write = on_write
        def write_frame(frame) = @on_write&.call(frame)
        def flush = nil
      end
    end

    it "protocol-http2 consume_local_window lets the receive window go negative " \
       "(delete Connection#consume_local_window and the Stream#receive_data window check)" do
      connection = Protocol::HTTP2::Client.new(framer.new)
      frame = Protocol::HTTP2::DataFrame.new(1)
      frame.pack("x" * 16_384)
      expect { 5.times { connection.consume_local_window(frame) } }.not_to raise_error
      expect(connection.local_window.available).to be_negative
    end

    it "protocol-http2 send_window_update writes WINDOW_UPDATE before expanding the window " \
       "(delete the expand-first Stream#request_window_update)" do
      written = nil
      connection = nil
      connection = Protocol::HTTP2::Client.new(framer.new { |_frame| written = connection.local_window.available })
      before = connection.local_window.available
      connection.send_window_update(100)
      expect(written).to eq(before)
      expect(connection.local_window.available).to eq(before + 100)
    end

    it "protocol-http2 update_local_settings retargets connection replenishment to the stream window " \
       "(delete Connection#update_local_settings)" do
      connection = Protocol::HTTP2::Client.new(framer.new)
      connection.local_window.desired = 16_777_216
      connection.send_settings([[Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE, 65_536]])
      connection.update_local_settings(connection.local_settings.acknowledge)
      expect(connection.local_window.desired).to eq(65_536)
    end

    it "async-http read_in_background runs its first pass, including close on EOF, before any caller can " \
       "reach its task (delete Connection#read_in_background)" do
      reactor do |task|
        left, right = Socket.pair(:UNIX, :STREAM, 0)
        connection = Async::HTTP::Protocol::HTTP2::Client.new(IO::Stream(left))
        right.close
        returned = nil
        observed = []
        connection.define_singleton_method(:close) do |error = nil|
          observed << [returned, respond_to?(:reader_task)]
          super(error)
        end
        returned = connection.read_in_background(parent: task)
        expect(observed).to eq([[nil, false]])
        expect(returned).to be_finished
      ensure
        AsyncHelpers::INTERNAL.raw_close(left)
      end
    end

    it "async-http Output#start runs the first body read before it stores its task (delete Output#start)" do
      reactor do |task|
        stream = Object.new
        stream.define_singleton_method(:finish_output) { |_error = nil| nil }
        observed = []
        output = nil
        body = Protocol::HTTP::Body::Readable.new
        body.define_singleton_method(:read) do
          observed << [output.instance_variable_get(:@task), output.respond_to?(:task)]
          nil
        end
        output = Async::HTTP::Protocol::HTTP2::Output.new(stream, body)
        output.start(parent: task)
        expect(observed).to eq([[nil, false]])
      end
    end

    it "protocol-http1 write_body(nil) after a request head emits content-length: 0 " \
       "(delete the raw CRLF GET head in HTTP1Transport#dispatch)" do
      wire = StringIO.new
      connection = Protocol::HTTP1::Connection.new(wire)
      connection.write_request("localhost", "GET", "/", "HTTP/1.1", {})
      connection.write_body("HTTP/1.1", nil)
      expect(wire.string).to eq("GET / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n")
    end
  end
end
