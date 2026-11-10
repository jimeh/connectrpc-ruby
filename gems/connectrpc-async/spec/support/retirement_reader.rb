# frozen_string_literal: true

require_relative "retirement_helpers"

module RetirementReader
  # Trace real lifecycle boundaries without replacing reader, abort or join.
  class Observation
    attr_reader :resource, :reader, :fiber, :boundary, :entered, :entry_gate, :return_gate,
                :disposals, :returned, :first_error

    def initialize(gated:, ready: nil)
      @gated = gated
      @ready = ready
      @entered = Async::Queue.new
      @entry_gate = RetirementHelpers::CleanupGate.new
      @return_gate = RetirementHelpers::CleanupGate.new
      @disposals = []
      @trace = TracePoint.new(:call, :return) { |event| observe(event) }
    end

    def enable = @trace.enable
    def disable = @trace.disable

    def release
      @entry_gate.release
      @return_gate.release
    end

    def report
      tasks = [@reader, @resource.task, @resource.instance_variable_get(:@cleanup)].compact.uniq
      puts "reader-boundary #{@boundary.inspect}"
      tasks.each do |task|
        puts "reader-state task=#{task.object_id} parent=#{task.parent&.object_id} " \
             "transient=#{task.transient?} status=#{task.status} finished=#{task.finished?} " \
             "fiber=#{task.fiber&.object_id}\n#{task.fiber&.backtrace&.join("\n")}"
      end
      $stdout.flush
    end

    private

    def observe(event)
      if event.method_id == :read_in_background
        observe_startup(event)
      elsif event.self.equal?(@resource)
        observe_resource(event)
      elsif event.self.equal?(@resource&.connection)
        if event.event == :call && event.method_id == :finish_close
          @disposals << event.binding.local_variable_get(:error)
        end
      end
    end

    def observe_startup(event)
      return unless event.defined_class == AsyncHelpers::INTERNAL::Connection

      if event.event == :call
        @resource ||= event.self.resource
        @expected_parent = if event.self.is_a?(AsyncHelpers::INTERNAL::ClientConnection)
                             @resource.instance_variable_get(:@lifetime)
                           else
                             @resource.task
                           end
        @ready&.wait
      else
        @returned = event.return_value
      end
    end

    def observe_resource(event)
      if event.method_id == :abort
        observe_abort(event)
      elsif event.method_id == :retire && event.event == :call
        @first_error = event.binding.local_variable_get(:error)
      end
    end

    def observe_abort(event)
      if event.event == :call && !@reader
        capture_reader(event)
        @entered.enqueue(true)
        @entry_gate.hold if @gated
      elsif event.event == :return && Async::Task.current.equal?(@reader) && !@held_return
        @held_return = true
        @return_gate.hold
      end
    end

    def capture_reader(event)
      @reader = Async::Task.current
      @fiber = @reader.fiber
      @boundary = { reader: @reader, published: @resource.connection.reader_task,
                    parent: @reader.parent, expected_parent: @expected_parent,
                    transient: @reader.transient?, fiber: @fiber,
                    stack: @fiber.backtrace, error: event.binding.local_variable_get(:error),
                    startup_returned: !!@returned }
    end
  end

  def reader_wire(failure, server:)
    settings = [0, 0, 0, 4, 0, 0].pack("C5N")
    invalid_ping = [0, 0, 1, 6, 0, 0, 120].pack("C5NC")
    prefix = server ? Protocol::HTTP2::CONNECTION_PREFACE : ""
    prefix + settings + (failure == :frame ? invalid_ping : "")
  end

  def with_reader_fixture(task, client:, failure:, gated:)
    ready = Async::Promise.new
    observer = Observation.new(gated: gated, ready: ready)
    release_peer = Async::Queue.new
    if client
      bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      listener = bound.sockets.first
      sender = task.async(finished: false) do
        peer, = listener.accept
        peer.write(reader_wire(failure, server: false))
        peer.shutdown(Socket::SHUT_WR)
        ready.resolve(nil)
        release_peer.dequeue
      ensure
        peer&.close
      end
      origin = "http://127.0.0.1:#{listener.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      observer.enable
      caller = task.async(finished: false) do
        transport.call(request("#{origin}/eager-reader", method: "GET"))
      rescue ConnectRPC::Error => e
        e
      end
    else
      host, bound, session = reader_listener
      listener = bound.sockets.first
      peer = IO::Endpoint.tcp("127.0.0.1", listener.local_address.ip_port).connect
      peer.write(reader_wire(failure, server: true))
      peer.shutdown(Socket::SHUT_WR)
      ready.resolve(nil)
      observer.enable
      caller = start_reader_listener(host, listener, session)
    end
    task.with_timeout(2) { observer.entered.dequeue }
    yield observer
  ensure
    close_reader_fixture(task, observer, tasks: [caller, sender], release_peer: release_peer,
                                         transport: transport, host: host, session: session, peer: peer, bound: bound)
  end

  def close_reader_fixture(task, observer, tasks:, release_peer:, transport:, host:, session:, peer:, bound:)
    release_reader_resource(task, observer, release_peer)
    transport&.close(grace: 0)
    host ? host.close(grace: 0) : session&.abort
    # Falcon's run task remains live until host close stops its accept loop.
    task.with_timeout(2) { tasks.compact.each(&:wait) }
    peer&.close
    bound&.close
    observer.disable
  end

  def release_reader_resource(task, observer, release_peer)
    observer.release
    # A failed publication assertion must still release both real-reader gates.
    task.with_timeout(2) { observer.resource.abort } if observer.resource
    release_peer.enqueue(true)
  end

  def assert_reader_retirement(task, observer, failure:, gated:)
    resource = observer.resource
    boundary = observer.boundary
    observer.report
    message = "actual transient reader must be published before eager retirement reentry"
    expect(boundary.fetch(:published)).to equal(observer.reader), message
    expect(boundary.fetch(:startup_returned)).to be(false)
    expect(boundary.fetch(:transient)).to be(true)
    expect(boundary.fetch(:error)).to be_a(failure == :eof ? EOFError : Protocol::HTTP2::ProtocolError)
    observer.entry_gate.release
    task.with_timeout(2) { observer.return_gate.entered.dequeue }
    expect(observer.returned).to equal(observer.reader)
    # Upstream can reparent the transient reader when its startup task finishes.
    expect(boundary.fetch(:parent)).to equal(boundary.fetch(:expected_parent))
    expect(resource.raw).to be_closed
    competitor = task.async(finished: false) { resource.abort(IOError.new("later reader competitor")) }
    expect(competitor).not_to be_finished
    expect(resource.send(:retirement_completed?)).to be(false)
    expect(observer.disposals).to be_empty
    expect(observer.fiber).to be_alive
    observer.return_gate.release
    task.with_timeout(2) { AsyncHelpers::INTERNAL.join_complete(competitor) }
    expect(resource.send(:retirement_completed?)).to be(true)
    expect(observer.reader).to be_finished
    expect(observer.fiber).not_to be_alive
    expect(observer.disposals.size).to eq(1)
    expect(observer.disposals.first).to equal(observer.first_error)
    # Entry observation suspends before the abort body, permitting serve's
    # ensure to request retirement first. The ungated reader is the initiator.
    expect(observer.first_error).to equal(boundary.fetch(:error)) unless gated
    expect(competitor.result).to be_nil
  end
end

RSpec.shared_examples "eager reader retirement" do
  include RetirementReader

  [false, true].product(%i[eof frame]).each do |client, failure|
    it "publishes #{client ? 'H2-client' : 'server H2'} #{failure} reader at eager retirement publication" do
      reactor do |task|
        with_reader_fixture(task, client: client, failure: failure, gated: true) do |observer|
          assert_reader_retirement(task, observer, failure: failure, gated: true)
        end
      end
    end

    it "retires #{client ? 'H2-client' : 'server H2'} #{failure} reader without entry suspension" do
      reactor do |task|
        with_reader_fixture(task, client: client, failure: failure, gated: false) do |observer|
          assert_reader_retirement(task, observer, failure: failure, gated: false)
        end
      end
    end
  end
end
