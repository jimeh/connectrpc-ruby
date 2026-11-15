# frozen_string_literal: true

RSpec.describe "ThreadedTransport across fork" do
  before { skip "Process.fork is unavailable" unless Process.respond_to?(:fork) }

  # Runs the block in a child that exits without at_exit hooks, and returns its
  # report. The parent's wait is bounded and a hung child is killed before reading.
  def in_child(&)
    reader, writer = IO.pipe
    pid = Process.fork { report_from_child(reader, writer, &) }
    writer.close
    status = Process.detach(pid).join(20)&.value
    Process.kill("KILL", pid) unless status
    [status&.success?, reader.read]
  ensure
    reader&.close
  end

  def report_from_child(reader, writer)
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

  def child_call(typed, name)
    typed.echo(AsyncHelpers::MESSAGE.new(name: name), timeout: 10).message.name
  end

  %i[tls mtls].each do |mode|
    context "over #{mode}" do
      def setup_peer(mode, ca_file, entered, release)
        tls, scheme, options = threaded_mode(mode, ca_file)
        peer = threaded_host(tls: tls, server: server(echo: lambda { |message, _|
          entered << message.name
          release.pop if message.name == "held"
          message
        }))
        origin = "#{scheme}://127.0.0.1:#{peer.peer.port}"
        [peer, origin, threaded(origin: origin, **options)]
      end

      it "gives an idle child its own connection and keeps the parent's" do
        with_ca do |ca_file|
          entered = Thread::Queue.new
          release = Thread::Queue.new
          peer, origin, transport = setup_peer(mode, ca_file, entered, release)
          typed = client(transport, origin)
          expect(child_call(typed, "parent")).to eq("parent")
          parent_engine = engine_of(transport)
          expect(in_child { child_call(typed, "child") }).to eq([true, "child"])
          expect(peer.peer.accepts.size).to eq(2)
          expect(child_call(typed, "parent again")).to eq("parent again")
          expect([peer.peer.accepts.size, engine_of(transport)]).to eq([2, parent_engine])
        ensure
          transport&.close(grace: 0)
          peer&.stop
        end
      end

      it "forks while another thread has a call in flight" do
        with_ca do |ca_file|
          entered = Thread::Queue.new
          release = Thread::Queue.new
          peer, origin, transport = setup_peer(mode, ca_file, entered, release)
          typed = client(transport, origin)
          held = Thread.new { child_call(typed, "held") }
          expect(pop_within(entered, "the held call in the handler")).to eq("held")
          expect(in_child { child_call(typed, "child") }).to eq([true, "child"])
          release << true
          expect(value_within(held, "the held caller")).to eq("held")
          expect(child_call(typed, "parent again")).to eq("parent again")
          expect([peer.peer.accepts.size, admitted(transport)]).to eq([2, 0])
        ensure
          release&.<<(true)
          transport&.close(grace: 0)
          peer&.stop
        end
      end

      it "forks while another thread is starting the engine under the front lock" do
        with_ca do |ca_file|
          entered = Thread::Queue.new
          release = Thread::Queue.new
          peer, origin, transport = setup_peer(mode, ca_file, entered, release)
          parent = Process.pid
          starting = Thread::Queue.new
          resume = Thread::Queue.new
          transport.define_singleton_method(:build_engine) do |admission|
            if Process.pid == parent
              starting << true
              resume.pop
            end
            super(admission)
          end
          typed = client(transport, origin)
          starter = Thread.new { child_call(typed, "starter") }
          pop_within(starting, "the starter to hold the front lock")
          expect(transport.instance_variable_get(:@lock)).to be_locked
          expect(in_child { child_call(typed, "child") }).to eq([true, "child"])
          resume << true
          expect(value_within(starter, "the starter")).to eq("starter")
          expect(child_call(typed, "parent again")).to eq("parent again")
          expect(peer.peer.accepts.size).to eq(2)
        ensure
          resume&.<<(true)
          transport&.close(grace: 0)
          peer&.stop
        end
      end
    end
  end

  it "keeps a transport closed before fork closed and closes only the child's engine in a child" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    expect(child_call(typed, "parent")).to eq("parent")
    report = in_child do
      child_call(typed, "child")
      [transport.close, transport.connection_state].inspect
    end
    expect(report).to eq([true, "[true, :closed]"])
    expect([transport.connection_state, child_call(typed, "parent again"), peer.peer.accepts.size])
      .to eq([:ready, "parent again", 2])
    expect(transport.close).to be(true)
    closed = in_child do
      child_call(typed, "child")
    rescue ConnectRPC::Error => e
      [e.code, transport.close, transport.connection_state].inspect
    end
    expect(closed).to eq([true, '["unavailable", true, :closed]'])
  ensure
    transport&.close(grace: 0)
    peer&.stop
  end

  it "treats a close still pending in the parent as closed in a child" do
    peer = threaded_host(server: server)
    origin = "http://127.0.0.1:#{peer.peer.port}"
    transport = threaded(origin: origin)
    typed = client(transport, origin)
    expect(child_call(typed, "parent")).to eq("parent")
    gate = hold_owner(transport)
    closer = Thread.new { transport.close(grace: 5) }
    eventually("parent close pending") { transport.connection_state == :closing && blocked?(closer) }
    report = in_child do
      child_call(typed, "child")
    rescue ConnectRPC::Error => e
      [e.code, transport.connection_state, transport.close].inspect
    end
    expect(report).to eq([true, '["unavailable", :closed, true]'])
    expect(transport.connection_state).to eq(:closing)
    gate << true
    expect(value_within(closer, "the parent closer")).to be(true)
    expect(peer.peer.accepts.size).to eq(1)
  ensure
    gate&.<<(true)
    transport&.close(grace: 0)
    peer&.stop
  end
end
