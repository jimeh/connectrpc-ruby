# frozen_string_literal: true

module RetirementHelpers
  class CleanupGate
    attr_reader :entered, :interruptions

    def initialize
      @entered = Async::Queue.new
      @release = Async::Queue.new
      @interruptions = []
    end

    def hold(original = nil)
      @interruptions << original if original
      @entered.enqueue(Async::Task.current)
      loop do
        @release.dequeue
        break
      rescue Async::Cancel => e
        @interruptions << e
      end
      raise @interruptions.first unless @interruptions.empty?
    end

    def release
      @release.enqueue(true)
    end
  end

  def retirement_host(core, **)
    host = ConnectRPC::Async::Host.new(server: core, **).start
    [host, "http://127.0.0.1:#{host.port}"]
  end

  def with_retirement_connection(task, mode: :h2c, protocol: :h2, **)
    with_ca do |ca_file|
      entered = Async::Queue.new
      commands = Async::Queue.new
      gate = CleanupGate.new
      state = { gate: gate, commands: commands }
      core = retirement_server(state, entered)
      tls, settings = retirement_tls(mode, ca_file)
      host, origin = retirement_host(core, tls: tls, idle_timeout: nil, **)
      origin = origin.sub("http:", "https:") if tls
      session = host.instance_variable_get(:@session)
      state.merge!(host: host, session: session, origin: origin)
      accepted = nil
      trace = TracePoint.new(:return) do |event|
        accepted = event.return_value if event.self.equal?(session) && event.method_id == :accept
      end
      transport_type = protocol == :h1 ? ConnectRPC::Async::HTTP1Transport : ConnectRPC::Async::Transport
      transport = transport_type.new(origin: origin, **settings)
      trace.enable
      call = task.async(finished: false) do
        client(transport, origin).echo(AsyncHelpers::MESSAGE.new(name: "held"), timeout: nil)
      rescue ConnectRPC::Error => e
        e
      end
      handler = entered.dequeue
      trace.disable
      resource = session.instance_variable_get(:@resources).first
      state.merge!(resource: resource, handler: handler, accepted: accepted, transport: transport, call: call,
                   client_options: settings)
      yield state
    ensure
      trace&.disable
      close_retirement_fixture(state, transport, call)
    end
  end

  def retirement_server(state, entered)
    server(echo: lambda { |message, _context|
      next message unless message.name == "held"

      begin
        entered.enqueue(Async::Task.current)
        state.fetch(:commands).dequeue.call(state)
        message
      ensure
        state[:on_cleanup]&.call
        state.fetch(:gate).hold($ERROR_INFO)
      end
    })
  end

  def retirement_tls(mode, ca_file)
    return [nil, {}] if mode == :h2c

    anchors = [tls_material.fetch(:client_ca)] if mode == :mtls
    settings = { ca_file: ca_file }
    settings.merge!(host_tls(tls_material.fetch(:identity)).except(:client_ca)) if mode == :mtls
    [host_tls(anchors: anchors), settings]
  end

  def close_retirement_fixture(state, transport, call)
    state.fetch(:gate).release
    transport&.close(grace: 0)
    host = state[:host]
    host&.close(grace: 0)
    call&.wait
    host&.instance_variable_get(:@owned_bound)&.close
  end

  def retirement_complete?(resource) = resource.send(:retirement_completed?)

  def join_retirement_tasks(task, *tasks)
    task.with_timeout(2) { tasks.compact.each(&:wait) }
    expect(tasks.compact).to all(be_finished)
  end
end
