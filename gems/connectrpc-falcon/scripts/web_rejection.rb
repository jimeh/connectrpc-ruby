# frozen_string_literal: true

module FalconWebRejection
  extend AsyncHelpers

  def self.run(binary, fixtures, identity, root)
    results = %w[http https].map do |scheme|
      reactor do |task|
        events = {}
        host = FalconInterop::FalconInteropHost.new(server: target(events),
                                                    tls: scheme == "https" ? host_tls(identity) : nil).start
        probe(task, host, "#{scheme}://localhost:#{host.port}", events: events,
                                                                binary: binary, fixtures: fixtures, root: root)
      ensure
        host&.close(grace: 0)
      end
    end
    raise "Go Web head regression failed" unless results.all?
  end

  def self.target(events)
    stream = ConnectRPC::ServerStreamMethod.new(service: "web.Rejection", name: "Watch",
                                                request_type: AsyncHelpers::MESSAGE,
                                                response_type: AsyncHelpers::MESSAGE)
    server = ConnectRPC::Server.new(
      services: [ConnectRPC::Service.new(name: stream.service, methods: [stream])],
      binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :grpc_web, request_limit: 100,
      handlers: { stream.path => ->(*) { raise "Oversized request reached handler" } }
    )
    original = server.method(:call_grpc_web)
    server.define_singleton_method(:call_grpc_web) do |**arguments|
      events[:body] = arguments[:input].instance_variable_get(:@body)
      read = events[:body].method(:read)
      events[:body].define_singleton_method(:read) do
        events[:cleanup_started] = true if events[:prepared]
        read.call
      end
      original.call(**arguments)
    ensure
      events[:prepared] = true
    end
    server
  end

  def self.release_on_wait(host, input, events)
    session = host.instance_variable_get(:@server).instance_variable_get(:@session)
    TracePoint.new(:call) do |event|
      next if events[:released] || !%i[wait write_response].include?(event.method_id)

      resource = session.instance_variable_get(:@resources).find { |owned| owned.task.equal?(Async::Task.current) }
      next unless resource

      observe_boundary(event, resource, input, events)
    end
  end

  def self.observe_boundary(event, resource, input, events)
    if event.self.equal?(resource.connection) && event.method_id == :write_response
      events[:head_written] = true
    elsif event.defined_class == AsyncHelpers::INTERNAL::Publication && event.method_id == :wait
      release_upload(input, events) unless events[:head_written]
    end
  end

  def self.release_upload(input, events)
    unless events[:prepared] && events[:cleanup_started] && !events[:body].empty?
      raise "Dispatch wait did not retain the original incomplete rejected input"
    end

    events[:released] = true
    input.puts("dispatch_wait")
  end

  def self.probe(task, host, origin, events:, binary:, fixtures:, root:)
    Open3.popen2(binary, "web-head", origin, fixtures, chdir: File.join(root, "interop")) do |input, output, process|
      input.sync = true
      trace = release_on_wait(host, input, events)
      trace.enable
      puts task.with_timeout(10) { output.read }
      process.value.success? && events[:released]
    ensure
      trace&.disable
      input.close unless input.closed?
      unless process.join(2)
        Process.kill("TERM", process.pid)
        process.join
        raise "Go Web head regression shutdown timeout"
      end
    end
  end
end
