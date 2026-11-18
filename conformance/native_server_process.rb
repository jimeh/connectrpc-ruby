# frozen_string_literal: true

require_relative "native_stall_report"

module Conformance
  class NativeServerAdapter
    def self.run(input = $stdin, output = $stdout)
      reader, writer = IO.pipe
      stall = NativeStallReport.new
      main = Thread.current
      previous = Signal.trap("TERM") do
        writer.write_nonblock(".", exception: false)
        stall.arm(main)
      end
      config = Framing.read(input, ServerCompatRequest)
      raise "Missing native server configuration" unless config

      owned = {}
      Async(finished: false) do
        stall.scheduler = Fiber.scheduler
        port = start_native_host(new(config), config, owned)
        Framing.write(output, ServerCompatResponse.new(host: "127.0.0.1", port: port,
                                                       pem_cert: config.use_tls ? config.server_creds.cert : ""))
        Telemetry.record("server_ready", port: port)
        owned[:ready] = true
        reader.read(1)
      ensure
        stop_native_host(owned, stall)
      end.wait
    ensure
      Signal.trap("TERM", previous) if previous
      close_signal(reader, writer)
    end

    # Records server_stopped whatever close raises, and what it raised: an
    # Async::Cancel escaping this task would otherwise end it silently. The
    # full message, with any startup failure as its cause, goes to stderr.
    def self.stop_native_host(owned, stall)
      stall.phase = :closing
      close_native_host(owned)
    rescue Exception => e # rubocop:disable Lint/RescueException -- report every close escape, then re-raise it
      Telemetry.record("server_close_failed", error_class: e.class.name, message: e.message)
      $stderr.write("Native server close failed: #{e.full_message(highlight: false)}")
      raise
    ensure
      Telemetry.record("server_stopped")
      stall.phase = :stopped
    end

    # Closes whatever startup acquired, the listener even when closing the
    # host raises. Only a host that became ready reports its closed lifetime.
    def self.close_native_host(owned)
      host = owned[:host]
      if ENV["CONNECTRPC_NATIVE_HOST"] == "rack"
        host&.close
        return
      end

      begin
        host&.close(grace: 0)
        observe_closed_host(host) if host && owned[:ready]
      ensure
        owned[:bound]&.close
      end
    end

    def self.observe_closed_host(host)
      session = host.instance_variable_get(:@session)
      resources = session.instance_variable_get(:@resources)
      raw_closed = resources.empty? ? nil : resources.all? { |resource| resource.raw.closed? }
      Telemetry.record("native_server_closed_lifetime", state: session.instance_variable_get(:@state),
                                                        admitted: session.instance_variable_get(:@admitted).size,
                                                        identities: session.instance_variable_get(:@identities).size,
                                                        resources_finished: resources.all?(&:finished?),
                                                        raw_closed: raw_closed,
                                                        tasks_finished: session.instance_variable_get(:@lifetime)
                                                                               .children.to_a.all?(&:finished?),
                                                        socket_interruption: nil)
    end

    def self.close_signal(reader, writer)
      reader&.close
      writer&.close
    end

    def self.native_tls(config)
      return unless config.use_tls

      { certificate_chain: [config.server_creds.cert], private_key: config.server_creds.key,
        client_ca: config.client_tls_cert.empty? ? nil : [config.client_tls_cert] }
    end

    # Fills `owned` as each resource is acquired, so cleanup reaches the
    # listener and host even when a later startup step raises.
    def self.start_native_host(adapter, config, owned)
      if ENV["CONNECTRPC_NATIVE_HOST"] == "rack"
        require_relative "../examples/localhost"
        rack = ConnectRPC::Rack.new(server: adapter.server)
        application = lambda do |env|
          Telemetry.record("native_server_wire_headers", name: env["HTTP_X_TEST_CASE_NAME"],
                                                         path: env["PATH_INFO"], stream: nil)
          rack.call(env)
        end
        owned[:host] = EchoExample::Localhost.new(application)
        return URI(owned[:host].url).port
      end

      if ENV.fetch("CONNECTRPC_NATIVE_HOST", "async") == "falcon"
        require "connectrpc/falcon"
        if ENV["CONNECTRPC_CONFORMANCE_WEB"] == "1" || ENV["CONNECTRPC_CONFORMANCE_HTTP1"] == "1"
          ConnectRPC::Falcon.const_get(:HTTP1Connection).prepend(WebHTTP1Arrival)
        end
        bound = owned[:bound] =
          ConnectRPC::Falcon::Endpoint.parse("#{config.use_tls ? 'https' : 'http'}://127.0.0.1:0").bound
        owned[:host] = ConnectRPC::Falcon::Server.new(server: adapter.server, endpoint: bound, tls: native_tls(config))
        owned[:host].run
        bound.sockets.first.local_address.ip_port
      else
        owned[:host] = ConnectRPC::Async::Host.new(server: adapter.server, tls: native_tls(config)).start
        owned[:host].port
      end
    end
  end
end
