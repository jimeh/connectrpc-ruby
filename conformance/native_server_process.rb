# frozen_string_literal: true

module Conformance
  class NativeServerAdapter
    def self.run(input = $stdin, output = $stdout)
      reader, writer = IO.pipe
      previous = Signal.trap("TERM") { writer.write_nonblock(".", exception: false) }
      config = Framing.read(input, ServerCompatRequest)
      raise "Missing native server configuration" unless config

      Async(finished: false) do
        host, port, bound = start_native_host(new(config), config)
        Framing.write(output, ServerCompatResponse.new(host: "127.0.0.1", port: port,
                                                       pem_cert: config.use_tls ? config.server_creds.cert : ""))
        Telemetry.record("server_ready", port: port)
        reader.read(1)
      ensure
        close_native_host(host, bound)
        Telemetry.record("server_stopped")
      end.wait
    ensure
      Signal.trap("TERM", previous) if previous
      close_signal(reader, writer)
    end

    def self.close_native_host(host, bound)
      if ENV["CONNECTRPC_NATIVE_HOST"] == "rack"
        host&.close
        return
      end

      host&.close(grace: 0)
      observe_closed_host(host) if host
      bound&.close
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

    def self.start_native_host(adapter, config)
      if ENV["CONNECTRPC_NATIVE_HOST"] == "rack"
        require_relative "../examples/localhost"
        rack = ConnectRPC::Rack.new(server: adapter.server)
        application = lambda do |env|
          Telemetry.record("native_server_wire_headers", name: env["HTTP_X_TEST_CASE_NAME"],
                                                         path: env["PATH_INFO"], stream: nil)
          rack.call(env)
        end
        host = EchoExample::Localhost.new(application)
        return [host, URI(host.url).port, nil]
      end

      if ENV.fetch("CONNECTRPC_NATIVE_HOST", "async") == "falcon"
        require "connectrpc/falcon"
        if ENV["CONNECTRPC_CONFORMANCE_WEB"] == "1"
          ConnectRPC::Falcon.const_get(:HTTP1Connection).prepend(WebHTTP1Arrival)
        end
        bound = ConnectRPC::Falcon::Endpoint.parse("#{config.use_tls ? 'https' : 'http'}://127.0.0.1:0").bound
        host = ConnectRPC::Falcon::Server.new(server: adapter.server, endpoint: bound, tls: native_tls(config))
        host.run
        [host, bound.sockets.first.local_address.ip_port, bound]
      else
        host = ConnectRPC::Async::Host.new(server: adapter.server, tls: native_tls(config)).start
        [host, host.port, nil]
      end
    end
  end
end
