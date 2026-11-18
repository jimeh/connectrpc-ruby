# frozen_string_literal: true

require_relative "../../connectrpc-async/spec/support/ingress_host_examples"
require_relative "../../connectrpc-async/spec/support/response_credit_examples"

RSpec.describe "Falcon native HTTP/2 ordinary Rack request ingress" do
  def with_ingress_host(app, tls: nil, **, &)
    rack = lambda do |env|
      body = env.fetch("protocol.http.request").body
      app.call(body, -> { env.fetch("rack.input").read(16_384) }, env.fetch("PATH_INFO"))
    end
    # Barrier PINGs outpace grpc-go's enforcement without server writes between them.
    falcon(rack_app: rack, tls: tls, scheme: tls ? "https" : "http",
           keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: nil), **, &)
  end

  include_examples "hosted HTTP/2 ingress backpressure"
  include_examples "hosted HTTP/2 shared response credit"

  it "advertises the configured receive windows on native HTTP/2 connections" do
    reactor do |task|
      falcon(rack_app: ->(_env) { [200, {}, ["ok"]] }, stream_receive_window: 131_072,
             connection_receive_window: 262_144) do |_host, origin|
        with_ingress_client(task, origin, :h2c) do |connection, wire|
          expect(connection.remote_settings.initial_window_size).to eq(131_072)
          expect(wire[:credit][0]).to eq(262_144 - 65_535)
        end
      end
    end
  end

  # End-to-end coverage of the smallest windows the options accept through a
  # real Transport: concurrent bodies in both directions wait on connection
  # credit rather than stream credit. The raw-peer example in
  # connectrpc-async's support/response_credit_examples.rb proves the
  # shared-credit race.
  [false, true].each do |dynamic|
    growth = dynamic ? "dynamic" : "static"
    it "serves concurrent native bodies at 65535-byte stream and connection windows, #{growth}" do
      reactor do |task|
        size = 262_144
        rack = lambda do |env|
          [200, {}, [env.fetch("PATH_INFO") == "/download" ? "z" * size : env.fetch("rack.input").read.bytesize.to_s]]
        end
        windows = { stream_receive_window: 65_535, connection_receive_window: 65_535,
                    dynamic_receive_windows: dynamic }
        falcon(rack_app: rack, **windows) do |_host, origin|
          transport = ConnectRPC::Async::Transport.new(origin: origin, **windows)
          calls = Array.new(4) { task.async { transport.call(request("#{origin}/download")).body.bytesize } } +
                  Array.new(4) do
                    task.async { transport.call(request("#{origin}/upload", body: "u" * size)).body }
                  end
          expect(calls.map(&:wait)).to eq(([size] * 4) + ([size.to_s] * 4))
        ensure
          calls&.each(&:stop)
          transport&.close(grace: 0)
        end
      end
    end
  end

  it "passes the dynamic receive window options to native HTTP/2 connections" do
    reactor do |task|
      rack = ->(env) { [200, {}, [env.fetch("rack.input").read.bytesize.to_s]] }
      [[{ max_receive_window: 98_304 }, [98_304]], [{ dynamic_receive_windows: false }, []]].each do |options, growth|
        falcon(rack_app: rack, keepalive: ConnectRPC::Async::ServerKeepalive.new(min_time: nil),
               **options) do |_host, origin|
          with_ingress_client(task, origin, :h2c) do |connection, wire|
            response = connection.create_response
            response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", "/upload", {}, nil])
            response.stream.send_headers([[":method", "POST"], [":scheme", "http"],
                                          [":authority", URI(origin).authority], [":path", "/upload"]])
            # One 64 KiB sample: twice it exceeds the 98,304-byte ceiling.
            ingress_burst(connection, [response.stream.id, 65_536])
            ingress_data(connection, response.stream.id, "", finish: true)
            connection.read_response(response)
            expect(response.read).to eq("65536")
            ingress_wait(task, "growth #{growth}", state: -> { wire }) { ingress_growth(wire) == growth }
            ingress_barrier(task, connection, wire)
            expect([ingress_growth(wire), wire[:probes].empty?]).to eq([growth, growth.empty?]), options.inspect
          end
        end
      end
    end
  end
end
