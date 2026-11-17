# frozen_string_literal: true

require "async/service/configuration"
require "falcon/environment/rack"

RSpec.describe "Falcon service environment" do
  def rack_configuration(root, core, connectrpc_last: true, &extra)
    Async::Service::Configuration.build do
      service "rack-facet" do
        if connectrpc_last
          include Falcon::Environment::Rack
          include ConnectRPC::Falcon::Environment
        else
          include ConnectRPC::Falcon::Environment
          include Falcon::Environment::Rack
        end

        root root
        url "http://127.0.0.1:0"
        connectrpc_server { core }
        instance_exec(&extra) if extra
      end
    end
  end

  def serve(configuration)
    bound = ConnectRPC::Falcon::Endpoint.parse("http://127.0.0.1:0").bound
    host = configuration.environments.fetch(0).evaluator.make_server(bound)
    host.run
    yield "http://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
  ensure
    host&.close(grace: 0)
    bound&.close
  end

  def with_rackup
    Dir.mktmpdir("connectrpc-falcon-rackup-") do |root|
      File.write(File.join(root, "config.ru"),
                 %(run ->(_env) { [200, { "content-type" => "text/plain" }, ["config.ru"]] }\n))
      yield root
    end
  end

  def echo(origin, name)
    bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: name))
    headers = { "content-type" => "application/proto" }
    status, _headers, body = http1(origin, AsyncHelpers::ECHO::ECHO.path, method: "POST", body: bytes, headers: headers)
    [status, AsyncHelpers::MESSAGE.decode(body).name]
  end

  it "keeps a Falcon Rack facet's config.ru as the fallback when included after it" do
    reactor do
      with_rackup do |root|
        serve(rack_configuration(root, server)) do |origin|
          headers = { "accept-encoding" => "identity" }
          expect(http1(origin, "/page", body: nil, headers: headers).values_at(0, 2)).to eq([200, "config.ru"])
          expect(echo(origin, "native")).to eq([200, "native"])
        end
      end
    end
  end

  it "prefers an explicit rack_app over the Rack facet's config.ru" do
    reactor do
      with_rackup do |root|
        explicit = ->(_env) { [200, { "content-type" => "text/plain" }, ["explicit"]] }
        configuration = rack_configuration(root, server) { rack_app { explicit } }
        serve(configuration) do |origin|
          expect(http1(origin, "/page", body: nil, headers: { "accept-encoding" => "identity" }).values_at(0, 2))
            .to eq([200, "explicit"])
        end
      end
    end
  end

  it "rejects including ConnectRPC::Falcon::Environment before Falcon's environments before binding" do
    with_rackup do |root|
      environment = rack_configuration(root, nil, connectrpc_last: false).environments.fetch(0)
      # The Rack app survives this order; Falcon's application endpoint is what breaks it.
      expect(environment.evaluator.rack_app.call({}).last).to eq(["config.ru"])
      service = ConnectRPC::Falcon::Service.new(environment)
      expect { service.start }.to raise_error(
        ArgumentError, /include ConnectRPC::Falcon::Environment after Falcon's environments/
      )
      expect(File).not_to exist(File.join(root, "application.ipc"))
    end
  end

  [-1, "5", Float::INFINITY, nil].each do |grace|
    it "fails the worker's initial load before make_server for connectrpc_close_grace #{grace.inspect}" do
      made = []
      recorder = Module.new do
        define_method(:make_server) do |bound|
          made << bound
          raise "make_server ran"
        end
      end
      configuration = Async::Service::Configuration.build do
        service "invalid-grace" do
          include ConnectRPC::Falcon::Environment
          include recorder

          connectrpc_close_grace grace
        end
      end
      environment = configuration.environments.fetch(0)
      service = ConnectRPC::Falcon::Service.new(environment)
      listener = Struct.new(:endpoint).new(:bound)
      reactor do
        expect { service.run(nil, environment.evaluator, listener) }.to raise_error(
          ArgumentError, "connectrpc_close_grace must be finite nonnegative seconds"
        )
      end
      expect(made).to be_empty
    end
  end
end
