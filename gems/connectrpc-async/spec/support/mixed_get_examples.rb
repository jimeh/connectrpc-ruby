# frozen_string_literal: true

RSpec.shared_examples "mixed GET protocol selection" do |backend, wire|
  cases = [
    [%i[connect grpc_web], "GET", true, 200, :connect],
    [%i[connect grpc_web], "GET", false, 405, :connect],
    [:grpc_web, "GET", true, 405, :grpc_web],
    [%i[connect grpc_web], "POST", true, 200, :grpc_web],
    [:connect, "GET", true, 200, :connect],
    [:auto, "GET", true, 200, :connect],
    [:connect, "POST", true, 415, :grpc_web],
    [:auto, "POST", true, 415, :grpc_web]
  ]
  cases.each do |protocol, verb, safe, status, selected|
    it "selects #{selected} for #{backend}/#{wire} #{protocol} #{safe ? 'safe' : 'unsafe'} #{verb}" do
      reactor do
        calls = []
        handled = []
        subscriber = Object.new
        subscriber.define_singleton_method(:start) do |call|
          calls << call
          nil
        end
        instrumentation = ConnectRPC::Instrumentation.new(subscribers: [subscriber])
        declaration = safe ? AsyncHelpers::ECHO::ECHO : AsyncHelpers::ECHO::FAIL
        handler = lambda do |message, _|
          handled << message
          message
        end
        options = if Array(protocol).include?(:grpc_web)
                    { cors: ConnectRPC::GRPCWebCORS.new(origins: ["https://allowed.example"]) }
                  else
                    {}
                  end
        target = ConnectRPC::Server.new(services: [AsyncHelpers::ECHO::SERVICE],
                                        binding: ConnectRPC::Bindings::GoogleProtobuf.new,
                                        handlers: { declaration.path => handler }, protocol: protocol,
                                        instrumentation: instrumentation, **options)
        bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "query"))
        query = "encoding=proto&base64=1&message=#{[bytes].pack('m0').tr('+/', '-_').delete('=')}"
        route = declaration.path + (verb == "GET" ? "?#{query}" : "")
        fields = { "content-type" => "application/grpc-web",
                   "origin" => selected == :connect ? "https://denied.example" : "https://allowed.example" }
        if verb == "GET"
          fields["content-encoding"] = "unsupported"
          fields["connect-protocol-version"] = "ignored"
        end
        body = verb == "GET" ? nil : [0, bytes.bytesize].pack("CN") + bytes
        exercise = lambda do |origin|
          if wire == :h2
            transport = ConnectRPC::Async::Transport.new(origin: origin)
            headers = fields.transform_values { |value| [value] }
            response = transport.call(request("#{origin}#{route}", method: verb, body: body || "", headers: headers))
            actual = [response.status, response.headers, response.body]
          else
            actual = http1(origin, route, method: verb, headers: fields, body: body)
          end
          expect(actual.first).to eq(status)
          expect(calls.map(&:protocol)).to eq([selected])
          expect(calls.first.carrier["content-type"]).to eq(["application/grpc-web"])
          expect(calls.first.carrier).to be_frozen
          expect(handled.map(&:name)).to eq(status == 200 ? ["query"] : [])
          if status == 200
            expected = if selected == :connect
                         bytes
                       else
                         "#{[0, bytes.bytesize].pack('CN')}#{bytes}#{[128, 16].pack('CN')}grpc-status: 0\r\n"
                       end
            expect(actual.last).to eq(expected)
          end
          expect(actual[1]).not_to have_key("access-control-allow-origin") if selected == :connect
        ensure
          transport&.close(grace: 0)
        end
        if backend == :async
          host = ConnectRPC::Async::Host.new(server: target).start
          exercise.call("http://127.0.0.1:#{host.port}")
          expect(host.close(grace: 1)).to be(true)
        else
          falcon(server: target) { |_host, origin| exercise.call(origin) }
        end
      ensure
        host&.close(grace: 0)
      end
    end
  end
end
