# frozen_string_literal: true

RSpec.describe "Falcon HTTP/1 idle expiry" do
  it "aborts an idle HTTP/1 connection without HTTP/2 GOAWAY upkeep" do
    reactor do
      rack = ->(_env) { [200, { "content-type" => "text/plain" }, ["ok"]] }
      falcon(rack_app: rack, idle_timeout: 0.05) do |host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        response = connection.call(Protocol::HTTP::Request["GET", "/idle", {}, nil, authority: "localhost"])
        expect(response.read).to eq("ok")
        resource = host.instance_variable_get(:@session).instance_variable_get(:@resources).first
        expect(resource).to be_a(ConnectRPC::Falcon.const_get(:HTTP1Resource))
        # Elapsed time is the behavior here: the real idle timer closes the
        # kept-alive connection while the client sends nothing.
        expect(raw.read).to eq("")
        expect(resource.raw).to be_closed
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        connection&.close
      end
    end
  end
end
