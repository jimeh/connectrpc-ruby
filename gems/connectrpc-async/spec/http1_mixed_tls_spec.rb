# frozen_string_literal: true

require_relative "support/http1_client_helpers"

RSpec.describe "HTTP/1 canceled TLS setup beside active HTTP/2 TLS" do
  include HTTP1ClientHelpers

  it "completes active and subsequent H2 calls on the same reactor after raw-first H1 setup cancellation" do
    stalled = Async::Promise.new
    handshake_release = Async::Promise.new
    hook = lambda do |*_|
      stalled.resolve(nil)
      handshake_release.wait
    end
    with_raw_http1(peer: ->(*) { raise "Canceled H1 setup dispatched" }, tls: true,
                   before_handshake: hook) do |h1, h1_origin, _accepted, task|
      h2_entered = Async::Queue.new
      h2_release = Async::Promise.new
      reactor_thread = Thread.current
      scheduler = Fiber.scheduler
      seen = []
      implementation = server(echo: lambda { |message, _|
        expect(Thread.current).to equal(reactor_thread)
        expect(Fiber.scheduler).to equal(scheduler)
        seen << message.name
        h2_entered.enqueue(message.name)
        h2_release.wait if message.name == "active"
        message
      })
      host = ConnectRPC::Async::Host.new(server: implementation, tls: host_tls).start
      h2_origin = "https://localhost:#{host.port}"
      ssl_errors = []
      # Observe original SSL exceptions without reading/clearing OpenSSL.errors.
      trace = TracePoint.new(:raise) do |event|
        error = event.raised_exception
        ssl_errors << [error.class.name, error.message, event.path, event.lineno] if error.is_a?(OpenSSL::SSL::SSLError)
      end
      with_ca do |ca_file|
        h2 = ConnectRPC::Async::Transport.new(origin: h2_origin, ca_file: ca_file)
        trace.enable do
          active = task.async { client(h2, h2_origin).echo(AsyncHelpers::MESSAGE.new(name: "active")) }
          # Handler invocation proves actual H2 HEADERS/DATA and decoding over TLS.
          expect(h2_entered.dequeue).to eq("active")
          h2_resource = h2.instance_variable_get(:@resources).first
          expect(h2_resource.socket.alpn_protocol).to eq("h2")
          source = ConnectRPC::CancellationSource.new
          setup = task.async do
            expect do
              h1.open_stream(stream_request(h1_origin, cancellation: source.token)) { raise "H1 setup escaped" }
            end.to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("canceled") }
          end
          stalled.wait
          h1_resource = h1.instance_variable_get(:@resources).first
          expect(h1_resource.socket).to be_a(OpenSSL::SSL::SSLSocket)
          expect(active).not_to be_finished
          source.cancel
          setup.wait
          expect(h1_resource.raw).to be_closed
          expect(h1_resource.setup).to be_finished
          expect(h1.instance_variable_get(:@calls)).to be_empty
          expect(h1.instance_variable_get(:@leases)).to be_empty
          expect(h1.instance_variable_get(:@lifetime).children.to_a).to all(be_finished)
          expect(h1.close).to be(true)
          expect(h1.instance_variable_get(:@lifetime)).to be_finished
          h2_release.resolve(nil)
          expect(active.wait.message.name).to eq("active")
          subsequent = client(h2, h2_origin).echo(AsyncHelpers::MESSAGE.new(name: "subsequent"))
          expect(subsequent.message.name).to eq("subsequent")
          expect(h2_entered.dequeue).to eq("subsequent")
          expect(seen).to eq(%w[active subsequent])
          expect(h2.instance_variable_get(:@resources)).to eq([h2_resource])
        end
        warn "Mixed H1/H2 TLS Ruby #{RUBY_VERSION}: H2 active/subsequent on one h2 lease; " \
             "H1 closed/joined; original SSL exceptions=#{ssl_errors.inspect}"
        expect(ssl_errors).to be_empty
      ensure
        trace&.disable
        h2_release.resolve(nil)
        h2&.close(grace: 0)
      end
    ensure
      handshake_release.resolve(nil)
      host&.close(grace: 0)
    end
  end
end
