# frozen_string_literal: true

RSpec.describe "Managed TLS timing and host lifecycle" do
  it "keeps trust-anchor ordering and equivalent PEM representations unchanged, while validating unchanged returns" do
    reactor do
      anchors = [tls_material[:client_ca], credentials(3, "second client root", authority: true)]
      source = host_tls(anchors: anchors).merge(version: "one")
      manager = ConnectRPC::Async::TLSConfiguration.new(tls_provider: -> { source }, on_error: ->(_) {}).start
      original = manager.instance_variable_get(:@snapshot)
      source = source.merge(client_ca: source[:client_ca].reverse.map { |pem| "\n#{pem}\n" })
      expect(manager.refresh).to eq(1)
      expect(manager.instance_variable_get(:@snapshot)).to equal(original)
      source = source.merge(certificate_chain: ["malformed"])
      expect { manager.refresh }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error)
      expect(manager.generation).to eq(1)
    ensure
      manager&.close
    end
  end

  it "checks the earliest identity chain expiry at raw capture and again on the same pending capture" do
    reactor do
      intermediate = credentials(10, "short intermediate", issuer: tls_material[:root], authority: true)
      intermediate.first.not_after = Time.now + 120
      intermediate.first.sign(tls_material[:root].last, "SHA256")
      leaf = credentials(11, "localhost", issuer: intermediate, usage: "serverAuth")
      manager = ConnectRPC::Async::TLSConfiguration.new(tls: host_tls(leaf, chain: [intermediate])).start
      raw = Object.new
      captured = manager.send(:capture, raw)
      expiry = intermediate.first.not_after
      allow(Time).to receive(:now).and_return(expiry)
      expect { manager.send(:capture, raw) }.to raise_error(ArgumentError, "Expired TLS snapshot")
      expect { captured.handshake }.to raise_error(ArgumentError, "Expired TLS snapshot")
      expect(manager.generation).to eq(1)
    ensure
      manager&.close
    end
  end

  it "schedules completion-relative refreshes with bounded jitter and stops its timer/provider on close" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      checks = []
      manager = ConnectRPC::Async::TLSConfiguration.new(tls_provider: lambda {
        checks << AsyncHelpers::INTERNAL.now
        if checks.size == 2
          entered.enqueue(true)
          release.dequeue
        end
        host_tls.merge(version: "same")
      }, refresh_interval: 0.03, refresh_jitter: 0.1).start
      entered.dequeue
      explicit = task.async { manager.refresh }
      release.enqueue(true)
      explicit.wait
      completed = AsyncHelpers::INTERNAL.now
      wait_until(task, "third scheduled source check") { checks.size >= 3 }
      expect(checks[2] - completed).to be >= 0.025
      expect(checks[2] - completed).to be < 0.3
      manager.close
      expect(manager.instance_variable_get(:@timer)).to be_finished
      expect(manager.instance_variable_get(:@callback)).to be_nil
      expect(manager.instance_variable_get(:@attempt)).to be_nil
    ensure
      manager&.close
    end
  end

  it "loads before binding the strict Host and forwards managed refresh/status without allowing manual reload" do
    reactor do
      source = nil
      host = ConnectRPC::Async::Host.new(server: server, tls_provider: -> { source }, on_tls_error: ->(_) {})
      expect { host.start }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error)
      expect(host.port).to be_nil
      expect(host.tls_generation).to be_nil
      expect { host.reload_tls(**host_tls) }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
      source = host_tls.merge(version: "one")
      fresh = ConnectRPC::Async::Host.new(server: server, tls_provider: -> { source }).start
      expect(fresh.tls_status[:generation]).to eq(1)
      expect(fresh.refresh_tls).to eq(1)
      expect { fresh.reload_tls(**host_tls) }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR, /Provider owns/)
      plain = ConnectRPC::Async::Host.new(server: server).start
      expect { plain.refresh_tls }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    ensure
      host&.close(grace: 0)
      fresh&.close(grace: 0)
      plain&.close(grace: 0)
    end
  end
  it "rejects host close invoked by its own provider before stopping the listener" do
    reactor do
      mode = :initial
      host = nil
      provider = lambda do
        host.close if mode == :recursive_close
        host_tls.merge(version: "one")
      end
      host = ConnectRPC::Async::Host.new(server: server, tls_provider: provider,
                                         refresh_timeout: 0.05, on_tls_error: ->(_) {}).start
      mode = :recursive_close
      expect { host.refresh_tls }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error) do |error|
        expect(error.category).to eq(:source)
      end
      expect(host.tls_status[:state]).to eq(:running)
      with_ca do |ca|
        origin = "https://127.0.0.1:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca)
        expect(client(transport, origin).echo(AsyncHelpers::MESSAGE.new).message).to eq(AsyncHelpers::MESSAGE.new)
      ensure
        transport&.close(grace: 0)
      end
    ensure
      host&.close(grace: 0)
    end
  end
end
