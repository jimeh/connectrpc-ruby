# frozen_string_literal: true

RSpec.describe ConnectRPC::Async::TLSConfiguration do
  def managed(provider, **)
    described_class.new(tls_provider: provider, on_error: ->(event) { @events << event }, **)
  end

  before { @events = [] }

  it "validates complete snapshots, preserves unchanged canonical content and allows deliberate rollback" do
    reactor do
      source = host_tls.merge(version: "opaque-secret")
      manager = managed(-> { source }).start
      first = manager.status
      expect(manager.generation).to eq(1)
      source = source.merge(certificate_chain: source[:certificate_chain].map { |pem| "\n#{pem}\n" })
      expect(manager.refresh).to eq(1)
      expect(manager.status[:publication_age]).to be >= first[:publication_age]
      expect(manager.status[:source_check_age]).to be < manager.status[:publication_age]
      source = source.merge(version: "other")
      expect(manager.refresh).to eq(2)
      source = source.merge(version: "opaque-secret")
      expect(manager.refresh).to eq(3)
      expect { manager.reload(**host_tls) }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR, /Provider owns/)
      expect([manager.inspect, manager.status.inspect, @events.inspect].join).not_to include("opaque-secret", "BEGIN")
    ensure
      manager&.close
    end
  end

  it "rejects changed content under the same version and preserves the last successful publication" do
    reactor do
      source = host_tls.merge(version: "v1")
      manager = managed(-> { source }).start
      source = host_tls(credentials(102, "localhost", issuer: tls_material[:root], usage: "serverAuth"))
               .merge(version: "v1")
      expect { manager.refresh }.to raise_error(described_class::Error, /version_conflict/) { |e|
        expect(e.cause).to be_nil
      }
      expect(manager.generation).to eq(1)
      expect(@events.last).to be_frozen
      expect(@events.last[:status]).to be_frozen
      expect(@events.last[:category]).to eq(:version_conflict)
    ensure
      manager&.close
    end
  end

  it "rejects every incomplete, malformed and mismatched provider candidate without downgrade" do
    reactor do
      source = host_tls.merge(version: "v1")
      manager = managed(-> { source }).start
      invalid = [nil, {}, source.except(:client_ca), source.merge(extra: true), source.merge(version: nil),
                 source.merge(version: ""), source.merge(private_key: "secret-credential"),
                 source.merge(client_ca: []), source.merge(private_key: tls_material[:identity].last.private_to_pem)]
      invalid.each do |candidate|
        source = candidate
        expect { manager.refresh }.to raise_error(described_class::Error) { |e|
          expect(e.cause).to be_nil
          expect(e.full_message).not_to include("secret-credential", "BEGIN")
        }
        expect(manager.generation).to eq(1)
      end
      source = host_tls.merge(version: "recovered")
      expect(manager.refresh).to eq(2)
      expect(manager.status[:category]).to be_nil
    ensure
      manager&.close
    end
  end

  it "coalesces callers and isolates cancellation of one waiter from provider work" do
    reactor do |task|
      calls = 0
      entered = Async::Queue.new
      release = Async::Queue.new
      manager = managed(lambda {
        calls += 1
        if calls > 1
          entered.enqueue(true)
          release.dequeue
        end
        host_tls.merge(version: calls.to_s)
      }).start
      first = task.async { manager.refresh }
      entered.dequeue
      second = task.async { manager.refresh }
      task.yield
      first.cancel
      first.wait
      expect(calls).to eq(2)
      release.enqueue(true)
      expect(second.wait).to eq(2)
      expect(manager.generation).to eq(2)
    ensure
      manager&.close
    end
  end

  it "times out cooperatively, sanitizes source and reporter exceptions, and recovers" do
    reactor do
      mode = :ok
      entered = Async::Queue.new
      manager = described_class.new(tls_provider: lambda {
        case mode
        when :error then raise "secret-database-url"
        when :blocked
          entered.enqueue(true)
          Async::Condition.new.wait
        end
        host_tls.merge(version: mode.to_s)
      }, refresh_timeout: 0.02, on_error: ->(_event) { raise "secret-reporter" }).start
      mode = :error
      expect { manager.refresh }.to raise_error(described_class::Error, /source/) { |e| expect(e.cause).to be_nil }
        .and output("ConnectRPC TLS error reporter failed\n").to_stderr
      mode = :blocked
      expect { manager.refresh }.to raise_error(described_class::Error, /timeout/)
        .and output("ConnectRPC TLS error reporter failed\n").to_stderr
      expect(entered.dequeue).to be(true)
      expect(manager.generation).to eq(1)
      mode = :recovered
      expect(manager.refresh).to eq(2)
    ensure
      manager&.close
    end
  end

  it "rejects recursive provider lifecycle calls safely and prevents late publication after close" do
    reactor do |task|
      mode = :ok
      entered = Async::Queue.new
      manager = nil
      manager = managed(lambda {
        case mode
        when :refresh then manager.refresh
        when :close then manager.close
        when :blocked
          entered.enqueue(true)
          Async::Condition.new.wait
        end
        host_tls.merge(version: mode.to_s)
      }).start
      %i[refresh close].each do |recursive|
        mode = recursive
        expect { manager.refresh }.to raise_error(described_class::Error, /source/)
      end
      mode = :blocked
      caller = task.async(finished: false) do
        manager.refresh
      rescue described_class::Error => e
        e.category
      end
      entered.dequeue
      manager.close
      caller.wait
      expect(manager.generation).to eq(1)
      expect(manager.status[:state]).to eq(:closed)
      expect { manager.refresh }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    ensure
      manager&.close
    end
  end

  it "fails initial provider loading closed and emits only a safe event" do
    reactor do
      manager = managed(-> { raise "secret-initial" })
      expect { manager.start }.to raise_error(described_class::Error, /source/)
      expect(manager.generation).to be_nil
      expect(@events.last[:category]).to eq(:source)
      expect(@events.inspect).not_to include("secret-initial")
      expect { manager.send(:capture, nil) }.to raise_error(AsyncHelpers::LIFECYCLE_ERROR)
    ensure
      manager&.close
    end
  end
  it "validates provider settings and sanitizes script-loading failures before task logging" do
    reactor do
      [
        { tls: false }, { tls_provider: false }, { on_error: false }, { refresh_interval: 0 },
        { refresh_timeout: Float::INFINITY }, { refresh_jitter: -0.1 }, { refresh_jitter: 1 },
        { refresh_jitter: Float::NAN }, { alpn_protocols: ["http/1.0"] }, { alpn_protocols: [] },
        { tls: host_tls, tls_provider: -> {} }
      ].each do |options|
        expect { described_class.new(**options) }.to raise_error(ArgumentError)
      end
      manager = managed(-> { raise SyntaxError, "secret-source-script" })
      expect { manager.start }.to raise_error(described_class::Error, /source/) do |error|
        expect(error.cause).to be_nil
        expect(error.full_message).not_to include("secret-source-script")
      end
    ensure
      manager&.close
    end
  end
  it "fails a self-cancelled initial provider closed rather than treating nil task completion as plaintext" do
    reactor do
      manager = managed(-> { raise Async::Cancel })
      expect { manager.start }.to raise_error(described_class::Error, /source/)
      expect(manager.generation).to be_nil
      expect(manager.status[:state]).to eq(:new)
      expect(@events.last[:category]).to eq(:source)
    ensure
      manager&.close
    end
  end
  it "reports periodic outages safely, keeps the last good generation and automatically recovers" do
    reactor do |task|
      calls = 0
      recover = false
      manager = described_class.new(tls_provider: lambda {
        calls += 1
        raise "secret-periodic-source" if calls > 1 && !recover

        host_tls.merge(version: recover ? "recovered" : "initial")
      }, refresh_interval: 0.01, refresh_jitter: 0).start
      expect do
        wait_until(task, "periodic source failure") { manager.status[:category] == :source }
      end.to output(/\A(?:ConnectRPC TLS refresh failed \(source\)\n)+\z/).to_stderr
      expect(manager.generation).to eq(1)
      recover = true
      wait_until(task, "automatic source recovery") { manager.generation == 2 }
      expect(manager.status[:category]).to be_nil
      expect(calls).to be >= 3
    ensure
      manager&.close
    end
  end
end
