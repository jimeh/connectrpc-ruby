# frozen_string_literal: true

# X.509 validity is wall-clock time, so these examples use real certificate
# lifetimes of about a second. Every wait on that boundary is bounded.
RSpec.describe "HTTP/2 TLS identity expiry" do
  def short_lived(serial)
    leaf = credentials(serial, "localhost", issuer: tls_material.fetch(:root), usage: "serverAuth")
    # Certificates hold whole seconds: this leaves 0.5 to 1.5 seconds.
    leaf.first.not_after = Time.at((Time.now + 1.5).to_i)
    leaf.first.sign(tls_material.fetch(:root).last, "SHA256")
    leaf
  end

  def await(task, queue, label)
    task.with_timeout(5) { queue.dequeue }
  rescue Async::TimeoutError
    raise "Timed out waiting for #{label}"
  end

  # OpenSSL compares validity in whole seconds and still accepts the
  # not_after second, so wait on the wall clock until a verifier must reject.
  def past_validity(task, certificate)
    remaining = certificate.not_after + 1 - Time.now
    task.with_timeout(remaining) { Async::Condition.new.wait } if remaining.positive?
  rescue Async::TimeoutError
    nil
  end

  def client_context(ca_file = nil)
    context = OpenSSL::SSL::SSLContext.new
    if ca_file
      context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER, verify_hostname: true)
      store = OpenSSL::X509::Store.new
      store.add_file(ca_file)
      context.cert_store = store
    else
      # Deliberately unverified: this peer only observes the served identity.
      context.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end
    context.alpn_protocols = ["h2"]
    context
  end

  def handshake(port, context)
    raw = IO::Endpoint.tcp("127.0.0.1", port).connect
    ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
    ssl.sync_close = true
    ssl.hostname = "localhost"
    ssl.connect
    ssl.peer_cert.serial.to_i
  ensure
    ssl&.close
    AsyncHelpers::INTERNAL.raw_close(raw)
  end

  it "keeps serving an expired static identity, reports it once and recovers on reload" do
    reactor do |task|
      leaf = short_lived(501)
      events = Async::Queue.new
      host = nil
      reporter_errors = []
      reporter = lambda do |event|
        host.close(grace: 0)
      rescue StandardError => e
        reporter_errors << e
      ensure
        events.enqueue(event)
      end
      host = ConnectRPC::Async::Host.new(server: server, tls: host_tls(leaf), on_tls_error: reporter).start
      expect(host.tls_status[:identity_expires_at]).to eq(leaf.first.not_after)
      expect(host.tls_status[:identity_expires_at]).to be_frozen
      with_ca do |ca_file|
        origin = "https://localhost:#{host.port}"
        transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca_file)
        typed = client(transport, origin)
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "before")).message.name).to eq("before")
        event = await(task, events, "the expired event")
        expect(event).to be_frozen
        expect(event[:category]).to eq(:expired)
        expect(event[:status]).to include(category: :expired, generation: 1)
        expect(event[:status][:identity_expires_at]).to be <= Time.now
        expect(reporter_errors.map(&:class)).to eq([AsyncHelpers::LIFECYCLE_ERROR])
        expect(host.tls_status).to include(state: :running, category: :expired)
        expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "established")).message.name).to eq("established")
        past_validity(task, leaf.first)
        expect { handshake(host.port, client_context(ca_file)) }
          .to raise_error(OpenSSL::SSL::SSLError, /certificate has expired/)
        expect(handshake(host.port, client_context)).to eq(501)
        expect(host.reload_tls(**host_tls)).to eq(2)
        expect(host.tls_status).to include(generation: 2, category: nil)
        expect(host.tls_status[:identity_expires_at]).to be > Time.now
        expect(handshake(host.port, client_context(ca_file))).to eq(101)
        expect(events).to be_empty
      ensure
        transport&.close(grace: 0)
      end
    ensure
      host&.close(grace: 0)
    end
  end

  it "keeps a provider's expiry visible through failed refreshes until rotation" do
    reactor do |task|
      leaf = short_lived(502)
      source = host_tls(leaf).merge(version: "one")
      outage = false
      events = Async::Queue.new
      host = nil
      reporter_errors = []
      reporter = lambda do |event|
        host.refresh_tls if event[:category] == :expired
      rescue StandardError => e
        reporter_errors << e
      ensure
        events.enqueue(event)
      end
      provider = lambda do
        raise "source outage" if outage

        source
      end
      host = ConnectRPC::Async::Host.new(server: server, tls_provider: provider, on_tls_error: reporter).start
      expect(await(task, events, "the expired event")[:category]).to eq(:expired)
      expect(reporter_errors.map(&:class)).to eq([AsyncHelpers::LIFECYCLE_ERROR])
      expect(host.tls_status).to include(generation: 1, category: :expired)
      outage = true
      expect { host.refresh_tls }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error, /source/)
      expect(await(task, events, "the source event")[:category]).to eq(:source)
      expect(host.tls_status).to include(generation: 1, category: :source)
      expect(host.tls_status[:identity_expires_at]).to be <= Time.now
      # Candidates are validated, so the unchanged expired source is invalid.
      outage = false
      expect { host.refresh_tls }.to raise_error(ConnectRPC::Async::TLSConfiguration::Error, /invalid/)
      expect(await(task, events, "the invalid event")[:category]).to eq(:invalid)
      expect(host.tls_status).to include(generation: 1, category: :invalid)
      expect(host.tls_status[:identity_expires_at]).to be <= Time.now
      source = host_tls.merge(version: "two")
      expect(host.refresh_tls).to eq(2)
      expect(host.tls_status).to include(generation: 2, category: nil)
      expect(host.tls_status[:identity_expires_at]).to be > Time.now
      expect(events).to be_empty
    ensure
      host&.close(grace: 0)
    end
  end

  it "re-arms the waiting watcher when a rotation publishes an earlier expiry" do
    reactor do |task|
      events = Async::Queue.new
      host = ConnectRPC::Async::Host.new(server: server, tls: host_tls,
                                         on_tls_error: ->(event) { events.enqueue(event) }).start
      # Start leaves the watcher waiting out the hour-long first identity.
      expect(host_session(host).instance_variable_get(:@tls).instance_variable_get(:@expiry_progress)).to be_waiting
      leaf = short_lived(503)
      expect(host.reload_tls(**host_tls(leaf))).to eq(2)
      event = await(task, events, "the rotated identity's expired event")
      expect(event[:category]).to eq(:expired)
      expect(event[:status]).to include(generation: 2)
      expect(event[:status][:identity_expires_at]).to eq(leaf.first.not_after)
    ensure
      host&.close(grace: 0)
    end
  end

  it "completes close while the expiry reporter is still running" do
    reactor do |task|
      entered = Async::Queue.new
      cancelled = Async::Queue.new
      reporter = lambda do |_event|
        entered.enqueue(true)
        Async::Queue.new.dequeue
      ensure
        cancelled.enqueue(true)
      end
      host = ConnectRPC::Async::Host.new(server: server, tls: host_tls(short_lived(504)), on_tls_error: reporter)
      host.start
      await(task, entered, "the reporter call")
      host.close(grace: 0)
      expect(cancelled).not_to be_empty
      expect(host.wait).to be_nil
      expect(host.tls_status[:state]).to eq(:closed)
    end
  end

  # Wall time read by the watcher's own loop is past expiry; every other read,
  # including candidate validation inside a reporter, sees real time.
  def expire_for_watcher(manager)
    allow(Time).to receive(:now).and_wrap_original do |original|
      reader = caller_locations.find { |frame| frame.path.end_with?("/async/tls.rb", "/async/tls_configuration.rb") }
      if reader&.base_label == "watch_expiry"
        manager.status.fetch(:identity_expires_at) + 1
      else
        original.call
      end
    end
  end

  it "guards a reporter that runs as soon as the watcher starts" do
    reactor do
      result = nil
      manager = nil
      reporter = lambda do |_event|
        result = manager.close
      rescue StandardError => e
        result = e
      end
      manager = ConnectRPC::Async::TLSConfiguration.new(tls: host_tls, on_error: reporter)
      # Models expiry between candidate validation and the watcher's first pass.
      expire_for_watcher(manager)
      expect(manager.start).to equal(manager)
      expect(result).to be_a(AsyncHelpers::LIFECYCLE_ERROR)
      expect(manager.status).to include(state: :running, category: :expired)
    ensure
      manager&.close
    end
  end

  it "lets a static reporter publish a replacement identity on :expired" do
    reactor do |task|
      events = Async::Queue.new
      result = nil
      replacement = host_tls
      manager = ConnectRPC::Async::TLSConfiguration.new(tls: host_tls, on_error: lambda { |event|
        result ||= manager.reload(**replacement)
        events.enqueue(event)
      }).start
      expire_for_watcher(manager)
      manager.instance_variable_get(:@expiry_progress).signal
      expect(await(task, events, "the expired event")[:status]).to include(category: :expired, generation: 1)
      expect(result).to eq(2)
      # The watcher's stubbed clock also sees the replacement as expired, so
      # it reports that generation once too, after re-arming for it.
      expect(await(task, events, "the replacement's event")[:status]).to include(category: :expired, generation: 2)
      expect(manager.status).to include(state: :running, generation: 2)
    ensure
      manager&.close
    end
  end

  it "keeps watching when the expiry warning cannot be written" do
    reactor do |task|
      warned = Async::Queue.new
      manager = ConnectRPC::Async::TLSConfiguration.new(tls: host_tls)
      allow(manager).to receive(:warn) do |*arguments|
        warned.enqueue(arguments)
        raise Errno::EPIPE, "stderr closed"
      end
      manager.start
      expire_for_watcher(manager)
      manager.instance_variable_get(:@expiry_progress).signal
      expect(await(task, warned, "the expiry warning")).to eq(["ConnectRPC TLS identity expired"])
      expect(manager.instance_variable_get(:@expiry)).not_to be_finished
      manager.reload(**host_tls)
      expect(await(task, warned, "the next generation's warning")).to eq(["ConnectRPC TLS identity expired"])
      expect(manager.status).to include(generation: 2, category: :expired)
    ensure
      manager&.close
    end
  end

  it "warns once without a reporter" do
    reactor do |task|
      warned = Async::Queue.new
      manager = ConnectRPC::Async::TLSConfiguration.new(tls: host_tls(short_lived(505)))
      allow(manager).to receive(:warn).and_wrap_original do |original, *arguments|
        original.call(*arguments)
        warned.enqueue(arguments)
      end
      expect do
        manager.start
        expect(await(task, warned, "the expiry warning")).to eq(["ConnectRPC TLS identity expired"])
        expect(manager.status[:category]).to eq(:expired)
      end.to output("ConnectRPC TLS identity expired\n").to_stderr
    ensure
      manager&.close
    end
  end
end
