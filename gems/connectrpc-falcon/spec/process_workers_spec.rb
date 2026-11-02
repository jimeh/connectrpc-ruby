# frozen_string_literal: true

require_relative "support/process_cluster"

RSpec.describe "Real Falcon process-worker lifecycle" do
  def configuration(writer, make_core:, provider: nil, rack: nil, prepare: nil, count: 2, **options)
    Async::Service::Configuration.build do
      service "connectrpc-process-spec" do
        include ConnectRPC::Falcon::Environment
        include FalconProcessFixtures::FalconWorkerObservation

        service_class FalconProcessFixtures::FalconObservedService
        url "#{provider ? 'https' : 'http'}://127.0.0.1:0"
        count count
        health_check_timeout nil
        events writer
        prepare! do |_instance|
          Thread.current[:falcon_worker_trace] = TracePoint.new(:call) do |event|
            if event.defined_class == Async::Task && event.method_id == :warn
              writer.puts(JSON.generate(kind: :unhandled, pid: Process.pid))
            end
          end.enable
          writer.puts(JSON.generate(kind: :prepared, pid: Process.pid))
          prepare&.call
        end
        connectrpc_server { make_core.call }
        tls_provider { provider }
        rack_app { rack }
        server_options options
      end
    end
  end

  it "loads and polls independently after prepare in at least two real PIDs on the same parent-bound listener" do
    reactor do |task|
      Dir.mktmpdir("connectrpc-falcon-workers-") do |directory|
        source = File.join(directory, "version")
        File.write(source, "one")
        reader, writer = IO.pipe
        writer = FalconProcessFixtures::FalconEventWriter.new(writer)
        writer.sync = true
        initial = host_tls
        replacement = host_tls(credentials(102, "localhost", issuer: tls_material[:root], usage: "serverAuth"))
        provider = lambda do
          version = File.read(source)
          writer.puts(JSON.generate(kind: :provider, pid: Process.pid, version: version))
          (version == "one" ? initial : replacement).merge(version: version)
        end
        core = -> { server }
        rack = lambda do |_env|
          worker = Thread.current.thread_variable_get(:falcon_observed_worker)
          [200, { "content-type" => "application/json" },
           [JSON.generate(pid: Process.pid, generation: worker.tls_generation)]]
        end
        config = configuration(writer, make_core: core, provider: provider, rack: rack,
                                       refresh_interval: 0.03, refresh_jitter: 0)
        cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
        cluster.event(:running, count: 2)
        pids = cluster.events.select { |event| event["kind"] == "running" }.map { |event| event["pid"] }.uniq
        expect(pids.size).to eq(2)
        expect(pids).not_to include(Process.pid)
        pids.each do |pid|
          events = cluster.events.select { |event| event["pid"] == pid }.map { |event| event["kind"] }
          expect(events.index("prepared")).to be < events.index("provider")
        end
        File.write(source, "two")
        task.with_timeout(5) do
          loop do
            cluster.event(:provider, count: cluster.events.count { |item| item["kind"] == "provider" } + 1)
            changed = cluster.events.select { |item| item["kind"] == "provider" && item["version"] == "two" }
            break if changed.map { |item| item["pid"] }.uniq.sort == pids.sort
          end
        end
        with_ca do |ca|
          origin = "https://127.0.0.1:#{cluster.service.port}"
          seen = []
          task.with_timeout(5) do
            until seen.size >= 6
              transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca)
              expect(client(transport,
                            origin).echo(AsyncHelpers::MESSAGE.new(name: "workers")).message.name).to eq("workers")
              resource = transport.instance_variable_get(:@resources).first
              seen << resource.socket.peer_cert.serial.to_i
              transport.close(grace: 0)
            end
          end
          expect(seen).to all(eq(102))
          ssl_context = OpenSSL::SSL::SSLContext.new
          ssl_context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
          ssl_context.cert_store = OpenSSL::X509::Store.new.tap { |store| store.add_file(ca) }
          ssl_context.alpn_protocols = ["http/1.1"]
          discovered = {}
          task.with_timeout(5) do
            until discovered.keys.sort == pids.sort
              status, _headers, bytes = http1(origin, "/worker", context: ssl_context)
              expect(status).to eq(200)
              worker = JSON.parse(bytes)
              discovered[worker.fetch("pid")] = worker.fetch("generation")
            end
          end
          expect(discovered.values).to all(eq(2))
        end
        cluster.stop
        cluster.event(:closed, count: 2)
        expect(cluster.events.none? { |item| item["kind"] == "unhandled" }).to be(true)
        closed = cluster.events.select { |item| item["kind"] == "closed" }
        expect(closed.map { |item| item.values_at("requests", "connections") }).to all(eq([0, 0]))
      ensure
        cluster&.close
      end
    end
  end

  it "uses the actual managed interrupt route to cancel an active refresh and drain an active response" do
    reactor do |task|
      Dir.mktmpdir("connectrpc-falcon-stop-") do |directory|
        release = File.join(directory, "release")
        block_refresh = File.join(directory, "block-refresh")
        reader, writer = IO.pipe
        writer = FalconProcessFixtures::FalconEventWriter.new(writer)
        writer.sync = true
        options = host_tls
        provider = lambda do
          if File.exist?(block_refresh)
            writer.puts(JSON.generate(kind: :refresh_blocked, pid: Process.pid))
            begin
              Async::Condition.new.wait
            ensure
              writer.puts(JSON.generate(kind: :refresh_cancelled, pid: Process.pid))
            end
          end
          options.merge(version: "one")
        end
        rack = lambda do |env|
          env["rack.response_finished"] << lambda { |*|
            writer.puts(JSON.generate(kind: :response_finished, pid: Process.pid))
          }
          body = Class.new(Protocol::HTTP::Body::Readable) do
            define_method(:initialize) { @offset = 0 }
            define_method(:length) { 2_000_000 }
            define_method(:read) do
              return if @offset >= 2_000_000

              if @offset == 16_384
                writer.puts(JSON.generate(kind: :response_blocked, pid: Process.pid))
                Async::Task.current.yield until File.exist?(release)
              end
              size = [16_384, 2_000_000 - @offset].min
              @offset += size
              "x" * size
            end
          end.new
          [200, { "content-type" => "text/plain" }, body]
        end
        config = configuration(writer, make_core: -> { server }, provider: provider, rack: rack, count: 1,
                                       refresh_interval: 0.03, refresh_jitter: 0)
        cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
        cluster.event(:running)
        with_ca do |ca|
          ssl_context = OpenSSL::SSL::SSLContext.new
          ssl_context.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
          ssl_context.cert_store = OpenSSL::X509::Store.new.tap { |store| store.add_file(ca) }
          ssl_context.alpn_protocols = ["http/1.1"]
          uri = "https://127.0.0.1:#{cluster.service.port}"
          response_task = task.async { http1(uri, "/stop", context: ssl_context) }
          cluster.event(:response_blocked)
          File.write(block_refresh, "yes")
          cluster.event(:refresh_blocked)
          cluster.stop
          cluster.event(:closing)
          File.write(release, "yes")
          expect(response_task.wait.values_at(0, 2)).to eq([200, "x" * 2_000_000])
          cluster.event(:closed)
          kinds = cluster.events.map { |item| item["kind"] }
          expect(kinds).to include("refresh_cancelled", "response_finished")
          expect(kinds).not_to include("unhandled")
          expect(cluster.events.last.values_at("requests", "connections")).to eq([0, 0])
        end
      ensure
        cluster&.close
      end
    end
  end
  it "fails initial loading before accepts and leaves default worker restart diagnostics sanitized" do
    reactor do |task|
      Dir.mktmpdir("connectrpc-falcon-startup-") do |directory|
        log = File.join(directory, "worker.log")
        ready = File.join(directory, "allow-provider")
        reader, writer = IO.pipe
        writer = FalconProcessFixtures::FalconEventWriter.new(writer)
        writer.sync = true
        options = host_tls
        provider = lambda do
          writer.puts(JSON.generate(kind: :source_entered, pid: Process.pid))
          raise "secret-initial-database-password" if File.exist?(ready)

          Async::Condition.new.wait
          options.merge(version: "never")
        end
        preparation = -> { $stderr.reopen(log, "a") }
        config = configuration(writer, make_core: -> { server }, provider: provider, prepare: preparation,
                                       count: 1, refresh_timeout: 0.08)
        cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
        first = cluster.event(:source_entered)
        raw = IO::Endpoint.tcp("127.0.0.1", cluster.service.port).connect
        raw.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        # Observe default restart after the initial supervised timeout. The
        # queued TCP peer cannot have had any application response in between.
        File.write(ready, "fail next startup")
        second = cluster.event(:source_entered, count: 2)
        expect(second.fetch("pid")).not_to eq(first.fetch("pid"))
        expect(raw.read_nonblock(1, exception: false)).to eq(:wait_readable)
        expect(cluster.events.none? { |item| item["kind"] == "running" }).to be(true)
        task.with_timeout(2) { task.yield until File.read(log).include?("TLS configuration failed") }
        expect(File.read(log)).not_to include("secret-initial-database-password", "BEGIN PRIVATE KEY")
        expect(config.environments.first.evaluator.container_options[:restart]).to be(true)
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        cluster&.close
      end
    end
  end
  it "keeps a healthy worker serving while default supervision restarts a failing worker" do
    reactor do
      Dir.mktmpdir("connectrpc-falcon-mixed-") do |directory|
        claim = File.join(directory, "healthy-owner")
        log = File.join(directory, "workers.log")
        reader, writer = IO.pipe
        writer = FalconProcessFixtures::FalconEventWriter.new(writer)
        writer.sync = true
        prepare = lambda do
          healthy = begin
            File.open(claim, File::WRONLY | File::CREAT | File::EXCL) { |file| file.write(Process.pid) }
            true
          rescue Errno::EEXIST
            false
          end
          Thread.current.thread_variable_set(:falcon_healthy_worker, healthy)
          $stderr.reopen(log, "a")
        end
        options = host_tls
        provider = lambda do
          unless Thread.current.thread_variable_get(:falcon_healthy_worker)
            writer.puts(JSON.generate(kind: :failed_source, pid: Process.pid))
            raise "secret-mixed-worker-source"
          end
          options.merge(version: "healthy")
        end
        config = configuration(writer, make_core: -> { server }, provider: provider, prepare: prepare)
        cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
        healthy = cluster.event(:running).fetch("pid")
        cluster.event(:failed_source, count: 2)
        failed = cluster.events.select { |item| item["kind"] == "failed_source" }.map { |item| item["pid"] }
        expect(failed.uniq.size).to be >= 2
        expect(failed).not_to include(healthy)
        with_ca do |ca|
          origin = "https://127.0.0.1:#{cluster.service.port}"
          transport = ConnectRPC::Async::Transport.new(origin: origin, ca_file: ca)
          expect(client(transport,
                        origin).echo(AsyncHelpers::MESSAGE.new(name: "healthy")).message.name).to eq("healthy")
        ensure
          transport&.close(grace: 0)
        end
        expect(File.read(log)).not_to include("secret-mixed-worker-source")
      ensure
        cluster&.close
      end
    end
  end
end
