# frozen_string_literal: true

require "digest"
require_relative "support/process_cluster"
require_relative "support/http1_helpers"
require_relative "../../connectrpc-async/spec/support/ingress_peer"

RSpec.describe "Real Falcon process-worker lifecycle" do
  include IngressPeer

  %i[http1 h2].each do |protocol|
    it "drains a real #{protocol} Web worker after first wire data and keeps Context cancellation honest" do
      reactor do
        Dir.mktmpdir("web-worker-") do |directory|
          release = File.join(directory, "release")
          reader, writer = IO.pipe
          writer = FalconProcessFixtures::FalconEventWriter.new(writer)
          writer.sync = true
          declaration = ConnectRPC::ServerStreamMethod.new(service: "worker.Web", name: "Watch",
                                                           request_type: AsyncHelpers::MESSAGE,
                                                           response_type: AsyncHelpers::MESSAGE)
          make_core = lambda do
            ConnectRPC::Server.new(
              services: [ConnectRPC::Service.new(name: declaration.service, methods: [declaration])],
              binding: ConnectRPC::Bindings::GoogleProtobuf.new, protocol: :grpc_web,
              handlers: { declaration.path => lambda { |message, output, context|
                begin
                  output.send_message(message)
                  writer.puts(JSON.generate(kind: :web_waiting, pid: Process.pid))
                  Async::Task.current.with_timeout(5) { Async::Task.current.yield until File.exist?(release) }
                  output.send_message(message)
                ensure
                  writer.puts(JSON.generate(kind: :web_handler_closed, reason: context.cancellation_reason))
                end
              } }
            )
          end
          rack = ->(_) { [200, { "content-type" => "text/plain", "cache-control" => "no-store" }, ["healthy"]] }
          config = configuration(writer, make_core: make_core, rack: rack, count: 1)
          cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
          pid = cluster.event(:running).fetch("pid")
          origin = "http://127.0.0.1:#{cluster.service.port}"
          raw = IO::Endpoint.tcp("127.0.0.1", cluster.service.port).connect
          implementation = protocol == :h2 ? Async::HTTP::Protocol::HTTP2 : Async::HTTP::Protocol::HTTP1
          connection = implementation.client(raw)
          bytes = AsyncHelpers::MESSAGE.encode(AsyncHelpers::MESSAGE.new(name: "wire progress"))
          frame = [0, bytes.bytesize].pack("CN") + bytes
          response = connection.call(Protocol::HTTP::Request["POST", declaration.path,
                                                             { "content-type" => "application/grpc-web" }, [frame],
                                                             authority: URI(origin).authority])
          first = +"".b
          first << response.body.read while first.bytesize < frame.bytesize
          expect(first).to eq(frame)
          expect(cluster.event(:web_waiting).fetch("pid")).to eq(pid)
          expect(http1(origin, "/healthy").values_at(0, 2)).to eq([200, "healthy"])
          cluster.stop
          cluster.event(:closing)
          File.write(release, "release after actual first response frame")
          tail = response.read
          expect(tail).to start_with(frame)
          expect(tail).to end_with("grpc-status: 0\r\n")
          connection.close
          AsyncHelpers::INTERNAL.raw_close(raw)
          expect(cluster.event(:web_handler_closed).fetch("reason")).to be_nil
          expect(cluster.event(:closed).values_at("requests", "connections")).to eq([0, 0])
          expect(cluster.events.map { |event| event["kind"] }).not_to include("unhandled")
        ensure
          File.write(release, "cleanup") if release
          connection&.close
          AsyncHelpers::INTERNAL.raw_close(raw)
          cluster&.close
        end
      end
    end
  end

  %i[discard remaining_grace].each do |phase|
    it "shuts down a real worker while Connect H1 #{phase} owns admission" do
      reactor do
        reader, writer = IO.pipe
        writer = FalconProcessFixtures::FalconEventWriter.new(writer)
        writer.sync = true
        declaration = ConnectRPC::ClientStreamMethod.new(service: "worker.Connect", name: "Collect",
                                                         request_type: AsyncHelpers::MESSAGE,
                                                         response_type: AsyncHelpers::MESSAGE)
        make_core = lambda do
          ConnectRPC::Server.new(
            services: [ConnectRPC::Service.new(name: declaration.service, methods: [declaration])],
            binding: ConnectRPC::Bindings::GoogleProtobuf.new,
            handlers: { declaration.path => ->(*) { AsyncHelpers::MESSAGE.new(name: "early") } }
          )
        end
        prepare = lambda do
          # Reuse the existing worker event pipe to observe the actual phase.
          Thread.current[:h1_hygiene_trace] = TracePoint.new(:call, :return) do |event|
            pending = if phase == :discard
                        event.event == :call && event.method_id == :read &&
                          event.defined_class == Protocol::HTTP1::Body::Fixed
                      else
                        event.event == :return && event.method_id == :discard &&
                          event.defined_class == ConnectRPC::Falcon.const_get(:HTTP1StreamInput)
                      end
            next unless pending

            worker = Thread.current.thread_variable_get(:falcon_observed_worker)
            session = worker.instance_variable_get(:@session)
            writer.puts(JSON.generate(kind: :hygiene_read, pid: Process.pid,
                                      admitted: session.instance_variable_get(:@admitted).size,
                                      raw_closed: session.instance_variable_get(:@resources).first.raw.closed?,
                                      phase: phase))
          end.enable
        end
        config = configuration(writer, make_core: make_core, prepare: prepare, count: 1)
        cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
        pid = cluster.event(:running).fetch("pid")
        peer = ConnectH1Wire.new("http://127.0.0.1:#{cluster.service.port}")
        peer.write("POST #{declaration.path} HTTP/1.1\r\nHost: localhost\r\n" \
                   "Content-Type: application/connect+proto\r\nContent-Length: 100000\r\n\r\n")
        expect(peer.head.first).to eq(200)
        expect(peer.body).to include("early")
        expect(cluster.event(:hygiene_read)).to include("pid" => pid, "admitted" => 1,
                                                        "raw_closed" => false, "phase" => phase.to_s)
        cluster.stop
        cluster.event(:closing)
        expect(cluster.event(:closed).values_at("requests", "connections")).to eq([0, 0])
        expect(cluster.events.map { |event| event["kind"] }).not_to include("unhandled")
      ensure
        peer&.close
        cluster&.close
      end
    end
  end

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

  it "bounds ordinary H2 Rack input in a real worker while sibling controls release partial reads" do
    reactor do |task|
      reader, writer = IO.pipe
      writer = FalconProcessFixtures::FalconEventWriter.new(writer)
      writer.sync = true
      commands = held = nil
      rack = lambda do |env|
        case env.fetch("PATH_INFO")
        when "/hold"
          held = env.fetch("protocol.http.request").body
          commands = Async::Queue.new
          writer.puts(JSON.generate(kind: :ingress_entered, pid: Process.pid))
          bytes = +"".b
          loop do
            command = commands.dequeue
            if command == :drain
              while (chunk = env.fetch("rack.input").read(16_384)) && !chunk.empty?
                bytes << chunk
              end
              break
            end
            bytes << env.fetch("rack.input").read(16_384)
            writer.puts(JSON.generate(kind: :ingress_read, bytes: bytes.bytesize))
          end
          [200, {}, [Digest::SHA256.hexdigest(bytes)]]
        when "/one", "/drain"
          commands.enqueue(env.fetch("PATH_INFO") == "/one" ? :one : :drain)
          [200, {}, ["released"]]
        else
          chunks = held.instance_variable_get(:@queue).instance_variable_get(:@chunks)
          [200, { "cache-control" => "no-store" },
           [JSON.generate(bytes: chunks.sum(&:bytesize), entries: chunks.size, pid: Process.pid)]]
        end
      end
      config = configuration(writer, make_core: -> { server }, rack: rack, count: 1)
      cluster = FalconProcessFixtures::FalconProcessCluster.new(config, reader, writer)
      pid = cluster.event(:running).fetch("pid")
      origin = "http://127.0.0.1:#{cluster.service.port}"
      with_ingress_client(task, origin, :h2c) do |connection, wire|
        response = connection.create_response
        response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", "/hold", {}, nil])
        response.stream.send_headers([[":method", "POST"], [":scheme", "http"],
                                      [":authority", URI(origin).authority], [":path", "/hold"]])
        expect(cluster.event(:ingress_entered).fetch("pid")).to eq(pid)
        id = response.stream.id
        4.times { ingress_data(connection, id, "a" * 16_384) }
        ingress_barrier(task, connection, wire)
        control = lambda do |route|
          verb = route == "/state" ? "GET" : "POST"
          connection.call(Protocol::HTTP::Request[verb, route, {}, nil, scheme: "http",
                                                                        authority: URI(origin).authority]).read
        end
        expect(JSON.parse(control.call("/state"))).to eq("bytes" => 65_536, "entries" => 4, "pid" => pid)
        expect(wire[:credit][id]).to eq(0)
        4.times do |round|
          expect(control.call("/one")).to eq("released")
          expect(cluster.event(:ingress_read, count: round + 1).fetch("bytes")).to eq((round + 1) * 16_384)
          ingress_barrier(task, connection, wire)
          expect(wire[:credit][id]).to eq((round + 1) * 16_384)
          ingress_data(connection, id, "a" * 16_384)
          ingress_barrier(task, connection, wire)
          expect(JSON.parse(control.call("/state")).values_at("bytes", "entries")).to eq([65_536, 4])
        end
        ingress_data(connection, id, "", finish: true)
        control.call("/drain")
        connection.read_response(response)
        expect(response.read).to eq(Digest::SHA256.hexdigest("a" * 131_072))
      end
      cluster.stop
      closed = cluster.event(:closed)
      expect(closed.values_at("requests", "connections")).to eq([0, 0])
      expect(cluster.events.none? { |event| event["kind"] == "unhandled" }).to be(true)
    ensure
      cluster&.close
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
