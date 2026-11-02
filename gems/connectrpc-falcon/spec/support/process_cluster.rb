# frozen_string_literal: true

require "async/service/configuration"
require "async/container/forked"
require "json"

module FalconProcessFixtures
  class FalconEventWriter < SimpleDelegator
    # puts can split the newline into a second write, interleaving worker events.
    # Each test event fits PIPE_BUF and is emitted in one atomic pipe write.
    def puts(message) = write("#{message}\n")
  end

  class FalconObservedWorker < ConnectRPC::Falcon::Server
    def initialize(events:, **)
      @events = events
      @metrics = Async::Utilization::Registry.new
      super(utilization_registry: @metrics, **)
    end

    def run
      super
      Thread.current.thread_variable_set(:falcon_observed_worker, self)
      emit(:running, generation: tls_generation)
      self
    end

    def close(**options)
      emit(:closing)
      result = super
      emit(:closed, result: result, connections: @metrics.metric(:connections_active).value,
                    requests: @metrics.metric(:requests_active).value)
      result
    end

    def emit(kind, **fields)
      @events.puts(JSON.generate(kind: kind, pid: Process.pid, **fields))
    end
  end

  module FalconWorkerObservation
    def make_server(bound)
      FalconObservedWorker.new(events: events, server: connectrpc_server, endpoint: bound, rack_app: rack_app,
                               tls: tls, tls_provider: tls_provider, **server_options)
    end
  end

  class FalconObservedService < ConnectRPC::Falcon::Service
    attr_reader :port

    def make_listener(evaluator, endpoint, bound)
      @port = bound.sockets.first.local_address.ip_port
      super
    end
  end

  class FalconProcessCluster
    attr_reader :events, :service

    def initialize(configuration, reader, writer)
      @events = []
      @reader = reader
      @writer = writer
      @service = FalconObservedService.new(configuration.environments.fetch(0))
      @service.start
      @container = Async::Container::Forked.new
      @service.setup(@container)
      @monitor = Async::Task.current.async { @container.wait }
    end

    def event(kind, count: 1)
      Async::Task.current.with_timeout(5) do
        until (matches = @events.select { |item| item.fetch("kind") == kind.to_s }).size >= count
          line = @reader.gets
          raise "Worker event pipe closed; last events #{@events.last(8)}" unless line

          @events << JSON.parse(line)
        end
        matches.last
      end
    rescue Async::TimeoutError
      raise "Timed out waiting for #{kind}; last events #{@events.last(8)}"
    end

    def stop
      return if @stopped

      @stopped = true
      @container.interrupt
    end

    def close
      stop
      @monitor.wait
      @service.stop
      @reader.close
      @writer.close
    end
  end
end
