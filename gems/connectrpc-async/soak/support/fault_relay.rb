# frozen_string_literal: true

module Soak
  # A TCP relay on the harness reactor between clients and one server port.
  # Each accepted connection gets one upstream connection and two pumps that
  # propagate half-close. Faults: `blackhole!` silently drops every byte of the
  # current connections (both directions) while keeping them open; `reset!`
  # sends RST on both sides of every current connection.
  class FaultRelay
    Pair = Struct.new(:client, :server, :pumps, :ended, :blackholed)

    attr_reader :accepted

    def initialize(task, target_port)
      @task = task
      @target = target_port
      @pairs = []
      @accepted = 0
      @bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
      @listener = @bound.sockets.first
      @acceptor = task.async { accept_loop }
    end

    def port = @listener.local_address.ip_port

    def blackhole!
      pairs.each { |pair| pair.blackholed = true }
    end

    def reset!
      pairs.each { |pair| [pair.client, pair.server].each { |socket| rst(socket) } }
    end

    # Connections whose pumps still run. `@pairs` keeps every accepted pair.
    def pairs = @pairs.reject { |pair| pair.pumps.all?(&:finished?) }

    def close
      Soak.stop(@acceptor)
      @bound.close
      @pairs.each { |pair| close_pair(pair) }
      @pairs.flat_map(&:pumps).each { |pump| Soak.stop(pump) }
    end

    def closed? = @listener.closed?

    # Only a closed relay owes anything: its acceptor, listener and pumps
    # stopped. Every pair it ever accepted counts, including those whose pumps
    # finished, which #pairs omits.
    def problems(label)
      return [] unless closed?

      problems = []
      problems << "#{label} acceptor running" unless @acceptor.finished?
      @pairs.each_with_index do |pair, index|
        problems << "#{label} pair #{index} open" unless pair.client.closed? && pair.server.closed?
      end
      problems
    end

    private

    def accept_loop
      loop do
        client, = @listener.accept
        @accepted += 1
        begin
          server = IO::Endpoint.tcp("127.0.0.1", @target).connect
        rescue SystemCallError
          Soak.close_io(client)
          next
        end
        pair = Pair.new(client, server, [], 0, false)
        @pairs << pair
        pair.pumps << @task.async { pump(pair, client, server) }
        pair.pumps << @task.async { pump(pair, server, client) }
      end
    end

    def pump(pair, from, to)
      loop do
        data = from.readpartial(65_536)
        to.write(data) unless pair.blackholed
      end
    rescue EOFError
      begin
        to.close_write unless to.closed?
      rescue IOError, SystemCallError
        close_pair(pair)
      end
      pair.ended += 1
      close_pair(pair) if pair.ended == 2
    rescue IOError, SystemCallError
      close_pair(pair)
    end

    def close_pair(pair)
      Soak.close_io(pair.client)
      Soak.close_io(pair.server)
    end

    def rst(socket)
      return if socket.closed?

      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack("ii"))
      Soak.close_io(socket)
    rescue IOError, SystemCallError
      Soak.close_io(socket)
    end
  end
end
