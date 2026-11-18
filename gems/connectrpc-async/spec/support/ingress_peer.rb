# frozen_string_literal: true

# Real localhost wire fixture. Only observations live here; parsing, HPACK,
# stream states and application bodies remain the pinned native stack.
require_relative "ingress_client"
require_relative "ingress_wire"

module IngressPeer
  include IngressClient

  # `pings` holds ACK payloads, `probes` the peer's own PING payloads and
  # `settings` its SETTINGS values. `control` orders the peer's stream-0
  # WINDOW_UPDATEs, SETTINGS and PINGs; `data` sums DATA lengths per stream.
  def observe_ingress_wire(connection)
    observations = { credit: Hash.new(0), resets: {}, pings: [], goaway: nil, probes: [], settings: [],
                     control: [], data: Hash.new(0) }
    connection.define_singleton_method(:read_frame) do
      super() { |frame| IngressWire.record(observations, frame) }
    end
    observations
  end

  def ingress_barrier(task, connection, observations)
    token = format("%08d", observations[:pings].size + 1)
    frame = Protocol::HTTP2::PingFrame.new
    frame.pack(token)
    connection.write_frame(frame)
    ingress_wait(task, "PING acknowledgement #{token}", state: -> { observations }) do
      observations[:pings].include?(token)
    end
  end

  # DATA in 16 KiB frames per [stream id, bytes], flushed once: the receiver
  # reads all of it before any reply this peer writes afterwards, such as the
  # ACK of a PING that the first frame prompts.
  def ingress_burst(connection, *bursts)
    connection.write_frames do |framer|
      bursts.each do |id, bytes|
        bytes.fdiv(16_384).ceil.times do |index|
          frame = Protocol::HTTP2::DataFrame.new(id)
          frame.pack("a" * [16_384, bytes - (index * 16_384)].min)
          framer.write_frame(frame)
        end
      end
    end
  end

  # The SETTINGS_INITIAL_WINDOW_SIZE values the peer sent alone, as receive
  # window growth does.
  def ingress_growth(observations)
    window = Protocol::HTTP2::Settings::INITIAL_WINDOW_SIZE
    observations[:settings].select { |values| values.map(&:first) == [window] }.map { |((_, value))| value }
  end

  def ingress_data(connection, id, bytes, padding: nil, finish: false)
    frame = Protocol::HTTP2::DataFrame.new(id, finish ? Protocol::HTTP2::END_STREAM : 0)
    frame.pack(bytes, padding_size: padding)
    connection.write_frame(frame)
  end

  def ingress_pair(task, direction)
    bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
    listener = bound.sockets.first
    accepted, accepting = ingress_accept(task, listener, direction)
    raw = IO::Endpoint.tcp("127.0.0.1", listener.local_address.ip_port).connect
    client_class = direction == :response ? AsyncHelpers::INTERNAL::ClientConnection : Async::HTTP::Protocol::HTTP2::Client
    client_connection = client_class.new(IO::Stream(raw))
    client_connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128))
    client_wire = observe_ingress_wire(client_connection)
    client_connection.read_in_background(parent: task)
    server_connection, server_wire = accepted.dequeue
    sender, receiver, observations = ingress_direction(direction, client_connection, server_connection,
                                                       client_wire, server_wire)
    ingress_barrier(task, sender, observations)
    yield client_connection, server_connection, sender, receiver, observations
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    client_connection&.close
    server_connection&.close
    accepting&.wait
    bound&.close
  end

  def ingress_direction(direction, client_connection, server_connection, client_wire, server_wire)
    if direction == :request
      [client_connection, server_connection, client_wire]
    else
      [server_connection, client_connection, server_wire]
    end
  end

  def ingress_accept(task, listener, direction)
    accepted = Async::Queue.new
    accepting = task.async do
      socket, = listener.accept
      server_class = direction == :request ? AsyncHelpers::INTERNAL::ServerConnection : Async::HTTP::Protocol::HTTP2::Server
      server_connection = server_class.new(IO::Stream(socket))
      server_connection.read_connection_preface(AsyncHelpers::INTERNAL.settings(128))
      server_wire = observe_ingress_wire(server_connection)
      server_connection.read_in_background(parent: task)
      accepted.enqueue([server_connection, server_wire])
    end
    [accepted, accepting]
  end

  def ingress_stream(client_connection, server_connection, direction, length: nil)
    response = client_connection.create_response
    headers = [[":method", "POST"], [":scheme", "http"], [":authority", "localhost"], [":path", "/body"]]
    headers << ["content-length", length.to_s] if length && direction == :request
    response.instance_variable_set(:@request, Protocol::HTTP::Request["POST", "/body", {}, nil])
    response.stream.send_headers(headers)
    incoming = server_connection.requests.dequeue
    if direction == :response
      headers = [[":status", "200"]]
      headers << ["content-length", length.to_s] if length
      incoming.stream.send_headers(headers)
      client_connection.read_response(response)
      [incoming.stream, response.stream, response.body]
    else
      [response.stream, incoming.stream, incoming.body]
    end
  end
end
