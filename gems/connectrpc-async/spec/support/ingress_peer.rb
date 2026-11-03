# frozen_string_literal: true

# Real localhost wire fixture. Only observations live here; parsing, HPACK,
# stream states and application bodies remain the pinned native stack.
require_relative "ingress_client"

module IngressPeer
  include IngressClient

  def observe_ingress_wire(connection)
    observations = { credit: Hash.new(0), resets: {}, pings: [], goaway: nil }
    connection.define_singleton_method(:read_frame) do
      super() do |frame|
        case frame
        when Protocol::HTTP2::WindowUpdateFrame
          observations[:credit][frame.stream_id] += frame.unpack
        when Protocol::HTTP2::ResetStreamFrame
          observations[:resets][frame.stream_id] = frame.unpack
        when Protocol::HTTP2::PingFrame
          observations[:pings] << frame.unpack if frame.acknowledgement?
        when Protocol::HTTP2::GoawayFrame
          observations[:goaway] = frame.unpack
        end
      end
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
