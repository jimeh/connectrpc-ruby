# frozen_string_literal: true

module IngressClient
  def with_ingress_client(task, origin, mode)
    uri = URI(origin)
    raw = IO::Endpoint.tcp(uri.hostname, uri.port).connect
    socket = raw
    connection = nil
    with_ca do |ca_file|
      socket = ingress_tls_socket(raw, uri.hostname, ca_file, mode) unless mode == :h2c
      connection = Async::HTTP::Protocol::HTTP2::Client.new(IO::Stream(socket))
      wire = observe_ingress_wire(connection)
      connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128))
      connection.read_in_background(parent: task)
      ingress_barrier(task, connection, wire)
      yield connection, wire
    end
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    connection&.close
    socket&.close unless socket.equal?(raw)
  end

  def ingress_tls_socket(raw, hostname, ca_file, mode)
    identity = tls_material[:identity] if mode == :mtls
    snapshot = AsyncHelpers::INTERNAL::TLS.client(ca_file: ca_file,
                                                  certificate_chain: identity && [identity.first.to_pem],
                                                  private_key: identity&.last&.private_to_pem)
    socket = snapshot.socket(raw)
    socket.hostname = hostname
    socket.connect
    socket.post_connection_check(hostname)
    expect(socket.alpn_protocol).to eq("h2")
    expect(socket.verify_result).to eq(OpenSSL::X509::V_OK)
    socket
  end

  def ingress_wait(task, message, state:)
    task.with_timeout(2) { task.yield until yield }
  rescue Async::TimeoutError
    raise "Timed out waiting for #{message}; last state: #{state.call.inspect}"
  end

  def ingress_backlog(body)
    queue = body.instance_variable_get(:@queue)
    # Stock Writable uses Thread::Queue. Drain/requeue only in this intended-red
    # observation, after a wire barrier and with no consumer running.
    chunks = if queue.is_a?(Thread::Queue)
               Array.new(queue.size) { queue.pop(true) }.tap { |values| values.each { |value| queue.push(value) } }
             else
               queue.instance_variable_get(:@chunks)
             end
    [chunks.sum(&:bytesize), chunks.size]
  end
end
