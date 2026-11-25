# frozen_string_literal: true

module FalconHelpers
  # Polls an observed value under a bounded deadline until it satisfies the
  # predicate, and reports the last observed value on timeout.
  def wait_observed(task, message, satisfied, within: 2)
    observed = nil
    task.with_timeout(within) { task.yield until satisfied.call(observed = yield) }
  rescue Async::TimeoutError
    raise "Timed out waiting for #{message}; last observed: #{observed.inspect}"
  end

  def falcon(server: self.server, scheme: "http", **)
    bound = ConnectRPC::Falcon::Endpoint.parse("#{scheme}://127.0.0.1:0").bound
    host = ConnectRPC::Falcon::Server.new(server: server, endpoint: bound, **)
    host.run
    yield host, "#{scheme}://127.0.0.1:#{bound.sockets.first.local_address.ip_port}"
  ensure
    host&.close(grace: 0)
    bound&.close
  end

  # Prior-knowledge h2c; `protocol` sends a `:protocol` pseudo-header.
  def h2(origin, route = "/", headers: {}, method: "GET", protocol: nil)
    uri = URI(origin)
    raw = IO::Endpoint.tcp(uri.hostname, uri.port).connect
    connection = Async::HTTP::Protocol::HTTP2.client(raw)
    request = Protocol::HTTP::Request[method, route, headers, nil, scheme: uri.scheme, authority: uri.authority,
                                                                   protocol: protocol]
    response = connection.call(request)
    [response.status, response.headers.to_h, response.read]
  ensure
    connection&.close
    AsyncHelpers::INTERNAL.raw_close(raw) if raw
  end

  def http1(origin, route = "/", headers: {}, body: "", method: "GET", context: nil)
    uri = URI(origin)
    raw = IO::Endpoint.tcp(uri.hostname, uri.port).connect
    socket = raw
    if context
      socket = OpenSSL::SSL::SSLSocket.new(raw, context)
      socket.hostname = uri.hostname
      socket.connect
    end
    connection = Async::HTTP::Protocol::HTTP1.client(socket)
    native = Protocol::HTTP::Request[method, route, headers, body.nil? ? nil : [body], scheme: uri.scheme,
                                                                                       authority: uri.authority]
    response = connection.call(native)
    [response.status, response.headers.to_h, response.read]
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    connection&.close
    socket&.close unless socket.equal?(raw)
  end
end
