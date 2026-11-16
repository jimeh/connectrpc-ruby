# frozen_string_literal: true

RSpec.describe "Async unary interceptors" do
  let(:log) { [] }
  let(:handled) { [] }
  let(:outcomes) { [] }
  let(:bus) do
    outcomes = self.outcomes
    subscriber = Object.new
    subscriber.define_singleton_method(:start) do |call|
      Object.new.tap do |observer|
        observer.define_singleton_method(:processing_finished) { |outcome| outcomes << [call.role, outcome] }
      end
    end
    ConnectRPC::Instrumentation.new(subscribers: [subscriber])
  end

  def layer(role, &body)
    Object.new.tap do |interceptor|
      interceptor.define_singleton_method(role) { |call, &continuation| body.call(call, &continuation) }
    end
  end

  def tracing(role, name)
    layer(role) do |call, &continuation|
      log << "#{name}>"
      if role == :server_unary
        call.context.request_metadata["x-principal"] = "#{call.protocol}:#{name}" if name == "s1"
        call.context.response_metadata["x-#{name}"] = "before"
      else
        call.request_metadata["x-#{name}"] = "sent"
      end
      result = continuation.call
      call.context.response_trailers["x-#{name}-tail"] = "after" if role == :server_unary
      log << "<#{name}"
      result
    end
  end

  def rejecting
    layer(:server_unary) do |call, &continuation|
      next continuation.call unless call.context.request_metadata["x-reject"]

      call.context.response_metadata["x-context"] = "set"
      raise ConnectRPC::Error.new(:permission_denied, "Rejected", metadata: { "x-error" => "meta" },
                                                                  trailers: { "x-error-tail" => "tail" })
    end
  end

  def replacing
    layer(:server_unary) do |call, &continuation|
      next continuation.call unless call.context.request_metadata["x-replace"]

      AsyncHelpers::MESSAGE.new(name: "replaced #{continuation.call.name}")
    end
  end

  # Runs the continuation in an Async child task and re-raises its rejection.
  def child_task(role)
    layer(role) do |_call, &continuation|
      error = Async do
        continuation.call
        nil
      rescue ConnectRPC::InterceptorStateError => e
        e
      end.wait
      raise error
    end
  end

  def intercepted_server(interceptors)
    handled = self.handled
    echo = lambda do |request, context|
      handled << [request.name, context.request_metadata["x-principal"]&.first]
      request
    end
    server(echo: echo, protocol: %i[connect grpc grpc_web], instrumentation: bus, interceptors: interceptors)
  end

  def message(name = "hello") = AsyncHelpers::MESSAGE.new(name: name)

  def frame(bytes) = [0, bytes.bytesize].pack("CN") + bytes

  # Server processing can finish after the client observes the response.
  def with_host(interceptors, calls: 1)
    reactor do |task|
      host = ConnectRPC::Async::Host.new(server: intercepted_server(interceptors)).start
      yield host, task
      wait_until(task, "server outcomes") { server_outcomes.size >= calls }
    ensure
      host&.close(grace: 0)
    end
  end

  def with_grpc_client(host, interceptors)
    origin = "http://127.0.0.1:#{host.port}"
    transport = ConnectRPC::Async::Transport.new(origin: origin)
    core = ConnectRPC::Client.new(base_url: origin, transport: transport, protocol: :grpc, instrumentation: bus,
                                  binding: ConnectRPC::Bindings::GoogleProtobuf.new, interceptors: interceptors)
    yield core
  ensure
    transport&.close(grace: 0)
  end

  def web_call(host, metadata = {})
    raw = IO::Endpoint.tcp("127.0.0.1", host.port).connect
    connection = Async::HTTP::Protocol::HTTP2.client(raw)
    headers = { "content-type" => "application/grpc-web+proto" }.merge(metadata)
    response = connection.call(Protocol::HTTP::Request["POST", AsyncHelpers::ECHO::ECHO.path, headers,
                                                       [frame(AsyncHelpers::MESSAGE.encode(message))],
                                                       scheme: "http", authority: "127.0.0.1:#{host.port}"])
    [response.status, response.headers.to_h, response.read]
  ensure
    response&.close
    connection&.close
    AsyncHelpers::INTERNAL.raw_close(raw) if raw
  end

  def server_outcomes = outcomes.filter_map { |role, outcome| outcome if role == :server }

  describe "native gRPC unary over h2c" do
    let(:server_chain) { [rejecting, replacing, *%w[s1 s2 s3].map { |name| tracing(:server_unary, name) }] }
    let(:client_chain) { %w[c1 c2 c3].map { |name| tracing(:client_unary, name) } }

    it "composes both chains in order and carries metadata both ways" do
      with_host(server_chain) do |host|
        with_grpc_client(host, client_chain) do |core|
          response = core.call(AsyncHelpers::ECHO::ECHO, message, metadata: { "x-caller" => "zero" })
          expect(response.message.name).to eq("hello")
          expect(response).to be_frozen
          expect(response.metadata.to_h).to include("x-s1" => ["before"], "x-s3" => ["before"])
          expect(response.trailers.to_h).to include("x-s1-tail" => ["after"], "x-s3-tail" => ["after"])
        end
      end
      expect(log).to eq(%w[c1> c2> c3> s1> s2> s3> <s3 <s2 <s1 <c3 <c2 <c1])
      expect(handled).to eq([%w[hello grpc:s1]])
      expect(server_outcomes.first.request_metadata).to include("x-caller" => ["zero"], "x-c3" => ["sent"])
      expect(server_outcomes.first.request_metadata).not_to have_key("x-principal")
    end

    it "sends a server rejection with its metadata and never runs the handler" do
      with_host(server_chain) do |host|
        with_grpc_client(host, client_chain) do |core|
          expect { core.call(AsyncHelpers::ECHO::ECHO, message, metadata: { "x-reject" => "yes" }) }
            .to raise_error(ConnectRPC::Error) { |error|
              expect(error.code).to eq("permission_denied")
              expect(error.message).to eq("Rejected")
              expect(error.metadata.to_h).to include("x-context" => ["set"], "x-error" => ["meta"])
              expect(error.trailers.to_h).to include("x-error-tail" => ["tail"])
            }
        end
      end
      expect(handled).to be_empty
      expect(server_outcomes.first.code).to eq("permission_denied")
    end

    it "sends a server replacement and returns a client replacement" do
      client_replacing = layer(:client_unary) do |_call, &continuation|
        response = continuation.call
        ConnectRPC::Response.new(message: message("client #{response.message.name}"), metadata: response.metadata,
                                 trailers: response.trailers)
      end
      with_host(server_chain) do |host|
        with_grpc_client(host, [client_replacing]) do |core|
          response = core.call(AsyncHelpers::ECHO::ECHO, message, metadata: { "x-replace" => "yes" })
          expect(response.message.name).to eq("client replaced hello")
          expect(response).to be_frozen
        end
      end
    end

    it "rejects a continuation run in an Async child task on both roles" do
      with_host([child_task(:server_unary)]) do |host|
        with_grpc_client(host, []) do |core|
          expect { core.call(AsyncHelpers::ECHO::ECHO, message) }
            .to raise_error(ConnectRPC::Error) { |error| expect(error.code).to eq("internal") }
        end
        with_grpc_client(host, [child_task(:client_unary)]) do |core|
          expect { core.call(AsyncHelpers::ECHO::ECHO, message) }
            .to raise_error(ConnectRPC::InterceptorStateError, /another fiber or thread/)
        end
      end
      expect(handled).to be_empty
      expect(server_outcomes.size).to eq(1)
      expect(server_outcomes.first.error).to be_a(ConnectRPC::InterceptorStateError)
      expect(server_outcomes.first.error.message).to match(/another fiber or thread/)
    end
  end

  describe "gRPC-Web unary over h2c" do
    let(:server_chain) { [rejecting, replacing, *%w[s1 s2].map { |name| tracing(:server_unary, name) }] }

    it "runs the chain in order with request metadata shared with the handler" do
      with_host(server_chain) do |host|
        status, headers, body = web_call(host, "x-caller" => "zero")
        expect(status).to eq(200)
        expect(headers).to include("x-s1" => ["before"])
        expect(body).to include("grpc-status: 0\r\n", "x-s1-tail: after\r\n")
      end
      expect(log).to eq(%w[s1> s2> <s2 <s1])
      expect(handled).to eq([%w[hello grpc_web:s1]])
      expect(server_outcomes.first.request_metadata).to include("x-caller" => ["zero"])
      expect(server_outcomes.first.request_metadata).not_to have_key("x-principal")
    end

    it "sends a rejection and a replacement" do
      with_host(server_chain, calls: 2) do |host|
        _, headers, body = web_call(host, "x-reject" => "yes")
        expect(headers).to include("x-context" => ["set"], "x-error" => ["meta"])
        expect(body).to include("grpc-status: 7\r\n", "grpc-message: Rejected\r\n", "x-error-tail: tail\r\n")
        _, _, body = web_call(host, "x-replace" => "yes")
        expect(body).to include("replaced hello", "grpc-status: 0\r\n")
      end
      expect(handled.size).to eq(1)
    end

    it "rejects a server continuation run in an Async child task" do
      with_host([child_task(:server_unary)]) do |host|
        _, _, body = web_call(host)
        expect(body).to include("grpc-status: 13\r\n")
      end
      expect(handled).to be_empty
      expect(server_outcomes.first.error).to be_a(ConnectRPC::InterceptorStateError)
    end
  end
end
