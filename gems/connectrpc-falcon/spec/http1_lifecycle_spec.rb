# frozen_string_literal: true

class FalconEventBody < Protocol::HTTP::Body::Readable
  def initialize(total:, gate: nil, entered: nil, chunked: false)
    super()
    @total = total
    @gate = gate
    @entered = entered
    @chunked = chunked
    @offset = 0
  end

  def length = @chunked ? nil : @total

  def read
    return if @offset >= @total

    if @offset == 16_384 && @gate
      @entered.enqueue(true)
      @gate.dequeue
    end
    size = [16_384, @total - @offset].min
    @offset += size
    "x" * size
  end
end

RSpec.describe "Falcon HTTP/1 wire lifecycle" do
  [false, true].each do |chunked|
    it "drains a large #{chunked ? 'chunked' : 'fixed'} response and refuses the pipelined next request" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        routes = []
        registry = Async::Utilization::Registry.new
        rack = lambda do |env|
          routes << env["PATH_INFO"]
          [200, { "content-type" => "text/plain" },
           FalconEventBody.new(total: 2_000_000, gate: release, entered: entered, chunked: chunked)]
        end
        falcon(rack_app: rack, utilization_registry: registry) do |host, origin|
          raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          connection = Async::HTTP::Protocol::HTTP1.client(raw)
          response = connection.call(Protocol::HTTP::Request["GET", "/first", {}, nil, authority: "localhost"])
          raw.write("GET /unadmitted HTTP/1.1\r\nHost: localhost\r\n\r\n")
          entered.dequeue
          closing = task.async { host.close(grace: 2) }
          release.enqueue(true)
          expect(response.read).to eq("x" * 2_000_000)
          expect(registry.metric(:connections_active).value).to eq(1)
          raw.shutdown(Socket::SHUT_WR)
          expect(raw.read).to eq("")
          expect(closing.wait).to be(true)
          expect(routes).to eq(["/first"])
          expect(registry.metric(:requests_active).value).to eq(0)
          expect(registry.metric(:connections_active).value).to eq(0)
        ensure
          AsyncHelpers::INTERNAL.raw_close(raw)
          connection&.close
        end
      end
    end
  end

  it "keeps an already admitted next request alive across retirement" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      rack = lambda do |env|
        body = if env["PATH_INFO"] == "/next"
                 FalconEventBody.new(total: 2_000_000, gate: release, entered: entered)
               else
                 ["first"]
               end
        [200, { "content-type" => "text/plain" }, body]
      end
      falcon(rack_app: rack) do |host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        first = connection.call(Protocol::HTTP::Request["GET", "/first", {}, nil, authority: "localhost"])
        expect(first.read).to eq("first")
        response = connection.call(Protocol::HTTP::Request["GET", "/next", {}, nil, authority: "localhost"])
        entered.dequeue
        closing = task.async { host.close(grace: 2) }
        release.enqueue(true)
        expect(response.read).to eq("x" * 2_000_000)
        raw.shutdown(Socket::SHUT_WR)
        expect(raw.read).to eq("")
        expect(closing.wait).to be(true)
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        connection&.close
      end
    end
  end

  it "bounds idle parsing and real Rack callbacks without reentry or metric leaks" do
    reactor do |task|
      entered = Async::Queue.new
      calls = 0
      registry = Async::Utilization::Registry.new
      rack = lambda do |env|
        env["rack.response_finished"] << lambda { |*|
          calls += 1
          entered.enqueue(true)
          Async::Condition.new.wait
        }
        [200, { "content-type" => "text/plain" }, ["finished-wire"]]
      end
      falcon(rack_app: rack, utilization_registry: registry) do |host, origin|
        idle = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        idle.write("GET /idle HTTP/1.1\r\n")
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        response = connection.call(Protocol::HTTP::Request["GET", "/callback", {}, nil, authority: "localhost"])
        expect(response.read).to eq("finished-wire")
        entered.dequeue
        expect(registry.metric(:requests_active).value).to eq(1)
        expect(host.close(grace: 0.03)).to be(false)
        wait_until(task, "all accepted tasks and callback metrics closed") do
          registry.metric(:connections_active).value.zero? && registry.metric(:requests_active).value.zero?
        end
        expect(calls).to eq(1)
        expect(idle.read).to eq("")
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        AsyncHelpers::INTERNAL.raw_close(idle)
        connection&.close
      end
    end
  end

  it "aborts a genuinely stalled writer at grace expiry after real response progress" do
    reactor do |task|
      registry = Async::Utilization::Registry.new
      rack = ->(_env) { [200, {}, FalconEventBody.new(total: 64_000_000)] }
      falcon(rack_app: rack, utilization_registry: registry) do |host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        raw.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 4096)
        raw.write("GET /stalled HTTP/1.1\r\nHost: localhost\r\n\r\n")
        expect(raw.readpartial(16_384)).to include("HTTP/1.1 200")
        expect(registry.metric(:requests_active).value).to eq(1)
        expect(host.close(grace: 0.03)).to be(false)
        wait_until(task, "stalled writer metrics closed") do
          registry.metric(:requests_active).value.zero? && registry.metric(:connections_active).value.zero?
        end
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
      end
    end
  end
  it "retains global request admission through HTTP/1 body wire completion" do
    reactor do
      entered = Async::Queue.new
      release = Async::Queue.new
      rack = lambda do |env|
        body = if env["PATH_INFO"] == "/held"
                 FalconEventBody.new(total: 200_000, gate: release, entered: entered)
               else
                 ["other"]
               end
        [200, {}, body]
      end
      falcon(rack_app: rack, max_requests: 1) do |_host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        response = connection.call(Protocol::HTTP::Request["GET", "/held", {}, nil, authority: "localhost"])
        entered.dequeue
        expect(http1(origin, "/while-held").first).to eq(503)
        release.enqueue(true)
        expect(response.read).to eq("x" * 200_000)
        expect(http1(origin, "/after-completion").values_at(0, 2)).to eq([200, "other"])
      ensure
        release.enqueue(true)
        AsyncHelpers::INTERNAL.raw_close(raw)
        connection&.close
      end
    end
  end
end
