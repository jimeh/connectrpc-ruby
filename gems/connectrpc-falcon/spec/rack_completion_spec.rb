# frozen_string_literal: true

class FalconGatedBody < Protocol::HTTP::Body::Readable
  attr_reader :closes, :readers

  def initialize(entered: nil, release: nil)
    super()
    @chunks = %w[first second]
    @entered = entered
    @release = release
    @closes = []
    @readers = []
  end

  def read
    @readers << Fiber.current
    if @chunks.size == 1 && @release
      @entered.enqueue(Async::Task.current)
      @release.dequeue
    end
    @chunks.shift
  end

  def close(error = nil)
    @closes << [Fiber.current, error]
    super
  end
end

RSpec.describe "Falcon Rack fallback completion" do
  def fallback(origin, protocol, path = "/fallback", method: "GET")
    protocol == :h2 ? h2(origin, path, method: method) : http1(origin, path, method: method, body: nil)
  end

  %i[http1 h2].each do |protocol|
    it "runs the app, its completion callback and its body close on one request fiber over #{protocol}" do
      reactor do
        closed = Async::Queue.new
        events = []
        rack = lambda do |env|
          called = Fiber.current
          env["rack.response_finished"] << ->(*) { events << [:callback, Fiber.current.equal?(called)] }
          body = FalconGatedBody.new
          body.define_singleton_method(:close) do |error = nil|
            events << [:close, Fiber.current.equal?(called)]
            closed.enqueue(readers.none?(called))
            super(error)
          end
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack) do |_host, origin|
          expect(fallback(origin, protocol).values_at(0, 2)).to eq([200, "firstsecond"])
          # Chunk reads stay on the connection writer or stream output task.
          expect(closed.dequeue).to be(true)
          expect(events).to eq([[:callback, true], [:close, true]])
        end
      end
    end

    %w[HEAD 204].each do |kind|
      it "completes a #{kind} fallback response once on the app's fiber over #{protocol}" do
        reactor do
          completed = Async::Queue.new
          rack = lambda do |env|
            called = Fiber.current
            env["rack.response_finished"] << ->(*) { completed.enqueue(Fiber.current.equal?(called)) }
            [kind == "204" ? 204 : 200, { "content-type" => "text/plain" }, ["ignored"]]
          end
          falcon(rack_app: rack) do |_host, origin|
            response = fallback(origin, protocol, method: kind == "HEAD" ? "HEAD" : "GET")
            expect(response.first).to eq(kind == "204" ? 204 : 200)
            expect(completed.dequeue).to be(true)
            expect(completed).to be_empty
          end
        end
      end
    end

    it "runs completion callbacks for an app StandardError on the app's fiber over #{protocol}" do
      reactor do
        completed = []
        failure = RuntimeError.new("app failed")
        rack = lambda do |env|
          called = Fiber.current
          env["rack.response_finished"] << lambda do |callback_env, status, headers, error|
            completed << [Fiber.current.equal?(called), callback_env.equal?(env), status, headers, error]
          end
          raise failure
        end
        falcon(rack_app: rack) do |_host, origin|
          expect(fallback(origin, protocol).first).to eq(500)
          expect(completed).to eq([[true, true, nil, nil, failure]])
        end
      end
    end

    it "runs no completion callback for an app-raised Async::Stop over #{protocol}" do
      reactor do |task|
        completed = []
        requests = Async::Queue.new
        rack = lambda do |env|
          requests.enqueue(Async::Task.current)
          env["rack.response_finished"] << ->(*) { completed << :callback }
          raise Async::Stop, "app stopped"
        end
        falcon(rack_app: rack) do |_host, origin|
          pending = task.async { fallback(origin, protocol) }
          # Callbacks for a StandardError run before the request task ends.
          task.with_timeout(2) { requests.dequeue.wait }
          expect(completed).to be_empty
          # As before the request task, HTTP/2 leaves this stream unanswered.
          expect(pending.wait.first).to eq(500) if protocol == :http1
        ensure
          pending&.stop
        end
      end
    end

    it "completes a response abandoned by failing response middleware on the app's fiber over #{protocol}" do
      reactor do |task|
        completed = Async::Queue.new
        closes = []
        failure = RuntimeError.new("empty? failed")
        rack = lambda do |env|
          called = Fiber.current
          env["rack.response_finished"] << lambda do |_env, _status, _headers, error|
            completed.enqueue([Fiber.current.equal?(called), error])
          end
          body = Object.new
          body.define_singleton_method(:each) { |&block| block.call("unreachable") }
          # Falcon's content-encoding middleware asks after this guard returned.
          body.define_singleton_method(:empty?) { raise failure }
          body.define_singleton_method(:close) { closes << Fiber.current.equal?(called) }
          [200, { "content-type" => "text/plain" }, body]
        end
        registry = Async::Utilization::Registry.new
        falcon(rack_app: rack, utilization_registry: registry) do |_host, origin|
          # The client sees the same failure as before the request task.
          failed = protocol == :h2 ? Protocol::HTTP::RemoteError : EOFError
          expect { fallback(origin, protocol) }.to raise_error(failed)
          expect(task.with_timeout(2) { completed.dequeue }).to eq([true, failure])
          expect(closes).to eq([true])
          wait_observed(task, "the abandoned request's release", lambda(&:zero?)) do
            registry.metric(:requests_active).value
          end
        end
      end
    end

    # Main ran completion inline on the closing fiber, with no request task;
    # these cover the handoff's cancellation paths.
    it "completes once on the closer when the parked request task was cancelled over #{protocol}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        requests = Async::Queue.new
        callbacks = []
        body = nil
        rack = lambda do |env|
          requests.enqueue(Async::Task.current)
          called = Fiber.current
          env["rack.response_finished"] << ->(*) { callbacks << Fiber.current.equal?(called) }
          body = FalconGatedBody.new(entered: entered, release: release)
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack) do |host, origin|
          pending = task.async { fallback(origin, protocol) }
          request_task = requests.dequeue
          entered.dequeue
          request_task.cancel
          task.with_timeout(2) { request_task.wait }
          expect(callbacks).to be_empty
          release.enqueue(true)
          expect(pending.wait.values_at(0, 2)).to eq([200, "firstsecond"])
          wait_observed(task, "the closer's inline completion", ->(seen) { seen.size >= 1 }) { body.closes.dup }
          # The writer closed the body and ran completion inline, not the
          # cancelled request task.
          expect(callbacks).to eq([false])
          expect(body.closes.size).to eq(1)
          expect(host.close(grace: 1)).to be(true)
        ensure
          pending&.stop
        end
      end
    end

    it "closes once when the request task is cancelled inside a completion callback over #{protocol}" do
      reactor do |task|
        entered = Async::Queue.new
        requests = Async::Queue.new
        registry = Async::Utilization::Registry.new
        calls = 0
        body = nil
        rack = lambda do |env|
          requests.enqueue(Async::Task.current)
          env["rack.response_finished"] << lambda { |*|
            calls += 1
            entered.enqueue(true)
            Async::Condition.new.wait
          }
          body = FalconGatedBody.new
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack, utilization_registry: registry) do |host, origin|
          # HTTP/2 ends the stream only after the body's close returns.
          pending = task.async { fallback(origin, protocol) }
          request_task = requests.dequeue
          entered.dequeue
          expect(body.closes).to be_empty
          request_task.cancel
          expect(pending.wait.values_at(0, 2)).to eq([200, "firstsecond"])
          wait_observed(task, "the cancelled callback's release", lambda(&:zero?)) do
            registry.metric(:requests_active).value
          end
          expect(calls).to eq(1)
          expect(body.closes.size).to eq(1)
          expect(host.close(grace: 1)).to be(true)
        ensure
          pending&.stop
        end
      end
    end
  end

  it "stops an interrupted closer while the request task finishes completion over http1" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      calls = 0
      body = nil
      called = nil
      rack = lambda do |env|
        called = Fiber.current
        env["rack.response_finished"] << lambda { |*|
          calls += 1
          entered.enqueue(true)
          release.dequeue
        }
        body = FalconGatedBody.new
        [200, { "content-type" => "text/plain" }, body]
      end
      falcon(rack_app: rack) do |host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        response = connection.call(Protocol::HTTP::Request["GET", "/held", {}, nil, authority: "localhost"])
        expect(response.read).to eq("firstsecond")
        entered.dequeue
        closer = body.readers.last.async_task
        # The connection defers its first cancel until the response is done;
        # a second cancel interrupts its wait for the running callback.
        closer.cancel
        closer.cancel
        task.with_timeout(2) { closer.wait }
        # Retirement of the dead connection may cancel the request task too,
        # so the callback ends either released or cancelled.
        release.enqueue(true)
        wait_observed(task, "the request task's completion", ->(seen) { seen.size >= 1 }) { body.closes.dup }
        expect(calls).to eq(1)
        expect(body.closes.map(&:first)).to eq([called])
        expect(host.close(grace: 1)).to be(true)
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        connection&.close
      end
    end
  end

  it "processes the next keep-alive request only after the previous completion finished over http1" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      events = []
      rack = lambda do |env|
        path = env["PATH_INFO"]
        events << [:call, path]
        env["rack.response_finished"] << lambda { |*|
          if path == "/first"
            entered.enqueue(true)
            release.dequeue
          end
          events << [:completed, path]
        }
        [200, { "content-type" => "text/plain" }, [path]]
      end
      falcon(rack_app: rack) do |_host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        first = connection.call(Protocol::HTTP::Request["GET", "/first", {}, nil, authority: "localhost"])
        expect(first.read).to eq("/first")
        entered.dequeue
        second = task.async do
          connection.call(Protocol::HTTP::Request["GET", "/second", {}, nil, authority: "localhost"]).read
        end
        # The second request is on the wire before this whole exchange on
        # another connection starts, so a connection that did not wait for the
        # first completion would have dispatched it by now.
        expect(http1(origin, "/other", body: nil).last).to eq("/other")
        expect(events).not_to include([:call, "/second"])
        release.enqueue(true)
        expect(second.wait).to eq("/second")
        expect(events.index([:completed, "/first"])).to be < events.index([:call, "/second"])
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw)
        connection&.close
      end
    end
  end

  it "completes an overlapping cached empty response once" do
    reactor do |task|
      entered = Async::Queue.new
      release = Async::Queue.new
      calls = 0
      closes = 0
      rack = lambda do |env|
        env["rack.response_finished"] << lambda { |*|
          calls += 1
          # Only the first run holds, so a second run shows up in the count.
          if calls == 1
            entered.enqueue(true)
            release.dequeue
          end
        }
        body = []
        body.define_singleton_method(:close) { closes += 1 }
        [200, { "content-type" => "text/plain", "cache-control" => "public, max-age=60" }, body]
      end
      falcon(rack_app: rack, cache: true) do |_host, origin|
        headers = { "accept-encoding" => "identity" }
        expect(http1(origin, "/empty", body: nil, headers: headers).values_at(0, 2)).to eq([200, nil])
        entered.dequeue
        # The hit's shallow copy shares the miss's completion, which is
        # still running, so its close neither waits nor runs it again.
        status, hit_headers, = http1(origin, "/empty", body: nil, headers: headers)
        expect([status, hit_headers["x-cache"]]).to eq([200, ["hit"]])
        expect(calls).to eq(1)
        release.enqueue(true)
        wait_observed(task, "the miss's original close", ->(seen) { seen >= 1 }) { closes }
        expect(calls).to eq(1)
        expect(closes).to eq(1)
      end
    end
  end
end
