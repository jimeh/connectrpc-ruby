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
        reactor do |task|
          completed = Async::Queue.new
          events = []
          rack = lambda do |env|
            called = Fiber.current
            env["rack.response_finished"] << lambda { |*|
              events << [:callback, Fiber.current.equal?(called)]
              completed.enqueue(Fiber.current.equal?(called))
            }
            # An array body converts without closing it, so its close follows
            # the callback.
            body = ["ignored"]
            body.define_singleton_method(:close) { events << [:close, Fiber.current.equal?(called)] }
            [kind == "204" ? 204 : 200, { "content-type" => "text/plain" }, body]
          end
          falcon(rack_app: rack) do |_host, origin|
            response = fallback(origin, protocol, method: kind == "HEAD" ? "HEAD" : "GET")
            expect(response.first).to eq(kind == "204" ? 204 : 200)
            expect(completed.dequeue).to be(true)
            expect(completed).to be_empty
            wait_observed(task, "the original close", ->(seen) { seen.size >= 2 }) { events.dup }
            expect(events).to eq([[:callback, true], [:close, true]])
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
          next [200, {}, [env["PATH_INFO"]]] if env["PATH_INFO"] == "/sibling"

          requests.enqueue(Async::Task.current)
          env["rack.response_finished"] << ->(*) { completed << :callback }
          raise Async::Stop, "app stopped"
        end
        falcon(rack_app: rack) do |host, origin|
          if protocol == :h2
            raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
            connection = Async::HTTP::Protocol::HTTP2.client(raw)
            request = ->(path) { Protocol::HTTP::Request["GET", path, {}, nil, authority: "localhost"] }
            pending = task.async do
              connection.call(request.call("/stop")).read
            rescue EOFError => e
              e
            end
          else
            pending = task.async { fallback(origin, protocol) }
          end
          # Callbacks for a StandardError run before the request task ends.
          task.with_timeout(2) { requests.dequeue.wait }
          expect(completed).to be_empty
          if protocol == :h2
            # As before the request task, HTTP/2 leaves this stream
            # unanswered, without a reset, while its connection serves another
            # stream. Only the connection's retirement ends it.
            expect(task.with_timeout(2) { connection.call(request.call("/sibling")).read }).to eq("/sibling")
            expect(pending).not_to be_finished
            expect(host.close(grace: 0.03)).to be(false)
            expect(task.with_timeout(2) { pending.wait }).to be_a(EOFError)
          else
            expect(pending.wait.first).to eq(500)
          end
        ensure
          pending&.stop
          connection&.close
          AsyncHelpers::INTERNAL.raw_close(raw) if raw
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

    it "completes once on the app's fiber when the handler is cancelled as the response arrives over #{protocol}" do
      reactor do |task|
        completed = Async::Queue.new
        closes = []
        rack = lambda do |env|
          called = Fiber.current
          env["rack.response_finished"] << lambda do |_env, _status, _headers, error|
            completed.enqueue([Fiber.current.equal?(called), error.class])
          end
          # Yield as real app I/O does, so the handler waits on the outcome.
          Async::Task.current.yield
          # Queues the handler's cancel ahead of its wake from the outcome, so
          # it arrives after the response was produced and before the handler
          # resumes.
          Fiber.scheduler.push(Async::Cancel::Later.new(Async::Task.current.parent))
          body = ["x"]
          body.define_singleton_method(:close) { closes << Fiber.current.equal?(called) }
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack) do |host, origin|
          pending = task.async do
            fallback(origin, protocol)
          rescue StandardError => e
            e
          end
          expect(task.with_timeout(2) { completed.dequeue }).to eq([true, Async::Cancel])
          expect(closes).to eq([true])
          expect(completed).to be_empty
          if protocol == :h2
            # A cancelled handler leaves its HTTP/2 stream unanswered, as an
            # app-raised Async::Stop does, until the connection retires.
            expect(host.close(grace: 0.03)).to be(false)
            expect(task.with_timeout(2) { pending.wait }).to be_a(EOFError)
          else
            expect(task.with_timeout(2) { pending.wait }.first).to eq(500)
            expect(host.close(grace: 1)).to be(true)
          end
        ensure
          pending&.stop
        end
      end
    end

    # The handler abandons the response while it waits for the outcome, or
    # while it is still queued behind the request task it has just started.
    { waiting: "", starting: " as it starts the request task" }.each do |window, phrase|
      it "completes on the app's fiber a response returned after the handler abandoned it#{phrase} over #{protocol}" do
        reactor do |task|
          requests = Async::Queue.new
          completed = Async::Queue.new
          closes = []
          rescued = []
          cause = RuntimeError.new("handler cancelled")
          rack = lambda do |env|
            called = Fiber.current
            env["rack.response_finished"] << lambda do |_env, status, _headers, error|
              completed.enqueue([Fiber.current.equal?(called), status, error.class, error&.cause])
            end
            begin
              requests.enqueue(Async::Task.current)
              Async::Condition.new.wait
            rescue Async::Cancel => e
              # As Rails' ShowExceptions does, the app renders the interruption.
              rescued << e.cause
            end
            body = ["failed"]
            body.define_singleton_method(:close) { closes << Fiber.current.equal?(called) }
            [500, { "content-type" => "text/plain" }, body]
          end
          falcon(rack_app: rack) do |host, origin|
            pending = task.async do
              fallback(origin, protocol)
            rescue StandardError => e
              e
            end
            # This fiber yields ahead of the handler, which the request task's
            # start queued behind it, as any canceller already ready in that
            # reactor turn would be.
            task.yield while window == :starting && requests.empty?
            # The handler leaves before the app returns; its cancellation reaches
            # the app through the request task.
            requests.dequeue.parent.cancel(cause: cause)
            expect(task.with_timeout(2) { completed.dequeue }).to eq([true, 500, Async::Cancel, cause])
            expect(rescued).to eq([cause])
            expect(closes).to eq([true])
            expect(completed).to be_empty
            if protocol == :h2
              # The abandoned stream stays unanswered until retirement, as for a
              # handler cancelled as the response arrives.
              expect(task.with_timeout(2) { host.close(grace: 0.03) }).to be(false)
              expect(task.with_timeout(2) { pending.wait }).to be_a(EOFError)
            else
              # The client gets the adapter's failure response, not the
              # abandoned one.
              expect(task.with_timeout(2) { pending.wait }.values_at(0, 2)).to eq([500, nil])
              expect(task.with_timeout(2) { host.close(grace: 1) }).to be(true)
            end
          ensure
            pending&.stop
          end
        end
      end
    end

    # The handler closes these responses itself and is not deferring when
    # retirement cancels it. It relays that cancellation, which the request
    # task defers, and takes on the deferral itself, so retirement's forced
    # delivery reaches the handler and, through it, the callback.
    [["HEAD", :close], ["HEAD", :abort], ["204", :close]].each do |kind, stop|
      it "bounds #{stop} while a #{kind} response's completion callback defers cancellation over #{protocol}" do
        reactor do |task|
          entered = Async::Queue.new
          release = Async::Queue.new
          requests = Async::Queue.new
          closes = []
          rack = lambda do |env|
            requests.enqueue(Async::Task.current)
            called = Fiber.current
            env["rack.response_finished"] << lambda { |*|
              Async::Task.current.defer_cancel do
                entered.enqueue(true)
                release.dequeue
              end
            }
            body = ["ignored"]
            body.define_singleton_method(:close) { closes << Fiber.current.equal?(called) }
            [kind == "204" ? 204 : 200, { "content-type" => "text/plain" }, body]
          end
          falcon(rack_app: rack) do |host, origin|
            pending = task.async do
              fallback(origin, protocol, method: kind == "HEAD" ? "HEAD" : "GET")
            rescue StandardError => e
              e
            end
            request_task = requests.dequeue
            entered.dequeue
            stopped = task.with_timeout(2) { stop == :abort ? host.abort : host.close(grace: 0.03) }
            expect(stopped).to be(false) if stop == :close
            expect(task.with_timeout(2) { request_task.wait }).to be_nil
            expect(request_task).to be_cancelled
            expect(closes).to eq([true])
          ensure
            release&.enqueue(true)
            pending&.stop
          end
        end
      end
    end
  end

  it "forwards a closer's interruption to the request task and holds the request until completion over http1" do
    reactor do |task|
      entered = Async::Queue.new
      interrupted = Async::Queue.new
      release = Async::Queue.new
      registry = Async::Utilization::Registry.new
      calls = 0
      finished = []
      body = nil
      called = nil
      request_task = nil
      rack = lambda do |env|
        next [200, { "content-type" => "text/plain" }, ["other"]] unless env["PATH_INFO"] == "/held"

        called = Fiber.current
        request_task = Async::Task.current
        env["rack.response_finished"] << lambda do |*|
          calls += 1
          Async::Task.current.defer_cancel do
            entered.enqueue(true)
            Async::Condition.new.wait
          end
        ensure
          # Cleanup after the forwarded cancellation holds completion until
          # released.
          interrupted.enqueue($ERROR_INFO.class)
          release.dequeue
          finished << :callback
        end
        body = FalconGatedBody.new
        [200, { "content-type" => "text/plain" }, body]
      end
      falcon(rack_app: rack, utilization_registry: registry, max_requests: 1) do |host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP1.client(raw)
        response = connection.call(Protocol::HTTP::Request["GET", "/held", {}, nil, authority: "localhost"])
        expect(response.read).to eq("firstsecond")
        entered.dequeue
        closer = body.readers.last.async_task
        # The connection defers its first cancel until the response is done;
        # the second is forced through that deferral and interrupts its wait.
        # The closer relays it with that force, so the request task gets it
        # even inside the callback's deferral, as that second cancel reached a
        # callback that ran on the connection before the request task existed.
        closer.cancel
        closer.cancel
        expect(task.with_timeout(2) { interrupted.dequeue }).to eq(Async::Cancel)
        # The closer keeps waiting, so the request keeps its metric and its
        # admission until completion settles.
        expect(closer).not_to be_finished
        expect(registry.metric(:requests_active).value).to eq(1)
        expect(http1(origin, "/other", body: nil).first).to eq(503)
        release.enqueue(true)
        task.with_timeout(2) { closer.wait }
        # The forwarded cancel ended the request task after the callback's
        # cleanup finished.
        expect(request_task).to be_cancelled
        expect(finished).to eq([:callback])
        expect(calls).to eq(1)
        expect(body.closes.map(&:first)).to eq([called])
        wait_observed(task, "the request's release", lambda(&:zero?)) { registry.metric(:requests_active).value }
        expect(http1(origin, "/other", body: nil).values_at(0, 2)).to eq([200, "other"])
        expect(host.close(grace: 1)).to be(true)
      ensure
        release&.enqueue(true)
        AsyncHelpers::INTERNAL.raw_close(raw)
        connection&.close
      end
    end
  end

  # Before the request task the callback ran on the HTTP/1.1 connection writer
  # or the HTTP/2 stream's output task, which retirement cancels and then
  # forces if it deferred. Now that task is the closer and relays both with
  # the force they had. Retirement also cancels the session's handler task,
  # whose cancellation cascades to the request task directly.
  [%i[http1 close], %i[http1 abort], %i[h2 close], %i[h2 abort]].each do |protocol, stop|
    it "bounds #{stop == :close ? 'forced retirement' : 'abort'} while a completion callback defers " \
       "cancellation over #{protocol}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        requests = Async::Queue.new
        body = nil
        called = nil
        rack = lambda do |env|
          requests.enqueue(Async::Task.current)
          called = Fiber.current
          env["rack.response_finished"] << lambda { |*|
            Async::Task.current.defer_cancel do
              entered.enqueue(true)
              release.dequeue
            end
          }
          body = FalconGatedBody.new
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack) do |host, origin|
          raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
          client = protocol == :h2 ? Async::HTTP::Protocol::HTTP2 : Async::HTTP::Protocol::HTTP1
          connection = client.client(raw)
          request = Protocol::HTTP::Request["GET", "/held", {}, nil, scheme: "http", authority: "localhost"]
          response = connection.call(request)
          if protocol == :h2
            # The stream ends only after completion, so read up to it.
            expect(response.body.read).to eq("first")
          else
            expect(response.read).to eq("firstsecond")
          end
          request_task = requests.dequeue
          entered.dequeue
          # The callback holds completion; the original closes after it.
          expect(body.closes).to be_empty
          stopped = task.with_timeout(2) { stop == :abort ? host.abort : host.close(grace: 0.03) }
          expect(stopped).to be(false) if stop == :close
          expect(task.with_timeout(2) { request_task.wait }).to be_nil
          expect(request_task).to be_cancelled
          expect(body.closes.map(&:first)).to eq([called])
        ensure
          release&.enqueue(true)
          AsyncHelpers::INTERNAL.raw_close(raw)
          connection&.close
        end
      end
    end
  end

  # The host sees the completion's deferral on the closer, as it did when the
  # closer ran the callback, and a second cancel forces both. Retirement does
  # this through force_deferred; here the spec plays the host.
  [[:h2, "GET", "output task"], [:http1, "HEAD", "handler"], [:h2, "HEAD", "handler"]].each do |protocol, method, kind|
    it "takes on a deferral of the #{kind} closer's cancellation until forced over #{protocol}" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        requests = Async::Queue.new
        events = []
        body = nil
        called = nil
        rack = lambda do |env|
          requests.enqueue(Async::Task.current)
          called = Fiber.current
          env["rack.response_finished"] << lambda do |*|
            Async::Task.current.defer_cancel do
              entered.enqueue(true)
              release.dequeue
              events << :deferred_section
            end
          rescue Async::Cancel
            events << :cancelled
            raise
          end
          body = FalconGatedBody.new
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack) do |_host, origin|
          pending = task.async do
            fallback(origin, protocol, method: method)
          rescue StandardError => e
            e
          end
          request_task = requests.dequeue
          entered.dequeue
          closer = method == "HEAD" ? request_task.parent : body.readers.last.async_task
          closer.cancel
          wait_observed(task, "the relayed cancellation", ->(seen) { seen != [false, []] }) do
            [request_task.cancel_deferred?, events.dup]
          end
          expect(events).to be_empty
          expect(closer.cancel_deferred?).to be(true)
          closer.cancel
          expect(task.with_timeout(2) { request_task.wait }).to be_nil
          expect(events).to eq([:cancelled])
          expect(task.with_timeout(2) { closer.wait }).to be_nil
          expect(closer).to be_cancelled
          expect(body.closes.map(&:first)).to eq([called])
        ensure
          release&.enqueue(true)
          pending&.stop
        end
      end
    end
  end

  # The forced delivery can reach a request task that already holds a pending
  # cancellation, relayed earlier or cascaded from the handler. A callback that
  # rescues it inside its deferral keeps that cancellation pending and cleans
  # up before the deferral exits. Nothing else interrupts that cleanup, and the
  # host waits for it, as when the closer ran the callback.
  [%w[HEAD handler], %w[GET transport]].product(%i[http1 h2], %i[close abort]).each do |(method, kind), protocol, stop|
    it "waits for a deferring callback's cleanup after its forced cancellation on the #{kind} closer " \
       "under #{stop} over #{protocol}" do
      reactor do |task|
        entered = Async::Queue.new
        cleaning = Async::Queue.new
        release = Async::Queue.new
        events = []
        registry = Async::Utilization::Registry.new
        rack = lambda do |env|
          env["rack.response_finished"] << lambda do |*|
            Async::Task.current.defer_cancel do
              entered.enqueue(true)
              Async::Condition.new.wait
            rescue Async::Cancel => e
              events << [:rescued, e.class]
              cleaning.enqueue(true)
              release.dequeue
              events << :cleaned_up
            end
          rescue Async::Cancel => e
            events << [:cancelled, e.class]
            raise
          end
          body = ["body"]
          body.define_singleton_method(:close) { events << :closed }
          [200, { "content-type" => "text/plain" }, body]
        end
        falcon(rack_app: rack, utilization_registry: registry) do |host, origin|
          pending = task.async do
            fallback(origin, protocol, method: method)
          rescue StandardError => e
            e
          end
          entered.dequeue
          stopper = task.async do
            stop == :abort ? host.abort : host.close(grace: 0.03)
            events << :stopped
          end
          cleaning.dequeue
          # The forced delivery has run; the cleanup still holds the request.
          expect(events).to eq([[:rescued, Async::Cancel]])
          expect(registry.metric(:requests_active).value).to eq(1)
          release.enqueue(true)
          task.with_timeout(2) { stopper.wait }
          expect(events).to eq([[:rescued, Async::Cancel], :cleaned_up, [:cancelled, Async::Cancel], :closed, :stopped])
          expect(registry.metric(:requests_active).value).to eq(0)
        ensure
          release&.enqueue(true)
          pending&.stop
        end
      end
    end
  end

  # A peer reset cancels the stream's output task, the closer here, once.
  # Before the request task the callback ran on that task, so the reset met
  # the callback's own deferral there.
  describe "when the peer resets an HTTP/2 stream during completion" do
    def held(called, closes)
      called << Fiber.current
      body = ["held"]
      body.define_singleton_method(:close) { closes << Fiber.current }
      [200, { "content-type" => "text/plain" }, body]
    end

    def reset_during_completion(rack)
      falcon(rack_app: rack) do |host, origin|
        raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
        connection = Async::HTTP::Protocol::HTTP2.client(raw)
        request = ->(path) { Protocol::HTTP::Request["GET", path, {}, nil, scheme: "http", authority: "localhost"] }
        response = connection.call(request.call("/held"))
        expect(response.body.read).to eq("held")
        yield host, -> { response.stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL) }, lambda {
          other = connection.call(request.call("/other"))
          [other.status, other.read]
        }
      ensure
        AsyncHelpers::INTERNAL.raw_close(raw) if raw
        connection&.close
      end
    end

    it "lets a deferring callback finish its deferred section, then cancels it" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        requests = Async::Queue.new
        events = []
        called = []
        closes = []
        rack = lambda do |env|
          next [200, { "content-type" => "text/plain" }, ["other"]] unless env["PATH_INFO"] == "/held"

          requests.enqueue(Async::Task.current)
          env["rack.response_finished"] << lambda do |*|
            Async::Task.current.defer_cancel do
              entered.enqueue(true)
              release.dequeue
              events << :deferred_section
            end
            events << :after_deferral
          rescue Async::Cancel
            events << :cancelled
            raise
          end
          held(called, closes)
        end
        reset_during_completion(rack) do |host, reset, other|
          request_task = requests.dequeue
          entered.dequeue
          reset.call
          wait_observed(task, "the reset to reach the request task", ->(seen) { seen != [false, []] }) do
            [request_task.cancel_deferred?, events.dup]
          end
          expect(events).to be_empty
          # Sibling streams still progress while the callback holds completion.
          expect(other.call).to eq([200, "other"])
          release.enqueue(true)
          task.with_timeout(2) { request_task.wait }
          expect(events).to eq(%i[deferred_section cancelled])
          expect(request_task).to be_cancelled
          expect(closes).to eq(called)
          expect(host.close(grace: 1)).to be(true)
        ensure
          release&.enqueue(true)
        end
      end
    end

    it "cancels a callback that does not defer once" do
      reactor do |task|
        entered = Async::Queue.new
        release = Async::Queue.new
        requests = Async::Queue.new
        events = []
        called = []
        closes = []
        rack = lambda do |env|
          next [200, { "content-type" => "text/plain" }, ["other"]] unless env["PATH_INFO"] == "/held"

          requests.enqueue(Async::Task.current)
          env["rack.response_finished"] << lambda do |*|
            begin
              entered.enqueue(true)
              Async::Condition.new.wait
            rescue Async::Cancel
              events << :cancelled
            end
            release.dequeue
            events << :released
          rescue Async::Cancel
            events << :cancelled_again
            raise
          end
          held(called, closes)
        end
        reset_during_completion(rack) do |host, reset, other|
          request_task = requests.dequeue
          entered.dequeue
          reset.call
          wait_observed(task, "the reset to reach the callback", ->(seen) { !seen.empty? }) { events.dup }
          # The closer relayed the reset before this connection could serve
          # another stream, so a second cancellation would have arrived by now.
          expect(other.call).to eq([200, "other"])
          expect(events).to eq([:cancelled])
          release.enqueue(true)
          task.with_timeout(2) { request_task.wait }
          expect(events).to eq(%i[cancelled released])
          expect(closes).to eq(called)
          expect(host.close(grace: 1)).to be(true)
        ensure
          release&.enqueue(true)
        end
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
      registry = Async::Utilization::Registry.new
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
      falcon(rack_app: rack, cache: true, utilization_registry: registry) do |_host, origin|
        headers = { "accept-encoding" => "identity" }
        expect(http1(origin, "/empty", body: nil, headers: headers).values_at(0, 2)).to eq([200, nil])
        entered.dequeue
        # The hit's shallow copy shares the miss's completion, which is
        # still running, so its close neither waits nor runs it again.
        status, hit_headers, = http1(origin, "/empty", body: nil, headers: headers)
        expect([status, hit_headers["x-cache"]]).to eq([200, ["hit"]])
        # The hit's request is released once its own close returns, which the
        # client does not wait for; only the held miss remains.
        wait_observed(task, "the hit's release", ->(active) { active == 1 }) do
          registry.metric(:requests_active).value
        end
        expect(calls).to eq(1)
        release.enqueue(true)
        wait_observed(task, "the miss's original close", ->(seen) { seen >= 1 }) { closes }
        expect(calls).to eq(1)
        expect(closes).to eq(1)
      end
    end
  end

  # A direct call from a plain fiber on the host's reactor has no Async task to
  # start a request task from, so the app runs on that fiber and its
  # completion runs on whichever fiber closes the body.
  it "serves a direct call from a fiber without an Async task" do
    reactor do |task|
      fibers = []
      results = []
      rack = lambda do |env|
        fibers << Fiber.current
        env["rack.response_finished"] << ->(*) { fibers << Fiber.current }
        [200, { "content-type" => "text/plain" }, ["rack"]]
      end
      falcon(rack_app: rack) do |host, _origin|
        caller = Fiber.new(blocking: false) do
          results << Async::Task.current?
          %w[GET CONNECT].each do |method|
            request = Protocol::HTTP::Request[method, "/rack", {}, nil, scheme: "http", authority: "localhost"]
            response = host.call(request)
            results << [response.status, response.read]
          rescue StandardError => e
            results << e
          end
        end
        caller.resume
        wait_observed(task, "the direct calls", lambda(&:!)) { caller.alive? }
        expect(results).to eq([nil, [200, "rack"], [501, nil]])
        expect(fibers).to eq([caller, caller])
      end
    end
  end

  # Protocol-rack's adapter, called on the current task, stands in for the
  # session's handler task.
  describe "behind protocol-rack's adapter" do
    def adapted(app)
      guard = ConnectRPC::Falcon.const_get(:RackGuard).new(app)
      response = Protocol::Rack::Adapter.new(guard).call(
        Protocol::HTTP::Request["GET", "/", {}, nil, scheme: "http", authority: "localhost"]
      )
      [guard, response]
    end

    it "raises a cancellation that the original's close raised to the closer" do
      reactor do
        events = []
        app = lambda do |env|
          called = Fiber.current
          env["rack.response_finished"] << ->(*) { events << [:callback, Fiber.current.equal?(called)] }
          body = ["x"]
          body.define_singleton_method(:close) do
            events << [:close, Fiber.current.equal?(called)]
            raise Async::Stop, "close escaped"
          end
          [200, {}, body]
        end
        guard, response = adapted(app)
        guard.settle(nil)
        expect { response.close }.to raise_error(Async::Stop, "close escaped")
        expect(events).to eq([[:callback, true], [:close, true]])
      end
    end

    it "keeps the error that abandoned a produced response when its close also fails" do
      reactor do
        finished = []
        app = lambda do |env|
          env["rack.response_finished"] << ->(_env, _status, _headers, error) { finished << error }
          body = ["x"]
          body.define_singleton_method(:close) { raise "close failed" }
          [200, {}, body]
        end
        guard, = adapted(app)
        abandoned = RuntimeError.new("abandoned")
        expect { guard.settle(abandoned) }.not_to raise_error
        expect(finished).to eq([abandoned])
      end
    end
  end
end
