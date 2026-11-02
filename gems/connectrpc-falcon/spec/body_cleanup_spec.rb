# frozen_string_literal: true

RSpec.describe "Falcon Rack body cleanup" do
  def fallback_response(origin, protocol)
    raw = IO::Endpoint.tcp("127.0.0.1", URI(origin).port).connect
    implementation = protocol == :h2 ? Async::HTTP::Protocol::HTTP2 : Async::HTTP::Protocol::HTTP1
    connection = implementation.client(raw)
    response = connection.call(Protocol::HTTP::Request["GET", "/fallback", {}, nil, authority: "localhost"])
    [response.status, response.read.to_s]
  ensure
    AsyncHelpers::INTERNAL.raw_close(raw)
    connection&.close
  end

  %i[http1 h2].each do |protocol|
    %i[length array status headers].each do |failure|
      it "closes the original body and reports #{failure} response failure over #{protocol}" do
        reactor do
          closed = 0
          finished = []
          conversion_error = IOError.new("array conversion failed")
          rack = lambda do |env|
            env["rack.response_finished"] << ->(_env, _status, _headers, error) { finished << error }
            body = ["fallback-body"]
            body.define_singleton_method(:close) { closed += 1 }
            body.define_singleton_method(:to_ary) { raise conversion_error } if failure == :array
            headers = failure == :length ? { "content-length" => "invalid" } : {}
            headers = nil if failure == :headers
            [failure == :status ? "invalid" : 200, headers, body]
          end
          falcon(rack_app: rack) do |_host, origin|
            expect(fallback_response(origin, protocol).first).to eq(500)
            expect(closed).to eq(1)
            expect(finished.size).to eq(1)
            if failure == :array
              expect(finished.first).to equal(conversion_error)
            elsif failure == :headers
              expect(finished.first).to be_a(StandardError)
            else
              expect(finished.first).to be_a(ArgumentError)
            end
          end
        end
      end
    end

    it "closes array and array-like bodies without completion callbacks over #{protocol}" do
      reactor do
        closed = []
        requests = 0
        rack = lambda do |_env|
          requests += 1
          current = requests
          body = current == 1 ? ["fallback-body"] : Object.new
          if current == 2
            body.define_singleton_method(:each) { |&block| block.call("fallback-body") }
            body.define_singleton_method(:to_ary) { ["fallback-body"] }
          end
          body.define_singleton_method(:close) { closed << current }
          [200, { "cache-control" => "no-store" }, body]
        end
        falcon(rack_app: rack) do |_host, origin|
          2.times { expect(fallback_response(origin, protocol)).to eq([200, "fallback-body"]) }
          expect(closed).to eq([1, 2])
        end
      end
    end

    it "closes the original on cancellation inside a conversion-error callback over #{protocol}" do
      reactor do |task|
        entered = Async::Queue.new
        closed = 0
        finished = []
        registry = Async::Utilization::Registry.new
        rack = lambda do |env|
          env["rack.response_finished"] << lambda do |_env, _status, _headers, error|
            finished << error
            entered.enqueue(true)
            Async::Condition.new.wait
          end
          body = ["fallback-body"]
          body.define_singleton_method(:close) { closed += 1 }
          [200, { "content-length" => "invalid" }, body]
        end
        falcon(rack_app: rack, utilization_registry: registry) do |host, origin|
          pending = task.async do
            fallback_response(origin, protocol)
          rescue EOFError
            :closed
          end
          entered.dequeue
          expect(closed).to eq(0)
          expect(registry.metric(:requests_active).value).to eq(1)
          expect(host.close(grace: 0.03)).to be(false)
          expect(closed).to eq(1)
          expect(finished.size).to eq(1)
          expect(finished.first).to be_a(ArgumentError)
          expect(registry.metric(:requests_active).value).to eq(0)
          expect(pending.wait).to eq(:closed)
        ensure
          pending&.stop
        end
      end
    end
  end
end
