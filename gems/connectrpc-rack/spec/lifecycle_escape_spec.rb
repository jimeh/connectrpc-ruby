# frozen_string_literal: true

require "connectrpc/rack"
require "rack/mock"
require_relative "../../../examples/echo_schema"

RSpec.describe "Rack lifecycle ownership on nonlocal exits" do
  let(:processing) { [] }
  let(:completions) { [] }
  let(:bus) do
    observer = Object.new
    outcomes = processing
    finished = completions
    observer.define_singleton_method(:start) { |_| observer }
    observer.define_singleton_method(:processing_finished) { |value| outcomes << value }
    observer.define_singleton_method(:finished) { |value| finished << value }
    ConnectRPC::Instrumentation.new(subscribers: [observer])
  end

  def middleware(&handler)
    target = ConnectRPC::Server.new(services: [EchoExample::SERVICE], binding: EchoExample::BINDING,
                                    instrumentation: bus, handlers: { EchoExample::ECHO.path => handler })
    ConnectRPC::Rack.new(server: target)
  end

  def environment(callbacks: false)
    env = Rack::MockRequest.env_for(EchoExample::ECHO.path, method: "POST", "CONTENT_TYPE" => "application/proto",
                                                            input: EchoExample::Message.new(name: "hello").to_proto)
    env["rack.response_finished"] = [] if callbacks
    env
  end

  [false, true].each do |callbacks|
    it "completes a handler throw exactly once before response ownership transfers, callbacks=#{callbacks}" do
      value = Object.new
      app = middleware { throw :escape, value }
      env = environment(callbacks: callbacks)
      result = begin
        raise "ambient error"
      rescue RuntimeError
        catch(:escape) { app.call(env) }
      end
      expect(result).to equal(value)
      expect(completions).to contain_exactly(have_attributes(delivery: :aborted, error: nil))
      expect(processing).to contain_exactly(have_attributes(code: nil, error: nil))
      expect(env["rack.response_finished"]).to be_empty if callbacks
    end

    it "keeps normal completion at the returned owner boundary, callbacks=#{callbacks}" do
      app = middleware { |request, _| request }
      env = environment(callbacks: callbacks)
      status, headers, body = app.call(env)
      expect(processing).to contain_exactly(have_attributes(code: "OK", error: nil))
      expect(completions).to be_empty
      body.each { |_| nil }
      expect(completions).to be_empty
      if callbacks
        body.close if body.respond_to?(:close)
        expect(completions).to be_empty
        2.times { env["rack.response_finished"].each { |callback| callback.call(env, status, headers, nil) } }
      else
        2.times { body.close }
      end
      expect(completions).to contain_exactly(have_attributes(delivery: callbacks ? :response_finished : :body_closed))
    end

    it "disposes an untransferred response on assembly throw while preserving host callbacks, callbacks=#{callbacks}" do
      app = middleware { |request, _| request }
      env = environment(callbacks: callbacks)
      value = Object.new
      returned = nil
      result = catch(:escape) do
        app.send(:transfer_response, env) do |response, _|
          returned = response
          expect(completions).to be_empty
          throw :escape, value
        end
      end
      expect(result).to equal(value)
      expect(processing).to contain_exactly(have_attributes(code: "OK", error: nil))
      status, headers, body = returned
      if callbacks
        expect(completions).to be_empty
        env["rack.response_finished"].each { |callback| callback.call(env, status, headers, nil) }
      else
        expect(completions).to contain_exactly(have_attributes(delivery: :aborted, error: nil))
        body.close
      end
      expect(completions.size).to eq(1)
    end
  end

  it "preserves an original fatal exception and completes it once" do
    original = Class.new(Exception).new("fatal") # rubocop:disable Lint/InheritException -- fatal Rack escape
    app = middleware { raise original }
    expect { app.call(environment) }.to(raise_error { |error| expect(error).to equal(original) })
    expect(completions).to contain_exactly(have_attributes(delivery: :aborted, error: original))
    expect(processing).to contain_exactly(have_attributes(error: original))
  end
end
