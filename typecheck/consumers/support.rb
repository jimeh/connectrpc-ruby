# frozen_string_literal: true

require "stringio"

class LoopbackTransport
  def initialize(server)
    @server = server
  end

  def call(request)
    @server.call(path: request.url.path || "/", http_method: request.http_method, query: request.url.query || "",
                 headers: request.headers, input: StringIO.new(request.body))
  end
end

class GoodHandler
  def call(request, context)
    context.check!
    RbsFixture::Messages::Envelope::Reply.new(count: request.text.size)
  end
end

class BadHandler
  def call(request, _context)
    request
  end
end

class LifecycleRecorder
  attr_reader :completions

  def initialize
    @completions = []
  end

  def start(_call) = self
  def finished(completion) = @completions << completion
end
