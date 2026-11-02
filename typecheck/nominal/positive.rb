# frozen_string_literal: true

request = RbsFixture::Messages::Envelope::Request.new
request.text = "application field"
handlers = RbsFixture::Api::Single.handlers(exchange: lambda { |input, context|
  context.check!
  input.text.size
  RbsFixture::Messages::Envelope::Reply.new
})
handlers.fetch(RbsFixture::Api::Single::EXCHANGE.path).call(request, ConnectRPC::Context.new)
