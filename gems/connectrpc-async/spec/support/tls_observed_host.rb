# frozen_string_literal: true

# Observe the real server-side TLS verifier without replacing its behavior.
class TLSObservedHost < ConnectRPC::Async::Host
  class Session < ConnectRPC::Async::ServerSession
    def initialize(errors:, **)
      @errors = errors
      super(**)
    end

    private

    def setup_connection(resource)
      super
    rescue OpenSSL::SSL::SSLError => e
      @errors << e.message
      raise
    end
  end

  attr_reader :setup_errors

  def initialize(**)
    @setup_errors = []
    super
  end

  private

  def build_session(**)
    Session.new(errors: @setup_errors, **)
  end
end
