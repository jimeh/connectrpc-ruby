# frozen_string_literal: true

# Observe the real server-side TLS verifier without replacing its behavior.
class TLSObservedHost < ConnectRPC::Async::Host
  attr_reader :setup_errors

  def initialize(**)
    @setup_errors = []
    super
  end

  private

  def setup_connection(resource)
    super
  rescue OpenSSL::SSL::SSLError => e
    @setup_errors << e.message
    raise
  end
end
