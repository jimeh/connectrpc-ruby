# frozen_string_literal: true

require "connectrpc/async"
require "falcon/server"
require "falcon/environment/server"
require "async/http/endpoint"
require "io/endpoint/wrapper"
require "delegate"

module ConnectRPC
  # Explicit optional listener. Falcon owns process supervision and accepts.
  module Falcon
  end
end

require_relative "falcon/endpoint"
require_relative "falcon/rack_guard"
require_relative "falcon/http1"
require_relative "falcon/server"
require_relative "falcon/environment"
