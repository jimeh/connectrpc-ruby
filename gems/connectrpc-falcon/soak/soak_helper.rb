# frozen_string_literal: true

# Loaded through soak/.rspec instead of spec/spec_helper: no SimpleCov.
require "connectrpc/falcon"
require "connectrpc/transports/net_http"
require "connectrpc/bindings/google_protobuf"
$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/echo", __dir__))
require "echo_connect"
async = File.expand_path("../../connectrpc-async", __dir__)
require_relative "#{async}/spec/support/helpers"
require_relative "#{async}/spec/support/frame_gate"
require_relative "#{async}/spec/support/connection_peer"
require_relative "#{async}/spec/support/threaded_helpers"
require_relative "#{async}/spec/support/keepalive_client"
require_relative "../spec/support/http1_wire"
require_relative "#{async}/soak/support/harness"
require_relative "#{async}/soak/support/rspec"

Soak.package = "falcon"
Soak::RSpecSetup.configure
