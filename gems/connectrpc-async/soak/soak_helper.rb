# frozen_string_literal: true

# Loaded through soak/.rspec instead of spec/spec_helper: no SimpleCov.
require "connectrpc/async"
require "connectrpc/bindings/google_protobuf"
$LOAD_PATH.unshift(File.expand_path("../../../spec/fixtures/generation/echo", __dir__))
require "echo_connect"
require_relative "../spec/support/helpers"
require_relative "../spec/support/frame_gate"
require_relative "../spec/support/connection_peer"
require_relative "../spec/support/threaded_helpers"
require_relative "../spec/support/keepalive_client"
require_relative "support/harness"
require_relative "support/rspec"

Soak.package ||= "async"
Soak::RSpecSetup.configure
