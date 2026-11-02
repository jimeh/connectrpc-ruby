# frozen_string_literal: true

require "connectrpc"

ConnectRPC::Context.new.record_failure("not an exception")
