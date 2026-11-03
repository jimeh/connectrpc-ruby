# frozen_string_literal: true

ConnectRPC::Async::TLSConfiguration.new(on_error: BadAPIReporter.new)
