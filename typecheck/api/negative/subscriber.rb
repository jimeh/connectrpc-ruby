# frozen_string_literal: true

ConnectRPC::Instrumentation.new(subscribers: [BadAPISubscriber.new])
