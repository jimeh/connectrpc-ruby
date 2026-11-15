# frozen_string_literal: true

definitions = ConnectRPC::Metrics::Definitions.new
ConnectRPC::Metrics::Subscriber.new(recorder: BadAPIRecorder.new, definitions: definitions)
