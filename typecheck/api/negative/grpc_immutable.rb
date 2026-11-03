# frozen_string_literal: true

ConnectRPC::GRPCHead.new(status: 200, headers: {}, end_stream: true).status = 500
