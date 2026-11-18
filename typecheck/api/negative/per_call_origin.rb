# frozen_string_literal: true

# Every call dials its request URL's origin; there is no origin option.
ConnectRPC::Async::PerCallTransport.new(origin: "https://example.invalid")
