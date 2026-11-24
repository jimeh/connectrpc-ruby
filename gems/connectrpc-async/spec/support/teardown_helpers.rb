# frozen_string_literal: true

module TeardownHelpers
  # The fixed warning each reported teardown failure writes, in order.
  def teardown_warnings(*classes) = classes.map { |error| "ConnectRPC teardown error: #{error}\n" }.join
end
