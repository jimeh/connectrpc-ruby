# frozen_string_literal: true

require "rbconfig"

abort "Unexpected cache Ruby" unless ENV.fetch("CI_EXPECTED_RUBY_VERSION") == RUBY_VERSION

parts = [RUBY_VERSION, RbConfig::CONFIG.fetch("ruby_version"), Gem::Platform.local.to_s,
         ENV.fetch("ImageOS"), ENV.fetch("ImageVersion")]
abort "Invalid cache platform component" unless parts.all? { |value| value.match?(/\A[A-Za-z0-9_.-]+\z/) }

platform = parts.join("-")
puts "RBS native bundle cache platform: #{platform}"
File.open(ENV.fetch("GITHUB_OUTPUT"), "a") { |output| output.puts("platform=#{platform}") }
