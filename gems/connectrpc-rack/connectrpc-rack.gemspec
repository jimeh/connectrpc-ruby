# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-rack"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/rack/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Rack middleware for Connect unary RPCs"
  spec.description = "Optional, experimental Rack integration for the unofficial ConnectRPC Ruby gem."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.chdir(__dir__) { Dir.glob("sig/**/*.{rbs,yaml}") + Dir.glob("lib/**/*.rb") + %w[README.md LICENSE] }
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
  spec.add_dependency "rack", ">= 3.2.7", "< 4"
end
