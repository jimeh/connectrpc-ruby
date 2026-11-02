# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Private, experimental Ruby Connect RPC project skeleton"
  spec.description = "Unofficial bootstrap only. No RPC implementation or production-ready API is provided."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "Nonstandard"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.glob("lib/**/*.rb") + %w[README.md docs/development.md docs/design.md]
  spec.require_paths = ["lib"]
end
