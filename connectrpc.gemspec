# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Private, experimental Connect unary RPC for Ruby"
  spec.description = "Unofficial typed Connect unary clients and Rack middleware. Experimental, not production ready."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "Nonstandard"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.glob("lib/**/*.rb") + %w[README.md docs/development.md docs/design.md]
  spec.require_paths = ["lib"]
end
