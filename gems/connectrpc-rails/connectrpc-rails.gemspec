# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-rails"
  spec.version = "0.1.0"
  spec.authors = ["jimeh"]
  spec.summary = "Native Rails controller execution for Connect unary RPCs"
  spec.description = "Optional, experimental Rails lifecycle integration for the unofficial ConnectRPC Ruby gem."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "Nonstandard"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.chdir(__dir__) { Dir.glob("lib/**/*.rb") + %w[README.md] }
  spec.require_paths = ["lib"]
  spec.add_dependency "actionpack", ">= 8.0.5.1", "< 8.2"
  spec.add_dependency "connectrpc", "~> 0.1.0"
  spec.add_dependency "rack", ">= 3.2.7", "< 4"
end
