# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Experimental Connect unary RPC and service generation for Ruby"
  spec.description = "Unofficial typed Connect unary clients and host-neutral servers. " \
                     "Experimental, not production ready."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.glob("lib/**/*.rb") + Dir.glob("exe/*") + Dir.glob("sig/**/*.{rbs,yaml}") +
               Dir.glob("rbs/**/*.rbs") + %w[LICENSE README.md docs/development.md docs/design.md docs/rbs.md]
  spec.require_paths = ["lib"]
  spec.bindir = "exe"
  spec.executables = ["protoc-gen-connectrpc-ruby"]
end
