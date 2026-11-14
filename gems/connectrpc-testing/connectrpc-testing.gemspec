# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-testing"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/testing/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "In-process transport for real ConnectRPC unary round trips"
  spec.description = "Optional synchronous testing transport using an application-owned core Server."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.glob("sig/**/*.{rbs,yaml}") + Dir.glob("lib/**/*.rb") + %w[README.md LICENSE]
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
end
