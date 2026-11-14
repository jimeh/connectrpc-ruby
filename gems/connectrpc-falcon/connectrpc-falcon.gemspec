# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-falcon"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/falcon/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.summary = "Optional Falcon worker hosting for ConnectRPC unary"
  spec.authors = ["jimeh"]
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.files = Dir.glob("sig/**/*.{rbs,yaml}") + Dir.glob("rbs/**/*.rbs") + Dir["lib/**/*.rb", "examples/*.rb",
                                                                                "README.md", "LICENSE"].sort
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  {
    "connectrpc-async" => local_requirement, "falcon" => "~> 0.57.0", "protocol-rack" => "~> 0.23.0",
    "async-service" => "~> 0.25.0", "async-container" => "~> 0.38.0", "async-utilization" => "~> 0.5.0",
    "protocol-http1" => "~> 0.41.1"
  }.each { |name, requirement| spec.add_dependency(name, requirement) }
end
