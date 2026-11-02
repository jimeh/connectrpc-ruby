# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-falcon"
  spec.version = "0.1.0"
  spec.summary = "Optional Falcon worker hosting for ConnectRPC unary"
  spec.authors = ["jimeh"]
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.files = Dir["lib/**/*.rb", "examples/*.rb", "README.md", "LICENSE"].sort
  spec.require_paths = ["lib"]
  {
    "connectrpc-async" => "~> 0.1.0", "falcon" => "~> 0.57.0", "protocol-rack" => "~> 0.23.0",
    "async-service" => "~> 0.25.0", "async-container" => "~> 0.38.0", "async-utilization" => "~> 0.5.0",
    "protocol-http1" => "~> 0.41.1"
  }.each { |name, requirement| spec.add_dependency(name, requirement) }
end
