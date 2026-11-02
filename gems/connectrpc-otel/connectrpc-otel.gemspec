# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-otel"
  spec.version = "0.1.0"
  spec.authors = ["jimeh"]
  spec.summary = "Optional OpenTelemetry traces for ConnectRPC unary calls"
  spec.description = "Application-owned OpenTelemetry tracing through the ConnectRPC lifecycle. Experimental."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://gems.invalid"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = %w[LICENSE README.md lib/connectrpc/otel.rb lib/connectrpc/otel/subscriber.rb]
  spec.require_paths = ["lib"]
  spec.add_dependency "connectrpc", "~> 0.1.0"
  spec.add_dependency "opentelemetry-api", ">= 1.11.1", "< 2"
end
