# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-otel"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/otel/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "OpenTelemetry tracing for ConnectRPC clients and servers"
  spec.description = "Emits one OpenTelemetry CLIENT or SERVER span per Connect, native gRPC or gRPC-Web RPC " \
                     "through the connectrpc instrumentation lifecycle. The application owns the SDK, sampling and " \
                     "exporters. Part of the unofficial connectrpc-ruby project. Experimental and not production " \
                     "ready."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = %w[LICENSE README.md lib/connectrpc/otel.rb lib/connectrpc/otel/subscriber.rb
                  lib/connectrpc/otel/version.rb] +
               Dir.glob("sig/**/*.{rbs,yaml}")
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
  spec.add_dependency "opentelemetry-api", ">= 1.11.1", "< 2"
end
