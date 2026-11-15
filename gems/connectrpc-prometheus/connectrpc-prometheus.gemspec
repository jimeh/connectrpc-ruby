# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-prometheus"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/prometheus/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Optional Prometheus metrics for ConnectRPC servers and clients"
  spec.description = "go-grpc-prometheus compatible RPC and connection metrics in a prometheus-client registry. " \
                     "Experimental."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = %w[LICENSE README.md lib/connectrpc/prometheus.rb lib/connectrpc/prometheus/metrics.rb
                  lib/connectrpc/prometheus/version.rb] +
               Dir.glob("sig/**/*.{rbs,yaml}")
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
  spec.add_dependency "prometheus-client", ">= 5.0", "< 6"
end
