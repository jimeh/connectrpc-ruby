# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-health"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/health/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Optional standard gRPC health service (grpc.health.v1) for ConnectRPC servers"
  spec.description = "Check, List and Watch for grpc.health.v1 with a thread- and fiber-safe status registry. " \
                     "Experimental."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  # Original work is MIT; the vendored grpc-proto schema and its generated code are Apache-2.0.
  spec.licenses = ["MIT", "Apache-2.0"]
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.files = %w[LICENSE README.md lib/connectrpc/health.rb lib/connectrpc/health/registry.rb
                  lib/connectrpc/health/version.rb lib/connectrpc/health/watcher.rb
                  lib/grpc/health/v1/health_connect.rb
                  lib/grpc/health/v1/health_pb.rb proto/LICENSE proto/README.md
                  proto/grpc/health/v1/health.proto] +
               Dir.glob("sig/**/*.{rbs,yaml}", base: __dir__) + Dir.glob("rbs/**/*.rbs", base: __dir__)
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
  spec.add_dependency "google-protobuf", ">= 4.36.2", "< 5"
end
