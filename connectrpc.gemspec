# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Unofficial typed Connect and gRPC clients, servers and code generation for Ruby"
  spec.description = "Ruby implementation of the Connect protocol: typed clients, a Net::HTTP transport, host-" \
                     "neutral servers and a protoc plugin that generates service modules from protobuf. The same " \
                     "declarations serve native gRPC and server gRPC-Web through optional host gems. Core has no " \
                     "runtime dependencies. Unofficial, experimental and not production ready."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.files = Dir.glob("lib/**/*.rb", base: __dir__) + Dir.glob("exe/*", base: __dir__) +
               Dir.glob("sig/**/*.{rbs,yaml}", base: __dir__) + Dir.glob("rbs/**/*.rbs", base: __dir__) +
               %w[lib/connectrpc/bindings/descriptors/status.bin
                  third_party/google/rpc/status.proto third_party/google/LICENSE
                  third_party/protobuf-LICENSE third_party/README.md
                  docs/grpc.md docs/grpc-web.md LICENSE README.md
                  docs/development.md docs/design.md docs/rbs.md
                  docs/streaming.md docs/unary.md]
  spec.require_paths = ["lib"]
  spec.bindir = "exe"
  spec.executables = ["protoc-gen-connectrpc-ruby"]
end
