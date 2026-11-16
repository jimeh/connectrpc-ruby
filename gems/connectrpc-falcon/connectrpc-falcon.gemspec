# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-falcon"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/falcon/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.summary = "Falcon worker hosting for ConnectRPC with Rack and Rails fallback"
  spec.description = "Serves connectrpc services from Falcon process workers over HTTP/1.1 and HTTP/2, next to an " \
                     "explicit Rack or Rails fallback app, with worker-local managed TLS and graceful drain. " \
                     "Supports Connect unary and streaming, native gRPC and server gRPC-Web. Part of the " \
                     "unofficial connectrpc-ruby project. Experimental and not production ready."
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
  # protocol-http1 stays patch-only (~> x.y.z) because the HTTP/1 listener couples to its private
  # internals (see issue #68). The rest use only public APIs, so they follow the floor policy: the
  # tested release as floor, capped below 1.
  {
    "connectrpc-async" => local_requirement, "falcon" => [">= 0.57.0", "< 1"], "protocol-rack" => [">= 0.23.0", "< 1"],
    "async-service" => [">= 0.25.0", "< 1"], "async-utilization" => [">= 0.5.0", "< 1"],
    "protocol-http1" => "~> 0.41.1"
  }.each { |name, requirement| spec.add_dependency(name, *requirement) }
end
