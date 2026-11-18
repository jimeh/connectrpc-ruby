# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-hyper"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/hyper/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Optional reactor-free ConnectRPC client transport on Rust hyper, h2 and rustls"
  spec.description = "Connect and native gRPC calls of every shape from any Ruby thread, over one shared HTTP/2 " \
                     "connection per origin, without an Async reactor. Interrupts, deadlines and cancellation " \
                     "stop a call at any stage. Experimental."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.files = %w[LICENSE README.md THIRD_PARTY_NOTICES.md Cargo.toml Cargo.lock
                  ext/connectrpc_hyper/Cargo.toml ext/connectrpc_hyper/extconf.rb
                  lib/connectrpc/hyper.rb lib/connectrpc/hyper/errors.rb lib/connectrpc/hyper/exchange.rb
                  lib/connectrpc/hyper/tls.rb lib/connectrpc/hyper/transport.rb
                  lib/connectrpc/hyper/version.rb] +
               Dir.glob("ext/connectrpc_hyper/src/*.rs", base: __dir__) +
               Dir.glob("sig/**/*.{rbs,yaml}", base: __dir__)
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/connectrpc_hyper/extconf.rb"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
  # Source gem only: builds the extension with the installer's cargo. Platform
  # gems built by RbSys::ExtensionTask drop it.
  spec.add_dependency "rb_sys", ">= 0.9.130", "< 1"
end
