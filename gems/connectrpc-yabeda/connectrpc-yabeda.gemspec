# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-yabeda"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/yabeda/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Optional Yabeda metrics for ConnectRPC servers and clients"
  spec.description = "ConnectRPC RPC and connection metrics declared through Yabeda for any Yabeda adapter. " \
                     "Experimental."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.files = %w[LICENSE README.md lib/connectrpc/yabeda.rb lib/connectrpc/yabeda/metrics.rb
                  lib/connectrpc/yabeda/version.rb] +
               Dir.glob("sig/**/*.{rbs,yaml}", base: __dir__)
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "connectrpc", local_requirement
  spec.add_dependency "yabeda", ">= 0.16", "< 1"
end
