# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "connectrpc-on-rails"
  # Read without loading library code before SimpleCov starts.
  spec.version = File.read(File.join(__dir__, "lib/connectrpc/on_rails/version.rb"))[/VERSION = "([^"]+)"/, 1]
  spec.authors = ["jimeh"]
  spec.summary = "Run ConnectRPC unary handlers as native Rails controller actions"
  spec.description = "Routes Connect unary RPCs to Rails controllers, so typed handlers run through the real " \
                     "process_action chain with callbacks, rescue_from and Rails instrumentation. Part of the " \
                     "unofficial connectrpc-ruby project. Experimental and not production ready."
  spec.homepage = "https://github.com/jimeh/connectrpc-ruby"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.4"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["rubygems_mfa_required"] = "true"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.files = Dir.chdir(__dir__) do
    Dir.glob("sig/**/*.{rbs,yaml}") + Dir.glob("rbs/**/*.rbs") + Dir.glob("lib/**/*.rb") + %w[LICENSE README.md]
  end
  spec.require_paths = ["lib"]
  local_requirement = "~> #{spec.version.to_s.split('.').first(2).join('.')}.0"
  spec.add_dependency "actionpack", ">= 8.0.5.1", "< 8.2"
  spec.add_dependency "connectrpc", local_requirement
  spec.add_dependency "connectrpc-rack", local_requirement
end
