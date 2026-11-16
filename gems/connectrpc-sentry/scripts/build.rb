# frozen_string_literal: true

require_relative "../../../scripts/package_build"
root = File.expand_path("../../..", __dir__)
output = File.join(root, "vendor/sentry/pkg")
[root, *%w[testing sentry].map { |name| File.join(root, "gems/connectrpc-#{name}") }].each do |directory|
  PackageBuild.build(directory: directory, output: output)
end
