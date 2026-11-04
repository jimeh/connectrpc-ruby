# frozen_string_literal: true

require "rake"
require_relative "scripts/package_build"
require "rubocop/rake_task"
require "rspec/core/rake_task"

desc "Build the experimental gem from the current exact manifest"
task :build do
  PackageBuild.build(directory: __dir__, output: File.join(__dir__, "pkg"))
end

RuboCop::RakeTask.new(:format) do |task|
  task.options = ["--only", "Layout", "--autocorrect"]
end
RuboCop::RakeTask.new("format:check") do |task|
  task.options = ["--only", "Layout"]
end
RuboCop::RakeTask.new(:lint) do |task|
  task.options = ["--except", "Layout"]
end
RSpec::Core::RakeTask.new(:test)

desc "Parse every maintained Ruby source, spec, script, gemspec, and Rakefile"
task :syntax do
  files = FileList["lib/**/*.rb", "spec/**/*.rb", "scripts/**/*.rb", "examples/**/*.rb", "conformance/**/*.rb",
                   "browser/**/*.rb",
                   "gems/connectrpc-rails/lib/**/*.rb", "gems/connectrpc-rails/spec/**/*.rb",
                   "gems/connectrpc-rails/*.gemspec", "gems/connectrpc-rails/Gemfile",
                   "gems/connectrpc-rails/Appraisals",
                   "gems/connectrpc-async/lib/**/*.rb", "gems/connectrpc-async/spec/**/*.rb",
                   "gems/connectrpc-async/scripts/**/*.rb", "gems/connectrpc-async/*.gemspec",
                   "gems/connectrpc-async/Gemfile",
                   "gems/connectrpc-rack/lib/**/*.rb", "gems/connectrpc-rack/spec/**/*.rb",
                   "gems/connectrpc-rack/scripts/**/*.rb", "gems/connectrpc-rack/*.gemspec",
                   "gems/connectrpc-rack/Gemfile",
                   "typecheck/**/*.rb", "typecheck/Gemfile",
                   "gems/connectrpc-falcon/lib/**/*.rb", "gems/connectrpc-falcon/spec/**/*.rb",
                   "gems/connectrpc-falcon/scripts/**/*.rb", "gems/connectrpc-falcon/examples/**/*.rb",
                   "gems/connectrpc-falcon/*.gemspec", "gems/connectrpc-falcon/Gemfile",
                   "gems/connectrpc-otel/{lib,spec,scripts}/**/*.rb",
                   "gems/connectrpc-otel/*.gemspec", "gems/connectrpc-otel/Gemfile",
                   "exe/*", "*.gemspec", "Gemfile",
                   "Rakefile"]
  files.each { |file| ruby "-c", file }
end

namespace :check do
  desc "Verify the built gem in a clean environment and separate working directory"
  task :package do
    ruby "scripts/check_package.rb"
  end
end
