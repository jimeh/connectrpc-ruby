# frozen_string_literal: true

require "rake"
require "rubygems/package"
require "rubocop/rake_task"
require "rspec/core/rake_task"

specification = Gem::Specification.load("connectrpc.gemspec")
desc "Build the experimental gem from the current exact manifest"
task :build do
  mkdir_p "pkg"
  Gem::Package.build(specification, false, false, File.join("pkg", specification.file_name))
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
                   "gems/connectrpc-rails/lib/**/*.rb", "gems/connectrpc-rails/spec/**/*.rb",
                   "gems/connectrpc-rails/*.gemspec", "gems/connectrpc-rails/Gemfile",
                   "gems/connectrpc-rails/Appraisals",
                   "gems/connectrpc-async/lib/**/*.rb", "gems/connectrpc-async/spec/**/*.rb",
                   "gems/connectrpc-async/scripts/**/*.rb", "gems/connectrpc-async/*.gemspec",
                   "gems/connectrpc-async/Gemfile",
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
