# frozen_string_literal: true

require "rake"
require "rubygems/package_task"
require "rubocop/rake_task"
require "rspec/core/rake_task"

specification = Gem::Specification.load("connectrpc.gemspec")
Gem::PackageTask.new(specification).define
file File.join("pkg", specification.file_name) => "connectrpc.gemspec"

desc "Build the private gem"
task build: :gem

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
  files = FileList["lib/**/*.rb", "spec/**/*.rb", "scripts/**/*.rb", "*.gemspec", "Gemfile", "Rakefile"]
  files.each { |file| ruby "-c", file }
end

namespace :check do
  desc "Verify the built gem in a clean environment and separate working directory"
  task :package do
    ruby "scripts/check_package.rb"
  end
end
