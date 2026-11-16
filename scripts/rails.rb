# frozen_string_literal: true

require "fileutils"
require "rbconfig"
require "English"
require_relative "dependency_floors"

root = File.expand_path("..", __dir__)
adapter = File.join(root, "gems/connectrpc-on-rails")
lane = ENV.fetch("RAILS_LANE", "8.1")
abort "Unknown RAILS_LANE #{lane.inspect}; use 8.0 or 8.1" unless %w[8.0 8.1].include?(lane)
floors = DependencyFloors.enabled?
# The gemspec's lowest allowed Action Pack release belongs to the 8.0 lane.
abort "The lowest-allowed Rails bundle uses RAILS_LANE=8.0" if floors && lane != "8.0"
# Check before Appraisal's legacy fallback can try installing Bundler globally.
abort "Bundler 4.0.23 is required" unless system(RbConfig.ruby, "-S", "gem", "list", "--silent", "-i", "bundler", "-v",
                                                 "4.0.23")
environment = {
  "BUNDLE_GEMFILE" => DependencyFloors.gemfile(File.join(adapter, "gemfiles/rails_#{lane}.gemfile")),
  "BUNDLE_LOCKFILE" => nil, "BUNDLE_IGNORE_CONFIG" => "true", "BUNDLE_COOLDOWN" => "3",
  "BUNDLE_PATH" => File.join(root, "vendor/bundle"), "BUNDLE_DISABLE_SHARED_GEMS" => "true",
  "BUNDLE_VERSION" => "4.0.23", "BUNDLE_USER_HOME" => File.join(root, "vendor/rails/bundler"),
  "CONNECTRPC_RAILS_LANE" => lane, "CONNECTRPC_ROOT" => root,
  # The floors bundle must load exactly the Action Pack floor; normal lanes accept any lane release.
  "CONNECTRPC_RAILS_VERSION" => (DependencyFloors.floors(%w[connectrpc-on-rails]).fetch("actionpack").to_s if floors)
}
# Floors run directly under the wrapper bundle: `appraisal <lane>` would reset BUNDLE_GEMFILE.
run = floors ? [] : ["appraisal", "rails-#{lane}"]
command = ARGV.shift
arguments = case command
            when "setup" then ["install"]
            when "floors" then ["exec", "ruby", File.join(root, "scripts/dependency_floors.rb")]
            when "exec" then ["exec", *ARGV]
            when "test" then ["exec", *run, "rspec", *ARGV]
            when "generation:check" then ["exec", "ruby", File.join(root, "scripts/check_appraisals.rb")]
            when "build" then ["exec", "ruby", "-r", File.join(root, "scripts/package_build"), "-e", <<~CODE]
              output = File.expand_path("../../vendor/rails/pkg")
              ["../..", "../connectrpc-rack", "."].each do |directory|
                PackageBuild.build(directory: directory, output: output)
              end
            CODE
            when "package" then ["exec", *run, "ruby", File.join(root, "scripts/check_rails_package.rb")]
            else abort "Usage: ruby scripts/rails.rb setup|floors|test|generation:check|build|package|exec [arguments]"
            end
unless command == "exec"
  puts "Rails lane #{lane}#{' (lowest allowed)' if floors}; Ruby #{RUBY_VERSION}; Bundler 4.0.23"
  puts "Gemfile #{environment.fetch('BUNDLE_GEMFILE')}"
end
$stdout.flush
exec(environment, RbConfig.ruby, "-S", "bundle", "_4.0.23_", *arguments, chdir: adapter)
