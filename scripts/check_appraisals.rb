# frozen_string_literal: true

require "appraisal"
require "appraisal/cli"
require "fileutils"
require "tmpdir"

root = File.expand_path("..", __dir__)
adapter = File.join(root, "gems/connectrpc-rails")
FileUtils.mkdir_p(File.join(root, "vendor/rails/tmp"))
Dir.mktmpdir("appraisals-", File.join(root, "vendor/rails/tmp")) do |temporary| # rubocop:disable Metrics/BlockLength
  destination = File.join(temporary, "gems/connectrpc-rails")
  FileUtils.mkdir_p(destination)
  FileUtils.mkdir_p(File.join(temporary, "lib/connectrpc"))
  FileUtils.cp(File.join(root, "connectrpc.gemspec"), temporary)
  FileUtils.cp(File.join(root, "lib/connectrpc/version.rb"), File.join(temporary, "lib/connectrpc"))
  %w[Gemfile Appraisals connectrpc-rails.gemspec].each { |file| FileUtils.cp(File.join(adapter, file), destination) }
  FileUtils.mkdir_p(File.join(temporary, "gems/connectrpc-rack"))
  FileUtils.cp(File.join(root, "gems/connectrpc-rack/connectrpc-rack.gemspec"),
               File.join(temporary, "gems/connectrpc-rack"))
  %w[rack rails].each do |name|
    relative = "gems/connectrpc-#{name}/lib/connectrpc/#{name}/version.rb"
    version_destination = File.join(temporary, relative)
    FileUtils.mkdir_p(File.dirname(version_destination))
    FileUtils.cp(File.join(root, relative), version_destination)
  end
  # Appraisal's real generator reads the development Gemfile, not the selected
  # generated lane. Bundler has already activated the frozen selected bundle.
  original = ENV.fetch("BUNDLE_GEMFILE")
  begin
    ENV["BUNDLE_GEMFILE"] = File.join(destination, "Gemfile")
    Dir.chdir(destination) { Appraisal::CLI.start(["generate"]) }
  ensure
    ENV["BUNDLE_GEMFILE"] = original
  end
  %w[8.0 8.1].each do |lane|
    file = "gemfiles/rails_#{lane}.gemfile"
    expected = File.binread(File.join(adapter, file))
    generated = File.binread(File.join(destination, file))
    abort "Appraisal generation differs: #{file}" unless expected == generated
  end
end
puts "Actual Appraisal 2.5.0 generation matches both checked-in lane Gemfiles"
