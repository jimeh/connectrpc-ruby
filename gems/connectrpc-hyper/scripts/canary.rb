# frozen_string_literal: true

# Non-gating newest-crates canary. The specs need the repository layout (core
# as a path gem, the shared Go peer, interop fixtures), so the canary copies
# the checkout's tracked and unignored files into vendor/hyper/canary/tree,
# runs `cargo update` there, reports what changed, then compiles and runs the
# connectrpc-hyper specs inside the copy. The checkout and its development
# extension are never modified.
require "bundler"
require "fileutils"
require "open3"
require "rbconfig"

root = File.expand_path("../../..", __dir__)
tree = File.join(root, "vendor/hyper/canary/tree")
peer = File.join(root, "vendor/hyper/peer")
abort "Build the Go peer first: mise run hyper:peer" unless File.executable?(peer)

files, status = Open3.capture2("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", chdir: root)
abort "git ls-files failed" unless status.success?
FileUtils.rm_rf(tree)
files.split("\0").each do |file|
  source = File.join(root, file)
  next unless File.file?(source)

  FileUtils.mkdir_p(File.join(tree, File.dirname(file)))
  FileUtils.cp(source, File.join(tree, file), preserve: true)
end
FileUtils.mkdir_p(File.join(tree, "vendor/hyper"))
FileUtils.ln_s(peer, File.join(tree, "vendor/hyper/peer"))

package = File.join(tree, "gems/connectrpc-hyper")
# cargo reports each changed crate on stderr ("Updating x v1 -> v2").
output, status = Open3.capture2e("cargo", "update", "--verbose", chdir: package)
abort "cargo update failed:\n#{output}" unless status.success?
changes = output.lines.map(&:strip).grep(/\A(?:Updating|Adding|Removing|Downgrading) (?!crates\.io index)/)
report = ["connectrpc-hyper newest-crates canary (Ruby #{RUBY_VERSION})", "",
          *(changes.empty? ? ["cargo update changed no locked crate"] : changes.map { "- #{it}" })]
puts report
summary = ENV.fetch("GITHUB_STEP_SUMMARY", nil)
File.write(summary, "#{report.join("\n")}\n\n", mode: "a") if summary

Bundler.with_unbundled_env do
  launcher = File.join(package, "scripts/run.rb")
  %w[setup test].each { |command| system(RbConfig.ruby, launcher, command, chdir: tree, exception: true) }
end
puts "connectrpc-hyper specs passed with the updated crate graph (#{changes.size} lock changes)"
