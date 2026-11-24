# frozen_string_literal: true

require "rbconfig"
root = File.expand_path("../../..", __dir__)
files = Dir.glob("{lib,spec,scripts}/**/*.rb") + Dir.glob("*.gemspec") + %w[Gemfile Rakefile
                                                                            ext/connectrpc_hyper/extconf.rb]
files.each { |file| system(RbConfig.ruby, "-c", file, exception: true, out: File::NULL) }
system("rubocop", ".", "--config", File.join(root, ".rubocop.yml"), exception: true)
cargo = { "CARGO_TARGET_DIR" => File.join(root, "vendor/hyper/target/lint") }
system(cargo, "cargo", "fmt", "--all", "--check", exception: true)
system(cargo, "cargo", "clippy", "--locked", "--all-targets", "--", "-D", "warnings", exception: true)
system(cargo, "cargo", "test", "--locked", "--lib", exception: true)
system(cargo, "cargo", "deny", "--locked", "check", exception: true)
system(cargo, "cargo", "audit", "--deny", "warnings", exception: true)
system(RbConfig.ruby, "scripts/notices.rb", "--check", exception: true)
puts "Ruby syntax and lint, cargo fmt, clippy -D warnings, Rust unit tests, cargo deny, cargo audit and notices passed"
