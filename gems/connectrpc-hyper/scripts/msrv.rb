# frozen_string_literal: true

# Builds the extension with the declared `rust-version` toolchain against the
# committed Cargo.lock, so the MSRV the README and Cargo.toml state is real.
require_relative "bindgen"

root = File.expand_path("../../..", __dir__)
package = File.expand_path("..", __dir__)
declared = File.read(File.join(package, "ext/connectrpc_hyper/Cargo.toml"))[/^rust-version = "([^"]+)"/, 1]
abort "ext/connectrpc_hyper/Cargo.toml declares no rust-version" unless declared
rustc = IO.popen(%w[rustc --version], err: File::NULL, &:read).to_s
abort "Run with the declared MSRV #{declared} (rustc: #{rustc.strip.inspect})" unless
  rustc.match?(/\Arustc #{Regexp.escape(declared)}(?:\.0)? /)
environment = { "CARGO_TARGET_DIR" => File.join(root, "vendor/hyper/target/msrv") }.merge(HyperBindgen.environment)
system(environment, "cargo", "build", "--locked", "--lib", chdir: package, exception: true)
puts "connectrpc-hyper extension built with its declared MSRV (#{rustc.strip}) and the locked crate graph"
