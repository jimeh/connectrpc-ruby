# frozen_string_literal: true

# Updates Cargo.lock (all crates, or the `cargo update` arguments given, such as
# `-p tokio`), then regenerates THIRD_PARTY_NOTICES.md for the new graph. Check
# that every new crate release is at least seven days old before committing.
require "rbconfig"

package = File.expand_path("..", __dir__)
system("cargo", "update", *ARGV, chdir: package, exception: true)
system(RbConfig.ruby, File.join(__dir__, "notices.rb"), chdir: package, exception: true)
system("git", "diff", "--stat", "--", "Cargo.lock", "THIRD_PARTY_NOTICES.md", chdir: package, exception: true)
