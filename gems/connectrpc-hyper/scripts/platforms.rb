# frozen_string_literal: true

# Platform gems that CI cross-builds with rb-sys-dock, and the Ruby minors each
# carries. The Rakefile and scripts/cross.rb share this list.
module HyperPlatforms
  NATIVE = %w[x86_64-linux aarch64-linux x86_64-linux-musl aarch64-linux-musl arm64-darwin
              x86_64-darwin x64-mingw-ucrt aarch64-mingw-ucrt].freeze
  RUBY_MINORS = %w[3.4 4.0].freeze
end
