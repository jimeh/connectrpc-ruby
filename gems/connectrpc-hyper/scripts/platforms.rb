# frozen_string_literal: true

# Platform gems that CI cross-builds with rb-sys-dock, and the Ruby minors each
# carries. The Rakefile, scripts/cross.rb and the release train share this list.
module HyperPlatforms
  NATIVE = %w[x86_64-linux aarch64-linux x86_64-linux-musl aarch64-linux-musl arm64-darwin
              x86_64-darwin x64-mingw-ucrt aarch64-mingw-ucrt].freeze
  RUBY_MINORS = %w[3.4 4.0].freeze
  # Reproducible PE output: no link-time timestamp and no COFF symbol table.
  MINGW_RUSTFLAGS = "-Cstrip=symbols -Clink-arg=-Wl,--no-insert-timestamp"
  # rb-sys-dock images by OCI index digest (`docker buildx imagetools inspect`),
  # so a re-pushed tag cannot change a build. The tag must equal the bundled
  # rb_sys version; update both together.
  IMAGES = {
    "x86_64-linux" => "sha256:f7fa7f596078264713966d653e91acf6f5e12389fd8d8db81550fa8b99e5b936",
    "aarch64-linux" => "sha256:a5f77df38dd2a1eceef294fb512c51c62e8b1f7deae8756295dda056f2ab8c30",
    "x86_64-linux-musl" => "sha256:53eb461ff585213551cf348c30a4329b0da1db735347b67877bb92e97018f176",
    "aarch64-linux-musl" => "sha256:bc92a3ee6f7df825b61f684cf09360b93aeccd762dcf3a3f7669a3f1dcb7908c",
    "arm64-darwin" => "sha256:1bd4042bbc11ef4fb6878b80288356f36e222aaf0402e2bf083d7340988ea6ea",
    "x86_64-darwin" => "sha256:2a55595e1828f9c89e29c7da0259e942f437b1f4ce237789f8285ae9ccf8b1b6",
    "x64-mingw-ucrt" => "sha256:fadd06c86e1a9e5173727b181c6f4595031d0f1e1fe736aa45019b51efb5aa0e",
    "aarch64-mingw-ucrt" => "sha256:87e8d5edeca794be47ef191875c2ed38b5c2a68d2480c2725d2af6a7e4b54b68"
  }.to_h { |platform, digest| [platform, "rbsys/#{platform}:0.9.130@#{digest}"] }.freeze
  # The crate graph inputs a cross build must leave byte-identical.
  LOCKED_FILES = %w[Cargo.toml Cargo.lock ext/connectrpc_hyper/Cargo.toml].freeze

  # LOCKED_FILES that differ between the package and a build copy.
  def self.locked_drift(package, copy)
    LOCKED_FILES.reject do |file|
      source = File.join(package, file)
      built = File.join(copy, file)
      File.file?(built) && File.binread(built) == File.binread(source)
    end
  end
end
