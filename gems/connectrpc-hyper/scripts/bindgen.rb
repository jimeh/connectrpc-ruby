# frozen_string_literal: true

require "rbconfig"

# Build environment for compiling the extension on the running host.
module HyperBindgen
  # Some distribution libclang packages omit clang's resource headers, which
  # rb-sys's bindgen needs; the C compiler's own include directory supplies them.
  def self.environment
    return {} unless RbConfig::CONFIG["host_os"].include?("linux") && !ENV.key?("BINDGEN_EXTRA_CLANG_ARGS")
    return {} unless Dir.glob("/usr/lib/llvm-*/lib/clang/*/include/stddef.h").empty?

    include_dir = IO.popen(%w[cc -print-file-name=include], err: File::NULL, &:read).to_s.strip
    File.file?(File.join(include_dir, "stddef.h")) ? { "BINDGEN_EXTRA_CLANG_ARGS" => "-I#{include_dir}" } : {}
  end
end
