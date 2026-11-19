# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("connectrpc/hyper/connectrpc_hyper") do |extension|
  # Cargo.lock is part of the package; a source install builds exactly it.
  extension.extra_cargo_args += ["--locked"]
end
