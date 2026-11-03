# frozen_string_literal: true

metadata = ConnectRPC::Metadata.new
response = ConnectRPC::Response.new(message: "reply", metadata: metadata, trailers: metadata)
# Keep one argument so this checks tuple inference rather than Symbol#to_proc.
response.each_pair { |name| name.id2name } # rubocop:disable Style/SymbolProc
