# frozen_string_literal: true

metadata = ConnectRPC::Metadata.new
complete = ConnectRPC::Response.new(message: "reply", metadata: metadata, trailers: metadata)
complete.message.size
complete[:metadata].to_h
complete["trailers"] = metadata
complete[-3] = "replacement"
complete.to_h
ConnectRPC::Response[{ message: "reply", metadata: metadata, trailers: metadata }].message.size
ConnectRPC::Response.new({ message: "reply", metadata: metadata, trailers: metadata }).metadata.to_h
partial = ConnectRPC::Response.new(message: "reply")
partial.message.size
partial.metadata&.to_h
partial[:metadata] = nil
ConnectRPC::Response[].message.nil?
ConnectRPC::Response.new({}).trailers&.to_h
ConnectRPC::Response[message: nil, metadata: nil].metadata&.to_h
ConnectRPC::TransportResponse[status: 200].body&.bytesize
ConnectRPC::TransportResponse.new({ status: 200, headers: {}, body: "bytes" })[:body].bytesize
ConnectRPC::TransportRequest.new.http_method&.upcase
ConnectRPC::TransportRequest[{ body: "bytes", deadline: nil }][:body]&.bytesize
ConnectRPC::TransportRequest[].url&.host
complete.members.each(&:to_s)
complete.values.size
