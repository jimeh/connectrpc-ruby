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

complete.each_pair { |pair| pair[0].id2name }
partial.each_pair { |pair| pair[0].id2name }
complete.each_pair { |name, value| name.id2name + value.to_s }
ConnectRPC::TransportRequest.new(url: URI("https://example.invalid"), http_method: "POST", headers: {},
                                 body: "", response_limit: 10).each_pair { |pair| pair[0].id2name }
ConnectRPC::TransportRequest.new.each_pair { |pair| pair[0].id2name }
ConnectRPC::TransportResponse.new(status: 200, headers: {}, body: "").each_pair { |pair| pair[0].id2name }
ConnectRPC::TransportResponse.new.each_pair { |pair| pair[0].id2name }

source = ConnectRPC::CancellationSource.new
outgoing = ConnectRPC::TransportRequest.new(url: URI("http://example.invalid"), http_method: "POST", headers: {},
                                            body: "", response_limit: 10, cancellation: source.token)
outgoing[-8].host
outgoing[-2]&.host_rejected?
outgoing[-1]&.check!
outgoing["cancellation"] = source.token
outgoing[:cancellation]&.on_cancel { nil }&.unregister
partial_request = ConnectRPC::TransportRequest[cancellation: source.token]
partial_request[-1]&.reason
partial_request[-8]&.host
partial_request[:cancellation] = nil
