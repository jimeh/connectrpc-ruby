# frozen_string_literal: true

require "connectrpc"
require "google/protobuf"

# The generated files use the canonical feature name grpc/health/v1/health_pb,
# which the grpc gem shares with an older descriptor that lacks List. Load this
# package's own file by path, so the glue's feature-name require cannot resolve
# to the grpc gem's file when that comes first on the load path. An older
# descriptor already in the generated pool still breaks the List declarations.
pool = Google::Protobuf::DescriptorPool.generated_pool
require_relative "../grpc/health/v1/health_pb" unless pool.lookup("grpc.health.v1.HealthCheckRequest")
unless pool.lookup("grpc.health.v1.HealthListRequest")
  raise LoadError, "An older grpc.health.v1 descriptor without List is already loaded; " \
                   "require \"connectrpc/health\" before the grpc gem's health files"
end

require "grpc/health/v1/health_connect"
require_relative "health/version"
require_relative "health/watcher"
require_relative "health/registry"

module ConnectRPC
  module Health
    SERVICE = ::Grpc::Health::V1::Health::SERVICE
  end
end
