# frozen_string_literal: true

require "async/http/version"
require "protocol/http2/version"

module ConnectRPC
  module Async
    # Raised by require when an installed socketry gem no longer provides a
    # behavior or hook that this package replaces or relies on.
    class UpstreamContractError < StandardError; end
    private_constant :UpstreamContractError

    module Internal
      # Checked once per process while the package loads, so an upstream
      # change fails require with a named reason instead of hanging later.
      # spec/upstream_contract_spec.rb pins the behavior behind each entry.
      module UpstreamContract
        ISSUE = "https://github.com/jimeh/connectrpc-ruby/issues/68"
        TESTED = { "async" => "2.46.0", "async-http" => "0.105.0", "protocol-http2" => "0.29.2" }.freeze
        VERSIONS = {
          "async" => "Async::VERSION", "async-http" => "Async::HTTP::VERSION",
          "protocol-http2" => "Protocol::HTTP2::VERSION", "protocol-http1" => "Protocol::HTTP1::VERSION"
        }.freeze

        H2 = "Async::HTTP::Protocol::HTTP2"
        CONNECTION_HOOKS = [
          ["async-http", :read_in_background, :public, "(parent: ?)", "#{H2}::Connection"],
          ["async-http", :synchronize, :public, "(&)", "#{H2}::Connection"],
          ["async-http", :write_frame, :public, "(_)", "#{H2}::Connection"],
          ["async-http", :write_frames, :public, "()", "#{H2}::Connection"]
        ].freeze
        STREAM_HOOKS = [
          ["async-http", :prepare_input, :public, "(_)", "#{H2}::Stream"],
          ["async-http", :process_data, :public, "(_)", "#{H2}::Stream"],
          ["async-http", :send_body, :public, "(_, _ = ?)", "#{H2}::Stream"],
          ["async-http", :send_headers, :public, "(*, **, &)", "#{H2}::Stream"],
          ["async-http", :finish_output, :public, "(_ = ?)", "#{H2}::Stream"],
          ["protocol-http2", :request_window_update, :public, "()", "Protocol::HTTP2::FlowControlled"]
        ].freeze
        # gem, receiver, method, visibility, parameter shape, defining owner.
        # Positional names are ignored; "?" marks a default value.
        HOOKS = [
          ["async-http", "#{H2}::Output", :start, :public, "(parent: ?)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :stream, :private, "(_)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :passthrough, :private, "(_)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :write, :public, "(_)", "#{H2}::Output"],
          ["async-http", "#{H2}::Output", :send_data, :private, "(_, _, _)", "#{H2}::Output"],
          *%w[Client Server].flat_map { |side| CONNECTION_HOOKS.map { |gem, *hook| [gem, "#{H2}::#{side}", *hook] } },
          ["async-http", "#{H2}::Client", :close, :public, "(_ = ?)", "#{H2}::Connection"],
          ["async-http", "#{H2}::Server", :close, :public, "(_ = ?)", "#{H2}::Server"],
          ["async-http", "#{H2}::Client", :create_response, :public, "()", "#{H2}::Client"],
          ["async-http", "#{H2}::Server", :each, :public, "(task: ?)", "#{H2}::Server"],
          ["async-http", "#{H2}::Request::Stream", :receive_initial_headers, :public, "(_, _)",
           "#{H2}::Request::Stream"],
          ["protocol-http2", "#{H2}::Server", :create_stream, :public, "(_ = ?, &)", "Protocol::HTTP2::Connection"],
          ["protocol-http2", "#{H2}::Server", :send_goaway, :public, "(_ = ?, _ = ?)", "Protocol::HTTP2::Connection"],
          *%w[Client Server].flat_map do |side|
            [["protocol-http2", "#{H2}::#{side}", :update_local_settings, :public, "(_)", "Protocol::HTTP2::Connection"],
             ["protocol-http2", "#{H2}::#{side}", :consume_local_window, :public, "(_)",
              "Protocol::HTTP2::FlowControlled"],
             ["protocol-http2", "#{H2}::#{side}", :request_window_update, :public, "()",
              "Protocol::HTTP2::FlowControlled"],
             ["protocol-http2", "#{H2}::#{side}", :receive_ping, :public, "(_)", "Protocol::HTTP2::Connection"]]
          end,
          ["protocol-http2", "Protocol::HTTP2::PendingSettings", :append, :public, "(_)",
           "Protocol::HTTP2::PendingSettings"],
          ["protocol-http2", "Protocol::HTTP2::Window", :capacity=, :public, "(_)", "Protocol::HTTP2::Window"],
          *%w[Request Response].flat_map do |side|
            STREAM_HOOKS.map { |gem, *hook| [gem, "#{H2}::#{side}::Stream", *hook] }
          end
        ].freeze

        def self.verify!(package: "connectrpc-async", version: VERSION, hooks: HOOKS, tested: TESTED)
          failures = hook_failures(hooks)
          return if failures.empty?

          raise UpstreamContractError, report(package, version, failures, tested)
        end

        def self.report(package, version, failures, tested)
          verified = tested.map { |gem, release| "#{gem} #{release}" }.join(", ")
          ["#{package} #{version} is incompatible with the installed socketry gems:",
           *failures.map { |failure| "  - #{failure}" },
           "Verified with #{verified}. See #{ISSUE}."].join("\n")
        end

        def self.failure(gem, text) = "#{gem} #{installed(gem)}: #{text}"

        def self.installed(gem)
          name = VERSIONS[gem]
          name ? Object.const_get(name) : "(unknown version)"
        rescue NameError
          "(unknown version)"
        end

        def self.hook_failures(hooks)
          hooks.filter_map do |gem, receiver, name, *expected|
            expected = Hook.format(name, *expected)
            found = Hook.describe(receiver, name)
            failure(gem, "#{receiver}##{name} expected #{expected}, found #{found}") unless found == expected
          end
        end

        # Visibility, parameter shape and defining owner, ignoring positional names.
        module Hook
          def self.format(name, visibility, parameters, owner) = "#{visibility} #{name}#{parameters} from #{owner}"

          def self.describe(receiver, name)
            target = Object.const_get(receiver)
            method = target.instance_method(name)
            visibility = if target.public_method_defined?(name) then :public
                         elsif target.protected_method_defined?(name) then :protected
                         else :private
                         end
            format(name, visibility, shape(method.parameters), method.owner)
          rescue NameError
            "nothing"
          end

          def self.shape(parameters)
            tokens = parameters.map do |kind, name|
              case kind
              when :req then "_"
              when :opt then "_ = ?"
              when :rest then "*"
              when :keyreq then "#{name}:"
              when :key then "#{name}: ?"
              when :keyrest then "**"
              when :nokey then "**nil"
              else "&"
              end
            end
            "(#{tokens.join(', ')})"
          end
        end
        private_constant :Hook
      end
    end
  end
end
