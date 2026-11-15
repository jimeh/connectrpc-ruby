# frozen_string_literal: true

require "securerandom"
require_relative "handlers"

module DemoExample
  # Calls one RPC through the generated client and prints what came back. Every
  # example client shares this code, so the client files differ only in how they
  # build the client. Each method below shows one RPC shape.
  module Calls
    TIMEOUT = 10
    DEFAULT_ARGS = {
      "greet" => %w[World], "fail" => [""], "countdown" => %w[3], "collect" => %w[Ada Grace], "chat" => %w[Ada Grace]
    }.freeze

    module_function

    # Returns true on success and false after printing the RPC error.
    def run(service, options)
      puts "#{options.client} -> #{options.server} at #{options.url} (#{options.settings})"
      # Request metadata: the server logs this ID next to what it received.
      metadata = { "demo-request-id" => [SecureRandom.hex(4)] }
      show("request", metadata)
      call(service, options.rpc, options.args, metadata)
      true
    rescue ConnectRPC::Error => e
      warn "error: #{e.code}: #{e.message}"
      false
    end

    def call(service, rpc, args, metadata)
      args = DEFAULT_ARGS.fetch(rpc) if rpc == "fail" || args.empty? # fail always sends an empty name.
      case rpc
      when "greet", "fail" then greet(service, args.first, metadata)
      when "countdown" then countdown(service, args.first.to_i, metadata) # The server rejects anything outside 1..10.
      when "collect" then collect(service, args, metadata)
      when "chat" then chat(service, args, metadata)
      end
    end

    # Unary: the call returns a response with the message, headers and trailers.
    def greet(service, name, metadata)
      response = service.greet(Demo::V1::GreetRequest.new(name: name), metadata: metadata, timeout: TIMEOUT)
      show("headers", response.metadata)
      puts "message: #{response.message.greeting}"
      show("trailers", response.trailers)
    end

    # Server streaming: the block reads messages until the server ends the stream.
    def countdown(service, from, metadata)
      service.countdown(Demo::V1::CountdownRequest.new(from: from), metadata: metadata, timeout: TIMEOUT) do |reader|
        show("headers", reader.headers)
        reader.each { |message| puts "message: #{message.number}" }
        show("trailers", reader.trailers)
      end
    end

    # Client streaming: the block sends messages. Leaving the block closes the
    # request stream, and the call then returns the single response.
    def collect(service, names, metadata)
      response = service.collect(metadata: metadata, timeout: TIMEOUT) do |writer|
        names.each { |name| writer.send_message(Demo::V1::CollectRequest.new(name: name)) }
      end
      show("headers", response.metadata)
      puts "message: #{response.message.summary}"
    end

    # Bidi streaming: the block sends and receives on one duplex stream.
    def chat(service, names, metadata)
      service.chat(metadata: metadata, timeout: TIMEOUT) do |duplex|
        names.each do |name|
          duplex.send_message(Demo::V1::ChatRequest.new(text: name))
          puts "message: #{duplex.receive.text}" # Wait for each reply before sending the next name.
        end
        duplex.close_send
        duplex.each { |message| puts "message: #{message.text}" } # Read any remaining replies until the stream ends.
        show("headers", duplex.headers)
      end
    end

    # Prints only this demo's own fields; protocol and framework headers are left out.
    def show(label, metadata)
      fields = metadata.to_h.select { |key, _| key.start_with?("demo-") }
      puts "#{label}: #{fields.map { |key, values| "#{key}=#{values.join(',')}" }.join(' ')}" unless fields.empty?
    end
  end
end
