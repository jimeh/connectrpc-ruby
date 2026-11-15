# frozen_string_literal: true

require_relative "handlers"

module DemoExample
  # Calls one RPC through the generated client and prints what came back. Every
  # example client shares this code, so the client files differ only in transport.
  module Calls
    TIMEOUT = 10
    DEFAULT_ARGS = {
      "greet" => %w[World], "fail" => [""], "countdown" => %w[3], "collect" => %w[Ada Grace], "chat" => %w[Ada Grace]
    }.freeze

    module_function

    # Returns true on success and false after printing the RPC error.
    def run(service, options)
      puts "#{options.client} -> #{options.server} at #{options.url} (#{options.settings})"
      call(service, options.rpc, options.args)
      true
    rescue ConnectRPC::Error => e
      warn "error: #{e.code}: #{e.message}"
      false
    end

    def call(service, rpc, args)
      args = DEFAULT_ARGS.fetch(rpc) if rpc == "fail" || args.empty?
      case rpc
      when "greet", "fail" then greet(service, args.first)
      when "countdown" then countdown(service, args.first.to_i) # The server rejects anything outside 1..10.
      when "collect" then collect(service, args)
      when "chat" then chat(service, args)
      end
    end

    def greet(service, name)
      response = service.greet(Demo::V1::GreetRequest.new(name: name), timeout: TIMEOUT)
      show("headers", response.metadata)
      puts "message: #{response.message.greeting}"
      show("trailers", response.trailers)
    end

    def countdown(service, from)
      service.countdown(Demo::V1::CountdownRequest.new(from: from), timeout: TIMEOUT) do |reader|
        show("headers", reader.headers)
        reader.each { |message| puts "message: #{message.number}" }
        show("trailers", reader.trailers)
      end
    end

    def collect(service, names)
      # Leaving the block closes the request stream; then the single response arrives.
      response = service.collect(timeout: TIMEOUT) do |writer|
        names.each { |name| writer.send_message(Demo::V1::CollectRequest.new(name: name)) }
      end
      show("headers", response.metadata)
      puts "message: #{response.message.summary}"
    end

    def chat(service, names)
      service.chat(timeout: TIMEOUT) do |duplex|
        names.each do |name|
          duplex.send_message(Demo::V1::ChatRequest.new(text: name))
          puts "message: #{duplex.receive.text}" # Each reply arrives before the next send.
        end
        duplex.close_send
        duplex.each { |message| puts "message: #{message.text}" } # Reads to the end of the stream.
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
