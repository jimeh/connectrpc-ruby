# frozen_string_literal: true

require "stringio"
require_relative "../scripts/support/interop_diagnostics"

RSpec.describe HTTP2InteropDiagnostics do
  it "reports the original setup exception without changing the public unavailable error" do
    reactor do
      # Binding without listening reserves the port while making TCP connect fail.
      reserved = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
      reserved.bind(Socket.sockaddr_in(0, "127.0.0.1"))
      origin = "http://127.0.0.1:#{reserved.local_address.ip_port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      output = StringIO.new
      instrumentation = described_class.instrumentation(transport, mode: "h2c", output: output)
      typed = client(transport, origin, instrumentation: instrumentation)

      expect do
        typed.echo(AsyncHelpers::MESSAGE.new(name: "private payload"), metadata: { "x-private" => "private metadata" })
      end.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("unavailable")
        expect(error.message).to eq("HTTP/2 transport failed")
        expect(error.cause).to be_nil
      }

      expect(output.string.lines.size).to eq(1)
      diagnostic = JSON.parse(output.string.delete_prefix("HTTP/2 interop failure "))
      expect(diagnostic).to include("mode" => "h2c", "codec" => "proto", "phase" => "setup_connection",
                                    "method" => AsyncHelpers::ECHO::ECHO.path, "code" => "unavailable")
      expect(diagnostic.fetch("original_error")).to include("class" => "Errno::ECONNREFUSED")
      expect(diagnostic.dig("original_error", "backtrace")).not_to be_empty
      expect(diagnostic.fetch("resources_after_cleanup")).to include(include("ready" => false))
      expect(output.string).not_to include("private payload", "private metadata", "x-private")
    ensure
      transport&.close(grace: 0)
      reserved&.close
    end
  end

  it "stays silent for success and the runner's expected RPC errors" do
    reactor do
      host = deadline_host(server: server(request_limit: 1024))
      origin = "http://127.0.0.1:#{host.port}"
      transport = ConnectRPC::Async::Transport.new(origin: origin)
      output = StringIO.new
      instrumentation = described_class.instrumentation(transport, mode: "h2c", output: output)
      typed = client(transport, origin, instrumentation: instrumentation)

      expect(typed.echo(AsyncHelpers::MESSAGE.new(name: "healthy")).message.name).to eq("healthy")
      expect { typed.fail(AsyncHelpers::MESSAGE.new) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("invalid_argument")
      }
      expect { typed.echo(AsyncHelpers::MESSAGE.new(name: "x" * 2048)) }.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("resource_exhausted")
      }
      source = ConnectRPC::CancellationSource.new
      source.cancel
      expect do
        typed.echo(AsyncHelpers::MESSAGE.new, cancellation: source.token)
      end.to raise_error(ConnectRPC::Error) { |error|
        expect(error.code).to eq("canceled")
      }
      expect(output.string).to be_empty
    ensure
      transport&.close(grace: 0)
      host&.close(grace: 0)
    end
  end
end
