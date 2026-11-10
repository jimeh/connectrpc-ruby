# frozen_string_literal: true

require_relative "support/retirement_reader"

RSpec.describe "Async reader ownership" do
  def reader_listener
    bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
    session = ConnectRPC::Async::ServerSession.new(application: ->(_) {}, idle_timeout: nil).start
    [nil, bound, session]
  end

  def start_reader_listener(_host, listener, session)
    raw, = listener.accept
    accepted = session.accept(raw)
    session.schedule { accepted.serve }
  end

  include_examples "eager reader retirement"
end

RSpec.describe "Pinned reader lifecycle semantics" do
  def with_reader_connection
    bound = IO::Endpoint.tcp("127.0.0.1", 0).bound
    listener = bound.sockets.first
    peer = IO::Endpoint.tcp("127.0.0.1", listener.local_address.ip_port).connect
    raw, = listener.accept
    connection = AsyncHelpers::INTERNAL::ClientConnection.new(IO::Stream(raw))
    peer.write([0, 0, 0, 4, 0, 0].pack("C5N"))
    connection.send_connection_preface(AsyncHelpers::INTERNAL.settings(128))
    yield connection
  ensure
    connection&.close
    peer&.close
    raw&.close unless raw&.closed?
    bound&.close
  end

  it "rejects a closed connection before checking its finished parent" do
    reactor do |task|
      with_reader_connection do |connection|
        parent = task.async { nil }
        expect(parent).to be_finished
        connection.close!
        expect { connection.read_in_background(parent: parent) }.to raise_error(RuntimeError, "Connection is closed!")
        expect(connection.reader_task).to be_nil
        expect(connection.instance_variable_get(:@reader)).to be_nil
      end
    end
  end

  it "rejects a finished parent before constructing or publishing a reader" do
    reactor do |task|
      with_reader_connection do |connection|
        parent = task.async { nil }
        expect { connection.read_in_background(parent: parent) }.to raise_error(Async::Task::FinishedError)
        expect(connection.reader_task).to be_nil
        expect(parent.children).to be_nil
        expect(connection.instance_variable_get(:@reader)).to be_nil
      end
    end
  end

  it "returns and reuses the actual annotated transient child until external close" do
    reactor do |task|
      with_reader_connection do |connection|
        closes = []
        trace = TracePoint.new(:call) do |event|
          if event.self.equal?(connection) && event.method_id == :close &&
             event.defined_class == AsyncHelpers::INTERNAL::Connection
            closes << Async::Task.current
          end
        end
        trace.enable
        reader = connection.start_connection
        fiber = reader.fiber
        expect(reader).to be_an_instance_of(Async::Task)
        expect(reader.parent).to equal(task)
        expect(reader).to be_transient
        expect(reader.annotation).to eq("HTTP/2 reading data for #{connection.class}.")
        expect(connection.reader_task).to equal(reader)
        expect(connection.instance_variable_get(:@reader)).to equal(reader)
        expect(connection.start_connection).to equal(reader)
        expect(reader).not_to be_finished
        connection.close
        task.with_timeout(2) { AsyncHelpers::INTERNAL.join_complete(reader) }
        expect(reader).to be_cancelled
        expect(fiber).not_to be_alive
        expect(connection.instance_variable_get(:@reader)).to be_nil
        expect(closes).to eq([task])
        trace.disable
      ensure
        trace&.disable
      end
    end
  end

  it "lets cancellation close from the current reader once without forwarding it as a StandardError" do
    reactor do |task|
      with_reader_connection do |connection|
        closes = []
        trace = TracePoint.new(:call) do |event|
          if event.self.equal?(connection) && event.method_id == :close &&
             event.defined_class == AsyncHelpers::INTERNAL::Connection
            closes << [Async::Task.current, event.binding.local_variable_get(:error),
                       connection.instance_variable_get(:@reader)]
          end
        end
        trace.enable
        reader = connection.read_in_background(parent: task)
        fiber = reader.fiber
        expect(reader.parent).to equal(task)
        reader.cancel
        task.with_timeout(2) { AsyncHelpers::INTERNAL.join_complete(reader) }
        expect(reader).to be_cancelled
        expect(reader.result).to be_nil
        expect(fiber).not_to be_alive
        expect(closes).to eq([[reader, nil, nil]])
        trace.disable
      ensure
        trace&.disable
      end
    end
  end
end
