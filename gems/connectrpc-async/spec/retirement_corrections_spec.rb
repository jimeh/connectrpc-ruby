# frozen_string_literal: true

require_relative "support/retirement_corrections"

RSpec.describe "Shared retirement first review corrections" do
  include_examples "retirement close corrections", :h2

  %i[abort close].each do |operation|
    it "allows #{operation} coordinator close reentry from listener cleanup" do
      reactor do
        reentries = []
        session = ConnectRPC::Async::ServerSession.new(
          application: ->(_) {}, stop_accepting: -> { reentries << session.close(grace: 0) }
        ).start
        result = operation == :abort ? session.abort : session.close(grace: 1)
        expect(result).to equal(operation == :abort ? session : true)
        expect(reentries).not_to be_empty
        expect(session.close(grace: 0)).to be(operation == :close)
      ensure
        session&.abort
      end
    end
  end

  [1, 0].each do |frame_type|
    it "forces the actual deferred server #{frame_type == 1 ? 'HEADERS' : 'DATA'} writer before gate release" do
      reactor do |task|
        with_retirement_connection(task) do |state|
          resource = state.fetch(:resource)
          gate = FrameGate.new(resource.socket, frame_type: frame_type)
          resource.connection.stream.instance_variable_set(:@io, gate)
          gate.armed = true
          state.fetch(:commands).enqueue(->(_) {})
          state.fetch(:gate).entered.dequeue
          state.fetch(:gate).release
          gate.started.dequeue
          writer = resource.connection.writers.keys.first
          expect(writer).to be_alive
          expect(writer.parent).not_to be_nil
          aborter = task.async(finished: false) { resource.abort }
          begin
            task.with_timeout(2) { writer.wait }
          rescue Async::TimeoutError
            # The assertion below owns this bounded pre-correction failure.
            nil
          end
          puts "deferred frame=#{frame_type} writer=#{writer.object_id} deferred=#{writer.cancel_deferred?} " \
               "alive=#{writer.alive?} finished=#{writer.finished?} " \
               "cleanup=#{resource.instance_variable_get(:@cleanup).object_id}"
          expect(writer).to be_finished, "registered deferred writer must finish before the fixture gate is released"
          join_retirement_tasks(task, aborter, writer, resource.task, resource.connection.reader_task)
          expect(retirement_complete?(resource)).to be(true)
        ensure
          gate&.release&.enqueue(true)
        end
      end
    end
  end
end
