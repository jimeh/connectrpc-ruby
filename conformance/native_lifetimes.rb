# frozen_string_literal: true

module Conformance
  module NativeLifetimes
    private

    def validate_client_lifetimes!(events)
      closed = events.select { |event| event.fetch("event") == "native_closed_lifetime" }
      exact_set!(closed.map { |event| event.fetch("name") }, @scheduled, "native closed lifetimes")
      closed.each do |event|
        verify(event.fetch("state") == "closed", "Native transport did not close")
        verify(event.fetch("calls").zero? && event.fetch("acquiring").zero?, "Native call ownership leaked")
        %w[tasks_finished streams_closed resources_finished lifetime_children_finished].each do |fact|
          verify(event.fetch(fact), "Native cleanup incomplete: #{fact}")
        end
        validate_client_scope_counts!(event, events)
      end
    end

    def validate_client_scope_counts!(event, events)
      counts = event.fetch("counts")
      verify(counts.fetch("library") == { "entries" => 1, "exits" => 1 }, "Native library invocation missing")
      counts.each do |phase, edges|
        actual = %w[enter exit].map do |edge|
          events.count do |item|
            item["event"] == "native_scope_#{edge}" && item["name"] == event["name"] && item["phase"] == phase
          end
        end
        verify(actual == [edges.fetch("entries"), edges.fetch("exits")], "Native scope counts lack actual events")
        verify(actual.first == actual.last, "Unbalanced native scope")
      end
    end

    def validate_server_lifetimes!(events)
      entered = events.select { |event| event.fetch("event") == "native_server_dispatch_enter" }
      exited = events.select { |event| event.fetch("event") == "native_server_dispatch_exit" }
      ids = ->(items) { items.map { |event| [event.fetch("pid"), event.fetch("id")] }.sort }
      verify(!entered.empty? && ids.call(entered) == ids.call(exited), "Unbalanced actual native server dispatch")
      validate_server_completions!(events)
      validate_server_closed!(events)
    end

    def validate_server_closed!(events)
      ready = events.select { |event| event.fetch("event") == "server_ready" }
      closed = events.select { |event| event.fetch("event") == "native_server_closed_lifetime" }
      exact_set!(closed.map { |event| event.fetch("pid") }, ready.map { |event| event.fetch("pid") },
                 "native server closed host lifetimes")
      closed.each { |event| validate_server_closed_host!(event) }
    end

    def validate_server_closed_host!(event)
      verify(event.fetch("state") == "closed", "Native host did not close")
      verify(event.fetch("admitted").zero? && event.fetch("identities").zero?, "Native server ownership leaked")
      verify(event.fetch("resources_finished") && event.fetch("tasks_finished"), "Native host cleanup incomplete")
    end

    def validate_server_completions!(events)
      admitted = events.select { |event| event.fetch("event") == "native_server_admitted" }
      finished = events.select { |event| event.fetch("event") == "native_server_finished" }
      verify(!admitted.empty? && admitted.size == finished.size, "Unfinished native server lifecycle")
      verify(paired_ids(admitted) == paired_ids(finished), "Unbalanced native server observer identities")
      verify(finished.none? do |event|
        event.fetch("processing") == "running"
      end, "Native finish preceded processing cleanup")
    end

    def paired_ids(items)
      items.map { |event| [event.fetch("pid"), event.fetch("id")] }.sort
    end

    def validate_scope_pairs!(events)
      entered = events.select { |event| event.fetch("event") == "native_scope_enter" }
      exited = events.select { |event| event.fetch("event") == "native_scope_exit" }
      keys = ->(items) { items.map { |event| [event.fetch("pid"), event.fetch("name"), event.fetch("id")] }.sort }
      verify(keys.call(entered) == keys.call(exited), "Unbalanced actual native operation identities")
    end

    def validate_native_cancellations!(events)
      calls = events.select do |event|
        event.fetch("event") == "native_call_started" && event.fetch("cancellation")
      end
      calls.each do |call|
        name = call.fetch("name")
        closed = events.find { |event| event.fetch("event") == "native_closed_lifetime" && event["name"] == name }
        verify(closed.fetch("source_canceled"), "Native cancellation source did not transition")
        library = library_exit(events, name)
        verify(library.fetch("code") == "canceled" && library.fetch("cause").nil?,
               "Native canceled library result missing")
        @cancel_stages[name] = "closed_library_and_owned_transport_scopes"
      end
    end

    def library_exit(events, name)
      events.find do |event|
        event.fetch("event") == "native_scope_exit" && event["name"] == name && event["phase"] == "library"
      end
    end
  end
end
