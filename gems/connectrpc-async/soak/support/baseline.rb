# frozen_string_literal: true

require "zlib"

module Soak
  class Failure < StandardError; end

  # Warmup rounds did not reach two identical consecutive post-cleanup samples.
  class Unstable < Failure; end

  SETTLE_SECONDS = 15
  GLOBAL_SECONDS = 5
  # Seconds a timer leaves when a scenario's success path must beat it (a
  # peer answering PINGs, a handler entered before its deadline), so that a
  # stalled scheduler does not read as the failure under test.
  TIMER_HEADROOM = 2

  class << self
    def rounds = Integer(ENV.fetch("CONNECTRPC_SOAK_ROUNDS", "5"))
    def seed = @seed ||= Integer(ENV.fetch("CONNECTRPC_SOAK_SEED") { Random.new_seed % (2**31) })
    def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    def report = @report ||= Report.new(ENV.fetch("CONNECTRPC_SOAK_REPORT") { Report.default_directory(package) })
    attr_writer :report
    attr_accessor :package, :current

    # Polls an asserted condition under a bounded deadline, for state with no
    # direct signal, and reports the last observation on timeout.
    def poll(message, timeout: SETTLE_SECONDS)
      expires = now + timeout
      until (last = yield)
        raise Failure, "Timed out waiting for #{message}; last observed: #{last.inspect}" if now > expires

        sleep 0.005 # Poll interval only; the deadline bounds the wait.
      end
      last
    end

    # Like #poll for a list of problems: returns once the list is empty.
    def settle(message, timeout: SETTLE_SECONDS)
      last = nil
      poll(message, timeout: timeout) { (last = yield).empty? || nil }
    rescue Failure
      raise Failure, "#{message} did not settle within #{timeout}s:\n  #{last.join("\n  ")}"
    end

    def close_io(io)
      io.close if io && !io.closed?
    rescue IOError, SystemCallError
      nil
    end

    def stop(task)
      return unless task && !task.finished?

      task.cancel
      task.wait
    rescue StandardError
      nil
    end

    # Descriptors opened or closed between two fd link snapshots.
    def link_diff(before, after)
      { opened: after.reject { |fd, link| before[fd] == link }, closed: before.reject { |fd, link| after[fd] == link } }
    end

    def rss_kb = File.read("/proc/self/status")[/^VmRSS:\s+(\d+)/, 1]&.to_i
  end

  # Maxima observed while load is established, each against its configured cap.
  class Peaks
    def initialize
      @values = {}
      @caps = {}
    end

    def observe(name, value, cap:)
      @values[name] = [@values.fetch(name, value), value].max
      @caps[name] = cap
      value
    end

    def to_h = @values.to_h { |name, value| [name, { peak: value, cap: @caps[name] }] }

    def problems
      @values.filter_map do |name, value|
        "#{name} peaked at #{value}, above its cap #{@caps[name]}" if value > @caps[name]
      end
    end
  end

  # Every resource the library creates during a campaign, observed by a
  # TracePoint on the resource constructors, so untracked leaks still count.
  class ResourceRegistry
    def initialize
      @lock = Mutex.new
      @created = []
      @replacements = {}.compare_by_identity
      created = @created
      lock = @lock
      @traces = Sampler.resource_classes.map do |klass|
        TracePoint.new(:return) { |event| lock.synchronize { created << event.self } }
                  .tap { |trace| trace.enable(target: klass.instance_method(:initialize)) }
      end
    end

    def stop = @traces.each(&:disable)
    def size = @lock.synchronize { @created.size }

    # Unretired resources outside every live owner's accounted set. Retired
    # resources are dropped once observed. A superseded H2 resource counts as
    # owned while its replacement is, and as retired only per
    # Sampler.superseded_problems.
    def problems(owned)
      unretired = @lock.synchronize do
        pair_replacements
        @created.reject! { |resource| retired?(resource).tap { |retired| @replacements.delete(resource) if retired } }
        @created.dup
      end
      stray = unretired.reject { |resource| owned.include?(resource) || owned.include?(@replacements[resource]) }
      stray.map do |resource|
        problem = "unretired #{resource.class} #{resource.object_id} owned by no live client or server"
        replacement = @replacements[resource]
        next problem unless replacement

        "#{problem} (superseded by #{replacement.class} #{replacement.object_id}; " \
          "#{Sampler.superseded_problems(resource).join(', ')})"
      end
    end

    private

    # Records each accepted H2 resource's HTTP/1 replacement while both are
    # still registered: the replacement stays registered until a poll sees it
    # retired, and this runs before that poll prunes it.
    def pair_replacements
      @created.each do |resource|
        next if @replacements.key?(resource)

        replacement = @created.find { |other| Sampler.replacement?(resource, other) }
        @replacements[resource] = replacement if replacement
      end
    end

    def retired?(resource)
      return true if Sampler.retired?(resource)

      @replacements.key?(resource) && Sampler.superseded_problems(resource).empty?
    end
  end

  # Owned objects of a campaign and their retirement or quiescence problems.
  class Tracker
    Entry = Struct.new(:kind, :object, :label, :options, :identity)

    def initialize
      @entries = []
    end

    def add(kind, object, label, **options)
      @entries << Entry.new(kind, object, label, options, nil)
      object
    end

    def remove(object) = @entries.reject! { |entry| entry.object.equal?(object) }
    def leftovers = @entries.reject { |entry| closed?(entry) }.map(&:object).reverse

    def self.force_close(object)
      case object
      when FaultRelay then object.close
      when IO then Soak.close_io(object)
      when Thread then object.kill.join(5)
      when ConnectRPC::CancellationSource then nil
      else object.close(grace: 0)
      end
    end

    def entries(kind) = @entries.select { |entry| entry.kind == kind }

    # Problems of every entry, then of every registered resource. A closed
    # entry with none has retired and is dropped. At the end of a round,
    # round-scoped entries must be closed.
    def problems(registry, round_end: false)
      owned = []
      problems = []
      @entries.dup.each do |entry|
        list, live = check(entry)
        list += ["#{entry.label} still open at the end of its round"] if round_end && entry.options[:round] &&
                                                                         !closed?(entry)
        owned.concat(live)
        problems.concat(list)
        @entries.delete(entry) if list.empty? && closed?(entry)
      end
      problems.concat(registry.problems(owned))
    end

    # A round-scoped cancellation source has nothing to close: it retires once
    # its registrations are empty. The campaign's shared source never retires.
    def closed?(entry)
      case entry.kind
      when :transport, :http1, :threaded then Sampler.ivar(entry.object, :@state) == :closed
      when :server, :io, :relay then entry.object.closed?
      when :thread then !entry.object.alive?
      when :source then entry.options[:round] == true
      else false
      end
    end

    # Unfinished tasks of every live ThreadedTransport owner, sampled on the owner.
    def owner_tasks
      entries(:threaded).filter_map { |entry| Sampler.owner_tasks(entry.object) }
    end

    private

    # [problems, live resources this entry accounts for]
    def check(entry)
      object = entry.object
      label = entry.label
      case entry.kind
      when :transport then check_transport(entry)
      when :threaded then check_threaded(entry)
      when :http1 then check_http1(entry)
      when :server then object.problems(label, connections: entry.options[:connections]&.call)
      when :relay then [object.problems(label), []]
      when :source
        count = Sampler.registrations(object)
        [count.zero? ? [] : ["#{label} has #{count} registrations"], []]
      when :io then [object.closed? ? [] : ["#{label} open"], []]
      when :thread then [object.alive? ? ["#{label} alive"] : [], []]
      end
    end

    def check_transport(entry)
      transport = entry.object
      problems = Sampler.transport_problems(transport, entry.label)
      live = closed?(entry) ? [] : Sampler.live_resources(transport)
      expected = entry.options[:connections]
      if expected && !closed?(entry) && (count = Sampler.live_connections(transport).size) != expected
        problems << "#{entry.label} has #{count} connections, expected #{expected} at this phase"
      end
      problems.concat(reuse_problems(entry, Sampler.connection_identity(transport))) if problems.empty?
      [problems, live]
    end

    def check_threaded(entry)
      transport = entry.object
      problems = Sampler.threaded_problems(transport, entry.label)
      live = closed?(entry) ? [] : Sampler.on_owner(transport) { |inner| Sampler.live_resources(inner) } || []
      if problems.empty? && !closed?(entry)
        problems.concat(reuse_problems(entry, Sampler.threaded_identity(transport) || []))
      end
      [problems, live]
    end

    def check_http1(entry)
      transport = entry.object
      problems = Sampler.http1_problems(transport, entry.label)
      problems.concat(reuse_problems(entry, Sampler.http1_idle_identity(transport))) if problems.empty?
      [problems, []]
    end

    # A reused persistent client keeps the identical connection across rounds.
    def reuse_problems(entry, identity)
      return [] unless entry.options[:reuse] && !closed?(entry)
      return [] if identity.empty?

      entry.identity ||= identity
      entry.identity == identity ? [] : ["#{entry.label} connections changed from #{entry.identity} to #{identity}"]
    end
  end

  # One scenario: warmup rounds until two consecutive post-cleanup global
  # samples are identical, then N measured rounds whose post-cleanup samples
  # must equal that baseline, every round settling owned objects first.
  class Campaign # rubocop:disable Metrics/ClassLength -- one scenario's whole measured lifecycle
    WARMUPS = 3

    # `shared_source` is a never-cancelled source that every call without its
    # own cancellation carries; its listeners must all unregister by the end
    # of each round.
    attr_reader :name, :task, :ledger, :gates, :random, :tracker, :peaks, :info, :round_phase, :shared_source

    # `settle` and `global` bound the polls for owned objects and for global
    # counts; the harness controls shorten them.
    def initialize(name, task:, info: {}, settle: SETTLE_SECONDS, global: GLOBAL_SECONDS)
      @settle_seconds = settle
      @global_seconds = global
      @name = name
      @task = task
      @info = info
      @ledger = Ledger.new
      @gates = Gates.new
      @random = Random.new(Soak.seed ^ Zlib.crc32(name))
      @tracker = Tracker.new
      @shared_source = track(:source, ConnectRPC::CancellationSource.new, "shared cancellation source")
      @peaks = Peaks.new
      @registry = ResourceRegistry.new
      @cleanups = { close: [], stop: [] }
      @peers = []
      @outcomes = Hash.new(0)
      @completed = 0
      @warmups = 0
    end

    def peer(peer)
      @peers << peer
      peer
    end

    # Teardown work, run last-registered first: `close` for clients and
    # servers, then `stop` for peer threads once their servers retired.
    def cleanup(stage = :close, &block)
      @cleanups.fetch(stage) << block
    end

    def track(kind, object, label, **) = @tracker.add(kind, object, label, **)

    # One ingress high-water meter per campaign, stopped with the peers.
    def ingress_meter
      @ingress_meter ||= Sampler::IngressMeter.new.tap { |meter| cleanup(:stop) { meter.stop } }
    end

    # Runs the scenario body (setup, then #run), and always tears down and
    # reports, re-raising the first failure.
    def conduct
      started = Soak.now
      error = nil
      Soak.current = name
      Diagnostics.register("harness #{name}", Fiber.scheduler)
      begin
        yield self
      rescue Exception => e # rubocop:disable Lint/RescueException -- report every escape, including interrupts
        error = e
        # Before teardown changes anything.
        @diagnostics = Report.diagnostics
      end
      finish(started, error)
    end

    # Warmups until the post-cleanup global sample is stable, then the rounds.
    def run(&)
      @baseline = warm_up(&)
      Soak.report.write(:baseline, scenario: name, warmups: @warmups, sample: @baseline)
      Soak.rounds.times do |index|
        round_started = Soak.now
        execute(:round, index, &)
        compare_global!(index)
        @completed += 1
        Soak.report.write(:round, scenario: name, round: index, elapsed: (Soak.now - round_started).round(3),
                                  outcomes: @ledger.summary, rss_kb: Soak.rss_kb,
                                  heap_live_slots: GC.stat(:heap_live_slots))
      end
    end

    def global
      peer_tasks = @peers.map { |peer| peer.run { Sampler.tasks(Fiber.scheduler) } }
      { threads: Sampler.threads, fds: Sampler.fds, harness_tasks: Sampler.tasks(Fiber.scheduler),
        peer_tasks: peer_tasks, owner_tasks: @tracker.owner_tasks }
    end

    # `timeout` overrides the campaign's settle bound, so a harness control can
    # let legitimate cleanup settle under the normal bound before it plants a
    # leak that its short bound then reports.
    def settle!(message = "owned objects of #{name}", round_end: false, timeout: @settle_seconds)
      Soak.settle(message, timeout: timeout) { @tracker.problems(@registry, round_end: round_end) }
    end

    private

    def warm_up(&)
      samples = []
      links = []
      WARMUPS.times do |index|
        execute(:warmup, index, &)
        @warmups += 1
        samples << global
        links << Sampler.fd_links
        if samples.size >= 2 && samples[-1] == samples[-2]
          @baseline_links = links.last
          return samples.last
        end
      end
      raise Unstable, "#{name} did not stabilize in #{WARMUPS} warmups: #{samples[-2].inspect} then " \
                      "#{samples[-1].inspect}; descriptors #{Soak.link_diff(links[-2], links[-1]).inspect}"
    end

    def execute(phase, index)
      @round_phase = phase
      @ledger.reset
      @gates.reset
      begin
        yield index
      ensure
        @gates.open_all
      end
      settle!("owned objects after #{phase} #{index} of #{name}", round_end: true)
      problems = @ledger.problems + @peaks.problems
      raise Failure, "#{name} #{phase} #{index} invariants:\n  #{problems.join("\n  ")}" unless problems.empty?

      @ledger.summary.each { |outcome, count| @outcomes[outcome] += count } if phase == :round
    end

    def compare_global!(index)
      last = nil
      Soak.poll("global counts equal to the baseline", timeout: @global_seconds) { (last = global) == @baseline }
    rescue Failure
      diff = last.to_h { |key, value| [key, { baseline: @baseline[key], observed: value }] }
                 .reject { |_, pair| pair[:baseline] == pair[:observed] }
      raise Failure, "#{name} round #{index} global counts differ from the warmup baseline: #{diff.inspect}; " \
                     "descriptors #{Soak.link_diff(@baseline_links, Sampler.fd_links).inspect}"
    end

    def finish(started, error)
      teardown_error = teardown
      error ||= teardown_error
      record = { scenario: name, status: error ? "failed" : "passed", warmups: @warmups, rounds: Soak.rounds,
                 rounds_completed: @completed, elapsed: (Soak.now - started).round(3), info: @info,
                 outcomes: @outcomes, peaks: @peaks.to_h, rss_kb: Soak.rss_kb,
                 heap_live_slots: GC.stat(:heap_live_slots) }
      if error
        record[:failure] = { error: error.class.name, message: error.message, backtrace: error.backtrace&.first(15),
                             diagnostics: @diagnostics || Report.diagnostics }
      end
      Soak.report.write(:scenario, **record)
      raise error if error
    ensure
      @registry.stop
      Diagnostics.unregister("harness #{name}")
      Soak.current = nil
    end

    # Closes everything, checks that every owned object retired, force-closes
    # whatever a failed round left open (so the reactor can exit), then stops
    # peers. Every step runs whatever an earlier one raised; the first error
    # wins.
    def teardown
      errors = @cleanups[:close].reverse.map { |block| attempt(&block) }
      errors << attempt { settle!("retirement of every owned object of #{name}", round_end: true) }
      @tracker.leftovers.each { |object| attempt { Tracker.force_close(object) } }
      errors.concat(@cleanups[:stop].reverse.map { |block| attempt(&block) })
      errors.compact.first
    end

    def attempt
      yield
      nil
    rescue Exception => e # rubocop:disable Lint/RescueException -- teardown keeps going and reports the first escape
      e
    end
  end
end
