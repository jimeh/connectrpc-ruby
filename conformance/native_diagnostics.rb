# frozen_string_literal: true

# Keep the adapter's stderr intact in its own artifact. The pinned runner's
# accounting stream then contains only runner records, without hiding warnings.
if (target = ENV.fetch("CONFORMANCE_DIAGNOSTICS", nil))
  $stderr.reopen(target, "a")
  $stderr.sync = true
end
