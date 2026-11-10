# frozen_string_literal: true

require_relative "release_train"

train = ReleaseTrain.new
case ARGV
when ["check"]
  train.check!
  puts "Release train #{train.version}: seven versions, instrumentation, eight locks and guards agree"
when ["prepare"]
  train.prepare!
  puts "Prepared local locks and installation guides for #{train.version}; review and sign the diff"
when ["build"]
  require_relative "release_candidate"
  puts "Dry-run archives only: #{ReleaseCandidate.new(train).build_from_git!}"
else
  abort "Usage: ruby scripts/release.rb check|prepare|build"
end
