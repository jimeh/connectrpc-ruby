# frozen_string_literal: true

abort "Use pinned Ruby 3.4.12 or 4.0.9" unless %w[3.4.12 4.0.9].include?(RUBY_VERSION)
gem "bundler", "= 4.0.23"
