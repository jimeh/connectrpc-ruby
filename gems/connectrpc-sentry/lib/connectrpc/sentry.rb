# frozen_string_literal: true

require "connectrpc"
require_relative "sentry/version"
require "sentry-ruby"
require "cgi/escape"
# sentry-ruby unescapes incoming baggage with CGI.unescape after requiring only
# cgi/escape. On Ruby 3.4 that default charset lives in cgi/util.
require "cgi/util" unless CGI.class_variable_defined?(:@@accept_charset)
require_relative "sentry/subscriber"
