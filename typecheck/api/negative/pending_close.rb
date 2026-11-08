# frozen_string_literal: true

session = ConnectRPC::Async::ServerSession.new(application: ->(_) { nil })
# @type var completed: bool
completed = session.close
!completed
