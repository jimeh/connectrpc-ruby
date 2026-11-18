# frozen_string_literal: true

# Frames a peer read, recorded into an observations hash; see
# IngressPeer#observe_ingress_wire.
module IngressWire
  module_function

  def record(observations, frame)
    case frame
    when Protocol::HTTP2::WindowUpdateFrame then record_credit(observations, frame)
    when Protocol::HTTP2::ResetStreamFrame then observations[:resets][frame.stream_id] = frame.unpack
    when Protocol::HTTP2::PingFrame then record_ping(observations, frame)
    when Protocol::HTTP2::SettingsFrame then record_settings(observations, frame)
    when Protocol::HTTP2::DataFrame then observations[:data][frame.stream_id] += frame.length
    when Protocol::HTTP2::GoawayFrame then observations[:goaway] = frame.unpack
    end
  end

  def record_credit(observations, frame)
    observations[:credit][frame.stream_id] += frame.unpack
    observations[:control] << [:window_update, frame.unpack] if frame.stream_id.zero?
  end

  def record_ping(observations, frame)
    return observations[:pings] << frame.unpack if frame.acknowledgement?

    observations[:probes] << frame.unpack
    observations[:control] << [:ping, frame.unpack]
  end

  def record_settings(observations, frame)
    return if frame.acknowledgement?

    observations[:settings] << frame.unpack
    observations[:control] << [:settings, frame.unpack]
  end
end
