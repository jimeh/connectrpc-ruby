# frozen_string_literal: true

# Frames a peer read, recorded into an observations hash; see
# IngressPeer#observe_ingress_wire. `resets` keeps each stream's last code,
# `reset_log` every code in arrival order, and `ended` the streams whose DATA
# or HEADERS carried END_STREAM.
module IngressWire
  module_function

  def observations
    { credit: Hash.new(0), resets: {}, reset_log: Hash.new { |log, id| log[id] = [] }, ended: [], pings: [],
      goaway: nil, probes: [], settings: [], control: [], data: Hash.new(0) }
  end

  def record(observations, frame)
    case frame
    when Protocol::HTTP2::WindowUpdateFrame then record_credit(observations, frame)
    when Protocol::HTTP2::ResetStreamFrame then record_reset(observations, frame)
    when Protocol::HTTP2::PingFrame then record_ping(observations, frame)
    when Protocol::HTTP2::SettingsFrame then record_settings(observations, frame)
    when Protocol::HTTP2::DataFrame, Protocol::HTTP2::HeadersFrame then record_stream(observations, frame)
    when Protocol::HTTP2::GoawayFrame then observations[:goaway] = frame.unpack
    end
  end

  def record_reset(observations, frame)
    observations[:resets][frame.stream_id] = frame.unpack
    observations[:reset_log]&.[](frame.stream_id)&.push(frame.unpack)
  end

  def record_stream(observations, frame)
    observations[:data][frame.stream_id] += frame.length if frame.is_a?(Protocol::HTTP2::DataFrame)
    observations[:ended]&.push(frame.stream_id) if frame.end_stream?
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
