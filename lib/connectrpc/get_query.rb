# frozen_string_literal: true

module ConnectRPC
  # Private, host-neutral form query processing. Unknown fields are validated
  # without retaining their values; only the five protocol controls survive.
  # Keep byte validation, preflight bounds and cooperative checks adjacent.
  # rubocop:disable-next Metrics/MethodLength, Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  class GetQuery # rubocop:disable Metrics/ClassLength
    KEYS = %w[connect base64 compression encoding message].freeze

    def self.parse(query, limit:, wire_limit:, deadline: nil)
      Protocol.check_size!(query, limit)
      new(wire_limit, deadline).parse(query.b)
    end

    def initialize(wire_limit, deadline)
      @wire_limit = wire_limit
      @deadline = deadline
    end

    def parse(query)
      values = {} #: Hash[String, String]
      offset = 0
      while offset < query.bytesize
        @deadline&.check!
        ending, separator = field(query, offset)
        name = component(query, offset, separator, 12)
        known = KEYS.include?(name)
        raise Error.new(:invalid_argument, "Repeated GET parameter") if known && values.key?(name)

        start = [separator + 1, ending].min
        # Controls need only enough bytes to distinguish every recognized value.
        cap = name == "message" ? 4 * ((@wire_limit + 2) / 3) : 16
        value = component(query, start, ending, known ? cap : 0, payload: name == "message")
        values[name] = value if known
        offset = ending + 1
      end
      @deadline&.check!
      values
    end

    def self.payload(values, limit:, deadline: nil)
      message = values.fetch("message")
      bytes = if values["base64"] == "1"
                decode_base64(message, limit)
              else
                text = message.dup.force_encoding(Encoding::UTF_8)
                raise Error.new(:invalid_argument, "Invalid GET message UTF-8") unless text.valid_encoding?

                text
              end
      Protocol.check_size!(bytes, limit)
      deadline&.check!
      bytes
    end

    def self.decode_base64(message, limit)
      raise Error.new(:invalid_argument, "Invalid GET base64") unless /\A[A-Za-z0-9_-]*={0,2}\z/.match?(message)

      unpadded = message.delete_suffix("==").delete_suffix("=")
      length = unpadded.bytesize
      if length % 4 == 1 || (message.include?("=") && message.bytesize % 4 != 0)
        raise Error.new(:invalid_argument, "Invalid GET base64")
      end
      raise Error.new(:resource_exhausted, "Message exceeds byte limit") if length * 3 / 4 > limit

      padded = message + ("=" * ((4 - (message.bytesize % 4)) % 4))
      bytes = padded.tr("-_", "+/").unpack1("m0") #: String
      unless [bytes].pack("m0").tr("+/", "-_").delete("=") == unpadded
        raise Error.new(:invalid_argument, "Invalid GET base64")
      end

      bytes
    rescue ArgumentError
      raise Error.new(:invalid_argument, "Invalid GET base64")
    end

    def self.build(url, bytes, codec:, encoding:, limit:, deadline: nil)
      base64 = codec == :proto || encoding == :gzip
      prefix = +"connect=v1&"
      prefix << "base64=1&" if base64
      prefix << "compression=gzip&" if encoding == :gzip
      prefix << "encoding=#{codec}&message="
      size = base64 ? ((bytes.bytesize * 4) + 2) / 3 : form_size(bytes, deadline)
      if url.to_s.bytesize + 1 + prefix.bytesize + size > limit
        raise Error.new(:resource_exhausted, "GET URL exceeds byte limit")
      end

      deadline&.check!
      message = base64 ? [bytes].pack("m0").tr("+/", "-_").delete("=") : URI.encode_www_form_component(bytes)
      url.query = prefix + message
      deadline&.check!
      url
    end

    def self.form_size(bytes, deadline)
      size = 0
      bytes.each_byte.with_index do |byte, index|
        deadline&.check! if (index % 1024).zero?
        size += byte == 32 || form_safe?(byte) ? 1 : 3
      end
      size
    end

    def self.form_safe?(byte)
      byte.between?(65, 90) || byte.between?(97, 122) || byte.between?(48, 57) || [42, 45, 46, 95].include?(byte)
    end

    private

    def field(query, offset)
      ending = offset
      separator = nil #: Integer?
      while ending < query.bytesize && query.getbyte(ending) != 38
        @deadline&.check! if ((ending - offset) % 1024).zero?
        separator ||= ending if query.getbyte(ending) == 61
        ending += 1
      end
      [ending, separator || ending]
    end

    def component(query, start, ending, cap, payload: false)
      bytes = +"".b
      index = start
      while index < ending
        @deadline&.check! if ((index - start) % 1024).zero?
        byte = query.getbyte(index) #: Integer
        if byte == 37
          raise Error.new(:invalid_argument, "Malformed GET percent encoding") if index + 2 >= ending

          high = hex(query.getbyte(index + 1))
          low = hex(query.getbyte(index + 2))
          byte = (high * 16) + low
          index += 2
        elsif byte == 43
          byte = 32
        end
        if bytes.bytesize < cap
          bytes << byte
        elsif payload
          raise Error.new(:resource_exhausted, "Message exceeds byte limit")
        end
        index += 1
      end
      bytes
    end

    def hex(byte)
      raise Error.new(:invalid_argument, "Malformed GET percent encoding") unless byte

      return byte - 48 if byte.between?(48, 57)
      return byte - 55 if byte.between?(65, 70)
      return byte - 87 if byte.between?(97, 102)

      raise Error.new(:invalid_argument, "Malformed GET percent encoding")
    end
  end
  private_constant :GetQuery
end
