# frozen_string_literal: true

require "stringio"

RSpec.describe "Native streaming shape capabilities" do
  let(:methods) do
    [ConnectRPC::ServerStreamMethod, ConnectRPC::ClientStreamMethod, ConnectRPC::BidiStreamMethod].map do |type|
      type.new(service: "test.Capabilities", name: type.name.split("::").last,
               request_type: String, response_type: String)
    end
  end
  let(:binding) do
    Object.new.tap do |value|
      value.define_singleton_method(:encode) { |message, **| message }
      value.define_singleton_method(:decode) { |bytes, **| bytes }
    end
  end
  let(:target) do
    ConnectRPC::Server.new(services: [ConnectRPC::Service.new(name: methods.first.service, methods: methods)],
                           binding: binding, handlers: methods.to_h { |rpc| [rpc.path, ->(*) { raise "invoked" }] })
  end

  def invoke(rpc = methods.first, **)
    @bytes = +""
    output = Object.new
    bytes = @bytes
    output.define_singleton_method(:write) { |chunk, **| bytes << chunk }
    headers = { "content-type" => ["application/connect+proto"] }
    target.call_stream(path: rpc.path, http_method: "POST", headers: headers,
                       input: @input ||= StringIO.new("unread"), output: output, publish: ->(_) {}, **)
  end

  [[], [:unary], ["server_stream"], %i[server_stream server_stream], false, :server_stream].each do |capabilities|
    it "rejects invalid capabilities #{capabilities.inspect} before input or codecs" do
      expect(binding).not_to receive(:decode)
      @input = StringIO.new("unread")
      expect(@input).not_to receive(:read)
      expect { invoke(capabilities: capabilities) }.to raise_error(ArgumentError, /capabilities/)
    end
  end

  it "rejects explicit capabilities combined with the legacy H2 grant" do
    expect { invoke(http2: true, capabilities: [:server_stream]) }.to raise_error(ArgumentError, /capabilities/)
  end

  it "keeps the default portable media rejection" do
    expect(invoke.status).to eq(415)
    expect(invoke(capabilities: nil).status).to eq(415)
  end

  it "returns bounded unimplemented terminals for omitted SS and CS without input or codec work" do
    expect(binding).not_to receive(:decode)
    expect(binding).not_to receive(:encode)
    @input = StringIO.new("unread")
    expect(@input).not_to receive(:read)
    methods.first(2).each do |rpc|
      expect(invoke(rpc, capabilities: [:bidi_stream])).to be_nil
      expect(@bytes.getbyte(0)).to eq(2)
      terminal = JSON.parse(@bytes.byteslice(5..))
      expect(terminal.fetch("error").fetch("code")).to eq("unimplemented")
      expect(@bytes.bytesize).to be < 1024
    end
  end

  it "returns host-neutral bare bidi 505 before method, media, codecs or input" do
    rpc = methods.last
    expect(binding).not_to receive(:encode)
    expect(binding).not_to receive(:decode)
    @input = StringIO.new("unread")
    expect(@input).not_to receive(:read)
    response = target.call_stream(path: rpc.path, http_method: "GET", headers: {}, input: @input,
                                  output: Object.new, publish: ->(_) { raise "published" },
                                  capabilities: %i[server_stream client_stream])
    expect([response.status, response.headers, response.body]).to eq([505, {}, ""])
  end
end
