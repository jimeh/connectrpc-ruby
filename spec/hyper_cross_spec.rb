# frozen_string_literal: true

require "fileutils"
require "tmpdir"
require_relative "../gems/connectrpc-hyper/scripts/platforms"

RSpec.describe HyperPlatforms do
  let(:package) { File.expand_path("../gems/connectrpc-hyper", __dir__) }

  it "pins one rb-sys-dock image per platform gem by tag and OCI index digest" do
    expect(described_class::IMAGES.keys).to eq(described_class::NATIVE)
    described_class::IMAGES.each do |platform, image|
      expect(image).to match(%r{\Arbsys/#{Regexp.escape(platform)}:0\.9\.130@sha256:\h{64}\z})
    end
  end

  it "reports crate graph inputs that a build copy changed or lost" do
    Dir.mktmpdir("connectrpc-hyper-copy-") do |copy|
      described_class::LOCKED_FILES.each do |file|
        FileUtils.mkdir_p(File.join(copy, File.dirname(file)))
        FileUtils.cp(File.join(package, file), File.join(copy, file))
      end
      expect(described_class.locked_drift(package, copy)).to be_empty

      File.write(File.join(copy, "Cargo.lock"), "#{File.read(File.join(copy, 'Cargo.lock'))}\n")
      File.delete(File.join(copy, "ext/connectrpc_hyper/Cargo.toml"))
      expect(described_class.locked_drift(package, copy)).to eq(%w[Cargo.lock ext/connectrpc_hyper/Cargo.toml])
    end
  end
end
