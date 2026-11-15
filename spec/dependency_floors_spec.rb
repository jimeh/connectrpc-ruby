# frozen_string_literal: true

require_relative "../scripts/dependency_floors"

RSpec.describe DependencyFloors do
  def requirement(*parts) = Gem::Requirement.new(*parts)

  it "takes the lowest version each requirement form admits" do
    expect(described_class.floor(requirement(">= 3.3"))).to eq(Gem::Version.new("3.3.0"))
    expect(described_class.floor(requirement("~> 0.105.0"))).to eq(Gem::Version.new("0.105.0"))
    expect(described_class.floor(requirement(">= 8.0.5.1", "< 8.2"))).to eq(Gem::Version.new("8.0.5.1"))
    expect { described_class.floor(requirement("< 1")) }.to raise_error(/No inclusive lower bound in < 1/)
  end

  it "shares the highest floor across packages and rejects one a requirement excludes" do
    shared = described_class.shared_floor("protocol-http1", [requirement("~> 0.41.1"), requirement(">= 0.41.0")])
    expect(shared).to eq(Gem::Version.new("0.41.1"))
    expect { described_class.shared_floor("rack", [requirement("~> 3.1.0"), requirement(">= 3.2.7")]) }
      .to raise_error(/Floor rack 3.2.7 conflicts with ~> 3.1.0; >= 3.2.7/)
  end

  it "derives floors from the loaded gemspecs and their local runtime closure only" do
    falcon = described_class.floors(%w[connectrpc-falcon])
    expect(falcon).to include("falcon" => Gem::Version.new("0.57.0"), "async" => Gem::Version.new("2.46.0"),
                              "openssl" => Gem::Version.new("3.3"))
    expect(falcon.keys).not_to include("rack", "actionpack", "connectrpc", "connectrpc-async")
    expect(described_class.floors(%w[connectrpc-on-rails rake])).to eq(
      "actionpack" => Gem::Version.new("8.0.5.1"), "rack" => Gem::Version.new("3.2.7")
    )
    expect(described_class.floors(%w[connectrpc-testing])).to be_empty
  end

  it "places the ignored wrapper beside the owning Gemfile" do
    expect(described_class.wrapper_path("/repo/gems/connectrpc-async/Gemfile"))
      .to eq("/repo/gems/connectrpc-async/Gemfile.floors")
    expect(described_class.wrapper_path("/repo/gemfiles/rails_8.0.gemfile"))
      .to eq("/repo/gemfiles/rails_8.0.floors.gemfile")
    expect { described_class.wrapper_path("/repo/gems.rb") }.to raise_error(/Cannot derive a floors wrapper name/)
  end
end
