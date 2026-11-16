# frozen_string_literal: true

require "tmpdir"
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

  describe ".check!" do
    def specs(**versions)
      versions.map { |name, version| Struct.new(:name, :version).new(name.to_s, Gem::Version.new(version)) }
    end

    it "passes and prints each floor when the bundle resolved it exactly" do
      expect { described_class.check!(specs("connectrpc-rack": "0.1.0", rack: "3.2.7")) }
        .to output("floor rack 3.2.7\n").to_stdout
    end

    it "rejects drift, a missing floor dependency and a bundle without floors" do
      expect { described_class.check!(specs("connectrpc-rack": "0.1.0", rack: "3.2.8")) }
        .to raise_error(/Floor drift: rack resolved 3.2.8, expected 3.2.7/)
      expect { described_class.check!(specs("connectrpc-on-rails": "0.1.0", rack: "3.2.7")) }
        .to raise_error(/Floor dependency actionpack is missing from the bundle/)
      expect { described_class.check!(specs("connectrpc-testing": "0.1.0", rake: "13.4.2")) }
        .to raise_error(/No floor-managed dependencies in this bundle/)
    end
  end

  it "evaluates a Gemfile with floors pinned, missing floors appended once and floors-only constraints added" do
    Dir.mktmpdir("connectrpc-floors-") do |directory|
      gemfile = File.join(directory, "Gemfile")
      File.write(gemfile, <<~RUBY)
        source "https://rubygems.org"
        gem "connectrpc-on-rails", path: #{File.join(described_class::ROOT, 'gems/connectrpc-on-rails').dump}
        gem "rake", "13.4.2"
        group :test do
          gem "rack", ">= 3", require: false
        end
      RUBY
      dsl = Bundler::Dsl.new
      described_class.evaluate(dsl, gemfile, constraints: { "json" => "< 3", "rack" => "< 4", "rake" => "< 14" })
      dependencies = dsl.dependencies.to_h { |dependency| [dependency.name, dependency] }

      expect(dsl.dependencies.map(&:name).tally.values).to all(eq(1))
      expect(dependencies.fetch("rack")).to have_attributes(requirement: requirement("= 3.2.7", "< 4"),
                                                            groups: [:test], autorequire: [])
      expect(dependencies.fetch("actionpack").requirement).to eq(requirement("= 8.0.5.1"))
      expect(dependencies.fetch("json").requirement).to eq(requirement("< 3"))
      expect(dependencies.fetch("rake").requirement).to eq(requirement("= 13.4.2", "< 14"))
    end
  end
end
