# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "net/http"
require "rbconfig"
require "tmpdir"
require "time"

# Optional development tools never enter core setup, runtime gems or global state.
module BrowserTools # rubocop:disable Metrics/ModuleLength
  ROOT = File.expand_path("..", __dir__)
  CACHE = File.join(ROOT, "vendor/browser")
  SOURCE = File.join(ROOT, "browser")
  AUDIT = JSON.parse(File.read(File.join(SOURCE, "artifacts.json"))).freeze

  def self.run(*command, **)
    system(*command, **, exception: true)
  end

  def self.artifact(name)
    return AUDIT.fetch("node") if name == "node-v24.22.1-linux-x64.tar.xz"

    AUDIT.fetch("artifacts").find { |entry| entry.fetch("name") == name } || raise("Unknown artifact: #{name}")
  end

  def self.download(name) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    record = artifact(name)
    expected = record["sha256"] || record.fetch("digest").delete_prefix("sha256:")
    target = File.join(CACHE, "archives", name)
    FileUtils.mkdir_p(File.dirname(target))
    unless File.file?(target)
      uri = URI(record.fetch("url"))
      8.times do
        response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: true) { |http| http.get(uri.request_uri) }
        if response.is_a?(Net::HTTPRedirection)
          uri = URI.join(uri, response.fetch("location"))
          next
        end
        raise "Download failed: #{name}: #{response.code}" unless response.is_a?(Net::HTTPSuccess)

        File.binwrite(target, response.body)
        break
      end
    end
    unless File.file?(target) && Digest::SHA256.file(target).hexdigest == expected
      raise "Artifact checksum mismatch: #{name}"
    end

    target
  end

  def self.setup # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/PerceivedComplexity
    unless RUBY_PLATFORM.include?("x86_64-linux") && File.read("/etc/os-release").include?('VERSION_ID="24.04"')
      raise "Browser tooling is audited only on Linux x86_64 / Ubuntu 24.04"
    end

    AUDIT.fetch("npm").each do |entry|
      unless Time.now.utc - Time.parse(entry.fetch("published")) >= 604_800
        raise "Browser dependency is within cooldown"
      end
    end
    tools = File.join(CACHE, "tools")
    FileUtils.mkdir_p(tools)
    unzip(download("bun-linux-x64.zip"), tools)
    run("tar", "-xzf", download("protobuf-javascript-4.0.3-linux-x86_64.tar.gz"), "-C", tools)
    run("tar", "-xJf", download("node-v24.22.1-linux-x64.tar.xz"), "-C", tools)
    FileUtils.cp(download("protoc-gen-grpc-web-2.1.1-linux-x86_64"), File.join(tools, "protoc-gen-grpc-web"))
    File.chmod(0o755, File.join(tools, "protoc-gen-grpc-web"))
    unzip(download("chrome-headless-shell-linux64.zip"), File.join(CACHE, "chromium"))
    %w[package.json bun.lock bunfig.toml].each { |name| FileUtils.cp(File.join(SOURCE, name), File.join(CACHE, name)) }
    run(bun, "install", "--frozen-lockfile", "--ignore-scripts", "--cache-dir", File.join(CACHE, "cache/npm"),
        chdir: CACHE)
    packages = Dir.glob("node_modules/{*,@*/*}/package.json", base: CACHE).map do |name|
      JSON.parse(File.read(File.join(CACHE, name))).values_at("name", "version")
    end.sort
    wanted = AUDIT.fetch("npm").reject { |entry| entry.fetch("package") == "fsevents" }.map do |entry|
      entry.values_at("package", "version")
    end.sort
    raise "Unexpected installed browser dependency closure: #{packages.inspect}" unless packages == wanted

    raise "Wrong Node runtime" unless `#{node} --version`.strip == "v24.22.1"

    puts "Browser tools: Bun 1.4.4, Node 24.22.1, Playwright 1.64.1, Chromium 156.0.8078.4; five frozen packages"
  end

  def self.bun = File.join(CACHE, "tools/bun-linux-x64/bun")
  def self.node = File.join(CACHE, "tools/node-v24.22.1-linux-x64/bin/node")

  def self.unzip(archive, destination) # rubocop:disable Metrics/MethodLength
    # Python's standard library avoids an OS package installation. Preserve the
    # executable modes from the already checksum-validated official archives.
    run("python3", "-c", <<~PYTHON, archive, destination)
      import os, pathlib, sys, zipfile
      root = pathlib.Path(sys.argv[2]).resolve()
      with zipfile.ZipFile(sys.argv[1]) as archive:
          for member in archive.infolist():
              target = (root / member.filename).resolve()
              if root not in target.parents:
                  raise RuntimeError('archive member outside destination')
              archive.extract(member, root)
              mode = member.external_attr >> 16
              if mode:
                  os.chmod(target, mode & 0o777)
    PYTHON
  end

  def self.generate(check: false) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    Dir.mktmpdir("generation-", CACHE) do |temporary|
      %w[grpcweb grpcwebtext].each do |mode|
        destination = File.join(temporary, mode)
        FileUtils.mkdir_p(destination)
        run("protoc", "-I", File.join(ROOT, "interop/grpc/proto"), "-I", File.join(ROOT, "third_party"),
            "--plugin=protoc-gen-js=#{File.join(CACHE, 'tools/bin/protoc-gen-js')}",
            "--plugin=protoc-gen-grpc-web=#{File.join(CACHE, 'tools/protoc-gen-grpc-web')}",
            "--js_out=import_style=commonjs,binary:#{destination}",
            "--grpc-web_out=import_style=commonjs,mode=#{mode}:#{destination}",
            "peer.proto", "google/rpc/status.proto", "google/protobuf/any.proto")
      end
      names = Dir.glob("**/*.js", base: temporary).sort
      target = File.join(SOURCE, "generated")
      raise "Browser generated file set differs" if check && Dir.glob("**/*.js", base: target).sort != names

      names.each do |name|
        expected = File.join(target, name)
        bytes = File.binread(File.join(temporary, name))
        if check
          raise "Generated browser source differs: #{name}" unless File.binread(expected) == bytes
        else
          FileUtils.mkdir_p(File.dirname(expected))
          File.binwrite(expected, bytes)
        end
        puts "browser/generated/#{name} SHA256 #{Digest::SHA256.hexdigest(bytes)}"
      end
    end
  end

  def self.bundle
    FileUtils.cp_r(File.join(SOURCE, "generated"), CACHE)
    FileUtils.cp(File.join(SOURCE, "entry.js"), File.join(CACHE, "entry.js"))
    run(bun, "build", File.join(CACHE, "entry.js"), "--target=browser", "--format=iife",
        "--outfile=#{File.join(CACHE, 'client.js')}", chdir: CACHE)
  end
end

if $PROGRAM_NAME == __FILE__
  case ARGV.shift
  when "setup" then BrowserTools.setup
  when "generation:build" then BrowserTools.generate
  when "generation:check" then BrowserTools.generate(check: true)
  when "test"
    BrowserTools.bundle
    exec(RbConfig.ruby, File.join(BrowserTools::ROOT, "gems/connectrpc-otel/scripts/run.rb"), "browser", *ARGV)
  else raise "Usage: browser.rb setup|generation:build|generation:check|test"
  end
end
