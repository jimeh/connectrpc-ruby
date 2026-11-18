# frozen_string_literal: true

abort "Use pinned Ruby 3.4.12 or 4.0.9" unless %w[3.4.12 4.0.9].include?(RUBY_VERSION)
gem "bundler", "= 4.0.23"
{ "rustc" => "1.98.0", "cargo" => "1.98.0" }.each do |tool, version|
  output = IO.popen([tool, "--version"], err: File::NULL, &:read).to_s
  abort "Use pinned Rust #{version} (#{tool}: #{output.strip.inspect})" unless output.start_with?("#{tool} #{version} ")
end
%w[cargo-deny cargo-audit].each do |tool|
  abort "Install #{tool} with mise run hyper:tools" unless system(tool, "--version", out: File::NULL, err: File::NULL)
end
