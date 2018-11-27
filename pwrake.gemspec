# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pwrake/version'

Gem::Specification.new do |gem|
  gem.name          = "pwrake"
  gem.version       = Pwrake::VERSION
  gem.authors       = ["Masahiro TANAKA"]
  gem.email         = ["masa16.tanaka@gmail.com"]
  gem.summary       = %q{Parallel and distributed Rake, which runs workflows on multicores, clusters, clouds. It allows parallel process execution on remote nodes via SSH.}
  gem.description   = %q{Parallel and distributed Rake, which runs workflows on multicores, clusters, clouds. It invokes Rake tasks in parallel and executes processes on remote nodes via SSH. It provides locality-aware scheduling on Gfarm file system.}
  gem.homepage      = "http://masa16.github.com/pwrake"
  gem.license       = 'MIT'

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.required_ruby_version = '>= 2.2.3'
end
