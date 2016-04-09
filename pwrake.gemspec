# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pwrake/version'

Gem::Specification.new do |gem|
  gem.name          = "pwrake"
  gem.version       = Pwrake::VERSION
  gem.authors       = ["Masahiro TANAKA"]
  gem.email         = ["masa16.tanaka@gmail.com"]
  gem.summary       = %q{Parallel workflow extension for Rake}
  gem.description   = %q{Parallel workflow extension for Rake, runs on multicores, clusters, clouds}
  gem.homepage      = "http://masa16.github.com/pwrake"
  gem.license       = 'MIT'

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.add_runtime_dependency 'parallel', '>= 1.2.4'
end
