Gem::Specification.new do |spec|
  spec.name          = "embulk-input-lkqd"
  spec.version       = "0.1.0"
  spec.authors       = ["Ming Liu"]
  spec.summary       = "LKQD input plugin for Embulk"
  spec.description   = "Loads reporting data from LKQD API."
  spec.email         = ["liuming@lmws.net"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com//embulk-input-lkqd"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'embulk', ['>= 0.8.28']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
