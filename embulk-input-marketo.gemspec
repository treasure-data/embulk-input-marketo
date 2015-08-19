Gem::Specification.new do |spec|
  spec.name          = "embulk-input-marketo"
  spec.version       = "0.1.1"
  spec.authors       = ["uu59", "yoshihara"]
  spec.summary       = "Marketo input plugin for Embulk"
  spec.description   = "Loads records from Marketo."
  spec.email         = ["k@uu59.org", "h.yoshihara@everyleaf.com"]
  spec.licenses      = ["Apache2"]
  spec.homepage      = "https://github.com/treasure-data/embulk-input-marketo"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'savon', ['~> 2.11.1']
  spec.add_development_dependency 'embulk', [">= 0.6.13", "< 1.0"]
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'test-unit'
  spec.add_development_dependency 'test-unit-rr'
  spec.add_development_dependency 'codeclimate-test-reporter'
  spec.add_development_dependency 'everyleaf-embulk_helper'
end
