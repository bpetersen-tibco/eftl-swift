Pod::Spec.new do |spec|

  spec.name          = "Eftl"
  spec.version       = "1.0.0"
  spec.summary       = "Eftl Swift SDK."
  spec.author        = { "Brent Petersen" => "bpeterse@tibco.com" }
  spec.homepage      = "https://github.com/bpetersen-tibco/eftl-swift"
  spec.license       = "MIT"
  spec.ios.deployment_target = "9.0"
  spec.osx.deployment_target = "10.10"
  spec.source        = { :git => "https://github.com/bpetersen-tibco/eftl-swift.git", :tag => "#{spec.version}" }
  spec.source_files  = "Eftl"
  spec.swift_version = "5.0"

  spec.dependency "Starscream", "~> 3.1.0"

end
