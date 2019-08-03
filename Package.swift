// swift-tools-version:4.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "danake",
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "danake",
            targets: ["danake"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
    .package(url: "https://github.com/mxcl/PromiseKit", from: "6.0.0"),
    .package(url: "https://github.com/neallester/JSONEquality.git", .branch ("master")),
    .package(url: "https://github.com/ianpartridge/swift-backtrace.git", from: "1.0.0"),
        // .package(url: "https://github.com/apple/example-package-fisheryates.git", from: "2.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "danake",
            dependencies: ["PromiseKit", "JSONEquality"]),
        .testTarget(
            name: "danakeTests",
            dependencies: ["danake", "JSONEquality", "Backtrace"]),
    ]
)
