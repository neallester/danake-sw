//
//  IntegrationTests.swift
//  danakeTests
//
//  Created by Neal Lester on 6/8/18.
//

import XCTest
@testable import danake

class IntegrationTests: XCTestCase {

    override func setUp() {
        BacktraceInstallation.install()
    }
    
    func testParallel() {
        let accessor = InMemoryAccessor()
        var repetitions = 100
        #if os(Linux)
            repetitions = 20
        #endif
        XCTAssertTrue (ParallelTest.performTest(accessor: accessor, repetitions: repetitions, logger: nil))
    }
    
}
