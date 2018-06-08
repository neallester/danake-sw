//
//  IntegrationTests.swift
//  danakeTests
//
//  Created by Neal Lester on 6/8/18.
//

import XCTest
@testable import danake

class IntegrationTests: XCTestCase {

    func testParallel() {
        let accessor = InMemoryAccessor()
        var repetitions = 100
        #if os(Linux)
            repetitions = 20
        #endif
        ParallelTest.performTest(accessor: accessor, repetitions: repetitions, logger: nil)
    }
    
    public func testInMemorySample() {
        let accessor = SampleInMemoryAccessor()
        
        SampleUsage.runSample (accessor: accessor)
    }

}
