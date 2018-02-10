//
//  loggingTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/9/18.
//

import XCTest
@testable import danake


class loggingTests: XCTestCase {

    func testLogLevel() {
        XCTAssertEqual ("none", LogLevel.none.rawValue)
        XCTAssertEqual ("debug", LogLevel.debug.rawValue)
        XCTAssertEqual ("fine", LogLevel.fine.rawValue)
        XCTAssertEqual ("info", LogLevel.info.rawValue)
        XCTAssertEqual ("business", LogLevel.business.rawValue)
        XCTAssertEqual ("error", LogLevel.error.rawValue)
        XCTAssertEqual ("emergency", LogLevel.emergency.rawValue)
        XCTAssertTrue (LogLevel.debug > LogLevel.none)
        XCTAssertTrue (LogLevel.fine > LogLevel.debug)
        XCTAssertTrue (LogLevel.info > LogLevel.fine)
        XCTAssertTrue (LogLevel.business > LogLevel.info)
        XCTAssertTrue (LogLevel.error > LogLevel.business)
        XCTAssertTrue (LogLevel.emergency > LogLevel.error)
        XCTAssertEqual (LogLevel.none, LogLevel.none)
        XCTAssertEqual (LogLevel.debug, LogLevel.debug)
        XCTAssertEqual (LogLevel.fine, LogLevel.fine)
        XCTAssertEqual (LogLevel.info, LogLevel.info)
        XCTAssertEqual (LogLevel.business, LogLevel.business)
        XCTAssertEqual (LogLevel.error, LogLevel.error)
        XCTAssertEqual (LogLevel.emergency, LogLevel.emergency)
    }
    
}
