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
        XCTAssertEqual ("warning", LogLevel.warning.rawValue)
        XCTAssertEqual ("error", LogLevel.error.rawValue)
        XCTAssertEqual ("business", LogLevel.business.rawValue)
        XCTAssertEqual ("emergency", LogLevel.emergency.rawValue)
        XCTAssertTrue (LogLevel.fine > LogLevel.debug)
        XCTAssertTrue (LogLevel.info > LogLevel.fine)
        XCTAssertTrue (LogLevel.warning > LogLevel.info)
        XCTAssertTrue (LogLevel.error > LogLevel.warning)
        XCTAssertTrue (LogLevel.business > LogLevel.error)
        XCTAssertTrue (LogLevel.emergency > LogLevel.business)
        XCTAssertEqual (LogLevel.none, LogLevel.none)
        XCTAssertEqual (LogLevel.debug, LogLevel.debug)
        XCTAssertEqual (LogLevel.fine, LogLevel.fine)
        XCTAssertEqual (LogLevel.info, LogLevel.info)
        XCTAssertEqual (LogLevel.warning, LogLevel.warning)
        XCTAssertEqual (LogLevel.error, LogLevel.error)
        XCTAssertEqual (LogLevel.business, LogLevel.business)
        XCTAssertEqual (LogLevel.emergency, LogLevel.emergency)
        XCTAssertTrue (LogLevel.none > LogLevel.emergency)
    }
    
    func testFormattedData() {
        XCTAssertEqual ("", LogEntryFormatter.formattedData(data: nil))
        XCTAssertEqual ("", LogEntryFormatter.formattedData(data: []))
        XCTAssertEqual ("name1=value1", LogEntryFormatter.formattedData(data: [(name: "name1", value:"value1")]))
        XCTAssertEqual ("name1=value1;name2=value2", LogEntryFormatter.formattedData(data: [(name: "name1", value:"value1"),(name: "name2", value:"value2")]))
        XCTAssertEqual ("name1=value1;name2=value2;name3=value3", LogEntryFormatter.formattedData(data: [(name: "name1", value:"value1"),(name: "name2", value:"value2"),(name: "name3", value:"value3")]))
        XCTAssertEqual ("name1=1", LogEntryFormatter.formattedData(data: [(name: "name1", value:1)]))
        XCTAssertEqual ("name1=1;name2=2", LogEntryFormatter.formattedData(data: [(name: "name1", value:1),(name: "name2", value:2)]))
        XCTAssertEqual ("name1=1;name2=2;name3=3", LogEntryFormatter.formattedData(data: [(name: "name1", value:1),(name: "name2", value:2),(name: "name3", value:3)]))
        XCTAssertEqual ("name1=1;name2=2;name3=3;name4=nil", LogEntryFormatter.formattedData(data: [(name: "name1", value:1),(name: "name2", value:2),(name: "name3", value:3), (name:"name4", nil)]))
        
    }
    
    func testStandardFormat() {
        XCTAssertEqual ("BUSINESS|loggingTests.testStandardFormat|Message 1", LogEntryFormatter.standardFormat(level: .business, source: self, featureName: "testStandardFormat", message: "Message 1", data: nil))
        XCTAssertEqual ("BUSINESS|loggingTests.testStandardFormat|Message 1", LogEntryFormatter.standardFormat(level: .business, source: self, featureName: "testStandardFormat", message: "Message 1", data: []))
        XCTAssertEqual ("DEBUG|loggingTests.testStandardFormat|Message 2|name1=1", LogEntryFormatter.standardFormat(level: .debug, source: self, featureName: "testStandardFormat", message: "Message 2", data: [(name: "name1", value:1)]))
    }
    
    func testLogEntry() {
        let entry = LogEntry (level: .debug, source: self, featureName: "testLogEntry", message: "Message 1", data: [(name: "name1", value:1)])
        let now = Date();
        XCTAssertTrue (now.timeIntervalSince1970 + 1 > entry.time.timeIntervalSince1970)
        XCTAssertTrue (now.timeIntervalSince1970 - 1 < entry.time.timeIntervalSince1970)
        XCTAssertEqual(LogLevel.debug, entry.level)
        XCTAssertEqual ("loggingTests", entry.source)
        XCTAssertEqual ("testLogEntry", entry.featureName)
        XCTAssertEqual ("Message 1", entry.message)
        XCTAssertEqual (1, entry.data!.count)
        XCTAssertEqual ("name1", entry.data![0].name)
        let entryValue1 = entry.data![0].value as! Int
        XCTAssertEqual (1, entryValue1)
        XCTAssertEqual ("DEBUG|loggingTests.testLogEntry|Message 1|name1=1", entry.asTestString())
    }
    
    func testInMemoryLogger() {
        var logger = InMemoryLogger()
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
        logger.log (level: .debug, source: self, featureName: "testInMemoryLogger", message: "Message 1", data: [(name: "name1", value:1)])
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            let entry = entries[0]
            let now = Date();
            XCTAssertTrue (now.timeIntervalSince1970 + 1 > entry.time.timeIntervalSince1970)
            XCTAssertTrue (now.timeIntervalSince1970 - 1 < entry.time.timeIntervalSince1970)
            XCTAssertEqual(LogLevel.debug, entry.level)
            XCTAssertEqual ("loggingTests", entry.source)
            XCTAssertEqual ("testInMemoryLogger", entry.featureName)
            XCTAssertEqual ("Message 1", entry.message)
            XCTAssertEqual (1, entry.data!.count)
            XCTAssertEqual ("name1", entry.data![0].name)
            let entryValue1 = entry.data![0].value as! Int
            XCTAssertEqual (1, entryValue1)
            XCTAssertEqual ("DEBUG|loggingTests.testInMemoryLogger|Message 1|name1=1", entry.asTestString())
        }
        logger = InMemoryLogger (level: .business)
        logger.log (level: .debug, source: self, featureName: "testInMemoryLogger", message: "Message 2", data: [(name: "name1", value:1)])
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
        logger.log (level: .business, source: self, featureName: "testInMemoryLogger", message: "Message 3", data: [(name: "name1", value:1)])
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            let entry = entries[0]
            let now = Date();
            XCTAssertTrue (now.timeIntervalSince1970 + 1 > entry.time.timeIntervalSince1970)
            XCTAssertTrue (now.timeIntervalSince1970 - 1 < entry.time.timeIntervalSince1970)
            XCTAssertEqual(LogLevel.business, entry.level)
            XCTAssertEqual ("loggingTests", entry.source)
            XCTAssertEqual ("testInMemoryLogger", entry.featureName)
            XCTAssertEqual ("Message 3", entry.message)
            XCTAssertEqual (1, entry.data!.count)
            XCTAssertEqual ("name1", entry.data![0].name)
            let entryValue1 = entry.data![0].value as! Int
            XCTAssertEqual (1, entryValue1)
            XCTAssertEqual ("BUSINESS|loggingTests.testInMemoryLogger|Message 3|name1=1", entry.asTestString())
        }
        logger = InMemoryLogger (level: .none)
        logger.log (level: .debug, source: self, featureName: "testInMemoryLogger", message: "Message 4", data: [(name: "name1", value:1)])
        logger.log (level: .fine, source: self, featureName: "testInMemoryLogger", message: "Message 5", data: [(name: "name1", value:1)])
        logger.log (level: .info, source: self, featureName: "testInMemoryLogger", message: "Message 6", data: [(name: "name1", value:1)])
        logger.log (level: .warning, source: self, featureName: "testInMemoryLogger", message: "Message 7", data: [(name: "name1", value:1)])
        logger.log (level: .error, source: self, featureName: "testInMemoryLogger", message: "Message 8", data: [(name: "name1", value:1)])
        logger.log (level: .business, source: self, featureName: "testInMemoryLogger", message: "Message 9", data: [(name: "name1", value:1)])
        logger.log (level: .emergency, source: self, featureName: "testInMemoryLogger", message: "Message 10", data: [(name: "name1", value:1)])
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
    }
}
