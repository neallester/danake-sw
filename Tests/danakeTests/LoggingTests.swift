//
//  LoggingTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/9/18.
//

import XCTest
@testable import danake


class LoggingTests: XCTestCase {

    override func setUp() {
        BacktraceInstallation.install()
    }
    
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
        XCTAssertTrue (LogLevel.none > LogLevel.emergency)
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
        XCTAssertEqual ("BUSINESS|LoggingTests.testStandardFormat|Message 1", LogEntryFormatter.standardFormat(level: .business, source: self, featureName: "testStandardFormat", message: "Message 1", data: nil))
        XCTAssertEqual ("BUSINESS|LoggingTests.testStandardFormat|Message 1", LogEntryFormatter.standardFormat(level: .business, source: self, featureName: "testStandardFormat", message: "Message 1", data: []))
        XCTAssertEqual ("DEBUG|LoggingTests.testStandardFormat|Message 2|name1=1", LogEntryFormatter.standardFormat(level: .debug, source: self, featureName: "testStandardFormat", message: "Message 2", data: [(name: "name1", value:1)]))
    }
    
    func testLogEntry() {
        let entry = LogEntry (level: .debug, source: self, featureName: "testLogEntry", message: "Message 1", data: [(name: "name1", value:1)])
        let now = Date();
        XCTAssertTrue (now.timeIntervalSince1970 + 1 > entry.time.timeIntervalSince1970)
        XCTAssertTrue (now.timeIntervalSince1970 - 1 < entry.time.timeIntervalSince1970)
        XCTAssertEqual(LogLevel.debug, entry.level)
        XCTAssertEqual ("LoggingTests", entry.source)
        XCTAssertEqual ("testLogEntry", entry.featureName)
        XCTAssertEqual ("Message 1", entry.message)
        XCTAssertEqual (1, entry.data!.count)
        XCTAssertEqual ("name1", entry.data![0].name)
        let entryValue1 = entry.data![0].value as! Int
        XCTAssertEqual (1, entryValue1)
        XCTAssertEqual ("DEBUG|LoggingTests.testLogEntry|Message 1|name1=1", entry.asTestString())
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
            XCTAssertEqual ("LoggingTests", entry.source)
            XCTAssertEqual ("testInMemoryLogger", entry.featureName)
            XCTAssertEqual ("Message 1", entry.message)
            XCTAssertEqual (1, entry.data!.count)
            XCTAssertEqual ("name1", entry.data![0].name)
            let entryValue1 = entry.data![0].value as! Int
            XCTAssertEqual (1, entryValue1)
            XCTAssertEqual ("DEBUG|LoggingTests.testInMemoryLogger|Message 1|name1=1", entry.asTestString())
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
            XCTAssertEqual ("LoggingTests", entry.source)
            XCTAssertEqual ("testInMemoryLogger", entry.featureName)
            XCTAssertEqual ("Message 3", entry.message)
            XCTAssertEqual (1, entry.data!.count)
            XCTAssertEqual ("name1", entry.data![0].name)
            let entryValue1 = entry.data![0].value as! Int
            XCTAssertEqual (1, entryValue1)
            XCTAssertEqual ("BUSINESS|LoggingTests.testInMemoryLogger|Message 3|name1=1", entry.asTestString())
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
    
    func testWaitForEntry() {
        let logger = InMemoryLogger()
        var foundEntry = logger.waitForEntry(intervalUseconds: 10, timeoutSeconds: 0.0001) { entries in
            return entries.count > 0
        }
        XCTAssertFalse(foundEntry)
        logger.logImplementation(level: .business, source: self, featureName: "testWaitForEntry", message: "FIRST", data: nil)
        foundEntry = logger.waitForEntry(intervalUseconds: 10, timeoutSeconds: 1.0) { entries in
            return entries.last?.asTestString().contains("FIRST") ?? false
        }
        XCTAssertTrue (foundEntry)
        foundEntry = false
        let queue = DispatchQueue (label: "Test", attributes: .concurrent)
        queue.asyncAfter(deadline: DispatchTime.now() + 0.2) {
            logger.log(level: .business, source: self, featureName: "testWaitForEntry", message: "SECOND", data: nil)
        }
        foundEntry = logger.waitForEntry(intervalUseconds: 10, timeoutSeconds: 0.0001) { entries in
            return entries.last?.asTestString().contains("SECOND") ?? false
        }
        XCTAssertFalse (foundEntry)
        foundEntry = logger.waitForEntry(intervalUseconds: 10, timeoutSeconds: 10.0) { entries in
            return entries.last?.asTestString().contains("SECOND") ?? false
        }
        XCTAssertTrue (foundEntry)
    }
    
    func testQueueIsolation() {
        
        var accessedEmptyCount = 0
        var timeoutCount = 0
        var foundFirstCount = 0
        for counter in 0...9999 {
            usleep(1000)
            let arrayQueue = DispatchQueue (label: "Array\(counter)")
            let workQueue = DispatchQueue (label: "Work\(counter)")
            var i: [String] = []
            workQueue.asyncAfter(deadline: DispatchTime.now() + 0.001) {
                arrayQueue.async {
                    i.append ("FIRST")
                }
            }
            var arrayIsEmpty = true
            let endTime = Date().timeIntervalSince1970 + 10.0
            while arrayIsEmpty && Date().timeIntervalSince1970 < endTime {
                arrayQueue.sync {
                    var item: String? = nil
                    if i.count > 0 {
                        item = i.last
                        if let item = item {
                            if item == "FIRST" {
                                foundFirstCount = foundFirstCount + 1
                            }
                        } else {
                            accessedEmptyCount = accessedEmptyCount + 1
                        }
                        arrayIsEmpty = false
                    }
                }
                usleep(10)
            }
            if arrayIsEmpty {
                timeoutCount = timeoutCount + 1
            }
        }
        XCTAssertEqual (10000, foundFirstCount)
        XCTAssertEqual (0, accessedEmptyCount)
        XCTAssertEqual (0, timeoutCount)
    }
}
