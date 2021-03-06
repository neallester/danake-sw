//
//  utilities.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import XCTest
import Foundation
@testable import danake

let standardCacheName = "myCollection"

class MyClass : Codable {
    
    var myInt = 0
    var myString = ""
    
}

func newTestClassEntity (myInt: Int, myString: String) -> Entity<MyClass> {
    let myClass = MyClass()
    myClass.myInt = myInt
    myClass.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    return Entity (cache: EntityCache<MyClass>(database: database, name: "myCollection"), id: id, version: 0, item: myClass)
    
}

func newTestEntity (myInt: Int, myString: String) -> Entity<MyStruct> {
    var myStruct = MyStruct()
    myStruct.myInt = myInt
    myStruct.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    let cache = EntityCache<MyStruct>(database: database, name: "myCollection")
    return Entity (cache: cache, id: id, version: 0, item: myStruct)
}

func newTestEntitySchema5 (myInt: Int, myString: String) -> Entity<MyStruct> {
    var myStruct = MyStruct()
    myStruct.myInt = myInt
    myStruct.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    let cache = EntityCache<MyStruct>(database: database, name: "myCollection")
    return Entity (cache: cache, id: id, version: 5, item: myStruct)
}


// JSONEncoder uses its own inscrutable rounding process for encoding dates, so this is what is necessary to reliably get the expected value of a date in a json encoded object
func jsonEncodedDate (date: Date) throws -> String? {
    let accessor = InMemoryAccessor()
    struct DateContainer : Encodable {
        init (_ d: Date) {
            self.d = d
        }
        let d: Date
    }
    let container = DateContainer.init(date)
    let encoded = try accessor.encoder.encode (container)
    let protoResult = String (data: encoded, encoding: .utf8)
    var result: String? = nil
    if let protoResult = protoResult {
        result = String (protoResult[protoResult.index (protoResult.startIndex, offsetBy: 5)...])
        result = String (result!.prefix(result!.count - 1))
    }
    return result
}

func msRounded (date: Date) -> Double {
    return (date.timeIntervalSince1970 * 1000).rounded()
}

/*
    This Entity descendant implements the ** timeoutTestingHook() ** function with a semaphore which blocks commit processing
    before the wait which implements the commit timeout in Entity. This enables tests to ensure that all of their test setup
    has comleted before Entity commit processing times out and continues.
*/
class TimeoutHookEntity<T: Codable> : Entity<T> {
    
    internal init (cache: EntityCache<T>, id: UUID, version: Int, item: T, semaphoreValue: Int) {
        self.timeoutSemaphore = DispatchSemaphore (value: semaphoreValue)
        super.init (cache: cache, id: id, version: version, item: item)
    }
    
    required init(from decoder: Decoder) throws {
        timeoutSemaphore = DispatchSemaphore(value: 1)
        try super.init (from: decoder)
    }
    
    internal let timeoutSemaphore: DispatchSemaphore
    
    override
    func timeoutTestingHook() {
        switch timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            timeoutSemaphore.signal()
        default:
            print ("****************************************** timeoutTestingHook.timeoutSemaphore .timedOut")
        }
    }
}

class DateExtensionTests : XCTestCase {
    
    public func testRoughlyEquals () {
        let n   = 1526050509.714
        let n03 = 1526050509.71403
        let n05 = 1526050509.71405
        let n08 = 1526050509.71408
        let n3  = 1526050509.717
        let n8  = 1526050509.722
        let n10 = 1526050509.724
        let n12 = 1526050509.726
        let b   = 1526050529.714
        let d1 = Date (timeIntervalSince1970: n)
        var d2 = Date (timeIntervalSince1970: n)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n03)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n05)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n08)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n3)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n8)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n10)
        XCTAssertTrue (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertTrue (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: n12)
        XCTAssertFalse (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertFalse (d2.roughlyEquals(d1, millisecondPrecision: 10))
        d2 = Date (timeIntervalSince1970: b)
        XCTAssertFalse (d1.roughlyEquals(d2, millisecondPrecision: 10))
        XCTAssertFalse (d2.roughlyEquals(d1, millisecondPrecision: 10))
    }
    
}

extension Date {
    
    func roughlyEquals (_ other: Date, millisecondPrecision: Int) -> Bool {
        let thisMS = Int ((self.timeIntervalSince1970 * 1000).rounded())
        let otherMS = Int ((other.timeIntervalSince1970 * 1000).rounded())
        return (thisMS + millisecondPrecision) >= otherMS && (otherMS + millisecondPrecision) >= thisMS
    }
}

class ExpectedOutput {
    
    static func asString (_ interval: DispatchTimeInterval) -> String {
        #if os(Linux)
            return "\(interval)"
        #else
            return "unknown()"
        #endif

    }
    
}

struct AtomicCounter {
    
    func increment() {
        queue.sync() {
            self._counter.item = self._counter.item + 1
        }
    }
    
    func reset() {
        queue.sync() {
            self._counter.item = 0
        }
    }
    
    var counter: Int {
        get {
            var result = 0
            queue.sync {
                result = _counter.item
            }
            return result
            
        }
        set (newValue) {
            queue.sync {
                self._counter.item = newValue
            }
        }
    }
    
    private var _counter = IntegerContainer()
    
    private let queue = DispatchQueue (label: "AtomicInteger")
    
    private class IntegerContainer {
        var item = 0
    }
}


