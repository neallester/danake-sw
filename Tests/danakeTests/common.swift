//
//  utilities.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import XCTest
import Foundation
@testable import danake

let standardCollectionName = "myCollection"

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
    return Entity (collection: PersistentCollection<MyClass>(database: database, name: "myCollection"), id: id, version: 0, item: myClass)
    
}

struct MyStruct : Codable {
    
    var myInt = 0
    var myString = ""
    
}

func newTestEntity (myInt: Int, myString: String) -> Entity<MyStruct> {
    var myStruct = MyStruct()
    myStruct.myInt = myInt
    myStruct.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    let collection = PersistentCollection<MyStruct>(database: database, name: "myCollection")
    return Entity (collection: collection, id: id, version: 0, item: myStruct)
}

func newTestEntitySchema5 (myInt: Int, myString: String) -> Entity<MyStruct> {
    var myStruct = MyStruct()
    myStruct.myInt = myInt
    myStruct.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    let collection = PersistentCollection<MyStruct>(database: database, name: "myCollection")
    return Entity (collection: collection, id: id, version: 5, item: myStruct)
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
    
    internal init (collection: PersistentCollection<T>, id: UUID, version: Int, item: T, semaphoreValue: Int) {
        self.timeoutSemaphore = DispatchSemaphore (value: semaphoreValue)
        super.init (collection: collection, id: id, version: version, item: item)
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

internal class MyStructContainer : Codable {
    
    init (parentData: EntityReferenceData<MyStructContainer>, myStruct: Entity<MyStruct>?) {
        self.myStruct = EntityReference<MyStructContainer, MyStruct> (parent: parentData, entity: myStruct)
    }
    
    init (parentData: EntityReferenceData<MyStructContainer>, structData: EntityReferenceSerializationData?) {
        self.myStruct = EntityReference<MyStructContainer, MyStruct> (parent: parentData, referenceData: structData)
    }
    
    let myStruct: EntityReference<MyStructContainer, MyStruct>
}

internal class ContainerCollection : PersistentCollection<MyStructContainer> {
    
    func new(batch: EventuallyConsistentBatch, myStruct: Entity<MyStruct>?) -> Entity<MyStructContainer> {
        return new (batch: batch) { parentData in
            return MyStructContainer (parentData: parentData, myStruct: myStruct)
        }
    }
    
    func new(batch: EventuallyConsistentBatch, structData: EntityReferenceSerializationData?) -> Entity<MyStructContainer> {
        return new (batch: batch) { parentData in
            return MyStructContainer (parentData: parentData, structData: structData)
        }
    }
    
}

internal class ParallelTestPersistence {
    
    init (accessor: DatabaseAccessor, logger: Logger?) {
        self.logger = logger
        let database = Database (accessor: accessor, schemaVersion: 1, logger: logger, referenceRetryInterval: 0.000001)
        myStructCollection = PersistentCollection<MyStruct> (database: database, name: "MyStructs")
        containerCollection = ContainerCollection (database: database, name: "myContainerCollection")
    }
    
    deinit {
        if let logger = logger {
            logger.log(level: .debug, source: "", featureName: "deinit", message: "start", data: nil)
            var myStructCount = 0
            var containerCount = 0
            myStructCollection.sync() { entities in
                myStructCount = entities.count
            }
            containerCollection.sync() { entities in
                containerCount = entities.count
            }
            logger.log(level: .debug, source: "", featureName: "deinit", message: "myStructCollection", data: [(name: "count", value: myStructCount)])
            logger.log(level: .debug, source: "", featureName: "deinit", message: "containerCollection", data: [(name: "count", value: containerCount)])
        }
    }
    
    let logger: Logger?
    
    let myStructCollection: PersistentCollection<MyStruct>
    
    let containerCollection: ContainerCollection
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
        print ("\(Date().timeIntervalSince1970))")
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


