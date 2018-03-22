//
//  BatchTests.swift
//  danakeTests
//
//  Created by Neal Lester on 1/26/18.
//

import XCTest
@testable import danake

class BatchTests: XCTestCase {
    

    func testInsertAsyncNoClosure() {
        // No Closure
        let batch = EventuallyConsistentBatch()
        let entity = newTestEntity(myInt: 10, myString: "Test Completed")
        batch.insertAsync(item: entity, closure: nil)
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (myStruct) in
            XCTAssertEqual(10, myStruct.myInt)
            XCTAssertEqual("Test Completed", myStruct.myString)
        }
        batch.insertAsync(item: entity, closure: nil)
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (myStruct) in
            XCTAssertEqual(10, myStruct.myInt)
            XCTAssertEqual("Test Completed", myStruct.myString)
        }
        let entity2 = newTestEntity(myInt: 0, myString: "")
        batch.insertAsync(item: entity2, closure: nil)
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(2, items.count)
            XCTAssertEqual(0, entity.getVersion())
            XCTAssertEqual(0, entity2.getVersion())
            var retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
            retrievedEntity = items[entity2.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity2 === retrievedEntity)
        }
        entity.sync() { (myStruct) in
            XCTAssertEqual(10, myStruct.myInt)
            XCTAssertEqual("Test Completed", myStruct.myString)
        }
    }

    func testInsertAsyncWithClosure() {
        let batch = EventuallyConsistentBatch()
        let entity = newTestClassEntity(myInt: 10, myString: "Test Started")
        var myClass: MyClass? = nil
        entity.sync () { item in
            myClass = item
        }
        batch.insertAsync(item: entity) { () in
            myClass!.myInt = 20
            myClass!.myString = "String Modified"
        }
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyClass>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (entityClass) in
            XCTAssertEqual(20, entityClass.myInt)
            XCTAssertEqual("String Modified", entityClass.myString)
        }
        batch.insertAsync(item: entity) { () in
            myClass!.myInt = 30
            myClass!.myString = "String Modified Again"
        }
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyClass>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (myClass) in
            XCTAssertEqual(30, myClass.myInt)
            XCTAssertEqual("String Modified Again", myClass.myString)
        }
        let entity2 = newTestClassEntity(myInt: 0, myString: "")
        var myClass2: MyClass? = nil
        entity2.sync() { item in
            myClass2 = item
        }
        batch.insertAsync(item: entity2) {
            myClass2!.myInt = 40
            myClass2!.myString = "Second Class Modified"
        }
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(2, items.count)
            XCTAssertEqual(0, entity.getVersion())
            XCTAssertEqual(0, entity2.getVersion())
            var retrievedEntity = items[entity.getId()]! as! Entity<MyClass>
            XCTAssertTrue (entity === retrievedEntity)
            retrievedEntity = items[entity2.getId()]! as! Entity<MyClass>
            XCTAssertTrue (entity2 === retrievedEntity)
        }
        entity.sync() { (myClass) in
            XCTAssertEqual(30, myClass.myInt)
            XCTAssertEqual("String Modified Again", myClass.myString)
        }
        entity2.sync() { (myClass) in
            XCTAssertEqual(40, myClass.myInt)
            XCTAssertEqual("Second Class Modified", myClass.myString)
        }
    }
    
    func testCommit() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let batch = EventuallyConsistentBatch()
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = collection.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        let waitFor = expectation (description: "waitFor")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch entity1.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch entity2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (2, accessor.count (name: collectionName))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity1.getId()))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity2.getId()))
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
    }
    
    func testCommitWithUnrecoverableError() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let delegateId = batch.delegateId()
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity1Id = entity1.getId().uuidString
        let entity2 = collection.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        let waitFor = expectation (description: "waitFor")
        let prefetch: (UUID) -> () = { id in
            if id.uuidString == entity1Id {
                accessor.throwError = true
            }
        }
        accessor.setPreFetch(prefetch)
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch entity1.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        switch entity2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (1, accessor.count (name: collectionName))
        XCTAssertFalse (accessor.has(name: collectionName, id: entity1.getId()))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity2.getId()))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|(BatchDelegate in _5AC4B1DA02E994E5118286AB05909266).commit|Database.unrecoverableError(\"Test Error\")|entityType=Entity<MyStruct>;entityId=\(entity1.getId().uuidString);batchId=\(delegateId.uuidString)", entries[0].asTestString())
        }
    }

    func testCommitWithError() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let delegateId = batch.delegateId()
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = collection.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        let id1 = entity1.getId().uuidString
        let waitFor = expectation (description: "waitFor")
        var preFetchCount = 0
        let prefetch: (UUID) -> () = { id in
            if id.uuidString == id1 {
                if preFetchCount == 1 {
                    accessor.throwError = true
                }
                preFetchCount = preFetchCount + 1
            }

        }
        accessor.setPreFetch(prefetch)
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch entity1.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch entity2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (2, accessor.count (name: collectionName))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity1.getId()))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity2.getId()))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("EMERGENCY|(BatchDelegate in _5AC4B1DA02E994E5118286AB05909266).commit|Database.error(\"Test Error\")|entityType=Entity<MyStruct>;entityId=\(entity1.getId().uuidString);batchId=\(delegateId.uuidString)", entries[0].asTestString())
        }
    }

    
    class SlowCodable : Codable {
        
        enum SlowCodableError : Error {
            case error
        }
        
        public func encode(to encoder: Encoder) throws {
            if !hasFired {
                usleep(100000)
            }
            hasFired = true
        }
        
        public init () {}
        
        public required init (from decoder: Decoder) throws {
        }

        private var hasFired = false
    }
    
    func testCommitWithBatchTimeout() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let slowCollectionName: CollectionName = "slowCollection"
        let slowCollection = PersistentCollection<Database, SlowCodable>(database: database, name: slowCollectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(130000), timeout: .microseconds (50000), logger: logger)
        let batchDelegateId = batch.delegateId().uuidString
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let slowCodable = SlowCodable()
        let entity2 = slowCollection.new (batch: batch, item: slowCodable)
        let waitFor = expectation (description: "waitFor")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch entity1.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch entity2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (1, accessor.count (name: collectionName))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity1.getId()))
        XCTAssertFalse (accessor.has(name: collectionName, id: entity2.getId()))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|(BatchDelegate in _5AC4B1DA02E994E5118286AB05909266).commit|batchTimeout|batchId=\(batchDelegateId);entityType=Entity<SlowCodable>;entityId=\(entity2.getId().uuidString);diagnosticHint=Entity.queue is blocked or endless loop in Entity serialization", entries[0].asTestString())
        }
    }
    
    func testDispatchTimeIntervalExtension() {
        var interval: DispatchTimeInterval = .seconds (1)
        var result = interval.multipliedBy (2)
        switch result {
        case .seconds(let value):
            XCTAssertEqual (2, value)
        default:
            XCTFail ("Expected .seconds")
        }
        interval = .milliseconds (2)
        result = interval.multipliedBy (2)
        switch result {
        case .milliseconds(let value):
            XCTAssertEqual (4, value)
        default:
            XCTFail ("Expected .milliseconds")
        }
        interval = .microseconds (3)
        result = interval.multipliedBy (2)
        switch result {
        case .microseconds(let value):
            XCTAssertEqual (6, value)
        default:
            XCTFail ("Expected .microseconds")
        }
        interval = .nanoseconds (4)
        result = interval.multipliedBy (2)
        switch result {
        case .nanoseconds(let value):
            XCTAssertEqual (8, value)
        default:
            XCTFail ("Expected .nanoseconds")
        }
    }
    
/*
     The following test verifies that a DispatchQueue declared as a local will fire even if
     the local has been collected. It emits the following output:
     
     ******************** Queue Fired
     ******************** Expecation fullfilled

*/
//    public func testDispatchQueueCollection() {
//        var queue: DispatchQueue? = DispatchQueue (label: "Test")
//        fullFill(queue: queue!)
//        queue = nil
//        let expecation = expectation(description: "wait")
//        fullFill(expectation: expecation)
//        waitForExpectations(timeout: 10.0, handler: nil)
//    }
//
//    func fullFill (expectation: XCTestExpectation) {
//        let queue = DispatchQueue (label: "Fullfill")
//        queue.asyncAfter(deadline: DispatchTime.now() + 2.0) {
//            expectation.fulfill()
//            print ("******************** Expecation fullfilled")
//        }
//    }
//
//    func fullFill (queue: DispatchQueue) {
//        queue.asyncAfter(deadline: DispatchTime.now() + 1.0) {
//            print ("******************** Queue Fired")
//        }
//    }

}
