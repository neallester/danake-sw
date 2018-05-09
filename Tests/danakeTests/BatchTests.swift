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
        batch.insertAsync(entity: entity, closure: nil)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (myStruct) in
            XCTAssertEqual(10, myStruct.myInt)
            XCTAssertEqual("Test Completed", myStruct.myString)
        }
        batch.insertAsync(entity: entity, closure: nil)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (myStruct) in
            XCTAssertEqual(10, myStruct.myInt)
            XCTAssertEqual("Test Completed", myStruct.myString)
        }
        let entity2 = newTestEntity(myInt: 0, myString: "")
        batch.insertAsync(entity: entity2, closure: nil)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(2, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            XCTAssertEqual(0, entity2.getVersion())
            var retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
            retrievedEntity = entities[entity2.id]! as! Entity<MyStruct>
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
        batch.insertAsync(entity: entity) { () in
            myClass!.myInt = 20
            myClass!.myString = "String Modified"
        }
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.id]! as! Entity<MyClass>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.sync() { (entityClass) in
            XCTAssertEqual(20, entityClass.myInt)
            XCTAssertEqual("String Modified", entityClass.myString)
        }
        batch.insertAsync(entity: entity) { () in
            myClass!.myInt = 30
            myClass!.myString = "String Modified Again"
        }
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.id]! as! Entity<MyClass>
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
        batch.insertAsync(entity: entity2) {
            myClass2!.myInt = 40
            myClass2!.myString = "Second Class Modified"
        }
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(2, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            XCTAssertEqual(0, entity2.getVersion())
            var retrievedEntity = entities[entity.id]! as! Entity<MyClass>
            XCTAssertTrue (entity === retrievedEntity)
            retrievedEntity = entities[entity2.id]! as! Entity<MyClass>
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
        let collection = PersistentCollection<MyStruct>(database: database, name: collectionName)
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
        XCTAssertTrue (accessor.has(name: collectionName, id: entity1.id))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity2.id))
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
    }
    
    func testCommitWithUnrecoverableError() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<MyStruct>(database: database, name: collectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let delegateId = batch.delegateId()
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity1Id = entity1.id.uuidString
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
        XCTAssertFalse (accessor.has(name: collectionName, id: entity1.id))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity2.id))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|BatchDelegate.commit|Database.unrecoverableError(\"addActionError\")|entityType=Entity<MyStruct>;entityId=\(entity1.id.uuidString);batchId=\(delegateId.uuidString)", entries[0].asTestString())
        }
    }

    func testCommitWithError() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<MyStruct>(database: database, name: collectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let delegateId = batch.delegateId()
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = collection.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        let id1 = entity1.id.uuidString
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
        XCTAssertTrue (accessor.has(name: collectionName, id: entity1.id))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity2.id))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("EMERGENCY|BatchDelegate.commit|Database.error(\"addError\")|entityType=Entity<MyStruct>;entityId=\(entity1.id.uuidString);batchId=\(delegateId.uuidString)", entries[0].asTestString())
        }
    }
    
    class SlowCodable : Codable {
        
        init () {
            switch semaphore.wait(timeout: DispatchTime.now() + 10) {
            case .success:
                break
            default:
                XCTFail("Expected .success")
            }
        }
        
        enum SlowCodableError : Error {
            case error
        }
        
        public func encode(to encoder: Encoder) throws {
            if !hasFired {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    semaphore.signal()
                default:
                    XCTFail("Expected .success")
                }
            }
            hasFired = true
        }
        
        public required init (from decoder: Decoder) throws {}
        
        private var hasFired = false
        internal var semaphore = DispatchSemaphore(value: 1)
    }
    
    func testCommitWithBatchTimeout() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<MyStruct>(database: database, name: collectionName)
        let slowCollectionName: CollectionName = "slowCollection"
        let slowCollection = PersistentCollection<SlowCodable>(database: database, name: slowCollectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(400000), timeout: .microseconds (100000), logger: logger)
        let batchDelegateId = batch.delegateId().uuidString
        let entity1 = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let slowCodable = SlowCodable()
        let entity2 = slowCollection.new (batch: batch, item: slowCodable)
        let waitFor = expectation (description: "waitFor")
        batch.commit() {
            waitFor.fulfill()
            slowCodable.semaphore.signal()
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
            XCTFail ("Expected .persistent but got \(entity2.getPersistenceState())")
        }
        XCTAssertEqual (1, accessor.count (name: collectionName))
        XCTAssertTrue (accessor.has(name: collectionName, id: entity1.id))
        XCTAssertFalse (accessor.has(name: collectionName, id: entity2.id))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|BatchDelegate.commit|batchTimeout|batchId=\(batchDelegateId);entityType=Entity<SlowCodable>;entityId=\(entity2.id.uuidString);diagnosticHint=Entity.queue is blocked or endless loop in Entity serialization", entries[0].asTestString())
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
    
    func testNoCommitLogging() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<MyStruct>(database: database, name: collectionName)
        var batch: EventuallyConsistentBatch? = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let entity1 = collection.new (batch: batch!, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = collection.new (batch: batch!, item: MyStruct(myInt: 20, myString: "20"))
        batch!.syncEntities() { entities in
            // Also forces this thread to wait on the batch queue
            XCTAssertEqual (2, entities.count)
        }
        batch = nil
        let entity1IdString = entity1.id.uuidString
        let entity2IdString = entity2.id.uuidString
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            var entryStrings = entries[0].asTestString()
            entryStrings.append(entries[1].asTestString())
            XCTAssertTrue (entryStrings.contains("ERROR|BatchDelegate.deinit|notCommitted:lostData|entityType=Entity<MyStruct>;entityId=\(entity1IdString);entityPersistenceState=new"))
            XCTAssertTrue (entryStrings.contains("ERROR|BatchDelegate.deinit|notCommitted:lostData|entityType=Entity<MyStruct>;entityId=\(entity2IdString);entityPersistenceState=new"))
        }
    }
    
    func testCommitSync() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<MyStruct>(database: database, name: collectionName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let _ = collection.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let _ = collection.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        batch.commitSync()
        XCTAssertEqual (2, accessor.count(name: collectionName))
    }
    
    // Random timeouts in commit
    public func testRandomTimeout() {
        var testCount = 0
        var totalExecutionTime = 0.0
        var delay = 0.0
        var timeout = BatchDefaults.timeout
        var batchTimeoutCount = 0
        var entityTimeoutCount = 0
        var succeededEventuallyCount = 0
        var noTimeoutCount = 0
        while testCount < 1000 {
            let logger = InMemoryLogger(level: .warning)
            let accessor = InMemoryAccessor()
            var needsDelay = true
            var startTime = Date()
            if testCount > 0 {
                delay = totalExecutionTime / Double (testCount)
                let delayAt = ParallelTests.randomInteger(maxValue: Int (1000000 * delay))
                timeout = .microseconds(Int (delay * 600000.0))
                accessor.setPreFetch() { uuid in
                    if needsDelay && Int ((1000000 * (Date().timeIntervalSince1970 - startTime.timeIntervalSince1970))) > delayAt {
                        usleep(UInt32 (delay * 1000000.0))
                        needsDelay = false
                    }
                }
            }
            let persistenceObjects = ParallelTestPersistence (accessor: accessor, logger: logger)
            startTime = Date()
            let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: timeout, logger: persistenceObjects.logger)
            let structs = ParallelTests.newStructs (persistenceObjects: persistenceObjects, batch: batch)
            let result = structs.ids
            batch.commitSync()
            let endTime = Date()
            totalExecutionTime = totalExecutionTime + endTime.timeIntervalSince1970 - startTime.timeIntervalSince1970 - delay
            var counter = 1
            var batchTimedOut = false
            var entityTimedOut = false
            logger.sync() { entries in
                for entry in entries {
                    batchTimedOut = batchTimedOut || entry.message.contains ("batchTimeout")
                    entityTimedOut = entityTimedOut || entry.message.contains ("Entity.commit():timedOut")
                }
            }
            if batchTimedOut {
                batchTimeoutCount = batchTimeoutCount + 1
            }
            if entityTimedOut {
                entityTimeoutCount = entityTimeoutCount + 1
                if !batchTimedOut {
                    succeededEventuallyCount = succeededEventuallyCount + 1
                }
            }
            if !batchTimedOut && !entityTimedOut {
                noTimeoutCount = noTimeoutCount + 1
            }
            for uuid in result {
                let entity = persistenceObjects.myStructCollection.get(id: uuid).item()!
                entity.sync { myStruct in
                    let expectedInt = counter * 10
                    XCTAssertEqual (expectedInt, myStruct.myInt)
                    XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                }
                if !batchTimedOut {
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                }
                counter = counter + 1
            }
            testCount = testCount + 1
        }
        // Technically speaking the occurrence of each scenario is dependent on factors which are randomized in the
        // Test so it is conceivable one of the following assertions could fail simply due to a random occurrence
        XCTAssertTrue (batchTimeoutCount > 0)
        XCTAssertTrue (entityTimeoutCount > 0)
        XCTAssertTrue (succeededEventuallyCount > 0)
        XCTAssertTrue (noTimeoutCount > 0)
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
