//
//  BatchTests.swift
//  danakeTests
//
//  Created by Neal Lester on 1/26/18.
//

import XCTest
@testable import danake

class BatchTests: XCTestCase {
    
    override func setUp() {
        BacktraceInstallation.install()
    }

    func testInsertAsyncNoClosure() {
        // No Closure
        let batch = EventuallyConsistentBatch()
        let entity = newTestEntity(myInt: 10, myString: "Test Completed")
        batch.insertAsync(entity: entity, closure: nil)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
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
            XCTAssertEqual(0, entity.version)
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
            XCTAssertEqual(0, entity.version)
            XCTAssertEqual(0, entity2.version)
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
            XCTAssertEqual(0, entity.version)
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
            XCTAssertEqual(0, entity.version)
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
            XCTAssertEqual(0, entity.version)
            XCTAssertEqual(0, entity2.version)
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
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let batch = EventuallyConsistentBatch()
        let entity1 = cache.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = cache.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        let waitFor = expectation (description: "waitFor")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch entity1.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch entity2.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (2, accessor.count (name: cacheName))
        XCTAssertTrue (accessor.has(name: cacheName, id: entity1.id))
        XCTAssertTrue (accessor.has(name: cacheName, id: entity2.id))
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
    }
    
    func testCommitWithUnrecoverableError() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let delegateId = batch.delegateId()
        let entity1 = cache.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity1Id = entity1.id.uuidString
        let entity2 = cache.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
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
        switch entity1.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        switch entity2.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (1, accessor.count (name: cacheName))
        XCTAssertFalse (accessor.has(name: cacheName, id: entity1.id))
        XCTAssertTrue (accessor.has(name: cacheName, id: entity2.id))
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|BatchDelegate.commit|Database.unrecoverableError(\"addActionError\")|entityType=Entity<MyStruct>;entityId=\(entity1.id.uuidString);batchId=\(delegateId.uuidString)", entries[0].asTestString())
        }
    }

    func testCommitWithError() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let delegateId = batch.delegateId()
        let entity1 = cache.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = cache.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
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
        switch entity1.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch entity2.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (2, accessor.count (name: cacheName))
        XCTAssertTrue (accessor.has(name: cacheName, id: entity1.id))
        XCTAssertTrue (accessor.has(name: cacheName, id: entity2.id))
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
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let slowCacheName: CacheName = "slowCollection"
        let slowCollection = EntityCache<SlowCodable>(database: database, name: slowCacheName)
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(400000), timeout: .microseconds (100000), logger: logger)
        let batchDelegateId = batch.delegateId().uuidString
        let entity1 = cache.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let slowCodable = SlowCodable()
        let entity2 = slowCollection.new (batch: batch, item: slowCodable)
        let waitFor = expectation (description: "waitFor")
        batch.commit() {
            slowCodable.semaphore.signal()
            entity2.sync() { slowCodable in }
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        batch.syncEntities() { entities in }
        let timeout = Date().timeIntervalSince1970 + 10
        var isPersistent = false
        while !isPersistent && Date().timeIntervalSince1970 < timeout {
            switch entity1.persistenceState {
            case .persistent:
                isPersistent = true
            default:
                break
            }
            if isPersistent {
                switch entity2.persistenceState {
                case .persistent:
                    isPersistent = true
                default:
                    isPersistent = false
                }
            }
            if !isPersistent {
                Thread.sleep(forTimeInterval: (0.00001))
            }
        }
        switch entity1.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent  but got \(entity1.persistenceState)")
        }
        switch entity2.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent but got \(entity2.persistenceState)")
        }
        XCTAssertEqual (1, accessor.count (name: cacheName))
        XCTAssertTrue (accessor.has(name: cacheName, id: entity1.id))
        XCTAssertFalse (accessor.has(name: cacheName, id: entity2.id))
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
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        var batch: EventuallyConsistentBatch? = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let entity1 = cache.new (batch: batch!, item: MyStruct(myInt: 10, myString: "10"))
        let entity2 = cache.new (batch: batch!, item: MyStruct(myInt: 20, myString: "20"))
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
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let batch = EventuallyConsistentBatch(retryInterval: .milliseconds(1), timeout: .seconds (20), logger: logger)
        let _ = cache.new (batch: batch, item: MyStruct(myInt: 10, myString: "10"))
        let _ = cache.new (batch: batch, item: MyStruct(myInt: 20, myString: "20"))
        batch.commitSync()
        XCTAssertEqual (2, accessor.count(name: cacheName))
    }
    
    // Random timeouts in commit
    public func testRandomTimeout() {
        var testCount = 0
        var totalExecutionTime = 0.0
        var batchTimeoutCount = 0
        var entityTimeoutCount = 0
        var succeededEventuallyCount = 0
        var noTimeoutCount = 0
        while testCount < 3000 {
            let logger = InMemoryLogger(level: .warning)
            logger.log(level: .emergency, source: self, featureName: "", message: "Start: \(testCount)")
            let accessor = InMemoryAccessor()
            var needsDelay = ParallelTest.randomInteger(maxValue: 100) < 50
            var hasDelayed = false
            var startTime = Date()
            var maxValue = 800
            #if os(Linux)
                if (testCount % 200) == 0 {
                    maxValue = 20
                } else {
                    maxValue = maxValue * 20
                }
            #endif
            // Some delay should end before entity times out (at timeout) (no timeout)
            // Some delay should end after entity times out but before batch times out (eventually swucceeded)
            // Some delay shoud end after batch times out (batcvh timeout)
            // Increasing maxValue seems to make the numbers less random and more ven, probably because
            // It reduces the impact of variations in context switching time
            var timeoutUs = Int (ParallelTest.randomInteger(maxValue: Int (maxValue)))
            if timeoutUs == 0 {
                timeoutUs = 2
            }
            let timeout = DispatchTimeInterval.microseconds(timeoutUs);
            let delay = Int (Double (timeoutUs) / 3) + ParallelTest.randomInteger(maxValue: timeoutUs)
            accessor.setPreFetch() { uuid in
                if needsDelay {
                    usleep(UInt32 (delay))
                    needsDelay = false
                    hasDelayed = true
                } else if (!hasDelayed) {
                    needsDelay = true
                }
            }
            let persistenceObjects = ParallelTestPersistence (accessor: accessor, logger: logger)
            startTime = Date()
            let batch = EventuallyConsistentBatch(retryInterval: .microseconds(5), timeout: timeout, logger: persistenceObjects.logger)
            let structs = ParallelTest.newStructs (persistenceObjects: persistenceObjects, batch: batch)
            let result = structs.ids
            batch.commitSync()
            let endTime = Date()
            totalExecutionTime = totalExecutionTime + endTime.timeIntervalSince1970 - startTime.timeIntervalSince1970 - (Double (delay) / 1000000.0)
            var batchTimedOut = false
            var entityTimedOut = false
            var entryCount = 0
            logger.log(level: .emergency, source: self, featureName: "", message: "End: \(testCount)")
            logger.sync() { entries in
                
                for entry in entries {
                    entryCount = entryCount + 1
                    batchTimedOut = batchTimedOut || entry.message.contains ("batchTimeout")
                    entityTimedOut = entityTimedOut || entry.message.contains ("timeout")
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
            var counter = 1
            var unexpectedStateFound = false
            for uuid in result {
                do {
                    let entity = try persistenceObjects.myStructCollection.getSync(id: uuid)
                    entity.sync { myStruct in
                        let expectedInt = counter * 10
                        XCTAssertEqual (expectedInt, myStruct.myInt)
                        XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                    }
                    if !batchTimedOut {
                        let state = entity.persistenceState
                        switch state {
                        case .persistent:
                            break
                        default:
                            unexpectedStateFound = true
                            print ("Entity: \(entity.id.uuidString); Counter \(counter): Expected .persistent but got .\(state)")
                        }
                    }
                    counter = counter + 1
                } catch {
                    XCTFail("Expected success but got \(error)")
                }
            }
            if unexpectedStateFound {
                print ("Batch Start Time:     \(startTime.timeIntervalSince1970)")
                print ("Batch End Time:       \(endTime.timeIntervalSince1970)")
                print ("Now:                  \(Date().timeIntervalSince1970)")
                print ("Total Execution Time: \(totalExecutionTime)")
                switch timeout {
                case .microseconds(let ms):
                    print ("Timeout Microseconds: \(ms)")
                default:
                    print ("Unexpected timeout denomination")
                }
                
                print ("Test Count: \(testCount)")
                print ("Batch Timed Out: \(batchTimedOut)")
                print ("Entity Timed Out: \(entityTimedOut)")
                logger.sync() { entries in
                    for entry in entries {
                        print (entry.asTestString())
                    }
                }
            }
            XCTAssertFalse(unexpectedStateFound)
            testCount = testCount + 1
        }
        // Technically speaking the occurrence of each scenario is dependent on factors which are randomized in the
        // Test so it is conceivable one of the following assertions could fail simply due to a random occurrence
        print ("batchTimeouts: \(batchTimeoutCount); entityTimeoutCount: \(entityTimeoutCount); eventuallyCount: \(succeededEventuallyCount)")
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
