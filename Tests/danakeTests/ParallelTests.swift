//
//  ParallelTests.swift
//  danakeTests
//
//  Created by Neal Lester on 4/14/18.
//

import XCTest
@testable import danake

class ParallelTests: XCTestCase {

    func testParallel() {
        let accessor = InMemoryAccessor()
        ParallelTests.performTest(accessor: accessor, repetitions: 100)
    }

    public static func performTest(accessor: DatabaseAccessor, repetitions: Int) {
        var testCount = 0
        var totalExecutionTime = 0.008
        while testCount < repetitions {
            let testGroup = DispatchGroup()
            var test1Results: [[UUID]] = []
            var test2Results: [[UUID]] = []
            var test3Results: [[UUID]] = []
            var test4Results: [[UUID]] = []
            var test100Results: [[UUID]] = []
            var test100aResults: [[UUID]] = []
            var test100nResults: [[UUID]] = []
            var test100naResults: [[UUID]] = []
            let resultQueue = DispatchQueue (label: "results")
            let testDispatcher = DispatchQueue (label: "testDispatcher", attributes: .concurrent)
            var persistenceObjects = ParallelTestPersistence (accessor: accessor)
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testStart", data: [(name: "testCount", value: testCount), (name: "separator", value:"<<<<<<<<<<<<<<<<<<<<<<<")])
            let setupBatch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
            let setupGroup = DispatchGroup()
            let removeExisting = ParallelTests.newStructs (persistenceObjects: persistenceObjects, batch: setupBatch).ids
            test2Results.append (removeExisting)
            let editExisting = ParallelTests.newStructs (persistenceObjects: persistenceObjects, batch: setupBatch).ids
            test3Results.append (editExisting)
            setupGroup.enter()
            setupBatch.commit() {
                setupGroup.leave()
            }
            setupGroup.wait()
            persistenceObjects = ParallelTestPersistence (accessor: accessor)
            let test1 = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test1.start", data: nil)
                let result = ParallelTests.myStructTest1(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test1.end", data: nil)
                resultQueue.async {
                    test1Results.append (result)
                }
            }
            let test2 = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2.start", data: nil)
                let result = ParallelTests.myStructTest2(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2.end", data: nil)
                resultQueue.async {
                    test2Results.append (result)
                }
                
            }
            let test2p = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2p.start", data: nil)
                let result = ParallelTests.myStructTest2p(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2p.end", data: nil)
                resultQueue.async {
                    test2Results.append (result)
                }
                
            }
            let test2r = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2r.start", data: nil)
                ParallelTests.myStructTest2r(persistenceObjects: persistenceObjects, group: testGroup, ids: removeExisting)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2r.end", data: nil)
            }
            let test3 = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3.start", data: nil)
                let result = ParallelTests.myStructTest3(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3.end", data: nil)
                resultQueue.async {
                    test3Results.append (result)
                }
            }
            let test3p = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3p.start", data: nil)
                let result = ParallelTests.myStructTest3p(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3p.end", data: nil)
                resultQueue.async {
                    test3Results.append (result)
                }
            }
            let test3r = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3r.start", data: nil)
                ParallelTests.myStructTest3r(persistenceObjects: persistenceObjects, group: testGroup, ids: editExisting)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3r.end", data: nil)
            }
            let test4 = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4.start", data: nil)
                let result = ParallelTests.myStructTest4(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4.end", data: nil)
                resultQueue.async {
                    test4Results.append (result)
                }
            }
            let test4b = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4b.start", data: nil)
                let result = ParallelTests.myStructTest4b(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4b.end", data: nil)
                resultQueue.async {
                    test4Results.append (result)
                }
            }
            let test100 = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100.start", data: nil)
                let result = ParallelTests.containerTest100(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100.end", data: nil)
                resultQueue.async {
                    test100Results.append (result)
                }
            }
            let test100a = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100a.start", data: nil)
                let result = ParallelTests.containerTest100(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100a.end", data: nil)
                resultQueue.async {
                    test100aResults.append (result)
                }
            }
            let test100n = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100n.start", data: nil)
                let result = ParallelTests.containerTest100n(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100n.end", data: nil)
                resultQueue.async {
                    test100nResults.append (result)
                }
            }
            let test100na = {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100na.start", data: nil)
                let result = ParallelTests.containerTest100n(persistenceObjects: persistenceObjects, group: testGroup)
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100na.end", data: nil)
                resultQueue.async {
                    test100naResults.append (result)
                }
            }
            var tests = [test1, test2, test2p, test2r, test3, test3p, test3r, test4, test4b, test100, test100a, test100n, test100na]
            var randomTests: [() -> ()] = []
            while tests.count > 0 {
                let itemToRemove = Int (arc4random_uniform(UInt32(tests.count)))
                randomTests.append (tests[itemToRemove])
                let _ = tests.remove(at: itemToRemove)
            }
            var finalTests: [() -> ()] = []
            if let inMemoryAccessor = accessor as? InMemoryAccessor, (arc4random_uniform(5) >= 0) {
                inMemoryAccessor.setThrowOnlyRecoverableErrors (true)
                var errorCounter = 0
                while errorCounter < 5 {
                    let newTest = {
                        let maxSleepTime = totalExecutionTime / (Double (testCount) + 1.0)
                        let sleepTime = arc4random_uniform(UInt32 ((1000000 * maxSleepTime)))
                        usleep (sleepTime)
                        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "setThrowError", data: nil)
                        inMemoryAccessor.setThrowError()
                        testGroup.leave()
                    }
                    finalTests.append (newTest)
                    errorCounter = errorCounter + 1
                }
            }
            finalTests.append (contentsOf: randomTests)
            for test in finalTests {
                testGroup.enter()
                testDispatcher.async(execute: test)
            }
            let executionStart = Date()
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "waiting", data: [(name: "testCount", value: testCount)])
            testGroup.wait()
            totalExecutionTime = totalExecutionTime + Date().timeIntervalSince1970 - executionStart.timeIntervalSince1970
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "waiting.finished", data: [(name: "testCount", value: testCount), (name:"separator", value: "================================")])
            if let inMemoryAccessor = accessor as? InMemoryAccessor {
                inMemoryAccessor.setThrowError(false)
            }
            persistenceObjects = ParallelTestPersistence (accessor: accessor)
            // Test 1
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.1", data: nil)
            for testResult in test1Results {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.myStructCollection.get(id: uuid).item()!
                    entity.sync { myStruct in
                        let expectedInt = counter * 10
                        XCTAssertEqual (expectedInt, myStruct.myInt)
                        XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            for testResult in test1Results {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.myStructCollection.get(id: uuid).item()!
                    entity.sync { myStruct in
                        let expectedInt = counter * 10
                        XCTAssertEqual (expectedInt, myStruct.myInt)
                        XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            // Test 2
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.2", data: nil)
            for testResult in test2Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                for uuid in testResult {
                    XCTAssertNil (persistenceObjects.myStructCollection.get(id: uuid).item())
                }
            }
            for testResult in test2Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                for uuid in testResult {
                    XCTAssertNil (persistenceObjects.myStructCollection.get(id: uuid).item())
                }
            }
            // Test 3
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.3", data: nil)
            for testResult in test3Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.myStructCollection.get(id: uuid).item()!
                    entity.sync { myStruct in
                        let expectedInt = counter * 100
                        XCTAssertEqual (expectedInt, myStruct.myInt)
                        XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            for testResult in test3Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.myStructCollection.get(id: uuid).item()!
                    entity.sync { myStruct in
                        let expectedInt = counter * 100
                        XCTAssertEqual (expectedInt, myStruct.myInt)
                        XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            // Test 4
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.4", data: nil)
            for testResult in test4Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    if let entity = persistenceObjects.myStructCollection.get(id: uuid).item() {
                        entity.sync { myStruct in
                            let expectedInt = counter * 100
                            XCTAssertEqual (expectedInt, myStruct.myInt)
                            XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                        }
                        switch entity.getPersistenceState() {
                        case .persistent:
                            break
                        default:
                            XCTFail ("Expected .persistent")
                        }
                    }
                    counter = counter + 1
                }
            }
            for testResult in test4Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    if let entity = persistenceObjects.myStructCollection.get(id: uuid).item() {
                        entity.sync { myStruct in
                            let expectedInt = counter * 100
                            XCTAssertEqual (expectedInt, myStruct.myInt)
                            XCTAssertEqual ("\(expectedInt)", myStruct.myString)
                        }
                        switch entity.getPersistenceState() {
                        case .persistent:
                            break
                        default:
                            XCTFail ("Expected .persistent")
                        }
                    }
                    counter = counter + 1
                }
            }
            // Test 100
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100", data: nil)
            for testResult in test100Results {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    let expectedInt = counter * 10
                    entity.sync() { container in
                        let myStruct = container.myStruct.get().item()!
                        myStruct.sync() { item in
                            XCTAssertEqual (expectedInt, item.myInt)
                            XCTAssertEqual ("\(expectedInt)", item.myString)
                        }
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            for testResult in test100Results {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    let expectedInt = counter * 10
                    entity.sync() { container in
                        let myStruct = container.myStruct.get().item()!
                        myStruct.sync() { item in
                            XCTAssertEqual (expectedInt, item.myInt)
                            XCTAssertEqual ("\(expectedInt)", item.myString)
                        }
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            // Test 100a
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100a", data: nil)
            for testResult in test100aResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                let group = DispatchGroup()
                for uuid in testResult {
                    group.enter()
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    let expectedInt = counter * 10
                    entity.async() { container in
                        let myStruct = container.myStruct.get().item()!
                        myStruct.async() { item in
                            XCTAssertEqual (expectedInt, item.myInt)
                            XCTAssertEqual ("\(expectedInt)", item.myString)
                            group.leave()
                        }
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
                group.wait()
            }
            for testResult in test100aResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                let group = DispatchGroup()
                for uuid in testResult {
                    group.enter()
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    let expectedInt = counter * 10
                    entity.async() { container in
                        let myStruct = container.myStruct.get().item()!
                        myStruct.async() { item in
                            XCTAssertEqual (expectedInt, item.myInt)
                            XCTAssertEqual ("\(expectedInt)", item.myString)
                            group.leave()
                        }
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
                group.wait()
            }




            // Test 100n
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100n", data: nil)
            for testResult in test100nResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    entity.sync() { container in
                        switch container.myStruct.get() {
                        case .ok (let result):
                            XCTAssertNil (result)
                        default:
                            XCTFail ("Expected .ok")
                        }
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            for testResult in test100nResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    entity.sync() { container in
                        switch container.myStruct.get() {
                        case .ok (let result):
                            XCTAssertNil (result)
                        default:
                            XCTFail ("Expected .ok")
                        }
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
            }
            // Test 100na
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100na", data: nil)
            for testResult in test100naResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                let group = DispatchGroup()
                for uuid in testResult {
                    group.enter()
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    entity.async() { container in
                        container.myStruct.async() { result in
                            switch result {
                            case .ok (let myStruct):
                                XCTAssertNil(myStruct)
                            default:
                                XCTFail ("Expected .ok")
                            }
                        }
                        group.leave()
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
                group.wait()
            }
            for testResult in test100naResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                let group = DispatchGroup()
                for uuid in testResult {
                    group.enter()
                    let entity = persistenceObjects.containerCollection.get(id: uuid).item()!
                    entity.async() { container in
                        container.myStruct.async() { result in
                            switch result {
                            case .ok (let myStruct):
                                XCTAssertNil(myStruct)
                            default:
                                XCTFail ("Expected .ok")
                            }
                        }
                        group.leave()
                    }
                    switch entity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    counter = counter + 1
                }
                group.wait()
            }




            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testEnd", data: [(name: "testCount", value: testCount), (name: "separator", value:">>>>>>>>>>>>>>>>>>>>>>>")])
            testCount = testCount + 1
        }
    }
    
    fileprivate class ParallelTestPersistence {
        
        init (accessor: DatabaseAccessor) {
            database = Database (accessor: accessor, schemaVersion: 1, logger: nil)
            myStructCollection = PersistentCollection<Database, MyStruct> (database: self.database, name: "MyStructs")
            containerCollection = ContainerCollection (database: self.database, name: "myContainerCollection")
        }
        
        let database: Database
        
        let myStructCollection: PersistentCollection<Database, MyStruct>
        
        let containerCollection: ContainerCollection
    }
    
    // Create
    private static func myStructTest1 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch)
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest1", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest1", message: "group.leave()", data: nil)
            group.leave()
        }
        return structs.ids
    }

    // Create -> Remove
    private static func myStructTest2 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
            for myStruct in structs.structs {
                myStruct.remove (batch: batch2)
            }
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2", message: "batch2.commit()", data: nil)
            batch2.commit() {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2", message: "group.leave()", data: nil)
                group.leave()
            }
        }
        return structs.ids
    }

    // Create || Remove
    private static func myStructTest2p (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        for myStruct in structs.structs {
            myStruct.remove (batch: batch2)
        }
        let localGroup = DispatchGroup()
        localGroup.enter()
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2p", message: "batch1.commit()", data: nil)
        batch1.commit() {
            localGroup.leave()
        }
        localGroup.enter()
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2p", message: "batch2.commit()", data: nil)
        batch2.commit() {
            localGroup.leave()
        }
        localGroup.wait()
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2p", message: "group.leave()", data: nil)
        group.leave()
        return structs.ids
    }
    
    // Remove (already existing)
    private static func myStructTest2r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let internalGroup = DispatchGroup()
        var counter = 1
        for id in ids {
            internalGroup.enter()
            executeOnMyStruct(persistenceObjects: persistenceObjects, id: id, group: internalGroup) { entity in
                entity.remove(batch: batch)
            }
            usleep(arc4random_uniform (500))
            counter = counter + 1
            
        }
        internalGroup.wait()
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2r", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest2r", message: "group.leave()", data: nil)
            group.leave()
        }
    }

    // Create -> Update
    private static func myStructTest3 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
            var counter = 1
            for myStruct in structs.structs {
                myStruct.update(batch: batch2) { item in
                    let newInt = item.myInt * 10
                    item.myInt = newInt
                    item.myString = "\(newInt)"                    
                }
                counter = counter + 1
            }
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest3", message: "batch2.commit()", data: nil)
            batch2.commit() {
                persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "myStructTest3", message: "group.leave()", data: nil)
                group.leave()
            }
        }
        return structs.ids
    }

    // Create || Update
    private static func myStructTest3p (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        var counter = 1
        for myStruct in structs.structs {
            myStruct.update(batch: batch2) { item in
                let newInt = item.myInt * 10
                item.myInt = newInt
                item.myString = "\(newInt)"
            }
            counter = counter + 1
        }
        let localGroup = DispatchGroup()
        localGroup.enter()
        batch1.commit() {
            localGroup.leave()
        }
        localGroup.enter()
        
        batch2.commit() {
            localGroup.leave()
        }
        localGroup.wait()
        group.leave()
        return structs.ids
    }
    
    // Update (already existing)
    private static func myStructTest3r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let internalGroup = DispatchGroup()
        var counter = 1
        for id in ids {
            internalGroup.enter()
            executeOnMyStruct(persistenceObjects: persistenceObjects, id: id, group: internalGroup) { entity in
                entity.update(batch: batch) { item in
                    let newInt = item.myInt * 10
                    item.myInt = newInt
                    item.myString = "\(newInt)"

                }
            }
            usleep(arc4random_uniform (500))
            counter = counter + 1

        }
        internalGroup.wait()
        batch.commit() {
            group.leave()
        }
    }
    
    // Create -> Update || Remove
    private static func myStructTest4 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
            let workQueue = DispatchQueue (label: "workQueue", attributes: .concurrent)
            let internalGroup = DispatchGroup()
            internalGroup.enter()
            workQueue.async {
                var counter = 1
                for myStruct in structs.structs {
                    usleep(arc4random_uniform (10))
                    myStruct.update(batch: batch2) { item in
                        let newInt = item.myInt * 10
                        item.myInt = newInt
                        item.myString = "\(newInt)"
                    }
                    counter = counter + 1
                }
                internalGroup.leave()
            }
            internalGroup.enter()
            workQueue.async {
                var counter = 1
                for myStruct in structs.structs {
                    usleep(arc4random_uniform (10))
                    myStruct.remove(batch: batch2)
                    counter = counter + 1
                }
                internalGroup.leave()
            }
            internalGroup.wait()
            batch2.commit() {
                group.leave()
            }
        }
        return structs.ids
    }

    // Create -> Update || Remove (separate batches)
    private static func myStructTest4b (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
            let batch3 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
            let workQueue = DispatchQueue (label: "workQueue", attributes: .concurrent)
            let internalGroup = DispatchGroup()
            internalGroup.enter()
            workQueue.async {
                var counter = 1
                for myStruct in structs.structs {
                    usleep(arc4random_uniform (10))
                    myStruct.update(batch: batch2) { item in
                        let newInt = item.myInt * 10
                        item.myInt = newInt
                        item.myString = "\(newInt)"
                    }
                    counter = counter + 1
                }
                internalGroup.leave()
            }
            internalGroup.enter()
            workQueue.async {
                var counter = 1
                for myStruct in structs.structs {
                    usleep(arc4random_uniform (10))
                    myStruct.remove(batch: batch3)
                    counter = counter + 1
                }
                internalGroup.leave()
            }
            internalGroup.wait()
            internalGroup.enter()
            batch2.commit() {
                internalGroup.leave()
            }
            internalGroup.enter()
            batch3.commit() {
                internalGroup.leave()
            }
            internalGroup.wait()
            group.leave()
        }
        return structs.ids
    }
    
    // MyStructContainer Create
    private static func containerTest100 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        let containers = newContainers(persistenceObjects: persistenceObjects, structs: nil, batch: batch)
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "containerTest100", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "containerTest100", message: "group.leave()", data: nil)
            group.leave()
        }
        return containers.ids
    }

    // MyStructContainer Create (nil struct)
    private static func containerTest100n (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.database.logger)
        var myStructs: [Entity<MyStruct>?] = []
        var index = 0
        while index < myStructCount {
            myStructs.append (nil)
            index = index + 1
        }
        let containers = newContainers(persistenceObjects: persistenceObjects, structs: myStructs, batch: batch)
        persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "containerTest100", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.database.logger?.log(level: .debug, source: self, featureName: "containerTest100", message: "group.leave()", data: nil)
            group.leave()
        }
        return containers.ids
    }

    private static func newStructs(persistenceObjects: ParallelTestPersistence, batch: EventuallyConsistentBatch) -> (structs: [Entity<MyStruct>], ids: [UUID]) {
        var counter = 1
        var structs: [Entity<MyStruct>] = []
        structs.reserveCapacity(myStructCount)
        var ids: [UUID] = []
        ids.reserveCapacity(myStructCount)
        while counter <= myStructCount {
            let myInt = 10 * counter
            let myString = "\(myInt)"
            let newEntity = persistenceObjects.myStructCollection.new (batch: batch, item: MyStruct (myInt: myInt, myString: myString))
            structs.append (newEntity)
            ids.append (newEntity.id)
            counter = counter + 1
        }
        return (structs: structs, ids: ids)
    }
    
    private static func executeOnMyStruct (persistenceObjects: ParallelTestPersistence, id: UUID, group: DispatchGroup, closure: @escaping (Entity<MyStruct>) -> ()) {
        persistenceObjects.myStructCollection.get(id: id) { retrievalResult in
            if let retrievedEntity = retrievalResult.item() {
                closure (retrievedEntity)
                group.leave()
            } else {
                executeOnMyStruct(persistenceObjects: persistenceObjects, id: id, group: group, closure: closure)
            }
        }
    }
    
    private static func newContainers (persistenceObjects: ParallelTestPersistence, structs: [Entity<MyStruct>?]?, batch: EventuallyConsistentBatch) -> (containers: [Entity<MyStructContainer>], ids: [UUID]) {
        var index = 0
        var containers: [Entity<MyStructContainer>] = []
        containers.reserveCapacity(myStructCount)
        var ids: [UUID] = []
        ids.reserveCapacity(myStructCount)
        var finalStructs: [Entity<MyStruct>?]? = nil
        if let structs = structs {
            finalStructs = structs
        } else {
            finalStructs = newStructs(persistenceObjects: persistenceObjects, batch: batch).structs
        }
        while index < myStructCount {
            let newContainer = persistenceObjects.containerCollection.new(batch: batch, myStruct: finalStructs![index])
            containers.append(newContainer)
            ids.append (newContainer.id)
            index = index + 1
        }
        return (containers: containers, ids: ids)
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
    
    internal class ContainerCollection : PersistentCollection<Database, MyStructContainer> {
        
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

    private static let myStructCount = 6

}
