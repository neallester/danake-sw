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
        ParallelTests.performTest(accessor: accessor, repetitions: 100, logger: nil)
    }

    public static func performTest(accessor: DatabaseAccessor, repetitions: Int, logger: Logger?) {
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
            var test300uResults: [(containers: [UUID], myStructs: [UUID])] = []
            var containerRemoveExistingContainertIds: [UUID] = []
            let resultQueue = DispatchQueue (label: "results")
            let testDispatcher = DispatchQueue (label: "testDispatcher", attributes: .concurrent)
            var persistenceObjects: ParallelTestPersistence? = ParallelTestPersistence (accessor: accessor, logger: logger)
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testStart", data: [(name: "testCount", value: testCount), (name: "separator", value:"<<<<<<<<<<<<<<<<<<<<<<<")])
            let setupBatch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects!.logger)
            let setupGroup = DispatchGroup()
            let removeExisting = ParallelTests.newStructs (persistenceObjects: persistenceObjects!, batch: setupBatch).ids
            test2Results.append (removeExisting)
            let editExisting = ParallelTests.newStructs (persistenceObjects: persistenceObjects!, batch: setupBatch).ids
            test3Results.append (editExisting)
            do {
                let containerRemoveExisting = ParallelTests.newContainers(persistenceObjects: persistenceObjects!, structs: nil, batch: setupBatch)
                test2Results.append (containerRemoveExisting.ids)
                var structIds: [UUID] = []
                for entity in containerRemoveExisting.containers {
                    containerRemoveExistingContainertIds.append(entity.id)
                    entity.sync() { container in
                        structIds.append(container.myStruct.get().item()!.id)
                    }
                }
                test2Results.append (structIds)
            }
            var test300prInput: (containers: [UUID], newRefs: [EntityReferenceSerializationData]) = ([], [])
            do {
                let containers = ParallelTests.newContainers(persistenceObjects: persistenceObjects!, structs: nil, batch: setupBatch)
                let newStructs = ParallelTests.newStructs(persistenceObjects: persistenceObjects!, batch: setupBatch)
                var newRefs: [EntityReferenceSerializationData] = []
                for myStruct in newStructs.structs {
                    persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "300u:build300pr", message: "myStruct.id", data: [(name:"id", value: myStruct.id.uuidString)])
                    newRefs.append(myStruct.referenceData())
                }
                test300prInput = (containers: containers.ids, newRefs)
                test300uResults.append((containers: containers.ids, myStructs: newStructs.ids))
                for id in newStructs.ids {
                    persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "300u:build300pr", message: "myStructs.myStruct.id", data: [(name:"id", value: id.uuidString)])
                }
            }
            var test300prplInput: (containers: [UUID], newRefs: [EntityReferenceSerializationData]) = ([], [])
            do {
                let containers = ParallelTests.newContainers(persistenceObjects: persistenceObjects!, structs: nil, batch: setupBatch)
                let newStructs = ParallelTests.newStructs(persistenceObjects: persistenceObjects!, batch: setupBatch)
                var newRefs: [EntityReferenceSerializationData] = []
                for myStruct in newStructs.structs {
                    persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "300u:build300pr", message: "myStruct.id", data: [(name:"id", value: myStruct.id.uuidString)])
                    newRefs.append(myStruct.referenceData())
                }
                test300prplInput = (containers: containers.ids, newRefs)
                test300uResults.append((containers: containers.ids, myStructs: newStructs.ids))
                for id in newStructs.ids {
                    persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "300u:build300prpl", message: "myStructs.myStruct.id", data: [(name:"id", value: id.uuidString)])
                }
            }
            setupGroup.enter()
            setupBatch.commit() {
                setupGroup.leave()
            }
            setupGroup.wait()
            persistenceObjects = nil
            Database.registrar.clear()
            persistenceObjects = ParallelTestPersistence (accessor: accessor, logger: logger)
            let test1 = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test1.start", data: nil)
                let result = ParallelTests.myStructTest1(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test1.end", data: nil)
                resultQueue.async {
                    test1Results.append (result)
                }
            }
            let test2 = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2.start", data: nil)
                let result = ParallelTests.myStructTest2(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2.end", data: nil)
                resultQueue.async {
                    test2Results.append (result)
                }
                
            }
            let test2p = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2p.start", data: nil)
                let result = ParallelTests.myStructTest2p(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2p.end", data: nil)
                resultQueue.async {
                    test2Results.append (result)
                }
                
            }
            let test2r = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2r.start", data: nil)
                ParallelTests.myStructTest2r(persistenceObjects: persistenceObjects!, group: testGroup, ids: removeExisting)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test2r.end", data: nil)
            }
            let test3 = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3.start", data: nil)
                let result = ParallelTests.myStructTest3(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3.end", data: nil)
                resultQueue.async {
                    test3Results.append (result)
                }
            }
            let test3p = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3p.start", data: nil)
                let result = ParallelTests.myStructTest3p(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3p.end", data: nil)
                resultQueue.async {
                    test3Results.append (result)
                }
            }
            let test3r = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3r.start", data: nil)
                ParallelTests.myStructTest3r(persistenceObjects: persistenceObjects!, group: testGroup, ids: editExisting)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test3r.end", data: nil)
            }
            let test4 = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4.start", data: nil)
                let result = ParallelTests.myStructTest4(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4.end", data: nil)
                resultQueue.async {
                    test4Results.append (result)
                }
            }
            let test4b = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4b.start", data: nil)
                let result = ParallelTests.myStructTest4b(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test4b.end", data: nil)
                resultQueue.async {
                    test4Results.append (result)
                }
            }
            let test100 = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100.start", data: nil)
                let result = ParallelTests.containerTest100(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100.end", data: nil)
                resultQueue.async {
                    test100Results.append (result)
                }
            }
            let test100a = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100a.start", data: nil)
                let result = ParallelTests.containerTest100(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100a.end", data: nil)
                resultQueue.async {
                    test100aResults.append (result)
                }
            }
            let test100n = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100n.start", data: nil)
                let result = ParallelTests.containerTest100n(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100n.end", data: nil)
                resultQueue.async {
                    test100nResults.append (result)
                }
            }
            let test100na = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100na.start", data: nil)
                let result = ParallelTests.containerTest100n(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test100na.end", data: nil)
                resultQueue.async {
                    test100naResults.append (result)
                }
            }
            let test200r = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test200r.start", data: nil)
                ParallelTests.containerTest200r(persistenceObjects: persistenceObjects!, group: testGroup, ids: containerRemoveExistingContainertIds)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test200r.end", data: nil)
            }
            let test300 = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300.start", data: nil)
                let result = ParallelTests.containerTest300(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300.end", data: nil)
                resultQueue.async {
                    test1Results.append(result.myStructs)
                    test100nResults.append (result.containers)
                }
            }
            let test300a = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300a.start", data: nil)
                let result = ParallelTests.containerTest300(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300a.end", data: nil)
                resultQueue.async {
                    test1Results.append(result.myStructs)
                    test100naResults.append (result.containers)
                }
            }
            let test300u = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300u.start", data: nil)
                let result = ParallelTests.containerTest300u(persistenceObjects: persistenceObjects!, group: testGroup)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300u.end", data: nil)
                resultQueue.async {
                    test300uResults.append(result)
                }
            }
            let test300pr = {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300pr.start", data: nil)
                ParallelTests.containerTest300pr(persistenceObjects: persistenceObjects!, group: testGroup, containers: test300prInput.containers, structRefs: test300prInput.newRefs)
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "test300pr.end", data: nil)
            }
            let test300prpl = {
                let localLogger = persistenceObjects!.logger
                localLogger?.log(level: .debug, source: self, featureName: "performTest", message: "test300prpl.start", data: nil)
                ParallelTests.containerTest300prpl(persistenceObjects: persistenceObjects!, group: testGroup, containers: test300prplInput.containers, structRefs: test300prplInput.newRefs)
                localLogger?.log(level: .debug, source: self, featureName: "performTest", message: "test300prpl.end", data: nil)
            }
            var tests = [test1, test2, test2p, test2r, test3, test3p, test3r, test4, test4b, test100, test100a, test100n, test100na, test200r, test300, test300a, test300u, test300pr, test300prpl]
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
                        persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "setThrowError", data: [(name:"maxSleepTime", value: sleepTime)])
                        usleep (sleepTime)
                        persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "setThrowError", data: nil)
                        inMemoryAccessor.setThrowError()
                        persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "setThrowError.group.leave()", data: nil)
                        testGroup.leave()
                    }
                    finalTests.append (newTest)
                    errorCounter = errorCounter + 1
                }
            }
            finalTests.append (contentsOf: randomTests)
            for test in finalTests {
                persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testGroup.enter()", data: nil)
                testGroup.enter()
                testDispatcher.async(execute: test)
            }
            let executionStart = Date()
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "waiting", data: [(name: "testCount", value: testCount), (name:"separator", value: "--------------------------------")])
            testGroup.wait()
            resultQueue.sync {}
            totalExecutionTime = totalExecutionTime + Date().timeIntervalSince1970 - executionStart.timeIntervalSince1970
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "waiting.finished", data: [(name: "testCount", value: testCount), (name:"separator", value: "================================")])
            if let inMemoryAccessor = accessor as? InMemoryAccessor {
                inMemoryAccessor.setThrowError(false)
            }
            persistenceObjects = nil
            Database.registrar.clear()
            persistenceObjects = ParallelTestPersistence (accessor: accessor, logger: logger)
            // Test 1
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.1", data: nil)
            for testResult in test1Results {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects!.myStructCollection.get(id: uuid).item()!
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
                    let entity = persistenceObjects!.myStructCollection.get(id: uuid).item()!
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
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.2", data: nil)
            for testResult in test2Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                for uuid in testResult {
                    XCTAssertNil (persistenceObjects!.myStructCollection.get(id: uuid).item())
                }
            }
            for testResult in test2Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                for uuid in testResult {
                    XCTAssertNil (persistenceObjects!.myStructCollection.get(id: uuid).item())
                }
            }
            // Test 3
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.3", data: nil)
            for testResult in test3Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects!.myStructCollection.get(id: uuid).item()!
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
                    let entity = persistenceObjects!.myStructCollection.get(id: uuid).item()!
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
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.4", data: nil)
            for testResult in test4Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    if let entity = persistenceObjects!.myStructCollection.get(id: uuid).item() {
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
                    if let entity = persistenceObjects!.myStructCollection.get(id: uuid).item() {
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
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100", data: nil)
            for testResult in test100Results {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects!.containerCollection.get(id: uuid).item()!
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
                    let entity = persistenceObjects!.containerCollection.get(id: uuid).item()!
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
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100a", data: nil)
            for testResult in test100aResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                let group = DispatchGroup()
                for uuid in testResult {
                    group.enter()
                    let entity = persistenceObjects!.containerCollection.get(id: uuid).item()!
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
                    let entity = persistenceObjects!.containerCollection.get(id: uuid).item()!
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
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.100n", data: nil)
            for testResult in test100nResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.count)
                var counter = 1
                for uuid in testResult {
                    let entity = persistenceObjects!.containerCollection.get(id: uuid).item()!
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
                    let entity = persistenceObjects!.containerCollection.get(id: uuid).item()!
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
            // Test 300u
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testResults.300u", data: nil)
            var resultCount = 0
            for testResult in test300uResults {
                XCTAssertEqual (ParallelTests.myStructCount, testResult.containers.count)
                XCTAssertEqual (ParallelTests.myStructCount, testResult.myStructs.count)
                var index = 0
                for uuid in testResult.containers {
                    let containerEntity = persistenceObjects!.containerCollection.get(id: uuid).item()!
                    containerEntity.sync() { container in
                        switch container.myStruct.get() {
                        case .ok (let myStruct):
                            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "expectedMyStructId", message: "testResults.300u", data: [(name: "resultCount", value: resultCount), (name: "expected", value: testResult.myStructs[index].uuidString), (name: "actual", value: myStruct!.id.uuidString)])
                            XCTAssertEqual (testResult.myStructs[index].uuidString, myStruct!.id.uuidString)
                            myStruct!.sync() { myStruct in
                                let expectedInt = (index + 1) * 100
                                XCTAssertEqual (expectedInt, myStruct.myInt)
                                XCTAssertEqual("\(expectedInt)", myStruct.myString)
                            }
                        default:
                            XCTFail ("Expected .ok")
                        }
                    }
                    switch containerEntity.getPersistenceState() {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    index = index + 1
                }
                resultCount = resultCount + 1
            }
            persistenceObjects!.logger?.log(level: .debug, source: self, featureName: "performTest", message: "testEnd", data: [(name: "testCount", value: testCount), (name: "separator", value:">>>>>>>>>>>>>>>>>>>>>>>")])
            testCount = testCount + 1
        }
    }
    
    fileprivate class ParallelTestPersistence {
        
        init (accessor: DatabaseAccessor, logger: Logger?) {
            self.logger = logger
            let database = Database (accessor: accessor, schemaVersion: 1, logger: logger, referenceRetryInterval: 0.000001)
            myStructCollection = PersistentCollection<Database, MyStruct> (database: database, name: "MyStructs")
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
//            var myStructCount = 0
//            var containerCount = 0
//            myStructCollection.sync() { entities in
//                myStructCount = entities.count
//            }
//            containerCollection.sync() { entities in
//                containerCount = entities.count
//                for value in entities.values {
//                    print ("RefCount: \(CFGetRetainCount(value.item!))")
//                }
//            }
//            print ("MyStructs: \(myStructCount)")
//            print ("containers: \(containerCount)")
        }
        
        let logger: Logger?
        
        let myStructCollection: PersistentCollection<Database, MyStruct>
        
        let containerCollection: ContainerCollection
    }
    
    // Create
    private static func myStructTest1 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch)
        let result = structs.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest1", message: "batch.commit()", data: nil)
        batch.commit() {
            structs = ([], [])
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest1", message: "group.leave()", data: nil)
            group.leave()
        }
        return result
    }

    // Create -> Remove
    private static func myStructTest2 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let result = structs.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2", message: "batch1.commit()", data: nil)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
            for myStruct in structs.structs {
                myStruct.remove (batch: batch2)
            }
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2", message: "batch2.commit()", data: nil)
            batch2.commit() {
                structs = ([], [])
                persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2", message: "group.leave()", data: nil)
                group.leave()
            }
        }
        return result
    }

    // Create || Remove
    private static func myStructTest2p (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let result = structs.ids
        let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        for myStruct in structs.structs {
            myStruct.remove (batch: batch2)
        }
        let localGroup = DispatchGroup()
        localGroup.enter()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2p", message: "batch1.commit()", data: nil)
        batch1.commit() {
            localGroup.leave()
        }
        localGroup.enter()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2p", message: "batch2.commit()", data: nil)
        batch2.commit() {
            localGroup.leave()
        }
        localGroup.wait()
        structs = ([], [])
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2p", message: "group.leave()", data: nil)
        group.leave()
        return result
    }
    
    // Remove (already existing)
    private static func myStructTest2r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let internalGroup = DispatchGroup()
        var counter = 1
        for id in ids {
            internalGroup.enter()
            executeOnMyStruct(persistenceObjects: persistenceObjects, id: id, group: internalGroup, logger: persistenceObjects.logger, sourceLabel: "myStructTest2r") { entity in
                entity.remove(batch: batch)
            }
            usleep(arc4random_uniform (500))
            counter = counter + 1
            
        }
        internalGroup.wait()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2r", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest2r", message: "group.leave()", data: nil)
            group.leave()
        }
    }

    // Create -> Update
    private static func myStructTest3 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let result = structs.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3", message: "batch1.commit()", data: nil)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
            var counter = 1
            for myStruct in structs.structs {
                myStruct.update(batch: batch2) { item in
                    let newInt = item.myInt * 10
                    item.myInt = newInt
                    item.myString = "\(newInt)"                    
                }
                counter = counter + 1
            }
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3", message: "batch2.commit()", data: nil)
            batch2.commit() {
                persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3", message: "group.leave()", data: nil)
                structs = ([], [])
                group.leave()
            }
        }
        return result
    }

    // Create || Update
    private static func myStructTest3p (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let result = structs.ids
        let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
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
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3p", message: "batch1.commit()", data: nil)
        batch1.commit() {
            localGroup.leave()
        }
        localGroup.enter()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3p", message: "batch2.commit()", data: nil)
        batch2.commit() {
            localGroup.leave()
        }
        localGroup.wait()
        structs = ([], [])
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3p", message: "group.leave()", data: nil)
        group.leave()
        return result
    }
    
    // Update (already existing)
    private static func myStructTest3r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let internalGroup = DispatchGroup()
        var counter = 1
        for id in ids {
            internalGroup.enter()
            executeOnMyStruct(persistenceObjects: persistenceObjects, id: id, group: internalGroup, logger: persistenceObjects.logger, sourceLabel: "myStructTest3r") { entity in
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
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3r", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest3r", message: "group.leave()", data: nil)
            group.leave()
        }
    }
    
    // Create -> Update || Remove
    private static func myStructTest4 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let result = structs.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4", message: "batch1.commit()", data: nil)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
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
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4", message: "batch2.commit()", data: nil)
            batch2.commit() {
                structs = ([], [])
                persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4", message: "group.leave()", data: nil)
                group.leave()
            }
        }
        return result
    }

    // Create -> Update || Remove (separate batches)
    private static func myStructTest4b (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let result = structs.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4b", message: "batch1.commit()", data: nil)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
            let batch3 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
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
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4b", message: "batch2.commit()", data: nil)
            batch2.commit() {
                internalGroup.leave()
            }
            internalGroup.enter()
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4b", message: "batch1.commit3()", data: nil)
            batch3.commit() {
                internalGroup.leave()
            }
            internalGroup.wait()
            structs = ([], [])
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "myStructTest4b", message: "group.leave()", data: nil)
            group.leave()
        }
        return result
    }
    
    // MyStructContainer Create
    private static func containerTest100 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var containers = newContainers(persistenceObjects: persistenceObjects, structs: nil, batch: batch)
        let result = containers.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest100", message: "batch.commit()", data: nil)
        batch.commit() {
            containers = ([], [])
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest100", message: "group.leave()", data: nil)
            group.leave()
        }
        return result
    }

    // MyStructContainer Create (nil struct)
    private static func containerTest100n (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var myStructs: [Entity<MyStruct>?] = []
        var index = 0
        while index < myStructCount {
            myStructs.append (nil)
            index = index + 1
        }
        var containers = newContainers(persistenceObjects: persistenceObjects, structs: myStructs, batch: batch)
        let result = containers.ids
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest100n", message: "batch.commit()", data: nil)
        batch.commit() {
            containers = ([], [])
            myStructs = ([])
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest100n", message: "group.leave()", data: nil)
            group.leave()
        }
        return result
    }
    
    // MyStructContainer Remove (already existing)
    private static func containerTest200r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let internalGroup = DispatchGroup()
        for id in ids {
            internalGroup.enter()
            executeOnContainer(persistenceObjects: persistenceObjects, id: id, group: internalGroup) { entity in
                entity.async() { container in
                    executeOnReference (reference: container.myStruct, logger: persistenceObjects.logger, sourceLabel: "containerTest200r") { entity in
                        entity!.remove(batch: batch)
                        internalGroup.leave()
                    }
                }
                entity.remove(batch: batch)
            }
            usleep(arc4random_uniform (500))
        }
        internalGroup.wait()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest200r", message: "batch.commit()", data: nil)
        batch.commit() {
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest200r", message: "group.leave()", data: nil)
            group.leave()
        }
    }
    
    private static func executeOnReference (reference: EntityReference<MyStructContainer, MyStruct>, logger: Logger?, sourceLabel: String, closure: @escaping (Entity<MyStruct>?) -> ()) {
        reference.async() { result in
            switch result {
            case .ok (let entity):
                closure (entity)
            case .error(let errorMessage):
                logger?.log (level: .debug, source: self, featureName: "executeOnReference", message: "error", data: [(name: "source", value: sourceLabel), (name: "message", value: errorMessage)])
                executeOnReference(reference: reference, logger: logger, sourceLabel: sourceLabel, closure: closure)
            }
        }
    }
    
    // MyStructContainer Create -> Update (set nil)
    private static func containerTest300 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> (containers: [UUID], myStructs: [UUID]) {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var myStructs = newStructs(persistenceObjects: persistenceObjects, batch: batch1)
        var containers = newContainers (persistenceObjects: persistenceObjects, structs: myStructs.structs, batch: batch1)
        let result = (containers.ids, myStructs.ids)
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300", message: "batch1.commit()", data: nil)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
            for container in containers.containers {
                container.sync { item in
                    item.myStruct.set(entity: nil, batch: batch2)
                }
            }
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300", message: "batch2.commit()", data: nil)
            batch2.commit() {
                persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300", message: "group.leave()", data: nil)
                myStructs = ([], [])
                containers = ([], [])
                group.leave()
            }
        }
        
        return result
    }

    // MyStructContainer Create -> Update
    private static func containerTest300u (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> (containers: [UUID], myStructs: [UUID]) {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        var myStructs = newStructs(persistenceObjects: persistenceObjects, batch: batch1)
        var containers = newContainers (persistenceObjects: persistenceObjects, structs: myStructs.structs, batch: batch1)
        let result = (containers.ids, myStructs.ids)
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300u", message: "batch1.commit()", data: nil)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
            let internalGroup = DispatchGroup()
            for container in containers.containers {
                internalGroup.enter()
                container.async { item in
                    executeOnMyStruct(persistenceObjects: persistenceObjects, container: item) { myStructEntity in
                        myStructEntity.update(batch: batch2) { item in
                            let newInt = item.myInt * 10
                            item.myInt = newInt
                            item.myString = "\(newInt)"
                        }
                        internalGroup.leave()
                    }
                }
            }
            internalGroup.wait()
            persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300u", message: "batch2.commit()", data: nil)
            batch2.commit() {
                myStructs = ([], [])
                containers = ([], [])
                persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300u", message: "group.leave()", data: nil)
                group.leave()
            }
        }
        return result
    }
    
    // MyContainer -> Update + Edit Struct independent in parallel
    private static func containerTest300pr (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, containers: [UUID], structRefs: [EntityReferenceSerializationData]) {
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let workQueue = DispatchQueue (label: "300pr.work", attributes: .concurrent)
        let internalGroup = DispatchGroup()
        var containerIndex = 0
        for containerId in containers {
            
            internalGroup.enter()
            let structRefIndex = containerIndex
            let closure = { (containerEntity: Entity<MyStructContainer>) in
                containerEntity.update(batch: batch1) { container in
                    persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300pr", message: "setRef", data: [(name: "structRefIndex", value: structRefIndex), (name: "refId", value: structRefs[structRefIndex].id.uuidString)])
                    container.myStruct.set(referenceData: structRefs[structRefIndex], batch: batch1)
                    internalGroup.leave()
                }
            }
            workQueue.async {
                usleep(arc4random_uniform(UInt32(1000)))
                self.executeOnContainer(persistenceObjects: persistenceObjects, id: containerId, group: internalGroup, closure: closure)
            }
            
            containerIndex = containerIndex + 1
        }
        for structRef in structRefs {
            internalGroup.enter()
            workQueue.async {
                usleep(arc4random_uniform(UInt32(1000)))
                executeOnMyStruct(persistenceObjects: persistenceObjects, id: structRef.id, group: internalGroup, logger: persistenceObjects.logger, sourceLabel: "containerTest300pr") { entity in
                    internalGroup.enter()
                    entity.update (batch: batch2) { item in
                        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300pr", message: "updateStruct", data: [(name: "structfId", value: entity.id.uuidString)])
                        let newInt = item.myInt * 10
                        item.myInt = newInt
                        item.myString = "\(newInt)"
                        internalGroup.leave()
                    }
                }
                
            }
        }
        internalGroup.wait()
        internalGroup.enter()
        batch1.commit() {
            internalGroup.leave()
        }
        internalGroup.enter()
        batch2.commit() {
            internalGroup.leave()
        }
        internalGroup.wait()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300pr", message: "group.leave()", data: nil)
        group.leave()
    }
    
    // MyContainer -> Update + Edit Struct independent in parallel with preloading
    private static func containerTest300prpl (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, containers: [UUID], structRefs: [EntityReferenceSerializationData]) {
        let containerPreload = preLoad(collection: persistenceObjects.containerCollection, logger: persistenceObjects.logger, label: "containerTest300prpl", ids: containers)
        let _ = containerPreload.count
        var structIds: [UUID] = []
        for ref in structRefs {
            structIds.append(ref.id)
        }
        let myStructPreload = preLoad(collection: persistenceObjects.myStructCollection, logger: persistenceObjects.logger, label: "containerTest300prpl", ids: structIds)
        let _ = myStructPreload.count
        let batch1 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let batch2 = EventuallyConsistentBatch(retryInterval: .microseconds(50), timeout: BatchDefaults.timeout, logger: persistenceObjects.logger)
        let workQueue = DispatchQueue (label: "300prpl.work", attributes: .concurrent)
        let internalGroup = DispatchGroup()
        var containerIndex = 0
        for containerId in containers {
            
            internalGroup.enter()
            let structRefIndex = containerIndex
            let closure = { (containerEntity: Entity<MyStructContainer>) in
                containerEntity.update(batch: batch1) { container in
                    persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300prpl", message: "setRef", data: [(name: "structRefIndex", value: structRefIndex), (name: "refId", value: structRefs[structRefIndex].id.uuidString)])
                    container.myStruct.set(referenceData: structRefs[structRefIndex], batch: batch1)
                    internalGroup.leave()
                }
            }
            workQueue.async {
                usleep(arc4random_uniform(UInt32(1000)))
                self.executeOnContainer(persistenceObjects: persistenceObjects, id: containerId, group: internalGroup, closure: closure)
            }
            
            containerIndex = containerIndex + 1
        }
        for structRef in structRefs {
            internalGroup.enter()
            workQueue.async {
                usleep(arc4random_uniform(UInt32(1000)))
                executeOnMyStruct(persistenceObjects: persistenceObjects, id: structRef.id, group: internalGroup, logger: persistenceObjects.logger, sourceLabel: "containerTest300pr") { entity in
                    internalGroup.enter()
                    entity.update (batch: batch2) { item in
                        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300prpl", message: "updateStruct", data: [(name: "structfId", value: entity.id.uuidString)])
                        let newInt = item.myInt * 10
                        item.myInt = newInt
                        item.myString = "\(newInt)"
                        internalGroup.leave()
                    }
                }
                
            }
        }
        internalGroup.wait()
        internalGroup.enter()
        batch1.commit() {
            internalGroup.leave()
        }
        internalGroup.enter()
        batch2.commit() {
            internalGroup.leave()
        }
        internalGroup.wait()
        persistenceObjects.logger?.log(level: .debug, source: self, featureName: "containerTest300prpl", message: "group.leave()", data: nil)
        group.leave()
    }

    private static func preLoad<T> (collection: PersistentCollection<Database, T>, logger: Logger?, label: String, ids: [UUID]) -> [Entity<T>] {
        logger?.log(level: .debug, source: self, featureName: "preLoad<T>", message: "start." + label, data: nil)
        var result: [Entity<T>] = []
        var badIds: [UUID] = []
        for id in ids {
            switch collection.get(id: id) {
            case .ok (let retrievedEntity):
                result.append (retrievedEntity!)
            case .error (let errorMessage):
                logger?.log(level: .debug, source: self, featureName: "preLoad<T>", message: "from." + label, data: [(name:"error", value: errorMessage)])
                badIds.append (id)
            }
        }
        if badIds.isEmpty {
            logger?.log(level: .debug, source: self, featureName: "preLoad<T>", message: "end." + label, data: [(name: "resultCount", value: result.count)])
            return result
        } else {
            let interimResult: [Entity<T>] = preLoad(collection: collection, logger: logger, label: label, ids: badIds)
            return result + interimResult
        }
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
    
    private static func executeOnMyStruct (persistenceObjects: ParallelTestPersistence, id: UUID, group: DispatchGroup, logger: Logger?, sourceLabel: String, closure: @escaping (Entity<MyStruct>) -> ()) {
        persistenceObjects.myStructCollection.get(id: id) { retrievalResult in
            switch retrievalResult {
            case .ok (let entity):
                if let entity = entity {
                    closure (entity)
                } else {
                    XCTFail ("Expected entity")
                }
                group.leave()
            case .error (let errorMessage):
                logger?.log (level: .debug, source: self, featureName: "executeOnMyStruct", message: "error", data: [(name: "source", value: sourceLabel), (name: "message", value: errorMessage)])
                executeOnMyStruct(persistenceObjects: persistenceObjects, id: id, group: group, logger: logger, sourceLabel: sourceLabel, closure: closure)
            }
        }
    }
    
    private static func executeOnContainer (persistenceObjects: ParallelTestPersistence, id: UUID, group: DispatchGroup, closure: @escaping (Entity<MyStructContainer>) -> ()) {
        persistenceObjects.containerCollection.get(id: id) { retrievalResult in
            if let retrievedEntity = retrievalResult.item() {
                closure (retrievedEntity)
            } else {
                executeOnContainer(persistenceObjects: persistenceObjects, id: id, group: group, closure: closure)
            }
        }
    }
    
    private static func executeOnMyStruct (persistenceObjects: ParallelTestPersistence, container: MyStructContainer, closure: @escaping (Entity<MyStruct>) -> ()) {
        container.myStruct.async() { result in
            switch result {
            case .ok (let entity):
                if let entity = entity {
                    closure (entity)
                } else {
                    XCTFail ("Expected entity")
                }
            default:
                executeOnMyStruct(persistenceObjects: persistenceObjects, container: container, closure: closure)
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

    private static let myStructCount = 6

}
