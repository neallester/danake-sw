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
        while testCount < repetitions {
            let testGroup = DispatchGroup()
            var test1Results: [[UUID]] = []
            var test2Results: [[UUID]] = []
            var test3Results: [[UUID]] = []
            var test4Results: [[UUID]] = []
            let resultQueue = DispatchQueue (label: "results")
            let testDispatcher = DispatchQueue (label: "testDispatcher", attributes: .concurrent)
            var persistenceObjects = ParallelTestPersistence (accessor: accessor)
            let setupBatch = EventuallyConsistentBatch()
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
                let result = ParallelTests.myStructTest1(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test1Results.append (result)
                }
            }
            let test2 = {
                let result = ParallelTests.myStructTest2(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test2Results.append (result)
                }
                
            }
            let test2p = {
                let result = ParallelTests.myStructTest2p(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test2Results.append (result)
                }
                
            }
            let test2r = {
                ParallelTests.myStructTest2r(persistenceObjects: persistenceObjects, group: testGroup, ids: removeExisting)
            }
            let test3 = {
                let result = ParallelTests.myStructTest3(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test3Results.append (result)
                }
            }
            let test3p = {
                let result = ParallelTests.myStructTest3p(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test3Results.append (result)
                }
            }
            let test3r = {
                ParallelTests.myStructTest3r(persistenceObjects: persistenceObjects, group: testGroup, ids: editExisting)
            }
            let test4 = {
                let result = ParallelTests.myStructTest4(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test4Results.append (result)
                }
            }
            let test4b = {
                let result = ParallelTests.myStructTest4b(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test4Results.append (result)
                }
            }
            var tests = [test1, test2, test2p, test2r, test3, test3p, test3r, test4, test4b]
//            if let inMemoryAccessor = accessor as? InMemoryAccessor, (arc4random_uniform(5) > 0) {
//                var errorCounter = 0
//                while errorCounter < 5 {
//                    let newTest = {
//                        inMemoryAccessor.setThrowError()
//                        testGroup.leave()
//                    }
//                    tests.append (newTest)
//                    errorCounter = errorCounter + 1
//                }
//            }
            var randomTests: [() -> ()] = []
            while tests.count > 0 {
                let itemToRemove = Int (arc4random_uniform(UInt32(tests.count)))
                randomTests.append (tests[itemToRemove])
                let _ = tests.remove(at: itemToRemove)
            }
            for test in randomTests {
                testGroup.enter()
                testDispatcher.async(execute: test)
            }
            testGroup.wait()
            persistenceObjects = ParallelTestPersistence (accessor: accessor)
            // Test 1
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
            for testResult in test2Results {
                XCTAssertEqual(ParallelTests.myStructCount, testResult.count)
                for uuid in testResult {
                    XCTAssertNil (persistenceObjects.myStructCollection.get(id: uuid).item())
                }
            }
            // Test 3
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
            testCount = testCount + 1
        }
    }
    
    fileprivate class ParallelTestPersistence {
        
        init (accessor: DatabaseAccessor) {
            database = Database (accessor: accessor, schemaVersion: 1, logger: nil)
            myStructCollection = PersistentCollection<Database, MyStruct> (database: self.database, name: "MyStructs")
        }
        
        let database: Database
        
        let myStructCollection: PersistentCollection<Database, MyStruct>
    }
    
    // Create
    private static func myStructTest1 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch)
        batch.commit() {
            group.leave()
        }
        return structs.ids
    }

    // Remove
    private static func myStructTest2 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch()
            for myStruct in structs.structs {
                myStruct.remove (batch: batch2)
            }
            batch2.commit() {
                group.leave()
            }
        }
        return structs.ids
    }

    // Remove
    private static func myStructTest2p (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let batch2 = EventuallyConsistentBatch()
        for myStruct in structs.structs {
            myStruct.remove (batch: batch2)
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
    
    private static func myStructTest2r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch()
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
        batch.commit() {
            group.leave()
        }
    }


    // Update
    private static func myStructTest3 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch()
            var counter = 1
            for myStruct in structs.structs {
                myStruct.update(batch: batch2) { item in
                    let newInt = item.myInt * 10
                    item.myInt = newInt
                    item.myString = "\(newInt)"                    
                }
                counter = counter + 1
            }
            batch2.commit() {
                group.leave()
            }
        }
        return structs.ids
    }

    private static func myStructTest3p (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        let batch2 = EventuallyConsistentBatch()
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
        group.leave()
        return structs.ids
    }
    
    private static func myStructTest3r (persistenceObjects: ParallelTestPersistence, group: DispatchGroup, ids: [UUID]) {
        let batch = EventuallyConsistentBatch()
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
    
    // Update or Edit
    private static func myStructTest4 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch()
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

    private static func myStructTest4b (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let structs = newStructs (persistenceObjects: persistenceObjects, batch: batch1)
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch()
            let batch3 = EventuallyConsistentBatch()
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
    
    private static let myStructCount = 6

}
