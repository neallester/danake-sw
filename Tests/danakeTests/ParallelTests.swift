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
            var persistenceObjects = ParallelTestPersistence (accessor: accessor)
            let resultQueue = DispatchQueue (label: "results")
            let testDispatcher = DispatchQueue (label: "testDispatcher", attributes: .concurrent)
            let testGroup = DispatchGroup()
            var test1Results: [[UUID]] = []
            var test2Results: [[UUID]] = []
            var test3Results: [[UUID]] = []
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
            let test3 = {
                let result = ParallelTests.myStructTest3(persistenceObjects: persistenceObjects, group: testGroup)
                resultQueue.async {
                    test3Results.append (result)
                }
                
            }
            var tests = [test1, test2, test3]
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
            XCTAssertEqual (1, test1Results.count)
            XCTAssertEqual (1, test2Results.count)
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
    
    private static func newStructs(persistenceObjects: ParallelTestPersistence, batch: EventuallyConsistentBatch) -> (structs: [Entity<MyStruct>], ids: [UUID]) {
        var counter = 1
        var structs: [Entity<MyStruct>] = []
        var ids: [UUID] = []
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
