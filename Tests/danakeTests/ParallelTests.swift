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
            let testDispatcher = DispatchQueue (label: "testDispatcher", attributes: .concurrent)
            let testGroup = DispatchGroup()
            var test1Results: [UUID] = []
            var test2Results: [UUID] = []
            let test1 = {
                test1Results = ParallelTests.myStructTest1(persistenceObjects: persistenceObjects, group: testGroup)
            }
            let test2 = {
                test2Results = ParallelTests.myStructTest2(persistenceObjects: persistenceObjects, group: testGroup)
            }
            var tests = [test1, test2]
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
            XCTAssertEqual (6, test1Results.count)
            var counter = 1
            for uuid in test1Results {
                let entity = persistenceObjects.myStructCollection.get(id: uuid).item()!
                entity.sync { myStruct in
                    XCTAssertEqual (counter * 10, myStruct.myInt)
                }
                switch entity.getPersistenceState() {
                case .persistent:
                    break
                default:
                    XCTFail ("Expected .persistent")
                }
                counter = counter + 1
            }
            testCount = testCount + 1
            // Test 2
            XCTAssertEqual(6, test2Results.count)
            for uuid in test2Results {
                XCTAssertNil (persistenceObjects.myStructCollection.get(id: uuid).item())
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
    
    private static func myStructTest1 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch = EventuallyConsistentBatch()
        let s1 = persistenceObjects.myStructCollection.new(batch: batch, item: MyStruct (myInt: 10, myString: "10"))
        let s2 = persistenceObjects.myStructCollection.new(batch: batch, item: MyStruct (myInt: 20, myString: "20"))
        let s3 = persistenceObjects.myStructCollection.new(batch: batch, item: MyStruct (myInt: 30, myString: "30"))
        let s4 = persistenceObjects.myStructCollection.new(batch: batch, item: MyStruct (myInt: 40, myString: "40"))
        let s5 = persistenceObjects.myStructCollection.new(batch: batch, item: MyStruct (myInt: 50, myString: "50"))
        let s6 = persistenceObjects.myStructCollection.new(batch: batch, item: MyStruct (myInt: 60, myString: "60"))
        batch.commit() {
            group.leave()
        }
        return [s1.id, s2.id, s3.id, s4.id, s5.id, s6.id]
    }

    private static func myStructTest2 (persistenceObjects: ParallelTestPersistence, group: DispatchGroup) -> [UUID] {
        let batch1 = EventuallyConsistentBatch()
        let s1 = persistenceObjects.myStructCollection.new(batch: batch1, item: MyStruct (myInt: 10, myString: "10"))
        let s2 = persistenceObjects.myStructCollection.new(batch: batch1, item: MyStruct (myInt: 20, myString: "20"))
        let s3 = persistenceObjects.myStructCollection.new(batch: batch1, item: MyStruct (myInt: 30, myString: "30"))
        let s4 = persistenceObjects.myStructCollection.new(batch: batch1, item: MyStruct (myInt: 40, myString: "40"))
        let s5 = persistenceObjects.myStructCollection.new(batch: batch1, item: MyStruct (myInt: 50, myString: "50"))
        let s6 = persistenceObjects.myStructCollection.new(batch: batch1, item: MyStruct (myInt: 60, myString: "60"))
        let result = [s1.id, s2.id, s3.id, s4.id, s5.id, s6.id]
        batch1.commit() {
            let batch2 = EventuallyConsistentBatch()
            s1.remove(batch: batch2)
            s2.remove(batch: batch2)
            s3.remove(batch: batch2)
            s4.remove(batch: batch2)
            s5.remove(batch: batch2)
            s6.remove(batch: batch2)
            batch2.commit() {
                group.leave()
            }
        }
        return result
    }

}
