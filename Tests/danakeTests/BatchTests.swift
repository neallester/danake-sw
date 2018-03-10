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
        let batch = Batch()
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
        let batch = Batch()
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

}
