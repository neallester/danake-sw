//
//  EntityTests.swift
//  danakeTests
//
//  Created by Neal Lester on 1/26/18.
//

import XCTest
@testable import danake


struct MyStruct : Codable {
    
    var myInt = 0
    var myString = ""
    
}

func newTestEntity (myInt: Int, myString: String) -> Entity<MyStruct> {
    var myStruct = MyStruct()
    myStruct.myInt = myInt
    myStruct.myString = myString
    let id = UUID()
    return Entity (id: id, version: 0, item: myStruct)

}

class EntityTests: XCTestCase {
    
    func testReadAccess() {
        let entity = newTestEntity(myInt: 10, myString: "Test Completed")
        var itemInt = 0
        var itemString = ""
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("Test Completed", itemString)
        itemInt = 0
        itemString = ""
        let waitFor = expectation(description: "testSyncAsync")
        entity.async() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("Test Completed", itemString)
    }

    func testWriteAccess() {
        let entity = newTestEntity(myInt: 0, myString: "")
        var batch = Batch()
        entity.incrementVersion()
        var itemInt = 0
        var itemString = ""
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 10
            item.myString = "Test Completed"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("Test Completed", itemString)
        batch.syncItems() { (items: Dictionary<UUID, (version: Int, item: EntityManagement)>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(1, entity.getVersion())
            XCTAssertEqual(entity.getVersion(), items[entity.getId()]!.version)
            let retrievedEntity = items[entity.getId()]!.item as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        batch = Batch()
        itemInt = 0
        itemString = ""
        let waitFor = expectation(description: "testSyncAsync")
        entity.async(batch: batch) { (item: inout MyStruct) in
            item.myInt = 20
            item.myString = "Test 2 Completed"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        XCTAssertEqual(20, itemInt)
        XCTAssertEqual("Test 2 Completed", itemString)
        batch.syncItems() { (items: Dictionary<UUID, (version: Int, item: EntityManagement)>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(1, entity.getVersion())
            XCTAssertEqual(entity.getVersion(), items[entity.getId()]!.version)
            let retrievedEntity = items[entity.getId()]!.item as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
    }

}
