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
    let database = Database (accessor: InMemoryAccessor(), logger: nil)
    let collection = PersistentCollection<Database, MyStruct>(database: database, name: "myCollection")
    return Entity (collection: collection, id: id, version: 0, item: myStruct)
}

class EntityTests: XCTestCase {
    
    func testEntityCreation() {
        // Creation with item
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let id = UUID()
        let database = Database (accessor: InMemoryAccessor(), logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let entity1 = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (id.uuidString, entity1.getId().uuidString)
        XCTAssertEqual (10, entity1.getVersion())
        switch entity1.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity1.sync() { item in
            XCTAssertEqual (100, item.myInt)
            XCTAssertEqual ("Test String 1", item.myString)
        }
        // Creation with itemClosure
        let id2 = UUID()
        let entity2 = Entity (collection: collection, id: id2, version: 20) { reference in
            return MyStruct (myInt: reference.version, myString: reference.id.uuidString)
        }
        XCTAssertEqual (id2.uuidString, entity2.getId().uuidString)
        XCTAssertEqual (20, entity2.getVersion())
        switch entity2.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity2.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual (entity2.getId().uuidString, item.myString)
        }
    }
    
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
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
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
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
    }

    func testWriteAccess() {
        var entity = newTestEntity(myInt: 0, myString: "")
        var batch = Batch()
        entity.incrementVersion()
        var itemInt = 0
        var itemString = ""
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 10
            item.myString = "Test Completed"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail("Expected .new")
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
        entity = newTestEntity(myInt: 0, myString: "")
        entity.incrementVersion()
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
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail("Expected .new")
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
    
    func testEncodeDecode() throws {
        let entity = newTestEntity(myInt: 100, myString: "A \"Quoted\" String")
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        let json = try String (data: JSONEncoder().encode(entity), encoding: .utf8)!
        XCTAssertEqual("{\"id\":\"\(entity.id.uuidString)\",\"version\":0,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"}}", json)
        let entity2 = try JSONDecoder().decode(Entity<MyStruct>.self, from: json.data (using: .utf8)!)
        XCTAssertEqual (entity.id.uuidString, entity2.id.uuidString)
        XCTAssertEqual (entity.version, entity2.version)
        switch entity2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        entity.sync() { item in
            entity2.sync() { item2 in
                XCTAssertEqual (item.myInt, item2.myInt)
                XCTAssertEqual(item.myString, item2.myString)
            }
        }
    }
    
}
