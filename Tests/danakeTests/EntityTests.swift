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
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
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
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let entity1 = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (id.uuidString, entity1.getId().uuidString)
        XCTAssertEqual (10, entity1.getVersion())
        XCTAssertEqual (5, entity1.schemaVersion)
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
        XCTAssertEqual (5, entity1.schemaVersion)
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
        let accessor = InMemoryAccessor()
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        var json = try String (data: accessor.encoder().encode(entity), encoding: .utf8)!
        try XCTAssertEqual("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":0}", json)
        try json = "{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        var entity2 = try accessor.decoder().decode(Entity<MyStruct>.self, from: json.data (using: .utf8)!)
        XCTAssertEqual (entity.id.uuidString, entity2.id.uuidString)
        XCTAssertEqual (5, entity2.schemaVersion)
        XCTAssertEqual (10, entity2.version)
        switch entity2.getPersistenceState() {
        case .new:
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
        XCTAssertEqual ((entity.created.timeIntervalSince1970 * 1000.0).rounded(), (entity2.created.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db
        // With a saved time
        let savedTime = Date()
        entity.saved = savedTime
        entity.persistenceState = .persistent
        json = try String (data: accessor.encoder().encode(entity), encoding: .utf8)!
        try XCTAssertEqual("{\"schemaVersion\":5,\"id\":\"\(entity.id.uuidString)\",\"saved\":\(jsonEncodedDate(date: entity.saved!)!),\"created\":\(jsonEncodedDate(date: entity.created)!),\"version\":0,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}", json)
        try json = "{\"schemaVersion\":5,\"id\":\"\(entity.id.uuidString)\",\"saved\":\(jsonEncodedDate(date: entity.saved!)!),\"created\":\(jsonEncodedDate(date: entity.created)!),\"version\":10,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}"
        //
        entity2 = try accessor.decoder().decode(Entity<MyStruct>.self, from: json.data (using: .utf8)!)
        XCTAssertEqual (entity.id.uuidString, entity2.id.uuidString)
        XCTAssertEqual (5, entity2.schemaVersion)
        XCTAssertEqual (10, entity2.version)
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
        XCTAssertEqual ((entity.created.timeIntervalSince1970 * 1000.0).rounded(), (entity2.created.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db
        XCTAssertEqual ((entity.saved!.timeIntervalSince1970 * 1000.0).rounded(), (entity2.saved!.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db

    }
    
    func testUpdateStatement() {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        XCTAssertEqual (0, entity.getVersion())
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        let result = entity.updateStatement() { (name: CollectionName, entity: AnyEntityManagement) -> EntityConversionResult<Data> in
            do {
                let accessor = InMemoryAccessor()
                let result = try accessor.encoder().encode (entity)
                return .ok(result)
            } catch {
                return .error ("\(error)")
            }
        }
        switch result {
        case .ok (let data):
            try XCTAssertEqual ("{\"schemaVersion\":5,\"id\":\"\(entity.getId())\",\"saved\":\(jsonEncodedDate(date: entity.saved!)!),\"created\":\(jsonEncodedDate(date: entity.created)!),\"version\":1,\"item\":{\"myInt\":10,\"myString\":\"A String\"},\"persistenceState\":\"persistent\"}", String (data: data, encoding: .utf8))
            break
        default:
            XCTFail()
        }
        XCTAssertEqual (1, entity.getVersion())
        switch entity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail("Expected .persistent")
        }
       // There is no convenient way to test the nil collection error condition, which as it should be
    }
    
    func testRemoveStatement() {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        XCTAssertEqual (0, entity.getVersion())
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        let result = entity.removeStatement() { (name: CollectionName, entity: AnyEntityManagement) -> EntityConversionResult<String> in
            return .ok("remove:collection=\(name);id=\(entity.getId())")
        }
        switch result {
        case .ok (let statement):
            XCTAssertEqual ("remove:collection=myCollection;id=\(entity.getId().uuidString)", statement)
            break
        default:
            XCTFail()
        }
        XCTAssertEqual (1, entity.getVersion())
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        // There is no convenient way to test the nil collection error condition, which as it should be
    }
    
    // JSONEncoder uses its own inscrutable rounding process for encoding dates, so this is what is necessary to reliably get the expected value of a date in a json encoded object
    func jsonEncodedDate (date: Date) throws -> String? {
        let accessor = InMemoryAccessor()
        struct DateContainer : Encodable {
            init (_ d: Date) {
                self.d = d
            }
            let d: Date
        }
        let container = DateContainer.init(date)
        let encoded = try accessor.encoder().encode (container)
        let protoResult = String (data: encoded, encoding: .utf8)
        var result: String? = nil
        if let protoResult = protoResult {
            result = String (protoResult[protoResult.index (protoResult.startIndex, offsetBy: 5)...])
            result = String (result!.prefix(result!.count - 1))
        }
        return result
    }
}

