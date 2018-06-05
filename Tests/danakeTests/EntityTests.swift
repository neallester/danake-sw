
//
//  EntityTests.swift
//  danakeTests
//
//  Created by Neal Lester on 1/26/18.
//

import XCTest
@testable import danake

class EntityTests: XCTestCase {
    
    func testCreation() {
        // Creation with item
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let id = UUID()
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = EntityCache<MyStruct> (database: database, name: "myCollection")
        let entity1 = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (id.uuidString, entity1.id.uuidString)
        XCTAssertEqual (10, entity1.version)
        XCTAssertEqual (5, entity1.getSchemaVersion())
        switch entity1.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity1.sync() { item in
            XCTAssertEqual (100, item.myInt)
            XCTAssertEqual ("Test String 1", item.myString)
        }
        XCTAssertTrue (entity1 === collection.cachedEntity(id: entity1.id))
        // Creation with itemClosure
        let id2 = UUID()
        let entity2 = Entity (collection: collection, id: id2, version: 20) { reference in
            return MyStruct (myInt: reference.version, myString: reference.id.uuidString)
        }
        XCTAssertEqual (id2.uuidString, entity2.id.uuidString)
        XCTAssertEqual (20, entity2.version)
        XCTAssertEqual (5, entity1.getSchemaVersion())
        switch entity2.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity2.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual (entity2.id.uuidString, item.myString)
        }
        XCTAssertTrue (entity2 === collection.cachedEntity(id: entity2.id))
    }

    func testSettersGetters() {
        // Creation with item
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let id = UUID()
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = EntityCache<MyStruct> (database: database, name: "myCollection")
        let entity = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (5, entity.getSchemaVersion())
        XCTAssertNil (entity.saved)
        let savedDate = Date()
        entity.saved = savedDate
        XCTAssertEqual (savedDate.timeIntervalSince1970, entity.saved!.timeIntervalSince1970)
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("expected .new")
        }
        entity.persistenceState = .dirty
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("expected .new")
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
        switch entity.persistenceState {
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
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
    }

    func testWriteAccess() {
        let entity = newTestEntity(myInt: 0, myString: "0")
        var batch = EventuallyConsistentBatch()
        var itemInt = 0
        var itemString = ""
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        // sync: persistentState = .new
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 10
            item.myString = "10"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("10", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .dirty
        entity.persistenceState = .dirty
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 20
            item.myString = "20"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(20, itemInt)
        XCTAssertEqual("20", itemString)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .persistent
        entity.persistenceState = .dirty
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 30
            item.myString = "30"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(30, itemInt)
        XCTAssertEqual("30", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .abandoned
        entity.persistenceState = .abandoned
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 40
            item.myString = "40"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(40, itemInt)
        XCTAssertEqual("40", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .pendingRemoval
        entity.persistenceState = .pendingRemoval
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 50
            item.myString = "50"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(50, itemInt)
        XCTAssertEqual("50", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .new
        entity.persistenceState = .new
        var waitFor = expectation(description: ".new")
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 10
            item.myString = "10"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("10", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .dirty
        waitFor = expectation(description: ".dirty")
        entity.persistenceState = .dirty
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 20
            item.myString = "20"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(20, itemInt)
        XCTAssertEqual("20", itemString)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .persistent
        waitFor = expectation(description: ".persistetnt")
        entity.persistenceState = .dirty
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 30
            item.myString = "30"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(30, itemInt)
        XCTAssertEqual("30", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .abandoned
        waitFor = expectation(description: ".abandoned")
        entity.persistenceState = .abandoned
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 40
            item.myString = "40"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(40, itemInt)
        XCTAssertEqual("40", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .pendingRemoval
        waitFor = expectation(description: ".pendingRemoval")
        entity.persistenceState = .pendingRemoval
        batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { (item: inout MyStruct) in
            item.myInt = 50
            item.myString = "50"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(50, itemInt)
        XCTAssertEqual("50", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.version)
            let retrievedEntity = entities[entity.id]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
    }
    
    func testSetDirty() {
        // Creation with item
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = EntityCache<MyStruct> (database: database, name: "myCollection")
        var batch = EventuallyConsistentBatch()
        let waitFor = expectation(description: "wait1")
        let entity = collection.new(batch: batch, item: myStruct)
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch entity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail("Expected .persistent")
        }
        entity.setDirty(batch: batch)
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        batch.syncEntities() { entities in
            XCTAssertTrue (entities[entity.id] as! Entity<MyStruct> === entity)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.setDirty(batch: batch)
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        batch.syncEntities() { entities in
            XCTAssertTrue (entities[entity.id] as! Entity<MyStruct> === entity)
        }
        XCTAssertNil (entity.getPendingAction())
        batch = EventuallyConsistentBatch()
        entity.persistenceState = .saving
        entity.setDirty(batch: batch)
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail("Expected .saving")
        }
        batch.syncEntities() { entities in
            XCTAssertTrue (entities[entity.id] as! Entity<MyStruct> === entity)
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
    }

    public let encoder: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .secondsSince1970
        return result
    }()
    
    public func decoder <T> (collection: EntityCache<T>) -> JSONDecoder {
        let result = JSONDecoder()
        result.dateDecodingStrategy = .secondsSince1970
        result.userInfo[Database.collectionKey] = collection
        return result
    }
    
    func testEncodeDecode() throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .secondsSince1970
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .secondsSince1970
        let creationDateString1 = try! jsonEncodedDate (date: Date())!
        let id1 = UUID()
        // No Collection
        var json = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch EntityDeserializationError<MyStruct>.NoCollectionInDecoderUserInfo {}
        // Wrong collection
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let wrongCollection = EntityCache<String>(database: database, name: "wrongCollection")
        decoder.userInfo[Database.collectionKey] = wrongCollection
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch EntityDeserializationError<MyStruct>.NoCollectionInDecoderUserInfo {}
        let collection = EntityCache<MyStruct>(database: database, name: standardCollectionName)
        // With correct collection (success)
        decoder.userInfo[Database.collectionKey] = collection
        let entity1 = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
        XCTAssertTrue (entity1 === collection.cachedEntity(id: id1)!)
        XCTAssertEqual (id1.uuidString, entity1.id.uuidString)
        XCTAssertEqual (5, entity1.getSchemaVersion()) // Schema version is taken from the collection, not the json
        XCTAssertEqual (10, entity1.version )
        switch entity1.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        entity1.sync() { item in
            XCTAssertEqual (100, item.myInt)
            XCTAssertEqual("A \"Quoted\" String", item.myString)
        }
        try XCTAssertEqual (jsonEncodedDate (date: entity1.created)!, creationDateString1)
        XCTAssertNil (entity1.saved)
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected exception")
        } catch EntityDeserializationError<MyStruct>.alreadyCached(let cachedEntity) {
            XCTAssertTrue (entity1 === cachedEntity)
        }
        #if os(Linux)
            json = try String (data: accessor.encoder.encode(entity1), encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(id1.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            XCTAssertTrue (json.contains("\"created\":\(creationDateString1)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":100"))
            XCTAssertTrue (json.contains("\"myString\":\"A \\\"Quoted\\\" String\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"new\""))
            XCTAssertTrue (json.contains("\"version\":10"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":5,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}", String (data: accessor.encoder.encode(entity1), encoding: .utf8)!)
        #endif
        // No Id
        json = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        json = "{\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", "\(error)")
        }
        // Invalid id
        json = "{\"id\":\"AAA\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch EntityDeserializationError<MyStruct>.illegalId(let idString){
            XCTAssertEqual ("AAA", idString)
        }
        // No schemaVersion
        let id2 = UUID()
        json = "{\"id\":\"\(id2.uuidString)\",\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let entity2 = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTAssertTrue (entity2 === collection.cachedEntity(id: id2)!)
            XCTAssertEqual (id2.uuidString, entity2.id.uuidString)
            XCTAssertEqual (5, entity2.getSchemaVersion())
            XCTAssertEqual (10, entity2.version )
            switch entity2.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            entity2.sync() { item in
                XCTAssertEqual (100, item.myInt)
                XCTAssertEqual("A \"Quoted\" String", item.myString)
            }
        }
        // Illegal Schema Version
        let id3 = UUID()
        json = "{\"id\":\"\(id3.uuidString)\",\"schemaVersion\":\"A\",\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let entity3 = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTAssertTrue (entity3 === collection.cachedEntity(id: id3)!)
            XCTAssertEqual (id3.uuidString, entity3.id.uuidString)
            XCTAssertEqual (5, entity3.getSchemaVersion())
            XCTAssertEqual (10, entity3.version )
            switch entity3.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            entity3.sync() { item in
                XCTAssertEqual (100, item.myInt)
                XCTAssertEqual("A \"Quoted\" String", item.myString)
            }
        }
        // No Created
        let id4 = UUID()
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"created\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"created\\\", intValue: nil) (\\\"created\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Created
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\"AAA\",\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Double, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"created\", intValue: nil)], debugDescription: \"Expected to decode Double but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // No Item
        // json = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"item\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"item\\\", intValue: nil) (\\\"item\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Item
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"noInt\":100,\"noString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"myInt\", intValue: nil), Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"item\", intValue: nil)], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"myInt\\\", intValue: nil) (\\\"myInt\\\").\", underlyingError: nil))", "\(error)")
        }
        // No persistenceState
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"persistenceState\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"persistenceState\\\", intValue: nil) (\\\"persistenceState\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal persistence state
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"illegal\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"persistenceState\", intValue: nil)], debugDescription: \"Cannot initialize PersistenceState from invalid String value illegal\", underlyingError: nil))", "\(error)")
        }
        // No version
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\"}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"version\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"version\\\", intValue: nil) (\\\"version\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal version
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":\"AAA\"}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Int, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"version\", intValue: nil)], debugDescription: \"Expected to decode Int but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // Illegal Json
        json = "\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            #if os(Linux)
                XCTAssertEqual ("The operation could not be completed", "\(error)")
            #else
                XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [], debugDescription: \"The given data was not valid JSON.\", underlyingError: Optional(Error Domain=NSCocoaErrorDomain Code=3840 \"JSON text did not start with array or object and option to allow fragments not set.\" UserInfo={NSDebugDescription=JSON text did not start with array or object and option to allow fragments not set.})))", "\(error)")
            #endif
        }
        // With illegal saved
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"saved\":\"AAA\",\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Double, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"saved\", intValue: nil)], debugDescription: \"Expected to decode Double but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // With Saved
        let savedDateString = try jsonEncodedDate(date: Date())!
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        do {
            let entity4 = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTAssertTrue (entity4 === collection.cachedEntity(id: id4)!)
            XCTAssertEqual (id4.uuidString, entity4.id.uuidString)
            XCTAssertEqual (5, entity4.getSchemaVersion())
            XCTAssertEqual (10, entity4.version )
            switch entity4.persistenceState {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            entity4.sync() { item in
                XCTAssertEqual (100, item.myInt)
                XCTAssertEqual("A \"Quoted\" String", item.myString)
            }
            try XCTAssertEqual (jsonEncodedDate (date: entity4.created)!, creationDateString1)
            try XCTAssertEqual (jsonEncodedDate (date: entity4.saved!)!, savedDateString)
            #if os(Linux)
                json = try String (data: accessor.encoder.encode(entity4), encoding: .utf8)!
                XCTAssertTrue (json.contains("\"schemaVersion\":5"))
                XCTAssertTrue (json.contains("\"id\":\"\(id4.uuidString)\""))
                XCTAssertTrue (json.contains("\"saved\":\(savedDateString)"))
                XCTAssertTrue (json.contains("\"created\":\(creationDateString1)"))
                XCTAssertTrue (json.contains("\"version\":10"))
                XCTAssertTrue (json.contains("\"item\":{"))
                XCTAssertTrue (json.contains("\"myInt\":100"))
                XCTAssertTrue (json.contains("\"myString\":\"A \\\"Quoted\\\" String\""))
                XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            #else
                try XCTAssertEqual ("{\"schemaVersion\":5,\"id\":\"\(id4.uuidString)\",\"saved\":\(savedDateString),\"created\":\(creationDateString1),\"version\":10,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}", String (data: accessor.encoder.encode(entity4), encoding: .utf8)!)
            #endif
        }
    }
    
    func testDecodeReferenceManager() throws {
        
        class EntityReferenceContainer : Codable {
            
            init (parentData: EntityReferenceData<EntityReferenceContainer>) {
                entityReference = ReferenceManager<EntityReferenceContainer, MyStruct> (parent: parentData, entity: nil)
            }
            
            internal let entityReference: ReferenceManager<EntityReferenceContainer, MyStruct>
        }
        
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let json = "{\"id\":\"438AF59B-0CC1-46C1-8C73-336BDC2AA606\",\"schemaVersion\":5,\"created\":543967928.51027906,\"item\":{\"entityReference\":{\"isEager\":false,\"isNil\":true}},\"version\":10,\"persistenceState\":\"persistent\"}"
        let parentId = UUID (uuidString: "438AF59B-0CC1-46C1-8C73-336BDC2AA606")!
        let json2 = "{\"id\":\"BE6458D5-8762-4AEF-9748-94870B0BBCB1\",\"schemaVersion\":5,\"created\":1522279213.187017,\"item\":{\"entityReference\":{\"qualifiedCollectionName\":\"\(accessor.hashValue()).childCollection\",\"id\":\"A7E75632-9780-42EE-BD4C-6D4A61943285\",\"isEager\":false,\"version\":3}},\"persistenceState\":\"new\",\"version\":0}"
        let parentId2 = UUID (uuidString: "BE6458D5-8762-4AEF-9748-94870B0BBCB1")!
        let parentCollection = EntityCache<EntityReferenceContainer> (database: database, name: "parentCollection")
        let _ = accessor.add(name: parentCollection.name, id: parentId, data: json.data(using: .utf8)!)
        let _ = accessor.add(name: parentCollection.name, id: parentId2, data: json2.data(using: .utf8)!)
        let parent = parentCollection.get(id: parentId).item()!
        var parentVersion = parent.version
        parent.sync() { item in
            XCTAssertNotNil(item.entityReference)
            item.entityReference.sync() { reference in
                XCTAssertNil (reference.entity)
                XCTAssertTrue (reference.parent === parent)
                XCTAssertTrue (reference.parentData.collection === parentCollection)
                XCTAssertTrue (reference.parentData.id.uuidString == parent.id.uuidString)
                XCTAssertEqual (parentVersion, reference.parentData.version)
                XCTAssertNil (reference.referenceData)
                switch reference.state {
                case .loaded:
                    break
                default:
                    XCTFail ("Expected .loaded")
                }
                XCTAssertFalse (reference.isEager)
                XCTAssertEqual (0, reference.pendingEntityClosureCount)
            }

        }
        let parent2 = parentCollection.get(id: parentId2).item()!
        parentVersion = parent2.version
        parent2.sync() { item in
            XCTAssertNotNil(item.entityReference)
            item.entityReference.sync() { reference in
                XCTAssertNil (reference.entity)
                XCTAssertTrue (reference.parent === parent2)
                XCTAssertTrue (reference.parentData.collection === parentCollection)
                XCTAssertTrue (reference.parentData.id.uuidString == parentId2.uuidString)
                XCTAssertEqual (parentVersion, reference.parentData.version)
                XCTAssertEqual (Database.qualifiedCollectionName(databaseHash: accessor.hashValue(), collectionName: "childCollection") , reference.referenceData!.qualifiedCollectionName)
                XCTAssertEqual ("A7E75632-9780-42EE-BD4C-6D4A61943285", reference.referenceData!.id.uuidString)
                XCTAssertEqual (3, reference.referenceData!.version)
                switch reference.state {
                case .decoded:
                    break
                default:
                    XCTFail ("Expected .decoded")
                }
                XCTAssertFalse (reference.isEager)
                XCTAssertEqual (0, reference.pendingEntityClosureCount)
            }
        }
    }
    
    func testAnyEntity() {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let anyEntity: AnyEntity = AnyEntity (entity)
        XCTAssertEqual (entity.id, anyEntity.id)
        XCTAssertEqual (entity.version, anyEntity.version)
        XCTAssertEqual (entity.persistenceState, anyEntity.persistenceState)
        XCTAssertEqual (entity.created, anyEntity.created)
        XCTAssertEqual (entity.saved, anyEntity.saved)
    }

    func testEntityPersistenceWrapper() throws {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let wrapper: EntityPersistenceWrapper = EntityPersistenceWrapper (collectionName: entity.collection.name, item: entity)
        XCTAssertEqual (entity.id, wrapper.id)
        XCTAssertEqual (entity.collection.name, wrapper.collectionName)
        let encoder = JSONEncoder()
        let entityData = try encoder.encode(entity)
        let wrapperData = try encoder.encode (wrapper)
        let entityJson = String (data: entityData, encoding: .utf8)
        let wrapperJson = String (data: wrapperData, encoding: .utf8)
        XCTAssertEqual (entityJson, wrapperJson)
    }
    
    func testHandleActionUpdateItem () {
        let entity = newTestEntity(myInt: 10, myString: "10")
        var action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 20
            item.myString = "20"
        }
        // persistenceState = .new
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        // persistenceState = .dirty
        entity.persistenceState = .dirty
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 30
            item.myString = "30"
        }
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        // persistenceState = .pendingRemoval
        entity.persistenceState = .pendingRemoval
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 40
            item.myString = "40"
        }
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        // persistenceState = .abandoned
        entity.persistenceState = .abandoned
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 50
            item.myString = "50"
        }
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        entity.sync() { item in
            XCTAssertEqual (50, item.myInt)
            XCTAssertEqual ("50", item.myString)
        }
        // persistenceState = .saving
        entity.persistenceState = .saving
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 60
            item.myString = "60"
        }
        entity.handleAction(action)
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        entity.sync() { item in
            XCTAssertEqual (60, item.myInt)
            XCTAssertEqual ("60", item.myString)
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
    }

    func testHandleActionSetDirty () {
        let entity = newTestEntity(myInt: 10, myString: "10")
        var action = PersistenceAction<MyStruct>.setDirty
        // persistenceState = .new
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .dirty
        entity.persistenceState = .dirty
        action = PersistenceAction<MyStruct>.setDirty
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .pendingRemoval
        entity.persistenceState = .pendingRemoval
        action = PersistenceAction<MyStruct>.setDirty
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .abandoned
        entity.persistenceState = .abandoned
        action = PersistenceAction<MyStruct>.setDirty
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .saving
        entity.persistenceState = .saving
        action = PersistenceAction<MyStruct>.setDirty
        entity.handleAction(action)
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
    }

    
    
    func testHandleActionRemove () {
        let entity = newTestEntity(myInt: 10, myString: "10")
        let action = PersistenceAction<MyStruct>.remove
        // persistenceState = .new
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .dirty
        entity.persistenceState = .dirty
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .pendingRemoval
        entity.persistenceState = .pendingRemoval
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .abandoned
        entity.persistenceState = .abandoned
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .saving
        entity.persistenceState = .saving
        entity.handleAction(action)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
    }

    func testRemove () {
        let entity = newTestEntity(myInt: 10, myString: "10")
        var batch = EventuallyConsistentBatch()
        // persistenceState = .new
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.id] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .dirty
        batch = EventuallyConsistentBatch()
        entity.persistenceState = .dirty
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.id] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .pendingRemoval
        entity.persistenceState = .pendingRemoval
        batch = EventuallyConsistentBatch()
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.id] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .abandoned
        entity.persistenceState = .abandoned
        batch = EventuallyConsistentBatch()
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.id] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .saving
        entity.persistenceState = .saving
        batch = EventuallyConsistentBatch()
        entity.remove(batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.id] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
    }
    
    func testAsData() {
        let entity = newTestEntity(myInt: 10, myString: "10")
        let encoder = JSONEncoder()
        try! XCTAssertEqual (encoder.encode (entity), entity.asData(encoder: encoder))
    }
    
    func testPersistenceStatePair() {
        let pair = PersistenceStatePair (success: .persistent, failure: .dirty)
        switch pair.success {
        case .persistent:
            break
        default:
            XCTFail("Expected .persistent")
        }
        switch pair.failure {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
    }
    
    func testEntityReferenceData() {
        let entity = newTestEntity(myInt: 10, myString: "10")
        let entity2 = newTestEntity(myInt: 20, myString: "20")
        let data1 = EntityReferenceData (collection: entity.collection, id: entity.id, version: entity.version)
        var data2 = EntityReferenceData (collection: entity.collection, id: entity.id, version: entity.version)
        XCTAssertEqual (data1, data2)
        data2 = EntityReferenceData (collection: entity2.collection, id: entity.id, version: entity.version)
        XCTAssertNotEqual (data1, data2)
        data2 = EntityReferenceData (collection: entity.collection, id: entity2.id, version: entity.version)
        XCTAssertNotEqual (data1, data2)
        data2 = EntityReferenceData (collection: entity.collection, id: entity.id, version: 2)
        XCTAssertNotEqual (data1, data2)
    }
    
    func testReferenceManagerSerializationData() {
        let id = UUID()
        var data = ReferenceManagerData (databaseId: "dbId", collectionName: "collectionName", id: id, version: 10)
        XCTAssertEqual (Database.qualifiedCollectionName(databaseHash: "dbId", collectionName: "collectionName"), data.qualifiedCollectionName)
        XCTAssertEqual (id.uuidString, data.id.uuidString)
        XCTAssertEqual (10, data.version)
        let data2 = ReferenceManagerData (databaseId: "dbId", collectionName: "collectionName", id: id, version: 10)
        XCTAssertEqual (data, data2)
        data = ReferenceManagerData (databaseId: "dbId1", collectionName: "collectionName", id: id, version: 10)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (databaseId: "dbId", collectionName: "collectionName1", id: id, version: 10)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (databaseId: "dbId", collectionName: "collectionName", id: UUID(), version: 10)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (databaseId: "dbId", collectionName: "collectionName", id: id, version: 11)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (qualifiedCollectionName: Database.qualifiedCollectionName(databaseHash: "dbId1", collectionName: "collectionName"), id: id, version: 10)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (qualifiedCollectionName: Database.qualifiedCollectionName(databaseHash: "dbId", collectionName: "collectionName1"), id: id, version: 10)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (qualifiedCollectionName: Database.qualifiedCollectionName(databaseHash: "dbId", collectionName: "collectionName"), id: UUID(), version: 10)
        XCTAssertNotEqual(data, data2)
        data = ReferenceManagerData (qualifiedCollectionName: Database.qualifiedCollectionName(databaseHash: "dbId", collectionName: "collectionName"), id: id, version: 11)
        XCTAssertNotEqual(data, data2)
        let entity = newTestEntity(myInt: 10, myString: "20")
        data = entity.referenceData()
        XCTAssertEqual (entity.collection.qualifiedName, data.qualifiedCollectionName)
        XCTAssertEqual (entity.id, data.id)
        XCTAssertEqual (entity.version, data.version)
    }
    
    func testReferenceManagerCycle() {
        
        class Node : Codable {
            
            init (parentData: EntityReferenceData<Node>) {
                parent = ReferenceManager<Node, Node> (parent: parentData, entity: nil)
            }
            
            init (parentData: EntityReferenceData<Node>, child: Entity<Node>) {
                parent = ReferenceManager<Node, Node> (parent: parentData, entity: child)
            }

            let parent: ReferenceManager<Node, Node>
            
        }
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 1, logger: nil)
        let collection = EntityCache<Node>(database: database, name: "node")
        let batch = EventuallyConsistentBatch()
        var parent: Entity<Node>? = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData)
        }
        var timeout = Date().timeIntervalSince1970 + 10.0
        var referenceCount = 0
        while referenceCount == 0 {
            usleep(100)
            parent!.referenceContainers() { references in
                referenceCount = references.count
            }
        }
        parent!.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
        }
        batch.commitSync()
        let parentId = parent!.id.uuidString
        collection.sync() { entities in
            XCTAssertEqual (1, entities.count)
        }
        parent = nil
        collection.sync() { entities in
            XCTAssertEqual (0, entities.count)
        }
        parent = collection.get(id: UUID (uuidString: parentId)!).item()!
        var child: Entity<Node>? = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData)
        }
        let childId = child!.id.uuidString
        child!.update(batch: batch) { node in
            node.parent.set(entity: parent, batch: batch)
        }
        batch.commitSync()
        collection.sync() { entities in
            XCTAssertEqual (2, entities.count)
        }
        parent = nil
        collection.sync() { entities in
            XCTAssertEqual (2, entities.count)
        }
        child = nil
        var collectionHasEntities = true
        timeout = Date().timeIntervalSince1970 + 30.0
        while (collectionHasEntities && Date().timeIntervalSince1970 < timeout) {
            usleep (100)
            collection.sync() { entities in
                collectionHasEntities = entities.count > 0
            }
            
        }
        collection.sync() { entities in
            XCTAssertEqual (0, entities.count)
        }
        child = collection.get(id: UUID (uuidString: childId)!).item()!
        child?.sync() { node in
            parent = node.parent.get().item()!
        }
        // Set up a reference cycle;
        // child.parent = parent
        // parent.parent = child
        parent!.update(batch: batch) { node in
            node.parent.set (entity: child, batch: batch)
        }
        batch.commitSync()
        child = nil
        parent = nil
        collection.sync() { entities in
            XCTAssertEqual (2, entities.count)
        }
        child = collection.get(id: UUID (uuidString: childId)!).item()!
        child!.sync() { node in
            parent = node.parent.get().item()!
        }
        parent!.sync() { node in
            child = node.parent.get().item()!
        }
        child!.breakReferences()
        child = nil
        parent!.breakReferences()
        parent = nil
        collectionHasEntities = true
        timeout = Date().timeIntervalSince1970 + 30.0
        while (collectionHasEntities && Date().timeIntervalSince1970 < timeout) {
            usleep (100)
            collection.sync() { entities in
                collectionHasEntities = entities.count > 0
            }

        }
        collection.sync() { entities in
            XCTAssertEqual (0, entities.count)
        }
        child = collection.get(id: UUID (uuidString: childId)!).item()!
        child!.sync() { node in
            parent = node.parent.get().item()!
        }
        parent!.sync() { node in
            child = node.parent.get().item()!
        }
        collection.sync() { entities in
            XCTAssertEqual (2, entities.count)
        }
        // Test Recursive Dereferencing
        child!.breakReferencesRecursive()
        var wereBothDereferenced = false
        timeout = Date().timeIntervalSince1970 + 30.0
        while (!wereBothDereferenced && Date().timeIntervalSince1970 < timeout) {
            var parentWasDereferenced = false
            var childWasDereferenced = false
            parent!.sync() { node in
                node.parent.sync() { reference in
                    switch reference.state {
                    case .dereferenced:
                        parentWasDereferenced = true
                    default:
                        break
                    }
                }
            }
            child!.sync() { node in
                node.parent.sync() { reference in
                    switch reference.state {
                    case .dereferenced:
                        childWasDereferenced = true
                    default:
                        break
                    }
                }
            }
            wereBothDereferenced = parentWasDereferenced && childWasDereferenced
            if !wereBothDereferenced {
                usleep (100)
            }
        }
        XCTAssertTrue (wereBothDereferenced)
        child = nil
        parent = nil
        collectionHasEntities = true
        timeout = Date().timeIntervalSince1970 + 30.0
        while (collectionHasEntities && Date().timeIntervalSince1970 < timeout) {
            usleep (100)
            collection.sync() { entities in
                collectionHasEntities = entities.count > 0
            }
            
        }
        collection.sync() { entities in
            XCTAssertEqual (0, entities.count)
        }
        // Test creation of parent with existing child
        // Registers the ReferenceManager with parent
        let child2: Entity<Node> = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData)
        }
        let parent2: Entity<Node> = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData, child: child2)
        }
        var childReference: ReferenceManager<Node, Node>? = nil
        parent2.sync() { node in
            childReference = node.parent
        }
        var collectionHasOnCache = true
        timeout = Date().timeIntervalSince1970 + 30.0
        while (collectionHasOnCache && Date().timeIntervalSince1970 < timeout) {
            usleep (100)
            collectionHasOnCache = collection.onCacheCount() > 0
        }
        parent2.referenceContainers() { containers in
            XCTAssertEqual (1, containers.count)
            XCTAssertTrue (containers[0] as! ReferenceManager<Node, Node> === childReference)
        }
        
    }
    
    func testTwoReferences () {
        
        class Node : Codable {
            
            init (parentData: EntityReferenceData<Node>) {
                n1 = ReferenceManager<Node, Node> (parent: parentData, entity: nil)
                n2 = ReferenceManager<Node, Node> (parent: parentData, entity: nil)
            }
            
            init (parentData: EntityReferenceData<Node>, n1: Entity<Node>?, n2: Entity<Node>?) {
                self.n1 = ReferenceManager<Node, Node> (parent: parentData, entity: n1)
                self.n2 = ReferenceManager<Node, Node> (parent: parentData, entity: n2)
            }
            
            let n1: ReferenceManager<Node, Node>
            let n2: ReferenceManager<Node, Node>
            
        }
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 1, logger: nil)
        let collection = EntityCache<Node>(database: database, name: "node")
        let batch = EventuallyConsistentBatch()
        var parent: Entity<Node>? = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData)
        }
        var timeout = Date().timeIntervalSince1970 + 10.0
        var referenceCount = 0
        while referenceCount != 2 && Date().timeIntervalSince1970 < timeout {
            usleep(100)
            parent!.referenceContainers() { references in
                referenceCount = references.count
            }
        }
        var ref1: ReferenceManager<Node, Node>? = nil
        var ref2: ReferenceManager<Node, Node>? = nil
        parent?.sync() { node in
            ref1 = node.n1
            ref2 = node.n2
        }
        parent!.referenceContainers() { references in
            XCTAssertEqual (2, references.count)
            var found1 = false
            var found2 = false
            for reference in references {
                if let reference = reference as? ReferenceManager<Node, Node>, reference === ref1 {
                    found1 = true
                }
                if let reference = reference as? ReferenceManager<Node, Node>, reference === ref2 {
                    found2 = true
                }
            }
            XCTAssertTrue (found1)
            XCTAssertTrue (found2)
        }
        batch.commitSync()
        let c1: Entity<Node> = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData)
        }
        let c2: Entity<Node> = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData)
        }
        parent = collection.new(batch: batch) { parentData in
            return Node (parentData: parentData, n1: c1, n2: c2)
        }
        timeout = Date().timeIntervalSince1970 + 10.0
        referenceCount = 0
        while referenceCount != 2 && Date().timeIntervalSince1970 < timeout {
            usleep(100)
            parent!.referenceContainers() { references in
                referenceCount = references.count
            }
        }
        ref1 = nil
        ref2 = nil
        parent?.sync() { node in
            ref1 = node.n1
            ref2 = node.n2
        }
        parent!.referenceContainers() { references in
            XCTAssertEqual (2, references.count)
            var found1 = false
            var found2 = false
            for reference in references {
                if let reference = reference as? ReferenceManager<Node, Node>, reference === ref1 {
                    found1 = true
                }
                if let reference = reference as? ReferenceManager<Node, Node>, reference === ref2 {
                    found2 = true
                }
            }
            XCTAssertTrue (found1)
            XCTAssertTrue (found2)
        }
        batch.commitSync()
    }

    func testRegisterReferenceContainer() {
        let entity = newTestEntity(myInt: 10, myString: "10")

        class TestContainer : ReferenceManagerContainer {
            func dereference() {}
            func dereferenceRecursive() {}
        }
        
        let container1 = TestContainer()
        let container2 = TestContainer()
        
        entity.registerReferenceContainer(container1)
        entity.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (container1 === references[0] as! TestContainer)
        }
        entity.registerReferenceContainer(container2)
        entity.referenceContainers() { references in
            XCTAssertEqual (2, references.count)
            XCTAssertTrue (container1 === references[0] as! TestContainer)
            XCTAssertTrue (container2 === references[1] as! TestContainer)
        }
    }
    
    func testUnsavedChangesLogging() {
        
        class SneakyUpdater : Codable {
            
            var label: String = ""
            
            func setLabel (_ label: String) {
                self.label = label
            }
        }
        
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger (level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 1, logger: logger)
        let collection = EntityCache<SneakyUpdater>(database: database, name: "sneak")
        let batch = EventuallyConsistentBatch()
        var updaterEntity: Entity<SneakyUpdater>? = collection.new(batch: batch, item: SneakyUpdater())
        let updaterId = updaterEntity!.id.uuidString
        updaterEntity!.update(batch: batch) { updater in
            updater.setLabel ("1")
        }
        batch.commitSync()
        updaterEntity!.sync() { updater in
            XCTAssertEqual ("1", updater.label)
        }
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
        updaterEntity!.sync() { updater in
            updater.setLabel("2")
        }
        updaterEntity = nil
        var loggerCount = 0
        let timeout = Date().timeIntervalSince1970 + 10.0
        while loggerCount != 1 && Date().timeIntervalSince1970 < timeout {
            logger.sync() { entries in
                loggerCount = entries.count
            }
        }
        logger.sync() { entries in
            XCTAssertEqual ("ERROR|Entity<SneakyUpdater #1>.Type.deinit|lostData:itemModifiedWithoutSave|collectionName=\(collection.qualifiedName);entityId=\(updaterId)", entries[0].asTestString())
        }
    }
    
}

