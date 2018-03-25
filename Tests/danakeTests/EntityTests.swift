
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
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let entity1 = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (id.uuidString, entity1.getId().uuidString)
        XCTAssertEqual (10, entity1.getVersion())
        XCTAssertEqual (5, entity1.getSchemaVersion())
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
        XCTAssertTrue (entity1 === collection.cachedEntity(id: entity1.getId()))
        // Creation with itemClosure
        let id2 = UUID()
        let entity2 = Entity (collection: collection, id: id2, version: 20) { reference in
            return MyStruct (myInt: reference.version, myString: reference.id.uuidString)
        }
        XCTAssertEqual (id2.uuidString, entity2.getId().uuidString)
        XCTAssertEqual (20, entity2.getVersion())
        XCTAssertEqual (5, entity1.getSchemaVersion())
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
        XCTAssertTrue (entity2 === collection.cachedEntity(id: entity2.getId()))
    }

    func testSettersGetters() {
        // Creation with item
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let id = UUID()
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let entity = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (5, entity.getSchemaVersion())
        XCTAssertNil (entity.getSaved())
        let savedDate = Date()
        entity.setSaved(savedDate)
        XCTAssertEqual (savedDate.timeIntervalSince1970, entity.getSaved()!.timeIntervalSince1970)
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail ("expected .new")
        }
        entity.setPersistenceState (.dirty)
        switch entity.getPersistenceState() {
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
        let entity = newTestEntity(myInt: 0, myString: "0")
        var batch = EventuallyConsistentBatch()
        var itemInt = 0
        var itemString = ""
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        // sync: persistentState = .new
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 10
            item.myString = "10"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("10", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .dirty
        entity.setPersistenceState(.dirty)
        batch = EventuallyConsistentBatch()
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 20
            item.myString = "20"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(20, itemInt)
        XCTAssertEqual("20", itemString)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .persistent
        entity.setPersistenceState(.dirty)
        batch = EventuallyConsistentBatch()
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 30
            item.myString = "30"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(30, itemInt)
        XCTAssertEqual("30", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .abandoned
        entity.setPersistenceState(.abandoned)
        batch = EventuallyConsistentBatch()
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 40
            item.myString = "40"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(40, itemInt)
        XCTAssertEqual("40", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .pendingRemoval
        entity.setPersistenceState(.pendingRemoval)
        batch = EventuallyConsistentBatch()
        entity.sync(batch: batch) { (item: inout MyStruct) in
            item.myInt = 50
            item.myString = "50"
        }
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(50, itemInt)
        XCTAssertEqual("50", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .new
        entity.setPersistenceState(.new)
        var waitFor = expectation(description: ".new")
        entity.async(batch: batch) { (item: inout MyStruct) in
            item.myInt = 10
            item.myString = "10"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(10, itemInt)
        XCTAssertEqual("10", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .dirty
        waitFor = expectation(description: ".dirty")
        entity.setPersistenceState(.dirty)
        batch = EventuallyConsistentBatch()
        entity.async(batch: batch) { (item: inout MyStruct) in
            item.myInt = 20
            item.myString = "20"
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
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(20, itemInt)
        XCTAssertEqual("20", itemString)
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .persistent
        waitFor = expectation(description: ".persistetnt")
        entity.setPersistenceState(.dirty)
        batch = EventuallyConsistentBatch()
        entity.async(batch: batch) { (item: inout MyStruct) in
            item.myInt = 30
            item.myString = "30"
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
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(30, itemInt)
        XCTAssertEqual("30", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .abandoned
        waitFor = expectation(description: ".abandoned")
        entity.setPersistenceState(.abandoned)
        batch = EventuallyConsistentBatch()
        entity.async(batch: batch) { (item: inout MyStruct) in
            item.myInt = 40
            item.myString = "40"
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        entity.sync() { (item: MyStruct) in
            itemInt = item.myInt
            itemString = item.myString
        }
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        XCTAssertEqual(40, itemInt)
        XCTAssertEqual("40", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .pendingRemoval
        waitFor = expectation(description: ".pendingRemoval")
        entity.setPersistenceState(.pendingRemoval)
        batch = EventuallyConsistentBatch()
        entity.async(batch: batch) { (item: inout MyStruct) in
            item.myInt = 50
            item.myString = "50"
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
            XCTFail("Expected .dirty")
        }
        XCTAssertEqual(50, itemInt)
        XCTAssertEqual("50", itemString)
        
        batch.syncEntities() { (entities: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = entities[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
    }
    
    
    public let encoder: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .secondsSince1970
        return result
    }()
    
    public func decoder <D, T> (collection: PersistentCollection<D, T>) -> JSONDecoder {
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
        let wrongCollection = PersistentCollection<Database, String>(database: database, name: "wrongCollection")
        decoder.userInfo[Database.collectionKey] = wrongCollection
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch EntityDeserializationError<MyStruct>.NoCollectionInDecoderUserInfo {}
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        // With correct collection (success)
        decoder.userInfo[Database.collectionKey] = collection
        let entity1 = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
        XCTAssertTrue (entity1 === collection.cachedEntity(id: id1)!)
        XCTAssertEqual (id1.uuidString, entity1.getId().uuidString)
        XCTAssertEqual (5, entity1.getSchemaVersion()) // Schema version is taken from the collection, not the json
        XCTAssertEqual (10, entity1.getVersion() )
        switch entity1.getPersistenceState() {
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
        XCTAssertNil (entity1.getSaved())
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected exception")
        } catch EntityDeserializationError<MyStruct>.alreadyCached(let cachedEntity) {
            XCTAssertTrue (entity1 === cachedEntity)
        }
        try XCTAssertEqual ("{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":5,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}", String (data: accessor.encoder.encode(entity1), encoding: .utf8)!)
        // No Id
        json = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        json = "{\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.id, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key id (\\\"id\\\").\", underlyingError: nil))", "\(error)")
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
            XCTAssertEqual (id2.uuidString, entity2.getId().uuidString)
            XCTAssertEqual (5, entity2.getSchemaVersion())
            XCTAssertEqual (10, entity2.getVersion() )
            switch entity2.getPersistenceState() {
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
            XCTAssertEqual (id3.uuidString, entity3.getId().uuidString)
            XCTAssertEqual (5, entity3.getSchemaVersion())
            XCTAssertEqual (10, entity3.getVersion() )
            switch entity3.getPersistenceState() {
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
            XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.created, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key created (\\\"created\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Created
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\"AAA\",\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Double, Swift.DecodingError.Context(codingPath: [danake.Entity<danakeTests.MyStruct>.CodingKeys.created], debugDescription: \"Expected to decode Double but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // No Item
        // json = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.item, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key item (\\\"item\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Item
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"noInt\":100,\"noString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danakeTests.MyStruct.(CodingKeys in _254DF1EB8F8A26790A8445B53E121042).myInt, Swift.DecodingError.Context(codingPath: [danake.Entity<danakeTests.MyStruct>.CodingKeys.item], debugDescription: \"No value associated with key myInt (\\\"myInt\\\").\", underlyingError: nil))", "\(error)")
        }
        // No persistenceState
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.persistenceState, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key persistenceState (\\\"persistenceState\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal persistence state
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"illegal\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [danake.Entity<danakeTests.MyStruct>.CodingKeys.persistenceState], debugDescription: \"Cannot initialize PersistenceState from invalid String value illegal\", underlyingError: nil))", "\(error)")
        }
        // No version
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\"}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.version, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key version (\\\"version\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal version
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":\"AAA\"}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Int, Swift.DecodingError.Context(codingPath: [danake.Entity<danakeTests.MyStruct>.CodingKeys.version], debugDescription: \"Expected to decode Int but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // Illegal Json
        json = "\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [], debugDescription: \"The given data was not valid JSON.\", underlyingError: Optional(Error Domain=NSCocoaErrorDomain Code=3840 \"JSON text did not start with array or object and option to allow fragments not set.\" UserInfo={NSDebugDescription=JSON text did not start with array or object and option to allow fragments not set.})))", "\(error)")
        }
        // With illegal saved
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"saved\":\"AAA\",\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        do {
            let _ = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Double, Swift.DecodingError.Context(codingPath: [danake.Entity<danakeTests.MyStruct>.CodingKeys.saved], debugDescription: \"Expected to decode Double but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // With Saved
        let savedDateString = try jsonEncodedDate(date: Date())!
        json = "{\"id\":\"\(id4.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        do {
            let entity4 = try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!)
            XCTAssertTrue (entity4 === collection.cachedEntity(id: id4)!)
            XCTAssertEqual (id4.uuidString, entity4.getId().uuidString)
            XCTAssertEqual (5, entity4.getSchemaVersion())
            XCTAssertEqual (10, entity4.getVersion() )
            switch entity4.getPersistenceState() {
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
            try XCTAssertEqual (jsonEncodedDate (date: entity4.getSaved()!)!, savedDateString)
            try XCTAssertEqual ("{\"schemaVersion\":5,\"id\":\"\(id4.uuidString)\",\"saved\":\(savedDateString),\"created\":\(creationDateString1),\"version\":10,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}", String (data: accessor.encoder.encode(entity4), encoding: .utf8)!)
        }
    }
    
    func testAnyEntity() {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let anyEntity: AnyEntity = AnyEntity (item: entity)
        XCTAssertEqual (entity.getId(), anyEntity.getId())
        XCTAssertEqual (entity.getVersion(), anyEntity.getVersion())
        XCTAssertEqual (entity.getPersistenceState(), anyEntity.getPersistenceState())
        XCTAssertEqual (entity.getCreated(), anyEntity.getCreated())
        XCTAssertEqual (entity.getSaved(), anyEntity.getSaved())
    }

    func testEntityPersistenceWrapper() throws {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let wrapper: EntityPersistenceWrapper = EntityPersistenceWrapper (collectionName: entity.collection.name, item: entity)
        XCTAssertEqual (entity.getId(), wrapper.getId())
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
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.dirty)
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 30
            item.myString = "30"
        }
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.pendingRemoval)
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 40
            item.myString = "40"
        }
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.abandoned)
        action = PersistenceAction<MyStruct>.updateItem()  { item in
            item.myInt = 50
            item.myString = "50"
        }
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.saving)
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
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        entity.sync() { item in
            XCTAssertEqual (60, item.myInt)
            XCTAssertEqual ("60", item.myString)
        }

    }
    
    func testHandleActionRemove () {
        let entity = newTestEntity(myInt: 10, myString: "10")
        let action = PersistenceAction<MyStruct>.remove
        // persistenceState = .new
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.dirty)
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.pendingRemoval)
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.abandoned)
        entity.handleAction(action)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
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
        entity.setPersistenceState(.saving)
        entity.handleAction(action)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        switch entity.getPersistenceState() {
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
        switch entity.getPersistenceState() {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .dirty
        batch = EventuallyConsistentBatch()
        entity.setPersistenceState(.dirty)
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .pendingRemoval
        entity.setPersistenceState(.pendingRemoval)
        batch = EventuallyConsistentBatch()
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .abandoned
        entity.setPersistenceState(.abandoned)
        batch = EventuallyConsistentBatch()
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .saving
        entity.setPersistenceState(.saving)
        batch = EventuallyConsistentBatch()
        entity.remove(batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entity === entities[entity.getId()] as! Entity<MyStruct>)
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
    
    func testEntityCreation() {
        
    }
    

}

