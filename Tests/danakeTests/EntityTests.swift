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
    }

    func testEntitySettersGetters() {
        // Creation with item
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let id = UUID()
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let entity = Entity (collection: collection, id: id, version: 10, item: myStruct)
        XCTAssertEqual (5, entity.getSchemaVersion())
        entity.setSchemaVersion (3)
        XCTAssertEqual (3, entity.getSchemaVersion())
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

    func testEntitySetCollection() {
        var myStruct = MyStruct()
        myStruct.myInt = 100
        myStruct.myString = "Test String 1"
        let id = UUID()
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        var entity1 = Entity (collection: collection, id: id, version: 10, item: myStruct)
        entity1.setSchemaVersion (3)
        let data1 = try! accessor.encoder.encode(entity1)
        entity1 = try! accessor.decoder.decode(Entity<MyStruct>.self, from: data1)
        XCTAssertFalse (entity1.isInitialized(onCollection: collection))
        XCTAssertNil (entity1.getCollection())
        XCTAssertEqual (3, entity1.getSchemaVersion())
        entity1.setCollection (collection: collection)
        XCTAssertTrue (entity1.isInitialized(onCollection: collection))
        XCTAssertTrue (entity1.getCollection()! === collection)
        XCTAssertEqual (5, entity1.getSchemaVersion())
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
        var batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .dirty
        entity.setPersistenceState(.dirty)
        batch = Batch()
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
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .persistent
        entity.setPersistenceState(.dirty)
        batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .abandoned
        entity.setPersistenceState(.abandoned)
        batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // sync: persistentState = .pendingRemoval
        entity.setPersistenceState(.pendingRemoval)
        batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .dirty
        waitFor = expectation(description: ".dirty")
        entity.setPersistenceState(.dirty)
        batch = Batch()
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
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .persistent
        waitFor = expectation(description: ".persistetnt")
        entity.setPersistenceState(.dirty)
        batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .abandoned
        waitFor = expectation(description: ".abandoned")
        entity.setPersistenceState(.abandoned)
        batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        // async: persistentState = .pendingRemoval
        waitFor = expectation(description: ".pendingRemoval")
        entity.setPersistenceState(.pendingRemoval)
        batch = Batch()
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
        
        batch.syncItems() { (items: Dictionary<UUID, EntityManagement>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(0, entity.getVersion())
            let retrievedEntity = items[entity.getId()]! as! Entity<MyStruct>
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
        var json = try String (data: accessor.encoder.encode(entity), encoding: .utf8)!
        try XCTAssertEqual("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(EntityTests.jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":0}", json)
        try json = "{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(EntityTests.jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        var entity2 = try accessor.decoder.decode(Entity<MyStruct>.self, from: json.data (using: .utf8)!)
        XCTAssertEqual (entity.id.uuidString, entity2.id.uuidString)
        XCTAssertEqual (5, entity2.getSchemaVersion())
        XCTAssertEqual (10, entity2.getVersion() )
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
        XCTAssertNil (entity.getSaved())
        // With a saved time
        let savedTime = Date()
        entity.setSaved (savedTime)
        entity.setPersistenceState (.persistent)
        json = try String (data: accessor.encoder.encode(entity), encoding: .utf8)!
        try XCTAssertEqual("{\"schemaVersion\":5,\"id\":\"\(entity.id.uuidString)\",\"saved\":\(EntityTests.jsonEncodedDate(date: entity.getSaved()!)!),\"created\":\(EntityTests.jsonEncodedDate(date: entity.created)!),\"version\":0,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}", json)
        try json = "{\"schemaVersion\":5,\"id\":\"\(entity.id.uuidString)\",\"saved\":\(EntityTests.jsonEncodedDate(date: entity.getSaved()!)!),\"created\":\(EntityTests.jsonEncodedDate(date: entity.created)!),\"version\":10,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}"
        //
        entity2 = try accessor.decoder.decode(Entity<MyStruct>.self, from: json.data (using: .utf8)!)
        XCTAssertEqual (entity.id.uuidString, entity2.id.uuidString)
        XCTAssertEqual (5, entity2.getSchemaVersion())
        XCTAssertEqual (10, entity2.getVersion())
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
        XCTAssertEqual ((entity.getSaved()!.timeIntervalSince1970 * 1000.0).rounded(), (entity2.getSaved()!.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db

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

    func testAnyEntityManagement() throws {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let anyEntity: AnyEntityManagement = AnyEntityManagement (item: entity)
        XCTAssertEqual (entity.getId(), anyEntity.getId())
        XCTAssertEqual (entity.getVersion(), anyEntity.getVersion())
        XCTAssertEqual (entity.getPersistenceState(), anyEntity.getPersistenceState())
        XCTAssertEqual (entity.getCreated(), anyEntity.getCreated())
        XCTAssertEqual (entity.getSaved(), anyEntity.getSaved())
        let encoder = JSONEncoder()
        let entityData = try encoder.encode(entity)
        let anyEntityData = try encoder.encode (anyEntity)
        let entityJson = String (data: entityData, encoding: .utf8)
        let anyEntityJson = String (data: anyEntityData, encoding: .utf8)
        XCTAssertEqual (entityJson, anyEntityJson)
    }

    func testUpdateStatement() {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        var anyEntity: AnyEntityManagement? = nil
        XCTAssertEqual (0, entity.getVersion())
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        var result = entity.updateStatement() { (name: CollectionName, entity: AnyEntityManagement) -> EntityConversionResult<Data> in
            do {
                anyEntity = entity
                let accessor = InMemoryAccessor()
                let result = try accessor.encoder.encode (entity)
                return .ok(result)
            } catch {
                return .error ("\(error)")
            }
        }
        switch result {
        case .ok (let data):
            try XCTAssertEqual ("{\"schemaVersion\":5,\"id\":\"\(entity.getId())\",\"saved\":\(EntityTests.jsonEncodedDate(date: entity.getSaved()!)!),\"created\":\(EntityTests.jsonEncodedDate(date: entity.created)!),\"version\":1,\"item\":{\"myInt\":10,\"myString\":\"A String\"},\"persistenceState\":\"persistent\"}", String (data: data, encoding: .utf8))
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
        XCTAssertEqual (entity.getId(), anyEntity!.getId())
        XCTAssertEqual (entity.getVersion(), anyEntity!.getVersion())
        XCTAssertEqual (entity.getPersistenceState(), anyEntity!.getPersistenceState())
        XCTAssertEqual (entity.getCreated(), anyEntity!.getCreated())
        XCTAssertEqual (entity.getSaved(), anyEntity!.getSaved())
        let accessor = InMemoryAccessor()
        let _ = accessor.add (name: "MyCollection", entity: anyEntity!)
        let accessorResult = accessor.get (type: Entity<MyStruct>.self, name: "MyCollection", id: entity.getId())
        // retrievedEntity is not initialized so collection is nil
        switch accessorResult {
        case .ok(let retrievedEntity):
            result = retrievedEntity!.removeStatement() { (name: CollectionName, entity: AnyEntityManagement) -> EntityConversionResult<Data> in
                do {
                    anyEntity = entity
                    let accessor = InMemoryAccessor()
                    let result = try accessor.encoder.encode (entity)
                    return .ok(result)
                } catch {
                    return .error ("\(error)")
                }
            }
            switch result {
            case .error (let errorMessage):
                XCTAssertEqual ("Entity<MyStruct>.removeStatement: Missing Collection: Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(retrievedEntity!.getId().uuidString)", errorMessage)
            default:
                XCTFail()
            }
        default:
            XCTFail ("Expected result")
        }
    }
    
    func testRemoveStatement() {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        var anyEntity: AnyEntityManagement? = nil
        XCTAssertEqual (0, entity.getVersion())
        switch entity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        var result = entity.removeStatement() { (name: CollectionName, entity: AnyEntityManagement) -> EntityConversionResult<String> in
            anyEntity = entity
            return .ok("remove:collection=\(name);id=\(entity.getId())")
        }
        switch result {
        case .ok (let statement):
            XCTAssertEqual ("remove:collection=myCollection;id=\(entity.getId().uuidString)", statement)
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
        XCTAssertEqual (entity.getId(), anyEntity!.getId())
        XCTAssertEqual (entity.getVersion(), anyEntity!.getVersion())
        XCTAssertEqual (entity.getPersistenceState(), anyEntity!.getPersistenceState())
        XCTAssertEqual (entity.getCreated(), anyEntity!.getCreated())
        XCTAssertEqual (entity.getSaved(), anyEntity!.getSaved())
        let accessor = InMemoryAccessor()
        let _ = accessor.add (name: "MyCollection", entity: anyEntity!)
        let accessorResult = accessor.get (type: Entity<MyStruct>.self, name: "MyCollection", id: entity.getId())
        // retrievedEntity is not initialized so collection is nil
        switch accessorResult {
        case .ok(let retrievedEntity):
            result = retrievedEntity!.removeStatement() { (name: CollectionName, entity: AnyEntityManagement) -> EntityConversionResult<String> in
                anyEntity = entity
                return .ok("remove:collection=\(name);id=\(entity.getId())")
            }
            switch result {
            case .error (let errorMessage):
                XCTAssertEqual ("Entity<MyStruct>.removeStatement: Missing Collection: Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(retrievedEntity!.getId().uuidString)", errorMessage)
            default:
                XCTFail()
            }
        default:
            XCTFail ("Expected result")
        }
    }
    
    // JSONEncoder uses its own inscrutable rounding process for encoding dates, so this is what is necessary to reliably get the expected value of a date in a json encoded object
    static func jsonEncodedDate (date: Date) throws -> String? {
        let accessor = InMemoryAccessor()
        struct DateContainer : Encodable {
            init (_ d: Date) {
                self.d = d
            }
            let d: Date
        }
        let container = DateContainer.init(date)
        let encoded = try accessor.encoder.encode (container)
        let protoResult = String (data: encoded, encoding: .utf8)
        var result: String? = nil
        if let protoResult = protoResult {
            result = String (protoResult[protoResult.index (protoResult.startIndex, offsetBy: 5)...])
            result = String (result!.prefix(result!.count - 1))
        }
        return result
    }
}

