
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

    func testSetCollection() {
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
        try XCTAssertEqual("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":0}", json)
        try json = "{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
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
        try XCTAssertEqual("{\"schemaVersion\":5,\"id\":\"\(entity.id.uuidString)\",\"saved\":\(jsonEncodedDate(date: entity.getSaved()!)!),\"created\":\(jsonEncodedDate(date: entity.created)!),\"version\":0,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}", json)
        try json = "{\"schemaVersion\":5,\"id\":\"\(entity.id.uuidString)\",\"saved\":\(jsonEncodedDate(date: entity.getSaved()!)!),\"created\":\(jsonEncodedDate(date: entity.created)!),\"version\":10,\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\"}"
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

    func testEntityPersistenceWrapper() throws {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let wrapper: EntityPersistenceWrapper = EntityPersistenceWrapper (collectionName: entity.getCollection()!.name, item: entity)
        XCTAssertEqual (entity.getId(), wrapper.getId())
        XCTAssertEqual (entity.getCollection()!.name, wrapper.collectionName)
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
        var batch = Batch()
        // persistenceState = .new
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            XCTAssertTrue (entity === items[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .dirty
        batch = Batch()
        entity.setPersistenceState(.dirty)
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            XCTAssertTrue (entity === items[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .pendingRemoval
        entity.setPersistenceState(.pendingRemoval)
        batch = Batch()
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .pendingRemoval:
            break
        default:
            XCTFail ("Expected .pendingRemoval")
        }
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            XCTAssertTrue (entity === items[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .abandoned
        entity.setPersistenceState(.abandoned)
        batch = Batch()
        entity.remove(batch: batch)
        XCTAssertNil (entity.getPendingAction())
        switch entity.getPersistenceState() {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            XCTAssertTrue (entity === items[entity.getId()] as! Entity<MyStruct>)
        }
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        // persistenceState = .saving
        entity.setPersistenceState(.saving)
        batch = Batch()
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
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            XCTAssertTrue (entity === items[entity.getId()] as! Entity<MyStruct>)
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
    

}

