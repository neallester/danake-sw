//
//  persistenceTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/2/18.
//

import XCTest
@testable import danake

class persistenceTests: XCTestCase {
    
    let standardCollectionName = "myCollection"
    
    func testDatabaseCreation() {
        XCTAssertEqual (0, Database.registrar.count())
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        var database: Database? = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        XCTAssertTrue (accessor === database!.getAccessor() as! InMemoryAccessor)
        XCTAssertTrue (logger === database!.logger as! InMemoryLogger)
        XCTAssertTrue (Database.registrar.isRegistered(key: accessor.hashValue()))
        XCTAssertEqual (5, database?.schemaVersion)
        XCTAssertEqual (1, Database.registrar.count())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("INFO|Database.init|created|hashValue=\(accessor.hashValue())", entries[0].asTestString())
        }
        let _ = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            XCTAssertEqual ("EMERGENCY|Database.init|registrationFailed|hashValue=\(accessor.hashValue())", entries[1].asTestString())
        }
        XCTAssertNotNil(database)
        XCTAssertTrue (Database.registrar.isRegistered(key: accessor.hashValue()))
        XCTAssertEqual (1, Database.registrar.count())
        database = nil
        XCTAssertFalse (Database.registrar.isRegistered(key: accessor.hashValue()))
        XCTAssertEqual (0, Database.registrar.count())
    }

    func testRetrievalResult() {
        var result = RetrievalResult.ok("String")
        XCTAssertTrue (result.isOk())
        XCTAssertEqual ("String", result.item()!)
        switch result {
        case .ok (let item):
            XCTAssertEqual ("String", item!)
        default:
            XCTFail("Expected OK")
        }
        result = .error ("An Error")
        XCTAssertFalse (result.isOk())
        XCTAssertNil (result.item())
    }
    
    func testPersistentCollectionCreation() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        var collection: PersistentCollection<Database, MyStruct>? = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let _ = collection // Quite a spurious xcode warning
        XCTAssertTrue (database.collectionRegistrar.isRegistered(key: collectionName))
        XCTAssertEqual (1, database.collectionRegistrar.count())
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
        let _ = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.init|collectionAlreadyRegistered|database=Database;databaseHashValue=\(database.getAccessor().hashValue());collectionName=myCollection", entries[0].asTestString())
        }
        collection = nil
        XCTAssertFalse (database.collectionRegistrar.isRegistered(key: collectionName))
        XCTAssertEqual (0, database.collectionRegistrar.count())
    }

    func testPersistentCollectionCreationInvalidName() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = ""
        let _ = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.init|Empty String is an illegal CollectionName|database=Database;accessor=InMemoryAccessor;databaseHashValue=\(database.accessor.hashValue());collectionName=", entries[0].asTestString())
        }
    }

    func testPersistenceCollectionNew() {
        // Creation with item
        let myStruct = MyStruct(myInt: 10, myString: "A String")
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        var batch = Batch()
        var entity: Entity<MyStruct>? = collection.new(batch: batch, item: myStruct)
        XCTAssertTrue (collection === entity!.getCollection ()!)
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            let item = items[entity!.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (item === entity!)
        }
        switch entity!.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity!.sync() { item in
            XCTAssertEqual(10, item.myInt)
            XCTAssertEqual("A String", item.myString)
        }
        collection.sync() { cache in
            XCTAssertEqual(1, cache.count)
            XCTAssertTrue (entity === cache[entity!.getId()]!.item!)
        }
        XCTAssertEqual (5, entity?.getSchemaVersion())
        entity = nil
        batch = Batch() // Ensures entity is collected
        collection.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
        // Creation with itemClosure
        
        entity = collection.new(batch: batch) { reference in
            return MyStruct (myInt: reference.version, myString: reference.id.uuidString)
        }
        XCTAssertTrue (collection === entity!.getCollection ()!)
        batch.syncItems() { items in
            XCTAssertEqual (1, items.count)
            let item = items[entity!.getId()]! as! Entity<MyStruct>
            XCTAssertTrue (item === entity!)
        }
        switch entity!.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity!.sync() { item in
            XCTAssertEqual(0, item.myInt)
            XCTAssertEqual(entity!.getId().uuidString, item.myString)
        }
        collection.sync() { cache in
            XCTAssertEqual(1, cache.count)
            XCTAssertTrue (entity === cache[entity!.getId()]!.item!)
        }
        XCTAssertEqual (5, entity?.getSchemaVersion())
        entity = nil
        batch = Batch() // Ensures entity is collected
        collection.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
    }
    
    func testPersistentCollectionEntityForProspect() throws {
        let entity = newTestEntity(myInt: 10, myString: "A String")
        entity.setSchemaVersion (3)// Verify that get updates the schema version
        entity.setPersistenceState (.persistent)
        entity.setSaved (Date())
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        XCTAssertFalse (entity.isInitialized(onCollection: collection))
        let entityForProspect = collection.entityForProspect(prospectEntity: entity)
        XCTAssertTrue (entity === entityForProspect)
        XCTAssertTrue (entityForProspect.isInitialized(onCollection: collection))
        switch entityForProspect.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        
        collection.sync() { items in
            XCTAssertEqual (1, items.count)
            XCTAssertTrue (entityForProspect === items[entity.getId()]!.item!)
        }
        let entityForProspect2 = collection.entityForProspect (prospectEntity: entityForProspect)
        XCTAssertTrue (entityForProspect === entityForProspect2)
        switch entityForProspect2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        let data = try! accessor.encoder.encode(entity)
        let prospect2 = try! accessor.decoder.decode(Entity<MyStruct>.self, from: data)
        let entityForProspect3 = collection.entityForProspect(prospectEntity: prospect2)
        XCTAssertTrue (entity === entityForProspect3)
    }
    
    func msRounded (date: Date) -> Double {
        return (date.timeIntervalSince1970 * 1000).rounded()
    }
    
    func testPersistentCollectionGet() throws {
        // Data In Cache=No; Data in Accessor=No
        let entity = newTestEntity(myInt: 10, myString: "A String")
        entity.setSchemaVersion (3)// Verify that get updates the schema version
        entity.setSaved (Date())
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        let data = try accessor.encoder.encode(entity)
        var result = collection.get (id: entity.getId())
        XCTAssertTrue (result.isOk())
        XCTAssertNil (result.item())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            let entry = entries[0].asTestString()
            XCTAssertEqual ("WARNING|PersistentCollection<Database, MyStruct>.get|Unknown id|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;id=\(entity.getId())", entry)
        }
        // Data In Cache=No; Data in Accessor=Yes
        accessor.add(name: standardCollectionName, id: entity.getId(), data: data)
        result = collection.get(id: entity.getId())
        let retrievedEntity = result.item()!
        XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
        XCTAssertEqual (entity.getVersion(), retrievedEntity.getVersion())
        XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
        XCTAssertEqual ((entity.created.timeIntervalSince1970 * 1000.0).rounded(), (retrievedEntity.created.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db
        XCTAssertEqual ((entity.getSaved ()!.timeIntervalSince1970 * 1000.0).rounded(), (retrievedEntity.getSaved ()!.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db
        switch retrievedEntity.getPersistenceState() {
        case .new:
            break
        default:
            XCTFail("Expected .persistent")
        }
        entity.sync() { entityStruct in
            retrievedEntity.sync() { retrievedEntityStruct in
                XCTAssertEqual(entityStruct.myInt, retrievedEntityStruct.myInt)
                XCTAssertEqual(entityStruct.myString, retrievedEntityStruct.myString)
            }
        }
        collection.sync() { cache in
            XCTAssertEqual (1, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.getId()]!.item!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Data In Cache=Yes; Data in Accessor=No
        let batch = Batch()
        let entity2 = collection.new(batch: batch, item: MyStruct())
        XCTAssertTrue (entity2 === collection.get(id: entity2.getId()).item()!)
        XCTAssertEqual (5, entity2.getSchemaVersion())
        XCTAssertTrue (entity2.getCollection () === collection)
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.getId()]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.getId()]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[standardCollectionName]![entity.getId()]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Data In Cache=Yes; Data in Accessor=Yes
        XCTAssertTrue (retrievedEntity === collection.get(id: entity.getId()).item()!)
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.getId()]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.getId()]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[standardCollectionName]![entity.getId()]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Invalid Data
        let json = "{}"
        let invalidData = json.data(using: .utf8)!
        let invalidDataUuid = UUID()
        accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
        let invalidEntity = collection.get(id: invalidDataUuid)
        switch invalidEntity {
        case .error (let errorMessage):
            XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.id, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key id (\\\"id\\\").\", underlyingError: nil))", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        XCTAssertNil (invalidEntity.item())
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            let entry = entries[1].asTestString()
            XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.get|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;id=\(invalidDataUuid);errorMessage=keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.id, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key id (\\\"id\\\").\", underlyingError: nil))", entry)
//            XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.get|Illegal Data|databaseHashValue=", entry.prefix(82))
//            XCTAssertEqual (";collection=myCollection;id=", entry[entry.index(entry.startIndex, offsetBy: 118)..<entry.index(entry.startIndex, offsetBy: 146)])
//            XCTAssertEqual (";data={};error=keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.id, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key id (\\\"id\\\").\", underlyingError: nil))", entries[1].asTestString().suffix(207))
//            XCTAssertEqual (389, entries[1].asTestString().count)
        }
        // Database Error
        let entity3 = newTestEntity(myInt: 30, myString: "A String 3")
        let data3 = try JSONEncoder().encode(entity)
        accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
        accessor.setThrowError()
        switch collection.get(id: entity3.getId()) {
        case .error (let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        logger.sync() { entries in
            XCTAssertEqual (3, entries.count)
            let entry = entries[2].asTestString()
            XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.get|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;id=\(entity3.getId());errorMessage=Test Error", entry)
        }
    }
    
    func testPerssistentCollectionGetParallel() throws {
        var counter = 0
        while counter < 100 {
            let accessor = InMemoryAccessor()
            let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
            let entity1 = newTestEntity(myInt: 10, myString: "A String1")
            entity1.setSchemaVersion (3)// Verify that get updates the schema version
            let data1 = try accessor.encoder.encode(entity1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String2")
            let data2 = try accessor.encoder.encode(entity2)
            let dispatchGroup = DispatchGroup()
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            let startSempaphore = DispatchSemaphore (value: 1)
            startSempaphore.wait()
            accessor.setPreFetch() { uuid in
                if uuid == entity1.getId() {
                    startSempaphore.wait()
                    startSempaphore.signal()
                }
            }
            var result1a: RetrievalResult<Entity<MyStruct>>? = nil
            var result1b: RetrievalResult<Entity<MyStruct>>? = nil
            var result2: RetrievalResult<Entity<MyStruct>>? = nil
            let waitFor1 = expectation(description: "Entity2")
            let workQueue = DispatchQueue (label: "WorkQueue", attributes: .concurrent)
            dispatchGroup.enter()
            workQueue.async {
                result1a = collection.get(id: entity1.getId())
                dispatchGroup.leave()
            }
            dispatchGroup.enter()
            workQueue.async {
                result1b = collection.get(id: entity1.getId())
                dispatchGroup.leave()
            }
            workQueue.async {
                result2 = collection.get(id: entity2.getId())
                waitFor1.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertNil (result1a)
            XCTAssertNil (result1b)
            switch result2! {
            case .ok (let retrievedEntity):
                XCTAssertEqual (entity2.getId(), retrievedEntity!.getId())
                XCTAssertEqual (entity2.getVersion(), retrievedEntity!.getVersion())
                XCTAssertEqual ((entity2.created.timeIntervalSince1970 * 1000.0).rounded(), (retrievedEntity!.created.timeIntervalSince1970 * 1000.0).rounded()) // We are keeping at least MS resolution in the db
                XCTAssertNil (retrievedEntity!.getSaved ())
            default:
                XCTFail("Expected data2")
            }
            startSempaphore.signal()
            switch dispatchGroup.wait(timeout: DispatchTime.now() + 10.0) {
            case .success:
                break
            default:
                XCTFail ("Timed Out")
            }
            var retrievedEntity1a: Entity<MyStruct>? = nil
            var retrievedEntity1b: Entity<MyStruct>? = nil
            switch result1a! {
            case .ok (let retrievedEntity):
                retrievedEntity1a = retrievedEntity
            default:
                XCTFail("Expected data1a")
            }
            switch result1b! {
            case .ok (let retrievedEntity):
                retrievedEntity1b = retrievedEntity
            default:
                XCTFail("Expected data1b")
            }
            XCTAssertNotNil(retrievedEntity1a)
            XCTAssertNotNil(retrievedEntity1b)
            XCTAssertTrue (retrievedEntity1a! === retrievedEntity1b!)
            counter = counter + 1
        }
    }

    
    func testPersistentCollectionGetAsync () throws {
        var counter = 0
        while counter < 100 {
            // Data In Cache=No; Data in Accessor=No
            let entity1 = newTestEntity(myInt: 10, myString: "A String1")
            entity1.setSchemaVersion (3)// Verify that get updates the schema version
            entity1.setPersistenceState (.persistent)
            let data1 = try JSONEncoder().encode(entity1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String2")
            let data2 = try JSONEncoder().encode(entity2)
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .warning)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            var waitFor1 = expectation(description: "wait1.1")
            var waitFor2 = expectation(description: "wait2.1")
            var result1: RetrievalResult<Entity<MyStruct>>? = nil
            var result2: RetrievalResult<Entity<MyStruct>>? = nil
            collection.get (id: entity1.getId()) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get (id: entity2.getId()) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertTrue (result1!.isOk())
            XCTAssertNil (result1!.item())
            XCTAssertTrue (result2!.isOk())
            XCTAssertNil (result2!.item())
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=No; Data in Accessor=Yes
            waitFor1 = expectation(description: "wait1.2")
            waitFor2 = expectation(description: "wait2.2")
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
            collection.get(id: entity1.getId()) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get(id: entity2.getId()) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            var retrievedEntity1 = result1!.item()!
            var retrievedEntity2 = result2!.item()!
            XCTAssertEqual (entity1.getId().uuidString, retrievedEntity1.getId().uuidString)
            XCTAssertEqual (entity1.getVersion(), retrievedEntity1.getVersion())
            XCTAssertEqual (entity2.getId().uuidString, retrievedEntity2.getId().uuidString)
            XCTAssertEqual (entity2.getVersion(), retrievedEntity2.getVersion())
            XCTAssertEqual (5, retrievedEntity1.getSchemaVersion())
            XCTAssertEqual (5, retrievedEntity2.getSchemaVersion())
            XCTAssertTrue (retrievedEntity1.getCollection () === collection)
            XCTAssertTrue (retrievedEntity2.getCollection () === collection)
            switch retrievedEntity1.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail("Expected .persistent")
            }
            switch retrievedEntity2.getPersistenceState() {
            case .new:
                break
            default:
                XCTFail("Expected .new")
            }
            entity1.sync() { entityStruct in
                retrievedEntity1.sync() { retrievedEntityStruct in
                    XCTAssertEqual(entityStruct.myInt, retrievedEntityStruct.myInt)
                    XCTAssertEqual(entityStruct.myString, retrievedEntityStruct.myString)
                }
            }
            entity2.sync() { entityStruct in
                retrievedEntity2.sync() { retrievedEntityStruct in
                    XCTAssertEqual(entityStruct.myInt, retrievedEntityStruct.myInt)
                    XCTAssertEqual(entityStruct.myString, retrievedEntityStruct.myString)
                }
            }
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.getId()]!.item!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.getId()]!.item!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=Yes; Data in Accessor=No
            waitFor1 = expectation(description: "wait1.3")
            waitFor2 = expectation(description: "wait2.3")
            let batch = Batch()
            let entity3 = collection.new(batch: batch, item: MyStruct())
            let entity4 = collection.new(batch: batch, item: MyStruct())
            collection.get(id: entity3.getId()) { item in
                result1 = item
                waitFor1.fulfill()
                
            }
            collection.get(id: entity4.getId())  { item in
                result2 = item
                waitFor2.fulfill()
                
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertTrue (entity3 === result1!.item()!)
            XCTAssertTrue (entity4 === result2!.item()!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.getId()]!.item!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.getId()]!.item!)
                XCTAssertTrue (entity3 === cache[entity3.getId()]!.item!)
                XCTAssertTrue (entity4 === cache[entity4.getId()]!.item!)
            }
            accessor.sync() { storage in
                XCTAssertEqual (2, storage[standardCollectionName]!.count)
                XCTAssertTrue (data1 == storage[standardCollectionName]![entity1.getId()]!)
                XCTAssertTrue (data2 == storage[standardCollectionName]![entity2.getId()]!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=Yes; Data in Accessor=Yes
            waitFor1 = expectation(description: "wait1.4")
            waitFor2 = expectation(description: "wait2.4")
            collection.get (id: entity1.getId()) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get (id: entity2.getId()) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            retrievedEntity1 = result1!.item()!
            retrievedEntity2 = result2!.item()!
            XCTAssertTrue (retrievedEntity1 === result1!.item()!)
            XCTAssertTrue (retrievedEntity2 === result2!.item()!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.getId()]!.item!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.getId()]!.item!)
                XCTAssertTrue (entity3 === cache[entity3.getId()]!.item!)
                XCTAssertTrue (entity4 === cache[entity4.getId()]!.item!)
            }
            accessor.sync() { storage in
                XCTAssertEqual (2, storage[standardCollectionName]!.count)
                XCTAssertTrue (data1 == storage[standardCollectionName]![entity1.getId()]!)
                XCTAssertTrue (data2 == storage[standardCollectionName]![entity2.getId()]!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Invalid Data
            waitFor1 = expectation(description: "wait1.5")
            waitFor2 = expectation(description: "wait2.5")
            let json1 = "{"
            let json2 = "{\"notanattribute\":1000}"
            let invalidData1 = json1.data(using: .utf8)!
            let invalidData2 = json2.data(using: .utf8)!
            let invalidDataUuid1 = UUID()
            let invalidDataUuid2 = UUID()
            accessor.add(name: standardCollectionName, id: invalidDataUuid1, data: invalidData1)
            accessor.add(name: standardCollectionName, id: invalidDataUuid2, data: invalidData2)
            collection.get(id: invalidDataUuid1) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get(id: invalidDataUuid2) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            switch result1! {
            case .error (let errorMessage):
                XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [], debugDescription: \"The given data was not valid JSON.\", underlyingError: Optional(Error Domain=NSCocoaErrorDomain Code=3840 \"Unexpected end of file during JSON parse.\" UserInfo={NSDebugDescription=Unexpected end of file during JSON parse.})))", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch result2! {
            case .error (let errorMessage):
                XCTAssertEqual ("keyNotFound(danake.Entity<danakeTests.MyStruct>.CodingKeys.id, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key id (\\\"id\\\").\", underlyingError: nil))", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            XCTAssertNil (result1!.item())
            XCTAssertNil (result2!.item())
            logger.sync() { entries in
                XCTAssertEqual (4, entries.count)
            }
            // Database Error
            let entity5 = newTestEntity(myInt: 50, myString: "A String5")
            let data5 = try JSONEncoder().encode(entity1)
            let entity6 = newTestEntity(myInt: 60, myString: "A String6")
            let data6 = try JSONEncoder().encode(entity2)
            accessor.add(name: standardCollectionName, id: entity5.getId(), data: data5)
            accessor.add(name: standardCollectionName, id: entity6.getId(), data: data6)
            accessor.setThrowError()
            waitFor1 = expectation(description: "wait1.6")
            waitFor2 = expectation(description: "wait2.6")
            var errorsReported = 0
            collection.get(id: entity5.getId()) { item in
                switch item {
                case .error:
                    errorsReported = errorsReported + 1
                default:
                    break
                }
                waitFor1.fulfill()
            }
            collection.get(id: entity6.getId()) { item in
                switch item {
                case .error:
                    errorsReported = errorsReported + 1
                default:
                    break
                }
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertEqual (1, errorsReported)
            logger.sync() { entries in
                XCTAssertEqual (5, entries.count)
                let entry = entries[4].asTestString()
                XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.get|Database Error|databaseHashValue=", entry.prefix(84))
                XCTAssertEqual (";collection=myCollection;id=", entry[entry.index(entry.startIndex, offsetBy: 120)..<entry.index(entry.startIndex, offsetBy: 148)])
                XCTAssertEqual (";errorMessage=Test Error", entry.suffix (24))
                XCTAssertEqual (208, entry.count)
            }
            counter = counter + 1
        }
    }

    func testInMemoryAccessor() throws {
        let accessor = InMemoryAccessor()
        let uuid = UUID()
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: uuid) {
        case .ok (let retrievedData):
            XCTAssertNil (retrievedData)
        default:
            XCTFail("Expected data")
        }
        switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .ok (let retrievedData):
            XCTAssertEqual (0, retrievedData.count)
        default:
            XCTFail("Expected data")
        }
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let data = try accessor.encoder.encode(entity)
        // Add using internal add
        accessor.add(name: standardCollectionName, id: entity.getId(), data: data)
        var retrievedEntity1a: Entity<MyStruct>? = nil
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .ok (let retrievedEntity):
            retrievedEntity1a = retrievedEntity
            XCTAssertTrue (entity !== retrievedEntity!)
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity!.getId().uuidString)
            XCTAssertEqual (entity.getVersion(), retrievedEntity!.getVersion())
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity!.getCreated()))
            XCTAssertEqual (entity.getSaved(), retrievedEntity!.getSaved())
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity!.getPersistenceState().rawValue)
            retrievedEntity!.sync() { retrievedStruct in
                XCTAssertEqual (10, retrievedStruct.myInt)
                XCTAssertEqual ("A String", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected .ok")
        }
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: uuid) {
        case .ok (let retrievedEntity):
            XCTAssertNil (retrievedEntity)
        default:
            XCTFail("Expected .ok")
        }
        switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (1, retrievedEntities.count)
            let retrievedEntity = retrievedEntities[0]
            XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
            XCTAssertEqual (entity.getVersion(), retrievedEntity.getVersion())
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity.getCreated()))
            XCTAssertEqual (entity.getSaved(), retrievedEntity.getSaved())
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
            retrievedEntity.sync() { retrievedStruct in
                XCTAssertEqual (10, retrievedStruct.myInt)
                XCTAssertEqual ("A String", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected .ok")
        }
        let batch = Batch()
        entity.sync(batch: batch) { item in
            item.myInt = 20
            item.myString = "A String 2"
        }
        // Update
        let updateResult = accessor.update(name: standardCollectionName, entity: entity)
        switch updateResult {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        var retrievedEntity1b: Entity<MyStruct>? = nil
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .ok (let retrievedEntity):
            XCTAssertTrue (entity !== retrievedEntity)
            XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
            retrievedEntity1b = retrievedEntity
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity!.getId().uuidString)
            XCTAssertEqual (entity.getVersion(), retrievedEntity!.getVersion())
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity!.getCreated()))
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity!.getSaved()!))
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity!.getPersistenceState().rawValue)
            retrievedEntity!.sync() { retrievedStruct in
                XCTAssertEqual (20, retrievedStruct.myInt)
                XCTAssertEqual ("A String 2", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected data")
        }
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: uuid) {
        case .ok (let retrievedData):
            XCTAssertNil (retrievedData)
        default:
            XCTFail("Expected .ok")
        }
        accessor.setThrowError()
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .error (let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        var retrievedEntity1c: Entity<MyStruct>? = nil
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .ok (let retrievedEntity):
            XCTAssertTrue (entity !== retrievedEntity)
            XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
            XCTAssertTrue (retrievedEntity !== retrievedEntity1b)
            retrievedEntity1c = retrievedEntity
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity!.getId().uuidString)
            XCTAssertEqual (entity.getVersion(), retrievedEntity!.getVersion())
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity!.getCreated()))
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity!.getSaved()!))
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity!.getPersistenceState().rawValue)
            retrievedEntity!.sync() { retrievedStruct in
                XCTAssertEqual (20, retrievedStruct.myInt)
                XCTAssertEqual ("A String 2", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected .ok")
        }
        accessor.setThrowError()
        switch accessor.scan (type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .error(let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail ("Expected .error")
        
        }
        var retrievedEntity1d: Entity<MyStruct>? = nil
        switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (1, retrievedEntities.count)
            let retrievedEntity = retrievedEntities[0]
            XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
            XCTAssertTrue (retrievedEntity !== retrievedEntity1b)
            XCTAssertTrue (retrievedEntity !== retrievedEntity1c)
            retrievedEntity1d = retrievedEntity
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity.getCreated()))
            try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity.getSaved()!))
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
            retrievedEntity.sync() { retrievedStruct in
                XCTAssertEqual (20, retrievedStruct.myInt)
                XCTAssertEqual ("A String 2", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected .ok")
        }
        // Second Entity added public add
        let entity2 = newTestEntity(myInt: 30, myString: "A String 3")
        entity2.setSaved (Date())
        // Need to set collection in entity2
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        entity2.setCollection(collection: collection)
        let anyEntity2 = AnyEntityManagement (item: entity2)
        var addResult = accessor.add(name: standardCollectionName, entity: anyEntity2)
        switch addResult {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        var foundEntity1 = false
        var foundEntity2 = false
        switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (2, retrievedEntities.count)
            for retrievedEntity in retrievedEntities {
                if (retrievedEntity.getId().uuidString == entity.getId().uuidString) {
                    foundEntity1 = true
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1b)
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1c)
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1d)
                    XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (20, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 2", retrievedStruct.myString)
                    }
                } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                    foundEntity2 = true
                    XCTAssertEqual (entity2.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity2.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity2.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity2.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity2.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (30, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 3", retrievedStruct.myString)
                    }
                }
            }
            
        default:
            XCTFail(".ok")
        }
        XCTAssertTrue (foundEntity1)
        XCTAssertTrue (foundEntity2)
        // Add Entity Without collection using public add
        var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
        entity3.setSaved (Date())
        let data3 = try accessor.encoder.encode (entity3)
        entity3 = try! accessor.decoder.decode(Entity<MyStruct>.self, from: data3)
        let anyEntity3 = AnyEntityManagement (item: entity3)
        addResult = accessor.add(name: standardCollectionName, entity: anyEntity3)
        switch addResult {
        case .error (let errorMessage):
            XCTAssertEqual ("Entity<MyStruct>.updateStatement: Missing Collection: Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(entity3.getId().uuidString)", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (2, retrievedEntities.count)
            for retrievedEntity in retrievedEntities {
                if (retrievedEntity.getId().uuidString == entity.getId().uuidString) {
                    foundEntity1 = true
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1b)
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1c)
                    XCTAssertTrue (retrievedEntity !== retrievedEntity1d)
                    XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (20, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 2", retrievedStruct.myString)
                    }
                } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                    foundEntity2 = true
                    XCTAssertEqual (entity2.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity2.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity2.getCreated()), EntityTests.jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (EntityTests.jsonEncodedDate (date: entity2.getSaved()!), EntityTests.jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity2.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (30, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 3", retrievedStruct.myString)
                    }
                }
            }
            
        default:
            XCTFail(".ok")
        }
        // Scan with throw error
        accessor.setThrowError()
        switch accessor.scan (type: Entity<MyStruct>.self, name: standardCollectionName) {
        case .error (let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        // Test preFetch
        var prefetchUuid: String? = nil
        accessor.setPreFetch() { uuid in
            prefetchUuid = uuid.uuidString
        }
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .ok (let retrievedEntity):
            XCTAssertEqual (prefetchUuid!, entity.getId().uuidString)
            XCTAssertTrue (entity.getId().uuidString == retrievedEntity?.getId().uuidString)
        default:
            XCTFail("Expected .ok")
        }
    }
    
    class RegistrarTestItem {
        
        init (stringValue: String) {
            self.stringValue = stringValue
        }
        
        let stringValue: String
    }
    
    func testPersistentCollectionInitialize() throws {
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .warning)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            // No Criteria
            var entities: [Entity<MyStruct>] = []
            collection.initialize(entities: &entities)
            XCTAssertEqual (0, entities.count)
            // entity1: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
            entity1.setSchemaVersion (3)// Verify that get updates the schema version
            let data1 = try accessor.encoder.encode(entity1)
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            // entity2: Entity in cache, data not in accessor
            let batch = Batch()
            let myStruct = MyStruct (myInt: 20, myString: "A String 2")
            let entity2 = collection.new(batch: batch, item: myStruct)
            entity2.setCollection(collection: collection)
            // entity3: Entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setCollection(collection: collection)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
            entity3 = collection.get (id: entity3.getId()).item()!
            // entity4: Entity not in cache, data not in accessor
            let entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
            }
            // Run Test1
            entities = [entity1, entity2, entity3, entity4]
            collection.initialize(entities: &entities)
            var convertedEntity1: Entity<MyStruct>? = nil
            var convertedEntity2: Entity<MyStruct>? = nil
            var convertedEntity3: Entity<MyStruct>? = nil
            var convertedEntity4: Entity<MyStruct>? = nil
            for convertedEntity in entities {
                XCTAssertTrue (convertedEntity.isInitialized(onCollection: collection))
                if convertedEntity.getId().uuidString == entity1.getId().uuidString {
                    XCTAssertNil (convertedEntity1)
                    convertedEntity1 = convertedEntity
                } else if convertedEntity.getId().uuidString == entity2.getId().uuidString {
                    XCTAssertNil (convertedEntity2)
                    convertedEntity2 = convertedEntity
                } else if convertedEntity.getId().uuidString == entity3.getId().uuidString {
                    XCTAssertNil (convertedEntity3)
                    convertedEntity3 = convertedEntity
                } else if convertedEntity.getId().uuidString == entity4.getId().uuidString {
                    XCTAssertNil (convertedEntity4)
                    convertedEntity4 = convertedEntity
                } else {
                    XCTFail("Unknown Converted Entity")
                }
            }
            XCTAssertEqual (4, entities.count)
            XCTAssertTrue (entity1 === convertedEntity1!)
            XCTAssertTrue (entity2 === convertedEntity2!)
            XCTAssertTrue (entity3 === convertedEntity3!)
            XCTAssertTrue (entity4 === convertedEntity4!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (cache[entity1.getId()]!.item! === convertedEntity1!)
                XCTAssertTrue (cache[entity2.getId()]!.item! === convertedEntity2!)
                XCTAssertTrue (cache[entity3.getId()]!.item! === convertedEntity3!)
                XCTAssertTrue (cache[entity4.getId()]!.item! === convertedEntity4!)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            // Multiple copies of same Entity
            let entity5 = newTestEntity(myInt: 50, myString: "String 5")
            let data5 = try accessor.encoder.encode(entity5)
            entities = try [accessor.decoder.decode (Entity<MyStruct>.self, from: data5), accessor.decoder.decode (Entity<MyStruct>.self, from: data5), accessor.decoder.decode (Entity<MyStruct>.self, from: data5), accessor.decoder.decode (Entity<MyStruct>.self, from: data5)]
            collection.initialize(entities: &entities)
            var convertedEntity5: Entity<MyStruct>? = nil
            XCTAssertEqual (4, entities.count)
            convertedEntity5 = entities[0]
            for convertetdEntity in entities {
                XCTAssertTrue (convertetdEntity.isInitialized(onCollection: collection))
                XCTAssertTrue (convertetdEntity === convertedEntity5)
            }
            collection.sync() { cache in
                XCTAssertEqual (5, cache.count)
                XCTAssertTrue (cache[entity5.getId()]!.item! === convertedEntity5!)
            }

            // With criteria
            entities = []
            var convertedEntities = collection.initialize(entities: entities) { myStruct in
                return (myStruct.myInt == 20)
            }
            XCTAssertEqual (0, convertedEntities.count)
            entities = [entity1, entity2, entity3, entity4]
            convertedEntities = collection.initialize(entities: entities) { myStruct in
                return (myStruct.myInt == 20)
            }
            XCTAssertEqual (1, convertedEntities.count)
            XCTAssertTrue (convertedEntities[0] === convertedEntity2!)
            // Convert Data with criteria matching none
            convertedEntities = collection.initialize (entities: entities) { myStruct in
                return (myStruct.myInt == 1000)
            }
            XCTAssertEqual (0, convertedEntities.count)
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }

        }
        // From scratch with Criteria selecting entity1 (Entity not in cache, data in accessor)
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .error)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            // entity1: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
            entity1.setSchemaVersion (3)// Verify schema version is updated
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            // entity2: Entity in cache, data not in accessor
            let batch = Batch()
            let myStruct = MyStruct (myInt: 20, myString: "A String 2")
            let entity2 = collection.new(batch: batch, item: myStruct)
            // entity3: Entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setCollection(collection: collection)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
            entity3 = collection.get (id: entity3.getId()).item()!
            // entity4: Entity not in cache, data not in accessor
            let entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            let entities = [entity1, entity2, entity3, entity4]
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
            }
            let convertedEntities = collection.initialize(entities: entities) { myStruct in
                return (myStruct.myInt == 10)
            }
            XCTAssertEqual (1, convertedEntities.count)
            let convertedEntity = convertedEntities[0]
            XCTAssertTrue (convertedEntity.isInitialized(onCollection: collection))
            XCTAssertTrue (convertedEntity === entity1)
            collection.sync() { cache in
                XCTAssertEqual (3, cache.count)
                XCTAssertTrue (cache[entity1.getId()]!.item! === convertedEntity)
                XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
        }
        // From scratch with Criteria selecting entity2 (Entity in cache, data not in accessor)
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .error)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            // entity1: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
            entity1.setSchemaVersion (3)// Verify schema version is updated
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            // entity2: Entity in cache, data not in accessor
            let batch = Batch()
            let myStruct = MyStruct (myInt: 20, myString: "A String 2")
            let entity2 = collection.new(batch: batch, item: myStruct)
            // entity3: Entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setCollection(collection: collection)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
            entity3 = collection.get (id: entity3.getId()).item()!
            // entity4: Entity not in cache, data not in accessor
            let entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
            }
            let entities = [entity1, entity2, entity3, entity4]
            let convertedEntities = collection.initialize(entities: entities) { myStruct in
                return (myStruct.myInt == 20)
            }
            XCTAssertEqual (1, convertedEntities.count)
            let convertedEntity = convertedEntities[0]
            XCTAssertTrue (convertedEntity.isInitialized(onCollection: collection))
            XCTAssertTrue (convertedEntity === entity2)
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            // From scratch with Criteria selecting entity3 (Entity in cache, data in accessor)
            do {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
                // entity1: Data in accessor
                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
                entity1.setSchemaVersion (3)// Verify schema version is updated
                entity1.setSaved (Date())
                let data1 = try accessor.encoder.encode(entity1)
                accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
                // entity2: Entity in cache, data not in accessor
                let batch = Batch()
                let myStruct = MyStruct (myInt: 20, myString: "A String 2")
                let entity2 = collection.new(batch: batch, item: myStruct)
                // entity3: Entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.setCollection(collection: collection)
                entity3.setSaved (Date())
                let data3 = try accessor.encoder.encode(entity3)
                accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
                entity3 = collection.get (id: entity3.getId()).item()!
                // entity4: Entity not in cache, data not in accessor
                let entity4 = newTestEntity(myInt: 40, myString: "A String 4")

                let entities = [entity1, entity2, entity3, entity4]
                collection.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                }
                let convertedEntities = collection.initialize(entities: entities) { myStruct in
                    return (myStruct.myInt == 30)
                }
                XCTAssertEqual (1, convertedEntities.count)
                let convertedEntity = convertedEntities[0]
                XCTAssertTrue (convertedEntity.isInitialized(onCollection: collection))
                XCTAssertTrue (convertedEntity === entity3)
                collection.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                }
                logger.sync() { entries in
                    XCTAssertEqual (0, entries.count)
                }
            }
            // From scratch with Criteria selecting entity4 (Entity not in cache, data not in accessor)
            do {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
                // entity1: Data in accessor
                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
                entity1.setSchemaVersion (3)// Verify schema version is updated
                entity1.setSaved (Date())
                let data1 = try accessor.encoder.encode(entity1)
                accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
                // entity2: Entity in cache, data not in accessor
                let batch = Batch()
                let myStruct = MyStruct (myInt: 20, myString: "A String 2")
                let entity2 = collection.new(batch: batch, item: myStruct)
                entity2.setCollection(collection: collection)
                // entity3: Entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.setSchemaVersion (5)
                entity3.setSaved (Date())
                let data3 = try accessor.encoder.encode(entity3)
                accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
                entity3 = collection.get (id: entity3.getId()).item()!
                // entity4: Entity not in cache, data not in accessor
                let entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                let entities = [entity1, entity2, entity3, entity4]
                collection.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                }
                let convertedEntities = collection.initialize(entities: entities) { myStruct in
                    return (myStruct.myInt == 40)
                }
                XCTAssertEqual (1, convertedEntities.count)
                let convertedEntity = convertedEntities[0]
                XCTAssertTrue (convertedEntity.isInitialized(onCollection: collection))
                XCTAssertTrue (convertedEntity === entity4)
                collection.sync() { cache in
                    XCTAssertEqual (3, cache.count)
                    XCTAssertTrue (cache[entity4.getId()]!.item! === convertedEntity)
                    XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                }
                logger.sync() { entries in
                    XCTAssertEqual (0, entries.count)
                }
            }
            // In Parallel
            do {
                var repeatCounter = 0
                while repeatCounter < 100 {
                    let accessor = InMemoryAccessor()
                    let logger = InMemoryLogger(level: .error)
                    let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                    let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
                    let dispatchGroup = DispatchGroup()
                    let workQueue = DispatchQueue (label: "workQueue", attributes: .concurrent)
                    var groupCounter = 0
                    while groupCounter < 3 {
                        workQueue.async {
                            do {
                                dispatchGroup.enter()
                                var entities: [Entity<MyStruct>] = []
                                collection.initialize(entities: &entities)
                                XCTAssertEqual (0, entities.count)
                                let convertedEntities = collection.initialize (entities: entities) { myStruct in
                                    return (myStruct.myInt == 20)
                                }
                                XCTAssertEqual (0, convertedEntities.count)
                                // entity1: Data in accessor
                                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
                                entity1.setSchemaVersion (3)// Verify that get updates the schema version
                                entity1.setSaved (Date())
                                let data1 = try accessor.encoder.encode(entity1)
                                accessor.add(name: self.standardCollectionName, id: entity1.getId(), data: data1)
                                // entity2: Entity in cache, data not in accessor
                                let batch = Batch()
                                let myStruct = MyStruct (myInt: 20, myString: "A String 2")
                                let entity2 = collection.new(batch: batch, item: myStruct)
                                // entity3: Entity in cache, data in accessor
                                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                                entity3.setCollection(collection: collection)
                                entity3.setSaved (Date())
                                let data3 = try accessor.encoder.encode(entity3)
                                accessor.add(name: self.standardCollectionName, id: entity3.getId(), data: data3)
                                entity3 = collection.get (id: entity3.getId()).item()!
                                // entity4: Entity not in cache, data not in accessor
                                let entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                                entities = [entity1, entity2, entity3, entity4]
                                collection.sync() { cache in
                                    XCTAssertTrue (cache[entity2.getId()]!.item! === entity2)
                                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                                }
                                // Convert Data
                                collection.initialize(entities: &entities)
                                var convertedEntity1: Entity<MyStruct>? = nil
                                var convertedEntity2: Entity<MyStruct>? = nil
                                var convertedEntity3: Entity<MyStruct>? = nil
                                var convertedEntity4: Entity<MyStruct>? = nil
                                for convertedEntity in entities {
                                    XCTAssertTrue (convertedEntity.isInitialized(onCollection: collection))
                                    if convertedEntity.getId().uuidString == entity1.getId().uuidString {
                                        XCTAssertNil (convertedEntity1)
                                        convertedEntity1 = convertedEntity
                                    } else if convertedEntity.getId().uuidString == entity2.getId().uuidString {
                                        XCTAssertNil (convertedEntity2)
                                        convertedEntity2 = convertedEntity
                                    } else if convertedEntity.getId().uuidString == entity3.getId().uuidString {
                                        XCTAssertNil (convertedEntity3)
                                        convertedEntity3 = convertedEntity
                                    } else if convertedEntity.getId().uuidString == entity4.getId().uuidString {
                                        XCTAssertNil (convertedEntity4)
                                        convertedEntity4 = convertedEntity
                                    } else {
                                        XCTFail("Unknown Converted Entity")
                                    }
                                }
                                XCTAssertEqual (4, entities.count)
                                XCTAssertNotNil(convertedEntity1)
                                XCTAssertEqual (entity1.getId().uuidString, convertedEntity1?.getId().uuidString)
                                XCTAssertTrue (entity1 === convertedEntity1!)
                                XCTAssertTrue (entity2 === convertedEntity2!)
                                XCTAssertTrue (entity3 === convertedEntity3!)
                                XCTAssertTrue (entity4 === convertedEntity4!)
                                collection.sync() { cache in
                                    XCTAssertTrue (cache[entity1.getId()]!.item! === convertedEntity1!)
                                    XCTAssertTrue (cache[entity2.getId()]!.item! === convertedEntity2!)
                                    XCTAssertTrue (cache[entity3.getId()]!.item! === convertedEntity3!)
                                    XCTAssertTrue (cache[entity4.getId()]!.item! === convertedEntity4!)
                                }
                                dispatchGroup.leave()
                            } catch {
                                XCTFail ("Exception Occurred: \(error)")
                            }
                        }
                        groupCounter = groupCounter + 1
                    }
                    switch dispatchGroup.wait(timeout: DispatchTime.now() + 10) {
                    case .success:
                        break
                    default:
                        XCTFail ("Timed Out")
                    }
                    
                    repeatCounter = repeatCounter + 1
                }
            }
        }

    }

    func testPersistentCollectionScan() throws {
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .warning)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            var retrievedEntities = collection.scan(criteria: nil).item()!
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = (collection.scan() { myStruct in
                return (myStruct.myInt == 20)
            }).item()!
            XCTAssertEqual (0, retrievedEntities.count)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
            entity1.setSchemaVersion (3)// Verify that get updates the schema version
            entity1.setPersistenceState (.persistent)
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
            entity2.setSchemaVersion (3)// Verify that get updates the schema version
            entity2.setPersistenceState (.persistent)
            entity2.setSaved (Date())
            let data2 = try accessor.encoder.encode(entity2)
            accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setSchemaVersion (5)
            entity3.setPersistenceState (.persistent)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
            entity3 = collection.get (id: entity3.getId()).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.setSchemaVersion (5)
            entity4.setPersistenceState (.persistent)
            entity4.setSaved (Date())
            let data4 = try accessor.encoder.encode(entity4)
            accessor.add(name: standardCollectionName, id: entity4.getId(), data: data4)
            entity4 = collection.get (id: entity4.getId()).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                XCTAssertTrue (cache[entity4.getId()]!.item! === entity4)
            }
            // Retrieve Data
            retrievedEntities = collection.scan(criteria: nil).item()!
            var retrievedEntity1: Entity<MyStruct>? = nil
            var retrievedEntity2: Entity<MyStruct>? = nil
            var retrievedEntity3: Entity<MyStruct>? = nil
            var retrievedEntity4: Entity<MyStruct>? = nil
            for retrievedEntity in retrievedEntities {
                XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
                switch retrievedEntity.getPersistenceState() {
                case .persistent:
                    break
                default:
                    XCTFail ("Expected .persistent")
                }
                if retrievedEntity.getId().uuidString == entity1.getId().uuidString {
                    XCTAssertNil (retrievedEntity1)
                    retrievedEntity1 = retrievedEntity
                } else if retrievedEntity.getId().uuidString == entity2.getId().uuidString {
                    XCTAssertNil (retrievedEntity2)
                    retrievedEntity2 = retrievedEntity
                } else if retrievedEntity.getId().uuidString == entity3.getId().uuidString {
                    XCTAssertNil (retrievedEntity3)
                    retrievedEntity3 = retrievedEntity
                } else if retrievedEntity.getId().uuidString == entity4.getId().uuidString {
                    XCTAssertNil (retrievedEntity4)
                    retrievedEntity4 = retrievedEntity
                } else {
                    XCTFail("Unknown Converted Entity")
                }
            }
            XCTAssertEqual (4, retrievedEntities.count)
            XCTAssertEqual (entity1.getId().uuidString, retrievedEntity1!.getId().uuidString)
            XCTAssertTrue (entity1 !== retrievedEntity1)
            entity1.sync() { item1 in
                retrievedEntity1!.sync() { retrievedItem1 in
                    XCTAssertEqual (item1.myInt, retrievedItem1.myInt)
                    XCTAssertEqual (item1.myString, retrievedItem1.myString)
                }
            }
            XCTAssertEqual (entity2.getId().uuidString, retrievedEntity2?.getId().uuidString)
            entity2.sync() { item2 in
                retrievedEntity2!.sync() { retrievedItem2 in
                    XCTAssertEqual (item2.myInt, retrievedItem2.myInt)
                    XCTAssertEqual (item2.myString, retrievedItem2.myString)
                }
            }
            XCTAssertEqual (entity3.getId().uuidString, retrievedEntity3!.getId().uuidString)
            XCTAssertTrue (entity3 === retrievedEntity3!)
            XCTAssertEqual (entity4.getId().uuidString, retrievedEntity4!.getId().uuidString)
            XCTAssertTrue (entity4 === retrievedEntity4!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (cache[entity1.getId()]!.item! === retrievedEntity1!)
                XCTAssertTrue (cache[entity2.getId()]!.item! === retrievedEntity2!)
                XCTAssertTrue (cache[entity3.getId()]!.item! === retrievedEntity3!)
                XCTAssertTrue (cache[entity4.getId()]!.item! === retrievedEntity4!)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            // With criteria
            var retrievalResult = collection.scan() { myStruct in
                return (myStruct.myInt == 20)
            }
            retrievedEntities = retrievalResult.item()!
            XCTAssertEqual (1, retrievedEntities.count)
            XCTAssertTrue (retrievedEntities[0] === retrievedEntity2!)
            // With criteria matching none
            retrievalResult = collection.scan() { myStruct in
                return (myStruct.myInt == 1000)
            }
            retrievedEntities = retrievalResult.item()!
            XCTAssertEqual (0, retrievedEntities.count)
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            
        }
        // From scratch with Criteria selecting entity1 (Entity not in cache, data in accessor)
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .error)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
            entity1.setSchemaVersion (3)// Verify that get updates the schema version
            entity1.setPersistenceState (.persistent)
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
            entity2.setSchemaVersion (3)// Verify that get updates the schema version
            entity2.setPersistenceState (.persistent)
            entity2.setSaved (Date())
            let data2 = try accessor.encoder.encode(entity2)
            accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setSchemaVersion (5)
            entity3.setPersistenceState (.persistent)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
            entity3 = collection.get (id: entity3.getId()).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.setSchemaVersion (5)
            entity4.setPersistenceState (.persistent)
            entity4.setSaved (Date())
            let data4 = try accessor.encoder.encode(entity4)
            accessor.add(name: standardCollectionName, id: entity4.getId(), data: data4)
            entity4 = collection.get (id: entity4.getId()).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                XCTAssertTrue (cache[entity4.getId()]!.item! === entity4)
            }
            // Retrieve Data
            let retrievedEntities = (collection.scan(){ item in
                return (item.myInt == 10)
            }).item()!
            XCTAssertEqual (1, retrievedEntities.count)
            let retrievedEntity = retrievedEntities[0]
            XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
            switch retrievedEntity.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (entity1.getId().uuidString, retrievedEntity.getId().uuidString)
            entity1.sync() { item1 in
                retrievedEntity.sync() { retrievedItem in
                    XCTAssertEqual (item1.myInt, retrievedItem.myInt)
                    XCTAssertEqual (item1.myString, retrievedItem.myString)
                }
            }
            collection.sync() { cache in
                XCTAssertEqual (3, cache.count)
                XCTAssertTrue (cache[entity1.getId()]!.item! === retrievedEntity)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                XCTAssertTrue (cache[entity4.getId()]!.item! === entity4)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            // From scratch with Criteria selecting entity3 (Entity cache, data in accessor)
            do {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
                // entity1, entity2: Data in accessor
                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
                entity1.setSchemaVersion (3)// Verify that get updates the schema version
                entity1.setPersistenceState (.persistent)
                entity1.setSaved (Date())
                let data1 = try accessor.encoder.encode(entity1)
                accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
                let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
                entity2.setSchemaVersion (3)// Verify that get updates the schema version
                entity2.setPersistenceState (.persistent)
                entity2.setSaved (Date())
                let data2 = try accessor.encoder.encode(entity2)
                accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
                // entity3, entity4: entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.setSchemaVersion (5)
                entity3.setPersistenceState (.persistent)
                entity3.setSaved (Date())
                let data3 = try accessor.encoder.encode(entity3)
                accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
                entity3 = collection.get (id: entity3.getId()).item()!
                var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                entity4.setSchemaVersion (5)
                entity4.setPersistenceState (.persistent)
                entity4.setSaved (Date())
                let data4 = try accessor.encoder.encode(entity4)
                accessor.add(name: standardCollectionName, id: entity4.getId(), data: data4)
                entity4 = collection.get (id: entity4.getId()).item()!
                // Invalid Data
                let json = "{}"
                let invalidData = json.data(using: .utf8)!
                let invalidDataUuid = UUID()
                accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
                collection.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                    XCTAssertTrue (cache[entity4.getId()]!.item! === entity4)
                }
                // Retrieve Data
                let retrievedEntities = (collection.scan(){ item in
                    return (item.myInt == 30)
                }).item()!
                XCTAssertEqual (1, retrievedEntities.count)
                let retrievedEntity = retrievedEntities[0]
                XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
                switch retrievedEntity.getPersistenceState() {
                case .persistent:
                    break
                default:
                    XCTFail ("Expected .persistent")
                }
                XCTAssert (entity3 === retrievedEntity)
                collection.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                    XCTAssertTrue (cache[entity4.getId()]!.item! === entity4)
                }
                logger.sync() { entries in
                    XCTAssertEqual (0, entries.count)
                }
            }
        }
        // Database Error
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .error)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
            entity1.setSchemaVersion (3)// Verify that get updates the schema version
            entity1.setPersistenceState (.persistent)
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
            entity2.setSchemaVersion (3)// Verify that get updates the schema version
            entity2.setPersistenceState (.persistent)
            entity2.setSaved (Date())
            let data2 = try accessor.encoder.encode(entity2)
            accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setSchemaVersion (5)
            entity3.setPersistenceState (.persistent)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
            entity3 = collection.get (id: entity3.getId()).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.setSchemaVersion (5)
            entity4.setPersistenceState (.persistent)
            entity4.setSaved (Date())
            let data4 = try accessor.encoder.encode(entity4)
            accessor.add(name: standardCollectionName, id: entity4.getId(), data: data4)
            entity4 = collection.get (id: entity4.getId()).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.getId()]!.item! === entity3)
                XCTAssertTrue (cache[entity4.getId()]!.item! === entity4)
            }
            // Retrieve Data
            accessor.setThrowError()
            switch collection.scan(criteria: nil) {
            case .error (let errorMessage):
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail("Expected .error")
            }
            logger.sync() { entries in
                XCTAssertEqual (1, entries.count)
                let entry = entries[0].asTestString()
                XCTAssertEqual ("ERROR|PersistentCollection<Database, MyStruct>.scan|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;errorMessage=Test Error", entry)
            }
        }
    }
    
    func testPersistentCollectionScanAsync() throws {
        var counter = 0
        while counter < 100 {
            var orderCode = 0
            while orderCode < 4 {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
                let dispatchGroup = DispatchGroup()
                // entity1, entity2: Data in accessor
                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
                entity1.setSchemaVersion (3)// Verify that get updates the schema version
                entity1.setPersistenceState (.persistent)
                entity1.setSaved (Date())
                let data1 = try accessor.encoder.encode(entity1)
                accessor.add(name: standardCollectionName, id: entity1.getId(), data: data1)
                let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
                entity2.setSchemaVersion (3)// Verify that get updates the schema version
                entity2.setPersistenceState (.persistent)
                entity2.setSaved (Date())
                let data2 = try accessor.encoder.encode(entity2)
                accessor.add(name: standardCollectionName, id: entity2.getId(), data: data2)
                // entity3, entity4: entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.setPersistenceState (.persistent)
                entity3.setSaved (Date())
                let data3 = try accessor.encoder.encode(entity3)
                accessor.add(name: standardCollectionName, id: entity3.getId(), data: data3)
                entity3 = collection.get (id: entity3.getId()).item()!
                var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                entity4.setPersistenceState (.persistent)
                entity4.setSaved (Date())
                let data4 = try accessor.encoder.encode(entity4)
                accessor.add(name: standardCollectionName, id: entity4.getId(), data: data4)
                entity4 = collection.get (id: entity4.getId()).item()!
                // Invalid Data
                let json = "{}"
                let invalidData = json.data(using: .utf8)!
                let invalidDataUuid = UUID()
                accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
                let test1 = {
                    collection.scan(criteria: nil) { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        var retrievedEntity1: Entity<MyStruct>? = nil
                        var retrievedEntity2: Entity<MyStruct>? = nil
                        var retrievedEntity3: Entity<MyStruct>? = nil
                        var retrievedEntity4: Entity<MyStruct>? = nil
                        for retrievedEntity in retrievedEntities {
                            XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
                            let persistenceState = retrievedEntity.getPersistenceState()
                            switch persistenceState {
                            case .persistent:
                                break
                            default:
                                XCTFail ("Expected .persistent but got \(persistenceState)")
                            }
                            if retrievedEntity.getId().uuidString == entity1.getId().uuidString {
                                XCTAssertNil (retrievedEntity1)
                                retrievedEntity1 = retrievedEntity
                            } else if retrievedEntity.getId().uuidString == entity2.getId().uuidString {
                                XCTAssertNil (retrievedEntity2)
                                retrievedEntity2 = retrievedEntity
                            } else if retrievedEntity.getId().uuidString == entity3.getId().uuidString {
                                XCTAssertNil (retrievedEntity3)
                                retrievedEntity3 = retrievedEntity
                            } else if retrievedEntity.getId().uuidString == entity4.getId().uuidString {
                                XCTAssertNil (retrievedEntity4)
                                retrievedEntity4 = retrievedEntity
                            } else {
                                XCTFail("Unknown Converted Entity")
                            }
                        }
                        XCTAssertEqual (4, retrievedEntities.count)
                        XCTAssertEqual (entity1.getId().uuidString, retrievedEntity1?.getId().uuidString)
                        entity1.sync() { item1 in
                            retrievedEntity1!.sync() { retrievedItem1 in
                                XCTAssertEqual (item1.myInt, retrievedItem1.myInt)
                                XCTAssertEqual (item1.myString, retrievedItem1.myString)
                            }
                        }
                        XCTAssertEqual (entity2.getId().uuidString, retrievedEntity2?.getId().uuidString)
                        entity2.sync() { item2 in
                            retrievedEntity2!.sync() { retrievedItem2 in
                                XCTAssertEqual (item2.myInt, retrievedItem2.myInt)
                                XCTAssertEqual (item2.myString, retrievedItem2.myString)
                            }
                        }
                        XCTAssertEqual (entity3.getId().uuidString, retrievedEntity3?.getId().uuidString)
                        XCTAssertTrue (entity3 === retrievedEntity3!)
                        XCTAssertEqual (entity4.getId().uuidString, retrievedEntity4?.getId().uuidString)
                        XCTAssertTrue (entity4 === retrievedEntity4!)
                        dispatchGroup.leave()
                    }
                }
                let test2 = {
                    collection.scan (criteria: { item in return (item.myInt == 10)}) { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        XCTAssertEqual (1, retrievedEntities.count)
                        let retrievedEntity = retrievedEntities[0]
                        XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
                        switch retrievedEntity.getPersistenceState() {
                        case .persistent:
                            break
                        default:
                            XCTFail ("Expected .persistent")
                        }
                        XCTAssertEqual (entity1.getId().uuidString, retrievedEntity.getId().uuidString)
                        entity1.sync() { item1 in
                            retrievedEntity.sync() { retrievedItem in
                                XCTAssertEqual (item1.myInt, retrievedItem.myInt)
                                XCTAssertEqual (item1.myString, retrievedItem.myString)
                            }
                        }
                        dispatchGroup.leave()
                    }
                }
                let test3 = {
                    collection.scan( criteria: { item in return (item.myInt == 30) }) { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        XCTAssertEqual (1, retrievedEntities.count)
                        let retrievedEntity = retrievedEntities[0]
                        XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
                        switch retrievedEntity.getPersistenceState() {
                        case .persistent:
                            break
                        default:
                            XCTFail ("Expected .persistent")
                        }
                        XCTAssert (entity3 === retrievedEntity)
                        dispatchGroup.leave()
                    }
                }
                var jobs: [() -> ()] = []
                switch orderCode {
                case 0:
                    jobs = [test1, test2, test3]
                case 1:
                    jobs = [test2, test1, test3]
                case 2:
                    jobs = [test1, test3, test2]
                case 3:
                    jobs = [test3, test2, test1]
                default:
                    XCTFail ("Unexpected Case")
                }
                for job in jobs {
                    dispatchGroup.enter()
                    job()
                }
                switch dispatchGroup.wait(timeout: DispatchTime.now() + 10) {
                case .success:
                    break
                default:
                    XCTFail ("Expected .success")
                }
                logger.sync() { entries in
                    XCTAssertEqual (0, entries.count)
                }
                orderCode = orderCode + 1
            }
            counter = counter + 1
        }
    }


    func testRegistrar() {
        let registrar = Registrar<Int, RegistrarTestItem>()
        XCTAssertEqual (0, registrar.count())
        let key1 = 10
        let value1 = RegistrarTestItem (stringValue: "10")
        let key2 = 20
        var value2: RegistrarTestItem? = RegistrarTestItem (stringValue: "20")
        let key3 = 30
        var value3: RegistrarTestItem? = RegistrarTestItem (stringValue: "30")
        XCTAssertTrue (registrar.register(key: key1, value: value1))
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.register(key: key1, value: value1))
        XCTAssertFalse (registrar.register(key: key1, value: value2!))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key2, value: value2!))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key2, value: value2!))
        XCTAssertFalse (registrar.register(key: key2, value: value1))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        registrar.deRegister(key: key2)
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key3, value: value3!))
        XCTAssertEqual (3, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
        value2 = nil
        registrar.deRegister(key: key2)
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key3, value: value3!))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
        value3 = nil
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        registrar.deRegister(key: key3)
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
    }
    
    func testValidationResult() {
        var validationResult = ValidationResult.ok
        XCTAssertTrue (validationResult.isOk())
        XCTAssertEqual ("ok", validationResult.description())
        validationResult = ValidationResult.error("Error Description")
        XCTAssertFalse (validationResult.isOk())
        XCTAssertEqual ("Error Description", validationResult.description())       
    }

}
