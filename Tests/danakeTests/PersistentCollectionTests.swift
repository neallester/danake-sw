//
//  persistenceTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/2/18.
//

import XCTest
@testable import danake

class PersistentCollectionTests: XCTestCase {
    
    func testCreation() {
        
        let myKey: CodingUserInfoKey = CodingUserInfoKey(rawValue: "myKey")!
        
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        var collection: PersistentCollection<MyStruct>? = PersistentCollection<MyStruct>(database: database, name: collectionName)
        let _ = collection // Quite a spurious xcode warning
        XCTAssertTrue (database.collectionRegistrar.isRegistered(key: collectionName))
        XCTAssertEqual (1, database.collectionRegistrar.count())
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
        let _ = PersistentCollection<MyStruct>(database: database, name: collectionName)
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.init|collectionAlreadyRegistered|database=Database;databaseHashValue=\(database.getAccessor().hashValue());collectionName=myCollection", entries[0].asTestString())
        }
        collection = nil
        XCTAssertFalse (database.collectionRegistrar.isRegistered(key: collectionName))
        XCTAssertEqual (0, database.collectionRegistrar.count())
        // Creation with closure
        let deserializationClosure: (inout [CodingUserInfoKey : Any]) -> () = { userInfo in
            userInfo[myKey] = "myValue"
        }
        collection = PersistentCollection<MyStruct> (database: database, name: collectionName, deserializationEnvironmentClosure: deserializationClosure)
        XCTAssertTrue (database.collectionRegistrar.isRegistered(key: collectionName))
        XCTAssertEqual (1, database.collectionRegistrar.count())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        var userInfo: [CodingUserInfoKey : Any] = [:]
        collection?.getDeserializationEnvironmentClosure()!(&userInfo)
        XCTAssertEqual ("myValue", userInfo[myKey] as! String)
    }

    func testCreationInvalidName() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = ""
        let _ = PersistentCollection<MyStruct>(database: database, name: collectionName)
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.init|Empty String is an illegal CollectionName|database=Database;accessor=InMemoryAccessor;databaseHashValue=\(database.accessor.hashValue());collectionName=", entries[0].asTestString())
        }
    }

    func testPersistenceCollectionNew() {
        // Creation with item
        let myStruct = MyStruct(myInt: 10, myString: "A String")
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
        var batch = EventuallyConsistentBatch()
        var entity: Entity<MyStruct>? = collection.new(batch: batch, item: myStruct)
        XCTAssertTrue (collection === entity!.collection)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            let item = entities[entity!.id]! as! Entity<MyStruct>
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
            XCTAssertTrue (entity === cache[entity!.id]!.item!)
        }
        XCTAssertEqual (5, entity?.getSchemaVersion())
        entity = nil
        batch = EventuallyConsistentBatch() // Ensures entity is collected
        collection.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
        // Creation with itemClosure
        
        entity = collection.new(batch: batch) { reference in
            return MyStruct (myInt: reference.version, myString: reference.id.uuidString)
        }
        XCTAssertTrue (collection === entity!.collection)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            let item = entities[entity!.id]! as! Entity<MyStruct>
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
            XCTAssertEqual(entity!.id.uuidString, item.myString)
        }
        collection.sync() { cache in
            XCTAssertEqual(1, cache.count)
            XCTAssertTrue (entity === cache[entity!.id]!.item!)
        }
        XCTAssertEqual (5, entity?.getSchemaVersion())
        entity = nil
        batch = EventuallyConsistentBatch() // Ensures entity is collected
        collection.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
    }
    
    func testGet() throws {
        // Data In Cache=No; Data in Accessor=No
        let entity = newTestEntity(myInt: 10, myString: "A String")

        entity.setSaved (Date())
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
        let data = try accessor.encoder.encode(entity)
        var result = collection.get (id: entity.id)
        XCTAssertTrue (result.isOk())
        XCTAssertNil (result.item())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            let entry = entries[0].asTestString()
            XCTAssertEqual ("WARNING|PersistentCollection<MyStruct>.get|Unknown id|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;id=\(entity.id)", entry)
        }
        // Data In Cache=No; Data in Accessor=Yes
        let _ = accessor.add(name: standardCollectionName, id: entity.id, data: data)
        result = collection.get(id: entity.id)
        let retrievedEntity = result.item()!
        XCTAssertEqual (entity.id.uuidString, retrievedEntity.id.uuidString)
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
            XCTAssertTrue (retrievedEntity === cache[entity.id]!.item!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Data In Cache=Yes; Data in Accessor=No
        let batch = EventuallyConsistentBatch()
        let entity2 = collection.new(batch: batch, item: MyStruct())
        XCTAssertTrue (entity2 === collection.get(id: entity2.id).item()!)
        XCTAssertEqual (5, entity2.getSchemaVersion())
        XCTAssertTrue (entity2.collection === collection)
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.id]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.id]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[standardCollectionName]![entity.id]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Data In Cache=Yes; Data in Accessor=Yes
        XCTAssertTrue (retrievedEntity === collection.get(id: entity.id).item()!)
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.id]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.id]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[standardCollectionName]![entity.id]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Invalid Data
        let json = "{}"
        let invalidData = json.data(using: .utf8)!
        let invalidDataUuid = UUID()
        let _ = accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData)
        let invalidEntity = collection.get(id: invalidDataUuid)
        switch invalidEntity {
        case .error (let errorMessage):
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        XCTAssertNil (invalidEntity.item())
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            let entry = entries[1].asTestString()
            XCTAssertEqual ("EMERGENCY|PersistentCollection<MyStruct>.get|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;id=\(invalidDataUuid);errorMessage=keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", entry)
        }
        // Database Error
        let entity3 = newTestEntity(myInt: 30, myString: "A String 3")
        let data3 = try JSONEncoder().encode(entity)
        let _ = accessor.add(name: standardCollectionName, id: entity3.id, data: data3)
        accessor.setThrowError()
        switch collection.get(id: entity3.id) {
        case .error (let errorMessage):
            XCTAssertEqual ("getError", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        logger.sync() { entries in
            XCTAssertEqual (3, entries.count)
            let entry = entries[2].asTestString()
            XCTAssertEqual ("EMERGENCY|PersistentCollection<MyStruct>.get|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;id=\(entity3.id);errorMessage=getError", entry)
        }
    }
    
    func testPerssistentCollectionGetParallel() throws {
        var counter = 0
        while counter < 100 {
            let accessor = InMemoryAccessor()
            let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
            let id1 = UUID()
            let data1 = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":5,\"created\":1525732114.0374,\"saved\":1525732132.8645,\"item\":{\"myInt\":10,\"myString\":\"A String1\"},\"persistenceState\":\"persistent\",\"version\":10}".data(using: .utf8)!
            let id2 = UUID()
            let data2 = "{\"id\":\"\(id2.uuidString)\",\"schemaVersion\":5,\"created\":1525732227.0376,\"saved\":1525732315.7534,\"item\":{\"myInt\":20,\"myString\":\"A String2\"},\"persistenceState\":\"persistent\",\"version\":20}".data(using: .utf8)!
            let dispatchGroup = DispatchGroup()
            let _ = accessor.add(name: standardCollectionName, id: id1, data: data1)
            let _ = accessor.add(name: standardCollectionName, id: id2, data: data2)
            let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
            let startSempaphore = DispatchSemaphore (value: 1)
            startSempaphore.wait()
            accessor.setPreFetch() { uuid in
                if uuid == id1 {
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
                result1a = collection.get(id: id1)
                dispatchGroup.leave()
            }
            dispatchGroup.enter()
            workQueue.async {
                result1b = collection.get(id: id1)
                dispatchGroup.leave()
            }
            workQueue.async {
                result2 = collection.get(id: id2)
                waitFor1.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertNil (result1a)
            XCTAssertNil (result1b)
            switch result2! {
            case .ok (let retrievedEntity):
                XCTAssertEqual (id2.uuidString, retrievedEntity!.id.uuidString)
                XCTAssertEqual (20, retrievedEntity!.getVersion())
                XCTAssertEqual (1525732227.0376, retrievedEntity!.created.timeIntervalSince1970)
                XCTAssertEqual (1525732315.7534, retrievedEntity!.getSaved()!.timeIntervalSince1970)
                switch retrievedEntity!.getPersistenceState() {
                case .persistent:
                    break
                default:
                    XCTFail("Expected .persistent")
                }
                retrievedEntity!.sync() { myStruct in
                    XCTAssertEqual (20, myStruct.myInt)
                    XCTAssertEqual ("A String2", myStruct.myString)
                }
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
            XCTAssertEqual (id1.uuidString, retrievedEntity1a!.id.uuidString)
            XCTAssertEqual (10, retrievedEntity1a!.getVersion())
            XCTAssertEqual (1525732114.0374, retrievedEntity1a!.created.timeIntervalSince1970)
            XCTAssertEqual (1525732132.8645, retrievedEntity1a!.getSaved()!.timeIntervalSince1970)
            switch retrievedEntity1a!.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail("Expected .persistent")
            }
            retrievedEntity1a!.sync() { myStruct in
                XCTAssertEqual (10, myStruct.myInt)
                XCTAssertEqual ("A String1", myStruct.myString)
            }
            counter = counter + 1
        }
    }

    
    func testGetAsync () throws {
        var counter = 0
        while counter < 100 {
            // Data In Cache=No; Data in Accessor=No
            let entity1 = newTestEntity(myInt: 10, myString: "A String1")
    
            entity1.setPersistenceState (.persistent)
            let data1 = try JSONEncoder().encode(entity1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String2")
            let data2 = try JSONEncoder().encode(entity2)
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .warning)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
            var waitFor1 = expectation(description: "wait1.1")
            var waitFor2 = expectation(description: "wait2.1")
            var result1: RetrievalResult<Entity<MyStruct>>? = nil
            var result2: RetrievalResult<Entity<MyStruct>>? = nil
            collection.get (id: entity1.id) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get (id: entity2.id) { item in
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
            let _ = accessor.add(name: standardCollectionName, id: entity1.id, data: data1)
            let _ = accessor.add(name: standardCollectionName, id: entity2.id, data: data2)
            collection.get(id: entity1.id) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get(id: entity2.id) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            var retrievedEntity1 = result1!.item()!
            var retrievedEntity2 = result2!.item()!
            XCTAssertEqual (entity1.id.uuidString, retrievedEntity1.id.uuidString)
            XCTAssertEqual (entity1.getVersion(), retrievedEntity1.getVersion())
            XCTAssertEqual (entity2.id.uuidString, retrievedEntity2.id.uuidString)
            XCTAssertEqual (entity2.getVersion(), retrievedEntity2.getVersion())
            XCTAssertEqual (5, retrievedEntity1.getSchemaVersion())
            XCTAssertEqual (5, retrievedEntity2.getSchemaVersion())
            XCTAssertTrue (retrievedEntity1.collection === collection)
            XCTAssertTrue (retrievedEntity2.collection === collection)
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
                XCTAssertTrue (retrievedEntity1 === cache[entity1.id]!.item!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.id]!.item!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=Yes; Data in Accessor=No
            waitFor1 = expectation(description: "wait1.3")
            waitFor2 = expectation(description: "wait2.3")
            let batch = EventuallyConsistentBatch()
            let entity3 = collection.new(batch: batch, item: MyStruct())
            let entity4 = collection.new(batch: batch, item: MyStruct())
            collection.get(id: entity3.id) { item in
                result1 = item
                waitFor1.fulfill()
                
            }
            collection.get(id: entity4.id)  { item in
                result2 = item
                waitFor2.fulfill()
                
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertTrue (entity3 === result1!.item()!)
            XCTAssertTrue (entity4 === result2!.item()!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.id]!.item!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.id]!.item!)
                XCTAssertTrue (entity3 === cache[entity3.id]!.item!)
                XCTAssertTrue (entity4 === cache[entity4.id]!.item!)
            }
            accessor.sync() { storage in
                XCTAssertEqual (2, storage[standardCollectionName]!.count)
                XCTAssertTrue (data1 == storage[standardCollectionName]![entity1.id]!)
                XCTAssertTrue (data2 == storage[standardCollectionName]![entity2.id]!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=Yes; Data in Accessor=Yes
            waitFor1 = expectation(description: "wait1.4")
            waitFor2 = expectation(description: "wait2.4")
            collection.get (id: entity1.id) { item in
                result1 = item
                waitFor1.fulfill()
            }
            collection.get (id: entity2.id) { item in
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
                XCTAssertTrue (retrievedEntity1 === cache[entity1.id]!.item!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.id]!.item!)
                XCTAssertTrue (entity3 === cache[entity3.id]!.item!)
                XCTAssertTrue (entity4 === cache[entity4.id]!.item!)
            }
            accessor.sync() { storage in
                XCTAssertEqual (2, storage[standardCollectionName]!.count)
                XCTAssertTrue (data1 == storage[standardCollectionName]![entity1.id]!)
                XCTAssertTrue (data2 == storage[standardCollectionName]![entity2.id]!)
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
            let _ = accessor.add(name: standardCollectionName, id: invalidDataUuid1, data: invalidData1)
            let _ = accessor.add(name: standardCollectionName, id: invalidDataUuid2, data: invalidData2)
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
                #if os(Linux)
                    XCTAssertEqual ("The operation could not be completed", errorMessage)
                #else
                    XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [], debugDescription: \"The given data was not valid JSON.\", underlyingError: Optional(Error Domain=NSCocoaErrorDomain Code=3840 \"Unexpected end of file during JSON parse.\" UserInfo={NSDebugDescription=Unexpected end of file during JSON parse.})))", errorMessage)
                #endif
            default:
                XCTFail ("Expected .error")
            }
            switch result2! {
            case .error (let errorMessage):
                XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", errorMessage)
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
            let _ = accessor.add(name: standardCollectionName, id: entity5.id, data: data5)
            let _ = accessor.add(name: standardCollectionName, id: entity6.id, data: data6)
            accessor.setThrowError()
            waitFor1 = expectation(description: "wait1.6")
            waitFor2 = expectation(description: "wait2.6")
            var errorsReported = 0
            collection.get(id: entity5.id) { item in
                switch item {
                case .error:
                    errorsReported = errorsReported + 1
                default:
                    break
                }
                waitFor1.fulfill()
            }
            collection.get(id: entity6.id) { item in
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
            }
            counter = counter + 1
        }
    }
    
    
    func testScan() throws {
        do {
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .warning)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
            var retrievedEntities = collection.scan(criteria: nil).item()!
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = collection.scan().item()!
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = (collection.scan() { myStruct in
                return (myStruct.myInt == 20)
            }).item()!
            XCTAssertEqual (0, retrievedEntities.count)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
    
            entity1.setPersistenceState (.persistent)
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            switch accessor.add(name: standardCollectionName, id: entity1.id, data: data1) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
    
            entity2.setPersistenceState (.persistent)
            entity2.setSaved (Date())
            let data2 = try accessor.encoder.encode(entity2)
            switch accessor.add(name: standardCollectionName, id: entity2.id, data: data2) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setPersistenceState (.persistent)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            switch accessor.add(name: standardCollectionName, id: entity3.id, data: data3) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity3 = collection.get (id: entity3.id).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.setPersistenceState (.persistent)
            entity4.setSaved (Date())
            let data4 = try accessor.encoder.encode(entity4)
            switch accessor.add(name: standardCollectionName, id: entity4.id, data: data4) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity4 = collection.get (id: entity4.id).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            switch accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.id]!.item! === entity3)
                XCTAssertTrue (cache[entity4.id]!.item! === entity4)
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
                if retrievedEntity.id.uuidString == entity1.id.uuidString {
                    XCTAssertNil (retrievedEntity1)
                    retrievedEntity1 = retrievedEntity
                } else if retrievedEntity.id.uuidString == entity2.id.uuidString {
                    XCTAssertNil (retrievedEntity2)
                    retrievedEntity2 = retrievedEntity
                } else if retrievedEntity.id.uuidString == entity3.id.uuidString {
                    XCTAssertNil (retrievedEntity3)
                    retrievedEntity3 = retrievedEntity
                } else if retrievedEntity.id.uuidString == entity4.id.uuidString {
                    XCTAssertNil (retrievedEntity4)
                    retrievedEntity4 = retrievedEntity
                } else {
                    XCTFail("Unknown Converted Entity")
                }
            }
            XCTAssertEqual (4, retrievedEntities.count)
            XCTAssertEqual (entity1.id.uuidString, retrievedEntity1!.id.uuidString)
            XCTAssertTrue (entity1 !== retrievedEntity1)
            entity1.sync() { item1 in
                retrievedEntity1!.sync() { retrievedItem1 in
                    XCTAssertEqual (item1.myInt, retrievedItem1.myInt)
                    XCTAssertEqual (item1.myString, retrievedItem1.myString)
                }
            }
            XCTAssertEqual (entity2.id.uuidString, retrievedEntity2?.id.uuidString)
            entity2.sync() { item2 in
                retrievedEntity2!.sync() { retrievedItem2 in
                    XCTAssertEqual (item2.myInt, retrievedItem2.myInt)
                    XCTAssertEqual (item2.myString, retrievedItem2.myString)
                }
            }
            XCTAssertEqual (entity3.id.uuidString, retrievedEntity3!.id.uuidString)
            XCTAssertTrue (entity3 === retrievedEntity3!)
            XCTAssertEqual (entity4.id.uuidString, retrievedEntity4!.id.uuidString)
            XCTAssertTrue (entity4 === retrievedEntity4!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (cache[entity1.id]!.item! === retrievedEntity1!)
                XCTAssertTrue (cache[entity2.id]!.item! === retrievedEntity2!)
                XCTAssertTrue (cache[entity3.id]!.item! === retrievedEntity3!)
                XCTAssertTrue (cache[entity4.id]!.item! === retrievedEntity4!)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            retrievedEntities = collection.scan().item()!
            retrievedEntity1 = nil
            retrievedEntity2 = nil
            retrievedEntity3 = nil
            retrievedEntity4 = nil
            for retrievedEntity in retrievedEntities {
                XCTAssertTrue (retrievedEntity.isInitialized(onCollection: collection))
                switch retrievedEntity.getPersistenceState() {
                case .persistent:
                    break
                default:
                    XCTFail ("Expected .persistent")
                }
                if retrievedEntity.id.uuidString == entity1.id.uuidString {
                    XCTAssertNil (retrievedEntity1)
                    retrievedEntity1 = retrievedEntity
                } else if retrievedEntity.id.uuidString == entity2.id.uuidString {
                    XCTAssertNil (retrievedEntity2)
                    retrievedEntity2 = retrievedEntity
                } else if retrievedEntity.id.uuidString == entity3.id.uuidString {
                    XCTAssertNil (retrievedEntity3)
                    retrievedEntity3 = retrievedEntity
                } else if retrievedEntity.id.uuidString == entity4.id.uuidString {
                    XCTAssertNil (retrievedEntity4)
                    retrievedEntity4 = retrievedEntity
                } else {
                    XCTFail("Unknown Converted Entity")
                }
            }
            XCTAssertEqual (4, retrievedEntities.count)
            XCTAssertEqual (entity1.id.uuidString, retrievedEntity1!.id.uuidString)
            XCTAssertTrue (entity1 !== retrievedEntity1)
            entity1.sync() { item1 in
                retrievedEntity1!.sync() { retrievedItem1 in
                    XCTAssertEqual (item1.myInt, retrievedItem1.myInt)
                    XCTAssertEqual (item1.myString, retrievedItem1.myString)
                }
            }
            XCTAssertEqual (entity2.id.uuidString, retrievedEntity2?.id.uuidString)
            entity2.sync() { item2 in
                retrievedEntity2!.sync() { retrievedItem2 in
                    XCTAssertEqual (item2.myInt, retrievedItem2.myInt)
                    XCTAssertEqual (item2.myString, retrievedItem2.myString)
                }
            }
            XCTAssertEqual (entity3.id.uuidString, retrievedEntity3!.id.uuidString)
            XCTAssertTrue (entity3 === retrievedEntity3!)
            XCTAssertEqual (entity4.id.uuidString, retrievedEntity4!.id.uuidString)
            XCTAssertTrue (entity4 === retrievedEntity4!)
            collection.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (cache[entity1.id]!.item! === retrievedEntity1!)
                XCTAssertTrue (cache[entity2.id]!.item! === retrievedEntity2!)
                XCTAssertTrue (cache[entity3.id]!.item! === retrievedEntity3!)
                XCTAssertTrue (cache[entity4.id]!.item! === retrievedEntity4!)
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
            let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
    
            entity1.setPersistenceState (.persistent)
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            switch accessor.add(name: standardCollectionName, id: entity1.id, data: data1) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
    
            entity2.setPersistenceState (.persistent)
            entity2.setSaved (Date())
            let data2 = try accessor.encoder.encode(entity2)
            switch accessor.add(name: standardCollectionName, id: entity2.id, data: data2) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setPersistenceState (.persistent)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            switch accessor.add(name: standardCollectionName, id: entity3.id, data: data3) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity3 = collection.get (id: entity3.id).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.setPersistenceState (.persistent)
            entity4.setSaved (Date())
            let data4 = try accessor.encoder.encode(entity4)
            switch accessor.add(name: standardCollectionName, id: entity4.id, data: data4) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity4 = collection.get (id: entity4.id).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            switch accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.id]!.item! === entity3)
                XCTAssertTrue (cache[entity4.id]!.item! === entity4)
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
            XCTAssertEqual (entity1.id.uuidString, retrievedEntity.id.uuidString)
            entity1.sync() { item1 in
                retrievedEntity.sync() { retrievedItem in
                    XCTAssertEqual (item1.myInt, retrievedItem.myInt)
                    XCTAssertEqual (item1.myString, retrievedItem.myString)
                }
            }
            collection.sync() { cache in
                XCTAssertEqual (3, cache.count)
                XCTAssertTrue (cache[entity1.id]!.item! === retrievedEntity)
                XCTAssertTrue (cache[entity3.id]!.item! === entity3)
                XCTAssertTrue (cache[entity4.id]!.item! === entity4)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            // From scratch with Criteria selecting entity3 (Entity cache, data in accessor)
            do {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
                // entity1, entity2: Data in accessor
                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
        
                entity1.setPersistenceState (.persistent)
                entity1.setSaved (Date())
                let data1 = try accessor.encoder.encode(entity1)
                switch accessor.add(name: standardCollectionName, id: entity1.id, data: data1) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
        
                entity2.setPersistenceState (.persistent)
                entity2.setSaved (Date())
                let data2 = try accessor.encoder.encode(entity2)
                switch accessor.add(name: standardCollectionName, id: entity2.id, data: data2) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                // entity3, entity4: entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.setPersistenceState (.persistent)
                entity3.setSaved (Date())
                let data3 = try accessor.encoder.encode(entity3)
                switch accessor.add(name: standardCollectionName, id: entity3.id, data: data3) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity3 = collection.get (id: entity3.id).item()!
                var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                entity4.setPersistenceState (.persistent)
                entity4.setSaved (Date())
                let data4 = try accessor.encoder.encode(entity4)
                switch accessor.add(name: standardCollectionName, id: entity4.id, data: data4) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity4 = collection.get (id: entity4.id).item()!
                // Invalid Data
                let json = "{}"
                let invalidData = json.data(using: .utf8)!
                let invalidDataUuid = UUID()
                switch accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                collection.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity3.id]!.item! === entity3)
                    XCTAssertTrue (cache[entity4.id]!.item! === entity4)
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
                    XCTAssertTrue (cache[entity3.id]!.item! === entity3)
                    XCTAssertTrue (cache[entity4.id]!.item! === entity4)
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
            let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
    
            entity1.setPersistenceState (.persistent)
            entity1.setSaved (Date())
            let data1 = try accessor.encoder.encode(entity1)
            switch accessor.add(name: standardCollectionName, id: entity1.id, data: data1) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
    
            entity2.setPersistenceState (.persistent)
            entity2.setSaved (Date())
            let data2 = try accessor.encoder.encode(entity2)
            switch accessor.add(name: standardCollectionName, id: entity2.id, data: data2) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.setPersistenceState (.persistent)
            entity3.setSaved (Date())
            let data3 = try accessor.encoder.encode(entity3)
            switch accessor.add(name: standardCollectionName, id: entity3.id, data: data3) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity3 = collection.get (id: entity3.id).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.setPersistenceState (.persistent)
            entity4.setSaved (Date())
            let data4 = try accessor.encoder.encode(entity4)
            switch accessor.add(name: standardCollectionName, id: entity4.id, data: data4) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity4 = collection.get (id: entity4.id).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            switch accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            collection.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.id]!.item! === entity3)
                XCTAssertTrue (cache[entity4.id]!.item! === entity4)
            }
            // Retrieve Data
            accessor.setThrowError()
            switch collection.scan(criteria: nil) {
            case .error (let errorMessage):
                XCTAssertEqual ("scanError", errorMessage)
            default:
                XCTFail("Expected .error")
            }
            accessor.setThrowError()
            switch collection.scan() {
            case .error (let errorMessage):
                XCTAssertEqual ("scanError", errorMessage)
            default:
                XCTFail("Expected .error")
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
                var entry = entries[0].asTestString()
                XCTAssertEqual ("EMERGENCY|PersistentCollection<MyStruct>.scan|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;errorMessage=scanError", entry)
                entry = entries[1].asTestString()
                XCTAssertEqual ("EMERGENCY|PersistentCollection<MyStruct>.scan|Database Error|databaseHashValue=\(database.accessor.hashValue());collection=myCollection;errorMessage=scanError", entry)
            }
        }
    }
    
    func testScanAsync() throws {
        var counter = 0
        while counter < 100 {
            var orderCode = 0
            while orderCode < 8 {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let collection = PersistentCollection<MyStruct>(database: database, name: standardCollectionName)
                let dispatchGroup = DispatchGroup()
                // entity1, entity2: Data in accessor
                let id1 = UUID()
                let data1 = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":5,\"created\":1525735252.2328,\"saved\":1525735368.8345,\"item\":{\"myInt\":10,\"myString\":\"A String 1\"},\"persistenceState\":\"persistent\",\"version\":1}".data(using: .utf8)!
                switch accessor.add(name: standardCollectionName, id: id1, data: data1) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                let id2 = UUID()
                let data2 = "{\"id\":\"\(id2.uuidString)\",\"schemaVersion\":5,\"created\":1525735262.2330,\"saved\":1525735311.3375,\"item\":{\"myInt\":20,\"myString\":\"A String 2\"},\"persistenceState\":\"persistent\",\"version\":2}".data(using: .utf8)!
                switch accessor.add(name: standardCollectionName, id: id2, data: data2) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                // entity3, entity4: entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.setPersistenceState (.persistent)
                entity3.setSaved (Date())
                let data3 = try accessor.encoder.encode(entity3)
                switch accessor.add(name: standardCollectionName, id: entity3.id, data: data3) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity3 = collection.get (id: entity3.id).item()!
                var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                entity4.setPersistenceState (.persistent)
                entity4.setSaved (Date())
                let data4 = try accessor.encoder.encode(entity4)
                switch accessor.add(name: standardCollectionName, id: entity4.id, data: data4) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity4 = collection.get (id: entity4.id).item()!
                // Invalid Data
                let json = "{}"
                let invalidData = json.data(using: .utf8)!
                let invalidDataUuid = UUID()
                switch accessor.add(name: standardCollectionName, id: invalidDataUuid, data: invalidData) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                let test1a = {
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
                            if retrievedEntity.id.uuidString == id1.uuidString {
                                XCTAssertNil (retrievedEntity1)
                                retrievedEntity1 = retrievedEntity
                            } else if retrievedEntity.id.uuidString == id2.uuidString {
                                XCTAssertNil (retrievedEntity2)
                                retrievedEntity2 = retrievedEntity
                            } else if retrievedEntity.id.uuidString == entity3.id.uuidString {
                                XCTAssertNil (retrievedEntity3)
                                retrievedEntity3 = retrievedEntity
                            } else if retrievedEntity.id.uuidString == entity4.id.uuidString {
                                XCTAssertNil (retrievedEntity4)
                                retrievedEntity4 = retrievedEntity
                            } else {
                                XCTFail("Unknown Converted Entity")
                            }
                        }
                        XCTAssertEqual (4, retrievedEntities.count)
                        XCTAssertEqual (id1.uuidString, retrievedEntity1?.id.uuidString)
                        retrievedEntity1!.sync() { retrievedItem1 in
                            XCTAssertEqual (10, retrievedItem1.myInt)
                            XCTAssertEqual ("A String 1", retrievedItem1.myString)
                        }
                        XCTAssertEqual (id2.uuidString, retrievedEntity2?.id.uuidString)
                        retrievedEntity2!.sync() { retrievedItem2 in
                            XCTAssertEqual (20, retrievedItem2.myInt)
                            XCTAssertEqual ("A String 2", retrievedItem2.myString)
                        }
                        XCTAssertEqual (entity3.id.uuidString, retrievedEntity3?.id.uuidString)
                        XCTAssertTrue (entity3 === retrievedEntity3!)
                        XCTAssertEqual (entity4.id.uuidString, retrievedEntity4?.id.uuidString)
                        XCTAssertTrue (entity4 === retrievedEntity4!)
                        dispatchGroup.leave()
                    }
                }
                let test1b = {
                    collection.scan() { retrievalResult in
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
                            if retrievedEntity.id.uuidString == id1.uuidString {
                                XCTAssertNil (retrievedEntity1)
                                retrievedEntity1 = retrievedEntity
                            } else if retrievedEntity.id.uuidString == id2.uuidString {
                                XCTAssertNil (retrievedEntity2)
                                retrievedEntity2 = retrievedEntity
                            } else if retrievedEntity.id.uuidString == entity3.id.uuidString {
                                XCTAssertNil (retrievedEntity3)
                                retrievedEntity3 = retrievedEntity
                            } else if retrievedEntity.id.uuidString == entity4.id.uuidString {
                                XCTAssertNil (retrievedEntity4)
                                retrievedEntity4 = retrievedEntity
                            } else {
                                XCTFail("Unknown Converted Entity")
                            }
                        }
                        XCTAssertEqual (4, retrievedEntities.count)
                        XCTAssertEqual (id1.uuidString, retrievedEntity1?.id.uuidString)
                        retrievedEntity1!.sync() { retrievedItem1 in
                            XCTAssertEqual (10, retrievedItem1.myInt)
                            XCTAssertEqual ("A String 1", retrievedItem1.myString)
                        }
                        XCTAssertEqual (id2.uuidString, retrievedEntity2?.id.uuidString)
                        retrievedEntity2!.sync() { retrievedItem2 in
                            XCTAssertEqual (20, retrievedItem2.myInt)
                            XCTAssertEqual ("A String 2", retrievedItem2.myString)
                        }
                        XCTAssertEqual (entity3.id.uuidString, retrievedEntity3?.id.uuidString)
                        XCTAssertTrue (entity3 === retrievedEntity3!)
                        XCTAssertEqual (entity4.id.uuidString, retrievedEntity4?.id.uuidString)
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
                        XCTAssertEqual (id1.uuidString, retrievedEntity.id.uuidString)
                        retrievedEntity.sync() { retrievedItem in
                            XCTAssertEqual (10, retrievedItem.myInt)
                            XCTAssertEqual ("A String 1", retrievedItem.myString)
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
                    jobs = [test1a, test2, test3]
                case 1:
                    jobs = [test2, test1a, test3]
                case 2:
                    jobs = [test1a, test3, test2]
                case 3:
                    jobs = [test3, test2, test1a]
                case 4:
                    jobs = [test1b, test2, test3]
                case 5:
                    jobs = [test2, test1b, test3]
                case 6:
                    jobs = [test1b, test3, test2]
                case 7:
                    jobs = [test3, test2, test1b]
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
    
}
