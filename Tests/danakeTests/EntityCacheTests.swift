//
//  persistenceTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/2/18.
//

import XCTest
@testable import danake

class EntityCacheTests: XCTestCase {
    
    func testCreation() {
        let myKey: CodingUserInfoKey = CodingUserInfoKey(rawValue: "myKey")!
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        database.cacheRegistrar.clear()
        Database.cacheRegistrar.clear()
        let cacheName: CacheName = "myCollection"
        var cache: EntityCache<MyStruct>? = EntityCache<MyStruct>(database: database, name: cacheName)
        let _ = cache // Quite a spurious xcode warning
        XCTAssertTrue (database.cacheRegistrar.isRegistered(key: cacheName))
        XCTAssertEqual (1, database.cacheRegistrar.count())
        XCTAssertTrue (Database.cacheRegistrar.isRegistered(key: cache!.qualifiedName))
        XCTAssertEqual (1, Database.cacheRegistrar.count())

        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
        let _ = EntityCache<MyStruct>(database: database, name: cacheName)
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            XCTAssertEqual ("ERROR|EntityCache<MyStruct>.init|cacheAlreadyRegistered|database=Database;databaseHashValue=\(database.accessor.hashValue);cacheName=myCollection", entries[0].asTestString())
            XCTAssertEqual ("ERROR|EntityCache<MyStruct>.init|qualifiedCollectionAlreadyRegistered|qualifiedCacheName=\(database.accessor.hashValue).myCollection", entries[1].asTestString())
        }
        cache = nil
        XCTAssertFalse (database.cacheRegistrar.isRegistered(key: cacheName))
        XCTAssertEqual (0, database.cacheRegistrar.count())
        XCTAssertFalse (Database.cacheRegistrar.isRegistered(key: cacheName))
        XCTAssertEqual (0, Database.cacheRegistrar.count())
        // Creation with closure
        let deserializationClosure: (inout [CodingUserInfoKey : Any]) -> () = { userInfo in
            userInfo[myKey] = "myValue"
        }
        cache = EntityCache<MyStruct> (database: database, name: cacheName, userInfoClosure: deserializationClosure)
        XCTAssertTrue (database.cacheRegistrar.isRegistered(key: cacheName))
        XCTAssertEqual (1, database.cacheRegistrar.count())
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
        }
        var userInfo: [CodingUserInfoKey : Any] = [:]
        cache?.getDeserializationEnvironmentClosure()!(&userInfo)
        XCTAssertEqual ("myValue", userInfo[myKey] as! String)
    }

    func testCreationInvalidName() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .error)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = ""
        let _ = EntityCache<MyStruct>(database: database, name: cacheName)
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|EntityCache<MyStruct>.init|Empty String is an illegal CacheName|database=Database;accessor=InMemoryAccessor;databaseHashValue=\(database.accessor.hashValue);cacheName=", entries[0].asTestString())
        }
    }

    func testPersistenceCollectionNew() {
        // Creation with item
        let myStruct = MyStruct(myInt: 10, myString: "A String")
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        var batch = EventuallyConsistentBatch()
        var entity: Entity<MyStruct>? = cache.new(batch: batch, item: myStruct)
        XCTAssertTrue (cache === entity!.cache)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            let item = entities[entity!.id]! as! Entity<MyStruct>
            XCTAssertTrue (item === entity!)
        }
        switch entity!.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity!.sync() { item in
            XCTAssertEqual(10, item.myInt)
            XCTAssertEqual("A String", item.myString)
        }
        cache.sync() { cache in
            XCTAssertEqual(1, cache.count)
            XCTAssertTrue (entity === cache[entity!.id]!.codable!)
        }
        XCTAssertEqual (5, entity?.getSchemaVersion())
        entity = nil
        batch = EventuallyConsistentBatch() // Ensures entity is collected
        cache.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
        // Creation with itemClosure
        
        entity = cache.new(batch: batch) { reference in
            return MyStruct (myInt: reference.version, myString: reference.id.uuidString)
        }
        XCTAssertTrue (cache === entity!.cache)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            let item = entities[entity!.id]! as! Entity<MyStruct>
            XCTAssertTrue (item === entity!)
        }
        switch entity!.persistenceState {
        case .new:
            break
        default:
            XCTFail("Expected .new")
        }
        entity!.sync() { item in
            XCTAssertEqual(0, item.myInt)
            XCTAssertEqual(entity!.id.uuidString, item.myString)
        }
        cache.sync() { cache in
            XCTAssertEqual(1, cache.count)
            XCTAssertTrue (entity === cache[entity!.id]!.codable!)
        }
        XCTAssertEqual (5, entity?.getSchemaVersion())
        entity = nil
        batch = EventuallyConsistentBatch() // Ensures entity is collected
        cache.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
    }
    
    func testGet() throws {
        // Data In Cache=No; Data in Accessor=No
        let entity = newTestEntity(myInt: 10, myString: "A String")

        entity.saved = Date()
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        let data = try accessor.encoder.encode(entity)
        var result = cache.get (id: entity.id)
        XCTAssertTrue (result.isOk())
        XCTAssertNil (result.item())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            let entry = entries[0].asTestString()
            XCTAssertEqual ("WARNING|EntityCache<MyStruct>.get|Unknown id|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;id=\(entity.id)", entry)
        }
        // Data In Cache=No; Data in Accessor=Yes
        let _ = accessor.add(name: standardCacheName, id: entity.id, data: data)
        result = cache.get(id: entity.id)
        let retrievedEntity = result.item()!
        XCTAssertEqual (entity.id.uuidString, retrievedEntity.id.uuidString)
        XCTAssertEqual (entity.version, retrievedEntity.version)
        XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
        XCTAssertTrue (entity.created.roughlyEquals (retrievedEntity.created, millisecondPrecision: 2))
        XCTAssertTrue (entity.saved!.roughlyEquals (retrievedEntity.saved!, millisecondPrecision: 2))
        switch retrievedEntity.persistenceState {
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
        cache.sync() { cache in
            XCTAssertEqual (1, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.id]!.codable!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Data In Cache=Yes; Data in Accessor=No
        let batch = EventuallyConsistentBatch()
        let entity2 = cache.new(batch: batch, item: MyStruct())
        XCTAssertTrue (entity2 === cache.get(id: entity2.id).item()!)
        XCTAssertEqual (5, entity2.getSchemaVersion())
        XCTAssertTrue (entity2.cache === cache)
        cache.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.id]!.codable!)
            XCTAssertTrue (entity2 === cache[entity2.id]!.codable!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[standardCacheName]![entity.id]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Data In Cache=Yes; Data in Accessor=Yes
        XCTAssertTrue (retrievedEntity === cache.get(id: entity.id).item()!)
        cache.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.id]!.codable!)
            XCTAssertTrue (entity2 === cache[entity2.id]!.codable!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[standardCacheName]![entity.id]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Invalid Data
        let json = "{}"
        let invalidData = json.data(using: .utf8)!
        let invalidDataUuid = UUID()
        let _ = accessor.add(name: standardCacheName, id: invalidDataUuid, data: invalidData)
        let invalidEntity = cache.get(id: invalidDataUuid)
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
            XCTAssertEqual ("EMERGENCY|EntityCache<MyStruct>.get|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;id=\(invalidDataUuid);errorMessage=keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", entry)
        }
        // Database Error
        let entity3 = newTestEntity(myInt: 30, myString: "A String 3")
        let data3 = try JSONEncoder().encode(entity)
        let _ = accessor.add(name: standardCacheName, id: entity3.id, data: data3)
        accessor.setThrowError()
        switch cache.get(id: entity3.id) {
        case .error (let errorMessage):
            XCTAssertEqual ("getError", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        logger.sync() { entries in
            XCTAssertEqual (3, entries.count)
            let entry = entries[2].asTestString()
            XCTAssertEqual ("EMERGENCY|EntityCache<MyStruct>.get|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;id=\(entity3.id);errorMessage=getError", entry)
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
            let _ = accessor.add(name: standardCacheName, id: id1, data: data1)
            let _ = accessor.add(name: standardCacheName, id: id2, data: data2)
            let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
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
                result1a = cache.get(id: id1)
                dispatchGroup.leave()
            }
            dispatchGroup.enter()
            workQueue.async {
                result1b = cache.get(id: id1)
                dispatchGroup.leave()
            }
            workQueue.async {
                result2 = cache.get(id: id2)
                waitFor1.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertNil (result1a)
            XCTAssertNil (result1b)
            switch result2! {
            case .ok (let retrievedEntity):
                XCTAssertEqual (id2.uuidString, retrievedEntity!.id.uuidString)
                XCTAssertEqual (20, retrievedEntity!.version)
                XCTAssertEqual (1525732227.0376, retrievedEntity!.created.timeIntervalSince1970)
                XCTAssertEqual (1525732315.7534, retrievedEntity!.saved!.timeIntervalSince1970)
                switch retrievedEntity!.persistenceState {
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
            XCTAssertEqual (10, retrievedEntity1a!.version)
            XCTAssertEqual (1525732114.0374, retrievedEntity1a!.created.timeIntervalSince1970)
            XCTAssertEqual (1525732132.8645, retrievedEntity1a!.saved!.timeIntervalSince1970)
            switch retrievedEntity1a!.persistenceState {
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
    
            entity1.persistenceState = .persistent
            let data1 = try JSONEncoder().encode(entity1)
            let entity2 = newTestEntity(myInt: 20, myString: "A String2")
            let data2 = try JSONEncoder().encode(entity2)
            let accessor = InMemoryAccessor()
            let logger = InMemoryLogger(level: .warning)
            let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
            let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
            var waitFor1 = expectation(description: "wait1.1")
            var waitFor2 = expectation(description: "wait2.1")
            var result1: RetrievalResult<Entity<MyStruct>>? = nil
            var result2: RetrievalResult<Entity<MyStruct>>? = nil
            cache.get (id: entity1.id) { item in
                result1 = item
                waitFor1.fulfill()
            }
            cache.get (id: entity2.id) { item in
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
            let _ = accessor.add(name: standardCacheName, id: entity1.id, data: data1)
            let _ = accessor.add(name: standardCacheName, id: entity2.id, data: data2)
            cache.get(id: entity1.id) { item in
                result1 = item
                waitFor1.fulfill()
            }
            cache.get(id: entity2.id) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            var retrievedEntity1 = result1!.item()!
            var retrievedEntity2 = result2!.item()!
            XCTAssertEqual (entity1.id.uuidString, retrievedEntity1.id.uuidString)
            XCTAssertEqual (entity1.version, retrievedEntity1.version)
            XCTAssertEqual (entity2.id.uuidString, retrievedEntity2.id.uuidString)
            XCTAssertEqual (entity2.version, retrievedEntity2.version)
            XCTAssertEqual (5, retrievedEntity1.getSchemaVersion())
            XCTAssertEqual (5, retrievedEntity2.getSchemaVersion())
            XCTAssertTrue (retrievedEntity1.cache === cache)
            XCTAssertTrue (retrievedEntity2.cache === cache)
            switch retrievedEntity1.persistenceState {
            case .persistent:
                break
            default:
                XCTFail("Expected .persistent")
            }
            switch retrievedEntity2.persistenceState {
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
            cache.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.id]!.codable!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.id]!.codable!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=Yes; Data in Accessor=No
            waitFor1 = expectation(description: "wait1.3")
            waitFor2 = expectation(description: "wait2.3")
            let batch = EventuallyConsistentBatch()
            let entity3 = cache.new(batch: batch, item: MyStruct())
            let entity4 = cache.new(batch: batch, item: MyStruct())
            cache.get(id: entity3.id) { item in
                result1 = item
                waitFor1.fulfill()
                
            }
            cache.get(id: entity4.id)  { item in
                result2 = item
                waitFor2.fulfill()
                
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertTrue (entity3 === result1!.item()!)
            XCTAssertTrue (entity4 === result2!.item()!)
            cache.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.id]!.codable!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.id]!.codable!)
                XCTAssertTrue (entity3 === cache[entity3.id]!.codable!)
                XCTAssertTrue (entity4 === cache[entity4.id]!.codable!)
            }
            accessor.sync() { storage in
                XCTAssertEqual (2, storage[standardCacheName]!.count)
                XCTAssertTrue (data1 == storage[standardCacheName]![entity1.id]!)
                XCTAssertTrue (data2 == storage[standardCacheName]![entity2.id]!)
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=Yes; Data in Accessor=Yes
            waitFor1 = expectation(description: "wait1.4")
            waitFor2 = expectation(description: "wait2.4")
            cache.get (id: entity1.id) { item in
                result1 = item
                waitFor1.fulfill()
            }
            cache.get (id: entity2.id) { item in
                result2 = item
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            retrievedEntity1 = result1!.item()!
            retrievedEntity2 = result2!.item()!
            XCTAssertTrue (retrievedEntity1 === result1!.item()!)
            XCTAssertTrue (retrievedEntity2 === result2!.item()!)
            cache.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (retrievedEntity1 === cache[entity1.id]!.codable!)
                XCTAssertTrue (retrievedEntity2 === cache[entity2.id]!.codable!)
                XCTAssertTrue (entity3 === cache[entity3.id]!.codable!)
                XCTAssertTrue (entity4 === cache[entity4.id]!.codable!)
            }
            accessor.sync() { storage in
                XCTAssertEqual (2, storage[standardCacheName]!.count)
                XCTAssertTrue (data1 == storage[standardCacheName]![entity1.id]!)
                XCTAssertTrue (data2 == storage[standardCacheName]![entity2.id]!)
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
            let _ = accessor.add(name: standardCacheName, id: invalidDataUuid1, data: invalidData1)
            let _ = accessor.add(name: standardCacheName, id: invalidDataUuid2, data: invalidData2)
            cache.get(id: invalidDataUuid1) { item in
                result1 = item
                waitFor1.fulfill()
            }
            cache.get(id: invalidDataUuid2) { item in
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
            let _ = accessor.add(name: standardCacheName, id: entity5.id, data: data5)
            let _ = accessor.add(name: standardCacheName, id: entity6.id, data: data6)
            accessor.setThrowError()
            waitFor1 = expectation(description: "wait1.6")
            waitFor2 = expectation(description: "wait2.6")
            var errorsReported = 0
            cache.get(id: entity5.id) { item in
                switch item {
                case .error:
                    errorsReported = errorsReported + 1
                default:
                    break
                }
                waitFor1.fulfill()
            }
            cache.get(id: entity6.id) { item in
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
            let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
            var retrievedEntities = cache.scan(criteria: nil).item()!
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = cache.scan().item()!
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = (cache.scan() { myStruct in
                return (myStruct.myInt == 20)
            }).item()!
            XCTAssertEqual (0, retrievedEntities.count)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
    
            entity1.persistenceState = .persistent
            entity1.saved = Date()
            let data1 = try accessor.encoder.encode(entity1)
            switch accessor.add(name: standardCacheName, id: entity1.id, data: data1) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
    
            entity2.persistenceState = .persistent
            entity2.saved = Date()
            let data2 = try accessor.encoder.encode(entity2)
            switch accessor.add(name: standardCacheName, id: entity2.id, data: data2) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.persistenceState = .persistent
            entity3.saved = Date()
            let data3 = try accessor.encoder.encode(entity3)
            switch accessor.add(name: standardCacheName, id: entity3.id, data: data3) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity3 = cache.get (id: entity3.id).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.persistenceState = .persistent
            entity4.saved = Date()
            let data4 = try accessor.encoder.encode(entity4)
            switch accessor.add(name: standardCacheName, id: entity4.id, data: data4) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity4 = cache.get (id: entity4.id).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            switch accessor.add(name: standardCacheName, id: invalidDataUuid, data: invalidData) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            cache.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.id]!.codable! === entity3)
                XCTAssertTrue (cache[entity4.id]!.codable! === entity4)
            }
            // Retrieve Data
            retrievedEntities = cache.scan(criteria: nil).item()!
            var retrievedEntity1: Entity<MyStruct>? = nil
            var retrievedEntity2: Entity<MyStruct>? = nil
            var retrievedEntity3: Entity<MyStruct>? = nil
            var retrievedEntity4: Entity<MyStruct>? = nil
            for retrievedEntity in retrievedEntities {
                XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                switch retrievedEntity.persistenceState {
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
            cache.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (cache[entity1.id]!.codable! === retrievedEntity1!)
                XCTAssertTrue (cache[entity2.id]!.codable! === retrievedEntity2!)
                XCTAssertTrue (cache[entity3.id]!.codable! === retrievedEntity3!)
                XCTAssertTrue (cache[entity4.id]!.codable! === retrievedEntity4!)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            retrievedEntities = cache.scan().item()!
            retrievedEntity1 = nil
            retrievedEntity2 = nil
            retrievedEntity3 = nil
            retrievedEntity4 = nil
            for retrievedEntity in retrievedEntities {
                XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                switch retrievedEntity.persistenceState {
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
            cache.sync() { cache in
                XCTAssertEqual (4, cache.count)
                XCTAssertTrue (cache[entity1.id]!.codable! === retrievedEntity1!)
                XCTAssertTrue (cache[entity2.id]!.codable! === retrievedEntity2!)
                XCTAssertTrue (cache[entity3.id]!.codable! === retrievedEntity3!)
                XCTAssertTrue (cache[entity4.id]!.codable! === retrievedEntity4!)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            
            // With criteria
            var retrievalResult = cache.scan() { myStruct in
                return (myStruct.myInt == 20)
            }
            retrievedEntities = retrievalResult.item()!
            XCTAssertEqual (1, retrievedEntities.count)
            XCTAssertTrue (retrievedEntities[0] === retrievedEntity2!)
            // With criteria matching none
            retrievalResult = cache.scan() { myStruct in
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
            let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
    
            entity1.persistenceState = .persistent
            entity1.saved = Date()
            let data1 = try accessor.encoder.encode(entity1)
            switch accessor.add(name: standardCacheName, id: entity1.id, data: data1) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
    
            entity2.persistenceState = .persistent
            entity2.saved = Date()
            let data2 = try accessor.encoder.encode(entity2)
            switch accessor.add(name: standardCacheName, id: entity2.id, data: data2) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.persistenceState = .persistent
            entity3.saved = Date()
            let data3 = try accessor.encoder.encode(entity3)
            switch accessor.add(name: standardCacheName, id: entity3.id, data: data3) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity3 = cache.get (id: entity3.id).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.persistenceState = .persistent
            entity4.saved = Date()
            let data4 = try accessor.encoder.encode(entity4)
            switch accessor.add(name: standardCacheName, id: entity4.id, data: data4) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity4 = cache.get (id: entity4.id).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            switch accessor.add(name: standardCacheName, id: invalidDataUuid, data: invalidData) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            cache.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.id]!.codable! === entity3)
                XCTAssertTrue (cache[entity4.id]!.codable! === entity4)
            }
            // Retrieve Data
            let retrievedEntities = (cache.scan(){ item in
                return (item.myInt == 10)
            }).item()!
            XCTAssertEqual (1, retrievedEntities.count)
            let retrievedEntity = retrievedEntities[0]
            XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
            switch retrievedEntity.persistenceState {
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
            cache.sync() { cache in
                XCTAssertEqual (3, cache.count)
                XCTAssertTrue (cache[entity1.id]!.codable! === retrievedEntity)
                XCTAssertTrue (cache[entity3.id]!.codable! === entity3)
                XCTAssertTrue (cache[entity4.id]!.codable! === entity4)
            }
            logger.sync() { entries in
                XCTAssertEqual (0, entries.count)
            }
            // From scratch with Criteria selecting entity3 (Entity cache, data in accessor)
            do {
                let accessor = InMemoryAccessor()
                let logger = InMemoryLogger(level: .error)
                let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
                let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
                // entity1, entity2: Data in accessor
                let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
        
                entity1.persistenceState = .persistent
                entity1.saved = Date()
                let data1 = try accessor.encoder.encode(entity1)
                switch accessor.add(name: standardCacheName, id: entity1.id, data: data1) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
        
                entity2.persistenceState = .persistent
                entity2.saved = Date()
                let data2 = try accessor.encoder.encode(entity2)
                switch accessor.add(name: standardCacheName, id: entity2.id, data: data2) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                // entity3, entity4: entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.persistenceState = .persistent
                entity3.saved = Date()
                let data3 = try accessor.encoder.encode(entity3)
                switch accessor.add(name: standardCacheName, id: entity3.id, data: data3) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity3 = cache.get (id: entity3.id).item()!
                var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                entity4.persistenceState = .persistent
                entity4.saved = Date()
                let data4 = try accessor.encoder.encode(entity4)
                switch accessor.add(name: standardCacheName, id: entity4.id, data: data4) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity4 = cache.get (id: entity4.id).item()!
                // Invalid Data
                let json = "{}"
                let invalidData = json.data(using: .utf8)!
                let invalidDataUuid = UUID()
                switch accessor.add(name: standardCacheName, id: invalidDataUuid, data: invalidData) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                cache.sync() { cache in
                    XCTAssertEqual (2, cache.count)
                    XCTAssertTrue (cache[entity3.id]!.codable! === entity3)
                    XCTAssertTrue (cache[entity4.id]!.codable! === entity4)
                }
                // Retrieve Data
                let retrievedEntities = (cache.scan(){ item in
                    return (item.myInt == 30)
                }).item()!
                XCTAssertEqual (1, retrievedEntities.count)
                let retrievedEntity = retrievedEntities[0]
                XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                switch retrievedEntity.persistenceState {
                case .persistent:
                    break
                default:
                    XCTFail ("Expected .persistent")
                }
                XCTAssert (entity3 === retrievedEntity)
                cache.sync() { cache in
                    XCTAssertTrue (cache[entity3.id]!.codable! === entity3)
                    XCTAssertTrue (cache[entity4.id]!.codable! === entity4)
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
            let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
            // entity1, entity2: Data in accessor
            let entity1 = newTestEntity(myInt: 10, myString: "A String 1")
    
            entity1.persistenceState = .persistent
            entity1.saved = Date()
            let data1 = try accessor.encoder.encode(entity1)
            switch accessor.add(name: standardCacheName, id: entity1.id, data: data1) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            let entity2 = newTestEntity(myInt: 20, myString: "A String 2")
    
            entity2.persistenceState = .persistent
            entity2.saved = Date()
            let data2 = try accessor.encoder.encode(entity2)
            switch accessor.add(name: standardCacheName, id: entity2.id, data: data2) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            // entity3, entity4: entity in cache, data in accessor
            var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
            entity3.persistenceState = .persistent
            entity3.saved = Date()
            let data3 = try accessor.encoder.encode(entity3)
            switch accessor.add(name: standardCacheName, id: entity3.id, data: data3) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity3 = cache.get (id: entity3.id).item()!
            var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
            entity4.persistenceState = .persistent
            entity4.saved = Date()
            let data4 = try accessor.encoder.encode(entity4)
            switch accessor.add(name: standardCacheName, id: entity4.id, data: data4) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            entity4 = cache.get (id: entity4.id).item()!
            // Invalid Data
            let json = "{}"
            let invalidData = json.data(using: .utf8)!
            let invalidDataUuid = UUID()
            switch accessor.add(name: standardCacheName, id: invalidDataUuid, data: invalidData) {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            cache.sync() { cache in
                XCTAssertEqual (2, cache.count)
                XCTAssertTrue (cache[entity3.id]!.codable! === entity3)
                XCTAssertTrue (cache[entity4.id]!.codable! === entity4)
            }
            // Retrieve Data
            accessor.setThrowError()
            switch cache.scan(criteria: nil) {
            case .error (let errorMessage):
                XCTAssertEqual ("scanError", errorMessage)
            default:
                XCTFail("Expected .error")
            }
            accessor.setThrowError()
            switch cache.scan() {
            case .error (let errorMessage):
                XCTAssertEqual ("scanError", errorMessage)
            default:
                XCTFail("Expected .error")
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
                var entry = entries[0].asTestString()
                XCTAssertEqual ("EMERGENCY|EntityCache<MyStruct>.scan|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;errorMessage=scanError", entry)
                entry = entries[1].asTestString()
                XCTAssertEqual ("EMERGENCY|EntityCache<MyStruct>.scan|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;errorMessage=scanError", entry)
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
                let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
                let dispatchGroup = DispatchGroup()
                // entity1, entity2: Data in accessor
                let id1 = UUID()
                let data1 = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":5,\"created\":1525735252.2328,\"saved\":1525735368.8345,\"item\":{\"myInt\":10,\"myString\":\"A String 1\"},\"persistenceState\":\"persistent\",\"version\":1}".data(using: .utf8)!
                switch accessor.add(name: standardCacheName, id: id1, data: data1) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                let id2 = UUID()
                let data2 = "{\"id\":\"\(id2.uuidString)\",\"schemaVersion\":5,\"created\":1525735262.2330,\"saved\":1525735311.3375,\"item\":{\"myInt\":20,\"myString\":\"A String 2\"},\"persistenceState\":\"persistent\",\"version\":2}".data(using: .utf8)!
                switch accessor.add(name: standardCacheName, id: id2, data: data2) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                // entity3, entity4: entity in cache, data in accessor
                var entity3 = newTestEntity(myInt: 30, myString: "A String 3")
                entity3.persistenceState = .persistent
                entity3.saved = Date()
                let data3 = try accessor.encoder.encode(entity3)
                switch accessor.add(name: standardCacheName, id: entity3.id, data: data3) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity3 = cache.get (id: entity3.id).item()!
                var entity4 = newTestEntity(myInt: 40, myString: "A String 4")
                entity4.persistenceState = .persistent
                entity4.saved = Date()
                let data4 = try accessor.encoder.encode(entity4)
                switch accessor.add(name: standardCacheName, id: entity4.id, data: data4) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                entity4 = cache.get (id: entity4.id).item()!
                // Invalid Data
                let json = "{}"
                let invalidData = json.data(using: .utf8)!
                let invalidDataUuid = UUID()
                switch accessor.add(name: standardCacheName, id: invalidDataUuid, data: invalidData) {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                let test1a = {
                    cache.scan(criteria: nil) { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        var retrievedEntity1: Entity<MyStruct>? = nil
                        var retrievedEntity2: Entity<MyStruct>? = nil
                        var retrievedEntity3: Entity<MyStruct>? = nil
                        var retrievedEntity4: Entity<MyStruct>? = nil
                        for retrievedEntity in retrievedEntities {
                            XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                            let persistenceState = retrievedEntity.persistenceState
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
                    cache.scan() { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        var retrievedEntity1: Entity<MyStruct>? = nil
                        var retrievedEntity2: Entity<MyStruct>? = nil
                        var retrievedEntity3: Entity<MyStruct>? = nil
                        var retrievedEntity4: Entity<MyStruct>? = nil
                        for retrievedEntity in retrievedEntities {
                            XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                            let persistenceState = retrievedEntity.persistenceState
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
                    cache.scan (criteria: { item in return (item.myInt == 10)}) { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        XCTAssertEqual (1, retrievedEntities.count)
                        let retrievedEntity = retrievedEntities[0]
                        XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                        switch retrievedEntity.persistenceState {
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
                    cache.scan( criteria: { item in return (item.myInt == 30) }) { retrievalResult in
                        let retrievedEntities = retrievalResult.item()!
                        XCTAssertEqual (1, retrievedEntities.count)
                        let retrievedEntity = retrievedEntities[0]
                        XCTAssertTrue (retrievedEntity.isInitialized(onCollection: cache))
                        switch retrievedEntity.persistenceState {
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
    
    public func testRegisterOnCache() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        var didFire1 = false
        var didFire2 = false
        let id = UUID()
        let waitFor1 = expectation(description: "wait1")
        let waitFor2 = expectation(description: "wait2")
        cache.registerOnEntityCached(id: id) { entity in
            didFire1 = true
            waitFor1.fulfill()
        }
        cache.registerOnEntityCached(id: id) { entity in
            didFire2 = true
            waitFor2.fulfill()
        }
        XCTAssertEqual (1, cache.onCacheCount())
        let entity =  Entity<MyStruct>(cache: cache, id: id, version: 10, item: MyStruct(myInt: 10, myString: "10"))
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertNotNil(entity)
        XCTAssertTrue (didFire1)
        XCTAssertTrue (didFire2)
        XCTAssertEqual (0, cache.onCacheCount())
    }
    
}
