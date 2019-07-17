//
//  persistenceTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/2/18.
//

import XCTest
@testable import danake
import PromiseKit

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
        cache?.userInfoClosure!(&userInfo)
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
    
    func testGetSync() throws {
        // Data In Cache=No; Data in Accessor=No
        let entity = newTestEntity(myInt: 10, myString: "A String")

        entity.saved = Date()
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        let data = try accessor.encoder.encode(entity)
        do {
            let _ = try cache.getSync (id: entity.id)
            XCTFail ("Expected Errr")
        } catch {
            XCTAssertEqual ("unknownUUID(\(entity.id))", "\(error)")
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            let entry = entries[0].asTestString()
            XCTAssertEqual ("WARNING|InMemoryAccessor.getSync|Unknown id|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;id=\(entity.id)", entry)
        }
        // Data In Cache=No; Data in Accessor=Yes
        let _ = accessor.add(name: standardCacheName, id: entity.id, data: data)
        let result = try cache.getSync(id: entity.id)
        let retrievedEntity = result
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
        var cachedEntity = try cache.getSync(id: entity2.id)
        XCTAssertTrue (entity2 === cachedEntity)
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
        cachedEntity = try cache.getSync(id: entity.id)
        XCTAssertTrue (retrievedEntity === cachedEntity)
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
        do {
            let _ = try cache.getSync(id: invalidDataUuid)
            XCTFail ("Expected error")
        } catch {
            XCTAssertEqual ("creation(\"keyNotFound(CodingKeys(stringValue: \\\"id\\\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \\\"No value associated with key CodingKeys(stringValue: \\\\\\\"id\\\\\\\", intValue: nil) (\\\\\\\"id\\\\\\\").\\\", underlyingError: nil))\")", "\(error)")
        }
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            let entry = entries[1].asTestString()
            XCTAssertEqual ("EMERGENCY|InMemoryAccessor.getSync|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;id=\(invalidDataUuid);errorMessage=creation(\"keyNotFound(CodingKeys(stringValue: \\\"id\\\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \\\"No value associated with key CodingKeys(stringValue: \\\\\\\"id\\\\\\\", intValue: nil) (\\\\\\\"id\\\\\\\").\\\", underlyingError: nil))\")", entry)
        }
        // Database Error
        let entity3 = newTestEntity(myInt: 30, myString: "A String 3")
        let data3 = try JSONEncoder().encode(entity)
        let _ = accessor.add(name: standardCacheName, id: entity3.id, data: data3)
        accessor.setThrowError()
        do {
            let _ = try cache.getSync(id: entity3.id)
            XCTFail("Expected error")
        } catch {
            XCTAssertEqual ("getError", "\(error)")
        }
        logger.sync() { entries in
            XCTAssertEqual (3, entries.count)
            let entry = entries[2].asTestString()
            XCTAssertEqual ("EMERGENCY|InMemoryAccessor.getSync|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;id=\(entity3.id);errorMessage=getError", entry)
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
            var result1a: Entity<MyStruct>? = nil
            var result1b: Entity<MyStruct>? = nil
            var result2: Entity<MyStruct>? = nil
            let waitFor1 = expectation(description: "Entity2")
            let workQueue = DispatchQueue (label: "WorkQueue", attributes: .concurrent)
            dispatchGroup.enter()
            workQueue.async {
                do {
                    result1a = try cache.getSync(id: id1)
                } catch {
                    XCTFail ("Expected success but got \(error)")
                }
                dispatchGroup.leave()
            }
            dispatchGroup.enter()
            workQueue.async {
                do {
                    result1b = try cache.getSync (id: id1)
                } catch {
                    XCTFail ("Expected success but got \(error)")
                }
                dispatchGroup.leave()
            }
            workQueue.async {
                do {
                    result2 = try cache.getSync (id: id2)
                } catch {
                    XCTFail ("Expected success but got \(error)")
                }
                waitFor1.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertNil (result1a)
            XCTAssertNil (result1b)
            if let retrievedEntity = result2 {
                XCTAssertEqual (id2.uuidString, retrievedEntity.id.uuidString)
                XCTAssertEqual (20, retrievedEntity.version)
                XCTAssertEqual (1525732227.0376, retrievedEntity.created.timeIntervalSince1970)
                XCTAssertEqual (1525732315.7534, retrievedEntity.saved!.timeIntervalSince1970)
                switch retrievedEntity.persistenceState {
                case .persistent:
                    break
                default:
                    XCTFail("Expected .persistent")
                }
                retrievedEntity.sync() { myStruct in
                    XCTAssertEqual (20, myStruct.myInt)
                    XCTAssertEqual ("A String2", myStruct.myString)
                }
            } else {
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
            if let retrievedEntity = result1a {
                retrievedEntity1a = retrievedEntity
            } else {
                XCTFail("Expected data1a")
            }
            if let retrievedEntity = result1b {
                retrievedEntity1b = retrievedEntity
            } else {
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
            var result1: Entity<MyStruct>? = nil
            var result2: Entity<MyStruct>? = nil
            firstly {
                cache.get (id: entity1.id)
            }.done { item in
                XCTFail ("Expected error")
            }.catch { error in
                XCTAssertEqual ("unknownUUID(\(entity1.id))", "\(error)")
            }.finally {
                waitFor1.fulfill()
            }
            firstly {
                cache.get (id: entity2.id)
            }.done { item in
                XCTFail ("Expected error")
            }.catch { error in
                XCTAssertEqual ("unknownUUID(\(entity2.id))", "\(error)")
            }.finally {
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertNil (result1)
            XCTAssertNil (result2)
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            // Data In Cache=No; Data in Accessor=Yes
            waitFor1 = expectation(description: "wait1.2")
            waitFor2 = expectation(description: "wait2.2")
            let _ = accessor.add(name: standardCacheName, id: entity1.id, data: data1)
            let _ = accessor.add(name: standardCacheName, id: entity2.id, data: data2)
            firstly {
                cache.get(id: entity1.id)
            }.done { item in
                result1 = item
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                waitFor1.fulfill()
            }
            
            firstly {
                cache.get(id: entity2.id)
            }.done { item in
                result2 = item
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            var retrievedEntity1 = result1!
            var retrievedEntity2 = result2!
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
            firstly {
                cache.get(id: entity3.id)
            }.done { item in
                result1 = item
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                waitFor1.fulfill()
            }
            
            firstly {
                cache.get(id: entity4.id)
            }.done { item in
                result2 = item
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            XCTAssertTrue (entity3 === result1!)
            XCTAssertTrue (entity4 === result2!)
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
            result1 = nil
            result2 = nil
            firstly {
                cache.get (id: entity1.id)
            }.done { item in
                result1 = item
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                waitFor1.fulfill()
            }
            
            firstly {
                cache.get (id: entity2.id)
            }.done { item in
                result2 = item
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            retrievedEntity1 = result1!
            retrievedEntity2 = result2!
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
            firstly {
                cache.get(id: invalidDataUuid1)
            }.done { item in
                XCTFail ("Expected error")
            }.catch { error in
                #if os(Linux)
                    XCTAssertEqual ("creation(\"dataCorrupted(Swift.DecodingError.Context(codingPath: [], debugDescription: \\\"The given data was not valid JSON.\\\", underlyingError: Optional(Error Domain=NSCocoaErrorDomain Code=3840 \\\"The data is not in the correct format.\\\")))")
                #else
                    XCTAssertEqual ("creation(\"dataCorrupted(Swift.DecodingError.Context(codingPath: [], debugDescription: \\\"The given data was not valid JSON.\\\", underlyingError: Optional(Error Domain=NSCocoaErrorDomain Code=3840 \\\"Unexpected end of file during JSON parse.\\\" UserInfo={NSDebugDescription=Unexpected end of file during JSON parse.})))\")", "\(error)")
                #endif
            }.finally {
                waitFor1.fulfill()
            }
            firstly {
                cache.get(id: invalidDataUuid2)
            }.done { item in
                XCTFail ("Expected error")
            }.catch { error in
                XCTAssertEqual ("creation(\"keyNotFound(CodingKeys(stringValue: \\\"id\\\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \\\"No value associated with key CodingKeys(stringValue: \\\\\\\"id\\\\\\\", intValue: nil) (\\\\\\\"id\\\\\\\").\\\", underlyingError: nil))\")", "\(error)")
            }.finally {
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
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
            let errorsReportedQueue = DispatchQueue(label: "errorsReported")
            firstly {
                cache.get(id: entity5.id)
            }.done { item in
                // Do Nothing
            }.catch {error in
                errorsReportedQueue.sync {
                    errorsReported = errorsReported + 1
                }
            }.finally {
                waitFor1.fulfill()
            }
            firstly {
                cache.get(id: entity6.id)
            }.done { item in
                // Do Nothing
            }.catch {error in
                errorsReportedQueue.sync {
                    errorsReported = errorsReported + 1
                }
            }.finally {
                waitFor2.fulfill()
            }
            waitForExpectations(timeout: 10, handler: nil)
            errorsReportedQueue.sync {
                XCTAssertEqual (1, errorsReported)
            }
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
            var retrievedEntities = try cache.scanSync(criteria: nil)
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = try cache.scanSync()
            XCTAssertEqual (0, retrievedEntities.count)
            retrievedEntities = try cache.scanSync() { myStruct in
                return (myStruct.myInt == 20)
            }
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
            entity3 = try cache.getSync (id: entity3.id)
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
            entity4 = try cache.getSync (id: entity4.id)
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
            retrievedEntities = try cache.scanSync(criteria: nil)
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
            retrievedEntities = try cache.scanSync()
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
            var retrievalResult = try cache.scanSync() { myStruct in
                return (myStruct.myInt == 20)
            }
            retrievedEntities = retrievalResult
            XCTAssertEqual (1, retrievedEntities.count)
            XCTAssertTrue (retrievedEntities[0] === retrievedEntity2!)
            // With criteria matching none
            retrievalResult = try cache.scanSync() { myStruct in
                return (myStruct.myInt == 1000)
            }
            retrievedEntities = retrievalResult
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
            entity3 = try cache.getSync (id: entity3.id)
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
            entity4 = try cache.getSync (id: entity4.id)
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
            let retrievedEntities = try cache.scanSync(){ item in
                return (item.myInt == 10)
            }
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
                entity3 = try cache.getSync (id: entity3.id)
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
                entity4 = try cache.getSync (id: entity4.id)
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
                let retrievedEntities = try cache.scanSync(){ item in
                    return (item.myInt == 30)
                }
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
                    for entry in entries {
                        print (entry.asTestString())
                    }
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
            entity3 = try cache.getSync (id: entity3.id)
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
            entity4 = try cache.getSync (id: entity4.id)
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
            do {
                let _ = try cache.scanSync(criteria: nil)
                XCTFail("Expected error")
            } catch {
                XCTAssertEqual ("scanError", "\(error)")
            }
            accessor.setThrowError()
            do {
                let _ = try cache.scanSync()
                XCTFail("Expected error")
            } catch {
                XCTAssertEqual ("scanError", "\(error)")
            }
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
                var entry = entries[0].asTestString()
                XCTAssertEqual ("EMERGENCY|InMemoryAccessor.scanSync|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;errorMessage=scanError", entry)
                entry = entries[1].asTestString()
                XCTAssertEqual ("EMERGENCY|InMemoryAccessor.scanSync|Database Error|databaseHashValue=\(database.accessor.hashValue);cache=myCollection;errorMessage=scanError", entry)
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
                entity3 = try cache.getSync (id: entity3.id)
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
                entity4 = try cache.getSync (id: entity4.id)
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
                    firstly {
                        cache.scan(criteria: nil)
                    }.done { retrievedEntities in
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

                    }.catch { error in
                        XCTFail ("Expected success but got \(error)")
                    }.finally {
                        dispatchGroup.leave()
                    }
                }
                let test1b = {
                    firstly {
                        cache.scan()
                    }.done { retrievedEntities in
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
                    }.catch { error in
                        XCTFail ("Expected success but got \(error)")
                    }.finally {
                        dispatchGroup.leave()
                    }
                }
                let test2 = {
                    firstly {
                        cache.scan (criteria: { item in return (item.myInt == 10)})
                    }.done { retrievedEntities in
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

                    }.catch { error in
                        XCTFail ("Expected success but got \(error)")
                    }.finally {
                        dispatchGroup.leave()
                    }
                }
                let test3 = {
                    firstly {
                        cache.scan() {
                            item in return (item.myInt == 30)
                        }
                    }.done { retrievedEntities in
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
                    }.catch { error in
                        XCTFail ("Expected success but got \(error)")
                    }.finally {
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
                    let _ = job()
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
    
    public func testHasCached() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        XCTAssertFalse (cache.hasCached(id: UUID()))
        let myStruct = MyStruct(myInt: 10, myString: "10")
        let batch = EventuallyConsistentBatch()
        let entity = cache.new(batch: batch, item: myStruct)
        XCTAssertTrue (cache.hasCached(id: entity.id))
    }
    
    public func testWaitWhileCached() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        XCTAssertFalse (cache.hasCached(id: UUID()))
        let myStruct = MyStruct(myInt: 10, myString: "10")
        let batch = EventuallyConsistentBatch()
        var entity: Entity<MyStruct>? = cache.new(batch: batch, item: myStruct)
        batch.commitSync()
        batch.syncEntities { entities in
            XCTAssertEqual (0, entities.count)
        }
        let id = entity!.id
        let timeout1 = 0.01
        let start = Date().timeIntervalSince1970
        cache.waitWhileCached(id: id, timeout: timeout1)
        XCTAssertTrue (Date().timeIntervalSince1970 >= start + timeout1)
        XCTAssertTrue (cache.hasCached(id: id))
        let queue = DispatchQueue(label: "test")
        queue.async {
            entity = nil
        }
        let timeout2 = 10.0
        cache.waitWhileCached(id: id, timeout: timeout2)
        XCTAssertNil (entity)
        XCTAssertFalse (cache.hasCached(id: id))
        XCTAssertTrue (Date().timeIntervalSince1970 < start + 1.0)
    }
    
}
