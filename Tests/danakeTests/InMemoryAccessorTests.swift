//
//  InMemoryAccessorTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import XCTest
import PromiseKit
import JSONEquality
@testable import danake

class InMemoryAccessorTests: XCTestCase {

    func testInMemoryAccessor() throws {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        let uuid = UUID()
        do {
            let _ = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: uuid)
            XCTFail("Expected failure")
        } catch {
            XCTAssertEqual("unknownUUID(\(uuid.uuidString))", "\(error)")
        }
        do {
            let retrievedData = try accessor.scanSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTAssertEqual (0, retrievedData.count)

        } catch {
            XCTFail("Expected success but got \(error)")
        }
        // Add using internal add
        let id1 = UUID()
        let creationDateString1 = try jsonEncodedDate(date: Date())!
        let json1 = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        switch accessor.add(name: standardCacheName, id: id1, data: json1.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertTrue (accessor.has(name: standardCacheName, id: id1))
        XCTAssertEqual (String (data: accessor.getData (name: standardCacheName, id: id1)!, encoding: .utf8), json1)
        var retrievedEntity1: Entity<MyStruct>? = nil
        do {
            let retrievedEntity = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: id1)
            retrievedEntity1 = retrievedEntity
            XCTAssertTrue (retrievedEntity === cache.cachedEntity(id: id1)!)
            XCTAssertEqual (id1.uuidString, retrievedEntity.id.uuidString)
            XCTAssertEqual (5, retrievedEntity.getSchemaVersion()) // Schema version is taken from the cache, not the json
            XCTAssertEqual (10, retrievedEntity.version )
            switch retrievedEntity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            retrievedEntity.sync() { item in
                XCTAssertEqual (100, item.myInt)
                XCTAssertEqual("A \"Quoted\" String", item.myString)
            }
            try XCTAssertEqual (jsonEncodedDate (date: retrievedEntity.created)!, creationDateString1)
            XCTAssertNil (retrievedEntity.saved)

        } catch {
            XCTFail("Expected success but got \(error)")
        }
        // Not present
        do {
            let _ = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: uuid)
            XCTFail("Expected Failure")
        } catch {
            XCTAssertEqual("unknownUUID(\(uuid.uuidString))", "\(error)")
        }
        do {
            let retrievedEntities = try accessor.scanSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTAssertEqual (1, retrievedEntities.count)
            XCTAssertTrue (retrievedEntities[0] === retrievedEntity1)
            XCTAssertTrue (retrievedEntity1 === cache.cachedEntity(id: id1)!)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        // Update
        let batch = EventuallyConsistentBatch()
        retrievedEntity1!.update(batch: batch) { item in
            item.myInt = 11
            item.myString = "11"
        }
        retrievedEntity1!.saved = Date()
        let wrapper = EntityPersistenceWrapper (cacheName: retrievedEntity1!.cache.name, entity: retrievedEntity1!)
        let group = DispatchGroup()
        switch accessor.updateAction(queue: database.workQueue, wrapper: wrapper, timeout: .seconds(1000)) {
        case .ok (let updateClosure):
            group.enter()
            firstly {
                updateClosure()
            }.done { updateResult in
                switch updateResult {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
            }.catch { error in
                XCTFail ("Expected .ok but got \(error)")
            }.finally {
                group.leave()
            }
        default:
            XCTFail("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        try JSONEquality.JSONEquals (String (data: accessor.getData (name: cache.name, id: retrievedEntity1!.id)!, encoding: .utf8)!, String (data: retrievedEntity1!.asData(encoder: accessor.encoder)!, encoding: .utf8)!)
        
        let id2 = UUID()
        let creationDateString2 = try jsonEncodedDate(date: Date())!
        let savedDateString2 = try jsonEncodedDate(date: Date())!
        let json2 = "{\"id\":\"\(id2.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString2),\"saved\":\(savedDateString2),\"item\":{\"myInt\":20,\"myString\":\"20\"},\"persistenceState\":\"persistent\",\"version\":10}"
        switch accessor.add(name: standardCacheName, id: id2, data: json2.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertTrue (accessor.has(name: standardCacheName, id: id2))
        XCTAssertEqual (String (data: accessor.getData (name: standardCacheName, id: id2)!, encoding: .utf8), json2)
        var found1 = false
        var retrievedEntity2: Entity<MyStruct>? = nil
        do {
            let retrievedEntities = try accessor.scanSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTAssertEqual (2, retrievedEntities.count)
            for entity in retrievedEntities {
                if entity === retrievedEntity1 {
                    found1 = true
                    XCTAssertTrue (entity === retrievedEntity1)
                    XCTAssertTrue (retrievedEntity1 === cache.cachedEntity(id: id1)!)
                } else {
                    XCTAssertTrue (entity === cache.cachedEntity(id: id2)!)
                    retrievedEntity2 = entity
                    XCTAssertEqual (id2.uuidString, entity.id.uuidString)
                    XCTAssertEqual (5, entity.getSchemaVersion()) // Schema version is taken from the cache, not the json
                    XCTAssertEqual (10, entity.version )
                    switch entity.persistenceState {
                    case .persistent:
                        break
                    default:
                        XCTFail ("Expected .persistent")
                    }
                    entity.sync() { item in
                        XCTAssertEqual (20, item.myInt)
                        XCTAssertEqual("20", item.myString)
                    }
                    try XCTAssertEqual (jsonEncodedDate (date: entity.created)!, creationDateString2)
                    try XCTAssertEqual (jsonEncodedDate (date: entity.saved!)!, savedDateString2)
                    
                }
            }
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        XCTAssertTrue (found1)
        found1 = false
        var found2 = false
        do {
            let retrievedEntities = try accessor.scanSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTAssertEqual (2, retrievedEntities.count)
            for entity in retrievedEntities {
                if entity === retrievedEntity1 {
                    found1 = true
                    XCTAssertTrue (entity === retrievedEntity1)
                    XCTAssertTrue (retrievedEntity1 === cache.cachedEntity(id: id1)!)
                }
                if entity === retrievedEntity2 {
                    found2 = true
                    XCTAssertTrue (entity === retrievedEntity2)
                    XCTAssertTrue (retrievedEntity2 === cache.cachedEntity(id: id2)!)
                }
            }
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        XCTAssertTrue (found1)
        XCTAssertTrue (found2)
        // Test get and scan throwError
        accessor.setThrowError()
        do {
            let _ = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: retrievedEntity1!.id)
            XCTFail("Expected error")
        } catch {
            XCTAssertEqual ("getError", "\(error)")
        }
        do {
            let retrievedEntity = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: retrievedEntity1!.id)
            XCTAssertNotNil(retrievedEntity)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        accessor.setThrowError()
        do {
            let _ = try accessor.scanSync (type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTFail ("Expected error")
        } catch {
            XCTAssertEqual ("scanError", "\(error)")
        }
        do {
            let _ = try accessor.scanSync (type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        // Test get and scan throwError with setThrowOnlyRecoverableErrors (true)
        accessor.setThrowOnlyRecoverableErrors(true)
        accessor.setThrowError()
        do {
            let _ = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: retrievedEntity1!.id)
            XCTFail("Expected error")
        } catch {
            XCTAssertEqual ("getError", "\(error)")
        }
        do {
            let _ = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>, id: retrievedEntity1!.id)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        accessor.setThrowError()
        do {
            let _ = try accessor.scanSync (type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTFail ("Expected error")
        } catch {
            XCTAssertEqual ("scanError", "\(error)")
        }
        do {
            let _ = try accessor.scanSync (type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        accessor.setThrowOnlyRecoverableErrors(false)
        // Second Entity added public add
        // Also test preFetch
        let entity3 = cache.new(batch: batch, item: MyStruct (myInt: 30, myString: "A String 3"))
        entity3.saved = Date()
        var prefetchUuid: String? = nil
        accessor.setPreFetch() { uuid in
            if uuid.uuidString == entity3.id.uuidString {
                prefetchUuid = uuid.uuidString
            }
        }
        let wrapper3 = EntityPersistenceWrapper (cacheName: cache.name, entity: entity3)
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper3, timeout: .seconds(1000)) {
        case .ok (let closure):
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
            }.finally {
                group.leave()
            }
        default:
            XCTFail("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        XCTAssertEqual (3, accessor.count(name: cache.name))
        XCTAssertEqual (prefetchUuid!, entity3.id.uuidString)
        XCTAssertEqual (String (data: accessor.getData (name: standardCacheName, id: entity3.id)!, encoding: .utf8), String (data: entity3.asData(encoder: accessor.encoder)!, encoding: .utf8))
        XCTAssertTrue (entity3 === cache.cachedEntity(id: entity3.id))
        prefetchUuid = nil
        do {
            let retrievedEntity = try accessor.getSync(type: Entity<MyStruct>.self, cache: cache, id: entity3.id)
            XCTAssertTrue (retrievedEntity === entity3)
            XCTAssertTrue (entity3 === cache.cachedEntity(id: entity3.id))
            XCTAssertEqual (prefetchUuid!, entity3.id.uuidString)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        
        found1 = false
        found2 = false
        var found3 = false
        do {
            let retrievedEntities = try accessor.scanSync(type: Entity<MyStruct>.self, cache: cache as EntityCache<MyStruct>)
            XCTAssertEqual (3, retrievedEntities.count)
            for entity in retrievedEntities {
                if entity === retrievedEntity1 {
                    found1 = true
                    XCTAssertTrue (entity === retrievedEntity1)
                    XCTAssertTrue (retrievedEntity1 === cache.cachedEntity(id: id1)!)
                }
                if entity === retrievedEntity2 {
                    found2 = true
                    XCTAssertTrue (entity === retrievedEntity2)
                    XCTAssertTrue (retrievedEntity2 === cache.cachedEntity(id: id2)!)
                }
                if entity === entity3 {
                    found3 = true
                    XCTAssertTrue (entity === entity3)
                    XCTAssertTrue (entity3 === cache.cachedEntity(id: entity3.id)!)
                }
            }
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        XCTAssertTrue (found1)
        XCTAssertTrue (found2)
        XCTAssertTrue (found3)
        // Public add with errors
        var entity4 = cache.new(batch: batch, item: MyStruct (myInt: 40, myString: "A String 4"))
        entity4.saved = Date()
        accessor.setThrowError()
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper3, timeout: .seconds(1000)) {
        case .error (let errorMessage):
            XCTAssertEqual ("addActionError", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        XCTAssertEqual (3, accessor.count(name: cache.name))
        var wrapper4 = EntityPersistenceWrapper (cacheName: cache.name, entity: entity4)
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds(1000)) {
        case .ok (let closure):
            XCTAssertEqual (3, accessor.count(name: cache.name))
            accessor.setThrowError()
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .error (let errorMessage):
                    XCTAssertEqual (3, accessor.count(name: cache.name))
                    XCTAssertEqual ("addError", errorMessage)
                default:
                    XCTFail ("Expected .error")
                }
            }.catch {error in
                XCTFail ("Expected success")
            }.finally {
                group.leave()
            }
        default:
            XCTFail("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds(1000)) {
        case .ok (let closure):
            XCTAssertEqual (3, accessor.count(name: cache.name))
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .ok:
                    XCTAssertEqual (4, accessor.count (name: cache.name))
                    XCTAssertTrue (accessor.has(name: cache.name, id: entity4.id))
                    
                default:
                    XCTFail ("Expected .ok")
                }
            }.catch {error in
                XCTFail ("Expected success")
            }.finally {
                group.leave()
            }
        default:
            XCTFail("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        accessor.setThrowError()
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds(1000)) {
        case .error (let error):
            XCTAssertEqual ("addActionError", error)
            XCTAssertEqual (4, accessor.count(name: cache.name))
        default:
            XCTFail ("Expected .error")
        }

        // Public addAction with errors with setThrowOnlyRecoverableErrors(true)
        accessor.setThrowOnlyRecoverableErrors(true)
        entity4 = cache.new(batch: batch, item: MyStruct (myInt: 40, myString: "A String 4"))
        entity4.saved = Date()
        accessor.setThrowError()
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper3, timeout: .seconds(1000)) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertEqual (4, accessor.count(name: cache.name))
        wrapper4 = EntityPersistenceWrapper (cacheName: cache.name, entity: entity4)
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds (1000)) {
        case .ok (let closure):
            group.enter()
            XCTAssertEqual (4, accessor.count(name: cache.name))
            accessor.setThrowError()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .error (let errorMessage):
                    XCTAssertEqual ("addError", errorMessage)
                default:
                    XCTFail ("Expected .error")
                }
            }.catch { error in
                XCTFail ("Expected success")
            }.finally {
                group.leave()
            }
        default:
            XCTFail ("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        switch accessor.addAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds (1000)) {
        case .ok (let closure):
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .ok:
                    break
                default:
                    XCTFail ("Expected .ok")
                }
                XCTAssertEqual (5, accessor.count (name: cache.name))
                XCTAssertTrue (accessor.has(name: cache.name, id: entity4.id))
            }.catch { error in
                XCTFail ("Expected success")
            }.finally {
                group.leave()
            }
        default:
            XCTFail("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        accessor.setThrowOnlyRecoverableErrors(false)


        // Test Remove with error and prefetch
        prefetchUuid = nil
        accessor.setPreFetch() { uuid in
            if uuid.uuidString == entity4.id.uuidString {
                prefetchUuid = uuid.uuidString
            }
        }
        accessor.setThrowError()
        switch accessor.removeAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds(1000)) {
        case .error (let errorMessage):
            XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
            prefetchUuid = nil
            XCTAssertEqual ("removeActionError", errorMessage)
            XCTAssertEqual (5, accessor.count(name: cache.name))
            XCTAssertTrue (accessor.has(name: cache.name, id: entity4.id))
        default:
            XCTFail ("Expected .error")
        }
        switch accessor.removeAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds(1000)) {
        case .ok (let closure):
            XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
            prefetchUuid = nil
            accessor.setThrowError()
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .error(let errorMessage):
                    XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
                    XCTAssertTrue (accessor.has(name: cache.name, id: entity4.id))
                    prefetchUuid = nil
                    XCTAssertEqual ("removeError", errorMessage)
                    XCTAssertEqual(5, accessor.count(name: cache.name))
                default:
                    XCTFail ("Expected .error")
                }
            }.catch { error in
                XCTFail ("Expected Success")
            }.finally {
                group.leave()
            }
        default:
            XCTFail ("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        switch accessor.removeAction(queue: database.workQueue, wrapper: wrapper4, timeout: .seconds(1000)) {
        case .ok (let closure):
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .ok:
                    XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
                    prefetchUuid = nil
                    XCTAssertEqual(4, accessor.count(name: cache.name))
                    XCTAssertFalse (accessor.has (name: cache.name, id: entity4.id))
                    XCTAssertTrue (accessor.has (name: cache.name, id: retrievedEntity1!.id))
                    XCTAssertTrue (accessor.has (name: cache.name, id: retrievedEntity2!.id))
                    XCTAssertTrue (accessor.has (name: cache.name, id: entity3.id))
                default:
                    XCTFail ("Expected .ok")
                }
            }.catch { error in
                XCTFail ("Expected Success")
            }.finally {
                group.leave()
            }
            let _ = group.wait(timeout: DispatchTime.now() + 10)
        default:
            XCTFail ("Expected .ok")
        }


        // Test Remove with removeActionError and removeError when accessor.setThrowOnlyRecoverableErrors(true)
        accessor.setThrowOnlyRecoverableErrors(true)
        accessor.setThrowError()
        switch accessor.removeAction(queue: database.workQueue, wrapper: wrapper3, timeout: .seconds(1000)) {
        case .ok:
            XCTAssertEqual (4, accessor.count(name: cache.name))
            XCTAssertTrue (accessor.has(name: cache.name, id: wrapper3.id))
        default:
            XCTFail ("Expected .ok")
        }
        switch accessor.removeAction(queue: database.workQueue, wrapper: wrapper3, timeout: .seconds(1000)) {
        case .ok (let closure):
            accessor.setThrowError()
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .error(let errorMessage):
                    XCTAssertEqual ("removeError", errorMessage)
                    XCTAssertEqual(4, accessor.count(name: cache.name))
                default:
                    XCTFail ("Expected .error")
                }
            }.catch { error in
                XCTFail ("Expected success")
            }.finally {
                group.leave()
            }
        default:
            XCTFail ("Expected .ok")
        }
        let _ = group.wait(timeout: DispatchTime.now() + 10)
        switch accessor.removeAction(queue: database.workQueue, wrapper: wrapper3, timeout: .seconds(1000)) {
        case .ok (let closure):
            group.enter()
            firstly {
                closure()
            }.done { result in
                switch result {
                case .ok:
                    XCTAssertEqual(3, accessor.count(name: cache.name))
                    XCTAssertFalse (accessor.has (name: cache.name, id: entity3.id))
                    XCTAssertTrue (accessor.has (name: cache.name, id: retrievedEntity1!.id))
                    XCTAssertTrue (accessor.has (name: cache.name, id: retrievedEntity2!.id))
                default:
                    XCTFail ("Expected .ok")
                }
            }.catch { error in
                XCTFail ("Expected success")
            }.finally {
                group.leave()
            }
            let _ = group.wait(timeout: DispatchTime.now() + 10)
       default:
            XCTFail ("Expected .ok")
        }
        accessor.setThrowOnlyRecoverableErrors(false)
    }
    
    func testDecoder() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct>(database: database, name: standardCacheName)
        let decoder = accessor.decoder(cache: cache)
        XCTAssertTrue (decoder.userInfo[Database.cacheKey] as! EntityCache<MyStruct> === cache)
        switch decoder.dateDecodingStrategy {
        case .secondsSince1970:
            break
        default:
            XCTFail("Expected .secondsSince1970")
        }
    }
    
    func testEncoder() {
        let accessor = InMemoryAccessor()
        switch accessor.encoder.dateEncodingStrategy {
        case .secondsSince1970:
            break
        default:
            XCTFail("Expected .secondsSince1970")
        }
    }
    
    func testCount() {
        let accessor = InMemoryAccessor()
        XCTAssertEqual (0, accessor.count (name: standardCacheName))
        let data = Data (base64Encoded: "")!
        let id = UUID()
        let _ = accessor.add(name: standardCacheName, id: id, data: data)
        XCTAssertEqual (1, accessor.count (name: standardCacheName))
    }

    func testSetThrowError() {
        let accessor = InMemoryAccessor()
        XCTAssertFalse (accessor.isThrowError())
        accessor.setThrowError()
        XCTAssertTrue (accessor.isThrowError())
        accessor.setThrowError (false)
        XCTAssertFalse (accessor.isThrowError())
        accessor.setThrowError (true)
        XCTAssertTrue (accessor.isThrowError())
    }

    fileprivate class MyStructContainer : Codable {
        
        public required init (from decoder: Decoder) throws {
            if let myStruct = decoder.userInfo[MyStructContainer.structKey] as? MyStruct {
                self.myStruct = myStruct
            } else {
                throw EntityDeserializationError<MyStruct>.missingUserInfoValue(MyStructContainer.structKey)
            }
        }

        let myStruct: MyStruct
        
        static let structKey = CodingUserInfoKey (rawValue: "struct")!
    }
    
    func testGetWithDeserializationClosure() throws {
        let creationDateString = try jsonEncodedDate(date: Date())!
        let savedDateString = try jsonEncodedDate(date: Date())!
        let id = UUID()
        let json = "{\"id\":\"\(id.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{},\"persistenceState\":\"persistent\",\"version\":10}"
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        var cache = EntityCache<MyStructContainer>(database: database, name: standardCacheName)
        let _ = accessor.add(name: cache.name, id: id, data: json.data(using: .utf8)!)
        do {
            let _ = try accessor.getSync(type: Entity<MyStructContainer>.self, cache: cache as EntityCache<MyStructContainer>, id: id)
            XCTFail ("Expected error")
        } catch {
            XCTAssertEqual ("creation(\"missingUserInfoValue(Swift.CodingUserInfoKey(rawValue: \\\"struct\\\"))\")", "\(error)")
        }
        let myStruct = MyStruct (myInt: 10, myString: "10")
        cache = EntityCache<MyStructContainer>(database: database, name: standardCacheName) { userInfo in
            userInfo[MyStructContainer.structKey] = myStruct
        }
        do {
            let retrievedEntity = try accessor.getSync(type: Entity<MyStructContainer>.self, cache: cache as EntityCache<MyStructContainer>, id: id)
            retrievedEntity.sync() { item in
                XCTAssertEqual (10, item.myStruct.myInt)
                XCTAssertEqual ("10", item.myStruct.myString)
            }

        } catch {
            XCTFail("Expected success but got \(error)")
        }
    }
    
    func testScanWithDeserializationClosure() throws {
        let creationDateString = try jsonEncodedDate(date: Date())!
        let savedDateString = try jsonEncodedDate(date: Date())!
        let id = UUID()
        let json = "{\"id\":\"\(id.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{},\"persistenceState\":\"persistent\",\"version\":10}"
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        var cache = EntityCache<MyStructContainer>(database: database, name: standardCacheName)
        let _ = accessor.add(name: cache.name, id: id, data: json.data(using: .utf8)!)
        do {
            let result = try accessor.scanSync(type: Entity<MyStructContainer>.self, cache: cache as EntityCache<MyStructContainer>)
            XCTAssertEqual (0, result.count)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
        let myStruct = MyStruct (myInt: 10, myString: "10")
        cache = EntityCache<MyStructContainer>(database: database, name: standardCacheName) { userInfo in
            userInfo[MyStructContainer.structKey] = myStruct
        }
        do {
            let result = try accessor.scanSync(type: Entity<MyStructContainer>.self, cache: cache as EntityCache<MyStructContainer>)
            XCTAssertEqual (1, result.count)
            result[0].sync() { item in
                XCTAssertEqual (10, item.myStruct.myInt)
                XCTAssertEqual ("10", item.myStruct.myString)
            }
        } catch {
            XCTFail("Expected success but got \(error)")
        }
    }

}
