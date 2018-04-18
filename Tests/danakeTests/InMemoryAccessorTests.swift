//
//  InMemoryAccessorTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import XCTest
@testable import danake

class InMemoryAccessorTests: XCTestCase {

    func testInMemoryAccessor() throws {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        let uuid = UUID()
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: uuid) {
        case .ok (let retrievedData):
            XCTAssertNil (retrievedData)
        default:
            XCTFail("Expected data")
        }
        switch accessor.scan(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok (let retrievedData):
            XCTAssertEqual (0, retrievedData.count)
        default:
            XCTFail("Expected data")
        }
        // Add using internal add
        let id1 = UUID()
        let creationDateString1 = try jsonEncodedDate(date: Date())!
        let json1 = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"new\",\"version\":10}"
        switch accessor.add(name: standardCollectionName, id: id1, data: json1.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertTrue (accessor.has(name: standardCollectionName, id: id1))
        XCTAssertEqual (String (data: accessor.getData (name: standardCollectionName, id: id1)!, encoding: .utf8), json1)
        var retrievedEntity1: Entity<MyStruct>? = nil
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: id1) {
        case .ok (let retrievedEntity):
            if let retrievedEntity = retrievedEntity {
                retrievedEntity1 = retrievedEntity
                XCTAssertTrue (retrievedEntity === collection.cachedEntity(id: id1)!)
                XCTAssertEqual (id1.uuidString, retrievedEntity.id.uuidString)
                XCTAssertEqual (5, retrievedEntity.getSchemaVersion()) // Schema version is taken from the collection, not the json
                XCTAssertEqual (10, retrievedEntity.getVersion() )
                switch retrievedEntity.getPersistenceState() {
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
                XCTAssertNil (retrievedEntity.getSaved())
            } else {
                XCTFail ("Expected retrievedEntity")
            }
        default:
            XCTFail("Expected .ok")
        }
        // Not present
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: uuid) {
        case .ok (let retrievedEntity):
            XCTAssertNil (retrievedEntity)
        default:
            XCTFail("Expected .ok")
        }
        switch accessor.scan(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (1, retrievedEntities.count)
            XCTAssertTrue (retrievedEntities[0] === retrievedEntity1)
            XCTAssertTrue (retrievedEntity1 === collection.cachedEntity(id: id1)!)
        default:
            XCTFail("Expected .ok")
        }
        // Update
        let batch = EventuallyConsistentBatch()
        retrievedEntity1!.update(batch: batch) { item in
            item.myInt = 11
            item.myString = "11"
        }
        retrievedEntity1!.setSaved(Date())
        let wrapper = EntityPersistenceWrapper (collectionName: retrievedEntity1!.collection.name, item: retrievedEntity1!)
        switch accessor.updateAction(wrapper: wrapper) {
        case .ok (let updateClosure):
            switch updateClosure() {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertEqual (String (data: accessor.getData (name: collection.name, id: retrievedEntity1!.id)!, encoding: .utf8), String (data: retrievedEntity1!.asData(encoder: accessor.encoder)!, encoding: .utf8))
        let id2 = UUID()
        let creationDateString2 = try jsonEncodedDate(date: Date())!
        let savedDateString2 = try jsonEncodedDate(date: Date())!
        let json2 = "{\"id\":\"\(id2.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString2),\"saved\":\(savedDateString2),\"item\":{\"myInt\":20,\"myString\":\"20\"},\"persistenceState\":\"persistent\",\"version\":10}"
        switch accessor.add(name: standardCollectionName, id: id2, data: json2.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertTrue (accessor.has(name: standardCollectionName, id: id2))
        XCTAssertEqual (String (data: accessor.getData (name: standardCollectionName, id: id2)!, encoding: .utf8), json2)
        var found1 = false
        var retrievedEntity2: Entity<MyStruct>? = nil
        switch accessor.scan(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (2, retrievedEntities.count)
            for entity in retrievedEntities {
                if entity === retrievedEntity1 {
                    found1 = true
                    XCTAssertTrue (entity === retrievedEntity1)
                    XCTAssertTrue (retrievedEntity1 === collection.cachedEntity(id: id1)!)
                } else {
                    XCTAssertTrue (entity === collection.cachedEntity(id: id2)!)
                    retrievedEntity2 = entity
                    XCTAssertEqual (id2.uuidString, entity.id.uuidString)
                    XCTAssertEqual (5, entity.getSchemaVersion()) // Schema version is taken from the collection, not the json
                    XCTAssertEqual (10, entity.getVersion() )
                    switch entity.getPersistenceState() {
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
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!)!, savedDateString2)

                }
            }
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertTrue (found1)
        found1 = false
        var found2 = false
        switch accessor.scan(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (2, retrievedEntities.count)
            for entity in retrievedEntities {
                if entity === retrievedEntity1 {
                    found1 = true
                    XCTAssertTrue (entity === retrievedEntity1)
                    XCTAssertTrue (retrievedEntity1 === collection.cachedEntity(id: id1)!)
                }
                if entity === retrievedEntity2 {
                    found2 = true
                    XCTAssertTrue (entity === retrievedEntity2)
                    XCTAssertTrue (retrievedEntity2 === collection.cachedEntity(id: id2)!)
                }
            }
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertTrue (found1)
        XCTAssertTrue (found2)
        // Test get and scan throwError
        accessor.setThrowError()
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: retrievedEntity1!.id) {
        case .error (let errorMessage):
            XCTAssertEqual ("getError", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: retrievedEntity1!.id) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }

        accessor.setThrowError()
        switch accessor.scan (type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .error(let errorMessage):
            XCTAssertEqual ("scanError", errorMessage)
        default:
            XCTFail ("Expected .error")

        }
        switch accessor.scan (type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        // Test get and scan throwError with setThrowOnlyRecoverableErrors (true)
        accessor.setThrowOnlyRecoverableErrors(true)
        accessor.setThrowError()
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: retrievedEntity1!.id) {
        case .error (let errorMessage):
            XCTAssertEqual ("getError", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>, id: retrievedEntity1!.id) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        
        accessor.setThrowError()
        switch accessor.scan (type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .error(let errorMessage):
            XCTAssertEqual ("scanError", errorMessage)
        default:
            XCTFail ("Expected .error")
            
        }
        switch accessor.scan (type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        accessor.setThrowOnlyRecoverableErrors(false)
        // Second Entity added public add
        // Also test preFetch
        let entity3 = collection.new(batch: batch, item: MyStruct (myInt: 30, myString: "A String 3"))
        entity3.setSaved (Date())
        var prefetchUuid: String? = nil
        accessor.setPreFetch() { uuid in
            if uuid.uuidString == entity3.id.uuidString {
                prefetchUuid = uuid.uuidString
            }
        }
        let wrapper3 = EntityPersistenceWrapper (collectionName: collection.name, item: entity3)
        switch accessor.addAction(wrapper: wrapper3) {
        case .ok (let closure):
            switch closure() {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertEqual (3, accessor.count(name: collection.name))
        XCTAssertEqual (prefetchUuid!, entity3.id.uuidString)
        XCTAssertEqual (String (data: accessor.getData (name: standardCollectionName, id: entity3.id)!, encoding: .utf8), String (data: entity3.asData(encoder: accessor.encoder)!, encoding: .utf8))
        XCTAssertTrue (entity3 === collection.cachedEntity(id: entity3.id))
        prefetchUuid = nil
        switch accessor.get(type: Entity<MyStruct>.self, collection: collection, id: entity3.id) {
        case .ok (let retrievedEntity):
            XCTAssertTrue (retrievedEntity === entity3)
            XCTAssertTrue (entity3 === collection.cachedEntity(id: entity3.id))
            XCTAssertEqual (prefetchUuid!, entity3.id.uuidString)
        default:
            XCTFail ("Expected .ok")
        }
        
        found1 = false
        found2 = false
        var found3 = false
        switch accessor.scan(type: Entity<MyStruct>.self, collection: collection as PersistentCollection<Database, MyStruct>) {
        case .ok (let retrievedEntities):
            XCTAssertEqual (3, retrievedEntities.count)
            for entity in retrievedEntities {
                if entity === retrievedEntity1 {
                    found1 = true
                    XCTAssertTrue (entity === retrievedEntity1)
                    XCTAssertTrue (retrievedEntity1 === collection.cachedEntity(id: id1)!)
                }
                if entity === retrievedEntity2 {
                    found2 = true
                    XCTAssertTrue (entity === retrievedEntity2)
                    XCTAssertTrue (retrievedEntity2 === collection.cachedEntity(id: id2)!)
                }
                if entity === entity3 {
                    found3 = true
                    XCTAssertTrue (entity === entity3)
                    XCTAssertTrue (entity3 === collection.cachedEntity(id: entity3.id)!)
                }
            }
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertTrue (found1)
        XCTAssertTrue (found2)
        XCTAssertTrue (found3)
        // Public add with errors
        var entity4 = collection.new(batch: batch, item: MyStruct (myInt: 40, myString: "A String 4"))
        entity4.setSaved (Date())
        accessor.setThrowError()
        switch accessor.addAction(wrapper: wrapper3) {
        case .error (let errorMessage):
            XCTAssertEqual ("addActionError", errorMessage)
        default:
            XCTFail("Expected .error")
        }
        XCTAssertEqual (3, accessor.count(name: collection.name))
        var wrapper4 = EntityPersistenceWrapper (collectionName: collection.name, item: entity4)
        switch accessor.addAction(wrapper: wrapper4) {
        case .ok (let closure):
            XCTAssertEqual (3, accessor.count(name: collection.name))
            accessor.setThrowError()
            switch closure() {
            case .error (let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch closure() {
            case .ok:
                XCTAssertEqual (4, accessor.count (name: collection.name))
                XCTAssertTrue (accessor.has(name: collection.name, id: entity4.id))
                
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail("Expected .ok")
        }


        // Public addAction with errors with setThrowOnlyRecoverableErrors(true)
        accessor.setThrowOnlyRecoverableErrors(true)
        entity4 = collection.new(batch: batch, item: MyStruct (myInt: 40, myString: "A String 4"))
        entity4.setSaved (Date())
        accessor.setThrowError()
        switch accessor.addAction(wrapper: wrapper3) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        XCTAssertEqual (4, accessor.count(name: collection.name))
        wrapper4 = EntityPersistenceWrapper (collectionName: collection.name, item: entity4)
        switch accessor.addAction(wrapper: wrapper4) {
        case .ok (let closure):
            XCTAssertEqual (4, accessor.count(name: collection.name))
            accessor.setThrowError()
            switch closure() {
            case .error (let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch closure() {
            case .ok:
                XCTAssertEqual (5, accessor.count (name: collection.name))
                XCTAssertTrue (accessor.has(name: collection.name, id: entity4.id))
                
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail("Expected .ok")
        }
        accessor.setThrowOnlyRecoverableErrors(false)


        // Test Remove with error and prefetch
        prefetchUuid = nil
        accessor.setPreFetch() { uuid in
            if uuid.uuidString == entity4.id.uuidString {
                prefetchUuid = uuid.uuidString
            }
        }
        accessor.setThrowError()
        switch accessor.removeAction(wrapper: wrapper4) {
        case .error (let errorMessage):
            XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
            prefetchUuid = nil
            XCTAssertEqual ("removeActionError", errorMessage)
            XCTAssertEqual (5, accessor.count(name: collection.name))
            XCTAssertTrue (accessor.has(name: collection.name, id: entity4.id))
        default:
            XCTFail ("Expected .error")
        }
        switch accessor.removeAction(wrapper: wrapper4) {
        case .ok (let closure):
            XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
            prefetchUuid = nil
            accessor.setThrowError()
            switch closure() {
            case .error(let errorMessage):
                XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
                prefetchUuid = nil
                XCTAssertEqual ("removeError", errorMessage)
                XCTAssertEqual(5, accessor.count(name: collection.name))
            default:
                XCTFail ("Expected .error")
            }
            switch closure() {
            case .ok:
                XCTAssertEqual (prefetchUuid!, entity4.id.uuidString)
                prefetchUuid = nil
                XCTAssertEqual(4, accessor.count(name: collection.name))
                XCTAssertFalse (accessor.has (name: collection.name, id: entity4.id))
                XCTAssertTrue (accessor.has (name: collection.name, id: retrievedEntity1!.id))
                XCTAssertTrue (accessor.has (name: collection.name, id: retrievedEntity2!.id))
                XCTAssertTrue (accessor.has (name: collection.name, id: entity3.id))
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail ("Expected .ok")
        }


        // Test Remove with removeActionError and removeError when accessor.setThrowOnlyRecoverableErrors(true)
        accessor.setThrowOnlyRecoverableErrors(true)
        accessor.setThrowError()
        switch accessor.removeAction(wrapper: wrapper3) {
        case .ok:
            XCTAssertEqual (4, accessor.count(name: collection.name))
            XCTAssertTrue (accessor.has(name: collection.name, id: wrapper3.id))
        default:
            XCTFail ("Expected .ok")
        }
        switch accessor.removeAction(wrapper: wrapper3) {
        case .ok (let closure):
            accessor.setThrowError()
            switch closure() {
            case .error(let errorMessage):
                XCTAssertEqual ("removeError", errorMessage)
                XCTAssertEqual(4, accessor.count(name: collection.name))
            default:
                XCTFail ("Expected .error")
            }
            switch closure() {
            case .ok:
                XCTAssertEqual(3, accessor.count(name: collection.name))
                XCTAssertFalse (accessor.has (name: collection.name, id: entity3.id))
                XCTAssertTrue (accessor.has (name: collection.name, id: retrievedEntity1!.id))
                XCTAssertTrue (accessor.has (name: collection.name, id: retrievedEntity2!.id))
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail ("Expected .ok")
        }
        accessor.setThrowOnlyRecoverableErrors(false)
    }
    
    func testDecoder() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        let decoder = accessor.decoder(collection: collection)
        XCTAssertTrue (decoder.userInfo[Database.collectionKey] as! PersistentCollection<Database, MyStruct> === collection)
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
        XCTAssertEqual (0, accessor.count (name: standardCollectionName))
        let data = Data (base64Encoded: "")!
        let id = UUID()
        let _ = accessor.add(name: standardCollectionName, id: id, data: data)
        XCTAssertEqual (1, accessor.count (name: standardCollectionName))
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
        var collection = PersistentCollection<Database, MyStructContainer>(database: database, name: standardCollectionName)
        let _ = accessor.add(name: collection.name, id: id, data: json.data(using: .utf8)!)
        switch accessor.get(type: Entity<MyStructContainer>.self, collection: collection as PersistentCollection<Database, MyStructContainer>, id: id) {
        case .error(let errorMessage):
            XCTAssertEqual ("missingUserInfoValue(Swift.CodingUserInfoKey(rawValue: \"struct\"))", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        let myStruct = MyStruct (myInt: 10, myString: "10")
        collection = PersistentCollection<Database, MyStructContainer>(database: database, name: standardCollectionName) { userInfo in
            userInfo[MyStructContainer.structKey] = myStruct
        }
        switch accessor.get(type: Entity<MyStructContainer>.self, collection: collection as PersistentCollection<Database, MyStructContainer>, id: id) {
        case .ok(let retrievedEntity):
            retrievedEntity?.sync() { item in
                XCTAssertEqual (10, item.myStruct.myInt)
                XCTAssertEqual ("10", item.myStruct.myString)
            }
        default:
            XCTFail ("Expected .ok")
        }

    }
    func testScanWithDeserializationClosure() throws {
        let creationDateString = try jsonEncodedDate(date: Date())!
        let savedDateString = try jsonEncodedDate(date: Date())!
        let id = UUID()
        let json = "{\"id\":\"\(id.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{},\"persistenceState\":\"persistent\",\"version\":10}"
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        var collection = PersistentCollection<Database, MyStructContainer>(database: database, name: standardCollectionName)
        let _ = accessor.add(name: collection.name, id: id, data: json.data(using: .utf8)!)
        switch accessor.scan(type: Entity<MyStructContainer>.self, collection: collection as PersistentCollection<Database, MyStructContainer>) {
        case .ok(let result):
            XCTAssertEqual (0, result.count)
        default:
            XCTFail ("Expected .ok")
        }
        let myStruct = MyStruct (myInt: 10, myString: "10")
        collection = PersistentCollection<Database, MyStructContainer>(database: database, name: standardCollectionName) { userInfo in
            userInfo[MyStructContainer.structKey] = myStruct
        }
        switch accessor.scan(type: Entity<MyStructContainer>.self, collection: collection as PersistentCollection<Database, MyStructContainer>) {
        case .ok(let result):
            XCTAssertEqual (1, result.count)
            result[0].sync() { item in
                XCTAssertEqual (10, item.myStruct.myInt)
                XCTAssertEqual ("10", item.myStruct.myString)
            }
        default:
            XCTFail ("Expected .ok")
        }
        
    }

}
