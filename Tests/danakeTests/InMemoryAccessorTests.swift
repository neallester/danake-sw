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
        XCTAssertFalse (accessor.has(name: entity.getCollection()!.name, id: entity.getId()))
        XCTAssertNil (accessor.getData(name: entity.getCollection()!.name, id: entity.getId()))
        // Add using internal add
        switch accessor.add(name: standardCollectionName, id: entity.getId(), data: data) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertTrue (accessor.has(name: entity.getCollection()!.name, id: entity.getId()))
        XCTAssertEqual (accessor.getData (name: entity.getCollection()!.name, id: entity.getId()), entity.asData(encoder: accessor.encoder))
        var retrievedEntity1a: Entity<MyStruct>? = nil
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .ok (let retrievedEntity):
            retrievedEntity1a = retrievedEntity
            XCTAssertTrue (entity !== retrievedEntity!)
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity!.getId().uuidString)
            XCTAssertEqual (entity.getVersion(), retrievedEntity!.getVersion())
            try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity!.getCreated()))
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
            try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
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
        entity.setSaved(Date())
        // Update
        let wrapper = EntityPersistenceWrapper (collectionName: entity.getCollection()!.name, item: entity)
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
        var retrievedEntity1b: Entity<MyStruct>? = nil
        switch accessor.get(type: Entity<MyStruct>.self, name: standardCollectionName, id: entity.getId()) {
        case .ok (let retrievedEntity):
            XCTAssertTrue (entity !== retrievedEntity)
            XCTAssertTrue (retrievedEntity !== retrievedEntity1a)
            retrievedEntity1b = retrievedEntity
            XCTAssertEqual (entity.getId().uuidString, retrievedEntity!.getId().uuidString)
            XCTAssertEqual (entity.getVersion(), retrievedEntity!.getVersion())
            try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity!.getCreated()))
            try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity!.getSaved()!))
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity!.getPersistenceState().rawValue)
            retrievedEntity!.sync() { retrievedStruct in
                XCTAssertEqual (20, retrievedStruct.myInt)
                XCTAssertEqual ("A String 2", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected data")
        }
        XCTAssertEqual (accessor.getData (name: entity.getCollection()!.name, id: entity.getId()), entity.asData(encoder: accessor.encoder))
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
            try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity!.getCreated()))
            try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity!.getSaved()!))
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
            try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
            try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
            XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
            retrievedEntity.sync() { retrievedStruct in
                XCTAssertEqual (20, retrievedStruct.myInt)
                XCTAssertEqual ("A String 2", retrievedStruct.myString)
            }
        default:
            XCTFail("Expected .ok")
        }
        // Second Entity added public add
        // Also test preFetch
        let entity2 = newTestEntity(myInt: 30, myString: "A String 3")
        entity2.setSaved (Date())
        // Need to set collection in entity2
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: standardCollectionName)
        entity2.setCollection(collection: collection)
        var prefetchUuid: String? = nil
        accessor.setPreFetch() { uuid in
            if uuid.uuidString == entity2.getId().uuidString {
                prefetchUuid = uuid.uuidString
            }
        }
        let wrapper2 = EntityPersistenceWrapper (collectionName: collection.name, item: entity2)
        switch accessor.addAction(wrapper: wrapper2) {
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
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (20, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 2", retrievedStruct.myString)
                    }
                } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                    foundEntity2 = true
                    XCTAssertEqual (entity2.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity2.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
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
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (20, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 2", retrievedStruct.myString)
                    }
                } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                    foundEntity2 = true
                    XCTAssertEqual (entity2.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity2.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
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
        // Public add with error
        let entity3 = newTestEntity(myInt: 30, myString: "A String 3")
        entity3.setSaved (Date())
        // Need to set collection in entity2
        entity3.setCollection(collection: collection)
        let wrapper3 = EntityPersistenceWrapper (collectionName: collection.name, item: entity3)
        switch accessor.addAction(wrapper: wrapper3) {
        case .ok (let closure):
            accessor.setThrowError()
            switch closure() {
            case .error (let errorMessage):
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
        default:
            XCTFail("Expected .ok")
        }
        foundEntity1 = false
        foundEntity2 = false
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
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (20, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 2", retrievedStruct.myString)
                    }
                } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                    foundEntity2 = true
                    XCTAssertEqual (entity2.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity2.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
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
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
                    XCTAssertEqual (entity.getPersistenceState().rawValue, retrievedEntity.getPersistenceState().rawValue)
                    retrievedEntity.sync() { retrievedStruct in
                        XCTAssertEqual (20, retrievedStruct.myInt)
                        XCTAssertEqual ("A String 2", retrievedStruct.myString)
                    }
                } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                    foundEntity2 = true
                    XCTAssertEqual (entity2.getId().uuidString, retrievedEntity.getId().uuidString)
                    XCTAssertEqual (entity2.getVersion(), retrievedEntity.getVersion())
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getCreated()), jsonEncodedDate (date: retrievedEntity.getCreated()))
                    try XCTAssertEqual (jsonEncodedDate (date: entity2.getSaved()!), jsonEncodedDate (date: retrievedEntity.getSaved()!))
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
        // Test get preFetch
        prefetchUuid = nil
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
        // Test Remove with error and prefetch
        foundEntity1 = false
        foundEntity2 = false
        prefetchUuid = nil
        accessor.setPreFetch() { uuid in
            if uuid.uuidString == entity2.getId().uuidString {
                prefetchUuid = uuid.uuidString
            }
        }
        switch accessor.removeAction(wrapper: wrapper2) {
        case .ok (let closure):
            accessor.setThrowError()
            switch closure() {
            case .error(let errorMessage):
                XCTAssertEqual ("Test Error", errorMessage)
                switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
                case .ok (let retrievedEntities):
                    XCTAssertEqual (2, retrievedEntities.count)
                    for retrievedEntity in retrievedEntities {
                        if (retrievedEntity.getId().uuidString == entity.getId().uuidString) {
                            foundEntity1 = true
                        } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                            foundEntity2 = true
                        }
                    }
                default:
                    XCTFail("Expected .ok")
                }
            default:
                XCTFail ("Expected .error")
            }
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertTrue (accessor.has(name: entity.getCollection()!.name, id: entity2.getId()))
        XCTAssertEqual (entity2.getId().uuidString, prefetchUuid)
        XCTAssertTrue (foundEntity1)
        XCTAssertTrue (foundEntity2)
        // Test Remove
        foundEntity1 = false
        foundEntity2 = false
        prefetchUuid = nil
        accessor.setPreFetch (nil)
        switch accessor.removeAction(wrapper: wrapper2) {
        case .ok (let closure):
            switch closure() {
            case .ok:
                switch accessor.scan(type: Entity<MyStruct>.self, name: standardCollectionName) {
                case .ok (let retrievedEntities):
                    XCTAssertEqual (1, retrievedEntities.count)
                    for retrievedEntity in retrievedEntities {
                        if (retrievedEntity.getId().uuidString == entity.getId().uuidString) {
                            foundEntity1 = true
                        } else if (retrievedEntity.getId().uuidString == entity2.getId().uuidString) {
                            foundEntity2 = true
                        }
                    }
                default:
                    XCTFail("Expected .ok")
                }
            default:
                XCTFail ("Expected .ok")
            }
        default:
            XCTFail ("Expected .ok")
        }
        XCTAssertFalse (accessor.has(name: entity.getCollection()!.name, id: entity2.getId()))
        XCTAssertNil (prefetchUuid)
        XCTAssertTrue (foundEntity1)
        XCTAssertFalse (foundEntity2)
        // Test throw error for addAction, updateAction, and removeAction
        accessor.setThrowError()
        switch accessor.addAction(wrapper: wrapper2) {
        case .error (let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        switch accessor.addAction(wrapper: wrapper2) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        accessor.setThrowError()
        switch accessor.updateAction(wrapper: wrapper2) {
        case .error (let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        switch accessor.updateAction(wrapper: wrapper2) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        accessor.setThrowError()
        switch accessor.removeAction(wrapper: wrapper2) {
        case .error (let errorMessage):
            XCTAssertEqual ("Test Error", errorMessage)
        default:
            XCTFail ("Expected .error")
        }
        switch accessor.removeAction(wrapper: wrapper2) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        
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
}
