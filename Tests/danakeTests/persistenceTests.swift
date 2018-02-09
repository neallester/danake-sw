//
//  persistenceTests.swift
//  danakeTests
//
//  Created by Neal Lester on 2/2/18.
//

import XCTest
@testable import danake

class persistenceTests: XCTestCase {

    func testPersistenceCollectionNew() {
        let myStruct = MyStruct(myInt: 10, myString: "A String")
        let collection = PersistentCollection<MyStruct>(accessor: InMemoryAccessor())
        var entity: Entity<MyStruct>? = collection.new(item: myStruct)
        XCTAssertTrue (collection === entity!.collection!)
        XCTAssertFalse(entity!.getIsPersistent())
        entity!.sync() { item in
            XCTAssertEqual(10, item.myInt)
            XCTAssertEqual("A String", item.myString)
        }
        collection.sync() { cache in
            XCTAssertEqual(1, cache.count)
            XCTAssertTrue (entity === cache[entity!.getId()]!.item!)
        }
        entity = nil
        collection.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
    }
    
    func testPersistentCollectionGet() throws {
        // Data In Cache=No; Data in Accessor=No
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let data = try JSONEncoder().encode(entity)
        let accessor = InMemoryAccessor()
        let collection = PersistentCollection<MyStruct>(accessor: accessor)
        XCTAssertNil (collection.get (id: entity.getId()))
        // Data In Cache=No; Data in Accessor=Yes
        accessor.add(id: entity.getId(), data: data)
        let retrievedEntity = collection.get(id: entity.getId())!
        XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
        XCTAssertEqual (entity.getVersion(), retrievedEntity.getVersion())
        XCTAssertTrue (retrievedEntity.getIsPersistent())
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
        // Data In Cache=Yes; Data in Accessor=No
        let entity2 = collection.new(item: MyStruct())
        XCTAssertTrue (entity2 === collection.get(id: entity2.getId()))
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.getId()]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.getId()]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[entity.getId()]!)
        }
        // Data In Cache=Yes; Data in Accessor=Yes
        XCTAssertTrue (retrievedEntity === collection.get(id: entity.getId())!)
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.getId()]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.getId()]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[entity.getId()]!)
        }
        // Invalid Data
        let invalidData = Data()
        let invalidDataUuid = UUID()
        accessor.add(id: invalidDataUuid, data: invalidData)
        let invalidEntity = collection.get(id: invalidDataUuid)
        XCTAssertNil (invalidEntity)
    }
    
    func testInMemoryAccessor() throws {
        let accessor = InMemoryAccessor()
        let uuid = UUID()
        XCTAssertNil(accessor.get(id: uuid))
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let data = try JSONEncoder().encode(entity)
        accessor.add(id: entity.getId(), data: data)
        XCTAssertTrue (data == accessor.get(id: entity.getId()))
        XCTAssertNil(accessor.get(id: uuid))
        accessor.update(id: entity.getId(), data: data)
        XCTAssertTrue (data == accessor.get(id: entity.getId()))
        XCTAssertNil(accessor.get(id: uuid))
    }

}
