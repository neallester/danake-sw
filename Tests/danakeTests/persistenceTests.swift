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
        let collection = PersistentCollection<MyStruct>(accessor: InMemoryAccessor(), workQueue: DispatchQueue (label: "Test"), logger: nil)
        var entity: Entity<MyStruct>? = collection.new(item: myStruct)
        XCTAssertTrue (collection === entity!.collection!)
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
        entity = nil
        collection.sync() { cache in
            XCTAssertEqual(0, cache.count)
        }
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
        result = .invalidData
        XCTAssertFalse (result.isOk())
        XCTAssertNil (result.item())       
    }
    
    func testPersistentCollectionGet() throws {
        // Data In Cache=No; Data in Accessor=No
        let entity = newTestEntity(myInt: 10, myString: "A String")
        let data = try JSONEncoder().encode(entity)
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let collection = PersistentCollection<MyStruct>(accessor: accessor, workQueue: DispatchQueue (label: "Test"), logger: logger)
        var result = collection.get (id: entity.getId())
        XCTAssertTrue (result.isOk())
        XCTAssertNil (result.item())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.get|Unknown id|id=", entries[0].asTestString().prefix(55))
            XCTAssertEqual (91, entries[0].asTestString().count)
            
        }
        // Data In Cache=No; Data in Accessor=Yes
        accessor.add(id: entity.getId(), data: data)
        result = collection.get(id: entity.getId())
        let retrievedEntity = result.item()!
        XCTAssertEqual (entity.getId().uuidString, retrievedEntity.getId().uuidString)
        XCTAssertEqual (entity.getVersion(), retrievedEntity.getVersion())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
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
        let entity2 = collection.new(item: MyStruct())
        XCTAssertTrue (entity2 === collection.get(id: entity2.getId()).item()!)
        collection.sync() { cache in
            XCTAssertEqual (2, cache.count)
            XCTAssertTrue (retrievedEntity === cache[entity.getId()]!.item!)
            XCTAssertTrue (entity2 === cache[entity2.getId()]!.item!)
        }
        accessor.sync() { storage in
            XCTAssertEqual (1, storage.count)
            XCTAssertTrue (data == storage[entity.getId()]!)
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
            XCTAssertTrue (data == storage[entity.getId()]!)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
        // Invalid Data
        let json = "{}"
        let invalidData = json.data(using: .utf8)!
        let invalidDataUuid = UUID()
        accessor.add(id: invalidDataUuid, data: invalidData)
        let invalidEntity = collection.get(id: invalidDataUuid)
        switch invalidEntity {
        case .invalidData:
            break
        default:
            XCTFail ("Expected .invalidData")
        }
        XCTAssertNil (invalidEntity.item())
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.get|Illegal Data|id=", entries[1].asTestString().prefix(57))
            XCTAssertEqual (";data={}", entries[1].asTestString().suffix(8))
            XCTAssertEqual (101, entries[1].asTestString().count)
        }
    }
    
    func testPersistentCollectionGetAsync () throws {
        // Data In Cache=No; Data in Accessor=No
        let entity1 = newTestEntity(myInt: 10, myString: "A String1")
        let data1 = try JSONEncoder().encode(entity1)
        let entity2 = newTestEntity(myInt: 20, myString: "A String2")
        let data2 = try JSONEncoder().encode(entity2)
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let collection = PersistentCollection<MyStruct>(accessor: accessor, workQueue: DispatchQueue (label: "Test", attributes: .concurrent), logger: logger)
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
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.get|Unknown id|id=", entries[0].asTestString().prefix(55))
            XCTAssertEqual (91, entries[0].asTestString().count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.get|Unknown id|id=", entries[1].asTestString().prefix(55))
            XCTAssertEqual (91, entries[1].asTestString().count)

        }
        // Data In Cache=No; Data in Accessor=Yes
        waitFor1 = expectation(description: "wait1.2")
        waitFor2 = expectation(description: "wait2.2")
        accessor.add(id: entity1.getId(), data: data1)
        accessor.add(id: entity2.getId(), data: data2)
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
        switch retrievedEntity1.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail("Expected .persistent")
        }
        switch retrievedEntity2.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail("Expected .persistent")
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
        let entity3 = collection.new(item: MyStruct())
        let entity4 = collection.new(item: MyStruct())
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
            XCTAssertEqual (2, storage.count)
            XCTAssertTrue (data1 == storage[entity1.getId()]!)
            XCTAssertTrue (data2 == storage[entity2.getId()]!)
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
            XCTAssertEqual (2, storage.count)
            XCTAssertTrue (data1 == storage[entity1.getId()]!)
            XCTAssertTrue (data2 == storage[entity2.getId()]!)
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
        accessor.add(id: invalidDataUuid1, data: invalidData1)
        accessor.add(id: invalidDataUuid2, data: invalidData2)
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
        case .invalidData:
            break
        default:
            XCTFail ("Expected .invalidData")
        }
        switch result2! {
        case .invalidData:
            break
        default:
            XCTFail ("Expected .invalidData")
        }
        XCTAssertNil (result1!.item())
        XCTAssertNil (result2!.item())
        logger.sync() { entries in
            XCTAssertEqual (4, entries.count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.get|Illegal Data|id=", entries[2].asTestString().prefix(57))
            XCTAssertEqual (";data={", entries[2].asTestString().suffix(7))
            XCTAssertEqual (100, entries[2].asTestString().count)
            XCTAssertEqual ("ERROR|PersistentCollection<MyStruct>.get|Illegal Data|id=", entries[3].asTestString().prefix(57))
            XCTAssertEqual (";data={\"notanattribute\":1000}", entries[3].asTestString().suffix(29))
            XCTAssertEqual (122, entries[3].asTestString().count)
        }
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
