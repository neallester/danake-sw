//
//  BatchTests.swift
//  danakeTests
//
//  Created by Neal Lester on 1/26/18.
//

import XCTest
@testable import danake

class BatchTests: XCTestCase {

    func testInsert() {
        let batch = Batch()
        let entity = newTestEntity(myInt: 10, myString: "Test Completed")
        entity.incrementVersion()
        batch.insert(item: entity)
        batch.syncItems() { (items: Dictionary<UUID, (version: Int, item: EntityManagement)>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(1, entity.getVersion())
            XCTAssertEqual(entity.getVersion(), items[entity.getId()]!.version)
            let retrievedEntity = items[entity.getId()]!.item as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        entity.incrementVersion()
        batch.insert(item: entity)
        batch.syncItems() { (items: Dictionary<UUID, (version: Int, item: EntityManagement)>) in
            XCTAssertEqual(1, items.count)
            XCTAssertEqual(2, entity.getVersion())
            XCTAssertEqual(entity.getVersion(), items[entity.getId()]!.version)
            let retrievedEntity = items[entity.getId()]!.item as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
        }
        let entity2 = newTestEntity(myInt: 0, myString: "")
        entity2.incrementVersion()
        batch.insert(item: entity2)
        batch.syncItems() { (items: Dictionary<UUID, (version: Int, item: EntityManagement)>) in
            XCTAssertEqual(2, items.count)
            XCTAssertEqual(2, entity.getVersion())
            XCTAssertEqual(1, entity2.getVersion())
            XCTAssertEqual(entity.getVersion(), items[entity.getId()]!.version)
            var retrievedEntity = items[entity.getId()]!.item as! Entity<MyStruct>
            XCTAssertTrue (entity === retrievedEntity)
            XCTAssertEqual(entity2.getVersion(), items[entity2.getId()]!.version)
            retrievedEntity = items[entity2.getId()]!.item as! Entity<MyStruct>
            XCTAssertTrue (entity2 === retrievedEntity)
        }

    }

}
