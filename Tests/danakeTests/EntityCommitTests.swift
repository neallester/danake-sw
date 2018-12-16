//
//  EntityCommitTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/19/18.
//

import XCTest
@testable import danake

class EntityCommitTests: XCTestCase {

    // Test calling entity.commit() when entity is PersistenceState.persistent
    func testEntityCommitPersistent() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        group.enter()
        entity.commit(context: nil) { result in
            switch result {
            case .ok:
                group.leave()
            default:
                XCTFail("Expected .ok")
            }
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
        group.enter()
        entity.commit(context: nil) { result in
            switch result {
            case .ok:
                group.leave()
            default:
                XCTFail("Expected .ok")
            }
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
    }

    // Test calling entity.commit() when entity is PersistenceState.abandoned
    func testEntityCommitAbandoned() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let batch = EventuallyConsistentBatch(context: nil)
        entity.remove(batch: batch)
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        XCTAssertFalse (accessor.has (name: cacheName, id: entity.id))
        group.enter()
        entity.commit(context: nil) { result in
            switch result {
            case .ok:
                group.leave()
            default:
                XCTFail("Expected .ok")
            }
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch entity.persistenceState {
        case .abandoned:
            break
        default:
            XCTFail ("Expected .abandoned")
        }
        XCTAssertFalse (accessor.has (name: cacheName, id: entity.id))
    }
    
    // Test calling entity.commit() when entity is PersistenceState.saving
    func testEntityCommitSaving() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        var group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        preFetchCount = 0
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group = DispatchGroup()
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit(context: nil) { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            switch entity.persistenceState {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: id))
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let group2 = DispatchGroup()
        group2.enter()
        entity.commit(context: nil) { result in
            switch result {
            case .ok:
                group2.leave()
            default:
                XCTFail ("Expected .ok")
            }
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch group2.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
    }

}
