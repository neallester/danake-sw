//
//  entityCommitNewTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import XCTest
@testable import danake

class EntityCommitNewTests: XCTestCase {

    // Test implementation of Entity.commit() from the PersistenceState.new state
    func testCommitNew() {
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
        // .new building addAction throws error
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        accessor.setThrowError()
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("addActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
            group.leave()
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        // .new firing updateAction throws error
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        // .new firing updateAction times out
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetchGroup = DispatchGroup()
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
                prefetchGroup.leave()
            }
            preFetchCount = preFetchCount + 1
        }
        preFetchCount = 0
        accessor.setPreFetch (prefetch)
        group.enter()
        prefetchGroup.enter()
        entity.commit(timeout: .nanoseconds (1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        semaphore.signal()
        switch prefetchGroup.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // .new Success
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
        group = DispatchGroup()
        prefetch = { id in
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
        entity.commit() { result in
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
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        // Move to a different test?
        // .persistent
        switch entity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persitent")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        group = DispatchGroup()
        prefetch = { id in
            XCTFail ("No Prefetch")
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
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
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending update
    func testCommitNewPendingUpdate() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        entity.commit() { result in
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
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending update and errors
    func testCommitNewPendingUpdateWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 2 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("addActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            var json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains ("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains ("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains ("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains ("\"item\":{"))
            XCTAssertTrue (json.contains ("\"myInt\":20"))
            XCTAssertTrue (json.contains ("\"myString\":\"20\""))
            XCTAssertTrue (json.contains ("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains ("\"version\":1"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":20,\"myString\":\"20\"},\"persistenceState\":\"persistent\",\"version\":1}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
        // Error occurs when firing the pending updateAction closure
        entity.remove(batch: batch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.update(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        preFetchCount = 0
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 3 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":30"))
            XCTAssertTrue (json.contains("\"myString\":\"30\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":3"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending update and timeouts
    func testCommitNewPendingUpdateWithTimeouts() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with 2 pending updates
    func testCommitNew2PendingUpdates() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
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
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group.enter()
        entity.commit() { result in
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
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.update (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with 2 pending updates and errors
    func testCommitNew2PendingUpdatesWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.update (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 2 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("addActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (50, item.myInt)
                XCTAssertEqual ("50", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.update (batch: batch) { item in
            item.myInt = 50
            item.myString = "50"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            var json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":30"))
            XCTAssertTrue (json.contains("\"myString\":\"30\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":1"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":1}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
        // Error occurs when firing the pending updateAction closure
        entity.remove(batch: batch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.update(batch: batch) { item in
            item.myInt = 50
            item.myString = "50"
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (50, item.myInt)
            XCTAssertEqual ("50", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        preFetchCount = 0
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 3 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (70, item.myInt)
                XCTAssertEqual ("70", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (50, item.myInt)
            XCTAssertEqual ("50", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update (batch: batch) { item in
            item.myInt = 60
            item.myString = "60"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.update (batch: batch) { item in
            item.myInt = 70
            item.myString = "70"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":50"))
            XCTAssertTrue (json.contains("\"myString\":\"50\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":3"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":50,\"myString\":\"50\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
    }

    // Test implementation of Entity.commit() from the PersistenceState.new state with 2 pending updates and timeouts
    func testCommitNew2PendingUpdatesWithTimeouts() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        switch entity.timeoutSemaphore.wait (timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.update (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with pending remove followed by pending update
    func testCommitNewPendingRemoveUpdate() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        entity.commit() { result in
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
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with pending remove followed by pending update and errors
    func testCommitNewPendingRemoveUpdateWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 2 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("addActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.update (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            var json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":20"))
            XCTAssertTrue (json.contains("\"myString\":\"20\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":1"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":20,\"myString\":\"20\"},\"persistenceState\":\"persistent\",\"version\":1}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
        // Error occurs when firing the pending updateAction closure
        entity.remove(batch: batch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.update(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        preFetchCount = 0
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 3 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.update (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":30"))
            XCTAssertTrue (json.contains("\"myString\":\"30\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":3"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
    }

    // Test implementation of Entity.commit() from the PersistenceState.new state with pending remove followed by pending update and timeouts
    func testCommitNewPendingRemoveUpdateWithTimeouts() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .new:
                break
            default:
                XCTFail ("Expected .new")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
    }

    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending remove
    func testCommitNewPendingRemove() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending remove and errors
    func testCommitNewPendingRemoveWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.update(batch: batch) { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 2 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("removeActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            var json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":10"))
            XCTAssertTrue (json.contains("\"myString\":\"10\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":1"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":10,\"myString\":\"10\"},\"persistenceState\":\"persistent\",\"version\":1}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
        // Error occurs when firing the pending removeAction closure
        entity.remove(batch: batch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        preFetchCount = 0
        entity.update(batch: batch) { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (2, entity.version)
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
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 3 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("removeError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":10"))
            XCTAssertTrue (json.contains("\"myString\":\"10\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":3"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":10,\"myString\":\"10\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
    }

    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending remove and timeouts
    func testCommitNewPendingRemoveWithTimeout() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }

        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with 2 pending removes
    func testCommitNew2PendingRemoves() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with two pending removes and errors
    func testCommitNew2PendingRemovesWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.update(batch: batch) { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 2 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("removeActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            let json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue (json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue (json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue (json.contains("\"item\":{"))
            XCTAssertTrue (json.contains("\"myInt\":10"))
            XCTAssertTrue (json.contains("\"myString\":\"10\""))
            XCTAssertTrue (json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue (json.contains("\"version\":1"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":10,\"myString\":\"10\"},\"persistenceState\":\"persistent\",\"version\":1}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
    }

    // Test implementation of Entity.commit() from the PersistenceState.new state with two pending removes and timeouts
    func testCommitNew2PendingRemovesWithTimeouts() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (10, item.myInt)
                XCTAssertEqual ("10", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        let batch = EventuallyConsistentBatch()
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with pending update followed by a pending remove
    func testCommitNewPendingUpdateRemove() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending update followed by a pending remove and errors
    func testCommitNewPendingUpdateRemoveWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        var prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("addError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: cacheName, id: id))
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.update(batch: batch) { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (0, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 2 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("removeActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            var json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue(json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue(json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue(json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue(json.contains("\"item\":{"))
            XCTAssertTrue(json.contains("\"myInt\":20"))
            XCTAssertTrue(json.contains("\"myString\":\"20\""))
            XCTAssertTrue(json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue(json.contains("\"version\":1"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":20,\"myString\":\"20\"},\"persistenceState\":\"persistent\",\"version\":1}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
        // Error occurs when firing the pending removeAction closure
        entity.remove(batch: batch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .ok:
                break
            default:
                XCTFail ("Expected .ok")
            }
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        preFetchCount = 0
        entity.update(batch: batch) { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            } else if preFetchCount == 3 {
                accessor.throwError = true
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        entity.commit() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("removeError", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        #if os(Linux)
            json = String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!
            XCTAssertTrue(json.contains("\"id\":\"\(entity.id.uuidString)\""))
            XCTAssertTrue(json.contains("\"schemaVersion\":5"))
            try XCTAssertTrue(json.contains("\"created\":\(jsonEncodedDate(date: entity.created)!)"))
            XCTAssertTrue(json.contains("\"item\":{"))
            XCTAssertTrue(json.contains("\"myInt\":30"))
            XCTAssertTrue(json.contains("\"myString\":\"30\""))
            XCTAssertTrue(json.contains("\"persistenceState\":\"persistent\""))
            XCTAssertTrue(json.contains("\"version\":3"))
        #else
            try XCTAssertEqual ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!)
        #endif
    }

    // Test implementation of Entity.commit() from the PersistenceState.new state with a pending update followed by a pending remove and timeouts
    func testCommitNewPendingUpdateRemoveWithTimeouts() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        switch entity.persistenceState {
        case .new:
            break
        default:
            XCTFail ("Expected .new")
        }
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
        let prefetch: (UUID) -> () = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.throwError = true
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        accessor.setPreFetch (prefetch)
        group.enter()
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("Entity.commit():timedOut:nanoseconds(1)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (0, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.remove (batch: batch)
        switch entity.getPendingAction()! {
        case .remove:
            break
        default:
            XCTFail ("Expected .remove")
        }
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        XCTAssertFalse (accessor.has(name: cacheName, id: id))
    }

    
    
}
