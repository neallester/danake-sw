//
//  entityCommitDirtyTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/12/18.
//

import XCTest
import JSONEquality
@testable import danake

class EntityCommitDirtyTests: XCTestCase {

    // Test implementation of Entity.commit() from the PersistenceState.dirty state
    func testCommitDirty() {
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
        group.enter()
        entity.commit() { result in
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
        let savedData = accessor.getData(name: cacheName, id: entity.id)!
        // .dirty building updateAction throws error
        let batch = EventuallyConsistentBatch()
        entity.update(batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
        accessor.setThrowError()
        group.enter()
        entity.commit() { result in
            switch result {
            case .unrecoverableError(let errorMessage):
                XCTAssertEqual ("updateActionError", errorMessage)
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
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData, accessor.getData(name: cacheName, id: id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        // .dirty firing updateAction throws error
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData, accessor.getData(name: cacheName, id: id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        // .dirty firing updateAction times out
        do {
            let isEqual = try JSONEquality.JSONEquals (savedData, accessor.getData(name: cacheName, id: id)!)
            XCTAssertTrue (isEqual)
        } catch {
            XCTFail ("Expected success but got \(error)")
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
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
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        do {
            let isEqual = try JSONEquality.JSONEquals (savedData, accessor.getData(name: cacheName, id: id)!)
            XCTAssertTrue (isEqual)
        } catch {
            XCTFail ("Expected success but got \(error)")
        }
        // .dirty Success
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        preFetchCount = 0
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
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
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
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
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
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
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update
    func testCommitDirtyPendingUpdate() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }

        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
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
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        do {
            let isEqual = try JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: entity.id)!)
            XCTAssertTrue (isEqual)
        } catch {
            XCTFail ("Expected success but got \(error)")
        }
    }
    
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update and errors
    func testCommitDirtyPendingUpdateWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = Entity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        group.enter()
        entity.commit() { result in
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
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        do {
            let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
            XCTAssertTrue (isEqual)
        } catch {
            XCTFail ("Expected success but got \(error)")
        }
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
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
                XCTAssertEqual ("updateActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (2, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":2}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
        // Error occurs when firing the pending updateAction closure
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        preFetchCount = 0
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
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
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
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
        try XCTAssertTrue (JSONEquality.JSONEquals("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cache.name, id: entity.id)!, encoding: .utf8)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update and timeout
    func testCommitDirtyPendingUpdateWithTimeouts() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cacheName: CacheName = "myCollection"
        let cache = EntityCache<MyStruct>(database: database, name: cacheName)
        let id = UUID()
        let entity = TimeoutHookEntity<MyStruct> (cache: cache, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"), semaphoreValue: 1)
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore (value: 1)
        group.enter()
        entity.commit() { result in
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
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // timeout occurs while firing the initial updateAction closure
        var preFetchCount = 0
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group.enter()
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
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
        XCTAssertEqual (2, entity.version)
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
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending updates
    func testCommitDirty2PendingUpdates() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: entity.id)!))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending updates and errors
    func testCommitDirty2PendingUpdatesWithErrors() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
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
                XCTAssertEqual ("updateActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (2, entity.version)
            entity.sync() { item in
                XCTAssertEqual (60, item.myInt)
                XCTAssertEqual ("60", item.myString)
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
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
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
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":2}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
        // Error occurs when firing the pending updateAction closure
        preFetchCount = 0
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (60, item.myInt)
            XCTAssertEqual ("60", item.myString)
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
                XCTAssertEqual (80, item.myInt)
                XCTAssertEqual ("80", item.myString)
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
            XCTAssertEqual (60, item.myInt)
            XCTAssertEqual ("60", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
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
        entity.update (batch: batch) { item in
            item.myInt = 80
            item.myString = "80"
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
        do {
            let _ = try accessor.decoder (cache: cache).decode(Entity<MyStruct>.self, from: accessor.getData(name: cacheName, id: id)!)
            XCTFail ("Expected exception")
        } catch EntityDeserializationError<MyStruct>.alreadyCached(let cachedEntity) {
            XCTAssertTrue (cachedEntity === entity)
        } catch {
            print (error)
        }
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending updates and timeouts
    func testCommitDirty2PendingUpdatesWithTimeouts() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending remove followed by pending update
    func testCommitDirtyPendingRemoveUpdate() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (entity.asData(encoder: accessor.encoder)!, accessor.getData(name: cacheName, id: entity.id)!))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending remove followed by pending update and errors
    func testCommitDirtyPendingRemoveUpdateWithErrors() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
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
                XCTAssertEqual ("updateActionError", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.persistenceState {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (2, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":2}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
        // Error occurs when firing the pending updateAction closure
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        preFetchCount = 0
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
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending remove followed by pending update and timeouts
    func testCommitDirtyPendingRemoveUpdateWithTimeouts() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group.enter()
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
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
        XCTAssertEqual (2, entity.version)
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
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending remove
    func testCommitDirtyPendingRemove() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
            XCTAssertEqual (3, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        XCTAssertFalse (accessor.has(name: cacheName, id: entity.id))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending remove and errors
    func testCommitDirtyPendingRemoveWithErrors() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.update(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
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
            XCTAssertEqual (2, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        semaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":2}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
        // Error occurs when firing the pending removeAction closure
        entity.update(batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        preFetchCount = 0
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
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending remove and timeouts
    func testCommitDirtyPendingRemoveWithTimeouts() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group.enter()
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        entity.timeoutSemaphore.signal()
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        semaphore.signal()
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending removes
    func testCommitDirty2PendingRemoves() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.version)
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
            XCTAssertEqual (3, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with two pending removes and errors
    func testCommitDirty2PendingRemovesWithErrors() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.update(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
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
            XCTAssertEqual (2, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"persistent\",\"version\":2}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
        // Error occurs when firing the pending removeAction closure
        entity.update(batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        preFetchCount = 0
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
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with two pending removes and timeouts
    func testCommitDirty2PendingRemovesWithTimeouts() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group.enter()
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.persistenceState {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.version)
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
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending update followed by a pending remove
    func testCommitDirtyPendingUpdateRemove() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
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
            XCTAssertEqual (3, entity.version)
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
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
        XCTAssertEqual (2, entity.version)
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
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update followed by a pending remove and errors
    func testCommitDirtyPendingUpdateRemoveWithErrors() throws {
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
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
            do {
                let isEqual = try JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!)
                XCTAssertTrue (isEqual)
            } catch {
                XCTFail ("Expected success but got \(error)")
            }
            group.leave()
        }
        switch entity.persistenceState {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.update (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
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
            XCTAssertEqual (2, entity.version)
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
        XCTAssertEqual (2, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.update(batch: batch) { item in
            item.myInt = 50
            item.myString = "50"
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":2}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
        // Error occurs when firing the pending removeAction closure
        preFetchCount = 0
        entity.update (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        XCTAssertEqual (2, entity.version)
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
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
        XCTAssertEqual (3, entity.version)
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
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
        try XCTAssertTrue (JSONEquality.JSONEquals ("{\"id\":\"\(entity.id.uuidString)\",\"schemaVersion\":5,\"created\":\(jsonEncodedDate(date: entity.created)!),\"item\":{\"myInt\":40,\"myString\":\"40\"},\"persistenceState\":\"persistent\",\"version\":3}", String (data: accessor.getData(name: cacheName, id: id)!, encoding: .utf8)!))
    }

    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update followed by a pending remove and timeouts
    func testCommitDirtyPendingUpdateRemoveWithTimeoutss() throws {
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
        // timeouts occurs while firing the initial updateAction closure
        var preFetchCount = 0
        group.enter()
        entity.commit() { result in
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
        let batch = EventuallyConsistentBatch()
        entity.update (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: cacheName, id: entity.id)!
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
        switch entity.timeoutSemaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected Success")
        }
        group.enter()
        entity.commit(timeout: .nanoseconds(1)) { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("timeout:InMemoryAccessor.update;database=\(accessor.hashValue);entityCache=myCollection;entityID=\(entity.id.uuidString)", errorMessage)
            default:
                XCTFail ("Expected .error")
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
        XCTAssertEqual (2, entity.version)
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
        try XCTAssertTrue (JSONEquality.JSONEquals (savedData0, accessor.getData(name: cacheName, id: entity.id)!))
    }

}
