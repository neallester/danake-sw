//
//  entityCommitDirtyTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/12/18.
//

import XCTest
@testable import danake

class EntityCommitDirtyTests: XCTestCase {

    // Test implementation of Entity.commit() from the PersistenceState.dirty state
    func testCommitDirty() {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        let savedData = accessor.getData(name: collectionName, id: entity.getId())!
        // .dirty building updateAction throws error
        let batch = Batch()
        entity.sync(batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (savedData, accessor.getData(name: collectionName, id: id))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (savedData, accessor.getData(name: collectionName, id: id))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        // .dirty Success
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        preFetchCount = 0
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        group = DispatchGroup()
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
            switch entity.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: id))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        switch entity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persitent")
        }
        XCTAssertEqual (2, entity.getVersion())
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
            switch entity.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: id))
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
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
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
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }

        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        switch entity.getPersistenceState() {
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
            switch entity.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: entity.getId()))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
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
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: entity.getId()))
    }
    
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update and errors
    func testCommitDirtyPendingUpdateWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: collectionName, id: entity.getId())!
        // building the initial updateAction closure occurs in the same block as the
        // state change to .saving so it is not possible for a pending update to post
        // if an error occurs when building the initial updateActionClosure
        //
        // error occurs while firing the initial updateAction closure
        var preFetchCount = 0
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
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
        XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
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
        var retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (2, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        // Error occurs when firing the pending updateAction closure
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        preFetchCount = 0
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (50, item.myInt)
                XCTAssertEqual ("50", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
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
        retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (3, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending updates
    func testCommitDirty2PendingUpdates() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
            switch entity.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: entity.getId()))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.async (batch: batch) { item in
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
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: entity.getId()))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending updates and errors
    func testCommitDirty2PendingUpdatesWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: collectionName, id: entity.getId())!
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.async (batch: batch) { item in
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
        XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (60, item.myInt)
                XCTAssertEqual ("60", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
            item.myInt = 50
            item.myString = "50"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.async (batch: batch) { item in
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
        var retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (2, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        // Error occurs when firing the pending updateAction closure
        preFetchCount = 0
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (80, item.myInt)
                XCTAssertEqual ("80", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (60, item.myInt)
            XCTAssertEqual ("60", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.async (batch: batch) { item in
            item.myInt = 70
            item.myString = "70"
        }
        switch entity.getPendingAction()! {
        case .update:
            break
        default:
            XCTFail ("Expected .update")
        }
        entity.async (batch: batch) { item in
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
        retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (3, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (60, item.myInt)
            XCTAssertEqual ("60", item.myString)
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending remove followed by pending update
    func testCommitDirtyPendingRemoveUpdate() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
            switch entity.getPersistenceState() {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: entity.getId()))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        entity.async (batch: batch) { item in
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
        XCTAssertEqual (entity.asData(encoder: accessor.encoder), accessor.getData(name: collectionName, id: entity.getId()))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending remove followed by pending update and errors
    func testCommitDirtyPendingRemoveUpdateWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: collectionName, id: entity.getId())!
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        entity.async (batch: batch) { item in
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
        XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
        // Error occurs when building the pending updateAction closure
        preFetchCount = 0
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        entity.async (batch: batch) { item in
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
        var retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (2, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        // Error occurs when firing the pending updateAction closure
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (50, item.myInt)
                XCTAssertEqual ("50", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.getVersion())
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
        entity.async (batch: batch) { item in
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
        retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (3, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending remove
    func testCommitDirtyPendingRemove() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
            switch entity.getPersistenceState() {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: collectionName, id: entity.getId()))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        XCTAssertFalse (accessor.has(name: collectionName, id: entity.getId()))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending remove and errors
    func testCommitDirtyPendingRemoveWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: collectionName, id: entity.getId())!
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.sync(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        var retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (2, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        // Error occurs when firing the pending removeAction closure
        entity.sync(batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.getVersion())
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
        retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (3, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with 2 pending removes
    func testCommitDirty2PendingRemoves() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
            switch entity.getPersistenceState() {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: collectionName, id: entity.getId()))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        XCTAssertFalse (accessor.has(name: collectionName, id: entity.getId()))
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with two pending removes and errors
    func testCommitDirty2PendingRemovesWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: collectionName, id: entity.getId())!
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (20, item.myInt)
                XCTAssertEqual ("20", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.sync(batch: batch) { item in
            item.myInt = 30
            item.myString = "30"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (1, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
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
        var retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (2, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (30, item.myInt)
            XCTAssertEqual ("30", item.myString)
        }
        // Error occurs when firing the pending removeAction closure
        entity.sync(batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertEqual (2, entity.getVersion())
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (40, item.myInt)
                XCTAssertEqual ("40", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.getVersion())
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
        retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (3, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with pending update followed by a pending remove
    func testCommitDirtyPendingUpdateRemove() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
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
            switch entity.getPersistenceState() {
            case .abandoned:
                break
            default:
                XCTFail ("Expected .abandoned")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertFalse (accessor.has(name: collectionName, id: entity.getId()))
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.sync (batch: batch) { item in
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
        XCTAssertFalse (accessor.has(name: collectionName, id: entity.getId()))
    
    }
    
    // Test implementation of Entity.commit() from the PersistenceState.dirty state with a pending update followed by a pending remove and errors
    func testCommitDirtyPendingUpdateRemoveWithErrors() throws {
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collectionName: CollectionName = "myCollection"
        let collection = PersistentCollection<Database, MyStruct>(database: database, name: collectionName)
        let id = UUID()
        let entity = Entity<MyStruct> (collection: collection, id: id, version: 0, item: MyStruct(myInt: 10, myString: "10"))
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
        XCTAssertEqual (1, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (10, item.myInt)
            XCTAssertEqual ("10", item.myString)
        }
        let batch = Batch()
        entity.async (batch: batch) { item in
            item.myInt = 20
            item.myString = "20"
        }
        switch entity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        XCTAssertNil (entity.getPendingAction())
        let savedData0 = accessor.getData(name: collectionName, id: entity.getId())!
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (1, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (30, item.myInt)
                XCTAssertEqual ("30", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (20, item.myInt)
            XCTAssertEqual ("20", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.sync (batch: batch) { item in
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
        XCTAssertEqual (String (data: savedData0, encoding: .utf8)!, String (data: accessor.getData(name: collectionName, id: entity.getId())!, encoding: .utf8)!)
        // Error occurs when building the pending removeAction closure
        preFetchCount = 0
        entity.async (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        switch entity.getPersistenceState() {
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .unrecoverableError")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (2, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (50, item.myInt)
                XCTAssertEqual ("50", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (2, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.sync(batch: batch) { item in
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
        var retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (2, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        // Error occurs when firing the pending removeAction closure
        preFetchCount = 0
        entity.async (batch: batch) { item in
            item.myInt = 40
            item.myString = "40"
        }
        XCTAssertEqual (2, entity.getVersion())
        switch entity.getPersistenceState() {
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
                XCTAssertEqual ("Test Error", errorMessage)
            default:
                XCTFail ("Expected .error")
            }
            switch entity.getPersistenceState() {
            case .pendingRemoval:
                break
            default:
                XCTFail ("Expected .pendingRemoval")
            }
            XCTAssertEqual (3, entity.getVersion())
            entity.sync() { item in
                XCTAssertEqual (50, item.myInt)
                XCTAssertEqual ("50", item.myString)
            }
            XCTAssertNil (entity.getPendingAction())
            group.leave()
        }
        switch entity.getPersistenceState() {
        case .saving:
            break
        default:
            XCTFail ("Expected .saving")
        }
        XCTAssertEqual (3, entity.getVersion())
        entity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
        XCTAssertNil (entity.getPendingAction())
        entity.sync (batch: batch) { item in
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
        retrievedEntity = try accessor.decoder.decode(Entity<MyStruct>.self, from: accessor.getData(name: collectionName, id: id)!)
        XCTAssertEqual (entity.getId(), retrievedEntity.getId())
        XCTAssertEqual (3, retrievedEntity.getVersion())
        XCTAssertNil (retrievedEntity.getPendingAction())
        switch retrievedEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        retrievedEntity.sync() { item in
            XCTAssertEqual (40, item.myInt)
            XCTAssertEqual ("40", item.myString)
        }
    }

}