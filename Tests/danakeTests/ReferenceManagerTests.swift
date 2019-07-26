//
//  ReferenceManagerTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/27/18.
//

import XCTest
@testable import danake
import PromiseKit

class ReferenceManagerTests: XCTestCase {

    func testCreationEncodeDecode() throws {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        let parentDataContainer = DataContainer()
        var parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        let childId = UUID()
        let child = Entity (cache: cache, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        decoder.userInfo[Database.parentDataKey] = parentDataContainer
        // not isEager
        // Creation with entity
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child)
        var parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === child)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        // Creation with nil entity
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Creation with referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, child.id.uuidString)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Creation with nil referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // isEager
        // Creation with entity
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === child)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Creation with nil entity
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Creation with referenceData for a cached object
        var waitFor = expectation(description: "wait1")
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        var testReference = RetrieveControlledReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData(), isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference = testReference
        switch testReference.contentsReadyGroup.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        if let reference = testReference.contents {
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, child.id.uuidString)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving but got \(reference.state)")
            }
            XCTAssertTrue (reference.isEager)
        } else {
            XCTFail ("Expected testReference.contents")
        }
        var promiseResolver: (promise: Promise<Entity<MyStruct>?>, resolver: Resolver<Entity<MyStruct>?>) = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Creation with referenceData for a uncached object
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        let persistentChildId1 = UUID()
        let persistentChildData1 = "{\"id\":\"\(persistentChildId1.uuidString)\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":100,\"myString\":\"100\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        switch accessor.add(name: cache.name, id: persistentChildId1, data: persistentChildData1) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        waitFor = expectation(description: "wait1a")
        testReference = RetrieveControlledReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: ReferenceManagerData(databaseId: accessor.hashValue, cacheName: cache.name, id: persistentChildId1, version: 10), isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference = testReference
        switch testReference.contentsReadyGroup.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        if let reference = testReference.contents {
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, persistentChildId1.uuidString)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving but got \(reference.state)")
            }
            XCTAssertTrue (reference.isEager)
        } else {
            XCTFail("Expected .contents")
        }
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == persistentChildId1.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Creation with nil referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: nil, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Decoding
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var json = "{\"isEager\":false,\"isNil\":true}"
        try XCTAssertTrue(String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":false"))
        try XCTAssertTrue(String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isNil\":true"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        json = "{\"isEager\":true,\"isNil\":true}"
        try XCTAssertTrue(String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":true"))
        try XCTAssertTrue(String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isNil\":true"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertTrue (child === reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (child.cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        json = "{\"isEager\":false,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"id\":\"\(child.id.uuidString)\"}"
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"id\":\"\(child.id.uuidString)\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":false"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData().id.uuidString, reference.referenceData!.id.uuidString)
            XCTAssertEqual (child.referenceData().qualifiedCacheName, reference.referenceData!.qualifiedCacheName)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertTrue (child === reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (child.cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        json = "{\"isEager\":true,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"id\":\"\(child.id.uuidString)\"}"
        waitFor = expectation(description: "wait1")
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        testReference = try decoder.decode(RetrieveControlledReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference = testReference
        switch testReference.contentsReadyGroup.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        if let reference = testReference.contents {
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData().id.uuidString, reference.referenceData!.id.uuidString)
            XCTAssertEqual (child.referenceData().qualifiedCacheName, reference.referenceData!.qualifiedCacheName)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        } else {
            XCTFail("Expected .contents")
        }
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        let _ = promiseResolver.promise.done() { entity in
            print ("done")
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"id\":\"\(child.id.uuidString)\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":true"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        // Creation with ReferenceManagerData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        json = "{\"isEager\":false,\"isNil\":true}"
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":false"))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isNil\":true"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: nil, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        json = "{\"isEager\":true,\"isNil\":true}"
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":true"))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isNil\":true"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData!)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        json = "{\"isEager\":false,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"id\":\"\(child.id.uuidString)\"}"
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"id\":\"\(child.id.uuidString)\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":false"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData().id.uuidString, reference.referenceData!.id.uuidString)
            XCTAssertEqual (child.referenceData().qualifiedCacheName, reference.referenceData!.qualifiedCacheName)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        let persistentChildId2 = UUID()
        let persistentChildData2 = "{\"id\":\"\(persistentChildId2.uuidString)\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":200,\"myString\":\"200\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        switch accessor.add(name: cache.name, id: persistentChildId2, data: persistentChildData2) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        json = "{\"isEager\":true,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"id\":\"\(persistentChildId2.uuidString)\"}"
        waitFor = expectation(description: "wait3")
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        testReference = try decoder.decode(RetrieveControlledReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference = testReference
        switch testReference.contentsReadyGroup.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail("Expected .success")
        }
        if let reference = testReference.contents {
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (persistentChildId2.uuidString, reference.referenceData!.id.uuidString)
            XCTAssertTrue (reference.cache === child.cache)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        } else {
            XCTFail ("Expected .contents")
        }
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == persistentChildId2.uuidString)
            XCTAssertTrue(reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"id\":\"\(persistentChildId2.uuidString)\""))
        try XCTAssertTrue (String (data: encoder.encode(reference), encoding: .utf8)!.contains("\"isEager\":true"))
        #if os(Linux)
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        // Decoding with isNil:false
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"version\":10,\"isNil\":false}"
        waitFor = expectation(description: "wait1")
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        parentDataContainer.data = parentData
        testReference = try decoder.decode(RetrieveControlledReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference = testReference
        switch testReference.contentsReadyGroup.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        if let reference = testReference.contents {
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData().id.uuidString, reference.referenceData!.id.uuidString)
            XCTAssertEqual (child.referenceData().qualifiedCacheName, reference.referenceData!.qualifiedCacheName)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        } else {
            XCTFail("Expected .contents")
        }
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (cache === reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Decoding errors
        // IsNil Present but False
        json = "{\"isEager\":false,\"isNil\":false}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"qualifiedCacheName\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"qualifiedCacheName\\\", intValue: nil) (\\\"qualifiedCacheName\\\").\", underlyingError: nil))", "\(error)")
        }
        // No qualifiedCacheName
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"version\":10}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"qualifiedCacheName\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"qualifiedCacheName\\\", intValue: nil) (\\\"qualifiedCacheName\\\").\", underlyingError: nil))", "\(error)")
        }
        // No id
        json = "{\"isEager\":true,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal id
        json = "{\"databaseId\":\"\",\"id\":\"AAA\",\"isEager\":true,\"qualifiedCacheName\":\"\(database.accessor.hashValue).myCollection\",\"version\":10,\"isNil\":false}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("dataCorrupted(Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"id\", intValue: nil)], debugDescription: \"Attempted to decode UUID from invalid UUID string.\", underlyingError: nil))", "\(error)")
        }
        // isEager missing
        json = "{\"databaseId\":\"\(database.accessor.hashValue)\",\"id\":\"\(child.id.uuidString)\",\"cacheName\":\"myCollection\",\"version\":10,\"isNil\":false}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"isEager\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"isEager\\\", intValue: nil) (\\\"isEager\\\").\", underlyingError: nil))", "\(error)")
        }
        // illegal isEager
        json = "{\"databaseId\":\"\(database.accessor.hashValue)\",\"id\":\"\(child.id.uuidString)\",\"isEager\":\"what?\",\"cacheName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Bool, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"isEager\", intValue: nil)], debugDescription: \"Expected to decode Bool but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // illegal qualifiedCacheName
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCacheName\":false,\"version\":10}"
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.String, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"qualifiedCacheName\", intValue: nil)], debugDescription: \"Expected to decode String but found a number instead.\", underlyingError: nil))", "\(error)")
        }
        json = "{\"databaseId\":\"\(database.accessor.hashValue)\",\"isEager\":true,\"cacheName\":\"myCollection\",\"version\":10}"
        // No parentData
        decoder.userInfo[Database.parentDataKey] = nil
        do {
            reference = try decoder.decode(ReferenceManager<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch ReferenceManagerSerializationError.noParentData {}
    }
    
    func testGetReference() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        let childId = UUID()
        let child = Entity (cache: cache, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        // Nil
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        let _ = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        XCTAssertNil (reference.getReferenceData())
        // Creation with entity
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child)
        let _ = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        XCTAssertEqual (child.referenceData(), reference.getReferenceData()!)
        // Creation with reference Data
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        let _ = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        XCTAssertEqual (child.referenceData(), reference.getReferenceData()!)
    }
    
    func testWillUpdate() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        let childId = UUID()
        let child = Entity (cache: cache, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.willUpdate(newId: nil) { willUpdate in
            XCTAssertFalse (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.willUpdate(newId: nil) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.willUpdate(newId: nil) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.willUpdate(newId: child.id) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.willUpdate(newId: child.id) { willUpdate in
            XCTAssertFalse (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.willUpdate(newId: child.id) { willUpdate in
            XCTAssertFalse (willUpdate)
        }
        let newID = UUID()
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.willUpdate(newId: newID) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.willUpdate(newId: newID) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.willUpdate(newId: newID) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
    }
    
    public func testAddParentToBatch() {
        let logger = InMemoryLogger(level: .warning)
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        // No such parent
        let parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        let reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        let batch = EventuallyConsistentBatch()
        reference.addParentTo (batch: batch)
        batch.syncEntities() { entities in
            XCTAssertEqual (0, entities.count)
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|ReferenceManager<MyStruct, MyStruct>.addParentTo|lostData:noParent|cacheName=\(cache.qualifiedName);parentId=\(parentId.uuidString);parentVersion=10", entries[0].asTestString())
        }
        XCTAssertEqual (1, cache.onCacheCount())
        // Valid parent
        let parent = Entity<MyStruct>(cache: cache, id: parentId, version: 10, item: MyStruct(myInt: 10, myString: "10"))
        reference.sync() { referenceAttributes in } // addParentTo is not thread safe; is always called within reference.Queue; this simulates that protection
        reference.addParentTo (batch: batch)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entities[parentId] as! Entity<MyStruct> === parent)
        }
        reference.sync() { referenceAttributes in
            XCTAssertTrue (referenceAttributes.parent === parent)
        }
        XCTAssertEqual (0, cache.onCacheCount())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
        }
    }

    public func testSetEntity() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        var parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var batch = EventuallyConsistentBatch()
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        XCTAssertNil (reference.entityId())
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        let entity1Id = UUID()
        let entity1 = Entity (cache: cache, id: entity1Id, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        let entity2Id = UUID()
        let entity2 = Entity (cache: cache, id: entity2Id, version: 10, item: MyStruct (myInt: 30, myString: "30"))
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (reference.entityId()!.uuidString, entity1.id.uuidString)
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        batch = EventuallyConsistentBatch()
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (reference.entityId()!.uuidString, entity1.id.uuidString)
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        reference.set (entity: entity2, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity2)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (reference.entityId()!.uuidString, entity2.id.uuidString)
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        batch = EventuallyConsistentBatch()
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertNil (reference.entityId())
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Same again, with pending closure
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        batch = EventuallyConsistentBatch()
        var retrievedEntity: Entity<MyStruct>? = nil
        var waitFor = expectation(description: "waitFor1")
        var promiseResolver: (promise: Promise<Entity<MyStruct>?>, resolver: Resolver<Entity<MyStruct>?>) = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            retrievedEntity = entity
            waitFor.fulfill()
        }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertNil (retrievedEntity)
        waitFor = expectation(description: "waitFor2")
        retrievedEntity = nil
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            retrievedEntity = entity
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertTrue (retrievedEntity === entity1)
        waitFor = expectation(description: "waitFor3")
        retrievedEntity = nil
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            retrievedEntity = entity
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }

        batch = EventuallyConsistentBatch()
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertTrue (retrievedEntity === entity1)
        waitFor = expectation(description: "waitFor4")
        retrievedEntity = nil
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            retrievedEntity = entity
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        reference.set (entity: entity2, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity2)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertTrue (retrievedEntity === entity2)
        waitFor = expectation(description: "waitFor5")
        retrievedEntity = nil
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            retrievedEntity = entity
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        batch = EventuallyConsistentBatch()
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertNil (retrievedEntity)
        waitFor = expectation(description: "waitFor6")

        retrievedEntity = nil
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            retrievedEntity = entity
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === entity1.cache)
            XCTAssertTrue (reference.cache === entity2.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        XCTAssertNil (retrievedEntity)
    }
    
    public func testSetReferenceData() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        var parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var batch = EventuallyConsistentBatch()
        // nil -> nil
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        XCTAssertNil (reference.entityId())
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        // nil -> referenceData
        let entityId = UUID()
        let entity = Entity (cache: cache, id: entityId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        reference.set(referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (entity.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // referenceData -> same referenceData
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (entity.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // referenceData -> new referenceData
        let entityId2 = UUID()
        let entity2 = Entity (cache: cache, id: entityId2, version: 10, item: MyStruct (myInt: 30, myString: "30"))
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entity2.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (entity2.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // referenceData -> nil
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        XCTAssertNil (reference.entityId())
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // entity -> entity.referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (entity.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // entity -> new referenceData
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: entity2.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        XCTAssertEqual (entity2.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // entity -> nil referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
    }
    
    public func testSetReferenceDataIsEager() throws {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        var parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var batch = EventuallyConsistentBatch()
        // nil -> nil
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // nil -> referenceData
        let semaphore = DispatchSemaphore(value: 1)
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        let entityId = UUID(uuidString: "D8691B27-C99B-4CA9-BBBE-689AEDE5464B")!
        let entityData = "{\"id\":\"D8691B27-C99B-4CA9-BBBE-689AEDE5464B\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":20,\"myString\":\"20\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let entityReferenceData = ReferenceManagerData (databaseId: accessor.hashValue, cacheName: cache.name, id: entityId, version: 10)
        let _ = accessor.add(name: cache.name, id: entityId, data: entityData)
        var waitFor = expectation(description: "wait1")
        
        var promiseResolver: (promise: Promise<Entity<MyStruct>?>, resolver: Resolver<Entity<MyStruct>?>) = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
        }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        accessor.setPreFetch() { id in
            switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
            case .success:
                semaphore.signal()
            default:
                XCTFail ("Expected .success")
            }
        }
        reference.set(referenceData: entityReferenceData, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entityId.uuidString, referenceData.id.uuidString)
            default:
                XCTFail ("Expected .retrieving but got \(reference.state)")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (1, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        let entity = try reference.getSync()!
        // referenceData -> same referenceData
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entityReferenceData, batch: batch)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // referenceData -> new referenceData
        let entityId2 = UUID(uuidString: "45097C35-DF05-4C13-84CC-087E72BC2D0E")!
        let entityData2 = "{\"id\":\"45097C35-DF05-4C13-84CC-087E72BC2D0E\",\"schemaVersion\":5,\"created\":1524348993.021544,\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let entity2ReferenceData = ReferenceManagerData(databaseId: accessor.hashValue, cacheName: cache.name, id: entityId2, version: 10)
        let _ = accessor.add(name: cache.name, id: entityId2, data: entityData2)
        batch = EventuallyConsistentBatch()
        waitFor = expectation(description: "wait2")
        switch semaphore.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }

        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }


        reference.set(referenceData: entity2ReferenceData, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entityId2.uuidString, referenceData.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (1, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        // referenceData -> nil
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // entity -> entity.referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        let testReference = RetrieveControlledReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference = testReference
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // entity -> new referenceData
        let entityId3 = UUID()
        let entityData3 = "{\"id\":\"\(entityId3.uuidString)\",\"schemaVersion\":5,\"created\":1524348993.021544,\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let entity3ReferenceData = ReferenceManagerData(databaseId: accessor.hashValue, cacheName: cache.name, id: entityId3, version: 10)
        let _ = accessor.add(name: cache.name, id: entityId3, data: entityData3)

        batch = EventuallyConsistentBatch()
        waitFor = expectation(description: "wait2")
        let workQueue = DispatchQueue(label: "workQueue")
        workQueue.async() {
            reference.set (referenceData: entity3ReferenceData, batch: batch)
        }
        switch testReference.contentsReadyGroup.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail("Expected .success")
        }
        if let reference = testReference.contents {
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId3.uuidString)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entityId3.uuidString, referenceData.id.uuidString)
            default:
                XCTFail ("Expected .retrieving but got \(reference.state)")
            }
            XCTAssertTrue (reference.isEager)

            XCTAssertEqual (0, reference.pendingResolverCount)
        } else {
            XCTFail("Expected .contents")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        promiseResolver = Promise.pending()
        reference.appendResolver(promiseResolver.resolver)
        promiseResolver.promise.done() { entity in
            waitFor.fulfill()
            }.catch { error in
                XCTFail ("Expected success but got \(error)")
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId3.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // entity -> nil referenceData
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache === cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
    }

    public func testGetSync() throws {
        let logger = InMemoryLogger(level: .warning)
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        let childId = UUID()
        let childData = "{\"id\":\"\(childId.uuidString)\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":100,\"myString\":\"100\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let childReferenceData = ReferenceManagerData (qualifiedCacheName: database.qualifiedCacheName(cache.name), id: childId, version: 10)
        switch accessor.add(name: cache.name, id: childId, data: childData) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        let reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: childReferenceData)
        try XCTAssertEqual (childId.uuidString, reference.getSync()?.id.uuidString)
        logger.sync() { entities in
            XCTAssertEqual (0, entities.count)
        }
    }

    
    public func testGetSyncWithError() throws {
        let logger = InMemoryLogger(level: .warning)
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        let childId = UUID()
        let childData = "{\"id\":\"\(childId.uuidString)\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":100,\"myString\":\"100\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let childReferenceData = ReferenceManagerData (qualifiedCacheName: database.qualifiedCacheName(cache.name), id: childId, version: 10)
        switch accessor.add(name: cache.name, id: childId, data: childData) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        let reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: childReferenceData)
        accessor.setThrowError()
        try XCTAssertNil (reference.getSync())
        logger.sync() { entities in
            XCTAssertEqual (2, entities.count)
            XCTAssertEqual ("EMERGENCY|InMemoryAccessor.getSync|Database Error|databaseHashValue=\(accessor.hashValue);cache=myCollection;id=\(childId.uuidString);errorMessage=getError", entities[0].asTestString())
            XCTAssertEqual ("ERROR|ReferenceManager<MyStruct, MyStruct>.getSync|error|parentId=\(parentId.uuidString);entityCollection=\(cache.qualifiedName);entityId=\(childId.uuidString);message=getError", entities[1].asTestString())
        }
        print ("testGetSyncWithError1")
        sleep(1)
        print ("testGetSyncWithError2")
    }
    
    public func testAsync() {
        print ("testAsync0")
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        print ("testAsync1")
        // loaded nil
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        var wasNil = false
        print ("testAsync1.1")
        var waitFor = expectation(description: "wait1")
        firstly {
            reference.get()
        }.done { entity in
            wasNil = (entity == nil)
            print ("testAsync1.1.3")
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync1.1.4")
            waitFor.fulfill()
            print ("testAsync1.1.5")
        }
        print ("testAsync1.2")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync1.3")
        XCTAssert (wasNil)
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        print ("testAsync1.4")
        let batch = EventuallyConsistentBatch()
        // loaded not nil
        print ("testAsync2.0")
        let entity = cache.new (batch: batch, item: MyStruct (myInt: 20, myString: "20"))
        print ("testAsync2.1")
        reference.set(entity: entity, batch: batch)
        print ("testAsync2.2")
        var retrievedEntity: Entity<MyStruct>? = nil
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        print ("testAsync2.3")
        waitFor = expectation(description: "wait2")
        print ("testAsync2.4")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync2.4.1")
            retrievedEntity = entity
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync2.4.2")
            waitFor.fulfill()
            print ("testAsync2.4.3")
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync2.5")
        XCTAssertTrue (entity === retrievedEntity)
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        print ("testAsync2.6")
        // Decoded with valid reference
        print ("testAsync3.0")
        reference = ReferenceManager (parent: parentData, referenceData: entity.referenceData())
        retrievedEntity = nil
        reference.sync() { contents in
            switch contents.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
        }
        print ("testAsync3.1")
        waitFor = expectation(description: "wait3")
        print ("testAsync3.2")

        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync3.2.1")
            retrievedEntity = entity
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync3.2.2")
            waitFor.fulfill()
            print ("testAsync3.2.3")
        }
        print ("testAsync3.3")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync3.4")
        XCTAssertTrue (entity === retrievedEntity)
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        print ("testAsync3.5")
        // Decoded with invalid reference
        let invalidReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: UUID(), version: 1)
        print ("testAsync4.0")
        reference = ReferenceManager (parent: parentData, referenceData: invalidReferenceData)
        print ("testAsync4.1")
        reference.sync() { contents in
            switch contents.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
        }
        print ("testAsync4.2")
        waitFor = expectation(description: "wait4")
        print ("testAsync4.3")
        firstly {
            reference.get()
        }.done { entity in
            XCTFail ("Expected .error")
        }.catch { error in
            print ("testAsync4.3.1")
            XCTAssertEqual ("unknownUUID(\(invalidReferenceData.id.uuidString))", "\(error)")
        }.finally {
            print ("testAsync4.3.2")
            waitFor.fulfill()
            print ("testAsync4.3.3")
        }
        print ("testAsync4.4")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync4.5")
        var suspendSeconds: TimeInterval = 0.0
        reference.sync() { contents in
            switch contents.state {
            case .retrievalError(let suspendtime, let error):
                XCTAssertEqual ("unknownUUID(\(invalidReferenceData.id.uuidString))", "\(error)")
                let now = Date()
                XCTAssertTrue (suspendtime.timeIntervalSince1970 > (now + cache.database.referenceRetryInterval - 1.0).timeIntervalSince1970)
                XCTAssertTrue (suspendtime.timeIntervalSince1970 < (now + cache.database.referenceRetryInterval + 1.0).timeIntervalSince1970)
                suspendSeconds = suspendtime.timeIntervalSince1970
            default:
                XCTFail ("Expected .retrievalError")
            }
        }
        print ("testAsync4.6")
        // retrievalError during suspense period        
        waitFor = expectation(description: "wait5")
        print ("testAsync5.0")
        firstly {
            reference.get()
        }.done { entity in
                XCTFail ("Expected .error")
        }.catch { error in
                XCTAssertEqual ("unknownUUID(\(invalidReferenceData.id.uuidString))", "\(error)")
            print ("testAsync5.0.1")

        }.finally {
            print ("testAsync5.0.2")
            waitFor.fulfill()
            print ("testAsync5.0.3")
        }
        print ("testAsync5.1")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync5.2")
        reference.sync() { contents in
            switch contents.state {
            case .retrievalError(let suspendtime, let error):
                XCTAssertEqual ("unknownUUID(\(invalidReferenceData.id.uuidString))", "\(error)")
                XCTAssertEqual (suspendSeconds, suspendtime.timeIntervalSince1970)
            default:
                XCTFail ("Expected .retrievalError")
            }
        }
        // retrievalError after suspense period
        let oldTime = Date (timeIntervalSince1970: Date().timeIntervalSince1970 - 1000.0)
        let badId = UUID()
        reference.setState(state: .retrievalError(oldTime, AccessorError.unknownUUID (badId)))
        waitFor = expectation(description: "wait6")
        print ("testAsync6.0")
        firstly {
            reference.get()
        }.done { entity in
            XCTFail ("Expected .error")
        }.catch { error in
            print ("testAsync6.0.1")
            XCTAssertEqual ("unknownUUID(\(invalidReferenceData.id.uuidString))", "\(error)")
        }.finally {
            print ("testAsync6.0.2")
            waitFor.fulfill()
            print ("testAsync6.0.3")
        }
        print ("testAsync6.1")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync6.2")
        suspendSeconds = 0.0
        reference.sync() { contents in
            switch contents.state {
            case .retrievalError(let suspendtime, let error):
                XCTAssertEqual ("unknownUUID(\(invalidReferenceData.id.uuidString))", "\(error)")
                let now = Date()
                XCTAssertTrue (suspendtime.timeIntervalSince1970 > (now + cache.database.referenceRetryInterval - 1.0).timeIntervalSince1970)
                XCTAssertTrue (suspendtime.timeIntervalSince1970 < (now + cache.database.referenceRetryInterval + 1.0).timeIntervalSince1970)
                suspendSeconds = suspendtime.timeIntervalSince1970
            default:
                XCTFail ("Expected .retrievalError")
            }
        }
        print ("testAsync6.3")
        // retrievalError after suspense period; no subsequent retrieval error
        var persistentUUID = UUID()
        let creationDateString = try! jsonEncodedDate (date: Date())!
        let savedDateString = try! jsonEncodedDate (date: Date())!
        var json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: cache.name, id: persistentUUID, data: json.data(using: .utf8)!)
        print ("testAsync7.0")
        var persistentReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: persistentUUID, version: 10)
        print ("testAsync7.1")
        reference = ReferenceManager (parent: parentData, referenceData: persistentReferenceData)
        print ("testAsync7.2")
        reference.setState(state: .retrievalError(oldTime, AccessorError.unknownUUID (badId)))
        print ("testAsync7.3")
        waitFor = expectation(description: "wait7")
        print ("testAsync7.4")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync7.4.1")
            XCTAssertEqual (persistentUUID.uuidString, entity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync7.4.2")
            waitFor.fulfill()
            print ("testAsync7.4.3")
        }
        print ("testAsync7.5")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync7.6")
        suspendSeconds = 0.0
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        print ("testAsync7.7")
        // .retrieving with retrieval success
        var semaphore = DispatchSemaphore(value: 1)
        var preFetchCount = 0
        var prefetch: (UUID) -> () = { id in
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
        print ("testAsync8.0")
        accessor.setPreFetch (prefetch)
        print ("testAsync8.1")
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        print ("testAsync8.2")
        let _ = accessor.add(name: cache.name, id: persistentUUID, data: json.data(using: .utf8)!)
        print ("testAsync8.3")
        persistentReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: persistentUUID, version: 10)
        print ("testAsync8.4")
        reference = ReferenceManager (parent: parentData, referenceData: persistentReferenceData)
        print ("testAsync8.5")
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        print ("testAsync8.6")
        waitFor = expectation(description: "wait7a")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync8.6.1")
            XCTAssertEqual (persistentUUID.uuidString, entity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync8.6.2")
            waitFor.fulfill()
            print ("testAsync8.6.3")
        }
        print ("testAsync8.7")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (1, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync8.8")
        var waitFor2 = expectation(description: "wait7a")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync8.8.1")
            XCTAssertEqual (persistentUUID.uuidString, entity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync8.8.2")
            waitFor2.fulfill()
            print ("testAsync8.8.3")
        }
        print ("testAsync8.9")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (2, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync8.10")
        semaphore.signal()
        print ("testAsync8.11")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync8.12")
        // .retrieving with retrieval failure
        semaphore = DispatchSemaphore(value: 1)
        preFetchCount = 0
        prefetch = { id in
            if preFetchCount == 1 {
                switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
                case .success:
                    accessor.setThrowError()
                    break
                default:
                    XCTFail ("Expected Success")
                }
                semaphore.signal()
            }
            preFetchCount = preFetchCount + 1
            
        }
        print ("testAsync9.0")
        accessor.setPreFetch (prefetch)
        print ("testAsync9.1")
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: cache.name, id: persistentUUID, data: json.data(using: .utf8)!)
        persistentReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: persistentUUID, version: 10)
        print ("testAsync9.2")
        reference = ReferenceManager (parent: parentData, referenceData: persistentReferenceData)
        print ("testAsync9.3")
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        print ("testAsync9.4")
        waitFor = expectation(description: "wait8")
        firstly {
            reference.get()
        }.done { entity in
            XCTFail ("Expected .error")
        }.catch { error in
            print ("testAsync9.4.1")
            XCTAssertEqual ("getError", "\(error)")
        }.finally {
            print ("testAsync9.4.2")
            waitFor.fulfill()
            print ("testAsync9.4.3")
        }
        print ("testAsync9.5")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (1, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync9.6")
        waitFor2 = expectation(description: "wait8a")
        print ("testAsync9.7")
        firstly {
            reference.get()
        }.done { entity in
            XCTFail ("Expected .error")
        }.catch { error in
            print ("testAsync9.7.1")
            XCTAssertEqual ("getError", "\(error)")
        }.finally {
            print ("testAsync9.7.2")
            waitFor2.fulfill()
            print ("testAsync9.7.3")
        }
        print ("testAsync9.8")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (2, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync9.9")
        semaphore.signal()
        print ("testAsync9.10")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync9.11")
        // Obsolete callback set entity
        semaphore = DispatchSemaphore(value: 1)
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
            }
            preFetchCount = preFetchCount + 1
            
        }
        print ("testAsync10.0")
        accessor.setPreFetch (prefetch)
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        print ("testAsync10.1")
        let _ = accessor.add(name: cache.name, id: persistentUUID, data: json.data(using: .utf8)!)
        print ("testAsync10.2")
        persistentReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: persistentUUID, version: 10)
        print ("testAsync10.3")
        reference = ReferenceManager (parent: parentData, referenceData: persistentReferenceData)
        print ("testAsync10.4")
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        print ("testAsync10.5")
        waitFor = expectation(description: "wait9")

        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync10.5.1")
            XCTAssertEqual (entity!.id.uuidString, retrievedEntity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync10.5.2")
            waitFor.fulfill()
            print ("testAsync10.5.2")
        }
        print ("testAsync10.6")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (1, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        waitFor2 = expectation(description: "wait9a")
        print ("testAsync10.7")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync10.7.1")
            XCTAssertEqual (entity!.id.uuidString, retrievedEntity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync10.7.2")
            waitFor2.fulfill()
            print ("testAsync10.7.3")
        }
        print ("testAsync10.8")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (2, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync10.9")
        reference.set (entity: entity, batch: batch)
        print ("testAsync10.10")
        semaphore.signal()
        print ("testAsync10.11")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync10.12")
        // Obsolete callback set referenceData
        semaphore = DispatchSemaphore(value: 1)
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
            }
            preFetchCount = preFetchCount + 1
            
        }
        print ("testAsync11.0")
        accessor.setPreFetch (prefetch)
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: cache.name, id: persistentUUID, data: json.data(using: .utf8)!)
        print ("testAsync11.1")
        persistentReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: persistentUUID, version: 10)
        print ("testAsync11.2")
        reference = ReferenceManager (parent: parentData, referenceData: persistentReferenceData)
        print ("testAsync11.3")
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        print ("testAsync11.4")
        waitFor = expectation(description: "wait9")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync11.4.1")
            XCTAssertEqual (entity!.id.uuidString, retrievedEntity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync11.4.2")
            waitFor.fulfill()
            print ("testAsync11.4.3")
        }
        print ("testAsync11.5")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (1, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync11.6")
        waitFor2 = expectation(description: "wait9a")
        firstly {
            reference.get()
        }.done { entity in
            print ("testAsync11.6.1")
            XCTAssertEqual (entity!.id.uuidString, retrievedEntity!.id.uuidString)
        }.catch { error in
            XCTFail ("Expected success but got \(error)")
        }.finally {
            print ("testAsync11.6.2")
            waitFor2.fulfill()
            print ("testAsync11.6.3")
        }
        print ("testAsync11.7")
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (cache.qualifiedName, data.qualifiedCacheName)
                XCTAssertEqual (2, contents.pendingResolverCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        print ("testAsync11.8")
        reference.set (referenceData: entity.referenceData(), batch: batch)
        print ("testAsync11.9")
        semaphore.signal()
        print ("testAsync11.10")
        waitForExpectations(timeout: 10.0, handler: nil)
        print ("testAsync11.11")
    }
    
    public func testGet() throws {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        let creationDateString = try! jsonEncodedDate (date: Date())!
        let savedDateString = try! jsonEncodedDate (date: Date())!
        let entityId = UUID()
        let json = "{\"id\":\"\(entityId.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: cache.name, id: entityId, data: json.data(using: .utf8)!)
        let persistentReferenceData = ReferenceManagerData (databaseId: database.accessor.hashValue, cacheName: cache.name, id: entityId, version: 10)
        reference = ReferenceManager (parent: parentData, referenceData: persistentReferenceData)
        if let retrievedEntity = try reference.getSync() {
            XCTAssertEqual (entityId.uuidString, retrievedEntity.id.uuidString)
        } else {
            XCTFail ("Expected entity")
        }
    }
    
    public func testSetWithinEntity() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let structCollection = EntityCache<MyStruct> (database: database, name: "structCollection")
        let containerCollection = ContainerCollection (database: database, name: "containerCollection")
        var batch = EventuallyConsistentBatch()
        let containerEntity = containerCollection.new(batch: batch, myStruct: nil)
        var structEntity = structCollection.new(batch: batch, item: MyStruct (myInt: 10, myString: "10"))
        var waitFor = expectation(description: "wait1")
        // Updating persistent reference within parent 'sync'
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch containerEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch = EventuallyConsistentBatch()
        containerEntity.sync() { container in
            container.myStruct.set(entity: structEntity, batch: batch)
        }
        switch containerEntity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertNotNil (entities[containerEntity.id])
        }
        waitFor = expectation(description: "wait2")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch containerEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        // Updating persistent reference within parent 'async'
        structEntity = structCollection.new(batch: batch, item: MyStruct (myInt: 20, myString: "20"))
        waitFor = expectation(description: "wait3")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch containerEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch = EventuallyConsistentBatch()
        containerEntity.async() { container in
            container.myStruct.set(entity: structEntity, batch: batch)
        }
        switch containerEntity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertNotNil (entities[containerEntity.id])
        }
        waitFor = expectation(description: "wait4")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch containerEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        // Updating persistent reference within parent 'update'
        structEntity = structCollection.new(batch: batch, item: MyStruct (myInt: 20, myString: "20"))
        waitFor = expectation(description: "wait5")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch containerEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch = EventuallyConsistentBatch()
        containerEntity.update(batch: batch) { container in
            container.myStruct.set(entity: structEntity, batch: batch)
        }
        switch containerEntity.persistenceState {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertNotNil (entities[containerEntity.id])
        }
        waitFor = expectation(description: "wait6")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        switch containerEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.persistenceState {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }

    }

    public func testDereference() throws {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let cache = EntityCache<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parentData = EntityReferenceData<MyStruct> (cache: cache, id: parentId, version: 10)
        var reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: nil)
        var parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        // Loaded nil
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        // loaded entity
        let entity = newTestEntity(myInt: 10, myString: "10")
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, entity: entity)
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === entity.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertTrue (reference.cache! === entity.cache)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        // Decoded with refeference
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: entity.referenceData())
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertNil (reference.cache)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? ReferenceManager<MyStruct, MyStruct> === reference)
        }
        // Loaded after decoding
        parentId = UUID()
        parentData = EntityReferenceData<MyStruct> (cache: parent.cache, id: parentId, version: 10)
        reference = ReferenceManager<MyStruct, MyStruct> (parent: parentData, referenceData: entity.referenceData())
        parent = Entity (cache: cache, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let _ = try reference.getSync()!
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === entity)
            XCTAssertTrue (reference.parent! === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.cache! === entity.cache)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent! === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertTrue (reference.cache! === entity.cache)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingResolverCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (reference === references[0] as! ReferenceManager<MyStruct, MyStruct>)
        }
    }
    
    // Verify that when the Child of ReferenceManager is updated (i.e. its version is incremented)
    // That this does not produce a spurious lostData message in its parent
    func testUpdateLogging() throws {

        class ReferenceContainer : Codable {
            
            init (parentData: EntityReferenceData<ReferenceContainer>, entity: Entity<MyStruct>? = nil) {
                child = ReferenceManager<ReferenceContainer, MyStruct> (parent: parentData, entity: entity)
            }
            
            let child: ReferenceManager<ReferenceContainer, MyStruct>
            
        }
        
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let containerCache = EntityCache<ReferenceContainer> (database: database, name: "containers")
        let structCache = EntityCache<MyStruct> (database: database, name: "structs")
        var batch: EventuallyConsistentBatch? = EventuallyConsistentBatch()
        var structEntity1: Entity<MyStruct>? = structCache.new(batch: batch!, item: MyStruct(myInt: 10, myString: "10"))
        var containerEntity: Entity<ReferenceContainer>? = containerCache.new(batch: batch!) { parentData in
            return ReferenceContainer (parentData: parentData, entity: structEntity1)
        }
        let structId1 = structEntity1!.id
        let containerId = containerEntity!.id
        batch!.commitSync()
        structEntity1?.update(batch: batch!) { myStruct in
            myStruct.myInt = 11
            myStruct.myString = "11"
        }
        batch!.commitSync()
        structEntity1 = nil
        containerEntity = nil
        containerCache.waitWhileCached(id: containerId)
        structCache.waitWhileCached(id: structId1)
        XCTAssertFalse (containerCache.hasCached(id: containerId))
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
            for entry in entries {
                print (entry.asTestString())
            }
        }
        let structEntity2: Entity<MyStruct> = structCache.new(batch: batch!, item: MyStruct(myInt: 20, myString: "20"))
        batch!.commitSync()
        containerEntity = try containerCache.getSync(id: containerId)
        containerEntity!.sync() { container in
            container.child.set(entity: structEntity2, batch: batch!)
        }
        containerEntity = nil
        batch = nil
        containerCache.waitWhileCached(id: containerId)
        XCTAssertFalse (containerCache.hasCached(id: containerId))
        var entryCount = 0
        let timeout = Date().timeIntervalSince1970 + 10.0
        while entryCount < 1 && Date().timeIntervalSince1970 < timeout {
            logger.sync() { entries in
                entryCount = entries.count
            }
        }
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("ERROR|Entity<ReferenceContainer>.Type.deinit|lostData:itemModifiedBatchAbandoned|cacheName=\(database.accessor.hashValue).containers;entityId=\(containerId.uuidString)", entries[0].asTestString())
        }
    }
}



// function retrieve() waits until the thread which created the reference
// signals the semaphore
class RetrieveControlledReferenceManager<P: Codable, T: Codable> : ReferenceManager<P, T> {
    
    override init (parent: EntityReferenceData<P>, entity: Entity<T>?, isEager: Bool) {
        switch self.semaphore.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        contentsReadyGroup.enter()
        super.init (parent: parent, entity: entity, isEager: isEager)
    }
    
    override init (parent: EntityReferenceData<P>, referenceData: ReferenceManagerData?, isEager: Bool) {
        switch self.semaphore.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        contentsReadyGroup.enter()
        super.init (parent: parent, referenceData: referenceData, isEager: isEager)
    }
    
    public required init (from decoder: Decoder) throws {
        switch self.semaphore.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        contentsReadyGroup.enter()
        try super.init (from: decoder)
    }
    
    // Not Thread safe, must be called within queue
    override internal func retrievalGetHook() {
        contents = contents()
        contentsReadyGroup.leave()
        switch self.semaphore.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            self.semaphore.signal()
        default:
            XCTFail ("Expected .success")
        }
    }

    var contents: ReferenceManagerContents<P, T>? = nil
    
    // Not thread safe, intended for use when waiting on retrievalGetHook()

    internal let contentsReadyGroup = DispatchGroup()
    internal let semaphore = DispatchSemaphore (value: 1)
    
}
