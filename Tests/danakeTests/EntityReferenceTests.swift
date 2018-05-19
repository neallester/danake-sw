//
//  EntityReferenceTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/27/18.
//

import XCTest
@testable import danake

class EntityReferenceTests: XCTestCase {

    func testCreationEncodeDecode() throws {
        
        print ("0.a")
        let accessor = InMemoryAccessor()
        print ("0.b")
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        print ("0.c")
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        print ("0.d")
        var parentId = UUID()
        print ("0.e")
        let parentDataContainer = DataContainer()
        print ("0.f")
        var parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        print ("0.g")
        var parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        print ("0.h")
        parentDataContainer.data = parentData
        print ("0.i")
        let childId = UUID()
        print ("0.j")
        let child = Entity (collection: collection, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        print ("0.k")
        let encoder = JSONEncoder()
        print ("0.l")
        let decoder = JSONDecoder()
        
        print ("1")
        
        decoder.userInfo[Database.parentDataKey] = parentDataContainer
        // not isEager
        // Creation with entity
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === child)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        
        print ("2")
        
        // Creation with nil entity
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        
        print ("3")
        
        // Creation with referenceData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, child.id.uuidString)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        
        print ("4")
        
        // Creation with nil referenceData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        
        print ("5")
        
        // isEager
        // Creation with entity
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child, isEager: true)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === child)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        
        print ("6")
        
        // Creation with nil entity
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        
        print ("7")
        
        // Creation with referenceData for a cached object
        var waitFor = expectation(description: "wait1")
        var testReference = RetrieveControlledEntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData(), isEager: true)
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
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        
        print ("8")
        
        // Creation with referenceData for a uncached object
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        let persistentChildId1 = UUID()
        let persistentChildData1 = "{\"id\":\"\(persistentChildId1.uuidString)\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":100,\"myString\":\"100\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        switch accessor.add(name: collection.name, id: persistentChildId1, data: persistentChildData1) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        waitFor = expectation(description: "wait1a")
        testReference = RetrieveControlledEntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: EntityReferenceSerializationData(databaseId: accessor.hashValue(), collectionName: collection.name, id: persistentChildId1, version: 10), isEager: true)
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
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == persistentChildId1.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
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
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        
        print ("9")
        
        // Creation with nil referenceData
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: nil, isEager: true)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        
        print ("10")
        
        // Decoding
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        var json = "{\"isEager\":false,\"isNil\":true}"
        #if os(Linux)
            XCTAssertTrue(json.contains("\"isEager\":false"))
            XCTAssertTrue(json.contains("\"isNil\":true"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        
        print ("11")
        
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        json = "{\"isEager\":true,\"isNil\":true}"
        #if os(Linux)
            XCTAssertTrue(json.contains("\"isEager\":true"))
            XCTAssertTrue(json.contains("\"isNil\":true"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        
        print ("12")
        
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        
        print ("13")
        
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.sync() { reference in
            XCTAssertTrue (child === reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (child.collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        
        print ("14")
        
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":false,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10}"
        #if os(Linux)
            XCTAssertTrue (json.contains("\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\""))
            XCTAssertTrue (json.contains("\"id\":\"\(child.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"isEager\":false"))
            XCTAssertTrue (json.contains("\"version\":10"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        
        print ("15")
        
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        
        print ("16")
        
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child, isEager: true)
        reference.sync() { reference in
            XCTAssertTrue (child === reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (child.collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        
        print ("17")
        
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10}"
        waitFor = expectation(description: "wait1")
        testReference = try decoder.decode(RetrieveControlledEntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
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
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
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
        reference.appendClosure() { retrievalResult in
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        
        print ("18")

        #if os(Linux)
            XCTAssertTrue (json.contains("\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\""))
            XCTAssertTrue (json.contains("\"id\":\"\(child.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"isEager\":true"))
            XCTAssertTrue (json.contains("\"version\":10"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        // Creation with EntityReferenceSerializationData
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        json = "{\"isEager\":false,\"isNil\":true}"
        #if os(Linux)
            XCTAssertTrue (json.contains("\"isEager\":false"))
            XCTAssertTrue (json.contains("\"isNil\":true"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif
        
        print ("19")
        
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        
        print ("20")

        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: nil, isEager: true)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        json = "{\"isEager\":true,\"isNil\":true}"
        #if os(Linux)
            XCTAssertTrue (json.contains("\"isEager\":true"))
            XCTAssertTrue (json.contains("\"isNil\":true"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
        #endif

        print ("21")

        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }

        print ("22")

        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData!)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":false,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10}"
        #if os(Linux)
            XCTAssertTrue (json.contains("\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\""))
            XCTAssertTrue (json.contains("\"id\":\"\(child.id.uuidString)\""))
            XCTAssertTrue (json.contains("\"isEager\":false"))
            XCTAssertTrue (json.contains("\"version\":10"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        
        print ("23")

        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        let persistentChildId2 = UUID()
        let persistentChildData2 = "{\"id\":\"\(persistentChildId2.uuidString)\",\"schemaVersion\":5,\"created\":1524347199.410666,\"item\":{\"myInt\":200,\"myString\":\"200\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        switch accessor.add(name: collection.name, id: persistentChildId2, data: persistentChildData2) {
        case .ok:
            break
        default:
            XCTFail("Expected .ok")
        }
        json = "{\"id\":\"\(persistentChildId2.uuidString)\",\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10}"
        waitFor = expectation(description: "wait3")
        
        print ("24")
        
        testReference = try decoder.decode(RetrieveControlledEntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
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
            XCTAssertTrue (reference.collection === child.collection)
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
        reference.appendClosure() { RetrievalResult in
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == persistentChildId2.uuidString)
            XCTAssertTrue(reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
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
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        #if os(Linux)
            XCTAssertTrue (json.contains("\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\""))
            XCTAssertTrue (json.contains("\"id\":\"\(persistentChildId2.uuidString)\""))
            XCTAssertTrue (json.contains("\"isEager\":true"))
            XCTAssertTrue (json.contains("\"version\":10"))
        #else
            try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        #endif
        
        print ("25")

        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        parentDataContainer.data = parentData
        // Decoding with isNil:false
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10,\"isNil\":false}"
        waitFor = expectation(description: "wait1")
        testReference = try decoder.decode(RetrieveControlledEntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
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
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
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
        reference.appendClosure() { retrievalResult in
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertTrue (reference.parent === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
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
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        
        print ("26")
        
        // Decoding errors
        // IsNil Present but False
        json = "{\"isEager\":false,\"isNil\":false}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"qualifiedCollectionName\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"qualifiedCollectionName\\\", intValue: nil) (\\\"qualifiedCollectionName\\\").\", underlyingError: nil))", "\(error)")
        }
        // No qualifiedCollectionName
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"qualifiedCollectionName\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"qualifiedCollectionName\\\", intValue: nil) (\\\"qualifiedCollectionName\\\").\", underlyingError: nil))", "\(error)")
        }
        // No id
        json = "{\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal id
        json = "{\"databaseId\":\"\",\"id\":\"AAA\",\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":10,\"isNil\":false}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("illegalId(\"AAA\")", "\(error)")
        }
        // isEager missing
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"collectionName\":\"myCollection\",\"version\":10,\"isNil\":false}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"isEager\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"isEager\\\", intValue: nil) (\\\"isEager\\\").\", underlyingError: nil))", "\(error)")
        }
        // illegal isEager
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":\"what?\",\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Bool, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"isEager\", intValue: nil)], debugDescription: \"Expected to decode Bool but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // illegal qualifiedCollectionName
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCollectionName\":false,\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.String, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"qualifiedCollectionName\", intValue: nil)], debugDescription: \"Expected to decode String but found a number instead.\", underlyingError: nil))", "\(error)")
        }
        // Missing Version
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\"}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"version\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"version\\\", intValue: nil) (\\\"version\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Version
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"qualifiedCollectionName\":\"\(database.accessor.hashValue()).myCollection\",\"version\":\"10\"}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Int, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"version\", intValue: nil)], debugDescription: \"Expected to decode Int but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        // No parentData
        decoder.userInfo[Database.parentDataKey] = nil
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch EntityReferenceSerializationError.noParentData {}
    }
    
    func testGetReference() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        let childId = UUID()
        let child = Entity (collection: collection, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        // Nil
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        XCTAssertNil (reference.getReference())
        // Creation with entity
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child)
        XCTAssertEqual (child.referenceData(), reference.getReference()!)
        // Creation with reference Data
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        XCTAssertEqual (child.referenceData(), reference.getReference()!)
    }
    
    func testWillUpdate() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        let childId = UUID()
        let child = Entity (collection: collection, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.willUpdate(newId: nil) { willUpdate in
            XCTAssertFalse (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.willUpdate(newId: nil) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.willUpdate(newId: nil) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.willUpdate(newId: child.id) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.willUpdate(newId: child.id) { willUpdate in
            XCTAssertFalse (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.willUpdate(newId: child.id) { willUpdate in
            XCTAssertFalse (willUpdate)
        }
        let newID = UUID()
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.willUpdate(newId: newID) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: child)
        reference.willUpdate(newId: newID) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData())
        reference.willUpdate(newId: newID) { willUpdate in
            XCTAssertTrue (willUpdate)
        }
    }
    
    public func testAddParentToBatch() {
        let logger = InMemoryLogger(level: .warning)
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        // No such parent
        let parentData = EntityReferenceData<MyStruct> (collection: collection, id: parentId, version: 10)
        let reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        var batch = EventuallyConsistentBatch()
        reference.addParentTo (batch: batch)
        batch.syncEntities() { entities in
            XCTAssertEqual (0, entities.count)
        }
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            XCTAssertEqual ("WARNING|PersistentCollection<MyStruct>.get|Unknown id|databaseHashValue=\(accessor.hashValue());collection=myCollection;id=\(parentId.uuidString)", entries[0].asTestString())
            XCTAssertEqual("ERROR|EntityReference<MyStruct, MyStruct>.retrieveParent|noParent|collectionName=myCollection;parentId=\(parentId.uuidString);parentVersion=10;errorMessage=ok(nil)", entries[1].asTestString())
        }
        // Valid parent
        let creationDateString = try! jsonEncodedDate (date: Date())!
        let savedDateString = try! jsonEncodedDate (date: Date())!
        let json = "{\"id\":\"\(parentId.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: parentId, data: json.data(using: .utf8)!)
        reference.addParentTo (batch: batch)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entities[parentId]!.referenceData().id.uuidString == parentId.uuidString)
            (entities[parentId] as! Entity<MyStruct>).referenceContainers() { references in
                XCTAssertEqual (1, references.count)
                XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
            }
        }
        reference.sync() { referenceAttributes in
            let parent = referenceAttributes.parent as! Entity<MyStruct>
            switch parent.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
        }

        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
        }
        // With cached values
        batch = EventuallyConsistentBatch()
        reference.addParentTo (batch: batch)
        batch.syncEntities() { entities in
            XCTAssertEqual (1, entities.count)
            XCTAssertTrue (entities[parentId]!.referenceData().id.uuidString == parentId.uuidString)
        }
        reference.sync() { referenceAttributes in
            let parent = referenceAttributes.parent as! Entity<MyStruct>
            switch parent.getPersistenceState() {
            case .dirty:
                break
            default:
                XCTFail ("Expected .dirty")
            }
        }
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
        }
    }

    public func testSetEntity() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        var batch = EventuallyConsistentBatch()
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        XCTAssertNil (reference.entityId())
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        let entity1Id = UUID()
        let entity1 = Entity (collection: collection, id: entity1Id, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        let entity2Id = UUID()
        let entity2 = Entity (collection: collection, id: entity2Id, version: 10, item: MyStruct (myInt: 30, myString: "30"))
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === entity1.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (reference.entityId()!.uuidString, entity1.id.uuidString)
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
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
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (reference.entityId()!.uuidString, entity1.id.uuidString)
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
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
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (reference.entityId()!.uuidString, entity2.id.uuidString)
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
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
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertNil (reference.entityId())
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
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
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // Same again, with pending closure
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        batch = EventuallyConsistentBatch()
        var retrievalResult: RetrievalResult<Entity<MyStruct>>? = nil
        var waitFor = expectation(description: "waitFor1")
        let closure: (RetrievalResult<Entity<MyStruct>>) -> () = { result in
            retrievalResult = result
            waitFor.fulfill()
        }
        reference.appendClosure(closure)
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch retrievalResult! {
        case .ok (let retrievedEntity):
            XCTAssertNil (retrievedEntity)
        default:
            XCTFail ("Expected .ok")
        }
        waitFor = expectation(description: "waitFor2")
        retrievalResult = nil
        reference.appendClosure(closure)
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === entity1.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch retrievalResult! {
        case .ok (let retrievedEntity):
            XCTAssertTrue (retrievedEntity === entity1)
        default:
            XCTFail ("Expected .ok")
        }
        waitFor = expectation(description: "waitFor3")
        retrievalResult = nil
        reference.appendClosure(closure)
        batch = EventuallyConsistentBatch()
        reference.set (entity: entity1, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity1)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch retrievalResult! {
        case .ok (let retrievedEntity):
            XCTAssertTrue (retrievedEntity === entity1)
        default:
            XCTFail ("Expected .ok")
        }
        waitFor = expectation(description: "waitFor4")
        retrievalResult = nil
        reference.appendClosure(closure)
        reference.set (entity: entity2, batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity2)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch retrievalResult! {
        case .ok (let retrievedEntity):
            XCTAssertTrue (retrievedEntity === entity2)
        default:
            XCTFail ("Expected .ok")
        }
        waitFor = expectation(description: "waitFor5")
        retrievalResult = nil
        reference.appendClosure(closure)
        batch = EventuallyConsistentBatch()
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch retrievalResult! {
        case .ok (let retrievedEntity):
            XCTAssertNil (retrievedEntity)
        default:
            XCTFail ("Expected .ok")
        }
        waitFor = expectation(description: "waitFor6")
        retrievalResult = nil
        reference.appendClosure(closure)
        reference.set (entity: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === entity1.collection)
            XCTAssertTrue (reference.collection === entity2.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertTrue (entities[parent.id] as! Entity<MyStruct> === parent)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        waitForExpectations(timeout: 10, handler: nil)
        switch retrievalResult! {
        case .ok (let retrievedEntity):
            XCTAssertNil (retrievedEntity)
        default:
            XCTFail ("Expected .ok")
        }
    }
    
    public func testSetReferenceData() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        var batch = EventuallyConsistentBatch()
        // nil -> nil
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
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
        let entity = Entity (collection: collection, id: entityId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        reference.set(referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (entity.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // referenceData -> same referenceData
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (entity.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // referenceData -> new referenceData
        let entityId2 = UUID()
        let entity2 = Entity (collection: collection, id: entityId2, version: 10, item: MyStruct (myInt: 30, myString: "30"))
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entity2.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (entity2.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // referenceData -> nil
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        XCTAssertNil (reference.entityId())
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // entity -> entity.referenceData
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (entity.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // entity -> new referenceData
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: entity2.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        XCTAssertEqual (entity2.id.uuidString, reference.entityId()?.uuidString)
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // entity -> nil referenceData
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
    }
    
    public func testSetReferenceDataIsEager() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        var batch = EventuallyConsistentBatch()
        // nil -> nil
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
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
        let entityReferenceData = EntityReferenceSerializationData (databaseId: accessor.hashValue(), collectionName: collection.name, id: entityId, version: 10)
        let _ = accessor.add(name: collection.name, id: entityId, data: entityData)
        var waitFor = expectation(description: "wait1")
        reference.appendClosure() { result in
            waitFor.fulfill()
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
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entityId.uuidString, referenceData.id.uuidString)
            default:
                XCTFail ("Expected .retrieving but got \(reference.state)")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (2, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        let entity = reference.get().item()!
        // referenceData -> same referenceData
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entityReferenceData, batch: batch)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // referenceData -> new referenceData
        let entityId2 = UUID(uuidString: "45097C35-DF05-4C13-84CC-087E72BC2D0E")!
        let entityData2 = "{\"id\":\"45097C35-DF05-4C13-84CC-087E72BC2D0E\",\"schemaVersion\":5,\"created\":1524348993.021544,\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let entity2ReferenceData = EntityReferenceSerializationData(databaseId: accessor.hashValue(), collectionName: collection.name, id: entityId2, version: 10)
        let _ = accessor.add(name: collection.name, id: entityId2, data: entityData2)
        batch = EventuallyConsistentBatch()
        waitFor = expectation(description: "wait2")
        switch semaphore.wait(timeout: DispatchTime.now() + 10) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        reference.set(referenceData: entity2ReferenceData, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entityId2.uuidString, referenceData.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (2, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        // referenceData -> nil
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // entity -> entity.referenceData
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        let testReference = RetrieveControlledEntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        reference = testReference
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // entity -> new referenceData
        let entityId3 = UUID()
        let entityData3 = "{\"id\":\"\(entityId3.uuidString)\",\"schemaVersion\":5,\"created\":1524348993.021544,\"item\":{\"myInt\":30,\"myString\":\"30\"},\"persistenceState\":\"new\",\"version\":10}".data(using: .utf8)!
        let entity3ReferenceData = EntityReferenceSerializationData(databaseId: accessor.hashValue(), collectionName: collection.name, id: entityId3, version: 10)
        let _ = accessor.add(name: collection.name, id: entityId3, data: entityData3)

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
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entityId3.uuidString, referenceData.id.uuidString)
            default:
                XCTFail ("Expected .retrieving but got \(reference.state)")
            }
            XCTAssertTrue (reference.isEager)

            XCTAssertEqual (1, reference.pendingEntityClosureCount)
        } else {
            XCTFail("Expected .contents")
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        testReference.semaphore.signal()
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertEqual (reference.entity!.id.uuidString, entityId3.uuidString)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
        // entity -> nil referenceData
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
        reference.set(entity: entity, batch: batch)
        batch = EventuallyConsistentBatch()
        reference.set (referenceData: nil, batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (references[0] as? EntityReference<MyStruct, MyStruct> === reference)
        }
    }
    
    public func testAsync() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
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
        var waitFor = expectation(description: "wait1")
        reference.async() { result in
            switch result {
            case .ok (let entity):
                wasNil = (entity == nil)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        XCTAssert (wasNil)
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        let batch = EventuallyConsistentBatch()
        // loaded not nil
        let entity = collection.new (batch: batch, item: MyStruct (myInt: 20, myString: "20"))
        reference.set(entity: entity, batch: batch)
        var retrievedEntity: Entity<MyStruct>? = nil
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        waitFor = expectation(description: "wait2")
        reference.async() { result in
            switch result {
            case .ok (let entity):
                retrievedEntity = entity
                waitFor.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        XCTAssertTrue (entity === retrievedEntity)
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        // Decoded with valid reference
        reference = EntityReference (parent: parentData, referenceData: entity.referenceData())
        retrievedEntity = nil
        reference.sync() { contents in
            switch contents.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
        }
        waitFor = expectation(description: "wait3")
        reference.async() { result in
            switch result {
            case .ok (let entity):
                retrievedEntity = entity
                waitFor.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        XCTAssertTrue (entity === retrievedEntity)
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
        // Decoded with invalid reference
        let invalidReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: UUID(), version: 1)
        reference = EntityReference (parent: parentData, referenceData: invalidReferenceData)
        reference.sync() { contents in
            switch contents.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
        }
        waitFor = expectation(description: "wait4")
        reference.async() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("EntityReference<MyStruct, MyStruct>: Unknown id \(invalidReferenceData.id.uuidString)", errorMessage)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        var suspendSeconds: TimeInterval = 0.0
        reference.sync() { contents in
            switch contents.state {
            case .retrievalError(let suspendtime, let errorMessage):
                XCTAssertEqual ("EntityReference<MyStruct, MyStruct>: Unknown id \(invalidReferenceData.id.uuidString)", errorMessage)
                let now = Date()
                XCTAssertTrue (suspendtime.timeIntervalSince1970 > (now + collection.database.referenceRetryInterval - 1.0).timeIntervalSince1970)
                XCTAssertTrue (suspendtime.timeIntervalSince1970 < (now + collection.database.referenceRetryInterval + 1.0).timeIntervalSince1970)
                suspendSeconds = suspendtime.timeIntervalSince1970
            default:
                XCTFail ("Expected .retrievalError")
            }
        }
        // retrievalError during suspense period        
        waitFor = expectation(description: "wait5")
        reference.async() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("EntityReference<MyStruct, MyStruct>: Unknown id \(invalidReferenceData.id.uuidString)", errorMessage)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        reference.sync() { contents in
            switch contents.state {
            case .retrievalError(let suspendtime, let errorMessage):
                XCTAssertEqual ("EntityReference<MyStruct, MyStruct>: Unknown id \(invalidReferenceData.id.uuidString)", errorMessage)
                XCTAssertEqual (suspendSeconds, suspendtime.timeIntervalSince1970)
            default:
                XCTFail ("Expected .retrievalError")
            }
        }
        // retrievalError after suspense period
        let oldTime = Date (timeIntervalSince1970: Date().timeIntervalSince1970 - 1000.0)
        reference.setState(state: .retrievalError(oldTime, "Test Error"))
        waitFor = expectation(description: "wait6")
        reference.async() { result in
            switch result {
            case .error(let errorMessage):
                XCTAssertEqual ("EntityReference<MyStruct, MyStruct>: Unknown id \(invalidReferenceData.id.uuidString)", errorMessage)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        suspendSeconds = 0.0
        reference.sync() { contents in
            switch contents.state {
            case .retrievalError(let suspendtime, let errorMessage):
                XCTAssertEqual ("EntityReference<MyStruct, MyStruct>: Unknown id \(invalidReferenceData.id.uuidString)", errorMessage)
                let now = Date()
                XCTAssertTrue (suspendtime.timeIntervalSince1970 > (now + collection.database.referenceRetryInterval - 1.0).timeIntervalSince1970)
                XCTAssertTrue (suspendtime.timeIntervalSince1970 < (now + collection.database.referenceRetryInterval + 1.0).timeIntervalSince1970)
                suspendSeconds = suspendtime.timeIntervalSince1970
            default:
                XCTFail ("Expected .retrievalError")
            }
        }
        // retrievalError after suspense period; no subsequent retrieval error
        var persistentUUID = UUID()
        let creationDateString = try! jsonEncodedDate (date: Date())!
        let savedDateString = try! jsonEncodedDate (date: Date())!
        var json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: persistentUUID, data: json.data(using: .utf8)!)
        var persistentReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: persistentUUID, version: 10)
        reference = EntityReference (parent: parentData, referenceData: persistentReferenceData)
        reference.setState(state: .retrievalError(oldTime, "Test Error"))
        waitFor = expectation(description: "wait7")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (persistentUUID.uuidString, retrievedEntity!.id.uuidString)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
        suspendSeconds = 0.0
        reference.sync() { contents in
            switch contents.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
        }
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
        accessor.setPreFetch (prefetch)
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: persistentUUID, data: json.data(using: .utf8)!)
        persistentReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: persistentUUID, version: 10)
        reference = EntityReference (parent: parentData, referenceData: persistentReferenceData)
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        
        waitFor = expectation(description: "wait7")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (persistentUUID.uuidString, retrievedEntity!.id.uuidString)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (1, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        var waitFor2 = expectation(description: "wait7a")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (persistentUUID.uuidString, retrievedEntity!.id.uuidString)
                waitFor2.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (2, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }

        semaphore.signal()
        waitForExpectations(timeout: 10.0, handler: nil)
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
        accessor.setPreFetch (prefetch)
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: persistentUUID, data: json.data(using: .utf8)!)
        persistentReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: persistentUUID, version: 10)
        reference = EntityReference (parent: parentData, referenceData: persistentReferenceData)
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        
        waitFor = expectation(description: "wait8")
        reference.async() { result in
            switch result {
            case .error (let errorMessage):
                XCTAssertEqual ("getError", errorMessage)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (1, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        waitFor2 = expectation(description: "wait8a")
        reference.async() { result in
            switch result {
            case .error (let errorMessage):
                XCTAssertEqual ("getError", errorMessage)
                waitFor2.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (2, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        semaphore.signal()
        waitForExpectations(timeout: 10.0, handler: nil)
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
        accessor.setPreFetch (prefetch)
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: persistentUUID, data: json.data(using: .utf8)!)
        persistentReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: persistentUUID, version: 10)
        reference = EntityReference (parent: parentData, referenceData: persistentReferenceData)
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        waitFor = expectation(description: "wait9")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (entity.id.uuidString, retrievedEntity!.id.uuidString)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (1, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        waitFor2 = expectation(description: "wait9a")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (entity.id.uuidString, retrievedEntity!.id.uuidString)
                waitFor2.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (2, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        reference.set (entity: entity, batch: batch)
        semaphore.signal()
        waitForExpectations(timeout: 10.0, handler: nil)
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
        accessor.setPreFetch (prefetch)
        persistentUUID = UUID()
        json = "{\"id\":\"\(persistentUUID.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: persistentUUID, data: json.data(using: .utf8)!)
        persistentReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: persistentUUID, version: 10)
        reference = EntityReference (parent: parentData, referenceData: persistentReferenceData)
        switch semaphore.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail ("Expected .success")
        }
        waitFor = expectation(description: "wait9")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (entity.id.uuidString, retrievedEntity!.id.uuidString)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (1, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        waitFor2 = expectation(description: "wait9a")
        reference.async() { result in
            switch result {
            case .ok(let retrievedEntity):
                XCTAssertEqual (entity.id.uuidString, retrievedEntity!.id.uuidString)
                waitFor2.fulfill()
            default:
                XCTFail ("Expected .ok")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (collection.qualifiedName, data.qualifiedCollectionName)
                XCTAssertEqual (2, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        reference.set (referenceData: entity.referenceData(), batch: batch)
        semaphore.signal()
        waitForExpectations(timeout: 10.0, handler: nil)
    }
    
    public func testGet() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        let creationDateString = try! jsonEncodedDate (date: Date())!
        let savedDateString = try! jsonEncodedDate (date: Date())!
        let entityId = UUID()
        let json = "{\"id\":\"\(entityId.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString),\"saved\":\(savedDateString),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let _ = accessor.add(name: collection.name, id: entityId, data: json.data(using: .utf8)!)
        let persistentReferenceData = EntityReferenceSerializationData (databaseId: database.getAccessor().hashValue(), collectionName: collection.name, id: entityId, version: 10)
        reference = EntityReference (parent: parentData, referenceData: persistentReferenceData)
        switch reference.get() {
        case .ok(let retrievedEntity):
            XCTAssertEqual (entityId.uuidString, retrievedEntity!.id.uuidString)
        default:
            XCTFail ("Expected .ok")
        }
    }
    
    public func testSetWithinEntity() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let structCollection = PersistentCollection<MyStruct> (database: database, name: "structCollection")
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
        switch containerEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch = EventuallyConsistentBatch()
        containerEntity.sync() { container in
            container.myStruct.set(entity: structEntity, batch: batch)
        }
        switch containerEntity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        switch structEntity.getPersistenceState() {
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
        switch containerEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.getPersistenceState() {
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
        switch containerEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch = EventuallyConsistentBatch()
        containerEntity.async() { container in
            container.myStruct.set(entity: structEntity, batch: batch)
        }
        switch containerEntity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        switch structEntity.getPersistenceState() {
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
        switch containerEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.getPersistenceState() {
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
        switch containerEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        batch = EventuallyConsistentBatch()
        containerEntity.update(batch: batch) { container in
            container.myStruct.set(entity: structEntity, batch: batch)
        }
        switch containerEntity.getPersistenceState() {
        case .dirty:
            break
        default:
            XCTFail ("Expected .dirty")
        }
        switch structEntity.getPersistenceState() {
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
        switch containerEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }
        switch structEntity.getPersistenceState() {
        case .persistent:
            break
        default:
            XCTFail ("Expected .persistent")
        }

    }

    public func testDereference() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<MyStruct> (database: database, name: "myCollection")
        var parentId = UUID()
        var parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        var parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
        // Loaded nil
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        // loaded entity
        let entity = newTestEntity(myInt: 10, myString: "10")
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: entity)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity === entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === entity.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertTrue (reference.collection! === entity.collection)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        // Decoded with refeference
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: entity.referenceData())
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .decoded:
                break
            default:
                XCTFail ("Expected .decoded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertNil (reference.collection)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (0, references.count)
        }
        // Loaded after decoding
        parentId = UUID()
        parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: entity.referenceData())
        let _ = reference.get().item()!
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === entity)
            XCTAssertTrue (reference.parent! === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertNil (reference.referenceData)
            XCTAssertTrue (reference.collection! === entity.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        reference.dereference()
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent! === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id, entity.id)
            XCTAssertTrue (reference.collection! === entity.collection)
            switch reference.state {
            case .dereferenced:
                break
            default:
                XCTFail ("Expected .dereferenced")
            }
            XCTAssertFalse (reference.isEager)
            XCTAssertEqual (0, reference.pendingEntityClosureCount)
        }
        parent.referenceContainers() { references in
            XCTAssertEqual (1, references.count)
            XCTAssertTrue (reference === references[0] as! EntityReference<MyStruct, MyStruct>)
        }
    }
    
}

// function retrieve() waits until the thread which created the reference
// signals the semaphore
class RetrieveControlledEntityReference<P: Codable, T: Codable> : EntityReference<P, T> {
    
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
    
    override init (parent: EntityReferenceData<P>, referenceData: EntityReferenceSerializationData?, isEager: Bool) {
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

    var contents: EntityReferenceContents<P, T>? = nil
    
    // Not thread safe, intended for use when waiting on retrievalGetHook()

    internal let contentsReadyGroup = DispatchGroup()
    internal let semaphore = DispatchSemaphore (value: 1)
    
}
