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
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
        let childId = UUID()
        let child = Entity (collection: collection, id: childId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        let parentDataContainer = DataContainer()
        parentDataContainer.data = parentData
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
        // Creation with nil entity
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
        // Creation with nil entity
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
        // Creation with referenceData
        var waitFor = expectation(description: "wait1")
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData(), isEager: true)
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, child.id.uuidString)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        }
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        // Creation with nil referenceData
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
        // Decoding
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: nil)
        var json = "{\"isEager\":false,\"isNil\":true}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
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
        json = "{\"isEager\":true,\"isNil\":true}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":false,\"collectionName\":\"myCollection\",\"version\":10}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        waitFor = expectation(description: "wait1")
        decoder.userInfo [Database.initialClosureKey] = ClosureContainer<MyStruct>() { result in
            waitFor.fulfill()
        }
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
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
        }
        waitForExpectations(timeout: 10, handler: nil)
        decoder.userInfo [Database.initialClosureKey] = nil
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        // Creation with EntityReferenceSerializationData
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
        json = "{\"isEager\":false,\"isNil\":true}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
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
        json = "{\"isEager\":true,\"isNil\":true}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8))
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":false,\"collectionName\":\"myCollection\",\"version\":10}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
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
        waitFor = expectation(description: "wait2")
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData(), isEager: true)
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData!)
            XCTAssertTrue (reference.collection === child.collection)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        }
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        try XCTAssertEqual (json, String (data: encoder.encode(reference), encoding: .utf8)!)
        waitFor = expectation(description: "wait3")
        decoder.userInfo [Database.initialClosureKey] = ClosureContainer<MyStruct>() { result in
            waitFor.fulfill()
        }
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (reference.collection === child.collection)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        }
        waitForExpectations(timeout: 10, handler: nil)
        decoder.userInfo [Database.initialClosureKey] = nil
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        // Decoding with isNil:false
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10,\"isNil\":false}"
        waitFor = expectation(description: "wait4")
        decoder.userInfo [Database.initialClosureKey] = ClosureContainer<MyStruct>() { result in
            waitFor.fulfill()
        }
        reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (reference.collection === child.collection)
            switch reference.state {
            case .retrieving (let referenceData):
                XCTAssertEqual (referenceData.id.uuidString, reference.referenceData?.id.uuidString)
            default:
                XCTFail ("Expected .retrieving")
            }
            XCTAssertTrue (reference.isEager)
        }
        waitForExpectations(timeout: 10, handler: nil)
        decoder.userInfo [Database.initialClosureKey] = nil
        reference.sync() { reference in
            XCTAssertTrue (reference.entity!.id.uuidString == child.id.uuidString)
            XCTAssertNil (reference.parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (child.referenceData(), reference.referenceData)
            XCTAssertTrue (collection === reference.collection)
            switch reference.state {
            case .loaded:
                break
            default:
                XCTFail ("Expected .loaded")
            }
            XCTAssertTrue (reference.isEager)
        }
        // Decoding errors
        // IsNil Present but False
        json = "{\"isEager\":false,\"isNil\":false}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"databaseId\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"databaseId\\\", intValue: nil) (\\\"databaseId\\\").\", underlyingError: nil))", "\(error)")
        }
        // No databaseId
        json = "{\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"databaseId\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"databaseId\\\", intValue: nil) (\\\"databaseId\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal databaseId
        json = "{\"databaseId\":44,\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.String, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"databaseId\", intValue: nil)], debugDescription: \"Expected to decode String but found a number instead.\", underlyingError: nil))", "\(error)")
        }
        // No id
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal id
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"AAA\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10,\"isNil\":false}"
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
        // Missing collectionName
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"collectionName\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"collectionName\\\", intValue: nil) (\\\"collectionName\\\").\", underlyingError: nil))", "\(error)")
        }
        // illegal collectionName
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":false,\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.String, Swift.DecodingError.Context(codingPath: [CodingKeys(stringValue: \"collectionName\", intValue: nil)], debugDescription: \"Expected to decode String but found a number instead.\", underlyingError: nil))", "\(error)")
        }
        // Missing Version
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\"}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"version\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"version\\\", intValue: nil) (\\\"version\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Version
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.id.uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":\"10\"}"
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
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
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
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
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
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
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
            XCTAssertEqual ("WARNING|PersistentCollection<Database, MyStruct>.get|Unknown id|databaseHashValue=\(accessor.hashValue());collection=myCollection;id=\(parentId.uuidString)", entries[0].asTestString())
            XCTAssertEqual("ERROR|EntityReference<MyStruct, MyStruct>.addParentToBatch|noParent|collectionName=myCollection;parentId=\(parentId.uuidString);parentVersion=10;errorMessage=ok(nil)", entries[1].asTestString())
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
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
        }
    }

    public func testSetEntity() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
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
        // Same again, with pending closure
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
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
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
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
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
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
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
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
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
        // entity -> entity.referenceData
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
        batch.syncEntities() { entities in
            XCTAssertEqual(0, entities.count)
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
        batch.syncEntities() { entities in
            XCTAssertEqual(1, entities.count)
            XCTAssertEqual (entities[parentId]?.referenceData().id.uuidString, parentId.uuidString)
        }
        // entity -> nil referenceData
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
    }
    
    public func testSetReferenceDataIsEager() {
        let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
        let parentId = UUID()
        let parent = Entity (collection: collection, id: parentId, version: 10, item: MyStruct (myInt: 10, myString: "10"))
        let parentData = EntityReferenceData<MyStruct> (collection: parent.collection, id: parentId, version: parent.getVersion())
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
        // nil -> referenceData
        let entityId = UUID()
        let entity = Entity (collection: collection, id: entityId, version: 10, item: MyStruct (myInt: 20, myString: "20"))
        var waitFor = expectation(description: "wait1")
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        reference.set(referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
            XCTAssertTrue (reference.collection! === collection)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entity.id.uuidString, referenceData.id.uuidString)
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
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
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
        // referenceData -> same referenceData
        batch = EventuallyConsistentBatch()
        reference.set(referenceData: entity.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId.uuidString)
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
        // referenceData -> new referenceData
        let entityId2 = UUID()
        let entity2 = Entity (collection: collection, id: entityId2, version: 10, item: MyStruct (myInt: 30, myString: "30"))
        batch = EventuallyConsistentBatch()
        waitFor = expectation(description: "wait2")
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        reference.set(referenceData: entity2.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entity2.id.uuidString, referenceData.id.uuidString)
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
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === entity2)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
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
        // entity -> entity.referenceData
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil, isEager: true)
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
        // entity -> new referenceData
        batch = EventuallyConsistentBatch()
        waitFor = expectation(description: "wait2")
        reference.appendClosure() { result in
            waitFor.fulfill()
        }
        reference.set (referenceData: entity2.referenceData(), batch: batch)
        reference.sync() { reference in
            XCTAssertNil (reference.entity)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
            XCTAssertTrue (reference.collection === collection)
            switch reference.state {
            case .retrieving(let referenceData):
                XCTAssertEqual (entity2.id.uuidString, referenceData.id.uuidString)
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
        waitForExpectations(timeout: 10, handler: nil)
        reference.sync() { reference in
            XCTAssertTrue (reference.entity! === entity2)
            XCTAssertTrue (reference.parent as! Entity<MyStruct> === parent)
            XCTAssertTrue (parentData == reference.parentData)
            XCTAssertEqual (reference.referenceData!.id.uuidString, entityId2.uuidString)
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
        // entity -> nil referenceData
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
    }
    
    public func testAsync() {
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
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
                XCTAssertTrue (suspendtime.timeIntervalSince1970 > (now + reference.retryInterval - 1.0).timeIntervalSince1970)
                XCTAssertTrue (suspendtime.timeIntervalSince1970 < (now + reference.retryInterval + 1.0).timeIntervalSince1970)
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
                XCTAssertTrue (suspendtime.timeIntervalSince1970 > (now + reference.retryInterval - 1.0).timeIntervalSince1970)
                XCTAssertTrue (suspendtime.timeIntervalSince1970 < (now + reference.retryInterval + 1.0).timeIntervalSince1970)
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
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
                XCTAssertEqual ("Test Error", errorMessage)
                waitFor.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
                XCTAssertEqual (1, contents.pendingEntityClosureCount)
            default:
                XCTFail ("Expected .retrieving")
            }
        }
        waitFor2 = expectation(description: "wait8a")
        reference.async() { result in
            switch result {
            case .error (let errorMessage):
                XCTAssertEqual ("Test Error", errorMessage)
                waitFor2.fulfill()
            default:
                XCTFail ("Expected .error")
            }
        }
        reference.sync() { contents in
            switch contents.state {
            case .retrieving (let data):
                XCTAssertEqual (data.id.uuidString, persistentUUID.uuidString)
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
                XCTAssertEqual (data.databaseId, database.getAccessor().hashValue())
                XCTAssertEqual (data.collectionName, collection.name)
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
        let collection = PersistentCollection<Database, MyStruct> (database: database, name: "myCollection")
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
}
