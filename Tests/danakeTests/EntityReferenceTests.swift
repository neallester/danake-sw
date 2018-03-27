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
        decoder.userInfo[Database.parentDataKey] = parentData
        // Creation with entity
        var reference = EntityReference<MyStruct, MyStruct> (parent: parentData, entity: nil)
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":false,\"collectionName\":\"myCollection\",\"version\":10}"
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":false,\"collectionName\":\"myCollection\",\"version\":10}"
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
        reference = EntityReference<MyStruct, MyStruct> (parent: parentData, referenceData: child.referenceData(), isEager: true)
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
            XCTAssertTrue (reference.isEager)
        }
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
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
            XCTAssertTrue (reference.isEager)
        }
        // Decoding with isNil:false
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10,\"isNil\":false}"
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
            XCTAssertTrue (reference.isEager)
        }
        // Decoding errors
        // IsNil Present but False
        json = "{\"isEager\":false,\"isNil\":false}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.databaseId, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key databaseId (\\\"databaseId\\\").\", underlyingError: nil))", "\(error)")
        }
        // No databaseId
        json = "{\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.databaseId, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key databaseId (\\\"databaseId\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal databaseId
        json = "{\"databaseId\":44,\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.String, Swift.DecodingError.Context(codingPath: [danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.databaseId], debugDescription: \"Expected to decode String but found a number instead.\", underlyingError: nil))", "\(error)")
        }
        // No id
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.id, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key id (\\\"id\\\").\", underlyingError: nil))", "\(error)")
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
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"collectionName\":\"myCollection\",\"version\":10,\"isNil\":false}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.isEager, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key isEager (\\\"isEager\\\").\", underlyingError: nil))", "\(error)")
        }
        // illegal isEager
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":\"what?\",\"collectionName\":\"myCollection\",\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Bool, Swift.DecodingError.Context(codingPath: [danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.isEager], debugDescription: \"Expected to decode Bool but found a string/data instead.\", underlyingError: nil))", "\(error)")
        }
        // Missing collectionName
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.collectionName, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key collectionName (\\\"collectionName\\\").\", underlyingError: nil))", "\(error)")
        }
        // illegal collectionName
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":false,\"version\":10}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.String, Swift.DecodingError.Context(codingPath: [danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.collectionName], debugDescription: \"Expected to decode String but found a number instead.\", underlyingError: nil))", "\(error)")
        }
        // Missing Version
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\"}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("keyNotFound(danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.version, Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key version (\\\"version\\\").\", underlyingError: nil))", "\(error)")
        }
        // Illegal Version
        json = "{\"databaseId\":\"\(database.accessor.hashValue())\",\"id\":\"\(child.getId().uuidString)\",\"isEager\":true,\"collectionName\":\"myCollection\",\"version\":\"10\"}"
        do {
            reference = try decoder.decode(EntityReference<MyStruct, MyStruct>.self, from: json.data(using: .utf8)!)
            XCTFail ("Expected Exception")
        } catch {
            XCTAssertEqual ("typeMismatch(Swift.Int, Swift.DecodingError.Context(codingPath: [danake.EntityReference<danakeTests.MyStruct, danakeTests.MyStruct>.CodingKeys.version], debugDescription: \"Expected to decode Int but found a string/data instead.\", underlyingError: nil))", "\(error)")
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
}
