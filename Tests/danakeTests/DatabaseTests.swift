//
//  databaseTests.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import XCTest
@testable import danake

class RegistrarTestItem {
    
    init (stringValue: String) {
        self.stringValue = stringValue
    }
    
    let stringValue: String
}

class DatabaseTests: XCTestCase {

    func testDatabaseCreation() {
        // XCTAssertEqual (0, Database.registrar.count()) occasionally fails on Linux. That is, this test occasionally starts
        // before a Database object created in another test is fully deallocated and removes itself from the database registry.
        // This could be cause by the deallocation occuring on another thread (in which case one would expect to see it occasionally
        // on OSX, or because Linux occasionally has problem with deterministic deallocation of constructs (which I have seen some other
        // evience for.
        let now = Date().timeIntervalSince1970
        while (Database.registrar.count() > 0 && (now + 30.0 > Date().timeIntervalSince1970)) {
            usleep(100)
        }
        XCTAssertEqual (0, Database.registrar.count())
        let accessor = InMemoryAccessor()
        let logger = InMemoryLogger()
        var database: Database? = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        XCTAssertTrue (accessor === database!.getAccessor() as! InMemoryAccessor)
        XCTAssertTrue (logger === database!.logger as! InMemoryLogger)
        XCTAssertTrue (Database.registrar.isRegistered(key: accessor.hashValue()))
        XCTAssertEqual (5, database?.schemaVersion)
        XCTAssertEqual (1, Database.registrar.count())
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("INFO|Database.init|created|hashValue=\(accessor.hashValue())", entries[0].asTestString())
        }
        let _ = Database (accessor: accessor, schemaVersion: 5, logger: logger)
        logger.sync() { entries in
            XCTAssertEqual (2, entries.count)
            XCTAssertEqual ("EMERGENCY|Database.init|registrationFailed|hashValue=\(accessor.hashValue())", entries[1].asTestString())
        }
        XCTAssertNotNil(database)
        XCTAssertTrue (Database.registrar.isRegistered(key: accessor.hashValue()))
        XCTAssertEqual (1, Database.registrar.count())
        database = nil
        XCTAssertFalse (Database.registrar.isRegistered(key: accessor.hashValue()))
        XCTAssertEqual (0, Database.registrar.count())
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
        result = .error ("An Error")
        XCTAssertFalse (result.isOk())
        XCTAssertNil (result.item())
    }
    
    func testRegistrar() {
        let registrar = Registrar<Int, RegistrarTestItem>()
        XCTAssertEqual (0, registrar.count())
        let key1 = 10
        let key2 = 20
        let key3 = 30
        XCTAssertNil (registrar.value (key: key1))
        XCTAssertNil (registrar.value (key: key2))
        XCTAssertNil (registrar.value (key: key3))
        let value1 = RegistrarTestItem (stringValue: "10")
        var value2: RegistrarTestItem? = RegistrarTestItem (stringValue: "20")
        var value3: RegistrarTestItem? = RegistrarTestItem (stringValue: "30")
        XCTAssertTrue (registrar.register(key: key1, value: value1))
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertNil (registrar.value (key: key2))
        XCTAssertNil (registrar.value (key: key3))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.register(key: key1, value: value1))
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertNil (registrar.value (key: key2))
        XCTAssertNil (registrar.value (key: key3))
        XCTAssertEqual (1, registrar.count())
        XCTAssertFalse (registrar.register(key: key1, value: value2!))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertNil (registrar.value (key: key2))
        XCTAssertNil (registrar.value (key: key3))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.register(key: key2, value: value2!))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertTrue (registrar.value(key: key2)! === value2)
        XCTAssertNil (registrar.value (key: key3))
        XCTAssertTrue (registrar.register(key: key2, value: value2!))
        XCTAssertFalse (registrar.register(key: key2, value: value1))
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertTrue (registrar.value(key: key2)! === value2)
        XCTAssertNil (registrar.value (key: key3))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        registrar.deRegister(key: key2)
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertTrue (registrar.value(key: key2)! === value2)
        XCTAssertNil (registrar.value (key: key3))
        XCTAssertTrue (registrar.register(key: key3, value: value3!))
        XCTAssertEqual (3, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertTrue (registrar.value(key: key2)! === value2)
        XCTAssertTrue (registrar.value(key: key3)! === value3)
        value2 = nil
        registrar.deRegister(key: key2)
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key3, value: value3!))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertTrue (registrar.value(key: key3)! === value3)
        XCTAssertNil (registrar.value (key: key2))
        value3 = nil
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertNil (registrar.value (key: key2))
        XCTAssertNil (registrar.value (key: key3))
        registrar.deRegister(key: key3)
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.value(key: key1)! === value1)
        XCTAssertNil (registrar.value (key: key2))
        XCTAssertNil (registrar.value (key: key3))
    }
    
    func testValidationResult() {
        var validationResult = ValidationResult.ok
        XCTAssertTrue (validationResult.isOk())
        XCTAssertEqual ("ok", validationResult.description())
        validationResult = ValidationResult.error("Error Description")
        XCTAssertFalse (validationResult.isOk())
        XCTAssertEqual ("Error Description", validationResult.description())
    }
    
    func testEntityCreation() {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .secondsSince1970
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .secondsSince1970
        let creationDateString1 = try! jsonEncodedDate (date: Date())!
        let id1 = UUID()
        var json = "{\"id\":\"\(id1.uuidString)\",\"schemaVersion\":3,\"created\":\(creationDateString1),\"item\":{\"myInt\":100,\"myString\":\"A \\\"Quoted\\\" String\"},\"persistenceState\":\"persistent\",\"version\":10}"
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        let collection = EntityCache<MyStruct>(database: database, name: standardCollectionName)
        // Create new
        decoder.userInfo[Database.collectionKey] = collection
        let creation = EntityCreation()
        var entity: Entity<MyStruct>? = nil
        switch creation.entity (creator: { try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!) }) {
        case .ok (let entity1):
            XCTAssertTrue (entity1 === collection.cachedEntity(id: id1)!)
            XCTAssertEqual (id1.uuidString, entity1.id.uuidString)
            XCTAssertEqual (5, entity1.getSchemaVersion()) // Schema version is taken from the collection, not the json
            XCTAssertEqual (10, entity1.version )
            switch entity1.persistenceState {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            entity1.sync() { item in
                XCTAssertEqual (100, item.myInt)
                XCTAssertEqual("A \"Quoted\" String", item.myString)
            }
            try XCTAssertEqual (jsonEncodedDate (date: entity1.created)!, creationDateString1)
            XCTAssertNil (entity1.saved)
            entity = entity1
        default:
            XCTFail("Expected .ok")
        }
        // Create existing
        switch creation.entity (creator: { try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!) }) {
        case .ok (let entity2):
            XCTAssertTrue (entity2 === collection.cachedEntity(id: id1)!)
            XCTAssertEqual (id1.uuidString, entity2.id.uuidString)
            XCTAssertEqual (5, entity2.getSchemaVersion()) // Schema version is taken from the collection, not the json
            XCTAssertEqual (10, entity2.version )
            switch entity2.persistenceState {
            case .persistent:
                break
            default:
                XCTFail ("Expected .persistent")
            }
            entity2.sync() { item in
                XCTAssertEqual (100, item.myInt)
                XCTAssertEqual("A \"Quoted\" String", item.myString)
            }
            try XCTAssertEqual (jsonEncodedDate (date: entity2.created)!, creationDateString1)
            XCTAssertNil (entity2.saved)
            XCTAssertTrue (entity === entity2)
        default:
            XCTFail("Expected .ok")
        }
        // Create error
        json = "{}"
        switch creation.entity (creator: { try decoder.decode(Entity<MyStruct>.self, from: json.data(using: .utf8)!) }) {
        case .error(let errorString):
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", errorString)
        default:
            XCTFail("Expected .eror")
        }
    }
    
    public func testQualifiedCollectionName() {
        let collectionName: CollectionName = "B"
        XCTAssertEqual ("A.B", Database.qualifiedCollectionName (databaseHash: "A", collectionName: collectionName))
        let accessor = InMemoryAccessor()
        let database = Database (accessor: accessor, schemaVersion: 5, logger: nil)
        XCTAssertEqual ("\(accessor.hashValue()).\(standardCollectionName)", database.qualifiedCollectionName(standardCollectionName))
    }
    
}
