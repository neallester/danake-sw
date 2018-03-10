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

class databaseTests: XCTestCase {

    func testDatabaseCreation() {
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
        let value1 = RegistrarTestItem (stringValue: "10")
        let key2 = 20
        var value2: RegistrarTestItem? = RegistrarTestItem (stringValue: "20")
        let key3 = 30
        var value3: RegistrarTestItem? = RegistrarTestItem (stringValue: "30")
        XCTAssertTrue (registrar.register(key: key1, value: value1))
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.register(key: key1, value: value1))
        XCTAssertFalse (registrar.register(key: key1, value: value2!))
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key2, value: value2!))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key2, value: value2!))
        XCTAssertFalse (registrar.register(key: key2, value: value1))
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        registrar.deRegister(key: key2)
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        XCTAssertTrue (registrar.register(key: key3, value: value3!))
        XCTAssertEqual (3, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertTrue (registrar.isRegistered (key: key2))
        XCTAssertTrue (registrar.isRegistered (key: key3))
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
        value3 = nil
        XCTAssertEqual (2, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
        registrar.deRegister(key: key3)
        XCTAssertEqual (1, registrar.count())
        XCTAssertTrue (registrar.isRegistered (key: key1))
        XCTAssertFalse (registrar.isRegistered (key: key2))
        XCTAssertFalse (registrar.isRegistered (key: key3))
    }
    
    func testValidationResult() {
        var validationResult = ValidationResult.ok
        XCTAssertTrue (validationResult.isOk())
        XCTAssertEqual ("ok", validationResult.description())
        validationResult = ValidationResult.error("Error Description")
        XCTAssertFalse (validationResult.isOk())
        XCTAssertEqual ("Error Description", validationResult.description())
    }

}
