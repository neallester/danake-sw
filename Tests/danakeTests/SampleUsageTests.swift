//
//  SampleUsageTests.swift
//  danakeTests
//
//  Created by Neal Lester on 4/24/18.
//

import XCTest
@testable import danake

/*
    ========
    Contents
    ========
 
        1. The Sample Application Model
        2. The Sample Persistence System
        3. Test demonstrating usage within application code
        4. Tests demonstrating deliberate generation of errors by InMemoryAccessor to test error handling in
           application code
        5. Tests of the basic functionality of the sample model and sample persistence system

    ============================
    The Sample Application Model
    ============================
 
    This sample assumes you have read the framework introduction in README.md
    https://github.com/neallester/danake-sw#introduction
 
    These classes demonstrate how to incorporate Danake into an application object model.
 
    class Company:  A Company is associated with zero or more Employees.
                    - Property `employeeCollection' demonstrates how to deserialize a property whose value
                      comes from the environment rather than the the serialized representation of the instance.
                    - property `id' demonstrates how to create a model object with an attribute of type UUID
                      value which is taken from the id of the enclosing Entity at creation and deserialization
                      time.
                    - The function `employees()' demonstrate functions whose results are obtained via queries to
                      the persistent media.
 
    class Employee: An employee is associated with zero or one companies, and has zero or one addresses.
                    - Properties `company' and `address' demonstrate the usage of EntityReference<PARENT, TYPE>
                    - company attribute demonstrates eager retrieval

    class Address:  Demonstrates the use of a struct as a reference.
 
 */

class Company : Codable {
    
    enum CodingKeys: CodingKey {}
    
    init (employeeCollection: EmployeeCollection, id: UUID) {
        self.id = id
        self.employeeCollection = employeeCollection
    }
    
    // Custom decoder sets the `employeeCollection' and `id' during deserialization
    public required init (from decoder: Decoder) throws {
        // employeeCollection set by persistence system. See CompanyCollection.init()
        if let employeeCollection = decoder.userInfo[Company.employeeCollectionKey] as? EmployeeCollection {
            self.employeeCollection = employeeCollection
        } else {
            throw EntityDeserializationError<Company>.missingUserInfoValue(Company.employeeCollectionKey)
        }
        // Use the enclosing entities value of `id'
        // Same method may be used to obtain schemaVersion at time of last save
        if let container = decoder.userInfo[Database.parentDataKey] as? DataContainer, let parentReferenceData = container.data as? EntityReferenceData<Company> {
            self.id = parentReferenceData.id
        } else {
            throw EntityDeserializationError<Company>.missingUserInfoValue(Database.parentDataKey)
        }
    }
    
    func encode(to encoder: Encoder) throws {
        var _ = encoder.container(keyedBy: CodingKeys.self)
    }
    
    // With this design the results of the `employees' function will not include any newly
    // created employee objects which have not yet been saved to the persistent media
    
    // Syncrhonous implementation
    public func employees() -> RetrievalResult<[Entity<Employee>]> {
        return employeeCollection.forCompany(self)
    }
    
    // Asyncrhonous implementation
    public func employees(closure: @escaping (RetrievalResult<[Entity<Employee>]>) -> Void) {
        employeeCollection.forCompany(self, closure: closure)
    }
    
    // In actual use within the application; id will match the id of the surrounding Entity wrapper
    public let id: UUID
    
    private let employeeCollection: EmployeeCollection
    public static let employeeCollectionKey = CodingUserInfoKey.init(rawValue: "employeeCollectionKey")!

}

class Employee : Codable {
    
    init (selfReference: EntityReferenceData<Employee>, company: Entity<Company>, name: String, address: Entity<Address>?) {
        self.name = name
        // company reference is created with eager retrieval
        self.company = ReferenceManager<Employee, Company> (parent: selfReference, entity: company, isEager: true)
        self.address = ReferenceManager<Employee, Address> (parent: selfReference, entity: address)
    }

    var name: String
    
    func resetName() {
        name = ""
    }
    
    // Always declare attributes of type 'EntityReference' using 'let'
    // The entities which are referenced may change, but the EntityReference itself does not
    let company: ReferenceManager<Employee, Company>
    let address: ReferenceManager<Employee, Address>
    
}

struct Address : Codable {
    
    init (street: String, city: String, state: String, zipCode: String) {
        self.street = street
        self.city = city
        self.state = state
        self.zipCode = zipCode
    }
    
    var street: String
    var city: String
    var state: String
    var zipCode: String
    
}

/*
    =============================
    The Sample Persistence System
    =============================
 
    These classes demonstrate the persistence system implementing persistence for the previously introduced sample model.
 
    protocol DatabaseAccessor:      Interface for the adapter used to access persistent media (an implementation is provided for each
                                    supported media). Lookup by Entity.id and collection scan (with optional selection criteria) are
                                    included.
 
    protocol SampleAccessor:        Application specific extension to DatabaseAccessor which adds specialized queries (e.g. lookups
                                    based on indexed criteria). In this case it includes selecting employees by the id of their
                                    associated company. A media specific implementation (e.g. SampleInMemoryAccessor) must be provided
                                    for each supported persistent media. The application specific DatabaseAccessor protocol is where
                                    application developers should place the application interface to specialized queries which rely on
                                    indexing capabilities in the underlying persistent media.

    class Database:                 Representation of a specific persistent media in the application. Only one instance of the
                                    Database object associated with any particular persistent storage media (database) may be present
                                    in system. Declare Database objects as let constants within a scope with process lifetime.
                                    Re-creating a Database object is not currently supported.
 
    class SampleDatabase:           Provides EntityCaches with access to the applicatio specific SampleAccessor. Only
                                    required for applications which include an application specific DatabaseAccessor.
 
    class EntityCache<T>:           Access to the persisted instances of a single type or a polymorphically related set of types (use
                                    polymorism if indexed queries based on attributes shared by all of the types are required). Each
                                    EntityCache must be associated with exactly one database. Declare EntityCache
                                    attributes with `let' within a scope with process lifetime. Re-creating a EntityCache
                                    object is not currently supported.
 
    class CompanyCollection:        Access to persistent Company objects. Demonstrates:
                                    - creation of model objects which include an id whose value matches the id of their enclosing
                                      Entity
                                    - setup of a EntityCache to support deserialization of objects which include properties
                                      which are populated from the environment rather than the serialized data.
                                    - custom `new' function for creating new Entity<Company> objects
 
    class EmployeeCollection:       Access to persistent Employee objects. Demonstrates:
                                    - deserialization of objects which include EntityReference properties
                                    - custom synchronous and asynchronous retrieval functions
 
    class SampleCollections:        An application specific convenience class for organizing all of the EntityCache objects
                                    associated with the SampleDatabase. Declare objects of this type using `let' within a scope with
                                    process lifetime. Ensure that all EntityCaches used within the application are fully
                                    created before proceeding with model processing.
*/

class SampleDatabase : Database {
    
    init(accessor: SampleAccessor, schemaVersion: Int, logger: Logger?, referenceRetryInterval: TimeInterval) {
        self.sampleAccessor = accessor
        super.init (accessor: accessor, schemaVersion: schemaVersion, logger: logger, referenceRetryInterval: referenceRetryInterval)
    }
    
    let sampleAccessor: SampleAccessor
    
}

protocol SampleAccessor : DatabaseAccessor {
    func employeesForCompany (collection: EntityCache<Employee>, company: Company) -> DatabaseAccessListResult<Entity<Employee>>
}

class SampleInMemoryAccessor : InMemoryAccessor, SampleAccessor {
    func employeesForCompany (collection: EntityCache<Employee>, company: Company) -> DatabaseAccessListResult<Entity<Employee>> {
        let retrievalResult = scan (type: Entity<Employee>.self, collection: collection)
        switch retrievalResult {
        case .ok(let allEmployees):
            var result: [Entity<Employee>] = []
            for employeeEntity in allEmployees {
                employeeEntity.sync() { employee in
                    if employee.company.entityId()!.uuidString == company.id.uuidString {
                        result.append(employeeEntity)
                    }
                }
            }
            return .ok (result)
        case .error:
            return retrievalResult
        }
    }
    
}

class CompanyCollection : EntityCache<Company> {
    
    init (database: SampleDatabase, employeeCollection: EmployeeCollection) {
        self.employeeCollection = employeeCollection
        // The following closure is fired on the decoders userInfo property before deserialization
        // setting the employeeCollection object which will be assigned to the appropriate property
        // during deserialization (see Company.init (decoder:)
        super.init (database: database, name: "company") { userInfo in
            userInfo[Company.employeeCollectionKey] = employeeCollection
        }
    }
    
    func new (batch: EventuallyConsistentBatch) -> Entity<Company> {
        return new (batch: batch) { selfReference in
            return Company (employeeCollection: employeeCollection, id: selfReference.id)
        }
    }
    
    private let employeeCollection: EmployeeCollection
}

class EmployeeCollection : EntityCache<Employee> {

    init(database: SampleDatabase) {
        forCompanyClosure = { collection, company in
            return database.sampleAccessor.employeesForCompany (collection: collection, company: company)
        }
        super.init (database: database, name: "employee", deserializationEnvironmentClosure: nil)
    }
    
    func new (batch: EventuallyConsistentBatch, company: Entity<Company>, name: String, address: Entity<Address>?) -> Entity<Employee> {
        return new (batch: batch) { selfReference in
            return Employee (selfReference: selfReference, company: company, name: name, address: address)
        }
    }
    
    func forCompany (_ company: Company) -> RetrievalResult<[Entity<Employee>]> {
        switch forCompanyClosure (self, company) {
        case .ok(let employees):
            return .ok (employees)
        case .error(let errorMessage):
            return .error (errorMessage)
        }
    }
    
    func forCompany (_ company: Company, closure: @escaping (RetrievalResult<[Entity<Employee>]>) -> ()) {
        database.workQueue.async {
            closure (self.forCompany (company))
        }
    }
    
    private let forCompanyClosure: ((EmployeeCollection, Company) -> DatabaseAccessListResult<Entity<Employee>>)
    
}

class SampleCollections {
    
    init (accessor: SampleAccessor, schemaVersion: Int, logger: Logger?) {
        let database = SampleDatabase (accessor: accessor, schemaVersion: schemaVersion, logger: logger, referenceRetryInterval: 180.0)
        self.logger = logger
        employees = EmployeeCollection (database: database)
        companies = CompanyCollection (database: database, employeeCollection: employees)
        addresses = EntityCache<Address> (database: database, name: "address")
    }
    
    public let logger: Logger?
    
    public let companies: CompanyCollection
    
    public let employees: EmployeeCollection
    
    public let addresses: EntityCache<Address>
    
}

class SampleTests: XCTestCase {
    
    // Execute the `runSample' test using the SampleInMemoryAccessor
    public func testInMemorySample() {
        let accessor = SampleInMemoryAccessor()
        
        SampleTests.runSample (accessor: accessor)
    }
    
/*
     ================================================
     Test demonstrating usage within application code
     ================================================
*/

    static func runSample (accessor: SampleAccessor) {
        // Application code can run with any DatabaseAccessor implementing SampleAccessor
        
        // Declare SampleCollections with `let'
        // See class Database header comment for explanation of `schemaVersion'
        let logger = InMemoryLogger()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: logger)
        
        // Creating a database logs INFO
        logger.sync() { entries in
            XCTAssertEqual (1, entries.count)
            XCTAssertEqual ("INFO|SampleDatabase.init|created|hashValue=\(collections.employees.database.accessor.hashValue)", entries[0].asTestString())
        }

        // Start test in known state by ensuring persistent media is empty
        removeAll(collections: collections)
        
        var company1id: UUID?

        // Setup an arbitrary scope for illustration purposes
        do {
            // batch may be declared with `let' or `var'
            // However, do not recreate batches which contain uncommitted updates
            // batch retry and timeout intervals are also settable; see
            // EventuallyConsistentBatch class header comment for these and other details
            // In application code always create batches with a logger
            let batch = EventuallyConsistentBatch(logger: collections.logger)
            
            let company1 = collections.companies.new(batch: batch)
            company1id = company1.id
            let company2 = collections.companies.new(batch: batch)
            
            // new objects are not available in persistent media until batch is committed
            if let companies = collections.companies.scan().item() {
                XCTAssertEqual (0, companies.count)
            } else {
                // Retrieval error
                XCTFail ("Expected valid item")
            }
            
            let group = DispatchGroup()
            group.enter()
            
            // Batch.commit() is asyncrhonous
            // use Batch.commitSync() to wait for batch processing
            batch.commit() {
                group.leave()
            }
            group.wait()
            
            // After commit our companies are in the persistent media
            if let companies = collections.companies.scan().item() {
                XCTAssertEqual (2, companies.count)
            } else {
                // Retrieval error
                XCTFail ("Expected valid item")
            }
            
            // There is only one instance representing any particular persistent Entity
            // `get' queries will return cached entities (if available) before hitting persistent media.
            // However, `scan' queries retrieving already cached entities are
            // still expensive as they still go persistent media (cache normalization for scans does
            // not occur until after the persistent objects are retrieved and partially deserialized)
            if let entity = collections.companies.get(id: company1.id).item() {
                XCTAssertTrue (company1 === entity)
            } else {
                // Retrieval error
                XCTFail ("Expected valid item")
            }
            
            // Because EntityCache.get() can be expensive,
            // use asynchronous version when possible
            group.enter()
            collections.companies.get(id: company2.id) { retrievalResult in
                if let company = retrievalResult.item() {
                    XCTAssertTrue (company === company2)
                } else {
                    // Retrieval error
                    XCTFail ("Expected valid")
                }
                group.leave()
            }
            group.wait()
            
            // Asynchronous version is also preferred for EntityCache.scan()
            group.enter()
            collections.companies.scan() { retrievalResult in
                if let companies = retrievalResult.item() {
                    XCTAssertEqual (2, companies.count)
                } else {
                    // Retrieval error
                    XCTFail ("Expected valid")
                }
                group.leave()
            }
            group.wait()

            // If a successful query may return nil, explicitly use retrievialResult instead of .item()
            group.enter()
            let badId = UUID()
            collections.companies.get(id: badId) { retrievalResult in
                switch retrievalResult {
                case .ok (let company):
                    XCTAssertNil (company)
                default:
                    // Retrieval error
                    XCTFail ("Expected .ok")
                }
                group.leave()
            }
            group.wait()
            
            // An unsuccessful EntityCache.get logs a WARNING
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
                XCTAssertEqual ("WARNING|CompanyCollection.get|Unknown id|databaseHashValue=\(collections.employees.database.accessor.hashValue);collection=company;id=\(badId.uuidString)", entries[1].asTestString())
            }

            // Retrieving persisted objects by criteria
            // The default implementation retrieves and deserializes all entries in a collection/table
            // before filtering the results.
            let scanResult = collections.companies.scan() { company in
                company.id.uuidString == company2.id.uuidString
            }
            if let companies = scanResult.item() {
                XCTAssertEqual (1, companies.count)
                XCTAssertEqual (companies[0].id.uuidString, company2.id.uuidString)
            } else {
                // Retrieval error
                XCTFail ("Expected valid item")
            }

            // after commit() the batch is left as a fresh empty batch and may be reused
            let _ = collections.employees.new(batch: batch, company: company1, name: "Name One", address: nil)
            var employee2: Entity<Employee>? = collections.employees.new(batch: batch, company: company2, name: "Name Two", address: nil)
            batch.commitSync()
            // Updating the name attribute of Employee and setting a new reference for its address
            let address1 = collections.addresses.new (batch: batch, item: Address(street: "Street 1", city: "City 1", state: "CA", zipCode: "94377"))
            let address2 = collections.addresses.new (batch: batch, item: Address(street: "Street 2", city: "City 2", state: "CA", zipCode: "94377"))
            let address3 = collections.addresses.new (batch: batch, item: Address(street: "Street 3", city: "City 3", state: "CA", zipCode: "94377"))
            batch.commitSync()
            employee2!.update(batch: batch) { employee in
                employee.name = "Name Updated2"
                employee.address.set(entity: address1, batch: batch)
            }
            batch.commitSync()

            // EntityReference may be also be updated within a synchronous access to the
            // parent entity
            employee2!.sync() { employee in
                employee.address.set(entity: address2, batch: batch)
            }
            batch.commitSync()

            // EntityReference may be updated within an asynchronous access, but it is the
            // application developer's responsibility to ensure the update occurs before
            // the batch is committed

            group.enter()
            employee2!.async() { employee in
                employee.address.set(entity: address3, batch: batch)
                group.leave()
            }
            group.wait()
            batch.commitSync()
            
            // Gotchas
            
            var employeeItem: Employee?
            employeeItem = nil
            employee2!.sync() { employee in
                // Capturing a reference to an entity's item outside of the
                // Entity.sync(), Entity.async(), or Entity.update() closures
                // bypasses the multi-threading protection offered by the Entity wrapper.
                // i.e. avoid doing the following:
                // employeeItem = employee
            }
            XCTAssertNil (employeeItem)

            // Closures which modify an item's state via functions must always be called within an
            // Entity.update() call. Failure to do so will cause lost data (which will be logged when
            // the entity is deallocated; this error is not demonstrated here)
            employee2!.update(batch: batch) { employee in
                employee.resetName()
            }
            batch.commitSync()
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
            }
            
            // EntityReference objects contain strong references to their referenced Entity
            // once loaded. Use Entity.breakReferences() to unload those entities if
            // they may create strong reference cycles (i.e. refer back to self).
            // There is also Entity.breakReferencesRecursive().
            // They make the EntityReferences within the Entity's item unusable so call only
            // when completely finished with the entity (and all reachable Entities for the recursive
            // version). Notes:
            // 1) breakReferences() will not interfere with a future or in progress
            //    asynchronous save.
            // 2) There isn't actually a reference cycle in employee2 below;
            //    This just demonstrates the usage of Entity.breakReferences()
            //    See EntityTests.testEntityReferenceCycle() for an example of a direct
            //    reference cycle (indirect cycles are also possible).
            employee2!.sync() { employee in
                let _ = employee.company.get().item()!
            }
            employee2!.breakReferences() // unloads the Entity<Company>;makes employee2.item.company unusuable
            employee2 = nil              // so only call after we are completely finished with employee2

        }

        // Closing the previous scope will cause `company1' and `company2' to be removed from cache
        // when they are deallocated. However, if batch.commit() rather than batch.commitSync() had
        // been used, then those entity objects could live on beyond the end of the scope until
        // the batch (processing asynchronously) was finished with them
        
        // Application developers must always commit batches
        // Updates to an Entity will be lost (and ERROR logged) if both the batch and entity go out of
        // scope before the batch is committed
        var lostChangesEmployeeUuidString = ""
        do {
            let batch = EventuallyConsistentBatch (logger: logger)
            if let companyEntity = collections.companies.get (id: company1id!).item() {
                
                
                companyEntity.sync() { company in
                    if let employeeEntity = company.employees().item()?[0] {
                        lostChangesEmployeeUuidString = employeeEntity.id.uuidString
                        employeeEntity.update(batch: batch) { employee in
                            employee.name = "Name Updated1"
                            XCTAssertEqual ("Name Updated1", employee.name)
                        }
                    } else {
                        // Retrieval error
                        XCTFail ("Expected valid")
                    }
                }
            } else {
                // Retrieval error
                XCTFail ("Expected valid")
            }
        }
        
        // Batch cleanup occurs asynchronously
        // For demonstration purposes only wait for it to complete using the internal
        // function EntityCache.sync()
        // In application code waiting like this should never be necessary because
        // batches should always be committed before they go out of scope
        var hasCached = true
        let lostChangesEmployeeUUID = UUID(uuidString: lostChangesEmployeeUuidString)!
        while hasCached {
            collections.employees.sync() { entities in
                hasCached = entities[lostChangesEmployeeUUID]?.codable != nil
                usleep(100)
            }
        }
        
        // An error has indeed been logged and the update has indeed been lost
        logger.sync() { entries in
            XCTAssertEqual (3, entries.count)
            XCTAssertEqual ("ERROR|BatchDelegate.deinit|notCommitted:lostData|entityType=Entity<Employee>;entityId=\(lostChangesEmployeeUuidString);entityPersistenceState=dirty", entries[2].asTestString())
        }
        do {
            if let companyEntity = collections.companies.get (id: company1id!).item() {
                companyEntity.sync() { company in
                    if let employeeEntity = company.employees().item()?[0] {
                        
                        
                        employeeEntity.sync() { employee in
                            XCTAssertEqual ("Name One", employee.name)
                        }
                    } else {
                        // Retrieval error
                        XCTFail ("Expected valid")
                    }
                }
            } else {
                // Retrieval error
                XCTFail ("Expected valid")
            }
        }

        // Clean up
        removeAll(collections: collections)
    }
    
/*
     ===========================================================
     Tests demonstrating deliberate generation of errors by
     InMemoryAccessor to test error handling in application code
     ===========================================================
*/
    public func testDemonstrateThrowError() {
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let collections = SampleCollections(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: collections.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        // Use InMemoryAccessor.setThrowError() to simulate persistent media errors for testing
        inMemoryAccessor.setThrowError()
        switch collections.companies.get(id: companyId) {
        case .error(let errorMessage):
            XCTAssertEqual ("getError", errorMessage)
            logger.sync() { entries in
                XCTAssertEqual (1, entries.count)
                XCTAssertEqual ("EMERGENCY|CompanyCollection.get|Database Error|databaseHashValue=\(collections.companies.database.accessor.hashValue);collection=company;id=\(companyId.uuidString);errorMessage=getError", entries[0].asTestString())
            }
        default:
            XCTFail ("Expected .error")
        }
        // Only one error will be thrown; subsequent operations will succeed
        switch collections.companies.get(id: companyId) {
        case .ok (let company):
            XCTAssertEqual (company!.id.uuidString, companyId.uuidString)
            logger.sync() { entries in
                XCTAssertEqual (1, entries.count)
            }
        default:
            XCTFail ("Expected .ok")
        }
    }

    /*
        Use InMemoryAccessor.prefetch to control the timing of persistent media operations
     */
    public func testDemonstratePrefetchWithGet () {
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let collections = SampleCollections(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: collections.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        let semaphore = DispatchSemaphore (value: 1)
        semaphore.wait()
        // This preset will cause the persistent media to wait until the main test thread signals the semaphore
        let preFetch = { (uuid: UUID) in
            if (uuid.uuidString == companyId.uuidString) {
                semaphore.wait()
                semaphore.signal()
            }
        }
        inMemoryAccessor.setPreFetch(preFetch)
        var retrievedCompany: Entity<Company>? = nil
        let group = DispatchGroup()
        group.enter()
        collections.companies.get(id: companyId) { retrievalResult in
            switch retrievalResult {
            case .ok (let company):
                XCTAssertEqual (company!.id.uuidString, companyId.uuidString)
                retrievedCompany = company
            default:
                XCTFail ("Expected .ok")
            }
            group.leave()
        }
        // Company has not yet been retrieved
        XCTAssertNil (retrievedCompany)
        semaphore.signal()
        group.wait()
        // Company has now been retrieved
        XCTAssertEqual (retrievedCompany!.id.uuidString, companyId.uuidString)
        logger.sync() { entries in
            XCTAssertEqual (0, entries.count)
        }
    }

/*
     Entity updates require 2 calls to the DatabaseAccessor:
     1) Serialization (fast)
     2) Writing to persistent media (slow)
     Errors during serialization are considered unrecoverable
     Unrecoverable errors are logged ERROR but not retried
     Recoverable errors are logged EMERGENCY and retried until completion
     Both kinds of errors may be simulated by InMemoryAccessor
*/
    public func testDemonstrateUpdateErrors () {
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let collections = SampleCollections(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: collections.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        let employeeId = UUID(uuidString: "05081CBC-5ABA-4EE9-A7B1-4882E047D715")!
        let employeeJson = "{\"id\":\"\(employeeId.uuidString)\",\"schemaVersion\":1,\"created\":1525459064.9665,\"saved\":1525459184.5832,\"item\":{\"name\":\"Name Two\",\"company\":{\"databaseId\":\"\(inMemoryAccessor.hashValue)\",\"id\":\"\(companyId.uuidString)\",\"isEager\":true,\"collectionName\":\"company\",\"version\":1},\"address\":{\"isEager\":false,\"isNil\":true}},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: collections.employees.name, id: employeeId, data: employeeJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            XCTFail ("Expected .ok")
        }
        let batch = EventuallyConsistentBatch (retryInterval: .milliseconds(10), timeout: .seconds(60), logger: logger)
        var batchIdString = batch.delegateId().uuidString
        
        if let employeeEntity = collections.employees.get(id: employeeId).item() {
            // Update employee name
            employeeEntity.update(batch: batch) { employee in
                employee.name = "Name Updated1"
                XCTAssertEqual ("Name Updated1", employee.name)
            }

            // Using setThrowError() before committing an update will throw an unrecoverable serialization error
            inMemoryAccessor.setThrowError()
            batch.commitSync()
            logger.sync() { entries in
                XCTAssertEqual (1, entries.count)
                XCTAssertEqual ("ERROR|BatchDelegate.commit|Database.unrecoverableError(\"addActionError\")|entityType=Entity<Employee>;entityId=\(employeeId.uuidString);batchId=\(batchIdString)", entries[0].asTestString())
            }
            
            // Demonstrate that the previous changes were lost due to the reported unrecoverable error
            #if os(Linux)
                // The order of attributes on serialized JSON is not always consistent on Linux
                XCTAssertTrue (employeeJson.contains("\"id\":\"\(employeeId.uuidString)\""))
                XCTAssertTrue (employeeJson.contains("\"schemaVersion\":1"))
                XCTAssertTrue (employeeJson.contains("\"created\":1525459064.9665"))
                XCTAssertTrue (employeeJson.contains("\"saved\":1525459184.5832"))
                XCTAssertTrue (employeeJson.contains("\"item\":{"))
                XCTAssertTrue (employeeJson.contains("\"name\":\"Name Two\""))
                XCTAssertTrue (employeeJson.contains("\"company\":{"))
                XCTAssertTrue (employeeJson.contains("\"databaseId\":\"\(inMemoryAccessor.hashValue)\""))
                XCTAssertTrue (employeeJson.contains("\"id\":\"\(companyId.uuidString)\""))
                XCTAssertTrue (employeeJson.contains("\"isEager\":true"))
                XCTAssertTrue (employeeJson.contains("\"collectionName\":\"company\",\"version\":1}"))
                XCTAssertTrue (employeeJson.contains("\"version\":1}"))
                XCTAssertTrue (employeeJson.contains("\"address\":{"))
                XCTAssertTrue (employeeJson.contains("\"isEager\":false"))
                XCTAssertTrue (employeeJson.contains("\"isNil\":true"))
                XCTAssertTrue (employeeJson.contains("\"persistenceState\":\"persistent\""))
            #else
                XCTAssertEqual (employeeJson, String (data: inMemoryAccessor.getData(name: collections.employees.name, id: employeeId)!, encoding: .utf8))
            #endif
            XCTAssertFalse (inMemoryAccessor.isThrowError())

            // Modify the employee again so that it is again added to the batch
            employeeEntity.update(batch: batch) { employee in
                employee.name = "Name Updated2"
                XCTAssertEqual ("Name Updated2", employee.name)
            }
            
            // Use preFetch to setup a recoverable update error
            var preFetchCount = 0
            let prefetch: (UUID) -> () = { id in
                // The prefetch ignores the first call to the accessor
                if preFetchCount == 1 {
                    // Set the throwError attribute directly (do not call setThrowError())
                    inMemoryAccessor.throwError = true
                }
                preFetchCount = preFetchCount + 1
            }
            inMemoryAccessor.setPreFetch(prefetch)
                        batchIdString = batch.delegateId().uuidString
            batch.commitSync()
            logger.sync() { entries in
                XCTAssertEqual (2, entries.count)
                XCTAssertEqual ("EMERGENCY|BatchDelegate.commit|Database.error(\"addError\")|entityType=Entity<Employee>;entityId=\(employeeId.uuidString);batchId=\(batchIdString)", entries[1].asTestString())
            }
            // Demonstrate that the data was updated in persistent media
            #if os(Linux)
                // The order of attributes on serialized JSON is not always consistent on Linux
                XCTAssertTrue (employeeJson.contains("\"id\":\"\(employeeId.uuidString)\""))
                XCTAssertTrue (employeeJson.contains("\"schemaVersion\":1"))
                XCTAssertTrue (employeeJson.contains("\"created\":1525459064.9665"))
                XCTAssertTrue (employeeJson.contains("\"saved\":1525459184.5832"))
                XCTAssertTrue (employeeJson.contains("\"item\":{"))
                XCTAssertTrue (employeeJson.contains("\"name\":\"Name Two\""))
                XCTAssertTrue (employeeJson.contains("\"company\":{"))
                XCTAssertTrue (employeeJson.contains("\"databaseId\":\"\(inMemoryAccessor.hashValue)\""))
                XCTAssertTrue (employeeJson.contains("\"id\":\"\(companyId.uuidString)\""))
                XCTAssertTrue (employeeJson.contains("\"isEager\":true"))
                XCTAssertTrue (employeeJson.contains("\"collectionName\":\"company\",\"version\":1}"))
                XCTAssertTrue (employeeJson.contains("\"version\":1}"))
                XCTAssertTrue (employeeJson.contains("\"address\":{"))
                XCTAssertTrue (employeeJson.contains("\"isEager\":false"))
                XCTAssertTrue (employeeJson.contains("\"isNil\":true"))
                XCTAssertTrue (employeeJson.contains("\"persistenceState\":\"persistent\""))
            #else
                XCTAssertNotEqual (employeeJson, String (data: inMemoryAccessor.getData(name: collections.employees.name, id: employeeId)!, encoding: .utf8))
            #endif
        }
    }

    static func removeAll (collections: SampleCollections) {
        let batch = EventuallyConsistentBatch()
        for entity in collections.companies.scan(criteria: nil).item()! {
            entity.remove(batch: batch)
        }
        for entity in collections.employees.scan(criteria: nil).item()! {
            entity.remove(batch: batch)
        }
        for entity in collections.addresses.scan(criteria: nil).item()! {
            entity.remove(batch: batch)
        }
        batch.commitSync()
    }
    
/*
     ===================================================================================
     Tests of the  basic functionality of the sample model and sample persistence system
     ===================================================================================
*/
    
    func testCompanyCreation() {
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let uuid = UUID()
        let company = Company (employeeCollection: collections.employees, id: uuid)
        XCTAssertEqual (uuid.uuidString, company.id.uuidString)
    }
    
    func testCompanyEncodeDecode() {
        let json = "{}"
        var jsonData = json.data(using: .utf8)!
        let decoder = JSONDecoder()
        jsonData = json.data(using: .utf8)!
        // Decoding With no collection or parentData
        do {
            let _ = try decoder.decode(Company.self, from: jsonData)
            XCTFail("Expected error")
        } catch EntityDeserializationError<Company>.missingUserInfoValue (let error) {
            XCTAssertEqual ("CodingUserInfoKey(rawValue: \"employeeCollectionKey\")", "\(error)")
        } catch {
            XCTFail("Expected missingUserInfoValue")
        }
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        // Decoding with collection and no parent data
        decoder.userInfo[Company.employeeCollectionKey] = collections.employees
        do {
            let _ = try decoder.decode(Company.self, from: jsonData)
            XCTFail("Expected error")
        } catch EntityDeserializationError<Company>.missingUserInfoValue (let error) {
            XCTAssertEqual ("CodingUserInfoKey(rawValue: \"parentData\")", "\(error)")
        } catch {
            XCTFail("Expected missingUserInfoValue")
        }
        // Decoding with collection and parentdata
        let uuid = UUID (uuidString: "30288A21-4798-4134-9F35-6195BEC7F352")!
        let parentData = EntityReferenceData<Company>(collection: collections.companies, id: uuid, version: 1)
        let container = DataContainer ()
        container.data = parentData
        decoder.userInfo[Database.parentDataKey] = container
        do {
            let company = try decoder.decode(Company.self, from: jsonData)
            XCTAssertEqual ("30288A21-4798-4134-9F35-6195BEC7F352", company.id.uuidString)
            let encoder = JSONEncoder()
            let encodedData = try encoder.encode(company)
            XCTAssertEqual (json, String (data: encodedData, encoding: .utf8)!)
        } catch {
            XCTFail("Expected success but got \(error)")
        }
    }
    
    func testCompanyEmployees() {
        let accessor = SampleInMemoryAccessor()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = collections.companies.new(batch: batch)
        let companyEntity2 = collections.companies.new(batch: batch)
        let companyEntity3 = collections.companies.new(batch: batch)
        let employeeEntity1 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let _ = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let _ = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let waitFor = expectation(description: "wait1")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        companyEntity1.sync() { company in
            let employees = company.employees().item()!
            XCTAssertEqual (2, employees.count)
            XCTAssertTrue (employees[0] === employeeEntity1 || employees[0] === employeeEntity2)
            XCTAssertTrue (employees[1] === employeeEntity1 || employees[1] === employeeEntity2)
        }
        companyEntity2.sync() { company in
            let employees = company.employees().item()!
            XCTAssertEqual (2, employees.count)
        }
        companyEntity3.sync() { company in
            let employees = company.employees().item()!
            XCTAssertEqual (0, employees.count)
        }
    }

    func testCompanyEmployeesAsync() {
        let accessor = SampleInMemoryAccessor()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = collections.companies.new(batch: batch)
        let companyEntity2 = collections.companies.new(batch: batch)
        let companyEntity3 = collections.companies.new(batch: batch)
        let employeeEntity1 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let _ = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let _ = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let waitFor1 = expectation(description: "wait1")
        batch.commit() {
            waitFor1.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        let waitFor2a = expectation(description: "wait2a")
        let waitFor2b = expectation(description: "wait2b")
        let waitFor2c = expectation(description: "wait2c")
        companyEntity1.async() { company in
            company.employees() { result in
                let employees = result.item()!
                XCTAssertEqual (2, employees.count)
                XCTAssertTrue (employees[0] === employeeEntity1 || employees[0] === employeeEntity2)
                XCTAssertTrue (employees[1] === employeeEntity1 || employees[1] === employeeEntity2)
                waitFor2a.fulfill()
            }
        }
        companyEntity2.async() { company in
            company.employees() { result in
                let employees = result.item()!
                XCTAssertEqual (2, employees.count)
                waitFor2b.fulfill()
            }
        }
        companyEntity3.async() { company in
            company.employees() { result in
                let employees = result.item()!
                XCTAssertEqual (0, employees.count)
                waitFor2c.fulfill()
            }
        }
        waitForExpectations(timeout: 10.0, handler: nil)
    }

    func testCompanyCollectionNew() {
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        XCTAssertNotNil (collections.companies.new(batch: batch))
    }
    
    func testAddressCreation() {
        let address = Address (street: "Street", city: "City", state: "CA", zipCode: "95010")
        XCTAssertEqual ("Street", address.street)
        XCTAssertEqual ("City", address.city)
        XCTAssertEqual ("CA", address.state)
        XCTAssertEqual ("95010", address.zipCode)
    }

    func testAddressFromCollection() {
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let address = Address (street: "Street", city: "City", state: "CA", zipCode: "95010")
        let addressEntity = collections.addresses.new(batch: batch, item: address)
        addressEntity.sync() { address in
            XCTAssertEqual ("Street", address.street)
            XCTAssertEqual ("City", address.city)
            XCTAssertEqual ("CA", address.state)
            XCTAssertEqual ("95010", address.zipCode)
        }
    }

    func testEmployeeCreation() {
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let company = Company(employeeCollection: collections.employees, id: UUID())
        let batch = EventuallyConsistentBatch()
        let companyEntity = collections.companies.new(batch: batch, item: company)
        let selfReference = EntityReferenceData<Employee> (collection: collections.employees, id: UUID(), version: 0)
        // Without address
        var employee = Employee(selfReference: selfReference, company: companyEntity, name: "Bob Carol", address: nil)
        var referencedCompanyEntity = employee.company.get().item()!
        referencedCompanyEntity.sync() { referencedCompany in
            XCTAssertTrue (referencedCompany === company)
        }
        XCTAssertEqual ("Bob Carol", employee.name)
        XCTAssertNil (employee.address.get().item())
        // With address
        let address = Address (street: "Street", city: "City", state: "CA", zipCode: "95010")
        let addressEntity = collections.addresses.new(batch: batch, item: address)
        employee = Employee(selfReference: selfReference, company: companyEntity, name: "Bob Carol", address: addressEntity)
        referencedCompanyEntity = employee.company.get().item()!
        referencedCompanyEntity.sync() { referencedCompany in
            XCTAssertTrue (referencedCompany === company)
        }
        XCTAssertEqual ("Bob Carol", employee.name)
        XCTAssertTrue (employee.address.get().item() === addressEntity)
        // resetName()
        employee.resetName()
        XCTAssertEqual ("", employee.name)
    }
    
    func testEmployeeFromCollection() {
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let company = Company(employeeCollection: collections.employees, id: UUID())
        let batch = EventuallyConsistentBatch()
        let companyEntity = collections.companies.new(batch: batch, item: company)
        // Without address
        var employeeEntity = collections.employees.new(batch: batch, company: companyEntity, name: "Bob Carol", address: nil)
        employeeEntity.sync() { employee in
            XCTAssertTrue (employee.company.get().item()! === companyEntity)
            XCTAssertEqual ("Bob Carol", employee.name)
            XCTAssertNil (employee.address.get().item())
        }
        let address = Address (street: "Street", city: "City", state: "CA", zipCode: "95010")
        let addressEntity = collections.addresses.new(batch: batch, item: address)
        employeeEntity = collections.employees.new(batch: batch, company: companyEntity, name: "Bob Carol", address: addressEntity)
        employeeEntity.sync() { employee in
            XCTAssertTrue (employee.company.get().item()! === companyEntity)
            XCTAssertEqual ("Bob Carol", employee.name)
            XCTAssertTrue (employee.address.get().item()! === addressEntity)
        }
    }
    
    func testInMemoryAccessorEmployeesForCompany() {
        let accessor = SampleInMemoryAccessor()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = collections.companies.new(batch: batch)
        let companyEntity2 = collections.companies.new(batch: batch)
        let companyEntity3 = collections.companies.new(batch: batch)
        let employeeEntity1 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let waitFor = expectation(description: "wait1")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        companyEntity1.sync() { company in
            switch accessor.employeesForCompany(collection: collections.employees, company: company) {
            case .ok (let company1Employees):
                XCTAssertEqual (2, company1Employees.count)
                XCTAssertTrue (company1Employees[0] === employeeEntity1 || company1Employees[0] === employeeEntity2)
                XCTAssertTrue (company1Employees[1] === employeeEntity1 || company1Employees[1] === employeeEntity2)
            default:
                XCTFail ("expected .ok")
            }
        }
        companyEntity2.sync() { company in
            switch accessor.employeesForCompany(collection: collections.employees, company: company) {
            case .ok (let company2Employees):
                XCTAssertEqual (2, company2Employees.count)
                XCTAssertTrue (company2Employees[0] === employeeEntity3 || company2Employees[0] === employeeEntity4)
                XCTAssertTrue (company2Employees[1] === employeeEntity3 || company2Employees[1] === employeeEntity4)
            default:
                XCTFail ("expected .ok")
            }
        }
        companyEntity3.sync() { company in
            switch accessor.employeesForCompany(collection: collections.employees, company: company) {
            case .ok (let company3Employees):
                XCTAssertEqual (0, company3Employees.count)
            default:
                XCTFail ("expected .ok")
            }
        }
    }

    func testEmployeeCollectionForCompany() {
        let accessor = SampleInMemoryAccessor()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = collections.companies.new(batch: batch)
        let companyEntity2 = collections.companies.new(batch: batch)
        let employeeEntity1 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let waitFor = expectation(description: "wait1")
        batch.commit() {
            waitFor.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        companyEntity1.sync() { company in
            switch collections.employees.forCompany(company) {
            case .ok (let company1Employees):
                XCTAssertEqual (2, company1Employees!.count)
                XCTAssertTrue (company1Employees![0] === employeeEntity1 || company1Employees![0] === employeeEntity2)
                XCTAssertTrue (company1Employees![1] === employeeEntity1 || company1Employees![1] === employeeEntity2)
            default:
                XCTFail ("expected .ok")
            }
        }
        companyEntity2.sync() { company in
            switch collections.employees.forCompany(company) {
            case .ok (let company2Employees):
                XCTAssertEqual (2, company2Employees!.count)
                XCTAssertTrue (company2Employees![0] === employeeEntity3 || company2Employees![0] === employeeEntity4)
                XCTAssertTrue (company2Employees![1] === employeeEntity3 || company2Employees![1] === employeeEntity4)
            default:
                XCTFail ("expected .ok")
            }
        }
    }

    func testEmployeeCollectionForCompanyAsync() {
        let accessor = SampleInMemoryAccessor()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = collections.companies.new(batch: batch)
        let companyEntity2 = collections.companies.new(batch: batch)
        let employeeEntity1 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let waitFor1 = expectation(description: "wait1")
        batch.commit() {
            waitFor1.fulfill()
        }
        waitForExpectations(timeout: 10, handler: nil)
        let waitFor2a = expectation(description: "wait2a")
        let waitFor2b = expectation(description: "wait2b")
        companyEntity1.sync() { company in
            collections.employees.forCompany(company) { result in
                switch result {
                case .ok (let company1Employees):
                    XCTAssertEqual (2, company1Employees!.count)
                    XCTAssertTrue (company1Employees![0] === employeeEntity1 || company1Employees![0] === employeeEntity2)
                    XCTAssertTrue (company1Employees![1] === employeeEntity1 || company1Employees![1] === employeeEntity2)
                default:
                    XCTFail ("expected .ok")
                }
                waitFor2a.fulfill()
            }
        }
        companyEntity2.sync() { company in
            collections.employees.forCompany(company) { result in
                switch result {
                case .ok (let company2Employees):
                    XCTAssertEqual (2, company2Employees!.count)
                    XCTAssertTrue (company2Employees![0] === employeeEntity3 || company2Employees![0] === employeeEntity4)
                    XCTAssertTrue (company2Employees![1] === employeeEntity3 || company2Employees![1] === employeeEntity4)
                default:
                    XCTFail ("expected .ok")
                }
                waitFor2b.fulfill()
            }
        }
        waitForExpectations(timeout: 10, handler: nil)
    }

    func testRemoveAll() {
        let accessor = SampleInMemoryAccessor()
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = collections.companies.new(batch: batch)
        let companyEntity2 = collections.companies.new(batch: batch)
        let employeeEntity1 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = collections.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = collections.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let address1 = collections.addresses.new(batch: batch, item: Address(street: "S1", city: "C1", state: "St1", zipCode: "Z1"))
        let address2 = collections.addresses.new(batch: batch, item: Address(street: "S2", city: "C2", state: "St2", zipCode: "Z2"))
        let address3 = collections.addresses.new(batch: batch, item: Address(street: "S3", city: "C3", state: "St3", zipCode: "Z3"))
        let address4 = collections.addresses.new(batch: batch, item: Address(street: "S4", city: "C4", state: "St4", zipCode: "Z4"))
        employeeEntity1.update(batch: batch) { employee in
            employee.address.set(entity: address1, batch: batch)
        }
        employeeEntity2.update(batch: batch) { employee in
            employee.address.set(entity: address2, batch: batch)
        }
        employeeEntity3.update(batch: batch) { employee in
            employee.address.set(entity: address3, batch: batch)
        }
        employeeEntity4.update(batch: batch) { employee in
            employee.address.set(entity: address4, batch: batch)
        }
        batch.commitSync()
        XCTAssertEqual (2, collections.companies.scan(criteria: nil).item()!.count)
        XCTAssertEqual (4, collections.employees.scan(criteria: nil).item()!.count)
        XCTAssertEqual (4, collections.addresses.scan(criteria: nil).item()!.count)
        SampleTests.removeAll(collections: collections)
        XCTAssertEqual (0, collections.companies.scan(criteria: nil).item()!.count)
        XCTAssertEqual (0, collections.employees.scan(criteria: nil).item()!.count)
        XCTAssertEqual (0, collections.addresses.scan(criteria: nil).item()!.count)
    }
    
}
