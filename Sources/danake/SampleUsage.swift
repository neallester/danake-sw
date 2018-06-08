//
//  SampleUsageTests.swift
//  danakeTests
//
//  Created by Neal Lester on 4/24/18.
//

import Foundation

/*
    ========
    Contents
    ========
 
        1. The Sample Application Model
        2. The Sample Persistence System
        3. Test demonstrating usage within application code
        4. Tests demonstrating deliberate generation of errors by InMemoryAccessor to test error handling in
           application code

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
                                    supported media). Lookup by Entity.id and cache scan (with optional selection criteria) are
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
    func employeesForCompany (cache: EntityCache<Employee>, company: Company) -> DatabaseAccessListResult<Entity<Employee>>
}

class SampleInMemoryAccessor : InMemoryAccessor, SampleAccessor {
    func employeesForCompany (cache: EntityCache<Employee>, company: Company) -> DatabaseAccessListResult<Entity<Employee>> {
        let retrievalResult = scan (type: Entity<Employee>.self, cache: cache)
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
        forCompanyClosure = { cache, company in
            return database.sampleAccessor.employeesForCompany (cache: cache, company: company)
        }
        super.init (database: database, name: "employee", userInfoClosure: nil)
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

/**
    An executable test intended to demonstrate the usage of the danake framework in application code.
    This is included in the main library so that it is available for testing DatabaseAccessors implementated
    in other packages.
 */
class SampleUsage  {
    
    // Execute the `runSample' test using the SampleInMemoryAccessor
    
/*
     ================================================
     Test demonstrating usage within application code
     ================================================
*/

/**
     Execute the sample code.
     
     - returns: **True** if all operations succeeded; **False** if a failure occurred
*/
    static func runSample (accessor: SampleAccessor) -> Bool {
        var overallTestResult = TestResult()
        // Application code can run with any DatabaseAccessor implementing SampleAccessor
        
        // Declare SampleCollections with `let'
        // See class Database header comment for explanation of `schemaVersion'
        let logger = InMemoryLogger()
        let caches = SampleCollections (accessor: accessor, schemaVersion: 1, logger: logger)
        
        // Creating a database logs INFO
        logger.sync() { entries in
            ParallelTest.AssertEqual (testResult: &overallTestResult, 1, entries.count)
            ParallelTest.AssertEqual (testResult: &overallTestResult, "INFO|SampleDatabase.init|created|hashValue=\(caches.employees.database.accessor.hashValue)", entries[0].asTestString())
        }

        // Start test in known state by ensuring persistent media is empty
        removeAll(caches: caches)
        
        var company1id: UUID?

        // Setup an arbitrary scope for illustration purposes
        do {
            // batch may be declared with `let' or `var'
            // However, do not recreate batches which contain uncommitted updates
            // batch retry and timeout intervals are also settable; see
            // EventuallyConsistentBatch class header comment for these and other details
            // In application code always create batches with a logger
            let batch = EventuallyConsistentBatch(logger: caches.logger)
            
            let company1 = caches.companies.new(batch: batch)
            company1id = company1.id
            let company2 = caches.companies.new(batch: batch)
            
            // new objects are not available in persistent media until batch is committed
            if let companies = caches.companies.scan().item() {
                ParallelTest.AssertEqual (testResult: &overallTestResult, 0, companies.count)
            } else {
                // Retrieval error
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid item")
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
            if let companies = caches.companies.scan().item() {
                ParallelTest.AssertEqual (testResult: &overallTestResult, 2, companies.count)
            } else {
                // Retrieval error
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid item")
            }
            
            // There is only one instance representing any particular persistent Entity
            // `get' queries will return cached entities (if available) before hitting persistent media.
            // However, `scan' queries retrieving already cached entities are
            // still expensive as they still go persistent media (cache normalization for scans does
            // not occur until after the persistent objects are retrieved and partially deserialized)
            if let entity = caches.companies.get(id: company1.id).item() {
                ParallelTest.AssertTrue (testResult: &overallTestResult, company1 === entity)
            } else {
                // Retrieval error
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid item")
            }
            
            // Because EntityCache.get() can be expensive,
            // use asynchronous version when possible
            group.enter()
            caches.companies.get(id: company2.id) { retrievalResult in
                if let company = retrievalResult.item() {
                    ParallelTest.AssertTrue (testResult: &overallTestResult, company === company2)
                } else {
                    // Retrieval error
                    ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid")
                }
                group.leave()
            }
            group.wait()
            
            // Asynchronous version is also preferred for EntityCache.scan()
            group.enter()
            caches.companies.scan() { retrievalResult in
                if let companies = retrievalResult.item() {
                    ParallelTest.AssertEqual (testResult: &overallTestResult, 2, companies.count)
                } else {
                    // Retrieval error
                    ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid")
                }
                group.leave()
            }
            group.wait()

            // If a successful query may return nil, explicitly use retrievialResult instead of .item()
            group.enter()
            let badId = UUID()
            caches.companies.get(id: badId) { retrievalResult in
                switch retrievalResult {
                case .ok (let company):
                    ParallelTest.AssertNil (testResult: &overallTestResult, company)
                default:
                    // Retrieval error
                    ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
                }
                group.leave()
            }
            group.wait()
            
            // An unsuccessful EntityCache.get logs a WARNING
            logger.sync() { entries in
                ParallelTest.AssertEqual (testResult: &overallTestResult, 2, entries.count)
                ParallelTest.AssertEqual (testResult: &overallTestResult, "WARNING|CompanyCollection.get|Unknown id|databaseHashValue=\(caches.employees.database.accessor.hashValue);cache=company;id=\(badId.uuidString)", entries[1].asTestString())
            }

            // Retrieving persisted objects by criteria
            // The default implementation retrieves and deserializes all entries in a cache/table
            // before filtering the results.
            let scanResult = caches.companies.scan() { company in
                company.id.uuidString == company2.id.uuidString
            }
            if let companies = scanResult.item() {
                ParallelTest.AssertEqual (testResult: &overallTestResult, 1, companies.count)
                ParallelTest.AssertEqual (testResult: &overallTestResult, companies[0].id.uuidString, company2.id.uuidString)
            } else {
                // Retrieval error
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid item")
            }

            // after commit() the batch is left as a fresh empty batch and may be reused
            let _ = caches.employees.new(batch: batch, company: company1, name: "Name One", address: nil)
            var employee2: Entity<Employee>? = caches.employees.new(batch: batch, company: company2, name: "Name Two", address: nil)
            batch.commitSync()
            // Updating the name attribute of Employee and setting a new reference for its address
            let address1 = caches.addresses.new (batch: batch, item: Address(street: "Street 1", city: "City 1", state: "CA", zipCode: "94377"))
            let address2 = caches.addresses.new (batch: batch, item: Address(street: "Street 2", city: "City 2", state: "CA", zipCode: "94377"))
            let address3 = caches.addresses.new (batch: batch, item: Address(street: "Street 3", city: "City 3", state: "CA", zipCode: "94377"))
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
            ParallelTest.AssertNil (testResult: &overallTestResult, employeeItem)

            // Closures which modify an item's state via functions must always be called within an
            // Entity.update() call. Failure to do so will cause lost data (which will be logged when
            // the entity is deallocated; this error is not demonstrated here)
            employee2!.update(batch: batch) { employee in
                employee.resetName()
            }
            batch.commitSync()
            logger.sync() { entries in
                ParallelTest.AssertEqual (testResult: &overallTestResult, 2, entries.count)
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
            if let companyEntity = caches.companies.get (id: company1id!).item() {
                
                
                companyEntity.sync() { company in
                    if let employeeEntity = company.employees().item()?[0] {
                        lostChangesEmployeeUuidString = employeeEntity.id.uuidString
                        employeeEntity.update(batch: batch) { employee in
                            employee.name = "Name Updated1"
                            ParallelTest.AssertEqual (testResult: &overallTestResult, "Name Updated1", employee.name)
                        }
                    } else {
                        // Retrieval error
                        ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid")
                    }
                }
            } else {
                // Retrieval error
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid")
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
            caches.employees.sync() { entities in
                hasCached = entities[lostChangesEmployeeUUID]?.codable != nil
                usleep(100)
            }
        }
        
        // An error has indeed been logged and the update has indeed been lost
        logger.sync() { entries in
            ParallelTest.AssertEqual (testResult: &overallTestResult, 3, entries.count)
            ParallelTest.AssertEqual (testResult: &overallTestResult, "ERROR|BatchDelegate.deinit|notCommitted:lostData|entityType=Entity<Employee>;entityId=\(lostChangesEmployeeUuidString);entityPersistenceState=dirty", entries[2].asTestString())
        }
        do {
            if let companyEntity = caches.companies.get (id: company1id!).item() {
                companyEntity.sync() { company in
                    if let employeeEntity = company.employees().item()?[0] {
                        
                        
                        employeeEntity.sync() { employee in
                            ParallelTest.AssertEqual (testResult: &overallTestResult, "Name One", employee.name)
                        }
                    } else {
                        // Retrieval error
                        ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid")
                    }
                }
            } else {
                // Retrieval error
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected valid")
            }
        }

        // Clean up
        removeAll(caches: caches)
        return !overallTestResult.failed
    }
    
/*
     ===========================================================
     Tests demonstrating deliberate generation of errors by
     InMemoryAccessor to test error handling in application code
     ===========================================================
*/
    public static func demonstrateThrowError() -> Bool {
        var overallTestResult = TestResult()
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let caches = SampleCollections(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: caches.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
        }
        // Use InMemoryAccessor.setThrowError() to simulate persistent media errors for testing
        inMemoryAccessor.setThrowError()
        switch caches.companies.get(id: companyId) {
        case .error(let errorMessage):
            ParallelTest.AssertEqual (testResult: &overallTestResult, "getError", errorMessage)
            logger.sync() { entries in
                ParallelTest.AssertEqual (testResult: &overallTestResult, 1, entries.count)
                ParallelTest.AssertEqual (testResult: &overallTestResult, "EMERGENCY|CompanyCollection.get|Database Error|databaseHashValue=\(caches.companies.database.accessor.hashValue);cache=company;id=\(companyId.uuidString);errorMessage=getError", entries[0].asTestString())
            }
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .error")
        }
        // Only one error will be thrown; subsequent operations will succeed
        switch caches.companies.get(id: companyId) {
        case .ok (let company):
            ParallelTest.AssertEqual (testResult: &overallTestResult, company!.id.uuidString, companyId.uuidString)
            logger.sync() { entries in
                ParallelTest.AssertEqual (testResult: &overallTestResult, 1, entries.count)
            }
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
        }
        return !overallTestResult.failed
    }

    /*
        Use InMemoryAccessor.prefetch to control the timing of persistent media operations
     */
    public static func testDemonstratePrefetchWithGet () -> Bool {
        var overallTestResult = TestResult()
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let caches = SampleCollections(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: caches.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
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
        caches.companies.get(id: companyId) { retrievalResult in
            switch retrievalResult {
            case .ok (let company):
                ParallelTest.AssertEqual (testResult: &overallTestResult, company!.id.uuidString, companyId.uuidString)
                retrievedCompany = company
            default:
                ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
            }
            group.leave()
        }
        // Company has not yet been retrieved
        ParallelTest.AssertNil (testResult: &overallTestResult, retrievedCompany)
        semaphore.signal()
        group.wait()
        // Company has now been retrieved
        ParallelTest.AssertEqual (testResult: &overallTestResult, retrievedCompany!.id.uuidString, companyId.uuidString)
        logger.sync() { entries in
            ParallelTest.AssertEqual (testResult: &overallTestResult, 0, entries.count)
        }
        return !overallTestResult.failed
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
    public static func testDemonstrateUpdateErrors () -> Bool {
        var overallTestResult = TestResult()
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let caches = SampleCollections(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: caches.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
        }
        let employeeId = UUID(uuidString: "05081CBC-5ABA-4EE9-A7B1-4882E047D715")!
        let employeeJson = "{\"id\":\"\(employeeId.uuidString)\",\"schemaVersion\":1,\"created\":1525459064.9665,\"saved\":1525459184.5832,\"item\":{\"name\":\"Name Two\",\"company\":{\"databaseId\":\"\(inMemoryAccessor.hashValue)\",\"id\":\"\(companyId.uuidString)\",\"isEager\":true,\"cacheName\":\"company\",\"version\":1},\"address\":{\"isEager\":false,\"isNil\":true}},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: caches.employees.name, id: employeeId, data: employeeJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
        }
        let batch = EventuallyConsistentBatch (retryInterval: .milliseconds(10), timeout: .seconds(60), logger: logger)
        var batchIdString = batch.delegateId().uuidString
        
        if let employeeEntity = caches.employees.get(id: employeeId).item() {
            // Update employee name
            employeeEntity.update(batch: batch) { employee in
                employee.name = "Name Updated1"
                ParallelTest.AssertEqual (testResult: &overallTestResult, "Name Updated1", employee.name)
            }

            // Using setThrowError() before committing an update will throw an unrecoverable serialization error
            inMemoryAccessor.setThrowError()
            batch.commitSync()
            logger.sync() { entries in
                ParallelTest.AssertEqual (testResult: &overallTestResult, 1, entries.count)
                ParallelTest.AssertEqual (testResult: &overallTestResult, "ERROR|BatchDelegate.commit|Database.unrecoverableError(\"addActionError\")|entityType=Entity<Employee>;entityId=\(employeeId.uuidString);batchId=\(batchIdString)", entries[0].asTestString())
            }
            
            // Demonstrate that the previous changes were lost due to the reported unrecoverable error
            #if os(Linux)
                // The order of attributes on serialized JSON is not always consistent on Linux
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(employeeId.uuidString)\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"schemaVersion\":1"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"created\":1525459064.9665"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"saved\":1525459184.5832"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"item\":{"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"name\":\"Name Two\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"company\":{"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"databaseId\":\"\(inMemoryAccessor.hashValue)\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(companyId.uuidString)\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"isEager\":true"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"cacheName\":\"company\",\"version\":1}"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"version\":1}"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"address\":{"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"isEager\":false"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"isNil\":true"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"persistenceState\":\"persistent\""))
            #else
                ParallelTest.AssertEqual (testResult: &overallTestResult, employeeJson, String (data: inMemoryAccessor.getData(name: caches.employees.name, id: employeeId)!, encoding: .utf8))
            #endif
            ParallelTest.AssertFalse (testResult: &overallTestResult, inMemoryAccessor.isThrowError())

            // Modify the employee again so that it is again added to the batch
            employeeEntity.update(batch: batch) { employee in
                employee.name = "Name Updated2"
                ParallelTest.AssertEqual (testResult: &overallTestResult, "Name Updated2", employee.name)
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
                ParallelTest.AssertEqual (testResult: &overallTestResult, 2, entries.count)
                ParallelTest.AssertEqual (testResult: &overallTestResult, "EMERGENCY|BatchDelegate.commit|Database.error(\"addError\")|entityType=Entity<Employee>;entityId=\(employeeId.uuidString);batchId=\(batchIdString)", entries[1].asTestString())
            }
            // Demonstrate that the data was updated in persistent media
            #if os(Linux)
                // The order of attributes on serialized JSON is not always consistent on Linux
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(employeeId.uuidString)\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"schemaVersion\":1"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"created\":1525459064.9665"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"saved\":1525459184.5832"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"item\":{"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"name\":\"Name Two\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"company\":{"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"databaseId\":\"\(inMemoryAccessor.hashValue)\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(companyId.uuidString)\""))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"isEager\":true"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"cacheName\":\"company\",\"version\":1}"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"version\":1}"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"address\":{"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"isEager\":false"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"isNil\":true"))
                ParallelTest.AssertTrue (testResult: &overallTestResult, employeeJson.contains("\"persistenceState\":\"persistent\""))
            #else
            ParallelTest.AssertNotEqual (testResult: &overallTestResult, employeeJson, String (data: inMemoryAccessor.getData(name: caches.employees.name, id: employeeId)!, encoding: .utf8))
            #endif
        }
        return !overallTestResult.failed
    }

    static func removeAll (caches: SampleCollections) {
        let batch = EventuallyConsistentBatch()
        for entity in caches.companies.scan(criteria: nil).item()! {
            entity.remove(batch: batch)
        }
        for entity in caches.employees.scan(criteria: nil).item()! {
            entity.remove(batch: batch)
        }
        for entity in caches.addresses.scan(criteria: nil).item()! {
            entity.remove(batch: batch)
        }
        batch.commitSync()
    }
        
}
