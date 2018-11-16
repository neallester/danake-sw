//
//  SampleUsageTests.swift
//  danakeTests
//
//  Created by Neal Lester on 4/24/18.
//

import Foundation
import PromiseKit

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
 
    class SampleCompany:  A SampleCompany is associated with zero or more Employees.
                          - Property `employeeCache' demonstrates how to deserialize a property whose value
                            comes from the environment rather than the the serialized representation of the instance.
                          - property `id' demonstrates how to create a model object with an attribute of type UUID
                            value which is taken from the id of the enclosing Entity at creation and deserialization
                            time.
                          - The function `employees()' demonstrate functions whose results are obtained via queries to
                            the persistent media.
 
    class SampleEmployee: An sampleEmployee is associated with zero or one companies, and has zero or one addresses.
                          - Properties `sampleCompany' and `sampleAddress' demonstrate the usage of EntityReference<PARENT, TYPE>
                          - sampleCompany attribute demonstrates eager retrieval

    class SampleAddress:  Demonstrates the use of a struct as a reference.
 
 */

public class SampleCompany : Codable {
    
    enum CodingKeys: CodingKey {}
    
    init (employeeCache: SampleEmployeeCache, id: UUID) {
        self.id = id
        self.employeeCache = employeeCache
    }
    
    // Custom decoder sets the `employeeCache' and `id' during deserialization
    public required init (from decoder: Decoder) throws {
        // employeeCache set by persistence system. See CompanyCache.init()
        if let employeeCache = decoder.userInfo[SampleCompany.employeeCacheKey] as? SampleEmployeeCache {
            self.employeeCache = employeeCache
        } else {
            throw EntityDeserializationError<SampleCompany>.missingUserInfoValue(SampleCompany.employeeCacheKey)
        }
        // Use the enclosing entities value of `id'
        // Same method may be used to obtain schemaVersion at time of last save
        if let container = decoder.userInfo[Database.parentDataKey] as? DataContainer, let parentReferenceData = container.data as? EntityReferenceData<SampleCompany> {
            self.id = parentReferenceData.id
        } else {
            throw EntityDeserializationError<SampleCompany>.missingUserInfoValue(Database.parentDataKey)
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var _ = encoder.container(keyedBy: CodingKeys.self)
    }
    
    // With this design the results of the `employees' function will not include any newly
    // created sampleEmployee objects which have not yet been saved to the persistent media
    
    // Syncrhonous implementation
    public func employeesSync() throws -> [Entity<SampleEmployee>] {
        return try employeeCache.forCompanySync(self)
    }
    
    // Asyncrhonous implementation
    public func employees() -> Promise<[Entity<SampleEmployee>]> {
        return employeeCache.forCompany(self)
    }
    
    // In actual use within the application; id will match the id of the surrounding Entity wrapper
    public let id: UUID
    
    private let employeeCache: SampleEmployeeCache
    public static let employeeCacheKey = CodingUserInfoKey.init(rawValue: "employeeCacheKey")!

}

public class SampleEmployee : Codable {
    
    init (selfReference: EntityReferenceData<SampleEmployee>, company: Entity<SampleCompany>, name: String, address: Entity<SampleAddress>?) {
        self.name = name
        // sampleCompany reference is created with eager retrieval
        self.company = ReferenceManager<SampleEmployee, SampleCompany> (parent: selfReference, entity: company, isEager: true)
        self.address = ReferenceManager<SampleEmployee, SampleAddress> (parent: selfReference, entity: address)
    }

    var name: String
    
    func resetName() {
        name = ""
    }
    
    // Always declare attributes of type 'EntityReference' using 'let'
    // The entities which are referenced may change, but the EntityReference itself does not
    let company: ReferenceManager<SampleEmployee, SampleCompany>
    let address: ReferenceManager<SampleEmployee, SampleAddress>
    
}

public struct SampleAddress : Codable {
    
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
                                    associated sampleCompany. A media specific implementation (e.g. SampleInMemoryAccessor) must be provided
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
 
    class SampleCompanyCache:       Access to persistent SampleCompany objects. Demonstrates:
                                    - creation of model objects which include an id whose value matches the id of their enclosing
                                      Entity
                                    - setup of a EntityCache to support deserialization of objects which include properties
                                      which are populated from the environment rather than the serialized data.
                                    - custom `new' function for creating new Entity<SampleCompany> objects
 
    class SampleEmployeeCache:      Access to persistent SampleEmployee objects. Demonstrates:
                                    - deserialization of objects which include EntityReference properties
                                    - custom synchronous and asynchronous retrieval functions
 
    class SampleCaches:             An application specific convenience class for organizing all of the EntityCache objects
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

public protocol SampleAccessor : DatabaseAccessor {

    func employeesForCompany (cache: EntityCache<SampleEmployee>, company: SampleCompany) throws -> [Entity<SampleEmployee>]

}

class SampleInMemoryAccessor : InMemoryAccessor, SampleAccessor {
    
    func employeesForCompany (cache: EntityCache<SampleEmployee>, company: SampleCompany) throws -> [Entity<SampleEmployee>] {
        let retrievalResult = try scan (type: Entity<SampleEmployee>.self, cache: cache)
        return retrievalResult.filter() { employeeEntity in
            var shouldInclude = false
            employeeEntity.sync() { sampleEmployee in
                if sampleEmployee.company.entityId()!.uuidString == company.id.uuidString {
                    shouldInclude = true
                }
            }
            return shouldInclude
        }
    }
    
}

public class SampleCompanyCache : EntityCache<SampleCompany> {
    
    init (database: SampleDatabase, employeeCache: SampleEmployeeCache) {
        self.employeeCache = employeeCache
        // The following closure is fired on the decoders userInfo property before deserialization
        // setting the employeeCache object which will be assigned to the appropriate property
        // during deserialization (see SampleCompany.init (decoder:)
        super.init (database: database, name: "sampleCompany") { userInfo in
            userInfo[SampleCompany.employeeCacheKey] = employeeCache
        }
    }
    
    func new (batch: EventuallyConsistentBatch) -> Entity<SampleCompany> {
        return new (batch: batch) { selfReference in
            return SampleCompany (employeeCache: employeeCache, id: selfReference.id)
        }
    }
    
    private let employeeCache: SampleEmployeeCache
}

public class SampleEmployeeCache : EntityCache<SampleEmployee> {

    init(database: SampleDatabase) {
        forCompanyClosure = { cache, sampleCompany in
            return try database.sampleAccessor.employeesForCompany (cache: cache, company: sampleCompany)
        }
        super.init (database: database, name: "sampleEmployee", userInfoClosure: nil)
    }
    
    func new (batch: EventuallyConsistentBatch, company: Entity<SampleCompany>, name: String, address: Entity<SampleAddress>?) -> Entity<SampleEmployee> {
        return new (batch: batch) { selfReference in
            return SampleEmployee (selfReference: selfReference, company: company, name: name, address: address)
        }
    }
    
    func forCompanySync (_ company: SampleCompany) throws -> [Entity<SampleEmployee>] {
        return try forCompanyClosure (self, company)
    }
    
    func forCompany (_ company: SampleCompany) -> Promise<[Entity<SampleEmployee>]> {
        return Promise { seal in
            database.workQueue.async {
                do {
                    try seal.fulfill(self.forCompanySync(company))
                } catch {
                    seal.reject(error)
                }
            }
        }
    }
    
    private let forCompanyClosure: ((SampleEmployeeCache, SampleCompany) throws -> [Entity<SampleEmployee>])
    
}

public class SampleCaches {
    
    init (accessor: SampleAccessor, schemaVersion: Int, logger: Logger?) {
        let database = SampleDatabase (accessor: accessor, schemaVersion: schemaVersion, logger: logger, referenceRetryInterval: 180.0)
        self.logger = logger
        employees = SampleEmployeeCache (database: database)
        companies = SampleCompanyCache (database: database, employeeCache: employees)
        addresses = EntityCache<SampleAddress> (database: database, name: "sampleAddress")
    }
    
    public let logger: Logger?
    
    public let companies: SampleCompanyCache
    
    public let employees: SampleEmployeeCache
    
    public let addresses: EntityCache<SampleAddress>
    
}

/**
    An executable test intended to demonstrate the usage of the danake framework in application code.
    This is included in the main library so that it is available for testing DatabaseAccessors implementated
    in other packages.
 */
public class SampleUsage  {
    
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
    public static func runSample (accessor: SampleAccessor) -> Bool {
        var overallTestResult = TestResult()
        // Application code can run with any DatabaseAccessor implementing SampleAccessor
        
        // Declare SampleCaches with `let'
        // See class Database header comment for explanation of `schemaVersion'
        let logger = InMemoryLogger()
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: logger)
        
        // Creating a database logs INFO
        logger.sync() { entries in
            ParallelTest.AssertEqual (label: "runSample.1", testResult: &overallTestResult, 1, entries.count)
            ParallelTest.AssertEqual (label: "runSample.2", testResult: &overallTestResult, "INFO|SampleDatabase.init|created|hashValue=\(caches.employees.database.accessor.hashValue)", entries[0].asTestString())
        }

        // Start test in known state by ensuring persistent media is empty
        do {
            try removeAll(caches: caches)
        } catch {
            ParallelTest.Fail(testResult: &overallTestResult, message: "Unable to removeAll()")
        }
        
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
            // Performing the query in synchronous style
            do {
                let companies = try caches.companies.scanSync()
                ParallelTest.AssertEqual (label: "runSample.3", testResult: &overallTestResult, 0, companies.count)
            } catch {
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.4: Expected valid item")
            }

            // Performing the query in asynchronous style
            
            let group = DispatchGroup()
            group.enter()
            firstly {
                caches.companies.scan()
            }.done { (companies: [Entity<SampleCompany>]) in
                ParallelTest.AssertEqual (label: "runSample.3async", testResult: &overallTestResult, 0, companies.count)
            }.ensure {
                group.leave()
            }.catch { error in
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.4async: Expected valid item")
            }
            group.wait()
            
            // Batch.commit() is asyncrhonous
            // use Batch.commitSync() to wait for batch processing
            group.enter()
            batch.commit() {
                group.leave()
            }
            group.wait()
            
            // After commit our companies are in the persistent media
            do {
                let companies = try caches.companies.scanSync()
                ParallelTest.AssertEqual (label: "runSample.5", testResult: &overallTestResult, 2, companies.count)
            } catch {
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.6: Expected valid item")
            }
            
            // There is only one instance representing any particular persistent Entity
            // `get' queries will return cached entities (if available) before hitting persistent media.
            // However, `scan' queries retrieving already cached entities are
            // still expensive as they still go persistent media (cache normalization for scans does
            // not occur until after the persistent objects are retrieved and partially deserialized)
            do {
                let entity = try caches.companies.getSync(id: company1.id)
                ParallelTest.AssertTrue (label: "runSample.7", testResult: &overallTestResult, company1 === entity)
            } catch {
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.8: Expected valid item")
            }
            
            // Because EntityCache.get() can be expensive,
            // use asynchronous version when possible
            group.enter()
            firstly {
                caches.companies.get(id: company2.id)
            }.done { sampleCompanyEntity in
                ParallelTest.AssertTrue (label: "runSample.9", testResult: &overallTestResult, sampleCompanyEntity === company2)
            }.ensure {
                group.leave()
            }.catch { error in
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.10: Expected valid (\(error))")
            }
            group.wait()
            
            // Asynchronous version is also preferred for EntityCache.scan()
            group.enter()
            firstly {
                caches.companies.scan()
            }.done { companies in
                ParallelTest.AssertEqual (label: "runSample.11", testResult: &overallTestResult, 2, companies.count)
            }.ensure {
                group.leave()
            }.catch { error in
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.12: Expected valid")
            }
            group.wait()

            // Attempting to retrieve an entity with a bad ID throws an error
            group.enter()
            let badId = UUID()
            firstly {
                caches.companies.get(id: badId)
            }.done { retrievalResult in
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.13: Expected Error")
            }.ensure {
                group.leave()
            }.catch { error in
                ParallelTest.AssertEqual(label: "runSample.14", testResult: &overallTestResult, "\(AccessorError.unknownUUID)", "\(error)")
            }
            group.wait()
            
            // And the unsuccessful EntityCache.get logs a WARNING
            logger.sync() { entries in
                ParallelTest.AssertEqual (label: "runSample.15", testResult: &overallTestResult, 2, entries.count)
                ParallelTest.AssertEqual (label: "runSample.16", testResult: &overallTestResult, "WARNING|SampleCompanyCache.get|Unknown id|databaseHashValue=\(caches.employees.database.accessor.hashValue);cache=sampleCompany;id=\(badId.uuidString)", entries[1].asTestString())
            }

            // Retrieving persisted objects by criteria
            // The default implementation retrieves and deserializes all entries in a cache/table
            // before filtering the results.
            group.enter()
            firstly {
                caches.companies.scan() { sampleCompany in
                    sampleCompany.id.uuidString == company2.id.uuidString
                }
            }.done { companies in
                ParallelTest.AssertEqual (label: "runSample.17", testResult: &overallTestResult, 1, companies.count)
                ParallelTest.AssertEqual (label: "runSample.18", testResult: &overallTestResult, companies[0].id.uuidString, company2.id.uuidString)
            }.ensure {
                group.leave()
            }.catch { error in
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.19: Expected valid item")
            }
            group.wait()
            
            
            // after commit() the batch is left as a fresh empty batch and may be reused
            let _ = caches.employees.new(batch: batch, company: company1, name: "Name One", address: nil)
            var employee2: Entity<SampleEmployee>? = caches.employees.new(batch: batch, company: company2, name: "Name Two", address: nil)
            batch.commitSync()
            // Updating the name attribute of SampleEmployee and setting a new reference for its sampleAddress
            let address1 = caches.addresses.new (batch: batch, item: SampleAddress(street: "Street 1", city: "City 1", state: "CA", zipCode: "94377"))
            let address2 = caches.addresses.new (batch: batch, item: SampleAddress(street: "Street 2", city: "City 2", state: "CA", zipCode: "94377"))
            let address3 = caches.addresses.new (batch: batch, item: SampleAddress(street: "Street 3", city: "City 3", state: "CA", zipCode: "94377"))
            batch.commitSync()
            employee2!.update(batch: batch) { sampleEmployee in
                sampleEmployee.name = "Name Updated2"
                sampleEmployee.address.set(entity: address1, batch: batch)
            }
            batch.commitSync()

            // EntityReference may be also be updated within a synchronous access to the
            // parent entity
            employee2!.sync() { sampleEmployee in
                sampleEmployee.address.set(entity: address2, batch: batch)
            }
            batch.commitSync()

            // EntityReference may be updated within an asynchronous access, but it is the
            // application developer's responsibility to ensure the update occurs before
            // the batch is committed

            group.enter()
            employee2!.async() { sampleEmployee in
                sampleEmployee.address.set(entity: address3, batch: batch)
                group.leave()
            }
            group.wait()
            batch.commitSync()
            
            // Retrieving Employees of a Company using a promise chain
            group.enter()
            firstly {
                caches.companies.get(id: company2.id)
            }.then { (companyEntity: Entity<SampleCompany>) -> Promise<[Entity<SampleEmployee>]> in
                return companyEntity.promiseFromItem() { company in
                    company.employees()
                }
            }.done { employees in
                ParallelTest.AssertEqual (label: "runSample.20", testResult: &overallTestResult, 1, employees.count)
                ParallelTest.AssertTrue(label: "runSample.21", testResult: &overallTestResult, employees[0] === employee2)
            }.ensure {
                group.leave()
            }.catch { error in
                ParallelTest.Fail(testResult: &overallTestResult, message: "runSample.22: Expected success but got \(error)")
            }
            group.wait()
            
            //
            
            // Gotchas
            
            var employeeItem: SampleEmployee?
            employeeItem = nil
            employee2!.sync() { sampleEmployee in
                // Capturing a reference to an entity's item outside of the
                // Entity.sync(), Entity.async(), or Entity.update() closures
                // bypasses the multi-threading protection offered by the Entity wrapper.
                // i.e. avoid doing the following:
                // employeeItem = sampleEmployee
            }
            ParallelTest.AssertNil (label: "runSample.23", testResult: &overallTestResult, employeeItem)

            // Closures which modify an item's state via functions must always be called within an
            // Entity.update() call. Failure to do so will cause lost data (which will be logged when
            // the entity is deallocated; this error is not demonstrated here)
            employee2!.update(batch: batch) { sampleEmployee in
                sampleEmployee.resetName()
            }
            batch.commitSync()
            logger.sync() { entries in
                ParallelTest.AssertEqual (label: "runSample.24", testResult: &overallTestResult, 2, entries.count)
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
            employee2!.sync() { sampleEmployee in
                do {
                    let _ = try sampleEmployee.company.getSync()
                } catch {
                    ParallelTest.Fail(testResult: &overallTestResult, message: "runSample.24b")
                }
                
            }
            employee2!.breakReferences() // unloads the Entity<SampleCompany>;makes employee2.item.sampleCompany unusuable
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
            do {
                let companyEntity = try caches.companies.getSync (id: company1id!)
                companyEntity.sync() { sampleCompany in
                    do {
                        let employees = try sampleCompany.employeesSync()
                        let employeeEntity = employees[0]
                        lostChangesEmployeeUuidString = employeeEntity.id.uuidString
                        employeeEntity.update(batch: batch) { sampleEmployee in
                            sampleEmployee.name = "Name Updated1"
                            ParallelTest.AssertEqual (label: "runSample.25", testResult: &overallTestResult, "Name Updated1", sampleEmployee.name)
                        }
                    } catch {
                        ParallelTest.Fail(testResult: &overallTestResult, message: "runSample.26: Expected .ok")
                    }
                }
            } catch {
                ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.27: Expected valid")
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
            ParallelTest.AssertEqual (label: "runSample.28", testResult: &overallTestResult, 4, entries.count)
            ParallelTest.AssertEqual (label: "runSample.29", testResult: &overallTestResult, "ERROR|BatchDelegate.deinit|notCommitted:lostData|entityType=Entity<SampleEmployee>;entityId=\(lostChangesEmployeeUuidString);entityPersistenceState=dirty", entries[2].asTestString())
            ParallelTest.AssertEqual (label: "runSample.30", testResult: &overallTestResult, "ERROR|Entity<SampleEmployee>.Type.deinit|lostData:itemModifiedBatchAbandoned|cacheName=\(caches.employees.database.accessor.hashValue).sampleEmployee;entityId=\(lostChangesEmployeeUuidString)", entries[3].asTestString())
        }
        do {
            let companyEntity = try caches.companies.getSync (id: company1id!)
            companyEntity.sync() { sampleCompany in
                do {
                    let employees = try sampleCompany.employeesSync()
                    employees[0].sync() { sampleEmployee in
                        ParallelTest.AssertEqual (label: "runSample.31", testResult: &overallTestResult, "Name One", sampleEmployee.name)
                    }
                } catch {
                    ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.32: Expected .ok")
                }
            }
        } catch {
            ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.33: Expected valid")
        }

        // Clean up
        do {
            try removeAll(caches: caches)
        } catch {
            ParallelTest.Fail (testResult: &overallTestResult, message: "runSample.34: Unable to perform final cleanup")
        }
        return !overallTestResult.isFailed()
    }
    
/*
     ===========================================================
     Tests demonstrating deliberate generation of errors by
     InMemoryAccessor to test error handling in application code
     ===========================================================
*/
    static func demonstrateThrowError() -> Bool {
        var overallTestResult = TestResult()
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let caches = SampleCaches(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
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
        let group = DispatchGroup()
        group.enter()
        inMemoryAccessor.setThrowError()
        firstly {
            caches.companies.get(id: companyId)
        }.done { companyEntity in
            ParallelTest.Fail (testResult: &overallTestResult, message: "demonstrateThrowerror.4: Expected error")
        }.ensure {
            group.leave()
        }.catch { error in
            ParallelTest.AssertEqual (label: "demonstrateThrowError.1", testResult: &overallTestResult, "getError", "\(error)")
            logger.sync() { entries in
                ParallelTest.AssertEqual (label: "demonstrateThrowError.2", testResult: &overallTestResult, 1, entries.count)
                ParallelTest.AssertEqual (label: "demonstrateThrowError.3", testResult: &overallTestResult, "EMERGENCY|SampleCompanyCache.get|Database Error|databaseHashValue=\(caches.companies.database.accessor.hashValue);cache=sampleCompany;id=\(companyId.uuidString);errorMessage=getError", entries[0].asTestString())
            }
        }
        group.wait()
        
        // Only one error will be thrown; subsequent operations will succeed
        firstly {
            caches.companies.get(id: companyId)
        }.done { sampleCompany in
            ParallelTest.AssertEqual (label: "demonstrateThrowError.5", testResult: &overallTestResult, sampleCompany.id.uuidString, companyId.uuidString)
            logger.sync() { entries in
                ParallelTest.AssertEqual (label: "demonstrateThrowError.6", testResult: &overallTestResult, 1, entries.count)
            }
        }.ensure {
            group.leave()
        }.catch { error in
            ParallelTest.Fail (testResult: &overallTestResult, message: "demonstrateThrowError.7: Expected success")
        }
        group.leave()
        
        return !overallTestResult.isFailed()
    }

    /*
        Use InMemoryAccessor.prefetch to control the timing of persistent media operations
     */
    static func testDemonstratePrefetchWithGet () -> Bool {
        var overallTestResult = TestResult()
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let caches = SampleCaches(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
        // For testing purposes: create and preload data into the database bypassing the persistence system
        let companyId = UUID(uuidString: "D175D409-7189-4375-A0A7-29916A08FD19")!
        let companyJson = "{\"id\":\"\(companyId.uuidString)\",\"schemaVersion\":1,\"created\":1525454726.7684,\"saved\":1525454841.3895,\"item\":{},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: caches.companies.name, id: companyId, data: companyJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "testDemonstratePrefetchWithGet.1: Expected .ok")
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
        var retrievedCompany: Entity<SampleCompany>? = nil
        let group = DispatchGroup()
        group.enter()
        firstly {
            caches.companies.get(id: companyId)
        }.done { sampleCompany in
            ParallelTest.AssertEqual (label: "testDemonstratePrefetchWithGet.2", testResult: &overallTestResult, sampleCompany.id.uuidString, companyId.uuidString)
            retrievedCompany = sampleCompany
        }.ensure {
            group.leave()
        }.catch { error in
            ParallelTest.Fail (testResult: &overallTestResult, message: "testDemonstratePrefetchWithGet.3: Expected .ok")
        }
        // SampleCompany has not yet been retrieved
        ParallelTest.AssertNil (label: "testDemonstratePrefetchWithGet.4", testResult: &overallTestResult, retrievedCompany)
        semaphore.signal()
        group.wait()
        // SampleCompany has now been retrieved
        ParallelTest.AssertEqual (label: "testDemonstratePrefetchWithGet.5", testResult: &overallTestResult, retrievedCompany!.id.uuidString, companyId.uuidString)
        logger.sync() { entries in
            ParallelTest.AssertEqual (label: "testDemonstratePrefetchWithGet.6", testResult: &overallTestResult, 0, entries.count)
        }
        return !overallTestResult.isFailed()
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
    static func testDemonstrateUpdateErrors () -> Bool {
        var overallTestResult = TestResult()
        let inMemoryAccessor = SampleInMemoryAccessor()
        let logger = InMemoryLogger(level: .warning)
        let caches = SampleCaches(accessor: inMemoryAccessor, schemaVersion: 1, logger: logger)
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
        let employeeJson = "{\"id\":\"\(employeeId.uuidString)\",\"schemaVersion\":1,\"created\":1525459064.9665,\"saved\":1525459184.5832,\"item\":{\"name\":\"Name Two\",\"sampleCompany\":{\"databaseId\":\"\(inMemoryAccessor.hashValue)\",\"id\":\"\(companyId.uuidString)\",\"isEager\":true,\"cacheName\":\"sampleCompany\",\"version\":1},\"sampleAddress\":{\"isEager\":false,\"isNil\":true}},\"persistenceState\":\"persistent\",\"version\":1}"
        switch inMemoryAccessor.add(name: caches.employees.name, id: employeeId, data: employeeJson.data (using: .utf8)!) {
        case .ok:
            break
        default:
            ParallelTest.Fail (testResult: &overallTestResult, message: "Expected .ok")
        }
        let batch = EventuallyConsistentBatch (retryInterval: .milliseconds(10), timeout: .seconds(60), logger: logger)
        var batchIdString = batch.delegateId().uuidString
        let group = DispatchGroup()
        group.enter()
        firstly {
            caches.employees.get(id: employeeId)
        }.done { employeeEntity in
            employeeEntity.update(batch: batch) { sampleEmployee in
                sampleEmployee.name = "Name Updated1"
                ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.1", testResult: &overallTestResult, "Name Updated1", sampleEmployee.name)
                // Using setThrowError() before committing an update will throw an unrecoverable serialization error
                inMemoryAccessor.setThrowError()
                batch.commitSync()
                logger.sync() { entries in
                    ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.2", testResult: &overallTestResult, 1, entries.count)
                    ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.3", testResult: &overallTestResult, "ERROR|BatchDelegate.commit|Database.unrecoverableError(\"addActionError\")|entityType=Entity<SampleEmployee>;entityId=\(employeeId.uuidString);batchId=\(batchIdString)", entries[0].asTestString())
                }
                
                // Demonstrate that the previous changes were lost due to the reported unrecoverable error
                #if os(Linux)
                    // The order of attributes on serialized JSON is not always consistent on Linux
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.1", testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(employeeId.uuidString)\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.2", testResult: &overallTestResult, employeeJson.contains("\"schemaVersion\":1"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.3", testResult: &overallTestResult, employeeJson.contains("\"created\":1525459064.9665"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.4", testResult: &overallTestResult, employeeJson.contains("\"saved\":1525459184.5832"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.5", testResult: &overallTestResult, employeeJson.contains("\"item\":{"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.6", testResult: &overallTestResult, employeeJson.contains("\"name\":\"Name Two\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.7", testResult: &overallTestResult, employeeJson.contains("\"sampleCompany\":{"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.8", testResult: &overallTestResult, employeeJson.contains("\"databaseId\":\"\(inMemoryAccessor.hashValue)\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.9", testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(companyId.uuidString)\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.10", testResult: &overallTestResult, employeeJson.contains("\"isEager\":true"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.11", testResult: &overallTestResult, employeeJson.contains("\"cacheName\":\"sampleCompany\",\"version\":1}"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.12", testResult: &overallTestResult, employeeJson.contains("\"version\":1}"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.13", testResult: &overallTestResult, employeeJson.contains("\"sampleAddress\":{"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.14", testResult: &overallTestResult, employeeJson.contains("\"isEager\":false"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.15", testResult: &overallTestResult, employeeJson.contains("\"isNil\":true"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.3.Linux.16", testResult: &overallTestResult, employeeJson.contains("\"persistenceState\":\"persistent\""))
                #else
                    ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.4", testResult: &overallTestResult, employeeJson, String (data: inMemoryAccessor.getData(name: caches.employees.name, id: employeeId)!, encoding: .utf8))
                #endif
                ParallelTest.AssertFalse (label: "testDemonstrateUpdateErrors.5", testResult: &overallTestResult, inMemoryAccessor.isThrowError())
                
                // Modify the sampleEmployee again so that it is again added to the batch
                employeeEntity.update(batch: batch) { sampleEmployee in
                    sampleEmployee.name = "Name Updated2"
                    ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.6", testResult: &overallTestResult, "Name Updated2", sampleEmployee.name)
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
                    ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.7", testResult: &overallTestResult, 2, entries.count)
                    ParallelTest.AssertEqual (label: "testDemonstrateUpdateErrors.8", testResult: &overallTestResult, "EMERGENCY|BatchDelegate.commit|Database.error(\"addError\")|entityType=Entity<SampleEmployee>;entityId=\(employeeId.uuidString);batchId=\(batchIdString)", entries[1].asTestString())
                }
                // Demonstrate that the data was updated in persistent media
                #if os(Linux)
                    // The order of attributes on serialized JSON is not always consistent on Linux
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.1", testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(employeeId.uuidString)\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.2", testResult: &overallTestResult, employeeJson.contains("\"schemaVersion\":1"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.3", testResult: &overallTestResult, employeeJson.contains("\"created\":1525459064.9665"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.4", testResult: &overallTestResult, employeeJson.contains("\"saved\":1525459184.5832"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.5", testResult: &overallTestResult, employeeJson.contains("\"item\":{"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.6", testResult: &overallTestResult, employeeJson.contains("\"name\":\"Name Two\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.7", testResult: &overallTestResult, employeeJson.contains("\"sampleCompany\":{"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.8", testResult: &overallTestResult, employeeJson.contains("\"databaseId\":\"\(inMemoryAccessor.hashValue)\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.9", testResult: &overallTestResult, employeeJson.contains("\"id\":\"\(companyId.uuidString)\""))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.10", testResult: &overallTestResult, employeeJson.contains("\"isEager\":true"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.11", testResult: &overallTestResult, employeeJson.contains("\"cacheName\":\"sampleCompany\",\"version\":1}"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.12", testResult: &overallTestResult, employeeJson.contains("\"version\":1}"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.13", testResult: &overallTestResult, employeeJson.contains("\"sampleAddress\":{"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.14", testResult: &overallTestResult, employeeJson.contains("\"isEager\":false"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.15", testResult: &overallTestResult, employeeJson.contains("\"isNil\":true"))
                    ParallelTest.AssertTrue (label: "testDemonstrateUpdateErrors.8.Linux.16", testResult: &overallTestResult, employeeJson.contains("\"persistenceState\":\"persistent\""))
                #else
                    ParallelTest.AssertNotEqual (label: "testDemonstrateUpdateErrors.9", testResult: &overallTestResult, employeeJson, String (data: inMemoryAccessor.getData(name: caches.employees.name, id: employeeId)!, encoding: .utf8))
                #endif
            }
        }.ensure {
            group.leave()
        }.catch { error in
            ParallelTest.Fail(testResult: &overallTestResult, message: "testDemonstrateUpdateErrors.10: Expected success")
        }
        return !overallTestResult.isFailed()
    }

    static func removeAll (caches: SampleCaches) throws {
        let batch = EventuallyConsistentBatch()
        for entity in try caches.companies.scanSync() {
            entity.remove(batch: batch)
        }
        for entity in try caches.employees.scanSync() {
            entity.remove(batch: batch)
        }
        for entity in try caches.addresses.scanSync() {
            entity.remove(batch: batch)
        }
        batch.commitSync()
    }
        
}
