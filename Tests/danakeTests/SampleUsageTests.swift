//
//  SampleUsageTests.swift
//  danakeTests
//
//  Created by Neal Lester on 4/24/18.
//

import XCTest
@testable import danake

/*
        This sample assumes you have read the framework introduction in README.md
        https://github.com/neallester/danake-sw#introduction
 
        The Application Model
 
        class Company:  A Company is associated with zero or more Employees.
 
                        Property `employeeCollection' demonstrates how to deserialize a property whose value
                        comes from the environment rather than the the serialized representation of the instance.
 
                        property `id' demonstrates how to create a model object with an attribute of type UUID
                        value which is taken from the id of the enclosing Entity at creation and deserialization
                        time.
 
                        The functions `employees()' demonstrate functions whose results are obtained via queries to
                        the persistent media.
 
        class Employee: An employee is associated with zero or one companies, and has zero or one addresses.
 
                        Properties `company' and `address' demonstrate the usage of EntityReference<PARENT, TYPE>

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
        if let parentReferenceData = decoder.userInfo[Database.parentDataKey] as? EntityReferenceData<Company> {
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
        self.company = EntityReference<Employee, Company> (parent: selfReference, entity: company)
        self.address = EntityReference<Employee, Address> (parent: selfReference, entity: address)
    }

    var name: String
    
    // Always declare attributes of type 'EntityReference' using 'let'
    // The entities which are referenced may change, but the EntityReference itself does not
    let company: EntityReference<Employee, Company>
    let address: EntityReference<Employee, Address>
    
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
    The Persistence System
 
    protocol DatabaseAccessor:      Interface for the adapter used to access persistent media (an implementation is provided for each supported media).
                                    Lookup by Entity.id and collection scan (with optional selection criteria) are included.
 
    protocol SampleAccessor:        Application specific extension to DatabaseAccessor which adds specialized queries (e.g. lookups based on indexed criteria).
                                    In this case it includes selecting employees by the id of their associated company. A media specific implementation
                                    (e.g. SampleInMemoryAccessor) must be provided for each supported persistent media.

    class Database:                 Representation of a specific persistent media in the application. Only one instance of the Database object associated with any
                                    particular persistent storage media (database) may be present in system. Declare Database objects as let constants within a scope
                                    with process lifetime. Re-creating a Database object is not currently supported.
 
    class SampleDatabase:           Provides PersistentCollections with access to the applicatio specific SampleAccessor. Only required for applications
                                    which include an application specific DatabaseAccessor.
 
    class PersistentCollection<T>:  Access to the persisted instances of a single type or a polymorphically related set of types (use polymorism if indexed queries
                                    based on attributes shared by all of the types are required). Each PersistentCollection must be associated with exactly one
                                    database. Declare PersistentCollection attributes with `let' within a scope with process lifetime. Re-creating a PersistentCollection
                                    object is not currently supported.
 
    class CompanyCollection:        Access to persistent Company objects. Demonstrates:
                                    - creation of model objects which include an id whose value matches the id of their enclosing Entity
                                    - setup of a PersistentCollection to support deserialization of objects which include properties which are populated from
                                      the environment rather than the serialized data.
                                    - custom `new' function for creating new Entity<Company> objects
 
    class EmployeeCollection:       Access to persistent Employee objects. Demonstrates:
                                    - deserialization of objects which include EntityReference properties
                                    - custom synchronous and asynchronous retrieval functions
 
    class SampleCollections:        An application specific convenience class for organizing all of the PersistentCollection objects associated with the
                                    SampleDatabase. Declare objects of this type using `let' within a scope with process lifetime
*/

class SampleDatabase : Database {
    
    init(accessor: SampleAccessor, schemaVersion: Int, logger: Logger?, referenceRetryInterval: TimeInterval) {
        self.sampleAccessor = accessor
        super.init (accessor: accessor, schemaVersion: schemaVersion, logger: logger, referenceRetryInterval: referenceRetryInterval)
    }
    
    func getAccessor() -> SampleAccessor {
        return sampleAccessor
    }

    private let sampleAccessor: SampleAccessor
    
}

protocol SampleAccessor : DatabaseAccessor {
    func employeesForCompany (collection: PersistentCollection<Employee>, company: Company) -> DatabaseAccessListResult<Entity<Employee>>
}

class SampleInMemoryAccessor : InMemoryAccessor, SampleAccessor {
    func employeesForCompany (collection: PersistentCollection<Employee>, company: Company) -> DatabaseAccessListResult<Entity<Employee>> {
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

class CompanyCollection : PersistentCollection<Company> {
    
    init (database: SampleDatabase, employeeCollection: EmployeeCollection) {
        self.employeeCollection = employeeCollection
        // The following closure is fired on the decoders userInfo property before deserialization
        // setting the employeeCollection object which will be assigned to the apprpriate property
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

class EmployeeCollection : PersistentCollection<Employee> {

    init(database: SampleDatabase) {
        forCompanyClosure = { collection, company in
            return database.getAccessor().employeesForCompany (collection: collection, company: company)
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
        addresses = PersistentCollection<Address> (database: database, name: "address")
    }
    
    public let logger: Logger?
    
    public let companies: CompanyCollection
    
    public let employees: EmployeeCollection
    
    public let addresses: PersistentCollection<Address>
    
}

class SampleTests: XCTestCase {
    
    // Execute the `runSample' test using the SampleInMemoryAccessor
    public func testInMemorySample() {
        let accessor = SampleInMemoryAccessor()
        let logger: Logger? = nil
//        let logger = InMemoryLogger()
//        let logger = ConsoleLogger()
        SampleTests.runSample (accessor: accessor, logger: logger)
    }
    
/*
     Test intended to demonstrate usage within application code
*/

    static func runSample (accessor: SampleAccessor, logger: Logger?) {
        // Declare SampleCollections with `let'
        // See class Database header comment for explanation of `schemaVersion'
        let collections = SampleCollections (accessor: accessor, schemaVersion: 1, logger: logger)
        let _ = collections.employees // TODO Remove
    }
    
/*
     Test Basic Functionality of Model and Persistence System
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
        decoder.userInfo[Database.parentDataKey] = parentData
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

    
}
