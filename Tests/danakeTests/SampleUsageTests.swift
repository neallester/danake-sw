//
//  SampleUsageTests.swift
//  danakeTests
//
//  Created by Neal Lester on 4/24/18.
//

import XCTest
@testable import danake

/*
        The Model
 */

class Company : Codable {
    
    enum CodingKeys: String, CodingKey {
        case id
    }
    
    init (employeeCollection: EmployeeCollection, id: UUID) {
        self.id = id
        self.employeeCollection = employeeCollection
    }
    
    public required init (from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        id = try values.decode(UUID.self, forKey: .id)
        if let employeeCollection = decoder.userInfo[Company.employeeCollectionKey] as? EmployeeCollection {
            self.employeeCollection = employeeCollection
        } else {
            throw EntityDeserializationError<Company>.missingUserInfoValue(Company.employeeCollectionKey)
        }
    }
    
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode (id, forKey: .id)
    }
    
    public func employees() -> RetrievalResult<[Entity<Employee>]> {
        return employeeCollection.forCompany(self)
    }
    
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
        var json = "{}"
        var jsonData = json.data(using: .utf8)!
        let decoder = JSONDecoder()
        // Missing id
        do {
            let _ = try decoder.decode(Company.self, from: jsonData)
            XCTFail("Expected error")
        } catch {
            XCTAssertEqual ("keyNotFound(CodingKeys(stringValue: \"id\", intValue: nil), Swift.DecodingError.Context(codingPath: [], debugDescription: \"No value associated with key CodingKeys(stringValue: \\\"id\\\", intValue: nil) (\\\"id\\\").\", underlyingError: nil))", "\(error)")
        }
        json = "{\"id\":\"30288A21-4798-4134-9F35-6195BEC7F352\"}"
        jsonData = json.data(using: .utf8)!
        print (UUID().uuidString)
        // Decoding With no collection
        do {
            let _ = try decoder.decode(Company.self, from: jsonData)
            XCTFail("Expected error")
        } catch EntityDeserializationError<Company>.missingUserInfoValue (let error) {
            XCTAssertEqual ("CodingUserInfoKey(rawValue: \"employeeCollectionKey\")", "\(error)")
        } catch {
            XCTFail("Expected missingUserInfoValue")
        }
        let collections = SampleCollections (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        // Decoding with collection
        decoder.userInfo[Company.employeeCollectionKey] = collections.employees
        do {
            let company = try decoder.decode(Company.self, from: jsonData)
            XCTAssertNotNil(company)
            let encoder = JSONEncoder()
            let encodedData = try encoder.encode(company)
            XCTAssertEqual ("{\"id\":\"30288A21-4798-4134-9F35-6195BEC7F352\"}", String (data: encodedData, encoding: .utf8)!)
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
            switch collections.employees.forCompany(company) {
            case .ok (let company2Employees):
                XCTAssertEqual (2, company2Employees!.count)
            default:
                XCTFail ("expected .ok")
            }
        }
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

    
}
