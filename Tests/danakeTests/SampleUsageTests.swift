//
//  SampleUsageTests.swift
//  danakeTests
//
//  Created by Neal Lester on 6/8/18.
//

import XCTest
@testable import danake

class SampleUsageTests: XCTestCase {
    
    public func testSamples() {
        let accessor = SampleInMemoryAccessor()
        XCTAssertTrue (SampleUsage.runSample (accessor: accessor))
        XCTAssertTrue (SampleUsage.demonstrateThrowError())
        XCTAssertTrue (SampleUsage.testDemonstratePrefetchWithGet())
        XCTAssertTrue (SampleUsage.testDemonstrateUpdateErrors())
        
    }
    
    func testCompanyCreation() {
        let caches = SampleCaches (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let uuid = UUID()
        let company = SampleCompany (employeeCache: caches.employees, id: uuid)
        XCTAssertEqual (uuid.uuidString, company.id.uuidString)
    }
    
    func testCompanyEncodeDecode() {
        let json = "{}"
        var jsonData = json.data(using: .utf8)!
        let decoder = JSONDecoder()
        jsonData = json.data(using: .utf8)!
        // Decoding With no cache or parentData
        do {
            let _ = try decoder.decode(SampleCompany.self, from: jsonData)
            XCTFail("Expected error")
        } catch EntityDeserializationError<SampleCompany>.missingUserInfoValue (let error) {
            XCTAssertEqual ("CodingUserInfoKey(rawValue: \"employeeCacheKey\")", "\(error)")
        } catch {
            XCTFail("Expected missingUserInfoValue")
        }
        let caches = SampleCaches (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        // Decoding with cache and no parent data
        decoder.userInfo[SampleCompany.employeeCacheKey] = caches.employees
        do {
            let _ = try decoder.decode(SampleCompany.self, from: jsonData)
            XCTFail("Expected error")
        } catch EntityDeserializationError<SampleCompany>.missingUserInfoValue (let error) {
            XCTAssertEqual ("CodingUserInfoKey(rawValue: \"parentData\")", "\(error)")
        } catch {
            XCTFail("Expected missingUserInfoValue")
        }
        // Decoding with cache and parentdata
        let uuid = UUID (uuidString: "30288A21-4798-4134-9F35-6195BEC7F352")!
        let parentData = EntityReferenceData<SampleCompany>(cache: caches.companies, id: uuid, version: 1)
        let container = DataContainer ()
        container.data = parentData
        decoder.userInfo[Database.parentDataKey] = container
        do {
            let company = try decoder.decode(SampleCompany.self, from: jsonData)
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
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = caches.companies.new(batch: batch)
        let companyEntity2 = caches.companies.new(batch: batch)
        let companyEntity3 = caches.companies.new(batch: batch)
        let employeeEntity1 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let _ = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let _ = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        batch.commitSync()
        companyEntity1.sync() { company in
            switch company.employees() {
            case .ok (let employees):
                XCTAssertEqual (2, employees.count)
                XCTAssertTrue (employees[0] === employeeEntity1 || employees[0] === employeeEntity2)
                XCTAssertTrue (employees[1] === employeeEntity1 || employees[1] === employeeEntity2)
            default:
                XCTFail ("Expected .ok")
            }
        }
        companyEntity2.sync() { company in
            switch company.employees() {
            case .ok (let employees):
                XCTAssertEqual (2, employees.count)
            default:
                XCTFail ("Expected .ok")
            }
            
        }
        companyEntity3.sync() { company in
            switch company.employees() {
            case .ok (let employees):
                XCTAssertEqual (0, employees.count)
            default:
                XCTFail ("Expected .ok")
            }
            
        }
    }
    
    func testCompanyEmployeesAsync() {
        let accessor = SampleInMemoryAccessor()
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = caches.companies.new(batch: batch)
        let companyEntity2 = caches.companies.new(batch: batch)
        let companyEntity3 = caches.companies.new(batch: batch)
        let employeeEntity1 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let _ = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let _ = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        batch.commitSync()
        let group = DispatchGroup()
        group.enter()
        group.enter()
        group.enter()
        companyEntity1.async() { company in
            switch company.employees() {
            case .ok (let employees):
                XCTAssertEqual (2, employees.count)
                XCTAssertTrue (employees[0] === employeeEntity1 || employees[0] === employeeEntity2)
                XCTAssertTrue (employees[1] === employeeEntity1 || employees[1] === employeeEntity2)
                group.leave()
            default:
                XCTFail("Expected .ok")
            }
        }
        companyEntity2.async() { company in
            switch company.employees() {
            case .ok (let employees):
                XCTAssertEqual (2, employees.count)
                group.leave()
            default:
                XCTFail("Expected .ok")
            }
        }
        companyEntity3.async() { company in
            switch company.employees() {
            case .ok (let employees):
                XCTAssertEqual (0, employees.count)
                group.leave()
            default:
                XCTFail ("Expected .ok")
            }
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail("Expected success")
        }
    }
    
    func testCompanyCacheNew() {
        let caches = SampleCaches (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        XCTAssertNotNil (caches.companies.new(batch: batch))
    }
    
    func testAddressCreation() {
        let address = SampleAddress (street: "Street", city: "City", state: "CA", zipCode: "95010")
        XCTAssertEqual ("Street", address.street)
        XCTAssertEqual ("City", address.city)
        XCTAssertEqual ("CA", address.state)
        XCTAssertEqual ("95010", address.zipCode)
    }
    
    func testAddressFromCache() {
        let caches = SampleCaches (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let address = SampleAddress (street: "Street", city: "City", state: "CA", zipCode: "95010")
        let addressEntity = caches.addresses.new(batch: batch, item: address)
        addressEntity.sync() { address in
            XCTAssertEqual ("Street", address.street)
            XCTAssertEqual ("City", address.city)
            XCTAssertEqual ("CA", address.state)
            XCTAssertEqual ("95010", address.zipCode)
        }
    }
    
    func testEmployeeCreation() {
        let caches = SampleCaches (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let company = SampleCompany(employeeCache: caches.employees, id: UUID())
        let batch = EventuallyConsistentBatch()
        let companyEntity = caches.companies.new(batch: batch, item: company)
        let selfReference = EntityReferenceData<SampleEmployee> (cache: caches.employees, id: UUID(), version: 0)
        // Without address
        var employee = SampleEmployee(selfReference: selfReference, company: companyEntity, name: "Bob Carol", address: nil)
        var referencedCompanyEntity = employee.company.get().item()!
        referencedCompanyEntity.sync() { referencedCompany in
            XCTAssertTrue (referencedCompany === company)
        }
        XCTAssertEqual ("Bob Carol", employee.name)
        XCTAssertNil (employee.address.get().item())
        // With address
        let address = SampleAddress (street: "Street", city: "City", state: "CA", zipCode: "95010")
        let addressEntity = caches.addresses.new(batch: batch, item: address)
        employee = SampleEmployee(selfReference: selfReference, company: companyEntity, name: "Bob Carol", address: addressEntity)
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
    
    func testEmployeeFromCache() {
        let caches = SampleCaches (accessor: SampleInMemoryAccessor(), schemaVersion: 1, logger: nil)
        let company = SampleCompany(employeeCache: caches.employees, id: UUID())
        let batch = EventuallyConsistentBatch()
        let companyEntity = caches.companies.new(batch: batch, item: company)
        // Without address
        var employeeEntity = caches.employees.new(batch: batch, company: companyEntity, name: "Bob Carol", address: nil)
        employeeEntity.sync() { employee in
            XCTAssertTrue (employee.company.get().item()! === companyEntity)
            XCTAssertEqual ("Bob Carol", employee.name)
            XCTAssertNil (employee.address.get().item())
        }
        let address = SampleAddress (street: "Street", city: "City", state: "CA", zipCode: "95010")
        let addressEntity = caches.addresses.new(batch: batch, item: address)
        employeeEntity = caches.employees.new(batch: batch, company: companyEntity, name: "Bob Carol", address: addressEntity)
        employeeEntity.sync() { employee in
            XCTAssertTrue (employee.company.get().item()! === companyEntity)
            XCTAssertEqual ("Bob Carol", employee.name)
            XCTAssertTrue (employee.address.get().item()! === addressEntity)
        }
    }
    
    func testInMemoryAccessorEmployeesForCompany() {
        let accessor = SampleInMemoryAccessor()
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = caches.companies.new(batch: batch)
        let companyEntity2 = caches.companies.new(batch: batch)
        let companyEntity3 = caches.companies.new(batch: batch)
        let employeeEntity1 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        batch.commitSync()
        companyEntity1.sync() { company in
            switch accessor.employeesForCompany(cache: caches.employees, company: company) {
            case .ok (let company1Employees):
                XCTAssertEqual (2, company1Employees.count)
                XCTAssertTrue (company1Employees[0] === employeeEntity1 || company1Employees[0] === employeeEntity2)
                XCTAssertTrue (company1Employees[1] === employeeEntity1 || company1Employees[1] === employeeEntity2)
            default:
                XCTFail ("expected .ok")
            }
        }
        companyEntity2.sync() { company in
            switch accessor.employeesForCompany(cache: caches.employees, company: company) {
            case .ok (let company2Employees):
                XCTAssertEqual (2, company2Employees.count)
                XCTAssertTrue (company2Employees[0] === employeeEntity3 || company2Employees[0] === employeeEntity4)
                XCTAssertTrue (company2Employees[1] === employeeEntity3 || company2Employees[1] === employeeEntity4)
            default:
                XCTFail ("expected .ok")
            }
        }
        companyEntity3.sync() { company in
            switch accessor.employeesForCompany(cache: caches.employees, company: company) {
            case .ok (let company3Employees):
                XCTAssertEqual (0, company3Employees.count)
            default:
                XCTFail ("expected .ok")
            }
        }
    }
    
    func testEmployeeCacheForCompany() {
        let accessor = SampleInMemoryAccessor()
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = caches.companies.new(batch: batch)
        let companyEntity2 = caches.companies.new(batch: batch)
        let employeeEntity1 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        batch.commitSync()
        companyEntity1.sync() { company in
            switch caches.employees.forCompany(company) {
            case .ok (let company1Employees):
                XCTAssertEqual (2, company1Employees.count)
                XCTAssertTrue (company1Employees[0] === employeeEntity1 || company1Employees[0] === employeeEntity2)
                XCTAssertTrue (company1Employees[1] === employeeEntity1 || company1Employees[1] === employeeEntity2)
            default:
                XCTFail ("expected .ok")
            }
        }
        companyEntity2.sync() { company in
            switch caches.employees.forCompany(company) {
            case .ok (let company2Employees):
                XCTAssertEqual (2, company2Employees.count)
                XCTAssertTrue (company2Employees[0] === employeeEntity3 || company2Employees[0] === employeeEntity4)
                XCTAssertTrue (company2Employees[1] === employeeEntity3 || company2Employees[1] === employeeEntity4)
            default:
                XCTFail ("expected .ok")
            }
        }
    }
    
    func testEmployeeCacheForCompanyAsync() {
        let accessor = SampleInMemoryAccessor()
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = caches.companies.new(batch: batch)
        let companyEntity2 = caches.companies.new(batch: batch)
        let employeeEntity1 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        batch.commitSync()
        let group = DispatchGroup()
        group.enter()
        group.enter()
        companyEntity1.sync() { company in
            caches.employees.forCompany(company) { result in
                switch result {
                case .ok (let company1Employees):
                    XCTAssertEqual (2, company1Employees.count)
                    XCTAssertTrue (company1Employees[0] === employeeEntity1 || company1Employees[0] === employeeEntity2)
                    XCTAssertTrue (company1Employees[1] === employeeEntity1 || company1Employees[1] === employeeEntity2)
                default:
                    XCTFail ("expected .ok")
                }
                group.leave()
            }
        }
        companyEntity2.sync() { company in
            caches.employees.forCompany(company) { result in
                switch result {
                case .ok (let company2Employees):
                    XCTAssertEqual (2, company2Employees.count)
                    XCTAssertTrue (company2Employees[0] === employeeEntity3 || company2Employees[0] === employeeEntity4)
                    XCTAssertTrue (company2Employees[1] === employeeEntity3 || company2Employees[1] === employeeEntity4)
                default:
                    XCTFail ("expected .ok")
                }
                group.leave()
            }
        }
        switch group.wait(timeout: DispatchTime.now() + 10.0) {
        case .success:
            break
        default:
            XCTFail("Expected success")
        }
    }
    
    func testRemoveAll() {
        let accessor = SampleInMemoryAccessor()
        let caches = SampleCaches (accessor: accessor, schemaVersion: 1, logger: nil)
        let batch = EventuallyConsistentBatch()
        let companyEntity1 = caches.companies.new(batch: batch)
        let companyEntity2 = caches.companies.new(batch: batch)
        let employeeEntity1 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp A", address: nil)
        let employeeEntity2 = caches.employees.new(batch: batch, company: companyEntity1, name: "Emp B", address: nil)
        let employeeEntity3 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp C", address: nil)
        let employeeEntity4 = caches.employees.new(batch: batch, company: companyEntity2, name: "Emp D", address: nil)
        let address1 = caches.addresses.new(batch: batch, item: SampleAddress(street: "S1", city: "C1", state: "St1", zipCode: "Z1"))
        let address2 = caches.addresses.new(batch: batch, item: SampleAddress(street: "S2", city: "C2", state: "St2", zipCode: "Z2"))
        let address3 = caches.addresses.new(batch: batch, item: SampleAddress(street: "S3", city: "C3", state: "St3", zipCode: "Z3"))
        let address4 = caches.addresses.new(batch: batch, item: SampleAddress(street: "S4", city: "C4", state: "St4", zipCode: "Z4"))
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
        XCTAssertEqual (2, caches.companies.scan(criteria: nil).item()!.count)
        XCTAssertEqual (4, caches.employees.scan(criteria: nil).item()!.count)
        XCTAssertEqual (4, caches.addresses.scan(criteria: nil).item()!.count)
        SampleUsage .removeAll(caches: caches)
        XCTAssertEqual (0, caches.companies.scan(criteria: nil).item()!.count)
        XCTAssertEqual (0, caches.employees.scan(criteria: nil).item()!.count)
        XCTAssertEqual (0, caches.addresses.scan(criteria: nil).item()!.count)
    }

}
