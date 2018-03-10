//
//  utilities.swift
//  danakeTests
//
//  Created by Neal Lester on 3/10/18.
//

import Foundation
@testable import danake

let standardCollectionName = "myCollection"

class MyClass : Codable {
    
    var myInt = 0
    var myString = ""
    
}

func newTestClassEntity (myInt: Int, myString: String) -> Entity<MyClass> {
    let myClass = MyClass()
    myClass.myInt = myInt
    myClass.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    return Entity (collection: PersistentCollection<Database, MyClass>(database: database, name: "myCollection"), id: id, version: 0, item: myClass)
    
}

struct MyStruct : Codable {
    
    var myInt = 0
    var myString = ""
    
}

func newTestEntity (myInt: Int, myString: String) -> Entity<MyStruct> {
    var myStruct = MyStruct()
    myStruct.myInt = myInt
    myStruct.myString = myString
    let id = UUID()
    let database = Database (accessor: InMemoryAccessor(), schemaVersion: 5, logger: nil)
    let collection = PersistentCollection<Database, MyStruct>(database: database, name: "myCollection")
    return Entity (collection: collection, id: id, version: 0, item: myStruct)
}

// JSONEncoder uses its own inscrutable rounding process for encoding dates, so this is what is necessary to reliably get the expected value of a date in a json encoded object
func jsonEncodedDate (date: Date) throws -> String? {
    let accessor = InMemoryAccessor()
    struct DateContainer : Encodable {
        init (_ d: Date) {
            self.d = d
        }
        let d: Date
    }
    let container = DateContainer.init(date)
    let encoded = try accessor.encoder.encode (container)
    let protoResult = String (data: encoded, encoding: .utf8)
    var result: String? = nil
    if let protoResult = protoResult {
        result = String (protoResult[protoResult.index (protoResult.startIndex, offsetBy: 5)...])
        result = String (result!.prefix(result!.count - 1))
    }
    return result
}

func msRounded (date: Date) -> Double {
    return (date.timeIntervalSince1970 * 1000).rounded()
}

