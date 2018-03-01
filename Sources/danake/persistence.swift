//
//  persistence.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 2/2/18.
//

import Foundation

//class SomeClass<E: RawRepresentable> where E.RawValue == Int {
//    func doSomething(e: E) {
//        print(e.rawValue)
//    }
//}

public class Database {
    
    /*
        The current ** schemaVersion ** is stored as metadata with every Entity when it is stored in the database. The value of ** schemaVersion **
        should be incremented whenever an Decodable incompatible change is made to any Entity.item stored in the database. That is, increment
        ** schemaVersion ** if an Entity.item (anywhere in the database) is no longer capable of decoding data stored under a previous
        ** schemaVersion **. The intention is that the ** schemaVersion ** at the time an Entity.item was stored will be made available to the
        code performing an Entity.item decode operation where it would facilitate decoding legacy JSON to the existing object structure. However,
        a mechanism for actually doing this is currently not present in the framework.
    */
    init (accessor: DatabaseAccessor, schemaVersion: Int, logger: Logger?) {
        self.accessor = accessor
        self.logger = logger
        self.hashValue = accessor.hashValue()
        self.schemaVersion = schemaVersion
        workQueue = DispatchQueue (label: "workQueue Database \(hashValue)", attributes: .concurrent)
        if Database.registrar.register(key: hashValue, value: self) {
            logger?.log(level: .info, source: self, featureName: "init", message: "created", data: [(name:"hashValue", hashValue)])
        } else {
            logger?.log(level: .emergency, source: self, featureName: "init", message: "registrationFailed", data: [(name:"hashValue", hashValue)])
        }
    }

    func getAccessor() -> DatabaseAccessor {
        return accessor
    }

    public let accessor: DatabaseAccessor
    public let logger: Logger?
    public let schemaVersion: Int
    public let workQueue: DispatchQueue
    private let hashValue: String
    let collectionRegistrar = Registrar<CollectionName, AnyObject>()

    deinit {
        Database.registrar.deRegister(key: hashValue)
    }
    
    static let registrar = Registrar<String, Database>()
    
}

public enum ValidationResult {
    
    case ok
    case error (String)
    
    func isOk() -> Bool {
        switch self {
        case .ok:
            return true
        default:
            return false
        }
    }
    
    func description() -> String {
        switch self {
        case .ok:
            return "ok"
        case .error (let description):
            return description
        }
    }
}

public protocol DatabaseAccessor {
    
    func get (name: CollectionName, id: UUID) -> DatabaseAccessResult
    
    func add (name: CollectionName, id: UUID, data: Data)
    
    func update (name: CollectionName, id: UUID, data: Data)
    
    func scan (name: CollectionName) -> DatabaseAccessListResult
    
    func encoder() -> JSONEncoder
    
    func decoder() -> JSONDecoder
    
    /*
        Is the format of ** name ** a valid CollectionName in this storage medium and,
        is ** name ** NOT a reserved word in this storage medium?
    */
    func isValidCollectionName (name: CollectionName) -> ValidationResult
    
    /*
        A unique identifier for the specific instance of the database being accessed
        Ideally this should be stored in and retrieved from the storage medium
    */
    func hashValue() -> String
    
}

struct WeakItem<T: Codable> {
    
    init (item: Entity<T>) {
        self.item = item
    }
    
    weak var item: Entity<T>?
}

struct WeakObject<T: AnyObject> {
    
    init (item: T) {
        self.item = item
    }
    
    weak var item: T?
}

/*
    Registers objects against a key until the object is deallocated
*/
class Registrar<K: Hashable, V: AnyObject> {
    
    init() {
        queue = DispatchQueue (label: "\(type (of: self)): \(UUID().uuidString)")
    }
    
    // Returns true if registration succeeded
    // Fails if there is already an object registered under this key
    func register (key: K, value: V) -> Bool {
        var result = false
        queue.sync {
            if let storedValue = items[key]?.item, storedValue !== value {
                // Do nothing
            } else {
                items[key] = WeakObject (item: value)
                result = true
            }

        }
        return result
    }
    
    // Only permitted if item associated with key is nil
    func deRegister (key: K) {
        queue.async {
            if let _ = self.items[key]?.item{
                // Do nothing
            } else {
                let _ = self.items.removeValue(forKey: key)
            }
        }
    }
    
    func isRegistered (key: K) -> Bool {
        var result = false
        queue.sync {
            if let _ = items[key]?.item {
                result = true
            }
            
        }
        return result
    }
    
    func count() -> Int {
        var result = 0
        queue.sync {
            result = items.count
        }
        return result
    }
    
    let queue: DispatchQueue
    var items = Dictionary<K, WeakObject<V>>()
    
}


public enum RetrievalResult<T> {
    
    func item() -> T? {
        switch self {
        case .ok(let item):
            return item
        default:
            return nil
        }
    }
    
    func isOk() -> Bool {
        switch self {
        case .ok:
            return true
        default:
            return false
        }
    }

    case ok (T?)
    
    case invalidData
    
    case databaseError
    
}

public enum DatabaseAccessResult {
    
    case ok (Data?)
    
    case error (String)
    
}

public enum DatabaseAccessListResult {
    
    case ok ([Data])
    
    case error (String)
    
}

public typealias CollectionName = String

public class PendingRequestData<T: Codable> {
    
    init (queue: DispatchQueue) {
        self.queue = queue
    }
    
    let queue: DispatchQueue
    var result: Entity<T>? = nil
}

public class PersistentCollection<D: Database, T: Codable> {
    
    // ** name ** must be unique within ** database ** and a valid collection/table identifier in all persistence media to be used
    public init (database: D, name: CollectionName) {
        self.database = database
        self.name = name
        cache = Dictionary<UUID, WeakItem<T>>()
        pendingRequests = Dictionary<UUID, PendingRequestData<T>>()
        cacheQueue = DispatchQueue(label: "Collection \(name)")
        self.workQueue = database.workQueue
        if !database.collectionRegistrar.register(key: name, value: self) {
            database.logger?.log(level: .error, source: self, featureName: "init", message: "collectionAlreadyRegistered", data: [(name: "database", value: "\(type (of: database))"), (name: "databaseHashValue", value: database.getAccessor().hashValue()), (name: "collectionName", value: name)])
        }
        let nameValidationResult = database.getAccessor().isValidCollectionName(name: name)
        if !nameValidationResult.isOk() {
            database.logger?.log (level: .error, source: self, featureName: "init", message: nameValidationResult.description(), data: [(name: "database", value: "\(type (of: database))"), (name: "accessor", value: "\(type (of: database.getAccessor()))"), (name: "databaseHashValue", value: database.getAccessor().hashValue()), (name: "collectionName", value: name)])
        }
    }

    deinit {
        database.collectionRegistrar.deRegister(key: name)
    }
    
    func decache (id: UUID) {
        cacheQueue.async() {
            if let cachedEntity = self.cache[id], cachedEntity.item == nil {
                self.cache.removeValue (forKey: id)
            }
        }
    }
    
    public func get (id: UUID) -> RetrievalResult<Entity<T>> {
        var result: Entity<T>? = nil
        var errorResult: RetrievalResult<Entity<T>>? = nil
        cacheQueue.sync {
            result = cache[id]?.item
        }
        if (result == nil) {
            switch self.database.getAccessor().get(name: self.name, id: id) {
            case .ok (let data):
                if let data = data {
                    do {
                        result = try self.entityForData (data: data)
                    } catch {
                        errorResult = .invalidData
                        self.database.logger?.log (level: .error, source: self, featureName: "get",message: "Illegal Data", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString), (name:"data", value: String (data: data, encoding: .utf8)), ("error", "\(error)")])
                    }
                } else {
                    self.database.logger?.log (level: .error, source: self, featureName: "get",message: "Unknown id", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString)])
                }
            case .error (let errorMessage):
                self.database.logger?.log (level: .error, source: self, featureName: "get",message: "Database Error", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString), (name: "errorMessage", errorMessage)])
                errorResult = .databaseError
            }
        }
        if let errorResult = errorResult {
            return errorResult
        }
        return .ok(result)
    }

    func get (id: UUID, closure: @escaping (RetrievalResult<Entity<T>>) -> Void) {
        workQueue.async {
            closure (self.get (id: id))
        }
    }
    
    internal func entityForData (data: Data) throws -> Entity<T>? {
        var result: Entity<T>? = nil
        try result = database.getAccessor().decoder().decode(Entity<T>.self, from: data)
        if let unwrappedResult = result {
            unwrappedResult.initialize(collection: self as! PersistentCollection<Database, T>, schemaVersion: self.database.schemaVersion)
            self.cacheQueue.sync {
                if let cachedResult = self.cache[unwrappedResult.getId()]?.item {
                    result = cachedResult
                } else {
                    self.cache[unwrappedResult.getId()] = WeakItem (item: unwrappedResult)
                    result = unwrappedResult
                }
            }
        }
        return result
    }
    
    /*
        Returns all entities in collection
        If ** criteria ** is provided, only those entities where criteria returns true are included
    */
    
    public func scan (criteria: ((T) -> Bool)?) -> RetrievalResult<[Entity<T>]> {
        let retrievalResult = database.getAccessor().scan(name: name)
        switch retrievalResult {
        case .ok (let data):
            return .ok (convert (data: data, criteria: criteria))
        case .error (let errorMessage):
            self.database.logger?.log (level: .error, source: self, featureName: "scan",message: "Database Error", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name: "errorMessage", errorMessage)])
            return .databaseError
        }
    }
    
    func scan (criteria: ((T) -> Bool)?, closure: @escaping (RetrievalResult<[Entity<T>]>) -> Void) {
        workQueue.async {
            closure (self.scan (criteria: criteria))
        }
    }

    /*
        converts all ** data ** to Entity (returns cached Entity if present)
        If ** criteria ** is provided, only those entities where criteria returns true are included
     
     
    */
    func convert (data: [Data], criteria: ((T) -> Bool)?) -> [Entity<T>] {
        var result: [Entity<T>] = []
        if criteria == nil {
            result.reserveCapacity(data.count)
        }
        for datum in data {
            do {
                let entity = try entityForData (data: datum)
                if let entity = entity {
                    if let criteria = criteria {
                        var isIncluded = false
                        entity.sync() { item in
                            isIncluded = criteria (item)
                        }
                        if isIncluded {
                            result.append (entity)
                        }
                    } else {
                        result.append (entity)
                    }
                }
            } catch {
                self.database.logger?.log (level: .error, source: self, featureName: "convert",message: "Illegal Data", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"data", value: String (data: datum, encoding: .utf8)), ("error", "\(error)")])
            }
        }
        return result
    }
    
    public func new (batch: Batch, item: T) -> Entity<T> {
        let result = Entity (collection: self as! PersistentCollection<Database, T>, id: UUID(), version: 0, item: item)
        cacheQueue.async() {
            self.cache[result.getId()] = WeakItem (item:result)
        }
        batch.insertAsync(item: result, closure: nil)
        return result
    }
    
/*
     Use when creation of some attribute of T requires a back reference to T
     e.g.
        class Parent
            let child: EntityReference<Child>
        class Child
            let parent: EntityReference<Parent>
 */
    public func new (batch: Batch, itemClosure: (EntityReferenceData<T>) -> T) -> Entity<T> {
        let result = Entity (collection: self as! PersistentCollection<Database, T>, id: UUID(), version: 0, itemClosure: itemClosure)
        cacheQueue.async() {
            self.cache[result.getId()] = WeakItem (item:result)
        }
        batch.insertAsync(item: result, closure: nil)
        return result
    }

    func sync (closure: (Dictionary<UUID, WeakItem<T>>) -> Void) {
        cacheQueue.sync () {
            closure (cache)
        }
    }

    internal let database: Database
    public let name: CollectionName
    private var cache: Dictionary<UUID, WeakItem<T>>
    private var pendingRequests: Dictionary<UUID, PendingRequestData<T>>
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    
}

public class InMemoryAccessor: DatabaseAccessor {
    
    init() {
        id = UUID()
        queue = DispatchQueue (label: "InMemoryAccessor \(id.uuidString)")
    }
    
    public func get(name: CollectionName, id: UUID) -> DatabaseAccessResult {
        var result: Data? = nil
        var returnError = false
        if let preFetch = preFetch {
            preFetch (id)
        }
        queue.sync() {
            if self.throwError {
                returnError = true
                self.throwError = false
            } else if let collectionDictionary = storage[name] {
                result = collectionDictionary[id]
            }
        }
        if returnError {
            return .error ("Test Error")
        }
        return .ok (result)
    }
    
    public func add (name: CollectionName, id: UUID, data: Data) {
        queue.async {
            if self.storage[name] == nil {
                let collectionDictionary = Dictionary<UUID, Data>()
                self.storage[name] = collectionDictionary
            }
            self.storage[name]![id] = data
        }
    }
        
    public func update (name: CollectionName, id: UUID, data: Data) {
        add (name: name, id: id, data: data)
    }
    
    public func scan(name: CollectionName) -> DatabaseAccessListResult {
        var resultData: [Data] = []
        var result = DatabaseAccessListResult.ok (resultData)
        queue.sync {
            if self.throwError {
                result = .error ("Test Error")
                self.throwError = false
            } else if let collectionDictionary = storage [name] {
                resultData.reserveCapacity (collectionDictionary.count)
                for item in collectionDictionary.values {
                    resultData.append (item)
                }
                result = .ok (resultData)
            }
        }
        return result
    }

    
    public func isValidCollectionName(name: CollectionName) -> ValidationResult {
        if name.count > 0 {
            return .ok
        } else {
            return .error ("Empty String is an illegal CollectionName")
        }
    }
    
    public func encoder() -> JSONEncoder {
        return encoderCache
    }
    
    public func decoder() -> JSONDecoder {
        return decoderCache
    }
    
    public func setThrowError() {
        queue.async {
            self.throwError = true
        }
    }
    
    func sync (closure: (Dictionary<CollectionName, Dictionary<UUID, Data>>) -> Void) {
        queue.sync () {
            closure (storage)
        }
    }
    
    func setPreFetch (preFetch: ((UUID) -> Void)?) {
        self.preFetch = preFetch
    }
    
    public func hashValue() -> String {
        return id.uuidString
    }
    
    private let encoderCache: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .secondsSince1970
        return result
    }()
    
    private let decoderCache: JSONDecoder = {
        let result = JSONDecoder()
        result.dateDecodingStrategy = .secondsSince1970
        return result
    }()
    
    private var preFetch: ((UUID) -> Void)? = nil
    private var throwError = false
    private var storage = Dictionary<CollectionName, Dictionary<UUID, Data>>()
    private var id: UUID
    private let queue: DispatchQueue
    
}
