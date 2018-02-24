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
    
    init (accessor: DatabaseAccessor, logger: Logger?) {
        self.accessor = accessor
        self.logger = logger
        self.hashValue = accessor.hashValue()
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
    public let workQueue: DispatchQueue
    private let hashValue: String

    deinit {
        Database.registrar.deRegister(key: hashValue)
    }
    
    static let registrar = Registrar<String, Database>()
    
}

public protocol DatabaseAccessor {
    
    func get (id: UUID) -> DatabaseAccessResult
    
    func add (id: UUID, data: Data)
    
    func update (id: UUID, data: Data)
    
    // A unique identifier for the database being accessed
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
        queue.sync {
            if let _ = items[key]?.item{
                // Do nothing
            } else {
                let _ = items.removeValue(forKey: key)
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


enum RetrievalResult<T> {
    
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
    
    case testError
    
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
    
    public init (database: D, name: CollectionName) {
        self.database = database
        self.name = name
        cache = Dictionary<UUID, WeakItem<T>>()
        pendingRequests = Dictionary<UUID, PendingRequestData<T>>()
        cacheQueue = DispatchQueue(label: "Collection \(name)")
        pendingRequestsQueue = DispatchQueue(label: "CollectionPendingRequests \(name)")
        self.workQueue = database.workQueue
    }
    
    func remove (id: UUID) {
        cacheQueue.async() {
            self.cache.removeValue (forKey: id)
        }
    }
    
    func get (id: UUID) -> RetrievalResult<Entity<T>> {
        var result: Entity<T>? = nil
        var errorResult: RetrievalResult<Entity<T>>? = nil
        cacheQueue.sync {
            result = cache[id]?.item
        }
        if (result == nil) {
            // Serialize all accessor requests for the same id
            var requestQueue: DispatchQueue? = nil
            var closure: (() -> Void)? = nil
            pendingRequestsQueue.sync {
                if let pendingRequest = pendingRequests[id] {
                    requestQueue = pendingRequest.queue
                    closure = {
                        result = pendingRequest.result
                    }
                } else {
                    requestQueue = DispatchQueue (label: "PendingRequest \(id.uuidString)")
                    let pendingRequest: PendingRequestData<T> = PendingRequestData(queue: requestQueue!)
                    pendingRequests[id] = pendingRequest
                    closure = {
                        switch self.database.getAccessor().get(id: id) {
                        case .ok (let data):
                            if let data = data {
                                do {
                                    try result = JSONDecoder().decode(Entity<T>.self, from: data)
                                    result?.setCollection(self as! PersistentCollection<Database, T>)
                                    if let result = result {
                                        self.cacheQueue.async {
                                            self.cache[id] = WeakItem (item: result)
                                        }
                                    }
                                    pendingRequest.result = result
                                } catch {
                                    self.database.logger?.log (level: .error, source: self, featureName: "get",message: "Illegal Data", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString), (name:"data", value: String (data: data, encoding: .utf8)), ("error", "\(error)")])
                                    errorResult = .invalidData
                                }
                            } else {
                                self.database.logger?.log (level: .error, source: self, featureName: "get",message: "Unknown id", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString)])
                            }
                        default:
                            self.database.logger?.log (level: .error, source: self, featureName: "get",message: "Database Error", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString)])
                            errorResult = .databaseError
                        }
                        self.pendingRequestsQueue.async {
                            let _ = self.pendingRequests.removeValue(forKey: id)
                        }
                    }
                }
                requestQueue!.async (execute: closure!)
            }
            requestQueue!.sync () {}
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

    private let name: CollectionName
    private let database: Database
    private var cache: Dictionary<UUID, WeakItem<T>>
    private var pendingRequests: Dictionary<UUID, PendingRequestData<T>>
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    private let pendingRequestsQueue: DispatchQueue
    
}

public class InMemoryAccessor: DatabaseAccessor {
    
    init() {
        id = UUID()
        queue = DispatchQueue (label: "InMemoryAccessor \(id.uuidString)")
    }
    
    public func get(id: UUID) -> DatabaseAccessResult {
        var result: Data? = nil
        var returnError = false
        if let preFetch = preFetch {
            preFetch (id)
        }
        queue.sync() {
            if self.throwError {
                returnError = true
                self.throwError = false
            } else {
                result = storage[id]
            }
        }
        if returnError {
            return .testError
        }
        return .ok (result)
    }
    
    public func add (id: UUID, data: Data) {
        queue.async {
            self.storage[id] = data
        }
    }
        
    public func update (id: UUID, data: Data) {
        add (id: id, data: data)
    }
    
    public func setThrowError() {
        queue.async {
            self.throwError = true
        }
    }
    
    func sync (closure: (Dictionary<UUID, Data>) -> Void) {
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
    
    private var preFetch: ((UUID) -> Void)? = nil
    private var throwError = false
    private var storage = Dictionary<UUID, Data>()
    private var id: UUID
    private let queue: DispatchQueue
    
}
