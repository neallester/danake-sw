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


public protocol CollectionAccessor {
    
    func get (id: UUID) -> DatabaseAccessResult
    
    func add (id: UUID, data: Data)
    
    func update (id: UUID, data: Data)
    
}

struct WeakItem<T: Codable> {
    
    init (item: Entity<T>) {
        self.item = item
    }
    
    weak var item: Entity<T>?
    
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

public class PersistentCollection<T: Codable> {
    
    public init<I: CollectionAccessor> (accessor: I, workQueue: DispatchQueue, logger: Logger?) {
        self.accessor = accessor
        cache = Dictionary<UUID, WeakItem<T>>()
        pendingRequests = Dictionary<UUID, (queue: DispatchQueue, result: Entity<T>?)>()
        cacheQueue = DispatchQueue(label: "Collection \(T.self)")
        pendingRequestsQueue = DispatchQueue(label: "CollectionPendingRequests \(T.self)")
        self.workQueue = workQueue
        self.logger = logger
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
            // We must serialize all accessor requests for the same id
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
                    var pendingRequest: (queue: DispatchQueue, result: Entity<T>?) = (queue: requestQueue!, result: nil)
                    pendingRequests[id] = pendingRequest
                    closure = {
                        switch self.accessor.get (id: id) {
                        case .ok (let data):
                            if let data = data {
                                do {
                                    try result = JSONDecoder().decode(Entity<T>.self, from: data)
                                    result?.setCollection(self)
                                    if let result = result {
                                        self.cacheQueue.async {
                                            self.cache[id] = WeakItem (item: result)
                                        }
                                    }
                                    pendingRequest.result = result
                                    self.pendingRequestsQueue.async {
                                        let _ = self.pendingRequests.removeValue(forKey: id)
                                    }
                                } catch {
                                    self.logger?.log (level: .error, source: self, featureName: "get",message: "Illegal Data", data: [(name:"id",value: id.uuidString), (name:"data", value: String (data: data, encoding: .utf8)), ("error", "\(error)")])
                                    errorResult = .invalidData
                                }
                            } else {
                                self.logger?.log (level: .error, source: self, featureName: "get",message: "Unknown id", data: [(name:"id",value: id.uuidString)])
                            }
                        default:
                            self.logger?.log (level: .error, source: self, featureName: "get",message: "Database Error", data: [(name:"id",value: id.uuidString)])
                            errorResult = .databaseError
                        }
                    }
                }
            }
            requestQueue!.sync (execute: closure!)
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
    
    public func new (item: T) -> Entity<T> {
        let result = Entity (collection: self, id: UUID(), version: 0, item: item)
        cacheQueue.async() {
            self.cache[result.getId()] = WeakItem (item:result)
        }
        return result
    }
    
    // Use when creation of some attribute of T requires a back reference to T
    // e.g.
    // class Parent
    //    let child: EntityReference<Child>
    // class Child
    //    let parent: EntityReference<Parent>
    public func new (itemClosure: (EntityReferenceData<T>) -> T) -> Entity<T> {
        let result = Entity (collection: self, id: UUID(), version: 0, itemClosure: itemClosure)
        cacheQueue.async() {
            self.cache[result.getId()] = WeakItem (item:result)
        }
        return result
    }

    func sync (closure: (Dictionary<UUID, WeakItem<T>>) -> Void) {
        cacheQueue.sync () {
            closure (cache)
        }
    }

    
    private let accessor: CollectionAccessor
    private var cache: Dictionary<UUID, WeakItem<T>>
    private var pendingRequests: Dictionary<UUID, (queue: DispatchQueue, result: Entity<T>?)>
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    private let pendingRequestsQueue: DispatchQueue
    private let logger: Logger?
    
}

public class InMemoryAccessor: CollectionAccessor {
    
    public func get(id: UUID) -> DatabaseAccessResult {
        var result: Data? = nil
        var returnError = false
        queue.sync() {
            if let preFetch = preFetch {
                preFetch (id)
            }
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
    
    private var preFetch: ((UUID) -> Void)? = nil
    private var throwError = false
    private var storage = Dictionary<UUID, Data>()
    private let queue = DispatchQueue (label: "InMemoryAccessor \(UUID().uuidString)")
    
}
