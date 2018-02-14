//
//  persistence.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 2/2/18.
//

import Foundation

public protocol CollectionAccessor {
    
    func get (id: UUID) -> Data?
    
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
    
}

public class PersistentCollection<T: Codable> {
    
    public init<I: CollectionAccessor> (accessor: I, workQueue: DispatchQueue, logger: Logger?) {
        self.accessor = accessor
        cache = Dictionary<UUID, WeakItem<T>>()
        cacheQueue = DispatchQueue(label: "Collection \(T.self)")
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
        cacheQueue.sync {
            result = cache[id]?.item
        }
        if (result == nil) {
            if let data = accessor.get (id: id) {
                do {
                    try result = JSONDecoder().decode(Entity<T>.self, from: data)
                    result?.setCollection(self)
                    if let result = result {
                        cacheQueue.async {
                            self.cache[id] = WeakItem (item: result)
                        }
                    }
                } catch {
                    logger?.log (level: .error, source: self, featureName: "get",message: "Illegal Data", data: [(name:"id",value: id.uuidString), (name:"data", value: String (data: data, encoding: .utf8))])
                    return .invalidData
                }
            } else {
                logger?.log (level: .error, source: self, featureName: "get",message: "Unknown id", data: [(name:"id",value: id.uuidString)])
            }
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
    
    func sync (closure: (Dictionary<UUID, WeakItem<T>>) -> Void) {
        cacheQueue.sync () {
            closure (cache)
        }
    }

    
    private let accessor: CollectionAccessor
    private var cache: Dictionary<UUID, WeakItem<T>>
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    private let logger: Logger?
    
}

public class InMemoryAccessor: CollectionAccessor {
    
    public func get(id: UUID) -> Data? {
        var result: Data? = nil
        queue.sync() {
            result = storage[id]
        }
        return result
    }
    
    public func add (id: UUID, data: Data) {
        queue.async {
            self.storage[id] = data
        }
    }
        
    public func update (id: UUID, data: Data) {
        add (id: id, data: data)
    }
    
    func sync (closure: (Dictionary<UUID, Data>) -> Void) {
        queue.sync () {
            closure (storage)
        }
    }
    
    private var storage = Dictionary<UUID, Data>()
    private let queue = DispatchQueue (label: "InMemoryAccessor \(UUID().uuidString)")
    
}
