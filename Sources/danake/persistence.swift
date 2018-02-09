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

public class PersistentCollection<T: Codable> {
    
    public init<I: CollectionAccessor> (accessor: I) {
        self.accessor = accessor
        cache = Dictionary<UUID, WeakItem<T>>()
        cacheQueue = DispatchQueue(label: "Collection \(T.self)")
    }
    
    func remove (id: UUID) {
        cacheQueue.async() {
            self.cache.removeValue (forKey: id)
        }
    }
    
    func get (id: UUID) -> Entity<T>? {
        
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
                    print ("error!")
                }
            }
        }
        return result
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
