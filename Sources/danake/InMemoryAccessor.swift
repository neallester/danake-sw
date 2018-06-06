//
//  InMemoryAccessor.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 3/10/18.
//

import Foundation

// Use for testing
public class InMemoryAccessor: DatabaseAccessor {
    
    
    init() {
        id = UUID()
        queue = DispatchQueue (label: "InMemoryAccessor \(id.uuidString)")
    }
    
    // Implement protocol DatabaseAccessor
    
    public func get<T, E: Entity<T>> (type: E.Type, cache: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>> {
        var result: Entity<T>? = nil
        var errorMessage: String? = nil
        if let preFetch = preFetch {
            preFetch (id)
            
        }
        queue.sync() {
            if self.throwError {
                errorMessage = "getError"
                self.throwError = false
            } else if let cacheDictionary = storage[cache.name] {
                let data = cacheDictionary[id]
                if let data = data {
                    switch entityCreator.entity(creator: {try decoder (cache: cache).decode(type, from: data)} ) {
                    case .ok (let entity):
                        result = entity
                    case .error (let creationError):
                        errorMessage = creationError
                    }
                }
            }
        }
        if let errorMessage = errorMessage {
            return .error (errorMessage)
        }
        return .ok (result)
    }
    
    public func scan<T, E: Entity<T>> (type: E.Type, cache: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>> {
        var resultList: [Entity<T>] = []
        var result = DatabaseAccessListResult<Entity<T>>.ok (resultList)
        queue.sync {
            if self.throwError {
                result = .error ("scanError")
                self.throwError = false
            } else if let cacheDictionary = storage [cache.name] {
                resultList.reserveCapacity (cacheDictionary.count)
                for item in cacheDictionary.values {
                    switch entityCreator.entity(creator: {try decoder (cache: cache).decode(type, from: item)} ) {
                    case .ok (let entity):
                        resultList.append (entity)
                    case .error:
                        break
                    }
                }
                result = .ok (resultList)
            }
        }
        return result
    }
    
    public func addAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        var errorResult: DatabaseActionResult? = nil
        queue.sync {
            if let preFetch = preFetch {
                preFetch (wrapper.id)
            }
            if throwError && !throwOnlyRecoverableErrors {
                throwError = false
                errorResult = .error ("addActionError")
            }
        }
        if let errorResult = errorResult {
            return errorResult
        }
        do {
            let data = try self.encoder.encode (wrapper)
            let result = { () -> DatabaseUpdateResult in
                return self.add (name: wrapper.cacheName, id: wrapper.id, data: data)
            }
            return .ok (result)
        } catch {
            return DatabaseActionResult.error("\(error)")
        }
    }
    
    public func updateAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        return addAction (wrapper: wrapper)
    }
    
    public func removeAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        var errorResult: DatabaseActionResult? = nil
        queue.sync {
            if let preFetch = preFetch {
                preFetch (wrapper.id)
            }
            if throwError && !throwOnlyRecoverableErrors {
                throwError = false
                errorResult = .error ("removeActionError")
            }
        }
        if let errorResult = errorResult {
            return errorResult
        }
        let result = { () -> DatabaseUpdateResult in
            return self.remove(name: wrapper.cacheName, id: wrapper.id)
        }
        return .ok (result)
    }
    
    public func isValidCacheName(_ name: CacheName) -> ValidationResult {
        if name.count > 0 {
            return .ok
        } else {
            return .error ("Empty String is an illegal CacheName")
        }
    }
    
    public var hashValue: String {
        return id.uuidString
    }
    
    // Access to internals (may be used within tests)
    
    public func add (name: CacheName, id: UUID, data: Data) -> DatabaseUpdateResult {
        var result = DatabaseUpdateResult.ok
        queue.sync {
            if let preFetch = preFetch {
                preFetch (id)
                
            }
            if throwError {
                throwError = false
                result = .error ("addError")
            } else {
                if self.storage[name] == nil {
                    let cacheDictionary = Dictionary<UUID, Data>()
                    self.storage[name] = cacheDictionary
                }
                self.storage[name]![id] = data
            }
        }
        return result
    }
    
    public func remove (name: CacheName, id: UUID) -> DatabaseUpdateResult {
        var result = DatabaseUpdateResult.ok
        queue.sync {
            if let preFetch = preFetch {
                preFetch (id)
            }
            if throwError {
                throwError = false
                result = .error ("removeError")
            } else {
                self.storage[name]?[id] = nil
            }
        }
        return result
        
    }
    
    public func has (name: CacheName, id: UUID) -> Bool {
        var result = false
        queue.sync {
            result = self.storage[name]?[id] != nil
        }
        return result
    }
    
    public func count (name: CacheName) -> Int {
        var result = 0
        queue.sync {
            if let cacheStorage = self.storage[name] {
                result = cacheStorage.count
            }
        }
        return result
    }

    public func getData (name: CacheName, id: UUID) -> Data? {
        var result: Data? = nil
        queue.sync() {
            result = storage[name]?[id]
        }
        return result
    }
    
    public func setThrowError() {
        queue.sync {
            self.throwError = true
        }
    }
    
    public func setThrowError(_ throwError: Bool) {
        queue.sync {
            self.throwError = throwError
        }
    }
    
    public func setThrowOnlyRecoverableErrors (_ throwRecoverableErrors: Bool) {
        queue.sync {
            self.throwOnlyRecoverableErrors = throwRecoverableErrors
        }
        
    }
    
    public func isThrowError() -> Bool {
        var result = false
        queue.sync {
            result = throwError
        }
        return result
    }
    
    
    func setPreFetch (_ preFetch: ((UUID) -> Void)?) {
        queue.sync {
            self.preFetch = preFetch
        }        
    }
    
    func sync (closure: (Dictionary<CacheName, Dictionary<UUID, Data>>) -> Void) {
        queue.sync () {
            closure (storage)
        }
    }
    
    public let encoder: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .secondsSince1970
        return result
    }()
    
    public func decoder <T> (cache: EntityCache<T>) -> JSONDecoder {
        let result = JSONDecoder()
        result.dateDecodingStrategy = .secondsSince1970
        result.userInfo[Database.cacheKey] = cache
        result.userInfo[Database.parentDataKey] = DataContainer()
        if let closure = cache.getDeserializationEnvironmentClosure() {
            closure (&result.userInfo)
        }
        return result
    }
    
    private var preFetch: ((UUID) -> Void)? = nil
    internal var throwError = false
    internal var throwOnlyRecoverableErrors = false
    private var storage = Dictionary<CacheName, Dictionary<UUID, Data>>()
    private var id: UUID
    private let queue: DispatchQueue
    private let entityCreator = EntityCreation()
    
}
