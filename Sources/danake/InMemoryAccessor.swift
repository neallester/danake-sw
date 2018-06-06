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
    
    public func get<T, E: Entity<T>> (type: E.Type, collection: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>> {
        var result: Entity<T>? = nil
        var errorMessage: String? = nil
        if let preFetch = preFetch {
            preFetch (id)
            
        }
        queue.sync() {
            if self.throwError {
                errorMessage = "getError"
                self.throwError = false
            } else if let collectionDictionary = storage[collection.name] {
                let data = collectionDictionary[id]
                if let data = data {
                    switch entityCreator.entity(creator: {try decoder (collection: collection).decode(type, from: data)} ) {
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
    
    public func scan<T, E: Entity<T>> (type: E.Type, collection: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>> {
        var resultList: [Entity<T>] = []
        var result = DatabaseAccessListResult<Entity<T>>.ok (resultList)
        queue.sync {
            if self.throwError {
                result = .error ("scanError")
                self.throwError = false
            } else if let collectionDictionary = storage [collection.name] {
                resultList.reserveCapacity (collectionDictionary.count)
                for item in collectionDictionary.values {
                    switch entityCreator.entity(creator: {try decoder (collection: collection).decode(type, from: item)} ) {
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
                return self.add (name: wrapper.collectionName, id: wrapper.id, data: data)
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
            return self.remove(name: wrapper.collectionName, id: wrapper.id)
        }
        return .ok (result)
    }
    
    public func isValidCollectionName(_ name: CollectionName) -> ValidationResult {
        if name.count > 0 {
            return .ok
        } else {
            return .error ("Empty String is an illegal CollectionName")
        }
    }
    
    public var hashValue: String {
        return id.uuidString
    }
    
    // Access to internals (may be used within tests)
    
    public func add (name: CollectionName, id: UUID, data: Data) -> DatabaseUpdateResult {
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
                    let collectionDictionary = Dictionary<UUID, Data>()
                    self.storage[name] = collectionDictionary
                }
                self.storage[name]![id] = data
            }
        }
        return result
    }
    
    public func remove (name: CollectionName, id: UUID) -> DatabaseUpdateResult {
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
    
    public func has (name: CollectionName, id: UUID) -> Bool {
        var result = false
        queue.sync {
            result = self.storage[name]?[id] != nil
        }
        return result
    }
    
    public func count (name: CollectionName) -> Int {
        var result = 0
        queue.sync {
            if let collectionStorage = self.storage[name] {
                result = collectionStorage.count
            }
        }
        return result
    }

    public func getData (name: CollectionName, id: UUID) -> Data? {
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
    
    func sync (closure: (Dictionary<CollectionName, Dictionary<UUID, Data>>) -> Void) {
        queue.sync () {
            closure (storage)
        }
    }
    
    public let encoder: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .secondsSince1970
        return result
    }()
    
    public func decoder <T> (collection: EntityCache<T>) -> JSONDecoder {
        let result = JSONDecoder()
        result.dateDecodingStrategy = .secondsSince1970
        result.userInfo[Database.collectionKey] = collection
        result.userInfo[Database.parentDataKey] = DataContainer()
        if let closure = collection.getDeserializationEnvironmentClosure() {
            closure (&result.userInfo)
        }
        return result
    }
    
    private var preFetch: ((UUID) -> Void)? = nil
    internal var throwError = false
    internal var throwOnlyRecoverableErrors = false
    private var storage = Dictionary<CollectionName, Dictionary<UUID, Data>>()
    private var id: UUID
    private let queue: DispatchQueue
    private let entityCreator = EntityCreation()
    
}
