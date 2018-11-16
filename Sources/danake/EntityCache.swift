//
//  persistence.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 2/2/18.
//

import Foundation
import PromiseKit


//class SomeClass<E: RawRepresentable> where E.RawValue == Int {
//    func doSomething(e: E) {
//        print(e.rawValue)
//    }
//}

public typealias CacheName = String
public typealias QualifiedCacheName = String
open class UntypedEntityCache {}

/**
    Access to the persisted instances of a single type; caching instances created so that each model object is
    represented by one and only one Entity and minimizing Database access. Each EntityCache must be associated with
    exactly one database. Declare EntityCache attributes with `let' within a scope with process lifetime. Re-creating
    a EntityCache object is not currently supported.
*/
open class EntityCache<T: Codable> : UntypedEntityCache {
    
    typealias entityType = T
    
/**
     - parameter database: The Database which houses the persistent objects.
     - parameter name: **Must** be unique within **database** and a valid collection/table identifier in all persistence media to be used
     - parameter userInfoClosure: A closure which adds entries to the Decoder.userInfo before deserialization. The model objects have access
                                  to the userInfo in their init (from: Decoder) feature.
*/
    public init (database: Database, name: CacheName, userInfoClosure: ((inout [CodingUserInfoKey : Any]) -> ())? = nil) {
        self.database = database
        self.name = name
        self.qualifiedName = database.qualifiedCacheName(name)
        self.userInfoClosure = userInfoClosure
        cache = Dictionary<UUID, WeakCodable<T>>()
        cacheQueue = DispatchQueue(label: "Collection \(name)")
        self.workQueue = database.workQueue
        super.init()
        if !database.cacheRegistrar.register(key: name, value: self) {
            database.logger?.log(level: .error, source: self, featureName: "init", message: "cacheAlreadyRegistered", data: [(name: "database", value: "\(type (of: database))"), (name: "databaseHashValue", value: database.accessor.hashValue), (name: "cacheName", value: name)])
        }
        if !Database.cacheRegistrar.register(key: qualifiedName, value: self) {
            database.logger?.log(level: .error, source: self, featureName: "init", message: "qualifiedCollectionAlreadyRegistered", data: [(name: "qualifiedCacheName", value: self.qualifiedName)])
        }
        let nameValidationResult = database.accessor.isValidCacheName(name)
        if !nameValidationResult.isOk() {
            database.logger?.log (level: .error, source: self, featureName: "init", message: nameValidationResult.description(), data: [(name: "database", value: "\(type (of: database))"), (name: "accessor", value: "\(type (of: database.accessor))"), (name: "databaseHashValue", value: database.accessor.hashValue), (name: "cacheName", value: name)])
        }

    }

    deinit {
        Database.cacheRegistrar.deRegister(key: qualifiedName)
        database.cacheRegistrar.deRegister(key: name)
    }
    
    internal func decache (id: UUID) {
        cacheQueue.async() {
            if let cachedEntity = self.cache[id], cachedEntity.codable == nil {
                self.cache.removeValue (forKey: id)
            }
        }
    }

/**
     Retrieve Entity<T> from cache or persistent media
     
     - parameter id: UUID of the Entity to be retrieved.
     
     - throws:  AccessorError.unknownUUID: ID not associated with any entities.
                Also throws all errors originating in the underlying persistent media.
*/
    public func getSync (id: UUID) throws -> Entity<T> {
        var result: Entity<T>? = nil
        cacheQueue.sync {
            result = cache[id]?.codable
        }
        if let result = result {
            return result
        } else {
            do {
                return try self.database.accessor.get(type: Entity<T>.self, cache: self, id: id)
            } catch AccessorError.unknownUUID {
                self.database.logger?.log (level: .warning, source: self, featureName: "getSync",message: "Unknown id", data: [("databaseHashValue", self.database.accessor.hashValue), (name:"cache", value: self.name), (name:"id",value: id.uuidString)])
                throw AccessorError.unknownUUID
            } catch {
                self.database.logger?.log (level: .emergency, source: self, featureName: "getSync",message: "Database Error", data: [("databaseHashValue", self.database.accessor.hashValue), (name:"cache", value: self.name), (name:"id",value: id.uuidString), (name: "errorMessage", "\(error)")])
                throw error
            }
        }
    }

/**
     Asynchronously retrieve Entity<T> (if any) from cache or persistent media and then apply
     **closure** to the retrieval result.
     
     - parameter id: UUID of the Entity to be retrieved.
     - parameter closure: The closure to call when the retrieval has completed.
*/
    public func get (id: UUID) -> Promise<Entity<T>> {
        return Promise { seal in
            workQueue.async {
                do {
                    try seal.fulfill(self.getSync(id: id))
                } catch {
                    seal.reject(error)
                }
            }
        }
    }
    
/**
    Returns all Entities associated with this cache from in the persistent media.
    New objects which have not yet been persisted are not included in the results
     
     - parameter criteria: If provided, only those Entities whose item matches the criteria
                           will be included in the results
     
     - throws:  Any errors originating in underlying persistent media
*/
    public func scanSync (criteria: ((T) -> Bool)? = nil) throws -> [Entity<T>] {
        let result = try database.accessor.scan(type: Entity<T>.self, cache: self)
        if let criteria = criteria  {
            let criteriaWrapper: ((Entity<T>) -> Bool) = { entity in
                var result = true
                entity.sync() { item in
                    result = criteria (item)
                }
                return result
            }
            return result.filter (criteriaWrapper)
        } else {
            return result
        }
    }
    
/**
        Returns the promise (fulfilled asynchronously) of all Entities associated with
        this cache from the persistent media. New objects which have not yet been persisted
        are not included in the results
 
        - parameter criteria: If provided, only those Entities whose item match the criteria
                              will be included in the results
        - parameter closure: The closure to call when the retrieval operation has completed.
 */
    public func scan (criteria: ((T) -> Bool)? = nil) -> Promise<[Entity<T>]> {
        return Promise { seal in
            workQueue.async {
                do {
                    try seal.fulfill(self.scanSync(criteria: criteria))
                } catch {
                    seal.reject(error)
                }
            }
        }
    }
    
/**
     Create a new Entity wrapping **item**.
     
     - parameter batch: The **batch** into which the new Entity will be placed. The new Entity will be written to
                        persistent media when the **batch** is committed.
     - parameter item: The model object to be wrapped
*/
    public func new (batch: EventuallyConsistentBatch, item: T) -> Entity<T> {
        let result = Entity (cache: self, id: UUID(), version: 0, item: item)
        cacheQueue.async() {
            self.cache[result.id] = WeakCodable (result)
        }
        batch.insertAsync(entity: result, closure: nil)
        return result
    }
    
/**
     Create a new Entity wrapping the new item returned by **itemClosure**. Use when creation of an attribute of
     T requires a back reference to T. For example:
     ````
     class Parent {
        // The parent's EntityReferenceData is required to create attribute reference
        let reference: ReferenceManager<Parent, ModelObject>
     }
     ````
     
     - parameter batch: The **batch** into which the new Entity will be placed. The new Entity will be written to
                        persistent media when the **batch** is committed.
     - parameter itemClosure: A function which returns a new model object of type T. The EntityReferenceData parameter
                              provided to **itemClosure** references the Entity under creation.
*/
    public func new (batch: EventuallyConsistentBatch, itemClosure: (EntityReferenceData<T>) -> T) -> Entity<T> {
        let result = Entity (cache: self, id: UUID(), version: 0, itemClosure: itemClosure)
        cacheQueue.async() {
            self.cache[result.id] = WeakCodable (result)
        }
        batch.insertAsync(entity: result, closure: nil)
        return result
    }
    
/**
     - parameter id: Parameter to be queried
     - returns: True if an Entity with **id** is currently present in the cache
*/
    public func hasCached (id: UUID) -> Bool {
        var result = false
        cacheQueue.sync() {
            if let cachedContainer = self.cache[id], let _ = cachedContainer.codable {
                result = true
            }
        }
        return result
    }
/**
     Causes the current thread to wait (loop + sleep) while an Entity with **id** is present in the
     cache or until **timeout** elapses.
     
     - parameter id: Id of the Entity
     - parameter timeout: TimeInterval after which the function gives up and moves on;
                          default = **10.0**
*/
    public func waitWhileCached (id: UUID, timeout: TimeInterval = 10.0) {
        let timeout = Date().timeIntervalSince1970 + timeout
        var wasFound = true
        while (wasFound && timeout > Date().timeIntervalSince1970) {
            usleep(100)
            wasFound = hasCached(id: id)
        }
    }

// Entity Initialization
    
    internal func cachedEntity (id: UUID) -> Entity<T>? {
        var result: Entity<T>? = nil
        cacheQueue.sync() {
            result = self.cache[id]?.codable
        }
        return result
    }
    
    internal func cacheEntity (_ entity: Entity<T>) {
        cacheQueue.sync() {
            precondition ( self.cache[entity.id]?.codable == nil)
            self.cache[entity.id] = WeakCodable (entity)
            if let closureList = self.onEntityCached[entity.id] {
                for closure in closureList {
                    closure(entity)
                }
                self.onEntityCached[entity.id] = nil
            }
        }
    }
    
    internal func registerOnEntityCached (id: UUID, closure: @escaping (Entity<T>) -> ()) {
        cacheQueue.async {
            if var closureList = self.onEntityCached[id] {
                closureList.append(closure)
                self.onEntityCached[id] = closureList
            } else {
                self.onEntityCached[id] = [closure]
            }
        }
    }

    // For testing
    
    internal func sync (closure: (Dictionary<UUID, WeakCodable<T>>) -> Void) {
        cacheQueue.sync () {
            closure (cache)
        }
    }
    
    internal func onCacheCount() -> Int {
        var result = 0
        cacheQueue.sync {
            result = onEntityCached.count
        }
        return result
    }
    
// Attributes
    
    /// Name associated with this cache
    public let name: CacheName
    
    /// The name associated with this cache qualified by the Database.hashValue
    public let qualifiedName: QualifiedCacheName
    
    /// A closure which adds entries to the Decoder.userInfo before deserialization. The model objects have access
    /// to the userInfo in their init (from: Decoder) feature.
    public let userInfoClosure: ((inout [CodingUserInfoKey : Any]) -> ())?
    
    internal let database: Database
    private var cache: Dictionary<UUID, WeakCodable<T>>
    private var onEntityCached: Dictionary<UUID, [(Entity<T>) -> ()]> = [:]
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    
}


