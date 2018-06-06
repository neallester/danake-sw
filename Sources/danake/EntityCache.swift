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

public typealias CollectionName = String
public typealias QualifiedCollectionName = String
public class UntypedEntityCache {}

/*
 
    Access to the persisted instances of a single type. Each EntityCache must be associated with exactly one
    database. Declare EntityCache attributes with `let' within a scope with process lifetime. Re-creating a
    EntityCache object is not currently supported.
 
*/

public class EntityCache<T: Codable> : UntypedEntityCache {
    
    typealias entityType = T
    
    // ** name ** must be unique within ** database ** and a valid collection/table identifier in all persistence media to be used
    public convenience init (database: Database, name: CollectionName) {
        self.init (database: database, name: name, deserializationEnvironmentClosure: nil)
    }
    
    // ** name ** must be unique within ** database ** and a valid collection/table identifier in all persistence media to be used
    public init (database: Database, name: CollectionName, deserializationEnvironmentClosure: ((inout [CodingUserInfoKey : Any]) -> ())?) {
        self.database = database
        self.name = name
        self.qualifiedName = database.qualifiedCollectionName(name)
        self.deserializationEnvironmentClosure = deserializationEnvironmentClosure
        cache = Dictionary<UUID, WeakItem<T>>()
        cacheQueue = DispatchQueue(label: "Collection \(name)")
        self.workQueue = database.workQueue
        super.init()
        if !database.collectionRegistrar.register(key: name, value: self) {
            database.logger?.log(level: .error, source: self, featureName: "init", message: "collectionAlreadyRegistered", data: [(name: "database", value: "\(type (of: database))"), (name: "databaseHashValue", value: database.accessor.hashValue()), (name: "collectionName", value: name)])
        }
        if !Database.collectionRegistrar.register(key: qualifiedName, value: self) {
            database.logger?.log(level: .error, source: self, featureName: "init", message: "qualifiedCollectionAlreadyRegistered", data: [(name: "qualifiedCollectionName", value: self.qualifiedName)])
        }
        let nameValidationResult = database.accessor.isValidCollectionName(name)
        if !nameValidationResult.isOk() {
            database.logger?.log (level: .error, source: self, featureName: "init", message: nameValidationResult.description(), data: [(name: "database", value: "\(type (of: database))"), (name: "accessor", value: "\(type (of: database.accessor))"), (name: "databaseHashValue", value: database.accessor.hashValue()), (name: "collectionName", value: name)])
        }

    }

    deinit {
        Database.collectionRegistrar.deRegister(key: qualifiedName)
        database.collectionRegistrar.deRegister(key: name)
    }
    
    internal func decache (id: UUID) {
        cacheQueue.async() {
            if let cachedEntity = self.cache[id], cachedEntity.item == nil {
                self.cache.removeValue (forKey: id)
            }
        }
    }

// Entity Queries
    
    public func get (id: UUID) -> RetrievalResult<Entity<T>> {
        var result: Entity<T>? = nil
        var errorResult: RetrievalResult<Entity<T>>? = nil
        cacheQueue.sync {
            result = cache[id]?.item
        }
        if (result == nil) {
            let retrievalResult = self.database.accessor.get(type: Entity<T>.self, collection: self, id: id)
            switch retrievalResult {
            case .ok (let prospectEntity):
                if let prospectEntity = prospectEntity {
                    result = prospectEntity
                } else {
                    self.database.logger?.log (level: .warning, source: self, featureName: "get",message: "Unknown id", data: [("databaseHashValue", self.database.accessor.hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString)])
                }
            case .error (let errorMessage):
                self.database.logger?.log (level: .emergency, source: self, featureName: "get",message: "Database Error", data: [("databaseHashValue", self.database.accessor.hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString), (name: "errorMessage", errorMessage)])
                errorResult = retrievalResult
            }
        }
        if let errorResult = errorResult {
            return errorResult
        }
        return .ok(result)
    }

    public func get (id: UUID, closure: @escaping (RetrievalResult<Entity<T>>) -> Void) {
        workQueue.async {
            closure (self.get (id: id))
        }
    }
    
    /*
        Returns all Entities from this collection in the persistent media
        New objects which have not yet been persisted are not included in the results
     
        If ** criteria ** is provided, only those Entities whose item matches the criteria
        will be included in the results
    */
    public func scan (criteria: ((T) -> Bool)?) -> RetrievalResult<[Entity<T>]> {
        let retrievalResult = database.accessor.scan(type: Entity<T>.self, collection: self)
        switch retrievalResult {
        case .ok (let resultList):
            if let criteria = criteria  {
                var result: [Entity<T>] = []
                for entity in resultList {
                    var matchesCriteria = true
                    entity.sync() { item in
                        matchesCriteria = criteria (item)
                    }
                    if matchesCriteria {
                        result.append (entity)
                    }
                }
                return .ok (result)
            } else {
                return .ok (resultList)
            }
        case .error (let errorMessage):
            self.database.logger?.log (level: .emergency, source: self, featureName: "scan",message: "Database Error", data: [("databaseHashValue", self.database.accessor.hashValue()), (name:"collection", value: self.name), (name: "errorMessage", errorMessage)])
            return .error (errorMessage)
        }
    }
    
    public func scan () -> RetrievalResult<[Entity<T>]> {
        return scan (criteria: nil)
    }
    
    /*
     Asynchronous access to all Entities from this collection in the persistent media
     New objects which have not yet been persisted are not included in the results
     
     If ** criteria ** is provided, only those Entities whose item matches the criteria
     will be included in the results
     */
    public func scan (criteria: ((T) -> Bool)?, closure: @escaping (RetrievalResult<[Entity<T>]>) -> Void) {
        workQueue.async {
            closure (self.scan (criteria: criteria))
        }
    }
    public func scan (closure: @escaping (RetrievalResult<[Entity<T>]>) -> Void) {
        self.scan (criteria: nil, closure: closure)
    }

    
// Entity Creation
    
    public func new (batch: EventuallyConsistentBatch, item: T) -> Entity<T> {
        let result = Entity (collection: self, id: UUID(), version: 0, item: item)
        cacheQueue.async() {
            self.cache[result.id] = WeakItem (item:result)
        }
        batch.insertAsync(entity: result, closure: nil)
        return result
    }
    
    /*
     Use when creation of some attribute of T requires a back reference to T
     e.g.
     class Parent
     let child: ReferenceManager<Child>
     class Child
     let parent: ReferenceManager<Parent>
     */
    public func new (batch: EventuallyConsistentBatch, itemClosure: (EntityReferenceData<T>) -> T) -> Entity<T> {
        let result = Entity (collection: self, id: UUID(), version: 0, itemClosure: itemClosure)
        cacheQueue.async() {
            self.cache[result.id] = WeakItem (item:result)
        }
        batch.insertAsync(entity: result, closure: nil)
        return result
    }

// Entity Initialization
    
    internal func cachedEntity (id: UUID) -> Entity<T>? {
        var result: Entity<T>? = nil
        cacheQueue.sync() {
            result = self.cache[id]?.item
        }
        return result
    }
    
    internal func cacheEntity (_ entity: Entity<T>) {
        cacheQueue.sync() {
            precondition ( self.cache[entity.id]?.item == nil)
            self.cache[entity.id] = WeakItem (item: entity)
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
    
    internal func sync (closure: (Dictionary<UUID, WeakItem<T>>) -> Void) {
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
    
// Deserialization Environment
    
    internal func getDeserializationEnvironmentClosure() -> ((inout [CodingUserInfoKey : Any]) -> ())? {
        return deserializationEnvironmentClosure
    }
    
    private let deserializationEnvironmentClosure: ((inout [CodingUserInfoKey : Any]) -> ())?
    
// Attributes
    
    internal let database: Database
    public let name: CollectionName
    public let qualifiedName: QualifiedCollectionName
    private var cache: Dictionary<UUID, WeakItem<T>>
    private var onEntityCached: Dictionary<UUID, [(Entity<T>) -> ()]> = [:]
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    
}


