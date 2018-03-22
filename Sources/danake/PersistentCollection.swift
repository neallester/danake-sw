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

public class PersistentCollection<D: Database, T: Codable> {
    
    typealias entityType = T
    
    // ** name ** must be unique within ** database ** and a valid collection/table identifier in all persistence media to be used
    public init (database: D, name: CollectionName) {
        self.database = database
        self.name = name
        cache = Dictionary<UUID, WeakItem<T>>()
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
            let retrievalResult = self.database.getAccessor().get(type: Entity<T>.self, name: self.name, id: id)
            switch retrievalResult {
            case .ok (let prospectEntity):
                if let prospectEntity = prospectEntity {
                    result = self.entityForProspect(prospectEntity: prospectEntity)
                } else {
                    self.database.logger?.log (level: .warning, source: self, featureName: "get",message: "Unknown id", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString)])
                }
            case .error (let errorMessage):
                self.database.logger?.log (level: .emergency, source: self, featureName: "get",message: "Database Error", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name:"id",value: id.uuidString), (name: "errorMessage", errorMessage)])
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
        let retrievalResult = database.getAccessor().scan(type: Entity<T>.self, name: name)
        switch retrievalResult {
        case .ok (var resultList):
            if let criteria = criteria  {
                return .ok (initialize(entities: resultList, criteria: criteria))
            } else {
                initialize (entities: &resultList)
                return .ok (resultList)
            }
        case .error (let errorMessage):
            self.database.logger?.log (level: .emergency, source: self, featureName: "scan",message: "Database Error", data: [("databaseHashValue", self.database.getAccessor().hashValue()), (name:"collection", value: self.name), (name: "errorMessage", errorMessage)])
            return .error (errorMessage)
        }
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

// Entity Creation
    
    public func new (batch: EventuallyConsistentBatch, item: T) -> Entity<T> {
        let result = Entity (collection: self as! PersistentCollection<Database, T>, id: UUID(), version: 0, item: item)
        cacheQueue.async() {
            self.cache[result.getId()] = WeakItem (item:result)
        }
        batch.insertAsync(entity: result, closure: nil)
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
    public func new (batch: EventuallyConsistentBatch, itemClosure: (EntityReferenceData<T>) -> T) -> Entity<T> {
        let result = Entity (collection: self as! PersistentCollection<Database, T>, id: UUID(), version: 0, itemClosure: itemClosure)
        cacheQueue.async() {
            self.cache[result.getId()] = WeakItem (item:result)
        }
        batch.insertAsync(entity: result, closure: nil)
        return result
    }

// Entity Initialization
    
    internal func entityForProspect (prospectEntity: Entity<T>) -> Entity<T> {
        var result: Entity<T>? = nil
        self.cacheQueue.sync {
            if let cachedResult = self.cache[prospectEntity.getId()]?.item {
                result = cachedResult
            } else {
                prospectEntity.setCollection (collection: self as! PersistentCollection<Database, T>)
                self.cache[prospectEntity.getId()] = WeakItem (item: prospectEntity)
                result = prospectEntity
            }
        }
        return result!
    }
    
    /*
        Replaces ** entities ** with their cached version or initializes if not in cache
    */
    public func initialize (entities: inout [Entity<T>]) {
        var index = 0
        for entity in entities {
            entities[index] = entityForProspect(prospectEntity: entity)
            index = index + 1
        }
    }
    
    /*
        Returns array containing cached or initilaized ** entities ** which match ** criteria **
    */
    public func initialize (entities: [Entity<T>], criteria: ((T) -> Bool)) -> [Entity<T>] {
        var result: [Entity<T>] = []
        for entity in entities {
            var matchesCriteria = true
            entity.sync() { item in
                matchesCriteria = criteria (item)
            }
            if matchesCriteria {
                result.append (entityForProspect(prospectEntity: entity))
            }
        }
        return result
    }


    // For testing
    
    internal func sync (closure: (Dictionary<UUID, WeakItem<T>>) -> Void) {
        cacheQueue.sync () {
            closure (cache)
        }
    }

// Attributes
    
    internal let database: Database
    public let name: CollectionName
    private var cache: Dictionary<UUID, WeakItem<T>>
    private let cacheQueue: DispatchQueue
    private let workQueue: DispatchQueue
    
}


