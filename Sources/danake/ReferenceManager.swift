//
//  ReferenceManager.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 3/26/18.
//

import Foundation
import PromiseKit

internal enum ReferenceManagerState {
    
    case decoded
    case retrieving (ReferenceManagerData)
    case retrievalError (Date, Error)
    case loaded
    case dereferenced
    
}

public enum ReferenceManagerSerializationError : Error {
    case noParentData
    case illegalId(String)
}

internal protocol ReferenceManagerContainer {
    
    func dereference()
    func dereferenceRecursive()
    
}

/**
    Manages a reference to an Enity which is stored and retrieved to/from persistent media independently. This is the
    Danake way of modeling a reference.

    The generic parameter **P** defines type of the construct containing the ReferenceManager (i.e. the Parent).
 
    The generic parameter **T** defines the type of construct the ReferenceManager points to (the type of Entity's item).
 
 For example:
 ````
 class Parent {
    // Instead of this:
    var child: Child
 
    // We do:
    var child: ReferenceManager<Parent, Child>
 }
 ````
*/
open class ReferenceManager<P: Codable, T: Codable> : ReferenceManagerContainer, Codable {
    
    enum CodingKeys: String, CodingKey {
        case isNil
        case isEager
        case id
        case version
        case qualifiedCacheName
    }
    
    enum Errors: Error {
        case CacheWrongType (name: String, type: String)
        case UnknownCacheName (String)
        case UnknownId (UUID)
        case Dereferenced
    }

/**
     - parameter parent: EntityReferenceData of the construct in which this object is being created
     - parameter entity: The Entity which this manager currently points to (if any)
     - parameter isEager: If true, will asynchronously retrieve the referenced Entity whenever needed
                          to ensure it is available as soon as possible (default = **false**; lazy retrieval).
*/
    init (parent: EntityReferenceData<P>, entity: Entity<T>?, isEager: Bool = false) {
        self.parentData = parent
        self.entity = entity
        self.state = .loaded
        self.isEager = isEager
        queue = DispatchQueue (label: ReferenceManager.queueName(cacheName: parentData.cache.name))
        parent.cache.registerOnEntityCached(id: parent.id, closure: setParent)
        if let entity = entity {
            cache = entity.cache
        }
    }

    /**
     - parameter parent: EntityReferenceData of the construct in which this object is being created
     - parameter entity: ReferenceManagerData defining the Entity which this manager currently points to (if any).
     - parameter isEager: If true, will asynchronously retrieve the referenced Entity whenever needed
                          to ensure it is available as soon as possible (default = **false**; lazy retrieval).
     */
    init (parent: EntityReferenceData<P>, referenceData: ReferenceManagerData?, isEager: Bool = false) {
        self.parentData = parent
        self.referenceData = referenceData
        if let _ = referenceData {
            self.state = .decoded
        } else {
            self.state = .loaded
        }        
        self.isEager = isEager
        queue = DispatchQueue (label: ReferenceManager.queueName(cacheName: parentData.cache.name))
        parent.cache.registerOnEntityCached(id: parent.id, closure: setParent)
        if self.isEager {
            queue.async {
                do {
                    try self.retrieve()
                } catch {}
            }
        }
    }

    public required init (from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        if let parentContainer = decoder.userInfo[Database.parentDataKey] as? DataContainer, let parentData = parentContainer.data as? EntityReferenceData<P> {
            self.parentData = parentData
            self.isEager = try values.decode (Bool.self, forKey: .isEager)
            queue = DispatchQueue (label: ReferenceManager.queueName(cacheName: parentData.cache.name))
            let isNil = try values.contains (.isNil) && values.decode (Bool.self, forKey: .isNil)
            if isNil {
                state = .loaded
            } else {
                let qualifiedCacheName = try values.decode (String.self, forKey: .qualifiedCacheName)
                let version = 0
                let idString = try values.decode (String.self, forKey: .id)
                let id = UUID (uuidString: idString)
                if let id = id {
                    self.referenceData = ReferenceManagerData (qualifiedCacheName: qualifiedCacheName, id: id, version: version)
                } else {
                    throw ReferenceManagerSerializationError.illegalId(idString)
                }
                state = .decoded
            }
            parentData.cache.registerOnEntityCached(id: parentData.id, closure: setParent)
            if self.isEager {
                queue.async {
                    do {
                        try self.retrieve()
                    } catch {}
                }
            }
        } else {
            throw ReferenceManagerSerializationError.noParentData
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try queue.sync {
            try container.encode (isEager, forKey: .isEager)
            if let entity = entity {
                try container.encode(entity.cache.qualifiedName, forKey: .qualifiedCacheName)
                try container.encode (entity.id, forKey: .id)
            } else if let referenceData = referenceData {
                try container.encode(referenceData.qualifiedCacheName, forKey: .qualifiedCacheName)
                try container.encode (referenceData.id, forKey: .id)
            } else {
                try container.encode (true, forKey: .isNil)
            }
        }
    }

/**
     - returns: ReferenceManagerData defining the Entity self points to, if any. May be used to
                copy a reference to this Entity to another ReferenceManager without necessarily
                retrieving the Entity from persistent storage.
*/
    public func getReferenceData() -> ReferenceManagerData? {
        var result: ReferenceManagerData? = nil
        queue.sync {
            if let entity = self.entity {
                result = entity.referenceData()
            } else {
                result = self.referenceData
            }
        }
        return result
    }
    
/**
     Returns the promise of the Entity referenced by this manager.
     
     **Note**: Retrieval result is cached so subsequent accesses will be fast.
     
     - parameter closure: The closure to which the retrieval result will be applied.
*/
    public func get() -> Promise<Entity<T>?> {
        let result: (promise: Promise<Entity<T>?>, resolver: Resolver<Entity<T>?>) = Promise.pending()
        queue.sync {
            switch self.state {
            case .loaded:
                result.resolver.resolve (.fulfilled (self.entity))
            case .decoded:
                do {
                    try self.retrieve ()
                    pendingResolvers.append(result.resolver)
                } catch {
                    result.resolver.reject (error)
                }
            case .retrieving:
                pendingResolvers.append(result.resolver)
            case .retrievalError(let retryTime, let previousError):
                let now = Date()
                if now.timeIntervalSince1970 > retryTime.timeIntervalSince1970 {
                    do {
                        try self.retrieve ()
                        pendingResolvers.append(result.resolver)
                    } catch {
                        result.resolver.reject (error)
                    }
                } else {
                    result.resolver.reject (previousError)
                }
            case .dereferenced:
                result.resolver.reject (Errors.Dereferenced)
            }
        }
        return result.promise
    }
    
    /**
     Retrieves the Entity self points to.
     
     **Note**: Retrieval result is cached so subsequent accesses will be fast.
     */
    public func getSync(timeoutSeconds: Double = 20.0) throws -> Entity<T>? {
        let group = DispatchGroup()
        group.enter()
        var result: Entity<T>? = nil
        firstly {
            get()
        }.done { entity in
            result = entity
        }.catch() { error in
            var entityId: UUID? = self.entity?.id
            if entityId == nil {
                entityId = self.referenceData?.id
            }
            self.cache?.database.logger?.log (level: .error, source: self, featureName: "getSync", message: "error", data: [(name: "parentId", self.parentData.id.uuidString), (name: "entityCollection", value: self.cache?.qualifiedName), (name: "entityId", value: entityId), (name: "message", value: "\(error)")])
        }.finally {
            group.leave()
        }
        group.wait()
        return result;
    }
    
    /// - returns: id of the referenced entity, if any
    public func entityId() -> UUID? {
        var result: UUID? = nil
        queue.sync {
            if let referenceData = self.referenceData {
                result = referenceData.id
            } else if let entity = self.entity {
                result = entity.id
            }
        }
        return result
    }
    
    // Not thread safe, must be called within queue
    internal func retrieve () throws {
        if let referenceData = self.referenceData {
            if cache == nil {
                if let candidateCollection = Database.cacheRegistrar.value(key: referenceData.qualifiedCacheName) {
                    if let cache = candidateCollection as? EntityCache<T> {
                        self.cache = cache
                    } else {
                        throw Errors.CacheWrongType(name: referenceData.qualifiedCacheName, type: "\(type (of: candidateCollection))")
                    }
                } else {
                    throw Errors.UnknownCacheName(referenceData.qualifiedCacheName)
                }
            }
            if let cache = cache {
                self.state = .retrieving (referenceData)
                self.retrievalGetHook()
                firstly {
                    cache.get(id: referenceData.id)
                }.done { retrievedEntity in
                    self.queue.sync {
                        switch self.state {
                        case .retrieving (let pendingReferenceData):
                            if referenceData.id.uuidString == pendingReferenceData.id.uuidString, retrievedEntity.id.uuidString == referenceData.id.uuidString {
                                self.state = .loaded
                                self.entity = retrievedEntity
                                self.referenceData = nil
                                for resolver in self.pendingResolvers {
                                    retrievedEntity.cache.database.workQueue.async {
                                        resolver.fulfill(retrievedEntity)
                                    }
                                }
                                self.pendingResolvers = []
                            }
                        default:
                            break
                        }
                        
                    }
                }.catch { error in
                    self.queue.sync {
                        switch self.state {
                        case .retrieving:
                                self.state = .retrievalError(Date() + cache.database.referenceRetryInterval, error)
                                for resolver in self.pendingResolvers {
                                    resolver.reject(error)
                                }
                                self.pendingResolvers = []
                        default:
                            break
                        }
                    }
                }
            }
        } else {
            state = .loaded
            self.entity = nil
        }
    }
    
/**
     Set self to point to **entity**.
     
     - parameter entity: The entity to which self should point to.
     - parameter batch: The batch to which **parent** will be added if this action
                        changes parent's persistent state.
*/
    public func set (entity: Entity<T>?, batch: EventuallyConsistentBatch) {
        queue.sync {
            switch self.state {
            case .dereferenced:
                cache?.database.logger?.log(level: .error, source: self, featureName: "set(entity:)", message: "alreadyDereferenced", data: [(name: "parentId", parent?.id.uuidString), (name: "entityCollection", value: entity?.cache.qualifiedName), (name: "entityId", value: entity?.id)])
            default:
                let wasUpdated = self.willUpdate(newId: entity?.id)
                self.entity = entity
                self.referenceData = nil
                self.state = .loaded
                if let entity = entity {
                    self.cache = entity.cache
                }
                if wasUpdated {
                    self.addParentTo(batch: batch)
                }
                var workQueue: DispatchQueue? = nil
                if let cache = cache {
                    workQueue = cache.database.workQueue
                } else if let entity = entity {
                    workQueue = entity.cache.database.workQueue
                }
                for resolver in  self.pendingResolvers {
                    if let workQueue = workQueue {
                        workQueue.async() {
                            resolver.resolve(.fulfilled (entity))
                        }
                    } else {
                        resolver.resolve(.fulfilled (entity))
                    }
                }
                self.pendingResolvers = []
            }
        }

    }

/**
     Set self to point to the entity designed by **referenceData**.
     
     - parameter referenceData: ReferenceManagerData designating the entity to which self should point to.
     - parameter batch: The batch to which **parent** will be added if this action
                        changes parent's persistent state.
*/
    public func set (referenceData: ReferenceManagerData?, batch: EventuallyConsistentBatch) {
        queue.sync {
            let wasUpdated = self.willUpdate(newId: referenceData?.id)
            if wasUpdated {
                self.addParentTo(batch: batch)
                self.entity = nil
                self.referenceData = referenceData
                switch self.state {
                case .loaded, .decoded:
                    if let _ = referenceData {
                        self.state = .decoded
                        if self.isEager {
                            do {
                                try self.retrieve()
                            } catch {}
                        }
                    } else {
                        self.state = .loaded
                    }
                case .retrieving, .retrievalError:
                    do {
                        try self.retrieve()
                    } catch {}
                case .dereferenced:
                    cache?.database.logger?.log(level: .error, source: self, featureName: "set(referenceData:)", message: "alreadyDereferenced", data: [(name: "parentId", parent?.id.uuidString), (name: "referenceDataCollection", value: referenceData?.qualifiedCacheName), (name: "referenceDataId", value: referenceData?.id)])
                }
            }
        }
    }
    
    
    internal func willUpdate (newId: UUID?, closure: (Bool) -> ()) {
        queue.sync {
            closure (willUpdate (newId: newId))
        }
    }
    
    // Not Thread safe, always call within queue
    internal func addParentTo (batch: EventuallyConsistentBatch) {
        if let parent = self.parent {
            parent.setDirty(batch: batch)
        } else {
            self.parentData.cache.database.logger?.log (level: .error, source: self, featureName: "addParentTo", message: "lostData:noParent", data: [(name:"cacheName", value: self.parentData.cache.qualifiedName), (name:"parentId", value: self.parentData.id.uuidString), (name:"parentVersion", value: self.parentData.version)])
        }
    }
    
    // Will setting an entity or referenceData with ** newId ** change the referenced entity?
    // Not thread safe, must call within closure on queue
    private func willUpdate (newId: UUID?) -> Bool {
        if let newId = newId {
                let bothExistingNil = (self.entity == nil && self.referenceData == nil)
                let entityIdChanges = (self.entity != nil && self.entity?.id.uuidString != newId.uuidString)
                let referenceIdchanges = (self.referenceData != nil && self.referenceData?.id.uuidString != newId.uuidString)
                return bothExistingNil || entityIdChanges || referenceIdchanges
        } else {
            return entity != nil || self.referenceData != nil
        }
    }
    
    internal func dereference() {
        queue.async {
            self.state = .dereferenced
            if let entity = self.entity {
                self.referenceData = entity.referenceData()
                self.entity = nil
            }
        }
    }

    internal func dereferenceRecursive() {
        queue.async {
            self.state = .dereferenced
            if let entity = self.entity {
                self.referenceData = entity.referenceData()
                self.entity = nil
                entity.breakReferencesRecursive()
            }
        }
    }
    
    private func setParent (parent: Entity<P>) {
        queue.async {
            self.parent = parent
            parent.registerReferenceContainer (self)
        }
    }

    // Used for testing by descendents
    internal func retrievalGetHook() {}

    private var entity: Entity<T>?
    private weak var parent: EntityManagement?
    private var parentData: EntityReferenceData<P>
    private var referenceData: ReferenceManagerData?
    private var cache: EntityCache<T>?
    private var state: ReferenceManagerState
    internal let queue: DispatchQueue
    private var pendingResolvers: [Resolver<Entity<T>?>] = []
    private var hasRegistered = false
    
    /// Should self retrieve referenced Entities as soon as possible (asynchronously) so that they will
    /// be available to clients as soon as possible
    public let isEager: Bool
    
    static func queueName (cacheName: String) -> String {
        return "ReferenceManager.parent: " + cacheName
    }
    
    // Not Thread Safe
    internal func contents() -> ReferenceManagerContents<P, T> {
        return (entity: self.entity, parent: self.parent, parentData: self.parentData, referenceData: self.referenceData, cache: self.cache, state: self.state, isEager: self.isEager, pendingResolverCount: self.pendingResolvers.count)
    }
    
    internal func sync (closure: (ReferenceManagerContents<P, T>) -> ()) {
        queue.sync {
            closure (contents())
        }
    }
    
    internal func appendResolver (_ resolver: Resolver<Entity<T>?>) {
        queue.async {
            self.pendingResolvers.append(resolver)
        }
    }
    
    internal func setState (state: ReferenceManagerState) {
        queue.sync {
            self.state = state
        }
    }
    
}

//internal struct ClosureContainer<T: Codable> {
//    
//    init (_ closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
//        self.closure = closure
//    }
//
//    let closure: (RetrievalResult<Entity<T>>) -> ()
//
//}

internal typealias ReferenceManagerContents<P: Codable, T: Codable> = (entity: Entity<T>?, parent: EntityManagement?, parentData: EntityReferenceData<P>, referenceData: ReferenceManagerData?, cache: EntityCache<T>?, state: ReferenceManagerState, isEager: Bool, pendingResolverCount: Int)
