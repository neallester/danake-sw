//
//  ReferenceManager.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 3/26/18.
//

import Foundation

internal enum ReferenceManagerState {
    
    case decoded
    case retrieving (ReferenceManagerData)
    case retrievalError (Date, String)
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
                self.retrieve() { result in}
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
                let version = try values.decode (Int.self, forKey: .version)
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
                    self.retrieve() { result in}
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
                try container.encode (entity.getVersionUnsafe(), forKey: .version)
            } else if let referenceData = referenceData {
                try container.encode(referenceData.qualifiedCacheName, forKey: .qualifiedCacheName)
                try container.encode (referenceData.id, forKey: .id)
                try container.encode (referenceData.version, forKey: .version)
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
     Asynchronously retrieves the Entity self points to and applies it to **closure**.
     
     **Note**: Retrieval result is cached so subsequent accesses will be fast.
     
     - parameter closure: The closure to which the retrieval result will be applied.
*/
    public func async (closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        queue.async {
            switch self.state {
            case .loaded:
                let result = self.entity
                self.parentData.cache.database.workQueue.async {
                    closure (.ok (result))
                }
            case .decoded:
                self.retrieve (closure: closure)
            case .retrieving:
                self.pendingEntityClosures.append(closure)
            case .retrievalError(let retryTime, let errorMessage):
                let now = Date()
                if now.timeIntervalSince1970 > retryTime.timeIntervalSince1970 {
                    self.retrieve (closure: closure)
                } else {
                    self.parentData.cache.database.workQueue.async {
                        closure (.error (errorMessage))
                    }
                }
            case .dereferenced:
                self.parentData.cache.database.workQueue.async {
                    closure (.error ("alreadyDereferenced"))
                }
            }
        }
    }
    
/**
     Retrieves the Entity self points to.
     
     **Note**: Retrieval result is cached so subsequent accesses will be fast.
*/
    public func get() -> RetrievalResult<Entity<T>> {
        let group = DispatchGroup()
        group .enter()
        var result = RetrievalResult<Entity<T>>.error("timedOut")
        async() { retrievalResult in
            result = retrievalResult
            group.leave()
        }
        group.wait()
        return result
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
    internal func retrieve (closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        if let referenceData = self.referenceData {
            if cache == nil {
                if let candidateCollection = Database.cacheRegistrar.value(key: referenceData.qualifiedCacheName) {
                    if let cache = candidateCollection as? EntityCache<T> {
                        self.cache = cache
                    } else {
                        closure (.error ("cacheName: \(referenceData.qualifiedCacheName) returns wrong type: \(type (of: candidateCollection))"))
                    }
                } else {
                    closure (.error ("Unknown cacheName: \(referenceData.qualifiedCacheName)"))
                }
            }
            if let cache = cache {
                pendingEntityClosures.append(closure)
                self.state = .retrieving (referenceData)
                self.retrievalGetHook()
                cache.get(id: referenceData.id) { retrievalResult in
                    self.queue.async {
                        switch self.state {
                        case .retrieving (let pendingReferenceData):
                            if referenceData.id.uuidString == pendingReferenceData.id.uuidString {
                                var finalResult = retrievalResult
                                let pendingClosures = self.pendingEntityClosures
                                self.pendingEntityClosures = []
                                switch retrievalResult {
                                case .ok (let retrievedEntity):
                                    if let retrievedEntity = retrievedEntity {
                                        self.entity = retrievedEntity
                                        self.state = .loaded
                                        self.referenceData = nil
                                    } else {
                                        let errorMessage = "\(type (of: self)): Unknown id \(referenceData.id.uuidString)"
                                        finalResult = .error (errorMessage)
                                        self.state = .retrievalError (Date() + cache.database.referenceRetryInterval, errorMessage)
                                    }
                                    
                                case .error(let errorMessage):
                                    self.state = .retrievalError (Date() + cache.database.referenceRetryInterval, errorMessage)
                                }
                                cache.database.workQueue.async {
                                    for closure in pendingClosures {
                                        closure (finalResult)
                                    }
                                }
                            }
                        default:
                            break
                        }
                        
                    }
                }
            }
        } else {
            state = .loaded
            self.parentData.cache.database.workQueue.async {
                closure (.ok (nil))
            }
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
                for closure in self.pendingEntityClosures {
                    self.parentData.cache.database.workQueue.async {
                        closure (.ok (entity))
                    }
                }
                if let entity = entity {
                    self.cache = entity.cache
                }
                self.pendingEntityClosures = []
                if wasUpdated {
                    self.addParentTo(batch: batch)
                }
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
                            self.retrieve() { result in }
                        }
                    } else {
                        self.state = .loaded
                    }
                case .retrieving, .retrievalError:
                    self.retrieve() { result in }
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
    private var pendingEntityClosures: [(RetrievalResult<Entity<T>>) -> ()] = []
    private var hasRegistered = false
    
    /// Should self retrieve referenced Entities as soon as possible (asynchronously) so that they will
    /// be available to clients as soon as possible
    public let isEager: Bool
    
    static func queueName (cacheName: String) -> String {
        return "ReferenceManager.parent: " + cacheName
    }
    
    // Not Thread Safe
    internal func contents() -> ReferenceManagerContents<P, T> {
        return (entity: self.entity, parent: self.parent, parentData: self.parentData, referenceData: self.referenceData, cache: self.cache, state: self.state, isEager: self.isEager, pendingEntityClosureCount: self.pendingEntityClosures.count)
    }
    
    internal func sync (closure: (ReferenceManagerContents<P, T>) -> ()) {
        queue.sync {
            closure (contents())
        }
    }
    
    internal func appendClosure (_ closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        queue.async {
            self.pendingEntityClosures.append(closure)
        }
    }
    
    internal func setState (state: ReferenceManagerState) {
        queue.sync {
            self.state = state
        }
    }
    
}

internal struct ClosureContainer<T: Codable> {
    
    init (_ closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        self.closure = closure
    }
    
    let closure: (RetrievalResult<Entity<T>>) -> ()
    
}

internal typealias ReferenceManagerContents<P: Codable, T: Codable> = (entity: Entity<T>?, parent: EntityManagement?, parentData: EntityReferenceData<P>, referenceData: ReferenceManagerData?, cache: EntityCache<T>?, state: ReferenceManagerState, isEager: Bool, pendingEntityClosureCount: Int)
