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

public class ReferenceManager<P: Codable, T: Codable> : ReferenceManagerContainer, Codable {
    
    enum CodingKeys: String, CodingKey {
        case isNil
        case isEager
        case id
        case version
        case qualifiedCollectionName
    }
    
    convenience init (parent: EntityReferenceData<P>, entity: Entity<T>?) {
        self.init (parent: parent, entity: entity, isEager: false)
    }
    
    init (parent: EntityReferenceData<P>, entity: Entity<T>?, isEager: Bool) {
        self.parentData = parent
        self.entity = entity
        self.state = .loaded
        self.isEager = isEager
        queue = DispatchQueue (label: ReferenceManager.queueName(collectionName: parentData.collection.name))
        parent.collection.registerOnEntityCached(id: parent.id, closure: setParent)
        if let entity = entity {
            collection = entity.collection
        }
    }

    convenience init (parent: EntityReferenceData<P>, referenceData: ReferenceManagerData?) {
        self.init (parent: parent, referenceData: referenceData, isEager: false)
    }

    init (parent: EntityReferenceData<P>, referenceData: ReferenceManagerData?, isEager: Bool) {
        self.parentData = parent
        self.referenceData = referenceData
        if let _ = referenceData {
            self.state = .decoded
        } else {
            self.state = .loaded
        }        
        self.isEager = isEager
        queue = DispatchQueue (label: ReferenceManager.queueName(collectionName: parentData.collection.name))
        parent.collection.registerOnEntityCached(id: parent.id, closure: setParent)
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
            queue = DispatchQueue (label: ReferenceManager.queueName(collectionName: parentData.collection.name))
            let isNil = try values.contains (.isNil) && values.decode (Bool.self, forKey: .isNil)
            if isNil {
                state = .loaded
            } else {
                let qualifiedCollectionName = try values.decode (String.self, forKey: .qualifiedCollectionName)
                let version = try values.decode (Int.self, forKey: .version)
                let idString = try values.decode (String.self, forKey: .id)
                let id = UUID (uuidString: idString)
                if let id = id {
                    self.referenceData = ReferenceManagerData (qualifiedCollectionName: qualifiedCollectionName, id: id, version: version)
                } else {
                    throw ReferenceManagerSerializationError.illegalId(idString)
                }
                state = .decoded
            }
            parentData.collection.registerOnEntityCached(id: parentData.id, closure: setParent)
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
                try container.encode(entity.collection.qualifiedName, forKey: .qualifiedCollectionName)
                try container.encode (entity.id, forKey: .id)
                try container.encode (entity.getVersionUnsafe(), forKey: .version)
            } else if let referenceData = referenceData {
                try container.encode(referenceData.qualifiedCollectionName, forKey: .qualifiedCollectionName)
                try container.encode (referenceData.id, forKey: .id)
                try container.encode (referenceData.version, forKey: .version)
            } else {
                try container.encode (true, forKey: .isNil)
            }
        }
    }
    
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
    
    public func async (closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        queue.async {
            switch self.state {
            case .loaded:
                let result = self.entity
                self.parentData.collection.database.workQueue.async {
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
                    self.parentData.collection.database.workQueue.async {
                        closure (.error (errorMessage))
                    }
                }
            case .dereferenced:
                self.parentData.collection.database.workQueue.async {
                    closure (.error ("alreadyDereferenced"))
                }
            }
        }
    }
    
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
    
    // id of the referenced entity, if any
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
            if collection == nil {
                if let candidateCollection = Database.collectionRegistrar.value(key: referenceData.qualifiedCollectionName) {
                    if let collection = candidateCollection as? EntityCache<T> {
                        self.collection = collection
                    } else {
                        closure (.error ("collectionName: \(referenceData.qualifiedCollectionName) returns wrong type: \(type (of: candidateCollection))"))
                    }
                } else {
                    closure (.error ("Unknown collectionName: \(referenceData.qualifiedCollectionName)"))
                }
            }
            if let collection = collection {
                pendingEntityClosures.append(closure)
                self.state = .retrieving (referenceData)
                self.retrievalGetHook()
                collection.get(id: referenceData.id) { retrievalResult in
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
                                        self.state = .retrievalError (Date() + collection.database.referenceRetryInterval, errorMessage)
                                    }
                                    
                                case .error(let errorMessage):
                                    self.state = .retrievalError (Date() + collection.database.referenceRetryInterval, errorMessage)
                                }
                                collection.database.workQueue.async {
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
            self.parentData.collection.database.workQueue.async {
                closure (.ok (nil))
            }
        }
    }
    
    public func set (entity: Entity<T>?, batch: EventuallyConsistentBatch) {
        queue.sync {
            switch self.state {
            case .dereferenced:
                collection?.database.logger?.log(level: .error, source: self, featureName: "set(entity:)", message: "alreadyDereferenced", data: [(name: "parentId", parent?.id.uuidString), (name: "entityCollection", value: entity?.collection.qualifiedName), (name: "entityId", value: entity?.id)])
            default:
                let wasUpdated = self.willUpdate(newId: entity?.id)
                self.entity = entity
                self.referenceData = nil
                self.state = .loaded
                for closure in self.pendingEntityClosures {
                    self.parentData.collection.database.workQueue.async {
                        closure (.ok (entity))
                    }
                }
                if let entity = entity {
                    self.collection = entity.collection
                }
                self.pendingEntityClosures = []
                if wasUpdated {
                    self.addParentTo(batch: batch)
                }
            }
        }
    }

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
                    collection?.database.logger?.log(level: .error, source: self, featureName: "set(referenceData:)", message: "alreadyDereferenced", data: [(name: "parentId", parent?.id.uuidString), (name: "referenceDataCollection", value: referenceData?.qualifiedCollectionName), (name: "referenceDataId", value: referenceData?.id)])
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
            self.parentData.collection.database.logger?.log (level: .error, source: self, featureName: "addParentTo", message: "lostData:noParent", data: [(name:"collectionName", value: self.parentData.collection.qualifiedName), (name:"parentId", value: self.parentData.id.uuidString), (name:"parentVersion", value: self.parentData.version)])
        }
    }
    
    // Will setting an entity or referenceData with ** newId ** change the referenced entity?
    // Not thread safe, must call within closure on queue
    private func willUpdate (newId: UUID?) -> Bool {
        if let newId = newId {
            return
                (self.entity == nil && self.referenceData == nil) ||
                (self.entity != nil && self.entity?.id.uuidString != newId.uuidString) ||
                (self.referenceData != nil && self.referenceData?.id.uuidString != newId.uuidString)
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
    private var collection: EntityCache<T>?
    private var state: ReferenceManagerState
    internal let queue: DispatchQueue
    private var pendingEntityClosures: [(RetrievalResult<Entity<T>>) -> ()] = []
    private var hasRegistered = false
    
    public let isEager: Bool
    
    static func queueName (collectionName: String) -> String {
        return "ReferenceManager.parent: " + collectionName
    }
    
    // Not Thread Safe
    internal func contents() -> ReferenceManagerContents<P, T> {
        return (entity: self.entity, parent: self.parent, parentData: self.parentData, referenceData: self.referenceData, collection: self.collection, state: self.state, isEager: self.isEager, pendingEntityClosureCount: self.pendingEntityClosures.count)
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

internal typealias ReferenceManagerContents<P: Codable, T: Codable> = (entity: Entity<T>?, parent: EntityManagement?, parentData: EntityReferenceData<P>, referenceData: ReferenceManagerData?, collection: EntityCache<T>?, state: ReferenceManagerState, isEager: Bool, pendingEntityClosureCount: Int)
