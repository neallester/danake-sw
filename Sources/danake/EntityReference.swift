//
//  EntityReference.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 3/26/18.
//

import Foundation

internal enum EntityReferenceState {
    
    case decoded
    case retrieving (EntityReferenceSerializationData)
    case retrievalError (Date, String)
    case loaded
    
}

public enum EntityReferenceSerializationError : Error {
    case noParentData
    case illegalId(String)
}

public class EntityReference<P: Codable, T: Codable> : Codable {
    
    enum CodingKeys: String, CodingKey {
        case isNil
        case isEager
        case id
        case version
        case databaseId
        case collectionName
    }
    
    convenience init (parent: EntityReferenceData<P>, entity: Entity<T>?) {
        self.init (parent: parent, entity: entity, isEager: false)
    }
    
    init (parent: EntityReferenceData<P>, entity: Entity<T>?, isEager: Bool) {
        self.parentData = parent
        self.entity = entity
        self.state = .loaded
        self.isEager = isEager
        if let entity = entity {
            collection = entity.collection
        }
        queue = DispatchQueue (label: EntityReference.queueName(collectionName: parentData.collection.name))
    }

    convenience init (parent: EntityReferenceData<P>, referenceData: EntityReferenceSerializationData?) {
        self.init (parent: parent, referenceData: referenceData, isEager: false)
    }

    init (parent: EntityReferenceData<P>, referenceData: EntityReferenceSerializationData?, isEager: Bool) {
        self.parentData = parent
        self.referenceData = referenceData
        if let _ = referenceData {
            self.state = .decoded
        } else {
            self.state = .loaded
        }        
        self.isEager = isEager
        queue = DispatchQueue (label: EntityReference.queueName(collectionName: parentData.collection.name))
    }

    public required init (from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        if let parentContainer = decoder.userInfo[Database.parentDataKey] as? DataContainer, let parentData = parentContainer.data as? EntityReferenceData<P> {
            self.parentData = parentData
            self.isEager = try values.decode (Bool.self, forKey: .isEager)
            queue = DispatchQueue (label: EntityReference.queueName(collectionName: parentData.collection.name))
            let isNil = try values.contains (.isNil) && values.decode (Bool.self, forKey: .isNil)
            if isNil {
                state = .loaded
            } else {
                let databaseId = try values.decode (String.self, forKey: .databaseId)
                let collectionName = try values.decode (String.self, forKey: .collectionName)
                let version = try values.decode (Int.self, forKey: .version)
                let idString = try values.decode (String.self, forKey: .id)
                let id = UUID (uuidString: idString)
                if let id = id {
                    self.referenceData = EntityReferenceSerializationData (databaseId: databaseId, collectionName: collectionName, id: id, version: version)
                } else {
                    throw EntityReferenceSerializationError.illegalId(idString)
                }
                state = .decoded
            }
        } else {
            throw EntityReferenceSerializationError.noParentData
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try queue.sync {
            try container.encode (isEager, forKey: .isEager)
            if let entity = entity {
                try container.encode(entity.collection.database.accessor.hashValue(), forKey: .databaseId)
                try container.encode (entity.collection.name, forKey: .collectionName)
                try container.encode (entity.id, forKey: .id)
                try container.encode (entity.getVersion(), forKey: .version)
            } else if let referenceData = referenceData {
                try container.encode(referenceData.databaseId, forKey: .databaseId)
                try container.encode (referenceData.collectionName, forKey: .collectionName)
                try container.encode (referenceData.id, forKey: .id)
                try container.encode (referenceData.version, forKey: .version)
            } else {
                try container.encode (true, forKey: .isNil)
            }
        }
    }
    
    public func getReference() -> EntityReferenceSerializationData? {
        var result: EntityReferenceSerializationData? = nil
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
    
    // Not thread safe, must be called within queue
    private func retrieve (closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        if let referenceData = self.referenceData {
            if collection == nil {
                if let database = Database.registrar.value(key: referenceData.databaseId) {
                    if let candidateCollection = database.collectionRegistrar.value(key: referenceData.collectionName) {
                        if let collection = candidateCollection as? PersistentCollection<Database, T> {
                            self.collection = collection
                        } else {
                            closure (.error ("collectionName: \(referenceData.collectionName) returns wrong type: \(type (of: candidateCollection))"))
                        }
                    } else {
                        closure (.error ("Unknown collectionName: \(referenceData.collectionName)"))
                    }
                } else {
                    closure (.error ("Unknown databaseId: \(referenceData.databaseId)"))
                }
            }
            if let collection = collection {
                pendingEntityClosures.append(closure)
                self.state = .retrieving (referenceData)
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
                                    } else {
                                        let errorMessage = "\(type (of: self)): Unknown id \(referenceData.id.uuidString)"
                                        finalResult = .error (errorMessage)
                                        self.state = .retrievalError (Date() + self.retryInterval, errorMessage)
                                    }
                                    
                                case .error(let errorMessage):
                                    self.state = .retrievalError (Date() + self.retryInterval, errorMessage)
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
        queue.async {
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

    public func set (referenceData: EntityReferenceSerializationData?, batch: EventuallyConsistentBatch) {
        queue.async {
            let wasUpdated = self.willUpdate(newId: referenceData?.id)
            if wasUpdated {
                self.addParentTo(batch: batch)
                self.entity = nil
                self.referenceData = referenceData
                switch self.state {
                case .loaded, .decoded:
                    if let _ = referenceData {
                        self.state = .decoded
                    } else {
                        self.state = .loaded
                    }
                case .retrieving, .retrievalError:
                    self.retrieve() { result in }
                }
            }
        }
    }

    internal func willUpdate (newId: UUID?, closure: (Bool) -> ()) {
        queue.sync {
            closure (willUpdate (newId: newId))
        }
    }
    
    internal func addParentTo (batch: EventuallyConsistentBatch) {
        if self.parent == nil {
            let retrievalResult = self.parentData.collection.get(id: self.parentData.id)
            if let parent = retrievalResult.item() {
                self.parent = parent
            } else {
                self.parentData.collection.database.logger?.log (level: .error, source: self, featureName: "addParentToBatch", message: "noParent", data: [(name:"collectionName", value: self.parentData.collection.name), (name:"parentId", value: self.parentData.id.uuidString), (name:"parentVersion", value: self.parentData.version), (name: "errorMessage", value: "\(retrievalResult)")])
            }
        }
        if let parent = self.parent {
            batch.insertAsync(entity: parent, closure: nil)
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

    private var entity: Entity<T>?
    private var parent: EntityManagement?
    private var parentData: EntityReferenceData<P>
    private var referenceData: EntityReferenceSerializationData?
    private var collection: PersistentCollection<Database, T>?
    private var state: EntityReferenceState
    private let queue: DispatchQueue
    private var pendingEntityClosures: [(RetrievalResult<Entity<T>>) -> ()] = []
    public let retryInterval: TimeInterval = 120.0
    
    public let isEager: Bool
    
    static func queueName (collectionName: String) -> String {
        return "EntityReference.parent: " + collectionName
    }
    
    internal func sync (closure: (EntityReferenceContents<P, T>) -> ()) {
        queue.sync {
            closure ((entity: self.entity, parent: self.parent, parentData: self.parentData, referenceData: self.referenceData, collection: self.collection, state: self.state, isEager: self.isEager, pendingEntityClosureCount: self.pendingEntityClosures.count))
        }
    }
    
    internal func appendClosure (_ closure: @escaping (RetrievalResult<Entity<T>>) -> ()) {
        queue.async {
            self.pendingEntityClosures.append(closure)
        }
    }
    
    internal func setState (state: EntityReferenceState) {
        queue.sync {
            self.state = state
        }
    }
    
}

internal typealias EntityReferenceContents<P: Codable, T: Codable> = (entity: Entity<T>?, parent: EntityManagement?, parentData: EntityReferenceData<P>, referenceData: EntityReferenceSerializationData?, collection: PersistentCollection<Database, T>?, state: EntityReferenceState, isEager: Bool, pendingEntityClosureCount: Int)
