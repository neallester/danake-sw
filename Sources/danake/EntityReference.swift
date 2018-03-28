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
        let parentData = decoder.userInfo[Database.parentDataKey] as? EntityReferenceData<P>
        if let parentData = parentData {
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
                if self.parent == nil {
                    let retrievalResult = self.parentData.collection.get(id: self.parentData.id)
                    if let parent = retrievalResult.item() {
                        self.parent = parent
                    } else {
                        self.parentData.collection.database.logger?.log (level: .error, source: self, featureName: "set:entity", message: "noParent", data: [(name:"collectionName", value: self.parentData.collection.name), (name:"parentId", value: self.parentData.id.uuidString), (name:"parentVersion", value: self.parentData.version), (name: "errorMessage", value: "\(retrievalResult)")])
                    }
                }
                if let parent = self.parent {
                    batch.insertAsync(entity: parent, closure: nil)
                }
            }
        }
    }
    
    internal func willUpdate (newId: UUID?, closure: (Bool) -> ()) {
        queue.sync {
            closure (willUpdate (newId: newId))
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
    
}

internal typealias EntityReferenceContents<P: Codable, T: Codable> = (entity: Entity<T>?, parent: EntityManagement?, parentData: EntityReferenceData<P>, referenceData: EntityReferenceSerializationData?, collection: PersistentCollection<Database, T>?, state: EntityReferenceState, isEager: Bool, pendingEntityClosureCount: Int)
