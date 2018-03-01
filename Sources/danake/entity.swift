//
//  entity.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

protocol EntityManagement : Encodable {
    
    func getId() -> UUID
    func getVersion() -> Int
    func updateStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S>
    
}

enum PersistenceState : String, Codable {
    
    case new
    case dirty
    case persistent
    
}

enum EntityConversionResult<R> {
    case ok (R)
    case error (String)
}

class AnyEntityManagement : EntityManagement {
    
    public func encode(to encoder: Encoder) throws {
        try item.encode (to: encoder)
    }
    
    func getId() -> UUID {
        return item.getId()
    }
    func getVersion() -> Int {
        return item.getVersion()
    }
    func updateStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S> {
        return item.updateStatement(converter: converter)
    }

    init (item: EntityManagement) {
        self.item = item
    }
    
    let item: EntityManagement
   
}

public class Entity<T: Codable> : EntityManagement, Codable {
    
    init (collection: PersistentCollection<Database, T>, id: UUID, version: Int, item: T) {
        self.collection = collection
        self.id = id
        self.version = version
        self.item = item
        self.schemaVersion = collection.database.schemaVersion
        persistenceState = .new
        self.queue = DispatchQueue (label: id.uuidString)
        created = Date()
    }

    convenience init (collection: PersistentCollection<Database, T>, id: UUID, version: Int, itemClosure: (EntityReferenceData<T>) -> T) {
        let selfReference = EntityReferenceData (collection: collection, id: id, version: version)
        let item = itemClosure(selfReference)
        self.init (collection: collection, id: id, version: version, item: item)
    }
    
    // deiniitalize
    
    deinit {
        collection?.decache (id: id)
    }
    


// EntityManagement
    
    // TODO Should go into queue (Batch doesn't need to access version, which is why this isn't in the queue
    //      but this change can't be done until the change to Batch is made
    func getVersion() -> Int {
            return version
    }
    
    func incrementVersion() {
        queue.sync {
            version = version + 1
        }
    }
    
    func getId() -> UUID {
        return self.id
    }
    
    func getPersistenceState() -> PersistenceState {
        var result = PersistenceState.new
        queue.sync {
            result = self.persistenceState
        }
        return result
    }
    
    func updateStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S> {
        if let collection = collection {
            var result: EntityConversionResult<S>? = nil
            queue.sync {
                version = version + 1
                persistenceState = .persistent
                saved = Date()
                result = converter (collection.name, AnyEntityManagement (item: self))
            }
            return result!
        } else {
            return .error ("\(type (of: self)): Missing Collection; id=\(id.uuidString)")
        }
    }
    
// Read Only Access to item
    
    public func async (closure: @escaping (T) -> Void) {
        queue.async () {
            closure (self.item)
        }
    }

    public func sync (closure: (T) -> Void) {
        queue.sync () {
            closure (self.item)
        }
    }

// Write Access to item
    
    /*
        TODO EventuallyConsistentBatch and ConsistentBatch both descedend from BatchImplementation and Implement Protocol Batch
        The exsting async & sync features take EventuallyConsistentBatch
        new sync feature which return an enum like
             enum BatchSubmitResult {
                case accepted
                case rejected
             }
        Dirty entities which are not already in the batch are rejected from a fully
        consistent batch
    */

    public func async (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.async () {
            batch.insertAsync(item: self) {
                self.persistenceState = .dirty
                closure (&self.item)
            }
        }
    }
    
    public func sync (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.sync {
            batch.insertSync (item: self) {
                self.persistenceState = .dirty
                closure (&self.item)
            }
        }
    }
    
// Codable
    
    enum CodingKeys: String, CodingKey {
        case id
        case version
        case item
        case schemaVersion
        case created
        case saved
        case persistenceState
    }
    
    // ** Not Thread Safe ** Caller must ensure encoding occurs on self.queue
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode (id, forKey: .id)
        try container.encode (version, forKey: .version)
        try container.encode (schemaVersion, forKey: .schemaVersion)
        try container.encode(item, forKey: .item)
        try container.encode (created, forKey: .created)
        try container.encode (persistenceState, forKey: .persistenceState)
        if let saved = saved {
            try container.encode (saved, forKey: .saved)
        }
    }
    
    public required init (from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        id = try UUID (uuidString: values.decode(String.self, forKey: .id))!
        version = try values.decode(Int.self, forKey: .version)
        item = try values.decode (T.self, forKey: .item)
        schemaVersion = try values.decode (Int.self, forKey: .schemaVersion)
        created = try values.decode (Date.self, forKey: .created)
        if values.contains(.saved) {
            saved = try values.decode (Date.self, forKey: .saved)
        }
        persistenceState = try values.decode (PersistenceState.self, forKey: .persistenceState)
        self.queue = DispatchQueue (label: id.uuidString)
    }
    
// Setters
    
    func setCollection (_ collection: PersistentCollection<Database, T>) {
        self.collection = collection
    }
    
// Attributes
    
    
    
    public let id: UUID
    public internal (set) var version: Int
    internal var schemaVersion: Int
    internal let created: Date
    internal var saved: Date?
    private var item: T
    private let queue: DispatchQueue
    internal var persistenceState: PersistenceState
    internal private(set) var collection: PersistentCollection<Database, T>? // is nil when first decoded after database retrieval
    
    
}

public struct EntityReferenceData<T: Codable> {
    
    public init (collection: PersistentCollection<Database, T>, id: UUID, version: Int) {
        self.collection = collection
        self.id = id
        self.version = version
    }

    let collection: PersistentCollection<Database, T>
    let id: UUID
    let version: Int
    
}
