//
//  entity.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

public protocol EntityProtocol {

    func getId() -> UUID
    func getVersion() -> Int
    func getPersistenceState() -> PersistenceState
    func getCreated() -> Date
    func getSaved() -> Date?

}

public protocol EntityManagement : EntityProtocol, Encodable {
    
    func updateStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S>
    func removeStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S>
    
}

public enum PersistenceState : String, Codable {
    
    case new
    case dirty
    case persistent
    
}

public enum EntityConversionResult<R> {
    case ok (R)
    case error (String)
}

// type erased access to the metadata for any Entity
public class AnyEntity : EntityProtocol {
    
    init (item: EntityProtocol) {
        self.item = item
    }
    
    public func getId() -> UUID {
        return item.getId()
    }
    
    public func getVersion() -> Int {
        return item.getVersion()
    }
    
    public func getPersistenceState() -> PersistenceState {
        return item.getPersistenceState()
    }
    
    public func getCreated() -> Date {
        return item.getCreated()
    }
    
    public func getSaved() -> Date? {
        return item.getSaved()
    }
    
    let item: EntityProtocol
    
}

/*
    Type erased wrapper for any Entity providing access to metadata and
    functionality needed for persistent management
 */
public class AnyEntityManagement : EntityManagement, Encodable {
    
    init (item: EntityManagement) {
        self.item = item
    }
    
    // Not Thread Safe
    public func encode(to encoder: Encoder) throws {
        try item.encode (to: encoder)
    }
    
    public func getId() -> UUID {
        return item.getId()
    }
    public func getVersion() -> Int {
        return item.getVersion()
    }
    
    public func getPersistenceState() -> PersistenceState {
        return item.getPersistenceState()
    }
    
    public func getCreated() -> Date {
        return item.getCreated()
    }
    
    public func getSaved() -> Date? {
        return item.getSaved()
    }
    
    public func updateStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S> {
        return item.updateStatement(converter: converter)
    }

    public func removeStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S> {
        return item.removeStatement(converter: converter)
    }

    let item: EntityManagement
   
}

/*
    Model object wrapper.
*/
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
    
    public func getId() -> UUID {
        return self.id
    }
    
    // Version is incremented each time the entity is stored in the persistent media
    public func getVersion() -> Int {
        var result: Int? = nil
        queue.sync {
            result = version
        }
        return result!
    }
    
    public func getPersistenceState() -> PersistenceState {
        var result = PersistenceState.new
        queue.sync {
            result = self.persistenceState
        }
        return result
    }
    
    public func getCreated() -> Date {
        return created
    }
    
    public func getSaved() -> Date? {
        var result: Date? = nil
        queue.sync {
            result = self.saved
        }
        return result
    }

    /*
        Update Entity metadata to reflect being saved to the persistent media and return the media specific update statement to make it so
     
        Calling other thread safe features of the AnyEntityManagement protocol (e.g. AnyEntityManagement.getVersion()) within ** converter **
        will produce a thread deadlock
    */
    public func updateStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S> {
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
            return .error ("\(type (of: self)).updateStatement: Missing Collection: Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(id.uuidString)")
        }
    }
    
    /*
        Update Entity metadata to reflect being removed from the persistent media and return the media specific update statement to make it so
     
        Calling other thread safe features of the AnyEntityManagement protocol (e.g. AnyEntityManagement.getVersion()) within ** converter **
        will produce a thread deadlock
     */
    public func removeStatement<S> (converter: (CollectionName, AnyEntityManagement) -> EntityConversionResult<S>) -> EntityConversionResult<S> {
        if let collection = collection {
            var result: EntityConversionResult<S>? = nil
            queue.sync {
                version = version + 1
                persistenceState = .new
                saved = Date()
                result = converter (collection.name, AnyEntityManagement (item: self))
            }
            return result!
        } else {
            return .error ("\(type (of: self)).removeStatement: Missing Collection: Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(id.uuidString)")
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
        TODO add asyncIfUpdated, syncIfUpdated, which only add to the batch if closure makes a change
        to the object
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
    
// Initialization
    
    internal func setCollection (collection: PersistentCollection<Database, T>) {
        queue.async {
            self.collection = collection
            self.schemaVersion = collection.database.schemaVersion
        }
    }
    
    internal func isInitialized (onCollection: PersistentCollection<Database, T>) -> Bool {
        var result = false
        queue.sync {
            if let collection = self.collection, collection === onCollection, collection.database.schemaVersion == self.schemaVersion {
                result = true
            }
        }
        return result
    }

// Internal access for testing purposes
    
    internal func setSchemaVersion (_ schemaVersion: Int) {
        queue.sync {
            self.schemaVersion = schemaVersion
        }
    }

    internal func getSchemaVersion () -> Int {
        var result: Int = 0
        queue.sync {
            result = self.schemaVersion
        }
        return result
    }
    
    internal func setSaved (_ saved: Date?) {
        queue.sync {
            self.saved = saved
        }
    }
    
    internal func setPersistenceState (_ persistenceState: PersistenceState) {
        queue.sync {
            self.persistenceState = persistenceState
        }
    }
    
    internal func getCollection() -> PersistentCollection<Database, T>? {
        var result: PersistentCollection<Database, T>? = nil
        queue.sync {
            result = collection
        }
        return result
    }
    
// Attributes
    
    public let id: UUID
    private var version: Int
    public let created: Date
    private var saved: Date?
    private var item: T
    private let queue: DispatchQueue
    private var persistenceState: PersistenceState
    private private(set) var collection: PersistentCollection<Database, T>? // is nil when first decoded after database retrieval
    private var schemaVersion: Int

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
