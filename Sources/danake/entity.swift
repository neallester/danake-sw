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

public protocol EntityManagement : EntityProtocol, Encodable {}

struct PersistenceStatePair : Codable {
    
    init (success: PersistenceState, failure: PersistenceState) {
        self.success = success
        self.failure = failure
    }
    
    let success: PersistenceState
    let failure: PersistenceState
    
}

public enum PersistenceState : String, Codable {
    
    case new                // Not yet in persistent media
    case dirty              // Application version differs from persistent media
    case persistent         // Application contents are the same as in persistent media
    case pendingRemoval     // Application has requested this entity be removed from persistent media
    case abandoned          // The entity was "removed" before being persisted (will be ignored when Batch is committed)
    case saving             // Entity is currently being saved to persistent media

}

public enum PendingAction {
    case update
    case remove
}

public enum PersistenceAction<T: Codable> {

    case updateItem ((inout T) -> ())
    
}

public enum EntityEncodingResult<R> {
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
    Type erased wrapper for Entity providing access to metadata and
    functionality needed for persistent management
 */
public class EntityPersistenceWrapper : Encodable {
    
    init (collectionName: CollectionName, item: EntityManagement) {
        self.collectionName = collectionName
        self.item = item
    }
    
    public func encode(to encoder: Encoder) throws {
        try item.encode (to: encoder)
    }
    
    public func getId() -> UUID {
        return item.getId()
    }

    public let collectionName: CollectionName
    private let item: EntityManagement
    
   
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

    // TODO Keep for log 
    //             return .error ("\(type (of: self)).updateStatement: Missing Collection: Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(id.uuidString)")


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
    
    public func async (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.async () {
            batch.insertAsync(item: self) {
                self.handleImplementation(.updateItem (closure))
            }
        }
    }
    
    public func sync (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.sync {
            batch.insertSync (item: self) {
                self.handleImplementation(.updateItem (closure))
            }
        }
    }
    
// Persistence Action Handling
    
    internal func handleAction (_ action: PersistenceAction<T>) {
        queue.async {
            self.handleImplementation(action)
        }
    }
    
    private func handleImplementation (_ action: PersistenceAction<T>) {
        switch action {
        case .updateItem(let closure):
            switch self.persistenceState {
            case .persistent, .pendingRemoval:
                persistenceState = .dirty
                closure(&item)
            case .abandoned:
                persistenceState = .new
                closure(&item)
            case .saving:
                pendingAction = .update
                closure(&item)
            case .new, .dirty:
                closure(&item)
            }
        }
    }

    
    // ************************* TODO Add test cases for .saving & .rmoving async & sync
    
// Removal
    
    /*
        Remove ** self ** from persistent media.
     
        Application developers may safely dereference an Entity after calling ** remove(). **
        Modifying an Entity on which ** remove() ** has been called will reanimate it.

        It is the application's responsibility to ensure that no other items contain references
        to the removed Entity; removing an Entity to which there are references will produce
        errors (i.e. RetrievalResult.error) when those references are accessed.
    */
//    public func remove (batch: Batch) {
//        queue.async {
//            batch.insertAsync(item: self) {
//                switch self.persistenceState {
//                case .persistent:
//                    self.persistenceState = .pendingRemoval
//                case .new:
//                    self.persistenceState = .abandoned
//                case .dirty:
//                    self.persistenceState = .pendingRemoval
//                case .abandoned:
//                    break
//                case .pendingRemoval:
//                    break
//                }
//            }
//        }
//    }
    
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
    
    internal func getPendingAction() -> PendingAction? {
        var result: PendingAction? = nil
        queue.sync {
            result = pendingAction
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
    private var pendingAction: PendingAction? = nil
    private var onDatabaseUpdateStates: PersistenceStatePair? = nil
    

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
