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

internal protocol EntityManagement : EntityProtocol, Encodable {
    
    func commit (completionHandler: @escaping (DatabaseUpdateResult) -> ())
    
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
    case remove
    case commit ((DatabaseUpdateResult) -> ())
    
}

public enum EntityEncodingResult<R> {
    case ok (R)
    case error (String)
}

struct PersistenceStatePair : Codable {
    
    init (success: PersistenceState, failure: PersistenceState) {
        self.success = success
        self.failure = failure
    }
    
    let success: PersistenceState
    let failure: PersistenceState
    
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
    
    // Not Thread Safe
    public func encode(to encoder: Encoder) throws {
        try item.encode (to: encoder)
    }
    
    public func getId() -> UUID {
        return item.getId()
    }
    
    public let collectionName: CollectionName
    private let item: EntityManagement
    
   
}

// Data for a reference to an Entity
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

/*
    **** Class Entity is the primary model object wrapper. ****
*/
public class Entity<T: Codable> : EntityManagement, Codable {
    
    internal init (collection: PersistentCollection<Database, T>, id: UUID, version: Int, item: T) {
        self.collection = collection
        self.id = id
        self.version = version
        self.item = item
        self.schemaVersion = collection.database.schemaVersion
        persistenceState = .new
        self.queue = DispatchQueue (label: id.uuidString)
        created = Date()
    }

    internal convenience init (collection: PersistentCollection<Database, T>, id: UUID, version: Int, itemClosure: (EntityReferenceData<T>) -> T) {
        let selfReference = EntityReferenceData (collection: collection, id: id, version: version)
        let item = itemClosure(selfReference)
        self.init (collection: collection, id: id, version: version, item: item)
    }
    
    // deiniitalize
    
    deinit {
        collection?.decache (id: id)
    }

// Metadata
    
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
    
    // EntityManagement
    
    func commit (completionHandler: @escaping (DatabaseUpdateResult) -> ()) {
        queue.async {
            self.handleAction (PersistenceAction.commit (completionHandler))
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
    
// Type Erased Access
// TODO Add and test
//    public lazy var asAnyEntity: AnyEntity = {
//        return AnyEntity(item: self)
//    }()

// Write Access to item

// Convention is Entity.queue -> Batch.queue
    
    public func async (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.async () {
            batch.insertSync(item: self) {
                self.handleAction(.updateItem (closure))
            }
        }
    }
    
    public func sync (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.sync {
            batch.insertSync (item: self) {
                self.handleAction(.updateItem (closure))
            }
        }
    }
    
// Removal
    
    /*
         Remove ** self ** from persistent media.
     
         Application developers may safely dereference an Entity after calling ** remove(). **
         Modifying an Entity on which ** remove() ** has been called will reanimate it.
     
        It is the application's responsibility to ensure that no other Items (in memory or persistent storage)
        contain persistent references to the removed Entity; removing an Entity to which there are references
        will produce errors (i.e. RetrievalResult.error) when those references are accessed.
     */
    public func remove (batch: Batch) {
        queue.async {
            batch.insertSync(item: self) {
                self.handleAction(.remove)
            }
        }
    }
    
// Persistence Action Handling
    
    // Not Thread Safe, caller must be within ** queue ***
    internal func handleAction (_ action: PersistenceAction<T>) {
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
        case .remove:
            switch self.persistenceState {
            case .persistent, .dirty:
                self.persistenceState = .pendingRemoval
            case .new:
                self.persistenceState = .abandoned
            case .saving:
                self.pendingAction = .remove
            case .pendingRemoval, .abandoned:
                break
            }
        case .commit(let completionHandler):
            switch self.persistenceState {
            case .persistent, .abandoned, .saving:
                callCommitCompletionHandler (completionHandler: completionHandler, result: .ok)
            case .new:
                commit (successState: .persistent, failureState: .new, completionHandler: completionHandler) { accessor in
                    accessor.addAction
                }
            case .dirty:
                commit (successState: .persistent, failureState: .dirty, completionHandler: completionHandler) { accessor in
                    accessor.updateAction
                }
            case .pendingRemoval:
                commit (successState: .persistent, failureState: .pendingRemoval, completionHandler: completionHandler) { accessor in
                    accessor.removeAction
                }
            }
        }
    }
    
    // Not thread safe: to be called on self.queue
    private func commit (successState: PersistenceState, failureState: PersistenceState, completionHandler: @escaping (DatabaseUpdateResult) -> (), databaseActionSource: (DatabaseAccessor) -> (EntityPersistenceWrapper) -> DatabaseActionResult) {
        if let collection = collection {
            persistenceState = successState
            version = version + 1
            let wrapper = EntityPersistenceWrapper (collectionName: collection.name, item: self)
            let actionSource = databaseActionSource (collection.database.accessor)
            let actionResult = actionSource (wrapper)
            switch actionResult {
            case .ok (let action):
                persistenceState = .saving
                collection.database.workQueue.async {
                    let result = action()
                    self.queue.async {
                        switch result {
                        case .ok:
                            self.persistenceState = successState
                        case .error, .unrecoverableError:
                            self.persistenceState = failureState
                            self.version = self.version - 1
                        }
                        if let pendingAction = self.pendingAction {
                            self.pendingAction = nil
                            switch pendingAction {
                            case .update:
                                self.handleAction(PersistenceAction.updateItem() { item in })
                            case .remove:
                                self.handleAction(PersistenceAction.remove)
                            }
                            switch result {
                            case .ok:
                                self.handleAction(.commit (completionHandler))
                            case .error, .unrecoverableError:
                                self.callCommitCompletionHandler (completionHandler: completionHandler, result: result)
                            }
                            
                        } else {
                            self.callCommitCompletionHandler (completionHandler: completionHandler, result: result)
                        }
                    }
                }
            case .error (let errorMessage):
                self.version = self.version - 1
                persistenceState = failureState
                self.callCommitCompletionHandler (completionHandler: completionHandler, result: .unrecoverableError(errorMessage))
            }
            
        } else {
            persistenceState = failureState
            self.callCommitCompletionHandler (completionHandler: completionHandler, result:.unrecoverableError ("\(type (of: self)).commit|missing_collection|Always use PersistentCollection.entityForProspect or PersistentCollection.initialize when implementing custom PersistentCollection getters; id=\(id.uuidString)"))
        }
    }
    
    private func callCommitCompletionHandler (completionHandler: @escaping (DatabaseUpdateResult) -> (), result: DatabaseUpdateResult) {
        if let collection = collection {
            collection.database.workQueue.async {
                completionHandler (result)
            }
        } else {
            queue.async {
                completionHandler (result)
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
    
    internal func getPendingAction() -> PendingAction? {
        var result: PendingAction? = nil
        queue.sync {
            result = pendingAction
        }
        return result
    }
    
    internal func asData (encoder: JSONEncoder) -> Data? {
        var result: Data? = nil
        queue.sync {
            do {
                try result = encoder.encode (self)
            } catch {
                print ("Trouble encoding: \(error)")
            }
        }
        return result
    }
    
// Attributes
    
    public let id: UUID
    private var version: Int
    public let created: Date
    private var saved: Date?
    private var item: T
    fileprivate let queue: DispatchQueue
    private var persistenceState: PersistenceState
    private private(set) var collection: PersistentCollection<Database, T>? // is nil when first decoded after database retrieval
    private var schemaVersion: Int
    private var pendingAction: PendingAction? = nil
    private var onDatabaseUpdateStates: PersistenceStatePair? = nil

}
