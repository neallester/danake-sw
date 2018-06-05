//
//  entity.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

public protocol EntityProtocol : class {

    var id: UUID {get}
    var version: Int { get }
    var persistenceState: PersistenceState { get }
    var created: Date { get }
    var saved: Date? { get }

}

internal protocol EntityManagement : EntityProtocol, Encodable {
    
    func commit (completionHandler: @escaping (DatabaseUpdateResult) -> ())
    func commit (timeout: DispatchTimeInterval, completionHandler: @escaping (DatabaseUpdateResult) -> ())
    func referenceData() -> ReferenceManagerData
    func setDirty (batch: EventuallyConsistentBatch)
    
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

    case setDirty
    case updateItem ((inout T) -> ())
    case remove
    case commit (DispatchTimeInterval, (DatabaseUpdateResult) -> ())
    
}

public enum EntityEncodingResult<R> {
    case ok (R)
    case error (String)
}

typealias PersistenceStatePair = (success: PersistenceState, failure: PersistenceState)

internal class DataContainer {
    
    var data: Any?
    
}

// type erased access to the metadata for any Entity
public class AnyEntity : EntityProtocol {
    
    init (_ item: EntityProtocol) {
        self.item = item
        self.id = item.id
    }
    
    public let id: UUID
    
    public var version: Int {
        get {
            return item.version
        }

    }
    
    public var persistenceState: PersistenceState {
        return item.persistenceState
    }

    
    public var created: Date {
        get {
            return item.created
        }
    
    }
    
    public var saved: Date? {
        get {
            return item.saved
        }
    }
    
    let item: EntityProtocol
    
}

/*
    Type erased wrapper for Entity providing access to metadata and
    functionality needed for persistent management
 */
public class EntityPersistenceWrapper : Encodable {
    
    init (collectionName: CollectionName, entity: EntityManagement) {
        self.collectionName = collectionName
        self.entity = entity
        self.id = entity.id
    }
    
    // Not Thread Safe
    public func encode(to encoder: Encoder) throws {
        try entity.encode (to: encoder)
    }
    
    let id: UUID
    
    public let collectionName: CollectionName
    private let entity: EntityManagement
    
   
}

// Data for a reference to an Entity
public struct EntityReferenceData<T: Codable> : Equatable {
    
    public init (collection: EntityCache<T>, id: UUID, version: Int) {
        self.collection = collection
        self.id = id
        self.version = version
    }
    
    let collection: EntityCache<T>
    let id: UUID
    let version: Int
    
    public static func == <T> (lhs: EntityReferenceData<T>, rhs: EntityReferenceData<T>) -> Bool {
        return
            lhs.collection === rhs.collection &&
            lhs.id.uuidString == rhs.id.uuidString &&
            lhs.version == rhs.version
    }
    
}

// Data for serializing the reference state of a ReferenceManager
public struct ReferenceManagerData: Equatable {
    
    internal init (databaseId: String, collectionName: CollectionName, id: UUID, version: Int) {
        self.qualifiedCollectionName = Database.qualifiedCollectionName(databaseHash: databaseId, collectionName: collectionName)
        self.id = id
        self.version = version
    }
    
    internal init (qualifiedCollectionName: QualifiedCollectionName, id: UUID, version: Int) {
        self.qualifiedCollectionName = qualifiedCollectionName
        self.id = id
        self.version = version
    }

    public let qualifiedCollectionName: QualifiedCollectionName
    public let id: UUID
    public let version: Int
    
    public static func == (lhs: ReferenceManagerData, rhs: ReferenceManagerData) -> Bool {
        return
            lhs.qualifiedCollectionName == rhs.qualifiedCollectionName &&
            lhs.id == rhs.id &&
            lhs.version == rhs.version
    }

}

public enum EntityDeserializationError<T: Codable> : Error {
    case NoCollectionInDecoderUserInfo
    case alreadyCached (Entity<T>)
    case illegalId (String)
    case missingUserInfoValue (CodingUserInfoKey)
}

/*
    **** Class Entity is the primary model object wrapper. ****
 
    Danake does not support polymorphic retrieval of Entity items because Swift
    generics are invariant rather than covariant. If polymorphic behavior is required
    the recommended approach is to use a Codable (non-entity) polymorphic delegate
    See https://medium.com/tsengineering/swift-4-0-codable-decoding-subclasses-inherited-classes-heterogeneous-arrays-ee3e180eb556
    for one approach to implementing Codable polymorphic constructs
 
*/
public class Entity<T: Codable> : EntityManagement, Codable {
    
    
    internal init (collection: EntityCache<T>, id: UUID, version: Int, item: T) {
        self.collection = collection
        self.id = id
        self._version = version
        self.item = item
        self.schemaVersion = collection.database.schemaVersion
        _persistenceState = .new
        self.queue = DispatchQueue (label: id.uuidString)
        self.referencesQueue = DispatchQueue (label: "childEntities: \(id.uuidString)")
        created = Date()
        collection.cacheEntity(self)
    }

    internal convenience init (collection: EntityCache<T>, id: UUID, version: Int, itemClosure: (EntityReferenceData<T>) -> T) {
        let selfReference = EntityReferenceData (collection: collection, id: id, version: version)
        let item = itemClosure(selfReference)
        self.init (collection: collection, id: id, version: version, item: item)
    }
    
    // deiniitalize
    
    deinit {
        collection.decache (id: id)
        switch self._persistenceState {
        case .persistent:
            let localItem = item
            if let itemData = itemData {
                let localCollection = collection
                let localId = id
                collection.database.workQueue.async {
                    do {
                        let currentData = try Database.encoder.encode(localItem)
                        if currentData != itemData {
                            localCollection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "lostData:itemModifiedWithoutSave", data: [(name:"collectionName", value: localCollection.qualifiedName), (name: "entityId", value: localId.uuidString)])
                        }
                    } catch {
                        localCollection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "exceptionSerailizingItem", data: [(name:"collectionName", value: localCollection.qualifiedName), (name: "entityId", value: localId.uuidString), (name: "message", value: "\(error)")])
                    }
                }
            } else {
                collection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "noCurrentData", data: [(name:"collectionName", value: collection.qualifiedName), (name: "entityId", value: self.id.uuidString)])
            }
        default:
            break
        }
    }

// Metadata
    
    // Version is incremented each time the entity is stored in the persistent media
    public var version: Int {
        get {
            var result: Int? = nil
            queue.sync {
                result = _version
            }
            return result!
        }
    }
    
    // For use when caller is being executed on queue
    internal func getVersionUnsafe() -> Int {
        return _version
    }
    
    public internal (set) var persistenceState: PersistenceState {
        get {
            var result = PersistenceState.new
            queue.sync {
                result = self._persistenceState
            }
            return result
        }
        set {
            queue.sync {
                self._persistenceState = newValue
            }
        }
    }
    
    public internal(set) var saved: Date? {
        get {
            var result: Date? = nil
            queue.sync {
                result = self._saved
            }
            return result
        }
        set {
            queue.sync {
                self._saved = newValue
            }
        }
    }
    
    // EntityManagement
    
    internal func commit (completionHandler: @escaping (DatabaseUpdateResult) -> ()) {
        commit (timeout: DispatchTimeInterval.seconds(60), completionHandler: completionHandler)
    }
    
    internal func commit (timeout: DispatchTimeInterval, completionHandler: @escaping (DatabaseUpdateResult) -> ()) {
        queue.async {
            self.handleAction (PersistenceAction.commit (timeout, completionHandler))
        }
    }

    // Type Erased Access
    // TODO Add and test
    //    public lazy var asAnyEntity: AnyEntity = {
    //        return AnyEntity(item: self)
    //    }()
    
    // Write Access to item
    

    // Read Only Access to item

    
/*

     *** IMPORTANT NOTES ***
     
     The async (closure:), sync (closure:) and update(closure:) must not assign a reference to item outside
     of the closure. Doing so negates thread safety for item.
     
     The async(closure:) and sync (closure:) cannot assign to item's attributes (because they do not receive an
     inout parameter), however they can still modify item via any of item's state modifying functions.
     Always use update when calling state modifying functions on item.
     
*/
    
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
    
// Convention is Entity.queue -> Batch.queue
    
    public func update (batch: EventuallyConsistentBatch, closure: @escaping (inout T) -> Void) {
        queue.sync {
            isInsertingToBatch = true
            batch.insertSync (entity: self) {
                self.handleAction(.updateItem (closure))
            }
            isInsertingToBatch = false
        }
    }

    // Not Thread Safe
    func setDirty (batch: EventuallyConsistentBatch) {
        if isInsertingToBatch {
            self.handleAction(.setDirty)
        } else {
            batch.insertSync(entity: self) {
                self.handleAction(.setDirty)
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
    public func remove (batch: EventuallyConsistentBatch) {
        queue.sync {
            self.isInsertingToBatch = true
            batch.insertSync(entity: self) {
                self.handleAction(.remove)
            }
            self.isInsertingToBatch = false
        }
    }
    
// Serialization Data
    
    public func referenceData() -> ReferenceManagerData {
        var localVersion = 0
        localVersion = version
        return ReferenceManagerData (databaseId: collection.database.accessor.hashValue(), collectionName: collection.name, id: id, version: localVersion)
    }
    
    /*
        Call before reference to self goes out of scope (or the only reachable reference is
        set to nil) if it is possible that an ReferenceManager owned by `item' contains a loaded
        reference back to self (which would create a strong reference cycle).
     
        This unloads the referenced entities and also makes the ReferenceManagers unusable for
        further application processing but will not interfere with asynchronous batch processing
     
    */
    
    public func breakReferences() {
        referencesQueue.async {
            if !self.hasDereferenced {
                self.hasDereferenced = true
                for reference in self.references {
                    reference.dereference()
                }
            }
        }
    }
    
    public func breakReferencesRecursive() {
        referencesQueue.async {
            if !self.hasDereferenced {
                self.hasDereferenced = true
                for reference in self.references {
                    reference.dereferenceRecursive()
                }
            }
        }
    }

    // Reference Registration
    // This is used by attributes of self containing references (e.g. ReferenceManager)
    // to register with self (to be notified, for example, when self receives instruction
    // to breakReferences). It is up to caller to only call once.
    internal func registerReferenceContainer (_ reference: ReferenceManagerContainer) {
        referencesQueue.async {
            self.references.append(reference)
        }
    }
    
// Persistence Action Handling
    
    // Not Thread Safe, caller must be within ** queue ***
    internal func handleAction (_ action: PersistenceAction<T>) {
        switch action {
        case .updateItem(let closure):
            switch self._persistenceState {
            case .persistent, .pendingRemoval:
                self._persistenceState = .dirty
                closure(&item)
            case .abandoned:
                self._persistenceState = .new
                closure(&item)
            case .saving:
                pendingAction = .update
                closure(&item)
            case .new, .dirty:
                closure(&item)
            }
        case .setDirty:
            switch self._persistenceState {
            case .persistent, .pendingRemoval:
                self._persistenceState = .dirty
            case .abandoned:
                self._persistenceState = .new
            case .saving:
                pendingAction = .update
            case .new, .dirty:
                break
            }
        case .remove:
            switch self._persistenceState {
            case .persistent, .dirty:
                self._persistenceState = .pendingRemoval
            case .new:
                self._persistenceState = .abandoned
            case .saving:
                self.pendingAction = .remove
            case .pendingRemoval, .abandoned:
                break
            }
        case .commit(let timeout, let completionHandler):
            switch self._persistenceState {
            case .persistent, .abandoned, .saving:
                callCommitCompletionHandler (completionHandler: completionHandler, result: .ok)
            case .new:
                commit (successState: .persistent, failureState: .new, timeout: timeout, completionHandler: completionHandler) { accessor in
                    accessor.addAction
                }
            case .dirty:
                commit (successState: .persistent, failureState: .dirty, timeout: timeout, completionHandler: completionHandler) { accessor in
                    accessor.updateAction
                }
            case .pendingRemoval:
                commit (successState: .abandoned, failureState: .pendingRemoval, timeout: timeout, completionHandler: completionHandler) { accessor in
                    accessor.removeAction
                }
            }
        }
    }
    
    // Not thread safe: to be called on self.queue
    private func commit (successState: PersistenceState, failureState: PersistenceState, timeout: DispatchTimeInterval, completionHandler: @escaping (DatabaseUpdateResult) -> (), databaseActionSource: (DatabaseAccessor) -> (EntityPersistenceWrapper) -> DatabaseActionResult) {
        _persistenceState = successState
        _version = _version + 1
        let wrapper = EntityPersistenceWrapper (collectionName: collection.name, entity: self)
        let actionSource = databaseActionSource (collection.database.accessor)
        let actionResult = actionSource (wrapper)
        var newItemData: Data? = nil
        do {
            newItemData = try Database.encoder.encode(item)
        } catch {}
        switch actionResult {
        case .ok (let action):
            _persistenceState = .saving
            var result: DatabaseUpdateResult? = nil
            collection.database.workQueue.async {
                let group = DispatchGroup()
                group.enter()
                self.collection.database.workQueue.async {
                    let tempResult = action()
                    self.queue.sync {
                        result = tempResult
                        group.leave()
                    }
                }
                self.collection.database.workQueue.async {
                    self.timeoutTestingHook()
                    let _ = group.wait(timeout: DispatchTime.now() + timeout)
                    self.queue.async {
                        if result == nil {
                            result = .error ("Entity.commit():timedOut:\(timeout)")
                        }
                        if let result = result {
                            switch result {
                            case .ok:
                                self._persistenceState = successState
                                self.itemData = newItemData
                            case .error, .unrecoverableError:
                                self._persistenceState = failureState
                                self._version = self._version - 1
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
                                    self.handleAction(.commit (timeout, completionHandler))
                                // see https://github.com/neallester/danake-sw/issues/3
                                case .error, .unrecoverableError:
                                    self.callCommitCompletionHandler (completionHandler: completionHandler, result: result)
                                }
                                
                            } else {
                                self.callCommitCompletionHandler (completionHandler: completionHandler, result: result)
                            }
                        }
                    }

                }
            }
        case .error (let errorMessage):
            self._version = self._version - 1
            _persistenceState = failureState
            self.callCommitCompletionHandler (completionHandler: completionHandler, result: .unrecoverableError(errorMessage))
        }
    }
    
    private func callCommitCompletionHandler (completionHandler: @escaping (DatabaseUpdateResult) -> (), result: DatabaseUpdateResult) {
        collection.database.workQueue.async {
            completionHandler (result)
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
        try container.encode (_version, forKey: .version)
        try container.encode (schemaVersion, forKey: .schemaVersion)
        try container.encode(item, forKey: .item)
        try container.encode (created, forKey: .created)
        try container.encode (_persistenceState, forKey: .persistenceState)
        if let _saved = _saved {
            try container.encode (_saved, forKey: .saved)
        }
    }
    
    // Model objects requiring access to the enclosing entities id or schemaVersion may obtain
    // it from the EntityReferenceData stored in userInfo[Database.parentKeyData]
    public required init (from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let collection = decoder.userInfo[Database.collectionKey] as? EntityCache<T>
        if let collection = collection {
            let idString = try values.decode(String.self, forKey: .id)
            let id = UUID (uuidString: idString)
            if let id = id {
                if let cachedVersion = collection.cachedEntity(id: id) {
                    throw EntityDeserializationError<T>.alreadyCached(cachedVersion)
                }
                self.id = id
                self.collection = collection
                schemaVersion = collection.database.schemaVersion
                let version = try values.decode(Int.self, forKey: .version)
                self._version = version
                if let container = decoder.userInfo[Database.parentDataKey] as? DataContainer {
                    container.data = EntityReferenceData (collection: collection, id: id, version: version)
                    item = try values.decode (T.self, forKey: .item)
                    container.data = nil
                } else {
                    item = try values.decode (T.self, forKey: .item)
                }
                created = try values.decode (Date.self, forKey: .created)
                if values.contains(.saved) {
                    _saved = try values.decode (Date.self, forKey: .saved)
                }
                _persistenceState = try values.decode (PersistenceState.self, forKey: .persistenceState)
                self.queue = DispatchQueue (label: id.uuidString)
                self.referencesQueue = DispatchQueue (label: "childEntities: \(id.uuidString)")
                do {
                    self.itemData = try Database.encoder.encode(self.item)
                } catch {
                    collection.database.logger?.log(level: .error, source: self, featureName: "init(from decoder:)", message: "itemDecodingFailed", data: [(name: "databaseId", value: (collection.database.accessor.hashValue())), (name: "collectionName", value: collection.name), (name: "entityId", value: self.id.uuidString)])
                }
                collection.cacheEntity(self)
            } else {
                throw EntityDeserializationError<T>.illegalId(idString)
            }
        } else {
            throw EntityDeserializationError<T>.NoCollectionInDecoderUserInfo
        }
    }
    
// Initialization
    
    internal func isInitialized (onCollection: EntityCache<T>) -> Bool {
        var result = false
        queue.sync {
            if self.collection === onCollection, collection.database.schemaVersion == self.schemaVersion {
                result = true
            }
        }
        return result
    }
    
// Internal access for testing purposes
    
    internal func getSchemaVersion () -> Int {
        var result: Int = 0
        queue.sync {
            result = self.schemaVersion
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
    
    internal func referenceContainers (closure: ([ReferenceManagerContainer]) -> ()) {
        referencesQueue.sync {
            closure (self.references)
        }
    }
    
    internal func timeoutTestingHook() {}
    
// Attributes
    
    public let id: UUID
    private var _version: Int
    public let created: Date
    private var _saved: Date?
    private var item: T
    private var itemData: Data?
    fileprivate let queue: DispatchQueue
    private var _persistenceState: PersistenceState
    internal let collection: EntityCache<T>
    private var schemaVersion: Int
    private var pendingAction: PendingAction? = nil
    private var onDatabaseUpdateStates: PersistenceStatePair? = nil
    private var isInsertingToBatch = false
    private var references: [ReferenceManagerContainer] = []
    private var referencesQueue: DispatchQueue
    private var hasDereferenced = false

}

