//
//  entity.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation
import PromiseKit

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

/**
    The state of an Entity with regard to persistent storage
*/
public enum PersistenceState : String, Codable {
    
    /// Not yet in persistent media
    case new
    
    /// Application version differs from persistent media
    case dirty
    
    /// Application contents are the same as in persistent media
    case persistent
    
    /// Application has requested this entity be removed from persistent media
    case pendingRemoval
    
    /// The entity was "removed" before being persisted (will be ignored when Batch is committed)
    case abandoned
    
    /// Entity is currently being saved to persistent media
    case saving

}

internal enum PendingAction {
    case update
    case remove
}

internal enum PersistenceAction<T: Codable> {

    case setDirty
    case updateItem ((inout T) -> ())
    case remove
    case commit (DispatchTimeInterval, (DatabaseUpdateResult) -> ())
    
}

internal enum EntityEncodingResult<R> {
    case ok (R)
    case error (String)
}

typealias PersistenceStatePair = (success: PersistenceState, failure: PersistenceState)

public class DataContainer {
    
    public init() {}
    
    var data: Any?
    
}

/**
    Type erased wrapper for Entity providing access to metadata and
    functionality needed for persistent management
 */
public class EntityPersistenceWrapper : Encodable {
    
    init (cacheName: CacheName, entity: EntityManagement) {
        self.cacheName = cacheName
        self.entity = entity
        self.id = entity.id
    }
    
    // Not Thread Safe
    public func encode(to encoder: Encoder) throws {
        try entity.encode (to: encoder)
    }
    
    public let id: UUID
    
    public let cacheName: CacheName
    private let entity: EntityManagement
   
}

/**
 Data defining a reference to an Entity.
 
 The generic parameter **T** defines the type of the Model construct associated with the
 referenced Entity (i.e. the Entities item.).
 
 Used when children require a reference to a parent entity during creation of the parent
*/
public struct EntityReferenceData<T: Codable> : Equatable {
    
    public init (cache: EntityCache<T>, id: UUID, version: Int) {
        self.cache = cache
        self.id = id
        self.version = version
    }
    
    /// The cache which managers the referenced Entity
    let cache: EntityCache<T>
    
    /// The id of the referenced Entity
    let id: UUID
    
    /// The version of the refrenced Entity
    let version: Int
    
    public static func == <T> (lhs: EntityReferenceData<T>, rhs: EntityReferenceData<T>) -> Bool {
        return
            lhs.cache === rhs.cache &&
            lhs.id.uuidString == rhs.id.uuidString &&
            lhs.version == rhs.version
    }
    
}

/**
    Data for serializing a reference to an Entity.
 
    May also be used as a kind of Entity pointer to copy a reference to an Entity between ReferenceManagers
    without retrieving the Entity from persistent storage.
 */
public struct ReferenceManagerData: Equatable {
    
    internal init (databaseId: String, cacheName: CacheName, id: UUID, version: Int) {
        self.qualifiedCacheName = Database.qualifiedCacheName(databaseHash: databaseId, cacheName: cacheName)
        self.id = id
        self.version = version
    }
    
    internal init (qualifiedCacheName: QualifiedCacheName, id: UUID, version: Int) {
        self.qualifiedCacheName = qualifiedCacheName
        self.id = id
        self.version = version
    }

    /// Name of the EntityCache which manages the referenced Entity qualified by the Database.hashValue
    public let qualifiedCacheName: QualifiedCacheName
    
    /// Id of the referenced Entity
    public let id: UUID
    
    /// Version of the referenced Entity
    public let version: Int
    
    public static func == (lhs: ReferenceManagerData, rhs: ReferenceManagerData) -> Bool {
        return
            lhs.qualifiedCacheName == rhs.qualifiedCacheName &&
            lhs.id == rhs.id &&
            lhs.version == rhs.version
    }

}

internal enum EntityDeserializationError<T: Codable> : Error {
    case NoCollectionInDecoderUserInfo
    case alreadyCached (Entity<T>)
    case illegalId (String)
    case missingUserInfoValue (CodingUserInfoKey)
}

/**
    Class Entity is the primary model object wrapper. Application developers work with objects of type
    Entity<Model: Codable> (where Model is any construct used in the model). The Entity wrapper provides thread
    safe access to the model construct, houses persistence related metadata about the model construct, and
    manages (some of) the details regarding persistent storage.
 
    The generic parameter **T** defines the type of the construct wrapped by the Entity (i.e. the type of its
    item*.
 
    Danake does not support polymorphic retrieval of Entity items because Swift
    generics are invariant rather than covariant. If polymorphic behavior is required
    the recommended approach is to use a Codable (non-entity) polymorphic delegate
    See https://medium.com/tsengineering/swift-4-0-codable-decoding-subclasses-inherited-classes-heterogeneous-arrays-ee3e180eb556
    for one approach to implementing Codable polymorphic constructs
*/
public class Entity<T: Codable> : EntityManagement, Codable {

    /// Mark - init and deinit
    
    internal init (cache: EntityCache<T>, id: UUID, version: Int, item: T) {
        self.cache = cache
        self.id = id
        self._version = version
        self.item = item
        self.schemaVersion = cache.database.schemaVersion
        _persistenceState = .new
        self.queue = DispatchQueue (label: id.uuidString)
        self.referencesQueue = DispatchQueue (label: "childEntities: \(id.uuidString)")
        created = Date()
        cache.cacheEntity(self)
    }

    internal convenience init (cache: EntityCache<T>, id: UUID, version: Int, itemClosure: (EntityReferenceData<T>) -> T) {
        let selfReference = EntityReferenceData (cache: cache, id: id, version: version)
        let item = itemClosure(selfReference)
        self.init (cache: cache, id: id, version: version, item: item)
    }
    
    deinit {
        cache.decache (id: id)
        let localCollection = cache
        let localId = id
        switch self._persistenceState {
        case .persistent:
            let localItem = item
            if let itemData = itemData {
                cache.database.workQueue.async {
                    do {
                        let currentData = try Database.encoder.encode(localItem)
                        if currentData != itemData {
                            localCollection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "lostData:itemModifiedOutsideOfBatch", data: [(name:"cacheName", value: localCollection.qualifiedName), (name: "entityId", value: localId.uuidString)])
                        }
                    } catch {
                        localCollection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "exceptionSerailizingItem", data: [(name:"cacheName", value: localCollection.qualifiedName), (name: "entityId", value: localId.uuidString), (name: "message", value: "\(error)")])
                    }
                }
            } else {
                cache.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "noCurrentData", data: [(name:"cacheName", value: cache.qualifiedName), (name: "entityId", value: self.id.uuidString)])
            }
        case .dirty:
            localCollection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "lostData:itemModifiedBatchAbandoned", data: [(name:"cacheName", value: localCollection.qualifiedName), (name: "entityId", value: localId.uuidString)])
        case .pendingRemoval:
            localCollection.database.logger?.log(level: .error, source: Entity.self, featureName: "deinit", message: "lostData:itemRemovedBatchAbandoned", data: [(name:"cacheName", value: localCollection.qualifiedName), (name: "entityId", value: localId.uuidString)])
        default:
            break
        }
    }

/// Mark - Metadata
    
    /// Version is incremented each time the entity is stored in the persistent media
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
    
    /// Mark - EntityManagement
    
    internal func commit (completionHandler: @escaping (DatabaseUpdateResult) -> ()) {
        commit (timeout: DispatchTimeInterval.seconds(60), completionHandler: completionHandler)
    }
    
    internal func commit (timeout: DispatchTimeInterval, completionHandler: @escaping (DatabaseUpdateResult) -> ()) {
        queue.async {
            self.handleAction (PersistenceAction.commit (timeout, completionHandler))
        }
    }

/**
     Asyncrhonously call **closure** with this Entity's **item** (thread safe).
     
     **Do not** assign a reference to item outside of the closure. Doing so negates thread safety for item.
     
     **Do not** modify the item's state in closure. Use **update** to modify item's state.

     - parameter closure: The closure to call.
 */
    public func async (closure: @escaping (T) -> Void) {
        queue.async () {
            closure (self.item)
        }
    }


/**
     Call **closure** with this Entity's **item** waiting for the computation to complete (thread safe).
     
     **Do not** assign a reference to item outside of the closure. Doing so negates thread safety for item.
     
     **Do not** modify the item's state in closure. Use **update** to modify item's state.
     
     - parameter closure: The closure to call.
*/
    public func sync (closure: (T) -> Void) {
        queue.sync () {
            closure (self.item)
        }
    }
    
/**
     Apply a **closure** which modifies the state of this Entity's item (thread safe) and add Entity to **batch**. The
     update is applied to **item** immediately and saved to persistent media when the **batch** is committed.
     
     - parameter batch: The batch which this entity is added.
     
     - parameter closure: The closure to call.
*/
    public func update (batch: EventuallyConsistentBatch, closure: @escaping (inout T) -> Void) {
        // Convention is Entity.queue -> Batch.queue
        queue.sync {
            isInsertingToBatch = true
            batch.insertSync (entity: self) {
                self.handleAction(.updateItem (closure))
            }
            isInsertingToBatch = false
        }
    }

    // Not Thread Safe
    internal func setDirty (batch: EventuallyConsistentBatch) {
        if isInsertingToBatch {
            self.handleAction(.setDirty)
        } else {
            batch.insertSync(entity: self) {
                self.handleAction(.setDirty)
            }
        }
    }
    
// Removal
    
/**
     Remove this item from persistent media when the **batch** is committed.
 
     Application developers may safely dereference an Entity after calling **remove().**
     Modifying an Entity on which **remove()** has been called will reanimate it and the item with modifications will be saved
     to persistent media.
 
     **Note:** It is the application's responsibility to ensure that no other Items (in memory or persistent storage)
    contain persistent references to the removed Entity; removing an Entity to which there are references
    will produce errors (i.e. RetrievalResult.error) when those references are accessed.
     
     - parameter batch: The batch to which this Entity is added.
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
    
/**
     Obtain the promise from a referenced entity contained in the item
*/

    public func referenceFromItem<R> (closure: (T) -> ReferenceManager<T, R>) -> Promise<Entity<R>?> {
        var result: Promise<Entity<R>?>? = nil
        queue.sync {
            result = closure (self.item).get()
        }
        return result!
    }
    
/**
     Obtain a promise from the item
*/
    
    public func promiseFromItem<P> (closure: (T) -> Promise<P>) -> Promise<P> {
        var result: Promise<P>? = nil
        queue.sync {
            result = closure (self.item)
        }
        return result!
    }
    
// Serialization Data
    
    public func referenceData() -> ReferenceManagerData {
        var localVersion = 0
        localVersion = version
        return ReferenceManagerData (databaseId: cache.database.accessor.hashValue, cacheName: cache.name, id: id, version: localVersion)
    }
    
/**
     Unloads the referenced entities for all ReferenceManagers contained in **item** and also makes the ReferenceManagers
     unusable for further application processing. This **will not** interfere with asynchronous batch processing

    Call just before the last reference to this Enity goes out of scope (or the only reachable reference is
    set to nil) if it is possible that a ReferenceManager owned by `item' contains a loaded
    reference back to this Entity (which would create a strong reference cycle).
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
    
/**
     Unloads the referenced entities for all ReferenceManagers contained in **item** and also makes the ReferenceManagers
     unusable for further application processing and then recursively does the same for each Entity loaded in a Reference Manager
     until breakReferenceRecursive() has been called on all loaded Entities reachable from this Entity. This **will not**
     interfere with asynchronous batch processing.
     
     Call just before the last reference to this Enity goes out of scope (or the only reachable reference is
     set to nil) if it is possible that a ReferenceManager owned by `item' contains a loaded
     reference back to this Entity (which would create a strong reference cycle). This feature will make all reachable Entities
     unusable.
*/
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
    private func commit (successState: PersistenceState, failureState: PersistenceState, timeout: DispatchTimeInterval, completionHandler: @escaping (DatabaseUpdateResult) -> (), databaseActionSource: (DatabaseAccessor) -> (DispatchQueue, EntityPersistenceWrapper, DispatchTimeInterval) -> DatabaseActionResult) {
        _persistenceState = successState
        _version = _version + 1
        let wrapper = EntityPersistenceWrapper (cacheName: cache.name, entity: self)
        let actionSource = databaseActionSource (cache.database.accessor)
        let actionResult = actionSource (cache.database.workQueue, wrapper, timeout)
        var newItemData: Data? = nil
        do {
            newItemData = try Database.encoder.encode(item)
        } catch {}
        switch actionResult {
        case .ok (let action):
            _persistenceState = .saving
            firstly {
                action()
            }.done (on: cache.database.workQueue) { result in
                self.timeoutTestingHook()
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
            }.catch { error in
                self._persistenceState = failureState
                self._version = self._version - 1
                self.callCommitCompletionHandler (completionHandler: completionHandler, result: .error("\(error)"))
            }
        case .error (let errorMessage):
            self._version = self._version - 1
            _persistenceState = failureState
            self.callCommitCompletionHandler (completionHandler: completionHandler, result: .unrecoverableError(errorMessage))
        }
    }
    
    private func callCommitCompletionHandler (completionHandler: @escaping (DatabaseUpdateResult) -> (), result: DatabaseUpdateResult) {
        cache.database.workQueue.async {
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
        let cache = decoder.userInfo[Database.cacheKey] as? EntityCache<T>
        if let cache = cache {
            let id = try values.decode(UUID.self, forKey: .id)
            if let cachedVersion = cache.cachedEntity(id: id) {
                throw EntityDeserializationError<T>.alreadyCached(cachedVersion)
            }
            self.id = id
            self.cache = cache
            schemaVersion = cache.database.schemaVersion
            let version = try values.decode(Int.self, forKey: .version)
            self._version = version
            if let container = decoder.userInfo[Database.parentDataKey] as? DataContainer {
                container.data = EntityReferenceData (cache: cache, id: id, version: version)
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
                cache.database.logger?.log(level: .error, source: self, featureName: "init(from decoder:)", message: "itemDecodingFailed", data: [(name: "databaseId", value: (cache.database.accessor.hashValue)), (name: "cacheName", value: cache.name), (name: "entityId", value: self.id.uuidString)])
            }
            cache.cacheEntity(self)
        } else {
            throw EntityDeserializationError<T>.NoCollectionInDecoderUserInfo
        }
    }
    
// Initialization
    
    internal func isInitialized (onCollection: EntityCache<T>) -> Bool {
        var result = false
        queue.sync {
            if self.cache === onCollection, cache.database.schemaVersion == self.schemaVersion {
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
    internal let cache: EntityCache<T>
    private var schemaVersion: Int
    private var pendingAction: PendingAction? = nil
    private var onDatabaseUpdateStates: PersistenceStatePair? = nil
    private var isInsertingToBatch = false
    private var references: [ReferenceManagerContainer] = []
    private var referencesQueue: DispatchQueue
    private var hasDereferenced = false

}

