//
//  database.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 3/10/18.
//

import Foundation
import PromiseKit

public enum ValidationResult {
    
    case ok
    case error (String)
    
    func isOk() -> Bool {
        switch self {
        case .ok:
            return true
        default:
            return false
        }
    }
    
    func description() -> String {
        switch self {
        case .ok:
            return "ok"
        case .error (let description):
            return description
        }
    }
}

struct WeakCodable<T: Codable> {
    
    init (_ codable: Entity<T>) {
        self.codable = codable
    }
    
    weak var codable: Entity<T>?
}

struct WeakObject<T: AnyObject> {
    
    init (_ object: T) {
        self.object = object
    }
    
    weak var object: T?
}

/*
    Registers objects against a key
 */
class Registrar<K: Hashable, V: AnyObject> {
    
    init() {
        queue = DispatchQueue (label: "\(type (of: self)): \(UUID().uuidString)")
    }
    
    // Returns true if registration succeeded
    // Fails if there is already an object registered under this key
    func register (key: K, value: V) -> Bool {
        var result = false
        queue.sync {
            if let storedValue = items[key]?.object, storedValue !== value {
                // Do nothing
            } else {
                items[key] = WeakObject (value)
                result = true
            }
            
        }
        return result
    }
    
    // Only permitted if item associated with key is nil
    func deRegister (key: K) {
        queue.async {
            if let _ = self.items[key]?.object{
                // Do nothing
            } else {
                let _ = self.items.removeValue(forKey: key)
            }
        }
    }
    
    func isRegistered (key: K) -> Bool {
        var result = false
        queue.sync {
            if let _ = items[key]?.object {
                result = true
            }
            
        }
        return result
    }
    
    func value (key: K) -> V? {
        var result: V? = nil
        queue.sync {
            result = items[key]?.object
        }
        return result
    }
    
    func count() -> Int {
        var result = 0
        queue.sync {
            result = items.count
        }
        return result
    }
    
    let queue: DispatchQueue
    var items = Dictionary<K, WeakObject<V>>()
    
    // For Testing only
    internal func clear() {
        queue.sync() {
            items = Dictionary<K, WeakObject<V>>()
        }
    }
    
}

/**
    The types of database actions supported; used for error reporting
 */
public enum DatabaseAction {
    case add
    case update
    case remove
}

/**
    The results of an attempt to update the persistent storage.
*/
public enum DatabaseUpdateResult {
    
    /// The update succeeded
    case ok
    
    /// A recoverable error occurred (Danake will retry). This typically indicates a transient
    /// problem with the persistent storage.
    case error (String)
    
    /// An unrecoverable error occurred (Danake will not retry; data was lost). Errors which occur during
    /// Entity serialization are considered unrecoverable. Additionally, implementers may designate specific
    /// errors as unrecoverable.
    case unrecoverableError (String)
    
}

/**
    The results of an attempt to build a closure which will perform an update on the persistent media.
*/
public enum DatabaseActionResult {
    
    /// Building the closure succeeded. The associated value is the closure which will actually perform the action
    case ok (() -> Promise<DatabaseUpdateResult>)
    
    /// An error occurred; the associated value is the error message.
    case error (String)
}

public enum EntityConversionResult<T> {
    
    case ok (T)
    case error (String)
    
}

/**
    Class representing a specific persistent media. Only one instance of the Database object associated with any
    particular persistent storage media (database) may be present in system. Declare Database objects as let constants
    within a scope with process lifetime, re-creating the Database object associated with an attribute is not currently
    supported.
 */
open class Database {
    
/**
     - parameter accessor: A delegate implementing access to the persistent media.
     - parameter schemaVersion: The **schemaVersion** is stored as metadata with every Entity when it is stored in the database.
                                The value of **schemaVersion** should be incremented whenever an Decodable incompatible change
                                is made to any Entity.item stored in the database. That is, increment **schemaVersion** if
                                an Entity.item (anywhere in the database) is no longer capable of decoding data stored under
                                a previous **schemaVersion**. The **schemaVersion** at the time an Entity.item was stored is
                                available to the model object's deserialization routine via the EntityReferenceData stored
                                in userInfo[Database.parentKeyData] (see Entity.init (from decoder:)
     - parameter logger:        The logger used to report issues with the persistent system (default = **nil** but providing
                                a logger is strongly recommended both during development and in production).
     - parameter referenceRetryInterval: The TimeInterval after which a ReferenceManager which has returned an error will retry an
                                         attempt to retrieve from persistent media. Before the **referenceRetryInterval** elapses
                                         subsequent attempts to retrieve from a ReferenceManager will return the same error without
                                         hitting the persistent media.
*/
    public init (accessor: DatabaseAccessor, schemaVersion: Int, logger: Logger? = nil, referenceRetryInterval: TimeInterval = 120.0) {
        self.referenceRetryInterval = referenceRetryInterval
        self.accessor = accessor
        self.logger = logger
        self.hashValue = accessor.hashValue
        self.schemaVersion = schemaVersion
        workQueue = DispatchQueue (label: "workQueue Database \(hashValue)", attributes: .concurrent)
        PromiseKit.conf.Q = (map: workQueue, return: workQueue)
        if let logger = logger {
            PromiseKit.conf.logHandler = { event in
                switch event {
                case .waitOnMainThread:
                    logger.log(level: .error, source: self, featureName: "init", message: "promiseKit.waitOnMainThread", data: nil)
                case .pendingPromiseDeallocated:
                    logger.log(level: .warning, source: self, featureName: "init", message: "promiseKit.pendingPromiseDeallocated", data: nil)
                case .pendingGuaranteeDeallocated:
                    logger.log(level: .warning, source: self, featureName: "init", message: "promiseKit.pendingGuaranteeDeallocated", data: nil)
                case .cauterized(let error):
                    logger.log(level: .debug, source: self, featureName: "init", message: "promiseKit.cauterized", data: [(name: "error", value: "\(error)")])
                }
            }
        } else {
            PromiseKit.conf.logHandler = { event in }
        }
        if Database.registrar.register(key: hashValue, value: self) {
            logger?.log(level: .info, source: self, featureName: "init", message: "created", data: [(name:"hashValue", hashValue)])
        } else {
            logger?.log(level: .emergency, source: self, featureName: "init", message: "registrationFailed", data: [(name:"hashValue", hashValue)])
        }
    }
    
    internal func qualifiedCacheName (_ cacheName: CacheName) -> QualifiedCacheName {
        return Database.qualifiedCacheName(databaseHash: accessor.hashValue, cacheName: cacheName)
    }
    
    /// A delegate implementing access to the persistent media.
    public let accessor: DatabaseAccessor
    
    public let logger: Logger?
    public let schemaVersion: Int
    
    /// A concurrent queue which may be used to schedule asynchronous work.
    public let workQueue: DispatchQueue

/**
     The TimeInterval after which a ReferenceManager which has returned an error will retry an
     attempt to retrieve from persistent media. Before the **referenceRetryInterval** elapses
     subsequent attempts to retrieve from a ReferenceManager will return the same error without
     hitting the persistent media.
 */
    public let referenceRetryInterval: TimeInterval
    private let hashValue: String
    let cacheRegistrar = Registrar<CacheName, AnyObject>()
    
    deinit {
        Database.registrar.deRegister(key: hashValue)
    }
    
    internal static func qualifiedCacheName (databaseHash: String,  cacheName: CacheName) -> QualifiedCacheName {
        return "\(databaseHash).\(cacheName)"
    }

    static let registrar = Registrar<String, Database>()
    static let cacheRegistrar = Registrar<QualifiedCacheName, UntypedEntityCache>()
    public static let cacheKey = CodingUserInfoKey (rawValue: "cache")!
    public static let parentDataKey = CodingUserInfoKey (rawValue: "parentData")!
    internal static let encoder: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .millisecondsSince1970
        return result
    }()
}

public enum AccessorError: Error {
    case unknownUUID (UUID)
    case creation (String)
    case timeout (DispatchTimeInterval, String)
    case updateAction (UUID)
    case addAction (UUID)
    case removeAction (UUID)
}

/**
    A delegate which implements access to a specific persistent media.
*/
public protocol DatabaseAccessor {
    
    func getSync<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) throws -> Entity<T>
    func get<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) -> Promise<Entity<T>>
    func scanSync<T> (type: Entity<T>.Type, cache: EntityCache<T>, criteria: ((T) -> Bool)?) throws -> [Entity<T>]
    func scan<T> (type: Entity<T>.Type, cache: EntityCache<T>, criteria: ((T) -> Bool)?) -> Promise<[Entity<T>]>
    func isSynchronous() -> Bool
    
/**
     - returns: Is the format of **name** a valid CacheName in this storage medium and,
                is **name** NOT a reserved word in this storage medium?
     */
    func isValidCacheName (_ name: CacheName) -> ValidationResult
    
/**
     A unique identifier for the specific instance of the storage media being accessed.
     Ideally this should be stored in and retrieved from the storage medium.
*/
    var hashValue: String { get }
    
/**
     Database updates are performed in 2 stages. In the first (fast) stage all required information is
     extracted from the Entity to be updated and "serialized" with the update instructions in a closure.
     
     In the second stage the closure is fired and the update is performed.
*/
    
/**
    Attempt to create a closure which **adds** an Entity to the persistent medium. This function should
    return fast; the necessary database update instructions should be fully serialized, but
    the actual database access and callback occur when the returned closure is fired.
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter callback: The closure to call to report the results of the attempted **add** action
     
     - returns: A closure which will perform the **add** when called
*/
    func addActionImplementation (wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> ()

/**
     Attempt to create a closure which **updates** an Entity to the persistent medium. This function should
     return fast; the necessary database update instructions should be fully serialized, but
     the actual database access and callback occur when the returned closure is fired.
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter callback: The closure to call to report the results of the attempted **update** action
     
     - returns: A closure which will perform the **update** when called
*/
    
    func updateActionImplementation (wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> ()
    
/**
     Attempt to create a closure which **removes** an Entity to the persistent medium. This function should
     return fast; the necessary database update instructions should be fully serialized, but
     the actual database access and callback occur when the returned closure is fired.
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter callback: The closure to call to report the results of the attempted **remove** action
     
     - returns: A closure which will perform the **remove** when called
*/
    
    func removeActionImplementation (wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> ()
    
}

extension DatabaseAccessor {
    
/**
     Create a DatabaseActionResult necessary to **add** an Entity to the storage
     
     - parameter queue: The **concurrent** dispatch queue used to execute the timeout (if required).
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter timeout: The interval to wait before timing out the **action**
     
     - returns: A DatabaseActionResult with a closure which when fired will return the promise of a DatabaseUpdateResult
                reporting the outcome of the attempted database update
     
*/
    func addAction (queue: DispatchQueue, wrapper: EntityPersistenceWrapper, timeout: DispatchTimeInterval) -> DatabaseActionResult {
        return wrapAction(queue: queue, wrapper: wrapper, action: addActionImplementation, actionType: .add, error: AccessorError.addAction(wrapper.id), timeout: timeout)
    }

/**
     Create a DatabaseActionResult necessary to **update** an Entity to the storage
     
     - parameter queue: The **concurrent** dispatch queue used to execute the timeout (if required).
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter timeout: The interval to wait before timing out the **action**
     
     - returns: A DatabaseActionResult with a closure which when fired will return the promise of a DatabaseUpdateResult
     reporting the outcome of the attempted database update
     
*/

    func updateAction (queue: DispatchQueue, wrapper: EntityPersistenceWrapper, timeout: DispatchTimeInterval) -> DatabaseActionResult {
        return wrapAction(queue: queue, wrapper: wrapper, action: updateActionImplementation, actionType: .update, error: AccessorError.updateAction(wrapper.id), timeout: timeout)
    }
    
/**
     Create a DatabaseActionResult necessary to **remove** an Entity to the storage
     
     - parameter queue: The **concurrent** dispatch queue used to execute the timeout (if required).
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter timeout: The interval to wait before timing out the **action**
     
     - returns: A DatabaseActionResult with a closure which when fired will return the promise of a DatabaseUpdateResult
     reporting the outcome of the attempted database update
     
*/
    func removeAction (queue: DispatchQueue, wrapper: EntityPersistenceWrapper, timeout: DispatchTimeInterval) -> DatabaseActionResult {
        return wrapAction(queue: queue, wrapper: wrapper, action: removeActionImplementation, actionType: .remove, error: AccessorError.removeAction(wrapper.id), timeout: timeout)
    }
    
/**
    Wraps **action** with a timeout.
     
    - parameter queue: The **concurrent** dispatch queue used to execute the timeout (if required).
     
    - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - parameter action: The database update action to be wrapped in a timeout
     
     - parameter actionType: Enum indicating the type of action (for error reporting)
     
     - parameter timeout: The interval to wait before timing out the **action**
     
     - returns: A DatabaseActionResult containing the timeout wrapped closure
*/
    func wrapAction (
        queue: DispatchQueue,
        wrapper: EntityPersistenceWrapper,
        action: ((EntityPersistenceWrapper, @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> ()),
        actionType: DatabaseAction,
        error: AccessorError,
        timeout: DispatchTimeInterval
    ) -> DatabaseActionResult {
        let pending = Promise<DatabaseUpdateResult>.pending()
        let cacheName = wrapper.cacheName
        let id = wrapper.id.uuidString
        let updateTask = DispatchWorkItem {
            pending.resolver.fulfill(.error ("timeout:\(type (of: self)).\(actionType);database=\(self.hashValue);entityCache=\(cacheName);entityID=\(id)"))
        }
        do {
            let updateAction = try action(wrapper) { databaseActionResult in
                updateTask.cancel()
                pending.resolver.fulfill(databaseActionResult)
            }
            let wrappedFunction: () -> Promise<DatabaseUpdateResult> = {
                queue.asyncAfter(deadline: DispatchTime.now() + timeout, execute: updateTask)
                if self.isSynchronous() {
                    queue.async {
                        updateAction()
                    }
                } else {
                    updateAction()
                }
                return pending.promise
            }
            return DatabaseActionResult.ok(wrappedFunction)
        } catch {
            pending.resolver.reject(error)
            return .error ("\(error)")
        }
    }

    
}


public protocol SynchronousAccessor : DatabaseAccessor {
    
    func getImplementation<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) throws -> Entity<T>?
    func scanImplementation<T> (type: Entity<T>.Type, cache: EntityCache<T>) throws -> [Entity<T>]
    
}

extension SynchronousAccessor {
    
    public func getSync<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) throws -> Entity<T> {
        do {
            if let result = try getImplementation(type: type, cache: cache, id: id) {
                return result
            } else {
                throw AccessorError.unknownUUID (id)
            }
        } catch AccessorError.unknownUUID {
            cache.database.logger?.log (level: .warning, source: self, featureName: "getSync",message: "Unknown id", data: [("databaseHashValue", cache.database.accessor.hashValue), (name:"cache", value: cache.name), (name:"id",value: id.uuidString)])
            throw AccessorError.unknownUUID (id)
        } catch {
            cache.database.logger?.log (level: .emergency, source: self, featureName: "getSync",message: "Database Error", data: [("databaseHashValue", cache.database.accessor.hashValue), (name:"cache", value: cache.name), (name:"id",value: id.uuidString), (name: "errorMessage", "\(error)")])
            throw error
        }
    }
    
    public func get<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) -> Promise<Entity<T>> {
        return Promise { seal in
            cache.database.workQueue.async {
                do {
                    try seal.fulfill(self.getSync(type: type, cache: cache, id: id))
                } catch {
                    seal.reject(error)
                }
            }
        }
    }
    
    public func scanSync<T> (type: Entity<T>.Type, cache: EntityCache<T>, criteria: ((T) -> Bool)? = nil) throws -> [Entity<T>] {
        do {
            let result = try scanImplementation (type: type, cache: cache)
            if let criteria = criteria  {
                let criteriaWrapper: ((Entity<T>) -> Bool) = { entity in
                    var result = true
                    entity.sync() { item in
                        result = criteria (item)
                    }
                    return result
                }
                return result.filter (criteriaWrapper)
            } else {
                return result
            }
        } catch {
            cache.database.logger?.log(level: .emergency, source: self, featureName: "scanSync", message: "Database Error", data: [(name: "databaseHashValue", value: cache.database.accessor.hashValue), (name: "cache", value: cache.name), (name: "errorMessage", value: "\(error)")])
            throw error
        }
    }
    
    public func scan<T> (type: Entity<T>.Type, cache: EntityCache<T>, criteria: ((T) -> Bool)? = nil) -> Promise<[Entity<T>]> {
        return Promise { seal in
            cache.database.workQueue.async {
                do {
                    try seal.fulfill(self.scanSync(type: type, cache: cache, criteria: criteria))
                } catch {
                    seal.reject(error)
                }
            }
        }
    }
    
    public func isSynchronous() -> Bool {
        return true
    }
    
}

public protocol AsynchronousAccessor : DatabaseAccessor {}

extension AsynchronousAccessor {
    
    public func getSync<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) throws -> Entity<T> {
        return try get (type: type, cache: cache, id: id).wait()
    }
    
    
    public func scanSync<T> (type: Entity<T>.Type, cache: EntityCache<T>, criteria: ((T) -> Bool)? = nil) throws -> [Entity<T>] {
        return try scan (type: type, cache: cache, criteria: criteria).wait()
    }
    
    public func isSynchronous() -> Bool {
        return false
    }
    
}

public class EntityCreation {
    
    public init () {}
    
    public func entity<T, E: Entity<T>> (creator: () throws -> E) -> EntityConversionResult<Entity<T>> {
        do {
            let result = try creator()
            return .ok (result)
        } catch EntityDeserializationError<T>.alreadyCached(let cachedEntity) {
            return .ok (cachedEntity)
        } catch {
            return .error ("\(error)")
        }
    }
    
}
