//
//  database.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 3/10/18.
//

import Foundation

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
    The result of an attempt to retrieve a construct or class from persistent storage.
 
    The generic parameter **T** indicates the type of the (possibly) retrieved construct.
*/
public enum RetrievalResult<T> {
    
    /// No Error occurred; the associated value is the item retrieved from persistent storage
    case ok (T?)
    
    /// An error occurred; the associated value is the error message.
    case error (String)
    
/**
     The item (of type **T**) retrieved from persistent storage (if any). A nil value
     indicates that either there was no such construct in storage or an error occurred during
     retrieval.
*/
    public func item() -> T? {
        switch self {
        case .ok(let item):
            return item
        default:
            return nil
        }
    }
    
    /// No Error Occurred
    public func isOk() -> Bool {
        switch self {
        case .ok:
            return true
        default:
            return false
        }
    }
    
}

/**
    The result of an attempt to retrieve a list of constructs from persistent storage.
 
    The generic parameter **T** indicates the type of the (possibly) retrieved constructs.
 */
public enum DatabaseAccessListResult<T> {

    /// No Error occurred; the associated value is the list of items retrieved from persistent storage
    case ok ([T])
    
    /// An error occurred; the associated value is the error message.
    case error (String)
    
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
    case ok (() -> DatabaseUpdateResult)
    
    /// An error occurred; the associated value is the error message.
    case error (String)
}

internal enum EntityConversionResult<T> {
    
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

/**
    A delegate which implements access to a specific persistent media.
*/
public protocol DatabaseAccessor {
    
    func get<T> (type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>>
    func scan<T> (type: Entity<T>.Type, cache: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>>
    
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
    Attempt to create a closure which **adds** an Entity to the persistent medium. This function should
    return fast; the actual database access should occur when the returned closure is fired.
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - returns: A DatabaseActionResult with the closure (or error message).
*/
    func addAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult

/**
     Attempt to create a closure which **updatess** an existing Entity in the persistent medium. This function
    should return fast; the actual database access should occur when the returned closure is fired.
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - returns: A DatabaseActionResult with the closure (or error message).
*/
    func updateAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult
    
/**
     Attempt to create a closure which **removes** an existing Entity in the persistent medium. This function
     should return fast; the actual database access should occur when the returned closure is fired.
     
     - parameter wrapper: A type erased wrapper "containing" the Entity to be added
     
     - returns: A DatabaseActionResult with the closure (or error message).
*/
    func removeAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult
    
}

internal class EntityCreation {
    
    func entity<T, E: Entity<T>> (creator: () throws -> E) -> EntityConversionResult<Entity<T>> {
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
