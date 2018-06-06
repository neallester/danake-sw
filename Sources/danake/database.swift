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


public enum RetrievalResult<T> {
    
    case ok (T?)
    case error (String)
    
    func item() -> T? {
        switch self {
        case .ok(let item):
            return item
        default:
            return nil
        }
    }
    
    func isOk() -> Bool {
        switch self {
        case .ok:
            return true
        default:
            return false
        }
    }
    
}

public enum EntityConversionResult<T> {
    
    case ok (T)
    case error (String)

}

public enum DatabaseAccessListResult<T> {
    
    case ok ([T])
    case error (String)
    
}

public enum DatabaseUpdateResult {
    
    case ok
    case error (String)
    case unrecoverableError (String)
    
}

public enum DatabaseActionResult {
    case ok (() -> DatabaseUpdateResult)
    case error (String)
}

open class Database {
    
/*
     Only one instance of the Database object associated with any particular persistent storage media (database) may be present in system.
     Declare Database objects as let constants  within a scope with process lifetime, re-creating the Database object associated with
     an attribute is not currently supported.
     
     The current ** schemaVersion ** is stored as metadata with every Entity when it is stored in the database. The value of ** schemaVersion **
     should be incremented whenever an Decodable incompatible change is made to any Entity.item stored in the database. That is, increment
     ** schemaVersion ** if an Entity.item (anywhere in the database) is no longer capable of decoding data stored under a previous
     ** schemaVersion **. The ** schemaVersion ** at the time an Entity.item was stored is available to the model object's deserialization
     routine via the EntityReferenceData stored in userInfo[Database.parentKeyData] (see Entity.init (from decoder:)
*/
    init (accessor: DatabaseAccessor, schemaVersion: Int, logger: Logger? = nil, referenceRetryInterval: TimeInterval = 120.0) {
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
    
    public let accessor: DatabaseAccessor
    public let logger: Logger?
    public let schemaVersion: Int
    public let workQueue: DispatchQueue
    public let referenceRetryInterval: TimeInterval
    private let hashValue: String
    let collectionRegistrar = Registrar<CacheName, AnyObject>()
    
    deinit {
        Database.registrar.deRegister(key: hashValue)
    }
    
    internal static func qualifiedCacheName (databaseHash: String,  cacheName: CacheName) -> QualifiedCacheName {
        return "\(databaseHash).\(cacheName)"
    }

    static let registrar = Registrar<String, Database>()
    static let collectionRegistrar = Registrar<QualifiedCacheName, UntypedEntityCache>()
    public static let collectionKey = CodingUserInfoKey (rawValue: "collection")!
    public static let parentDataKey = CodingUserInfoKey (rawValue: "parentData")!
    internal static let encoder: JSONEncoder = {
        let result = JSONEncoder()
        result.dateEncodingStrategy = .millisecondsSince1970
        return result
    }()
}

public protocol DatabaseAccessor {
    
    func get<T> (type: Entity<T>.Type, collection: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>>
    func scan<T> (type: Entity<T>.Type, collection: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>>
    
    /*
     Is the format of ** name ** a valid CacheName in this storage medium and,
     is ** name ** NOT a reserved word in this storage medium?
     */
    func isValidCacheName (_ name: CacheName) -> ValidationResult
    
    /*
     A unique identifier for the specific instance of the database being accessed
     Ideally this should be stored in and retrieved from the storage medium
     */
    var hashValue: String { get }
    
    /*
     The following ** DatabaseActionResult ** functions should return fast: The actual database
     access should occur when the returned closure is fired
     
     */
    func addAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult
    func updateAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult
    func removeAction (wrapper: EntityPersistenceWrapper) -> DatabaseActionResult
    
}

public class EntityCreation {
    
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
