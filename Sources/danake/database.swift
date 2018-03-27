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

struct WeakItem<T: Codable> {
    
    init (item: Entity<T>) {
        self.item = item
    }
    
    weak var item: Entity<T>?
}

struct WeakObject<T: AnyObject> {
    
    init (item: T) {
        self.item = item
    }
    
    weak var item: T?
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
            if let storedValue = items[key]?.item, storedValue !== value {
                // Do nothing
            } else {
                items[key] = WeakObject (item: value)
                result = true
            }
            
        }
        return result
    }
    
    // Only permitted if item associated with key is nil
    func deRegister (key: K) {
        queue.async {
            if let _ = self.items[key]?.item{
                // Do nothing
            } else {
                let _ = self.items.removeValue(forKey: key)
            }
        }
    }
    
    func isRegistered (key: K) -> Bool {
        var result = false
        queue.sync {
            if let _ = items[key]?.item {
                result = true
            }
            
        }
        return result
    }
    
    func value (key: K) -> V? {
        var result: V? = nil
        queue.sync {
            result = items[key]?.item
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

public class Database {
    
    /*
     The current ** schemaVersion ** is stored as metadata with every Entity when it is stored in the database. The value of ** schemaVersion **
     should be incremented whenever an Decodable incompatible change is made to any Entity.item stored in the database. That is, increment
     ** schemaVersion ** if an Entity.item (anywhere in the database) is no longer capable of decoding data stored under a previous
     ** schemaVersion **. The intention is that the ** schemaVersion ** at the time an Entity.item was stored will be made available to the
     code performing an Entity.item decode operation where it would facilitate decoding legacy JSON to the existing object structure. However,
     a mechanism for actually doing this is currently not present in the framework.
     */
    init (accessor: DatabaseAccessor, schemaVersion: Int, logger: Logger?) {
        self.accessor = accessor
        self.logger = logger
        self.hashValue = accessor.hashValue()
        self.schemaVersion = schemaVersion
        workQueue = DispatchQueue (label: "workQueue Database \(hashValue)", attributes: .concurrent)
        if Database.registrar.register(key: hashValue, value: self) {
            logger?.log(level: .info, source: self, featureName: "init", message: "created", data: [(name:"hashValue", hashValue)])
        } else {
            logger?.log(level: .emergency, source: self, featureName: "init", message: "registrationFailed", data: [(name:"hashValue", hashValue)])
        }
    }
    
    func getAccessor() -> DatabaseAccessor {
        return accessor
    }
    
    public let accessor: DatabaseAccessor
    public let logger: Logger?
    public let schemaVersion: Int
    public let workQueue: DispatchQueue
    private let hashValue: String
    let collectionRegistrar = Registrar<CollectionName, AnyObject>()
    
    deinit {
        Database.registrar.deRegister(key: hashValue)
    }
    
    static let registrar = Registrar<String, Database>()
    public static let collectionKey = CodingUserInfoKey (rawValue: "collection")!
    public static let parentDataKey = CodingUserInfoKey (rawValue: "parentData")!
    
}

public protocol DatabaseAccessor {
    
    func get<T> (type: Entity<T>.Type, collection: PersistentCollection<Database, T>, id: UUID) -> RetrievalResult<Entity<T>>
    func scan<T> (type: Entity<T>.Type, collection: PersistentCollection<Database, T>) -> DatabaseAccessListResult<Entity<T>>
    
    /*
     Is the format of ** name ** a valid CollectionName in this storage medium and,
     is ** name ** NOT a reserved word in this storage medium?
     */
    func isValidCollectionName (name: CollectionName) -> ValidationResult
    
    /*
     A unique identifier for the specific instance of the database being accessed
     Ideally this should be stored in and retrieved from the storage medium
     */
    func hashValue() -> String
    
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
