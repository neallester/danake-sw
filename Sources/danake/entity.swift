//
//  entity.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

protocol EntityManagement {
    
    func getId() -> UUID
    func getVersion() -> Int
    func incrementVersion() -> Void
    func getPersistenceState() -> PersistenceState
    
}

enum PersistenceState {
    
    case new
    case dirty
    case persistent
    
}

public class Entity<T: Codable> : EntityManagement, Codable {
    
    init (collection: PersistentCollection<T>, id: UUID, version: Int, item: T) {
        self.collection = collection
        self.id = id
        self.version = version
        self.item = item
        persistenceState = .new
        self.queue = DispatchQueue (label: id.uuidString)
    }

// EntityManagement
    
    func getVersion() -> Int {
        return self.version
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
            self.persistenceState = .dirty
            batch.insertAsync(item: self) {
                closure (&self.item)
            }
        }
    }
    
    public func sync (batch: Batch, closure: @escaping (inout T) -> Void) {
        queue.sync {
            self.persistenceState = .dirty
            batch.insertSync (item: self) {
                closure (&self.item)
            }
        }
    }
    
// Codable
    
    enum CodingKeys: String, CodingKey {
        case id
        case version
        case item
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try queue.sync {
            try container.encode (id, forKey: .id)
            try container.encode (version, forKey: .version)
            try container.encode(item, forKey: .item)
        }
    }
    
    public required init (from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        id = try UUID (uuidString: values.decode(String.self, forKey: .id))!
        version = try values.decode(Int.self, forKey: .version)
        item = try values.decode (T.self, forKey: .item)
        persistenceState = .persistent
        self.queue = DispatchQueue (label: id.uuidString)
    }
    
// deiniitalize
    
    deinit {
        collection?.remove(id: id)
    }
    
// Setters
    
    func setCollection (_ collection: PersistentCollection<T>) {
        self.collection = collection
    }
    
// Attributes
    
    public let id: UUID
    public private(set) var version: Int
    private var item: T
    private let queue: DispatchQueue
    private var persistenceState: PersistenceState
    internal private(set) var collection: PersistentCollection<T>?
    
}

