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
    
}

class Entity<T: Codable> : EntityManagement {
    
    init (id: UUID, version: Int, item: T) {
        self.id = id
        self.version = version
        self.item = item
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
        batch.insert(item: self)
        queue.async () {
            closure (&self.item)
        }
    }
    
    public func sync (batch: Batch, closure: (inout T) -> Void) {
        batch.insert(item: self)
        queue.sync () {
            closure (&self.item)
        }
    }

// Attributes
    
    public let id: UUID
    public private(set) var version: Int
    private var item: T
    private let queue: DispatchQueue
    
}
