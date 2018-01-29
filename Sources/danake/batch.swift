//
//  batch.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

class Batch {
    
    init() {
        id = UUID()
        queue = DispatchQueue (label: id.uuidString)
        items = Dictionary()
    }
    
    public func insertAsync (item: EntityManagement, closure: (() -> Void)?) {
        queue.async {
            self.items[item.getId()] = (item.getVersion(), item)
            if let closure = closure {
                closure()
            }
        }
    }

    public func insertSync (item: EntityManagement, closure: (() -> Void)?) {
        queue.sync {
            self.items[item.getId()] = (item.getVersion(), item)
            if let closure = closure {
                closure()
            }
        }
    }

    public func syncItems (closure: (Dictionary<UUID, (version: Int, item: EntityManagement)>) -> Void) {
        queue.sync () {
            closure (self.items)
        }
    }

    public let id: UUID
    private let queue: DispatchQueue
    private var items: Dictionary<UUID, (version: Int, item: EntityManagement)>
    
}


