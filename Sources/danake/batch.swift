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
    
    public func insert (item: EntityManagement) {
        queue.async {
            self.items[item.getId()] = (item.getVersion(), item)
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


