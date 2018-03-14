//
//  batch.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

// TODO Rename to EventuallyConsistentBatch
// TODO Items are removed from batch as they process successfully
// TODO If batch is not empty, it dispatches itself in the future with a delay 

public class EventuallyConsistentBatch {
    
    init() {
        id = UUID()
        queue = DispatchQueue (label: id.uuidString)
        items = Dictionary()
    }
    
    func insertAsync (item: EntityManagement, closure: (() -> Void)?) {
        queue.async {
            self.items[item.getId()] = item
            if let closure = closure {
                closure()
            }
        }
    }

    func insertSync (item: EntityManagement, closure: (() -> Void)?) {
        queue.sync {
            self.items[item.getId()] = item
            if let closure = closure {
                closure()
            }
        }
    }

    func syncItems (closure: (Dictionary<UUID, EntityManagement>) -> Void) {
        queue.sync () {
            closure (self.items)
        }
    }

    public let id: UUID
    private let queue: DispatchQueue
    private var items: Dictionary<UUID, EntityManagement>
    
}


