//
//  batch.swift
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

public class BatchDefaults {
    public static let retryInterval: DispatchTimeInterval = .seconds (60)
    public static let timeout: DispatchTimeInterval = .seconds (300)
}

/*
    Accumulates Entities whose state is different than their state in persistent memory. The framework will ensure these
    Entities are committed to persistent memory when the batch object is fully dereferenced. Alternatively, application
    developers may manually push these Entities to persistent storage using .commit. The batch object remains fully
    usable as a "new" empty batch after calling .commit.
 
    In the usual case, all of the Entities in the batch will be saved to persistent storage more or less simultaneously.
    However, there are edge cases (e.g. database errors or an Entity present in more than batch) which may result in
    a significant time difference between when various Entities in the batch actually make it to persistent storage.
 
    In the event of recoverable persistent media errors (recoverable as defined by the DatabaseAccessor author), the
    framework will try to save the Entity until it succeeds or the process ends. Errors which occur while serializing
    an Entity are treated as unrecoverable (the framework will not retry after an unrecoverable error). Provide a logger
    when creating a batch to learn of errors which occur while committing that batch.
 
    ** retryInterval: ** How long the framework waits until retrying a batch which experienced recoverable errors
                         during processing.
    ** batchTimeout: **  How long the framework waits for a batch to complete processing. The timeout for individual
                         save operations on Entities is set to 80% of ** batchTimeout **. Note that the database bindings
                         used by an implementation of DatabaseAccessor may have their own timeouts which will govern
                         if they are shorter than those used in the batch.
*/
public class EventuallyConsistentBatch {
    
    convenience init() {
        self.init (retryInterval: BatchDefaults.retryInterval, batchTimeout: BatchDefaults.timeout, logger: nil)
    }
    
    convenience init(logger: Logger) {
        self.init (retryInterval: BatchDefaults.retryInterval, batchTimeout: BatchDefaults.timeout, logger: logger)
    }
    
    init(retryInterval: DispatchTimeInterval, batchTimeout: DispatchTimeInterval, logger: Logger?) {
        self.retryInterval = retryInterval
        self.batchTimeout = batchTimeout
        delegate = BatchDelegate(logger: logger)
        self.logger = logger
        queue = DispatchQueue (label: "EventuallyConsistentBatch \(delegate.id.uuidString)")
    }

    // TODO denit calls commit
    
    public func commit () {
        commit (completionHandler: nil)
    }
    
    public func commit (completionHandler: (() -> ())?) {
        queue.sync {
            let oldDelegate = delegate
            delegate = BatchDelegate(logger: logger)
            commit (delegate: oldDelegate, completionHandler: completionHandler)
        }
    }
    
    private func commit (delegate: BatchDelegate, completionHandler: (() -> ())?) {
        delegate.commit (retryInterval: retryInterval, batchTimeout: batchTimeout, completionHandler: completionHandler)
    }

    internal func insertAsync (item: EntityManagement, closure: (() -> Void)?) {
        queue.async {
            self.delegate.items[item.getId()] = item
            if let closure = closure {
                closure()
            }
        }
    }
    
    internal func insertSync (item: EntityManagement, closure: (() -> Void)?) {
        queue.sync {
            self.delegate.items[item.getId()] = item
            if let closure = closure {
                closure()
            }
        }
    }
    
    internal func syncItems (closure: (Dictionary<UUID, EntityManagement>) -> Void) {
        queue.sync () {
            closure (self.delegate.items)
        }
    }
    
    private let queue: DispatchQueue
    private var delegate: BatchDelegate
    private let logger: Logger?
    public let retryInterval: DispatchTimeInterval
    public let batchTimeout: DispatchTimeInterval
    
}

fileprivate class BatchDelegate {
    
    init(logger: Logger?) {
        self.logger = logger
        id = UUID()
        items = Dictionary()
        queue = DispatchQueue (label: "BatchDelegate \(id.uuidString)")
    }
    
    fileprivate func commit (retryInterval: DispatchTimeInterval, batchTimeout: DispatchTimeInterval, completionHandler: (() -> ())?) {
        DispatchQueue.main.asyncAfter (deadline: DispatchTime.now()) {
            self.commitImplementation(retryInterval: retryInterval, batchTimeout: batchTimeout, completionHandler: completionHandler)
        }
    }
    
    private func commitImplementation (retryInterval: DispatchTimeInterval, batchTimeout: DispatchTimeInterval, completionHandler: (() -> ())?) {
        let group = DispatchGroup()
        queue.sync {
            for key in items.keys {
                group.enter()
                if let entity = items[key] {
                    entity.commit() { result in
                        var logLevel: LogLevel? = nil
                        var errorMessage: String = ""
                        switch result {
                        case .ok:
                            self.queue.sync {
                                let _ = self.items.removeValue(forKey: entity.getId())
                            }
                        case .unrecoverableError(let message):
                            self.queue.sync {
                                let _ = self.items.removeValue(forKey: entity.getId())
                            }
                            logLevel = .error
                            errorMessage = message
                        case .error(let message):
                            logLevel = .emergency
                            errorMessage = message
                        }
                        if let logLevel = logLevel {
                            self.logger?.log(level: logLevel, source: self, featureName: "commitImplementation", message: "Database.\(result)", data: [(name: "entityType", value: "\(type (of: entity))"), (name: "entityId", value: entity.getId().uuidString), (name: "batchId", value: self.id.uuidString), (name:"errorMessage", value: "\(errorMessage)")])
                        }
                        group.leave()
                    }
                }
            }
        }
        switch group.wait(timeout: DispatchTime.now() + batchTimeout) {
        case .success:
            break
        default:
            self.logger?.log(level: .warning, source: self, featureName: "commitImplementation", message: "batchTimeout", data: [(name: "batchId", value: self.id.uuidString)])
        }
        var isEmpty = false
        queue.sync {
            isEmpty = items.isEmpty
        }
        if !isEmpty {
            DispatchQueue.main.asyncAfter (deadline: DispatchTime.now() + retryInterval) {
                self.commitImplementation(retryInterval: retryInterval, batchTimeout: batchTimeout, completionHandler: completionHandler)
            }
        } else if let completionHandler = completionHandler {
            completionHandler()
        }
    }
    
    
    private let logger: Logger?
    private let queue: DispatchQueue
    fileprivate let id: UUID
    fileprivate var items: Dictionary<UUID, EntityManagement>
}


