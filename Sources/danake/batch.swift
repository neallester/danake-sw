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

internal extension DispatchTimeInterval {

    func multipliedBy (_ multiplier: Int) -> DispatchTimeInterval {
        switch self {
        case .seconds(let value):
            return .seconds (value * multiplier)
        case .milliseconds(let value):
            return .milliseconds(value * multiplier)
        case .microseconds(let value):
            return .microseconds (value * multiplier)
        case .nanoseconds(let value):
            return .nanoseconds (value * multiplier)
        case .never:
            return .never
        }
    }
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
    ** timeout: **       The timeout for individual commit operations on Entities. The timeout for the entire batch
                         is 2x ** timeout **. That is, if all individual commit operations on Entities within the batch
                         do not complete within 2x timeout the batch will timeout. This could occur if access to
                         Entity.queue is blocked or if Entity serialization causes an endless loop. Note that the
                         database bindings used by an implementation of DatabaseAccessor may have their own timeouts
                         which will govern if they are shorter than those used in the batch.
*/
public class EventuallyConsistentBatch {
    
    convenience init() {
        self.init (retryInterval: BatchDefaults.retryInterval, timeout: BatchDefaults.timeout, logger: nil)
    }
    
    convenience init(logger: Logger) {
        self.init (retryInterval: BatchDefaults.retryInterval, timeout: BatchDefaults.timeout, logger: logger)
    }
    
    init(retryInterval: DispatchTimeInterval, timeout: DispatchTimeInterval, logger: Logger?) {
        self.retryInterval = retryInterval
        self.timeout = timeout
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
        delegate.commit (retryInterval: retryInterval, timeout: timeout, completionHandler: completionHandler)
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
    
    internal func delegateId() -> UUID {
        var result: UUID? = nil
        queue.sync() {
            result = delegate.id
        }
        return result!
    }
    
    private let queue: DispatchQueue
    private var delegate: BatchDelegate
    private let logger: Logger?
    public let retryInterval: DispatchTimeInterval
    public let timeout: DispatchTimeInterval
    
}

fileprivate class BatchDelegate {
    
    init(logger: Logger?) {
        self.logger = logger
        id = UUID()
        items = Dictionary()
        queue = DispatchQueue (label: "BatchDelegate \(id.uuidString)")
    }
    
    public func commit (retryInterval: DispatchTimeInterval, timeout: DispatchTimeInterval, completionHandler: (() -> ())?) {
        commit (delay: .nanoseconds(0), retryInterval: retryInterval, timeout: timeout, completionHandler: completionHandler)
    }
    
    fileprivate func commit (delay: DispatchTimeInterval, retryInterval: DispatchTimeInterval, timeout: DispatchTimeInterval, completionHandler: (() -> ())?) {
        let dispatchQueue = DispatchQueue (label: "EventuallyConsistentBatch.dispatchQueue \(id.uuidString)")
        dispatchQueue.asyncAfter (deadline: DispatchTime.now() + delay) {
            let group = DispatchGroup()
            self.queue.sync {
                for key in self.items.keys {
                    group.enter()
                    if let entity = self.items[key] {
                        entity.commit (timeout: timeout) { result in
                            var logLevel: LogLevel? = nil
                            switch result {
                            case .ok:
                                self.queue.sync {
                                    let _ = self.items.removeValue(forKey: entity.getId())
                                }
                            case .unrecoverableError(_):
                                self.queue.sync {
                                    let _ = self.items.removeValue(forKey: entity.getId())
                                }
                                logLevel = .error
                            case .error(_):
                                logLevel = .emergency
                            }
                            if let logLevel = logLevel {
                                self.logger?.log(level: logLevel, source: self, featureName: "commit", message: "Database.\(result)", data: [(name: "entityType", value: "\(type (of: entity))"), (name: "entityId", value: entity.getId().uuidString), (name: "batchId", value: self.id.uuidString)])
                            }
                            
                            
                            group.leave()
                        }
                    }
                }
            }
            switch group.wait(timeout: DispatchTime.now() + timeout.multipliedBy(2)) {
            case .success:
                break
            default:
                self.queue.async {
                    for entity in self.items.values {
                        self.logger?.log(level: .error, source: self, featureName: "commit", message: "batchTimeout", data: [(name: "batchId", value: self.id.uuidString), (name: "entityType", value: "\(type (of: entity))"), (name: "entityId", value: entity.getId().uuidString), (name: "diagnosticHint", value: "Entity.queue is blocked or endless loop in Entity serialization")])
                    }
                    self.items.removeAll()
                }
            }
            var isEmpty = false
            self.queue.sync {
                isEmpty = self.items.isEmpty
            }
            if !isEmpty {
                self.commit (delay: retryInterval, retryInterval: retryInterval, timeout: timeout, completionHandler: completionHandler)
            } else if let completionHandler = completionHandler {
                completionHandler()
            }
        }
    }
    
    private let logger: Logger?
    private let queue: DispatchQueue
    fileprivate let id: UUID
    fileprivate var items: Dictionary<UUID, EntityManagement>
}


