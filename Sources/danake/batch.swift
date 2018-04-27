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
    Entities are committed to persistent memory when the batch is pushed to persistent storage using .commit(). The batch
    object remains fully usable as a "new" empty batch after calling .commit. If a batch is dereferenced without a .commit()
    all pending changes are lost and logged as errors if a logger has been provided.
 
    In the usual case, all of the Entities in the batch will be saved to persistent storage more or less simultaneously.
    However, there are edge cases (e.g. database errors or an Entity present in more than batch) which may result in
    a significant time difference between when various Entities in the batch actually make it to persistent storage.
 
    In the event of recoverable persistent media errors (recoverable as defined by the DatabaseAccessor author), the
    framework will try to save the Entity until it succeeds or the process ends. Errors which occur while serializing
    an Entity are treated as unrecoverable (the framework will not retry after an unrecoverable error). Provide a logger
    when creating a batch to learn of errors which occur while committing that batch.
 
    ** retryInterval: ** How long the framework waits until retrying a batch which experienced recoverable errors
                         during processing.
    ** timeout: **       The timeout for individual commit operations on Entities. Note that the database bindings used by
                         an implementation of DatabaseAccessor may have their own timeouts which will govern if they are
                         shorter than those specified here. All database operations are protected by this timeout, but some
                         operations which occur during the commit process are not. Thus, if the Entity.queue is blocked or
                         if Entity serialization causes an endless loop the individual Entity commit timeout will not
                         fire. For these scenarios a timeout for the entire batch of 2x ** timeout ** is used. That is,
                         if all individual commit operations on Entities within the batch do not complete within 2x timeout
                         the batch will timeout. If the batch times out, any pending updates remaining in the batch are lost
                         and logged if a logger has been provided. Successful database updates for other entities in the
                         batch are NOT rolled back.
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

    deinit {
        if let logger = logger {
            delegate.sync() { entities in
                for entity in entities.values {
                    logger.log(level: .error, source: delegate, featureName: "deinit", message: "notCommitted:lostData", data: [(name: "entityType", value: "\(type(of: entity))"), (name: "entityId", value: entity.id), (name: "entityPersistenceState", value: "\(entity.getPersistenceState())")])
                }
            }
        }
    }
    
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
    
    // Waits until batch processing has completed
    public func commitSync() {
        let group = DispatchGroup()
        group.enter()
        commit() {
            group.leave()
        }
        group.wait()
    }
    
    private func commit (delegate: BatchDelegate, completionHandler: (() -> ())?) {
        delegate.commit (retryInterval: retryInterval, timeout: timeout, completionHandler: completionHandler)
    }

    internal func insertAsync (entity: EntityManagement, closure: (() -> Void)?) {
        queue.async {
            self.delegate.entities[entity.id] = entity
            if let closure = closure {
                closure()
            }
        }
    }
    
    internal func insertSync (entity: EntityManagement, closure: (() -> Void)?) {
        queue.sync {
            self.delegate.entities[entity.id] = entity
            if let closure = closure {
                closure()
            }
        }
    }
    
    internal func syncEntities (closure: (Dictionary<UUID, EntityManagement>) -> Void) {
        queue.sync () {
            closure (self.delegate.entities)
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
        entities = Dictionary()
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
                for key in self.entities.keys {
                    group.enter()
                    if let entity = self.entities[key] {
                        entity.commit (timeout: timeout) { result in
                            var logLevel: LogLevel? = nil
                            switch result {
                            case .ok:
                                self.queue.sync {
                                    let _ = self.entities.removeValue(forKey: entity.id)
                                }
                            case .unrecoverableError(_):
                                self.queue.sync {
                                    let _ = self.entities.removeValue(forKey: entity.id)
                                }
                                logLevel = .error
                            case .error(_):
                                logLevel = .emergency
                            }
                            if let logLevel = logLevel {
                                self.logger?.log(level: logLevel, source: self, featureName: "commit", message: "Database.\(result)", data: [(name: "entityType", value: "\(type (of: entity))"), (name: "entityId", value: entity.id.uuidString), (name: "batchId", value: self.id.uuidString)])
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
                    for entity in self.entities.values {
                        self.logger?.log(level: .error, source: self, featureName: "commit", message: "batchTimeout", data: [(name: "batchId", value: self.id.uuidString), (name: "entityType", value: "\(type (of: entity))"), (name: "entityId", value: entity.id.uuidString), (name: "diagnosticHint", value: "Entity.queue is blocked or endless loop in Entity serialization")])
                    }
                    self.entities.removeAll()
                }
            }
            var isEmpty = false
            self.queue.sync {
                isEmpty = self.entities.isEmpty
            }
            if !isEmpty {
                self.commit (delay: retryInterval, retryInterval: retryInterval, timeout: timeout, completionHandler: completionHandler)
            } else if let completionHandler = completionHandler {
                completionHandler()
            }
        }
    }
    
    fileprivate func sync (closure: (Dictionary<UUID, EntityManagement>) -> ()) {
        queue.sync {
            closure (entities)
        }
    }
    
    private let logger: Logger?
    private let queue: DispatchQueue
    fileprivate let id: UUID
    fileprivate var entities: Dictionary<UUID, EntityManagement>
}


