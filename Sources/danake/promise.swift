//
//  promise.swift
//  danake
//
//  Created by Neal Lester on 11/28/18.
//

import Foundation
import PromiseKit

extension Promise {
    
    func sync<I:Codable>(closure: @escaping (I) -> ()) -> Promise<Void> where T == Entity<I> {
        return done() { (entity: Entity<I>) in
            entity.sync (closure: closure)
        }
    }
    
    func update<I:Codable>(batch: EventuallyConsistentBatch, closure: @escaping (inout I) -> ()) -> Promise<Void> where T == Entity<I> {
        return done() { (entity: Entity<I>) in
            entity.update (batch: batch, closure: closure)
        }
    }
 
    func referenceFromItem<I, R> (closure: @escaping (I) -> ReferenceManager<I, R>) -> Promise<Entity<R>?> where T == Entity<I> {
        return then() { entity in
            return entity.referenceFromItem(closure: closure)
        }
    }
    
    func promiseFromItem<I, R> (closure: @escaping (I) -> Promise<R>) -> Promise<R> where T == Entity<I> {
        return then() { entity in
            return entity.promiseFromItem(closure: closure)
        }
    }
    
}

