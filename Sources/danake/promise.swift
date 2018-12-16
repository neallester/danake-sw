//
//  promise.swift
//  danake
//
//  Created by Neal Lester on 11/28/18.
//

import Foundation
import PromiseKit

extension Promise {
    
/**
     Thread safe synchronous access the item in the promise of an entity.
     
     e.g. Access the company in a Promise\<Entity\<Company\>\>
     
     - parameter closure: A closure with one parameter of type entity's item
     
     - Returns: Promise\<Void\>
*/
    func sync<I:Codable>(closure: @escaping (I) -> ()) -> Promise<Void> where T == Entity<I> {
        return done() { (entity: Entity<I>) in
            entity.sync (closure: closure)
        }
    }
    
/**
     Update the item in the promise of an entity.
     
     e.g. update the company in a Promise\<Entity\<Company\>\>
     
     - parameter batch: The **EventuallyConsistentBatch** to which the entity is added
     
     - parameter closure: A closure with one inout parameter of type entity's item
     
     - Returns: Promise\<Void\>.
*/
    func update<I:Codable>(batch: EventuallyConsistentBatch, closure: @escaping (inout I) -> ()) -> Promise<Void> where T == Entity<I> {
        return done() { (entity: Entity<I>) in
            entity.update (batch: batch, closure: closure)
        }
    }
 
/**
     Obtain the promise of an entity from the **ReferenceManager** of another promised entity's item
     
     e.g. obtain Promise\<Entity\<CHILD\>\> from a Promise\<Entity\<PARENT\>\> where PARENT contains a ReferenceManager\<PARENT, CHILD\>
     
     - parameter closure: A closure with one parameter of type entity's item which returns the **ReferenceManager** from that item
     
     - Returns: Promise\<Entity\<CHILD\>\>

*/
    func referenceFromItem<I, R> (context: String?, closure: @escaping (I) -> ReferenceManager<I, R>) -> Promise<Entity<R>?> where T == Entity<I> {
        return then() { entity in
            return entity.referenceFromItem(context: context, closure: closure)
        }
    }
    
/**
     Obtain a promise from a promised entity's item.
     
     - parameter: A closure with parameter of type entity's item which returns a Promise.
     
     - Returns: Promise\<R\>
*/
    func promiseFromItem<I, R> (closure: @escaping (I) -> Promise<R>) -> Promise<R> where T == Entity<I> {
        return then() { entity in
            return entity.promiseFromItem(closure: closure)
        }
    }
    
}

