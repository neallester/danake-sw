//
//  entity.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 1/26/18.
//

import Foundation

class Entity<T: Codable> {
    
    public private(set) var id: UUID
    public private(set) var version: Int
    private var item: T
    
    
}
