//
//  logging.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 2/9/18.
//

import Foundation

public enum LogLevel : String, Comparable {
    
    case none
    case debug
    case fine
    case info
    case business
    case error
    case emergency
    
    public static func < (lhs: LogLevel, rhs: LogLevel) -> Bool {
        return lhs.hashValue < rhs.hashValue
    }
    
}

public protocol Logger {
    
    func log (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible)?])
    
}

