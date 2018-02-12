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
    
    func log (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?)
    
}

public class LogEntryFormatter {
    
    static func formattedData (data: [(name: String, value: CustomStringConvertible?)]?) -> String {
        var result = ""
        if let data = data {
            for entry in data where data.count > 0 {
                if (result.count > 0) {
                    result = result + ";"
                }
                var value: CustomStringConvertible = "nil"
                if let entryValue = entry.value {
                    value = entryValue
                }
                result = "\(result)\(entry.name)=\(String (describing: value))"
            }
        }
        return result
    }
    
    static func standardFormat (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) -> String {
        var formattedData = LogEntryFormatter.formattedData (data: data)
        if (formattedData.count > 0) {
            formattedData = "|" + formattedData
        }
        return "\(level.rawValue.uppercased())|\(type (of: source)).\(featureName)|\(message)\(formattedData)"
        
    }
    
}

struct LogEntry {
    
    init (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {
        time = Date()
        self.level = level
        self.source = source
        self.featureName = featureName
        self.message = message
        self.data = data
    }
    
    let time: Date
    let level: LogLevel
    let source: Any
    let featureName: String
    let message: String
    let data: [(name: String, value: CustomStringConvertible?)]?
    
    public func asTestString() -> String {
        return LogEntryFormatter.standardFormat (level: level, source: source, featureName: featureName, message: message, data: data)
    }
    
}

class InMemoryLogger : Logger {
    
    func log (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {
        if (level >= self.level) {
            queue.async () {
                self.entries.append(LogEntry (level: level, source: source, featureName: featureName, message: message, data: data))
            }
        }
    }
    
    func sync (closure: ([LogEntry]) -> Void) {
        queue.sync () {
            closure (self.entries)
        }
    }
    
    var level = LogLevel.debug
    
    private var entries: [LogEntry] = []
    private let queue = DispatchQueue (label: UUID().uuidString)
    
}

