//
//  logging.swift
//  danakePackageDescription
//
//  Created by Neal Lester on 2/9/18.
//

import Foundation

public enum LogLevel : String, Comparable {
    
    case debug
    case fine
    case info
    case warning
    case error
    case business
    case emergency
    case none

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
        return LogEntryFormatter.standardEntryFormat (level: level, source: "\(type (of: source))", featureName: featureName, message: message, data: data)
    }
    
    static func standardEntryFormat (level: LogLevel, source: String, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) -> String {
        var formattedData = LogEntryFormatter.formattedData (data: data)
        if (formattedData.count > 0) {
            formattedData = "|" + formattedData
        }
        return "\(level.rawValue.uppercased())|\(source).\(featureName)|\(message)\(formattedData)"
        
    }
    
}

struct LogEntry {
    
    init (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {
        time = Date()
        self.level = level
        self.source = "\(type (of: source))"
        self.featureName = featureName
        self.message = message
        self.data = data
    }
    
    let time: Date
    let level: LogLevel
    let source: String
    let featureName: String
    let message: String
    let data: [(name: String, value: CustomStringConvertible?)]?
    
    public func asTestString() -> String {
        return LogEntryFormatter.standardEntryFormat (level: level, source: source, featureName: featureName, message: message, data: data)
    }
    
}

class InMemoryLogger : Logger {
    
    init() {
        level = .debug
    }
    
    init (level: LogLevel) {
        self.level = level
    }
    
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
    
    let level: LogLevel
    
    private var entries: [LogEntry] = []
    private let queue = DispatchQueue (label: UUID().uuidString)
    
}

