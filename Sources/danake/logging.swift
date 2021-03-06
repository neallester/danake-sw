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
        switch rhs {
        case .debug:
            return false
        case .fine:
            switch lhs {
            case .debug:
                return true
            default:
                return false
            }
        case .info:
            switch lhs {
            case .debug, .fine:
                return true
            default:
                return false
            }
        case .warning:
            switch lhs {
            case .debug, .fine, .info:
                return true
            default:
                return false
            }
        case .error:
            switch lhs {
            case .debug, .fine, .info, .warning:
                return true
            default:
                return false
            }
        case .business:
            switch lhs {
            case .debug, .fine, info, .warning, .error:
                return true
            default:
                return false
            }
        case .emergency:
            switch lhs {
            case .debug, .fine, info, .warning, .error, .business:
                return true
            default:
                return false
            }
        case .none:
            switch lhs {
            case .debug, .fine, info, .warning, .error, .business, .emergency:
                return true
            default:
                return false
            }
        }
    }
    
}

public protocol Logger {
    
    func log (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?)
    
}

open class LogEntryFormatter {
    
    public static func formattedData (data: [(name: String, value: CustomStringConvertible?)]?) -> String {
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

    public static func standardFormat (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) -> String {
        return LogEntryFormatter.standardEntryFormat (level: level, source: "\(type (of: source))", featureName: featureName, message: message, data: data)
    }
    
    public static func standardEntryFormat (level: LogLevel, source: String, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) -> String {
        var formattedData = LogEntryFormatter.formattedData (data: data)
        if (formattedData.count > 0) {
            formattedData = "|" + formattedData
        }
        return "\(level.rawValue.uppercased())|\(source).\(featureName)|\(message)\(formattedData)"
        
    }
    
}

public struct LogEntry {
    
    init (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {
        time = Date()
        self.level = level
        self.source = "\(type (of: source))"
        self.featureName = featureName
        self.message = message
        self.data = data
    }
    
    public let time: Date
    public let level: LogLevel
    public let source: String
    public let featureName: String
    public let message: String
    public let data: [(name: String, value: CustomStringConvertible?)]?
    
    public func asTestString() -> String {
        return LogEntryFormatter.standardEntryFormat (level: level, source: source, featureName: featureName, message: message, data: data)
    }
    
}

open class ThreadSafeLogger : Logger {

    public init() {
        level = .debug
    }
    
    public init (level: LogLevel) {
        self.level = level
    }
    
    public func log (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]? = nil) {
        if (level >= self.level) {
            queue.async () {
                self.logImplementation(level: level, source: source, featureName: featureName, message: message, data: data)
            }
        }
    }
    
    let level: LogLevel

    fileprivate func logImplementation (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {}
    
    fileprivate let queue = DispatchQueue (label: UUID().uuidString)
}

open class InMemoryLogger : ThreadSafeLogger {
    
    override func logImplementation (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {
        entries.append(LogEntry (level: level, source: source, featureName: featureName, message: message, data: data))
    }
    
    public func sync (closure: ([LogEntry]) -> Void) {
        queue.sync () {
            closure (self.entries)
        }
    }
    
    public func printAll() {
        queue.sync {
            for entry in entries {
                print (entry.asTestString())
            }
        }
    }
    
    public func waitForEntry(intervalUseconds: UInt32, timeoutSeconds: Double, condition: (([LogEntry]) -> Bool)) -> Bool {
        var foundEntry = false
        let endTime = Date().timeIntervalSince1970 + timeoutSeconds
        while (!foundEntry && Date().timeIntervalSince1970 < endTime) {
            queue.sync {
                if !entries.isEmpty {
                    foundEntry = condition (entries)
                }
            }
            if !foundEntry {
                usleep (intervalUseconds)
            }
        }
        return foundEntry
    }
    
    private var entries: [LogEntry] = []
    
}

open class ConsoleLogger : ThreadSafeLogger {
    
    override func logImplementation (level: LogLevel, source: Any, featureName: String, message: String, data: [(name: String, value: CustomStringConvertible?)]?) {
        print (LogEntry (level: level, source: source, featureName: featureName, message: message, data: data).asTestString())
    }
    
    public func sync (closure: ([LogEntry]) -> Void) {
        queue.sync () {
            closure (self.entries)
        }
    }
    
    private var entries: [LogEntry] = []
    
}


